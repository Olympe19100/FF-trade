"""
Optimal Order Execution — Calendar Spread Fills

Centralized execution engine for placing and filling calendar spread orders
on IBKR. Handles pricing, smart routing, and fill optimization.

Execution flow:
  1. Price: ThetaData WS -> IBKR reqMktData -> EODHD scanner mid
  2. Route: Adaptive LMT @ mid — algo handles price discovery
  3. Routing: BAG-only (no individual legs fallback). Failed fills go to pending.

Usage:
    from core.execution import execute_spread

    result, cost, slip, details = execute_spread(ib, ticker, legs, ...)
"""

import random
import time as _time

import numpy as np
from ib_insync import IB, Option, Bag, ComboLeg, LimitOrder, MarketOrder, TagValue

from core.config import (
    FILL_TIMEOUT,
    COMBO_WALK_STEP, COMBO_WALK_WAIT,
    LMT_WALK_STEP, LMT_WALK_MAX, LMT_WALK_WAIT,
    SLICE_THRESHOLD, SLICE_SIZE,
    WALK_STEP_PCT, WALK_STEP_MIN, TIGHT_SPREAD_THRESHOLD,
    WALK_BACKOFF_FACTOR, WALK_BACKOFF_CAP,
    SLICE_PAUSE_LIQUID, SLICE_PAUSE_NORMAL, SLICE_PAUSE_ILLIQUID,
    SLICE_PAUSE_OI_THRESHOLD_HIGH, SLICE_PAUSE_OI_THRESHOLD_LOW,
    JITTER_PCT, ENABLE_JITTER,
    IBKR_QUOTE_SETTLE, MAX_COMBO_WALK_STEPS,
    QUOTE_SNIPE_SAMPLES, QUOTE_SNIPE_INTERVAL,
    FALLBACK_EXCHANGE, ENABLE_EXCHANGE_FALLBACK,
    get_logger,
)

log = get_logger(__name__)


# ═══════════════════════════════════════════════════════════════
#  ADAPTIVE HELPERS (academic-based improvements)
# ═══════════════════════════════════════════════════════════════

def _jitter(value: float) -> float:
    """Apply ±JITTER_PCT random variance to a value (Wyart et al. 2008, BIS 2020).

    Prevents market makers from fingerprinting deterministic walk/wait patterns.
    Returns value × uniform(1-JITTER_PCT, 1+JITTER_PCT).
    """
    if not ENABLE_JITTER:
        return value
    factor = random.uniform(1.0 - JITTER_PCT, 1.0 + JITTER_PCT)
    return value * factor


def _compute_walk_step(bid: float, ask: float, fallback: float) -> float:
    """Proportional walk step = 20% of bid-ask spread (Cont & Kukanov 2017).

    Jittered ±20% to prevent pattern detection (Wyart et al. 2008).
    Returns max(WALK_STEP_MIN, ~20% of BA), or fallback if BA is 0/invalid.
    """
    if bid > 0 and ask > bid:
        ba = ask - bid
        step = round(ba * WALK_STEP_PCT, 4)
        step = round(_jitter(step), 4)
        return max(WALK_STEP_MIN, step)
    return fallback


def _compute_walk_wait(initial_wait: int, step_idx: int) -> int:
    """Exponential backoff on walk wait (CFM 2018).

    Wait × 1.5 per step, capped at initial × WALK_BACKOFF_CAP.
    Jittered ±20% to prevent timing fingerprint (BIS 2020).
    step_idx is 0-based (0 = first walk step after initial attempt).
    """
    wait = initial_wait * (WALK_BACKOFF_FACTOR ** step_idx)
    cap = initial_wait * WALK_BACKOFF_CAP
    wait = min(wait, cap)
    return int(_jitter(wait))


def _compute_initial_price(action: str, bid: float, ask: float, mid: float) -> float:
    """Adaptive initial price based on spread width (Cont & Kukanov 2017).

    Tight spread (<$0.10): passive (BUY→bid+tick, SELL→ask-tick) to capture rebate.
    Wide spread (≥$0.10): mid (standard).
    """
    spread = round(ask - bid, 4) if ask > bid > 0 else 0.0
    if spread > 0 and spread < TIGHT_SPREAD_THRESHOLD:
        tick = 0.01
        if action == "BUY":
            return round(bid + tick, 2)
        else:
            return round(ask - tick, 2)
    return round(mid, 2)


def _compute_slice_size(contracts: int, min_leg_oi: int) -> int:
    """Adaptive slice size based on OI (target ~2% of min leg OI per slice)."""
    if min_leg_oi <= 0:
        return SLICE_SIZE  # default when OI unknown
    oi_based = max(1, int(min_leg_oi * 0.02))  # 2% of OI per slice
    return min(oi_based, SLICE_SIZE)  # never exceed SLICE_SIZE


def _compute_slice_pause(min_leg_oi: int) -> int:
    """Adaptive pause between slices based on liquidity (Bouchaud et al. 2009).

    OI >= 5000: ~60s (liquid), 500-5000: ~90s (normal), <500: ~120s (illiquid).
    Jittered ±20% to prevent temporal pattern detection (BIS 2020).
    """
    if min_leg_oi >= SLICE_PAUSE_OI_THRESHOLD_HIGH:
        base = SLICE_PAUSE_LIQUID
    elif min_leg_oi >= SLICE_PAUSE_OI_THRESHOLD_LOW:
        base = SLICE_PAUSE_NORMAL
    else:
        base = SLICE_PAUSE_ILLIQUID
    return int(_jitter(base))


# ═══════════════════════════════════════════════════════════════
#  PRICING — ThetaData WS primary, IBKR fallback
# ═══════════════════════════════════════════════════════════════

def get_combo_price(ib: IB, combo: Bag,
                    legs: list[tuple] | None = None) -> tuple[float, float, float]:
    """Synthetic combo bid/ask/mid from individual leg quotes.

    PRIMARY: IBKR individual leg quotes (synthesize combo price).
    FALLBACK 1: ThetaData WebSocket.
    FALLBACK 2: IBKR BAG reqMktData.

    Returns (bid, ask, mid) or (0, 0, 0) if unavailable.
    """
    # PRIMARY: Synthesize combo from IBKR individual leg quotes
    if legs is not None:
        combo_bid = combo_ask = 0.0
        all_ok = True
        leg_quotes = []

        # Request all legs in parallel
        for option_contract, _ in legs:
            ib.reqMktData(option_contract, "", False, False)
        ib.sleep(IBKR_QUOTE_SETTLE)

        for option_contract, action in legs:
            tk = ib.ticker(option_contract)
            b = a = 0.0
            if tk:
                raw_b = tk.bid
                raw_a = tk.ask
                b = float(raw_b) if raw_b and raw_b > 0 and not np.isinf(raw_b) else 0
                a = float(raw_a) if raw_a and raw_a > 0 and not np.isinf(raw_a) else 0
            if b > 0 and a > 0:
                leg_quotes.append((b, a, action))
            else:
                all_ok = False

        # Cancel all
        for option_contract, _ in legs:
            ib.cancelMktData(option_contract)

        if all_ok and leg_quotes:
            for b, a, action in leg_quotes:
                if action == "BUY":
                    combo_bid += b    # worst case: pay ask for buys
                    combo_ask += a
                else:
                    combo_bid -= a    # worst case: receive bid for sells
                    combo_ask -= b
            # combo_bid = minimum we'd pay (buy at bid, sell at ask)
            # combo_ask = maximum we'd pay (buy at ask, sell at bid)
            # Swap if needed (combo perspective: we're buying the spread)
            if combo_bid > combo_ask:
                combo_bid, combo_ask = combo_ask, combo_bid
            combo_bid = round(combo_bid, 4)
            combo_ask = round(combo_ask, 4)
            mid = round((combo_bid + combo_ask) / 2, 4)
            if mid > 0:
                log.info("      IBKR combo: bid=%.4f ask=%.4f mid=%.4f", combo_bid, combo_ask, mid)
                return combo_bid, combo_ask, mid

    # FALLBACK 1: ThetaData WebSocket
    if legs is not None:
        try:
            from core.theta_ws import theta_ws_get_combo_price
            bid, ask, mid = theta_ws_get_combo_price(legs)
            if mid > 0:
                log.info("      ThetaWS combo: bid=%.4f ask=%.4f mid=%.4f", bid, ask, mid)
                return bid, ask, mid
        except Exception as ex:
            log.debug("      ThetaWS combo failed: %s", ex)

    # FALLBACK 2: IBKR BAG reqMktData
    log.debug("      Trying IBKR BAG reqMktData")
    ib.reqMktData(combo, "", False, False)
    ib.sleep(IBKR_QUOTE_SETTLE)

    tk = ib.ticker(combo)
    bid = ask = mid = 0.0

    if tk:
        b = tk.bid
        a = tk.ask
        bid = float(b) if b and b > 0 and not np.isinf(b) else 0
        ask = float(a) if a and a > 0 and not np.isinf(a) else 0
        if bid > 0 and ask > 0:
            mid = round((bid + ask) / 2, 2)
            log.info("      IBKR BAG: bid=%.4f ask=%.4f mid=%.4f", bid, ask, mid)

    ib.cancelMktData(combo)
    return bid, ask, mid


def _snipe_best_quote(ib: IB, combo: Bag,
                      legs: list[tuple] | None,
                      samples: int, interval: int) -> tuple[float, float, float]:
    """Sample combo quotes N times and return the tightest spread.

    Calls get_combo_price() up to `samples` times, sleeping `interval`
    seconds between each. Returns the sample with the narrowest bid-ask
    spread, or (0, 0, 0) if all samples fail.

    Early exit if spread <= TIGHT_SPREAD_THRESHOLD ($0.10).
    """
    best = (0.0, 0.0, 0.0)
    best_spread = float("inf")

    for i in range(samples):
        bid, ask, mid = get_combo_price(ib, combo, legs=legs)
        if mid <= 0:
            log.info("      Snipe %d/%d: no quote", i + 1, samples)
            if i < samples - 1:
                ib.sleep(interval)
            continue

        spread = round(ask - bid, 4) if ask > bid else 0.0
        log.info("      Snipe %d/%d: bid=%.4f ask=%.4f spread=%.4f",
                 i + 1, samples, bid, ask, spread)

        if spread < best_spread:
            best = (bid, ask, mid)
            best_spread = spread

        # Early exit on tight spread
        if spread <= TIGHT_SPREAD_THRESHOLD:
            log.info("      Snipe: spread $%.4f <= $%.2f threshold, using immediately",
                     spread, TIGHT_SPREAD_THRESHOLD)
            break

        if i < samples - 1:
            ib.sleep(interval)

    return best


def get_leg_price(ib: IB, option_contract: Option) -> tuple[float, float, float]:
    """Bid/ask/mid for a single option contract.

    PRIMARY: IBKR reqMktData (OPRA subscription).
    FALLBACK: ThetaData WebSocket.

    Returns (bid, ask, mid) or (0, 0, 0) if unavailable.
    """
    # PRIMARY: IBKR reqMktData (OPRA subscription)
    ib.reqMktData(option_contract, "", False, False)
    ib.sleep(IBKR_QUOTE_SETTLE)

    tk = ib.ticker(option_contract)
    bid = ask = mid = 0.0

    if tk:
        b = tk.bid
        a = tk.ask
        bid = float(b) if b and b > 0 and not np.isinf(b) else 0
        ask = float(a) if a and a > 0 and not np.isinf(a) else 0
        if bid > 0 and ask > 0:
            mid = round((bid + ask) / 2, 2)
            log.info("      IBKR leg: bid=%.2f ask=%.2f mid=%.2f", bid, ask, mid)

    ib.cancelMktData(option_contract)

    if mid > 0:
        return bid, ask, mid

    # FALLBACK: ThetaData WebSocket
    try:
        from core.theta_ws import theta_ws_get_leg_price
        bid, ask, mid = theta_ws_get_leg_price(option_contract)
        if mid > 0:
            log.info("      ThetaWS leg: bid=%.2f ask=%.2f mid=%.2f", bid, ask, mid)
            return bid, ask, mid
    except Exception as ex:
        log.debug("      ThetaWS leg failed: %s", ex)

    log.warning("      No leg price from IBKR or ThetaWS for %s %s K=%s",
                option_contract.right, option_contract.lastTradeDateOrContractMonth,
                option_contract.strike)
    return 0.0, 0.0, 0.0


# ═══════════════════════════════════════════════════════════════
#  COMBO EXECUTION — Adaptive @ mid, algo manages price
# ═══════════════════════════════════════════════════════════════

def _wait_for_fill(ib: IB, trade, seconds: int) -> str | None:
    """Block up to `seconds`, return status on terminal state or None on timeout."""
    for _ in range(seconds):
        ib.sleep(1)
        status = trade.orderStatus.status
        if status == "Filled":
            return "Filled"
        if status in ("Cancelled", "ApiCancelled", "Inactive"):
            return status
    return None


def execute_combo(ib: IB, combo: Bag, action: str, contracts: int,
                  eodhd_mid: float,
                  legs: list[tuple] | None = None,
                  priority: str = "Normal") -> tuple[bool, float, float, dict]:
    """Fill a combo order — LMT @ mid with progressive walk.

    Pricing waterfall: ThetaData WS -> IBKR reqMktData -> eodhd_mid.
    Routing: LMT @ mid with Adaptive algo, then walk toward far
    side of spread in COMBO_WALK_STEP increments every COMBO_WALK_WAIT seconds.

    Args:
        ib: IBKR connection.
        combo: Bag contract.
        action: "BUY" for entry, "SELL" for close.
        contracts: number of contracts.
        eodhd_mid: scanner's cost estimate as last-resort price.
        legs: (Option, action) tuples for ThetaData pricing.
        priority: Adaptive algo priority — "Normal" (patient) or "Urgent".

    Returns (filled, fill_price, slippage_vs_eodhd, exec_details).
    """
    t0 = _time.time()
    walk_steps_taken = 0

    if QUOTE_SNIPE_SAMPLES > 1:
        bid, ask, mid = _snipe_best_quote(
            ib, combo, legs, QUOTE_SNIPE_SAMPLES, QUOTE_SNIPE_INTERVAL
        )
    else:
        bid, ask, mid = get_combo_price(ib, combo, legs=legs)

    # Last-resort: use EODHD mid when live data is unavailable
    if mid <= 0 and eodhd_mid > 0:
        log.info("      Combo: no live data, using EODHD mid=$%.2f", eodhd_mid)
        mid = eodhd_mid
        bid = ask = mid

    if mid <= 0:
        log.warning("      Combo: no price at all, skipping")
        return False, 0.0, 0.0, {}

    spread = round(ask - bid, 4) if ask > bid else 0.0

    # ── Adaptive initial price (Cont & Kukanov 2017) ──
    limit_price = _compute_initial_price(action, bid, ask, mid)
    initial_limit = limit_price
    walk_step = _compute_walk_step(bid, ask, COMBO_WALK_STEP)
    log.info("      Combo %s: LMT $%.2f x%d [bid=%.2f ask=%.2f spread=%.4f walk_step=%.3f priority=%s]",
             action, limit_price, contracts, bid, ask, spread, walk_step, priority)

    # Plain LMT for combos — IBKR Adaptive algo does NOT support BAG orders
    # (Error 329: "Cannot change to the new order type.IBALGO")
    order = LimitOrder(action, contracts, limit_price)
    order.outsideRth = False
    order.tif = "DAY"

    initial_wait = COMBO_WALK_WAIT
    trade = ib.placeOrder(combo, order)
    result = _wait_for_fill(ib, trade, initial_wait)

    def _build_exec_details():
        return {
            "theta_bid": bid,
            "theta_ask": ask,
            "theta_mid": mid,
            "walk_steps": walk_steps_taken,
            "initial_limit": initial_limit,
            "final_limit": limit_price,
            "exec_seconds": round(_time.time() - t0, 2),
            "priority": priority,
        }

    if result == "Filled":
        fill_px = trade.orderStatus.avgFillPrice
        slip = fill_px - eodhd_mid if action == "BUY" else eodhd_mid - fill_px
        log.info("      Combo FILLED @ $%.2f (slip=%+.2f)", fill_px, slip)
        return True, fill_px, slip, _build_exec_details()

    if result in ("Cancelled", "ApiCancelled", "Inactive"):
        log.error("      Combo rejected: %s", result)
        return False, 0.0, 0.0, _build_exec_details()

    # Cancel before walking (cancel+replace avoids Error 103/329 on modify)
    ib.cancelOrder(order)
    ib.sleep(1)

    # ── Progressive walk toward far side of spread ──
    if action == "BUY":
        walk_limit = round(ask if ask > mid else mid * 1.02, 2)
    else:
        walk_limit = round(bid if bid < mid else mid * 0.98, 2)

    max_steps = max(1, int(abs(walk_limit - limit_price) / walk_step)) if walk_step > 0 else 1
    max_steps = min(max_steps, MAX_COMBO_WALK_STEPS)

    for step in range(max_steps):
        walk_steps_taken += 1

        # Re-price every 3 walk steps to avoid walking toward stale target
        if step > 0 and step % 3 == 0 and legs is not None:
            new_bid, new_ask, new_mid = get_combo_price(ib, combo, legs=legs)
            if new_mid > 0:
                old_limit = walk_limit
                if action == "BUY":
                    walk_limit = round(new_ask if new_ask > new_mid else new_mid * 1.02, 2)
                else:
                    walk_limit = round(new_bid if new_bid < new_mid else new_mid * 0.98, 2)
                if walk_limit != old_limit:
                    log.info("      Re-priced walk target: $%.2f -> $%.2f", old_limit, walk_limit)

        if action == "BUY":
            limit_price = round(min(limit_price + walk_step, walk_limit), 2)
        else:
            limit_price = round(max(limit_price - walk_step, walk_limit), 2)

        wait = _compute_walk_wait(initial_wait, step)
        log.info("      Combo walk %d/%d: LMT $%.2f (wait %ds)",
                 step + 1, max_steps, limit_price, wait)
        new_order = LimitOrder(action, contracts, limit_price)
        new_order.outsideRth = False
        new_order.tif = "DAY"
        trade = ib.placeOrder(combo, new_order)

        result = _wait_for_fill(ib, trade, wait)

        if result == "Filled":
            fill_px = trade.orderStatus.avgFillPrice
            slip = fill_px - eodhd_mid if action == "BUY" else eodhd_mid - fill_px
            log.info("      Combo FILLED @ $%.2f (slip=%+.2f, walk %d)",
                     fill_px, slip, step + 1)
            return True, fill_px, slip, _build_exec_details()

        if result in ("Cancelled", "ApiCancelled", "Inactive"):
            log.error("      Combo rejected during walk: %s", result)
            return False, 0.0, 0.0, _build_exec_details()

        # Cancel before next step
        ib.cancelOrder(new_order)
        ib.sleep(1)

    log.info("      Combo: not filled after walk to $%.2f (%d steps)",
             limit_price, max_steps)
    return False, 0.0, 0.0, _build_exec_details()


# ═══════════════════════════════════════════════════════════════
#  LEG EXECUTION — Adaptive Urgent + MKT fallback
# ═══════════════════════════════════════════════════════════════

def execute_leg(ib: IB, leg_contract: Option, action: str,
                contracts: int, eodhd_mid: float = 0) -> tuple[bool, float]:
    """Execute a single leg — LMT @ mid with progressive walk.

    Routing:
      - Has price: Adaptive Urgent LMT @ mid, walk by LMT_WALK_STEP.
      - No live data + EODHD: Adaptive Urgent LMT @ eodhd_mid, walk.
      - No price at all: MKT order (last resort).

    Returns (filled, fill_price).
    """
    bid, ask, mid = get_leg_price(ib, leg_contract)

    if mid <= 0 and eodhd_mid > 0:
        log.info("        No live data, using EODHD mid=$%.2f", eodhd_mid)
        mid = eodhd_mid
        bid = ask = mid

    # No price at all → MKT order (last resort)
    if mid <= 0:
        log.warning("        No price data, using MKT")
        order = MarketOrder(action, contracts)
        order.outsideRth = False
        order.tif = "DAY"
        trade = ib.placeOrder(leg_contract, order)
        result = _wait_for_fill(ib, trade, FILL_TIMEOUT)
        if result == "Filled":
            return True, trade.orderStatus.avgFillPrice
        if result is None:
            ib.cancelOrder(order)
            ib.sleep(2)
        return False, 0.0

    # ── Adaptive initial price (Cont & Kukanov 2017) ──
    limit_price = _compute_initial_price(action, bid, ask, mid)
    walk_step = _compute_walk_step(bid, ask, LMT_WALK_STEP)
    log.info("      %s %s %s K=%s x%d LMT $%.2f [bid=%.2f ask=%.2f walk_step=%.3f]",
             action, leg_contract.right,
             leg_contract.lastTradeDateOrContractMonth,
             leg_contract.strike, contracts, limit_price, bid, ask, walk_step)

    order = LimitOrder(action, contracts, limit_price)
    order.algoStrategy = "Adaptive"
    order.algoParams = [TagValue("adaptivePriority", "Urgent")]
    order.outsideRth = False
    order.tif = "DAY"

    initial_wait = LMT_WALK_WAIT
    trade = ib.placeOrder(leg_contract, order)
    result = _wait_for_fill(ib, trade, initial_wait)

    if result == "Filled":
        fill_px = trade.orderStatus.avgFillPrice
        log.info("        -> FILLED @ $%.2f", fill_px)
        return True, fill_px
    if result in ("Cancelled", "ApiCancelled", "Inactive"):
        log.error("        -> REJECTED (%s)", result)
        return False, 0.0

    # Cancel before walking (cancel+replace avoids Error 103/329 on modify)
    ib.cancelOrder(order)
    ib.sleep(1)

    # ── Progressive walk toward far side (cancel+replace at each step) ──
    for step in range(LMT_WALK_MAX):
        if action == "BUY":
            limit_price = round(limit_price + walk_step, 2)
            if ask > 0 and limit_price > ask:
                limit_price = round(ask, 2)
        else:
            limit_price = round(limit_price - walk_step, 2)
            if bid > 0 and limit_price < bid:
                limit_price = round(bid, 2)

        wait = _compute_walk_wait(initial_wait, step)
        log.info("        Walk %d/%d: LMT $%.2f (wait %ds)",
                 step + 1, LMT_WALK_MAX, limit_price, wait)
        new_order = LimitOrder(action, contracts, limit_price)
        new_order.algoStrategy = "Adaptive"
        new_order.algoParams = [TagValue("adaptivePriority", "Urgent")]
        new_order.outsideRth = False
        new_order.tif = "DAY"
        trade = ib.placeOrder(leg_contract, new_order)

        result = _wait_for_fill(ib, trade, wait)

        if result == "Filled":
            fill_px = trade.orderStatus.avgFillPrice
            log.info("        -> FILLED @ $%.2f (walk %d)", fill_px, step + 1)
            return True, fill_px
        if result in ("Cancelled", "ApiCancelled", "Inactive"):
            log.error("        -> REJECTED during walk (%s)", result)
            return False, 0.0

        # Cancel before next step
        ib.cancelOrder(new_order)
        ib.sleep(1)

    # Walk exhausted
    log.info("        -> NOT FILLED after %d walk steps", LMT_WALK_MAX)
    return False, 0.0


# ═══════════════════════════════════════════════════════════════
#  SPREAD EXECUTION — Combo-first, legs-fallback orchestrator
# ═══════════════════════════════════════════════════════════════

def _build_combo(legs: list[tuple[Option, str]],
                 exchange: str = "SMART") -> Bag | None:
    """Build a BAG contract from qualified individual legs."""
    if not legs:
        return None

    combo = Bag()
    combo.symbol = legs[0][0].symbol
    combo.exchange = exchange
    combo.currency = "USD"

    combo_legs = []
    for option_contract, action in legs:
        if option_contract.conId == 0:
            return None
        cl = ComboLeg()
        cl.conId = option_contract.conId
        cl.ratio = 1
        cl.action = action
        cl.exchange = exchange
        combo_legs.append(cl)

    combo.comboLegs = combo_legs
    return combo


def _execute_spread_once(ib: IB, ticker: str,
                         legs: list[tuple[Option, str]], n_legs: int,
                         contracts: int, eodhd_cps: float,
                         spread_type: str,
                         priority: str = "Normal") -> tuple[str, float, float, dict]:
    """Execute a single batch of a calendar spread (no slicing).

    BAG-only: combo order via IBKR matching engine. No individual legs fallback.
    Failed fills go to pending for retry the next day.

    Returns (result, fill_cost, slippage, details).
        result: "full" | "failed"
    """
    t0 = _time.time()

    details = {
        "method": None,
        "combo_attempted": False,
        "combo_result": None,
        "leg_fills": [],
        "theta_bid": 0.0,
        "theta_ask": 0.0,
        "theta_mid": 0.0,
        "walk_steps": 0,
        "initial_limit": 0.0,
        "final_limit": 0.0,
        "exec_seconds": 0.0,
        "priority": priority,
    }

    # ── Combo (BAG-only, no legs fallback) ──
    log.info("    %s: BAG-only combo (%d legs)", ticker, n_legs)
    combo = _build_combo(legs)

    if combo is not None:
        details["combo_attempted"] = True
        filled, fill_px, slippage, exec_details = execute_combo(
            ib, combo, "BUY", contracts, eodhd_cps, legs=legs,
            priority=priority
        )
        # Capture timing metadata from combo execution
        details.update({
            "theta_bid": exec_details.get("theta_bid", 0.0),
            "theta_ask": exec_details.get("theta_ask", 0.0),
            "theta_mid": exec_details.get("theta_mid", 0.0),
            "walk_steps": exec_details.get("walk_steps", 0),
            "initial_limit": exec_details.get("initial_limit", 0.0),
            "final_limit": exec_details.get("final_limit", 0.0),
        })
        if filled:
            details["method"] = "combo"
            details["combo_result"] = "filled"
            details["exec_seconds"] = round(_time.time() - t0, 2)
            _update_fill_stats_safe(ticker, "combo", True, True, True,
                                    details["exec_seconds"])
            return "full", fill_px, slippage, details
        else:
            details["combo_result"] = "no_fill"
            # ── BOX exchange fallback (price improvement auction) ──
            if ENABLE_EXCHANGE_FALLBACK:
                log.info("    %s: SMART exhausted, trying %s fallback", ticker, FALLBACK_EXCHANGE)
                box_combo = _build_combo(legs, exchange=FALLBACK_EXCHANGE)
                if box_combo is not None:
                    filled, fill_px, slippage, exec_details = execute_combo(
                        ib, box_combo, "BUY", contracts, eodhd_cps, legs=legs,
                        priority=priority
                    )
                    details["fallback_exchange"] = FALLBACK_EXCHANGE
                    details["fallback_attempted"] = True
                    details["walk_steps"] += exec_details.get("walk_steps", 0)
                    for k in ("theta_bid", "theta_ask", "theta_mid", "initial_limit", "final_limit"):
                        details[k] = exec_details.get(k, 0.0)
                    if filled:
                        details["method"] = f"combo_{FALLBACK_EXCHANGE}"
                        details["combo_result"] = f"filled_{FALLBACK_EXCHANGE}"
                        details["exec_seconds"] = round(_time.time() - t0, 2)
                        _update_fill_stats_safe(ticker, f"combo_{FALLBACK_EXCHANGE}", True, True, True,
                                                details["exec_seconds"])
                        return "full", fill_px, slippage, details
                    else:
                        log.warning("    %s: %s fallback also failed", ticker, FALLBACK_EXCHANGE)

            log.warning("    %s: BAG combo failed — signal goes to pending", ticker)
    else:
        log.warning("    %s: Cannot build combo — signal goes to pending", ticker)

    details["exec_seconds"] = round(_time.time() - t0, 2)
    _update_fill_stats_safe(
        ticker, "combo", False,
        details["combo_attempted"],
        False,
        details["exec_seconds"],
    )
    return "failed", 0.0, 0.0, details


def _update_fill_stats_safe(ticker, method, filled, combo_attempted,
                             combo_filled, exec_seconds):
    """Update fill stats DB, silently ignoring errors."""
    try:
        from core.fill_db import update_fill_stats
        update_fill_stats(ticker, method, filled, combo_attempted,
                          combo_filled, exec_seconds)
    except Exception:
        pass


def execute_spread(ib: IB, ticker: str,
                   legs: list[tuple[Option, str]], n_legs: int,
                   contracts: int, eodhd_cps: float,
                   spread_type: str,
                   min_leg_oi: int = 0,
                   priority: str = "Normal") -> tuple[str, float, float, dict]:
    """Execute a calendar spread with optimal routing + order slicing.

    For orders <= SLICE_THRESHOLD contracts: execute in one batch.
    For larger orders: slice into SLICE_SIZE chunks with adaptive pause
    between them to minimize market impact (Almgren & Chriss 2000,
    Bouchaud et al. 2009).

    Args:
        min_leg_oi: minimum OI across legs, used for adaptive slice pause.
        priority: Adaptive algo priority — "Normal" or "Urgent".

    Returns (result, fill_cost, slippage, details).
        result: "full" | "partial" | "failed"
    """
    if contracts <= SLICE_THRESHOLD:
        return _execute_spread_once(
            ib, ticker, legs, n_legs, contracts, eodhd_cps, spread_type,
            priority=priority
        )

    # ── Order slicing: split into chunks ──
    slice_pause = _compute_slice_pause(min_leg_oi)
    adaptive_slice = _compute_slice_size(contracts, min_leg_oi)
    n_slices = (contracts + adaptive_slice - 1) // adaptive_slice
    log.info("    %s: SLICING %d contracts into %d chunks of max %d (pause=%ds, OI=%d)",
             ticker, contracts, n_slices, adaptive_slice, slice_pause, min_leg_oi)

    remaining = contracts
    fill_prices = []
    total_slippage = 0.0
    agg_details = {
        "method": None,
        "combo_attempted": False,
        "combo_result": None,
        "leg_fills": [],
        "slices": [],
    }

    slice_idx = 0
    live_ref_price = eodhd_cps  # initial reference from scanner
    while remaining > 0:
        chunk = min(adaptive_slice, remaining)
        slice_idx += 1

        # Re-price via IBKR live data before each slice (avoid stale reference)
        if slice_idx > 1:
            combo = _build_combo(legs)
            if combo is not None:
                _, _, live_mid = get_combo_price(ib, combo, legs=legs)
                if live_mid > 0:
                    log.info("    %s: Slice %d re-priced: $%.2f -> $%.2f",
                             ticker, slice_idx, live_ref_price, live_mid)
                    live_ref_price = live_mid

        log.info("    %s: Slice %d/%d (%d cts, ref=$%.2f)",
                 ticker, slice_idx, n_slices, chunk, live_ref_price)

        result, cost, slip, details = _execute_spread_once(
            ib, ticker, legs, n_legs, chunk, live_ref_price, spread_type,
            priority=priority
        )

        agg_details["slices"].append({
            "contracts": chunk, "result": result,
            "cost": cost, "slip": slip,
        })

        if result == "full":
            fill_prices.append(cost)
            total_slippage += slip
            remaining -= chunk
            agg_details["method"] = agg_details["method"] or details.get("method")
            agg_details["combo_attempted"] = (
                agg_details["combo_attempted"]
                or details.get("combo_attempted", False)
            )
            if details.get("combo_result"):
                agg_details["combo_result"] = details["combo_result"]
            agg_details["leg_fills"].extend(details.get("leg_fills", []))

            if remaining > 0:
                log.info("    %s: Slice OK. Pausing %ds (%d remaining)...",
                         ticker, slice_pause, remaining)
                ib.sleep(slice_pause)
        else:
            log.warning("    %s: Slice failed, stopping", ticker)
            if fill_prices:
                avg_cost = sum(fill_prices) / len(fill_prices)
                avg_slip = total_slippage / len(fill_prices)
                filled_cts = sum(
                    s["contracts"] for s in agg_details["slices"]
                    if s["result"] == "full"
                )
                agg_details["filled_contracts"] = filled_cts
                return "partial", avg_cost, avg_slip, agg_details
            return "failed", 0.0, 0.0, agg_details

    # All slices filled
    avg_cost = sum(fill_prices) / len(fill_prices)
    avg_slip = total_slippage / len(fill_prices)
    log.info("    %s: ALL %d slices filled. Avg cost=$%.2f, avg slip=$%+.2f",
             ticker, len(fill_prices), avg_cost, avg_slip)
    return "full", avg_cost, avg_slip, agg_details


def execute_spread_close(ib: IB, ticker: str,
                         legs: list[tuple[Option, str]], n_legs: int,
                         contracts: int) -> tuple[bool, float, str, list]:
    """Close a spread position (reverse entry actions).

    BAG-only: combo SELL. No individual legs fallback.
    If combo close fails, manual intervention is needed.

    Returns (success, exit_price, method, filled_legs).
    """
    # Reverse actions for close (BUY→SELL, SELL→BUY)
    # Close SELL (short front) first, then close BUY (long back)
    close_legs = []
    for leg_contract, entry_action in reversed(legs):
        close_action = "SELL" if entry_action == "BUY" else "BUY"
        close_legs.append((leg_contract, close_action))

    # ── Combo close (BAG-only, no legs fallback) ──
    log.info("    %s: Close BAG-only combo (%d legs)", ticker, n_legs)
    combo = _build_combo(close_legs)
    if combo is not None:
        filled, fill_px, _, exec_details = execute_combo(
            ib, combo, "SELL", contracts, eodhd_mid=0, legs=close_legs
        )
        if filled:
            return True, fill_px, "combo", []

        # ── BOX exchange fallback for close ──
        if ENABLE_EXCHANGE_FALLBACK:
            log.info("    %s: SMART close exhausted, trying %s fallback", ticker, FALLBACK_EXCHANGE)
            box_combo = _build_combo(close_legs, exchange=FALLBACK_EXCHANGE)
            if box_combo is not None:
                filled, fill_px, _, _ = execute_combo(
                    ib, box_combo, "SELL", contracts, eodhd_mid=0, legs=close_legs
                )
                if filled:
                    return True, fill_px, f"combo_{FALLBACK_EXCHANGE}", []
                else:
                    log.warning("    %s: %s close fallback also failed", ticker, FALLBACK_EXCHANGE)

    log.warning("    %s: BAG combo close failed — manual intervention needed", ticker)
    return False, 0.0, "failed", []
