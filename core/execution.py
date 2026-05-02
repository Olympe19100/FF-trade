"""
Optimal Order Execution — Calendar Spread Fills

Centralized execution engine for placing and filling calendar spread orders
on IBKR. Handles pricing, smart routing, and fill optimization.

Execution flow:
  1. Price: ThetaData WS -> IBKR reqMktData -> EODHD scanner mid
  2. Route: Adaptive LMT @ mid — algo handles price discovery
  3. Fallback: combo BAG -> individual legs (BUY-first, no naked short)

Usage:
    from core.execution import execute_spread

    result, cost, slip, details = execute_spread(ib, ticker, legs, ...)
"""

import numpy as np
from ib_insync import IB, Option, Bag, ComboLeg, LimitOrder, MarketOrder, TagValue

from core.config import (
    FILL_TIMEOUT,
    COMBO_TIMEOUT,
    LEG_TIMEOUT,
    get_logger,
)

log = get_logger(__name__)


# ═══════════════════════════════════════════════════════════════
#  PRICING — ThetaData WS primary, IBKR fallback
# ═══════════════════════════════════════════════════════════════

def get_combo_price(ib: IB, combo: Bag,
                    legs: list[tuple] | None = None) -> tuple[float, float, float]:
    """Synthetic combo bid/ask/mid from individual leg quotes.

    PRIMARY: ThetaData WebSocket (resilient per-leg retry).
    FALLBACK: IBKR BAG reqMktData.

    Returns (bid, ask, mid) or (0, 0, 0) if unavailable.
    """
    # PRIMARY: ThetaData WebSocket
    if legs is not None:
        try:
            from core.theta_ws import theta_ws_get_combo_price
            bid, ask, mid = theta_ws_get_combo_price(legs)
            if mid > 0:
                log.info("      ThetaWS combo: bid=%.4f ask=%.4f mid=%.4f", bid, ask, mid)
                return bid, ask, mid
        except Exception as ex:
            log.debug("      ThetaWS combo failed: %s", ex)

    # FALLBACK: IBKR BAG reqMktData
    ib.reqMktData(combo, "", False, False)
    ib.sleep(3)

    tk = ib.ticker(combo)
    bid = ask = mid = 0.0

    if tk:
        b = tk.bid
        a = tk.ask
        bid = float(b) if b and b > 0 and not np.isinf(b) else 0
        ask = float(a) if a and a > 0 and not np.isinf(a) else 0
        if bid > 0 and ask > 0:
            mid = round((bid + ask) / 2, 2)

    ib.cancelMktData(combo)
    return bid, ask, mid


def get_leg_price(ib: IB, option_contract: Option) -> tuple[float, float, float]:
    """Bid/ask/mid for a single option contract.

    PRIMARY: ThetaData WebSocket.
    FALLBACK: IBKR reqMktData.

    Returns (bid, ask, mid) or (0, 0, 0) if unavailable.
    """
    # PRIMARY: ThetaData WebSocket
    try:
        from core.theta_ws import theta_ws_get_leg_price
        bid, ask, mid = theta_ws_get_leg_price(option_contract)
        if mid > 0:
            log.info("      ThetaWS leg: bid=%.2f ask=%.2f mid=%.2f", bid, ask, mid)
            return bid, ask, mid
    except Exception as ex:
        log.debug("      ThetaWS leg failed: %s", ex)

    # FALLBACK: IBKR reqMktData
    ib.reqMktData(option_contract, "", False, False)
    ib.sleep(2)

    tk = ib.ticker(option_contract)
    bid = ask = mid = 0.0

    if tk:
        b = tk.bid
        a = tk.ask
        bid = float(b) if b and b > 0 and not np.isinf(b) else 0
        ask = float(a) if a and a > 0 and not np.isinf(a) else 0
        if bid > 0 and ask > 0:
            mid = round((bid + ask) / 2, 2)

    ib.cancelMktData(option_contract)
    return bid, ask, mid


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
                  legs: list[tuple] | None = None) -> tuple[bool, float, float]:
    """Fill a combo order — Adaptive algo manages price discovery.

    Pricing waterfall: ThetaData WS -> IBKR reqMktData -> eodhd_mid.
    Routing: LMT @ mid — Adaptive algo optimizes fill price from there.

    Args:
        ib: IBKR connection.
        combo: Bag contract.
        action: "BUY" for entry, "SELL" for close.
        contracts: number of contracts.
        eodhd_mid: scanner's cost estimate as last-resort price.
        legs: (Option, action) tuples for ThetaData pricing.

    Returns (filled, fill_price, slippage_vs_eodhd).
    """
    bid, ask, mid = get_combo_price(ib, combo, legs=legs)

    # Last-resort: use EODHD mid when live data is unavailable
    if mid <= 0 and eodhd_mid > 0:
        log.info("      Combo: no live data, using EODHD mid=$%.2f", eodhd_mid)
        mid = eodhd_mid
        bid = ask = mid

    if mid <= 0:
        log.warning("      Combo: no price at all, skipping")
        return False, 0.0, 0.0

    spread = round(ask - bid, 4) if ask > bid else 0.0

    # ── Market Order ──
    log.info(f"      Combo {action}: MARKET order ({contracts} cts)")

    order = MarketOrder(action, contracts)
    order.outsideRth = False
    order.tif = "DAY"

    trade = ib.placeOrder(combo, order)
    result = _wait_for_fill(ib, trade, 30) # Wait up to 30s for market fill

    if result == "Filled":
        fill_px = trade.orderStatus.avgFillPrice
        slip = fill_px - eodhd_mid if action == "BUY" else eodhd_mid - fill_px
        log.info("      Combo FILLED @ $%.2f (slip=%+.2f, Adaptive %ds)",
                 fill_px, slip, COMBO_TIMEOUT)
        return True, fill_px, slip

    if result in ("Cancelled", "ApiCancelled", "Inactive"):
        log.error("      Combo rejected: %s", result)
        return False, 0.0, 0.0

    # Timeout — cancel and return
    ib.cancelOrder(order)
    ib.sleep(2)
    log.info("      Combo: not filled after %ds, Adaptive LMT $%.2f",
             COMBO_TIMEOUT, limit_price)
    return False, 0.0, 0.0


# ═══════════════════════════════════════════════════════════════
#  LEG EXECUTION — Adaptive Urgent + MKT fallback
# ═══════════════════════════════════════════════════════════════

def execute_leg(ib: IB, leg_contract: Option, action: str,
                contracts: int, eodhd_mid: float = 0) -> tuple[bool, float]:
    """Execute a single leg — Adaptive algo manages price discovery.

    Routing:
      - Has price: Adaptive Urgent LMT @ mid.
      - No live data + EODHD: Adaptive Urgent @ eodhd_mid.
      - No price at all: MKT order.

    Returns (filled, fill_price).
    """
    bid, ask, mid = get_leg_price(ib, leg_contract)

    if mid <= 0 and eodhd_mid > 0:
        log.info("        No live data, using EODHD mid=$%.2f", eodhd_mid)
        mid = eodhd_mid
        bid = ask = mid

    # No price at all → MKT order
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

    spread = round(ask - bid, 4) if ask > bid > 0 else 0.0

    # Market Order fallback/primary
    log.info(f"      {action} {leg_contract.right} {leg_contract.lastTradeDateOrContractMonth} K={leg_contract.strike} x{contracts} MARKET order")

    order = MarketOrder(action, contracts)
    order.outsideRth = False
    order.tif = "DAY"

    trade = ib.placeOrder(leg_contract, order)
    result = _wait_for_fill(ib, trade, 30)

    if result == "Filled":
        fill_px = trade.orderStatus.avgFillPrice
        log.info("        -> FILLED @ $%.2f", fill_px)
        return True, fill_px
    if result in ("Cancelled", "ApiCancelled", "Inactive"):
        log.error("        -> REJECTED (%s)", result)
        return False, 0.0

    # Timeout
    ib.cancelOrder(order)
    ib.sleep(2)
    log.info("        -> NOT FILLED (%ds)", LEG_TIMEOUT)
    return False, 0.0


# ═══════════════════════════════════════════════════════════════
#  SPREAD EXECUTION — Combo-first, legs-fallback orchestrator
# ═══════════════════════════════════════════════════════════════

def _build_combo(legs: list[tuple[Option, str]]) -> Bag | None:
    """Build a BAG contract from qualified individual legs."""
    if not legs:
        return None

    combo = Bag()
    combo.symbol = legs[0][0].symbol
    combo.exchange = "SMART"
    combo.currency = "USD"

    combo_legs = []
    for option_contract, action in legs:
        if option_contract.conId == 0:
            return None
        cl = ComboLeg()
        cl.conId = option_contract.conId
        cl.ratio = 1
        cl.action = action
        cl.exchange = "SMART"
        combo_legs.append(cl)

    combo.comboLegs = combo_legs
    return combo


def execute_spread(ib: IB, ticker: str,
                   legs: list[tuple[Option, str]], n_legs: int,
                   contracts: int, eodhd_cps: float,
                   spread_type: str) -> tuple[str, float, float, dict]:
    """Execute a calendar spread with optimal routing.

    Step A: Combo order (BAG) — IBKR matching engine optimizes cross-leg fills.
    Step B: Individual legs (BUY back-month first) — fallback if combo fails.

    Returns (result, fill_cost, slippage, details).
        result: "full" | "partial" | "failed"
    """
    details = {
        "method": None,
        "combo_attempted": False,
        "combo_result": None,
        "leg_fills": [],
    }

    # ── Step A: Combo ──
    log.info("    %s: Step A — Combo (%d legs as BAG)", ticker, n_legs)
    combo = _build_combo(legs)

    if combo is not None:
        details["combo_attempted"] = True
        filled, fill_px, slippage = execute_combo(
            ib, combo, "BUY", contracts, eodhd_cps, legs=legs
        )
        if filled:
            details["method"] = "combo"
            details["combo_result"] = "filled"
            return "full", fill_px, slippage, details
        else:
            details["combo_result"] = "no_fill"
            log.info("    %s: Combo failed → individual legs", ticker)
    else:
        log.info("    %s: Cannot build combo → individual legs", ticker)

    # ── Step B: Individual legs (BUY back-month first) ──
    log.info("    %s: Step B — Individual legs (%d)", ticker, n_legs)
    details["method"] = "legs"

    est_leg_mid = abs(eodhd_cps) / max(n_legs // 2, 1) if eodhd_cps > 0 else 0

    filled_legs = []
    total_fill_cost = 0.0

    for leg_contract, action in legs:
        filled, fill_px = execute_leg(
            ib, leg_contract, action, contracts, eodhd_mid=est_leg_mid
        )
        if filled:
            if action == "BUY":
                total_fill_cost += fill_px
            else:
                total_fill_cost -= fill_px
            filled_legs.append({
                "action": action,
                "right": leg_contract.right,
                "exp": leg_contract.lastTradeDateOrContractMonth,
                "strike": float(leg_contract.strike),
                "fill_price": fill_px,
            })
            details["leg_fills"].append(filled_legs[-1])
        else:
            log.error("    %s: Leg failed, stopping (no naked short risk)", ticker)
            break

    if len(filled_legs) == n_legs:
        slippage = total_fill_cost - eodhd_cps
        log.info("    %s: ALL %d LEGS FILLED, net=$%.2f/sh (slip=%+.2f vs EODHD $%.2f)",
                 ticker, n_legs, total_fill_cost, slippage, eodhd_cps)
        return "full", total_fill_cost, slippage, details
    elif filled_legs:
        log.warning("    %s: PARTIAL %d/%d legs. Manual cleanup needed.",
                    ticker, len(filled_legs), n_legs)
        return "partial", total_fill_cost, 0.0, details
    else:
        return "failed", 0.0, 0.0, details


def execute_spread_close(ib: IB, ticker: str,
                         legs: list[tuple[Option, str]], n_legs: int,
                         contracts: int) -> tuple[bool, float, str, list]:
    """Close a spread position (reverse entry actions).

    Step A: Combo SELL.
    Step B: Individual legs fallback.

    Returns (success, exit_price, method, filled_legs).
    """
    # Reverse actions for close (BUY→SELL, SELL→BUY)
    # Close SELL (short front) first, then close BUY (long back)
    close_legs = []
    for leg_contract, entry_action in reversed(legs):
        close_action = "SELL" if entry_action == "BUY" else "BUY"
        close_legs.append((leg_contract, close_action))

    total_exit_price = 0.0
    method = "legs"

    # ── Step A: Combo close ──
    log.info("    %s: Close Step A — Combo (%d legs, %ds)", ticker, n_legs, COMBO_TIMEOUT)
    combo = _build_combo(close_legs)
    if combo is not None:
        filled, fill_px, _ = execute_combo(
            ib, combo, "SELL", contracts, eodhd_mid=0, legs=close_legs
        )
        if filled:
            return True, fill_px, "combo", []

    # ── Step B: Individual legs ──
    log.info("    %s: Close Step B — Individual legs (%d)", ticker, n_legs)
    filled_legs = []
    for leg_contract, action in close_legs:
        filled, fill_px = execute_leg(ib, leg_contract, action, contracts)
        if filled:
            if action == "SELL":
                total_exit_price += fill_px
            else:
                total_exit_price -= fill_px
            filled_legs.append({
                "action": action,
                "right": leg_contract.right,
                "exp": leg_contract.lastTradeDateOrContractMonth,
                "strike": float(leg_contract.strike),
                "fill_price": fill_px,
            })
        else:
            log.error("    %s: Close leg failed — partial close!", ticker)
            break

    success = len(filled_legs) == n_legs
    return success, total_exit_price, method, filled_legs
