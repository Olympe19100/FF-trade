"""
IBKR Paper Trading — Forward Factor Calendar Spreads

Connects to Interactive Brokers TWS/Gateway via ib_insync,
reads scanner signals, and places calendar spread orders.

Usage:
    python trader.py                    # Show portfolio status
    python trader.py --enter            # Place new orders from latest signals
    python trader.py --close            # Close expiring positions (J-1)
    python trader.py --status           # Detailed P&L per position
    python trader.py --port 4002        # Use IB Gateway instead of TWS

Prerequisites:
    - TWS or IB Gateway running in PAPER TRADING mode
    - API enabled: Edit -> Global Config -> API -> Enable ActiveX and Socket Clients
    - pip install ib_insync
"""

import json
import time
import sys
import asyncio
import argparse
import numpy as np
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta

# Fix Python 3.14 asyncio event loop before importing ib_insync
try:
    asyncio.get_event_loop()
except RuntimeError:
    asyncio.set_event_loop(asyncio.new_event_loop())

from ib_insync import IB, Stock, Option, Bag, ComboLeg, LimitOrder, MarketOrder, util

from core.config import (
    OUTPUT, STATE, PORTFOLIO_FILE, TRADES_FILE, BACKTEST_TRADES_FILE,
    DEFAULT_HOST, TWS_PAPER, GW_PAPER, CLIENT_ID,
    MAX_POSITIONS, MAX_CONTRACTS, DEFAULT_ALLOC,
    KELLY_FRAC, MIN_KELLY_TRADES, CONTRACT_MULT,
    COMMISSION_LEG, SLIPPAGE_BUFFER, CLOSE_DAYS,
    FILL_TIMEOUT, LMT_WALK_STEP, LMT_WALK_MAX, LMT_WALK_WAIT,
    COMBO_TIMEOUT, COMBO_WALK_STEP, COMBO_WALK_WAIT,
    LEG_WALK_STEP, LEG_WALK_WAIT, LEG_MAX_WALK, LEG_TIMEOUT,
    OPTIMAL_START_ET, OPTIMAL_END_ET, ENFORCE_WINDOW,
    get_logger,
)
from core.portfolio import (
    load_latest_signals, load_portfolio, save_portfolio,
    add_position, record_trade,
    load_trade_history, compute_kelly, cost_per_contract, size_portfolio,
)

log = get_logger(__name__)


# ═══════════════════════════════════════════════════════════════
#  CONNECTION
# ═══════════════════════════════════════════════════════════════

def connect_ibkr(host: str = DEFAULT_HOST, port: int = TWS_PAPER, client_id: int = CLIENT_ID) -> IB:
    """Connect to IBKR TWS/Gateway. Returns IB instance."""
    ib = IB()
    log.info("Connecting to IBKR at %s:%d ...", host, port)
    ib.connect(host, port, clientId=client_id, timeout=10, readonly=False)
    log.info("  Connected: %s", ib.isConnected())
    return ib


def verify_paper(ib: IB) -> str:
    """Verify we're on a paper trading account. Returns account ID."""
    accounts = ib.managedAccounts()
    if not accounts:
        raise RuntimeError("No accounts found")
    acct = accounts[0]
    # Paper accounts typically start with 'D' or contain 'PAPER'
    is_paper = acct.startswith("D") or "PAPER" in acct.upper()
    if not is_paper:
        log.warning("  Account %s may be LIVE (not paper).", acct)
        log.warning("  Paper accounts usually start with 'D'.")
        resp = input("  Continue anyway? (yes/no): ").strip().lower()
        if resp != "yes":
            raise RuntimeError("Aborted: not a paper account")
    else:
        log.info("  Paper account: %s", acct)
    return acct


def get_account_info(ib: IB, acct: str = "") -> dict:
    """Get key account metrics (handles EUR and USD accounts)."""
    ib.sleep(2)  # Wait for account data
    summary = ib.accountSummary(acct)
    info = {}

    # Detect base currency from NetLiquidation
    for s in summary:
        if s.tag == "NetLiquidation":
            info["base_currency"] = s.currency
            break

    for s in summary:
        if s.tag in ("NetLiquidation", "BuyingPower", "AvailableFunds",
                       "TotalCashValue", "GrossPositionValue"):
            try:
                info[s.tag] = float(s.value)
            except (ValueError, TypeError):
                pass
    return info


# ═══════════════════════════════════════════════════════════════
#  CONTRACT BUILDER
# ═══════════════════════════════════════════════════════════════

def fmt_exp(date_str: str) -> str:
    """Convert 'YYYY-MM-DD' to 'YYYYMMDD' for IBKR."""
    return str(date_str).replace("-", "")[:8]


def get_ibkr_option_params(ib: IB, ticker: str) -> tuple[set | None, set | None]:
    """Query IBKR for valid option expirations and strikes.

    Returns (expirations_set, strikes_set) or (None, None) on failure.
    """
    stock = Stock(ticker, "SMART", "USD")
    try:
        ib.qualifyContracts(stock)
    except Exception as ex:
        log.error("    Cannot qualify stock %s: %s", ticker, ex)
        return None, None

    if stock.conId == 0:
        log.error("    Stock %s not found on IBKR", ticker)
        return None, None

    params_list = ib.reqSecDefOptParams(
        stock.symbol, "", stock.secType, stock.conId
    )
    ib.sleep(1)

    if not params_list:
        log.error("    No option params returned for %s", ticker)
        return None, None

    # Merge all exchanges — take the one with most strikes (usually SMART)
    best = max(params_list, key=lambda p: len(p.strikes))
    return set(best.expirations), set(best.strikes)


def snap_to_valid(value: float, valid_set: set, max_diff: float | None = None) -> float | None:
    """Find the closest valid value. Returns None if outside max_diff."""
    if not valid_set:
        return None
    closest = min(valid_set, key=lambda v: abs(float(v) - float(value)))
    if max_diff is not None and abs(float(closest) - float(value)) > max_diff:
        return None
    return closest


def create_calendar_legs(ib: IB, ticker: str, strike: float, front_exp: str, back_exp: str,
                         double: bool = True, put_strike: float | None = None) -> tuple[list, int, float | None, float | None]:
    """Build individual option contracts for a calendar spread.

    Returns list of (Option, action) tuples ordered BUY-first (no naked short risk),
    plus (n_legs, actual_call_strike, actual_put_strike) or ([], 0, None, None) on failure.

    Call legs use `strike`, put legs use `put_strike` (35-delta OTM, different from call).
    Execution order: BUY back-month first, then SELL front-month.
    """
    strike = float(strike)
    front_str = fmt_exp(front_exp)
    back_str = fmt_exp(back_exp)

    # Step 1: Get valid expirations and strikes from IBKR
    valid_exps, valid_strikes = get_ibkr_option_params(ib, ticker)
    if valid_exps is None:
        return [], 0, None, None

    # Step 2: Snap expiration dates
    ibkr_front = snap_to_valid(front_str, valid_exps)
    ibkr_back = snap_to_valid(back_str, valid_exps)

    if ibkr_front is None or ibkr_back is None:
        log.warning("    No matching IBKR expirations for %s: want %s/%s",
                    ticker, front_str, back_str)
        return [], 0, None, None

    if ibkr_front == ibkr_back:
        log.warning("    Front and back snap to same expiration for %s", ticker)
        return [], 0, None, None

    if ibkr_front != front_str or ibkr_back != back_str:
        log.info("    Snapped exps: %s->%s, %s->%s",
                 front_str, ibkr_front, back_str, ibkr_back)

    # Step 3: Snap call strike
    max_strike_diff = strike * 0.03
    ibkr_call_strike = snap_to_valid(strike, valid_strikes, max_diff=max_strike_diff)
    if ibkr_call_strike is None:
        log.warning("    No valid IBKR call strike near %.0f for %s", strike, ticker)
        return [], 0, None, None

    if ibkr_call_strike != strike:
        log.info("    Snapped call strike: %.1f -> %s", strike, ibkr_call_strike)

    # Step 4: Create and qualify call options
    front_call = Option(ticker, ibkr_front, ibkr_call_strike, "C", "SMART", "100", "USD")
    back_call  = Option(ticker, ibkr_back,  ibkr_call_strike, "C", "SMART", "100", "USD")

    try:
        ib.qualifyContracts(front_call, back_call)
    except Exception as ex:
        log.error("    Failed to qualify call options for %s: %s", ticker, ex)
        return [], 0, None, None

    if front_call.conId == 0 or back_call.conId == 0:
        log.error("    Could not resolve call contracts for %s K=%s %s/%s",
                  ticker, ibkr_call_strike, ibkr_front, ibkr_back)
        return [], 0, None, None

    # BUY first, then SELL (never naked short)
    legs = [
        (back_call,  "BUY"),
        (front_call, "SELL"),
    ]

    actual_put_strike = None

    # Put legs for double calendar (separate put strike)
    if double:
        # Use put_strike if provided, otherwise fall back to call strike
        ps = float(put_strike) if put_strike is not None else strike
        ibkr_put_strike = snap_to_valid(ps, valid_strikes, max_diff=ps * 0.03)
        if ibkr_put_strike is None:
            log.warning("    No valid IBKR put strike near %.0f for %s, using single calendar",
                        ps, ticker)
        else:
            if ibkr_put_strike != ps:
                log.info("    Snapped put strike: %.1f -> %s", ps, ibkr_put_strike)

            front_put = Option(ticker, ibkr_front, ibkr_put_strike, "P", "SMART", "100", "USD")
            back_put  = Option(ticker, ibkr_back,  ibkr_put_strike, "P", "SMART", "100", "USD")
            try:
                ib.qualifyContracts(front_put, back_put)
                if front_put.conId > 0 and back_put.conId > 0:
                    legs.extend([
                        (back_put,  "BUY"),
                        (front_put, "SELL"),
                    ])
                    actual_put_strike = ibkr_put_strike
                else:
                    log.warning("    Put contracts not found for %s, using single calendar", ticker)
            except Exception:
                log.warning("    Put qualification failed for %s, using single calendar", ticker)

    return legs, len(legs), ibkr_call_strike, actual_put_strike


# ═══════════════════════════════════════════════════════════════
#  OPTIMAL EXECUTION (Muravyev & Pearson 2020, Cont & Kukanov 2013)
# ═══════════════════════════════════════════════════════════════

def check_optimal_window() -> tuple[bool, str]:
    """Check if current time is within optimal trading window.

    Returns (is_optimal: bool, message: str).

    Window: 10:00-15:00 ET (Muravyev & Pearson 2020, Cont & Kukanov 2013).
    - Avoid 9:30-10:00: opening auction, wide spreads, stale quotes
    - Avoid 15:00-16:00: end-of-day widening, gamma risk near close
    - Sweet spot 10:00-15:00: tightest bid-ask, highest fill rates
    """
    try:
        from zoneinfo import ZoneInfo
    except ImportError:
        from backports.zoneinfo import ZoneInfo
    now_et = datetime.now(ZoneInfo("America/New_York"))
    current_time = now_et.strftime("%H:%M")

    is_optimal = OPTIMAL_START_ET <= current_time <= OPTIMAL_END_ET
    if is_optimal:
        msg = (f"Within optimal window "
               f"({OPTIMAL_START_ET}-{OPTIMAL_END_ET} ET): {current_time} ET")
    else:
        msg = (f"BLOCKED: Outside optimal window "
               f"({OPTIMAL_START_ET}-{OPTIMAL_END_ET} ET). "
               f"Current: {current_time} ET — no orders sent.")
    return is_optimal, msg


def create_calendar_combo(legs: list[tuple[Option, str]]) -> Bag | None:
    """Build a BAG (combo) contract from qualified individual legs.

    Args:
        legs: list of (Option, action) from create_calendar_legs()

    Returns:
        Bag contract or None on failure.
    """
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


def get_combo_mid_price(ib: IB, combo: Bag) -> tuple[float, float, float]:
    """Request market data for a combo and return (bid, ask, mid).

    Returns (0, 0, 0) if data unavailable (common on paper accounts).
    """
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


def get_leg_mid_price(ib: IB, option_contract: Option) -> tuple[float, float, float]:
    """Get current bid/ask/mid for a single option contract.

    Returns (bid, ask, mid). Returns (0, 0, 0) if data unavailable.
    """
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


def execute_combo_order(ib: IB, combo: Bag, contracts: int, eodhd_mid: float) -> tuple[bool, float, float]:
    """Try to fill a combo order with price walking.

    Strategy (ORATS methodology, 56% spread slippage):
      1. Start LMT at mid
      2. Walk toward ask by COMBO_WALK_STEP every COMBO_WALK_WAIT seconds
      3. Give up after COMBO_TIMEOUT

    Returns (filled: bool, fill_price: float, slippage: float).
    """
    bid, ask, mid = get_combo_mid_price(ib, combo)

    if mid <= 0:
        log.warning("      Combo: no market data (bid=%s, ask=%s), skipping", bid, ask)
        return False, 0.0, 0.0

    # Calendar spread is a debit: we BUY the combo
    limit_price = round(mid, 2)
    max_price = round(ask, 2) if ask > 0 else round(mid * 1.15, 2)

    log.info("      Combo: bid=%.2f ask=%.2f mid=%.2f, LMT start @ %.2f",
             bid, ask, mid, limit_price)

    n_walks = int(COMBO_TIMEOUT / COMBO_WALK_WAIT)

    for walk in range(n_walks + 1):
        order = LimitOrder("BUY", contracts, limit_price)
        order.outsideRth = False
        order.tif = "DAY"

        trade = ib.placeOrder(combo, order)

        for _ in range(COMBO_WALK_WAIT):
            ib.sleep(1)
            status = trade.orderStatus.status
            if status == "Filled":
                fill_px = trade.orderStatus.avgFillPrice
                slippage = fill_px - eodhd_mid
                log.info("      Combo FILLED @ $%.2f (slip=%+.2f vs EODHD $%.2f)",
                         fill_px, slippage, eodhd_mid)
                return True, fill_px, slippage
            elif status in ("Cancelled", "ApiCancelled", "Inactive"):
                log.error("      Combo rejected: %s", status)
                return False, 0.0, 0.0

        # Not filled — cancel and walk price
        ib.cancelOrder(order)
        ib.sleep(2)

        new_price = round(limit_price + COMBO_WALK_STEP, 2)
        if new_price > max_price:
            log.info("      Combo: hit ceiling $%.2f, giving up", max_price)
            break

        limit_price = new_price
        log.info("      Combo: walk -> $%.2f (step %d/%d)",
                 limit_price, walk + 1, n_walks)

    log.info("      Combo: not filled after %ds", COMBO_TIMEOUT)
    return False, 0.0, 0.0


def execute_leg_order(ib: IB, leg_contract: Option, action: str, contracts: int, eodhd_mid: float = 0) -> tuple[bool, float]:
    """Execute a single leg with adaptive LMT price walking.

    Strategy (Cont & Kukanov 2013, SteadyOptions):
      1. Start LMT at IBKR mid (or EODHD mid if no live data)
      2. Walk by LEG_WALK_STEP every LEG_WALK_WAIT seconds
      3. BUY: walk toward ask. SELL: walk toward bid.
      4. Max deviation: LEG_MAX_WALK (15%) from mid

    Returns (filled: bool, fill_price: float).
    """
    bid, ask, mid = get_leg_mid_price(ib, leg_contract)

    if mid <= 0 and eodhd_mid > 0:
        log.info("        No live data, using EODHD mid=$%.2f", eodhd_mid)
        mid = eodhd_mid
        bid = eodhd_mid * 0.90
        ask = eodhd_mid * 1.10

    if mid <= 0:
        log.warning("        No price data at all, using MKT")
        # Fallback to MKT as last resort
        order = MarketOrder(action, contracts)
        order.outsideRth = False
        order.tif = "DAY"
        trade = ib.placeOrder(leg_contract, order)
        for _ in range(FILL_TIMEOUT):
            ib.sleep(1)
            if trade.orderStatus.status == "Filled":
                return True, trade.orderStatus.avgFillPrice
            elif trade.orderStatus.status in ("Cancelled", "ApiCancelled", "Inactive"):
                return False, 0.0
        ib.cancelOrder(order)
        ib.sleep(2)
        return False, 0.0

    if action == "BUY":
        limit_price = round(mid, 2)
        max_limit = round(mid * (1 + LEG_MAX_WALK), 2)
        walk_dir = LEG_WALK_STEP
    else:
        limit_price = round(mid, 2)
        max_limit = round(mid * (1 - LEG_MAX_WALK), 2)
        walk_dir = -LEG_WALK_STEP

    right_label = leg_contract.right
    exp_label = leg_contract.lastTradeDateOrContractMonth
    log.info("      %s %s %s K=%.0f x%d LMT $%.2f (bid=%.2f ask=%.2f)",
             action, right_label, exp_label,
             leg_contract.strike, contracts, limit_price, bid, ask)

    elapsed = 0
    order = None

    while elapsed < LEG_TIMEOUT:
        order = LimitOrder(action, contracts, limit_price)
        order.outsideRth = False
        order.tif = "DAY"

        trade = ib.placeOrder(leg_contract, order)

        wait_time = min(LEG_WALK_WAIT, LEG_TIMEOUT - elapsed)
        for _ in range(wait_time):
            ib.sleep(1)
            elapsed += 1
            status = trade.orderStatus.status
            if status == "Filled":
                fill_px = trade.orderStatus.avgFillPrice
                log.info("        -> FILLED @ $%.2f", fill_px)
                return True, fill_px
            elif status in ("Cancelled", "ApiCancelled", "Inactive"):
                log.error("        -> REJECTED (%s)", status)
                return False, 0.0

        if elapsed >= LEG_TIMEOUT:
            break

        # Cancel and walk
        ib.cancelOrder(order)
        ib.sleep(1)
        elapsed += 1

        new_price = round(limit_price + walk_dir, 2)
        if action == "BUY" and new_price > max_limit:
            log.info("        -> hit ceiling $%.2f", max_limit)
            break
        elif action == "SELL" and new_price < max_limit:
            log.info("        -> hit floor $%.2f", max_limit)
            break

        limit_price = new_price
        log.info("        walk -> $%.2f", limit_price)

    # Final cancel
    if order:
        try:
            ib.cancelOrder(order)
            ib.sleep(1)
        except Exception:
            pass

    log.info("        -> NOT FILLED (%ds)", elapsed)
    return False, 0.0


def execute_spread_optimal(ib: IB, ticker: str, legs: list[tuple[Option, str]], n_legs: int, contracts: int,
                           eodhd_cps: float, spread_type: str) -> tuple[str, float, float, dict]:
    """Execute a calendar spread using optimal execution strategy.

    Step A: Try combo order (LMT at mid, walk toward natural) — 5 min
    Step B: If combo fails, fallback to individual legs (BUY first) — 2 min/leg

    Returns (result, fill_cost, slippage, details).
        result: "full" | "partial" | "failed"
    """
    details = {
        "method": None,
        "combo_attempted": False,
        "combo_result": None,
        "leg_fills": [],
    }

    # ── Step A: Try combo order ──
    log.info("    %s: Step A -- Combo (%d legs as BAG)", ticker, n_legs)
    combo = create_calendar_combo(legs)

    if combo is not None:
        details["combo_attempted"] = True
        filled, fill_px, slippage = execute_combo_order(
            ib, combo, contracts, eodhd_cps
        )
        if filled:
            details["method"] = "combo"
            details["combo_result"] = "filled"
            return "full", fill_px, slippage, details
        else:
            details["combo_result"] = "no_fill"
            log.info("    %s: Combo failed -> fallback to individual legs", ticker)
    else:
        log.info("    %s: Cannot build combo -> individual legs", ticker)

    # ── Step B: Individual legs (BUY back-month first) ──
    log.info("    %s: Step B -- Individual legs (LMT + walk)", ticker)
    details["method"] = "legs"

    filled_legs = []
    total_fill_cost = 0.0

    for leg_contract, action in legs:
        filled, fill_px = execute_leg_order(
            ib, leg_contract, action, contracts
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


# ═══════════════════════════════════════════════════════════════
#  ENTER NEW POSITIONS (--enter)
# ═══════════════════════════════════════════════════════════════

def enter_new_positions(ib: IB, acct: str, max_new: int | None = None) -> dict | None:
    """Place new calendar spread orders from scanner signals.

    Two-pass approach:
      Pass 1: Qualify contracts on IBKR (identify valid positions)
      Pass 2: Size all positions globally via Kelly to hit target allocation
      Pass 3: Place orders
    """
    log.info("=" * 60)
    log.info("ENTERING NEW POSITIONS")
    log.info("=" * 60)

    # Load signals
    signals = load_latest_signals()
    if signals.empty:
        return

    # Current portfolio
    portfolio = load_portfolio()
    active = [p for p in portfolio["positions"] if "exit_date" not in p]
    n_active = len(active)
    active_tickers = {p["ticker"] for p in active}

    slots = MAX_POSITIONS - n_active
    if max_new is not None:
        slots = min(slots, max_new)

    if slots <= 0:
        log.info("  Portfolio full: %d/%d positions", n_active, MAX_POSITIONS)
        return

    # Account info
    info = get_account_info(ib, acct)
    account_value = info.get("NetLiquidation", 0)
    buying_power = info.get("BuyingPower", 0)

    # Walk-forward Kelly (bootstrap backtest + live trades)
    returns = load_trade_history()
    kelly_f = compute_kelly(returns)
    kelly_target = kelly_f * account_value

    log.info("  Active:        %d/%d", n_active, MAX_POSITIONS)
    log.info("  Slots:         %d", slots)
    log.info("  Signals:       %d", len(signals))
    log.info("  Account:       $%,.0f", account_value)
    log.info("  Buying power:  $%,.0f", buying_power)
    log.info("  Kelly history: %d trades", len(returns))
    log.info("  Half Kelly f:  %.4f (%.1f%%)", kelly_f, kelly_f * 100)
    log.info("  Kelly target:  $%,.0f (f * W)", kelly_target)

    # ── PASS 1: Qualify all contracts (individual legs) ──
    log.info("  --- PASS 1: Qualifying contracts on IBKR ---")
    qualified = []  # (sig, legs, n_legs, actual_strike, actual_put_strike, cost_per_share, spread_type)
    skipped = 0

    for _, sig in signals.iterrows():
        if len(qualified) >= slots:
            break

        ticker = sig["ticker"]
        if ticker in active_tickers:
            continue

        has_double = pd.notna(sig.get("dbl_cost")) and sig["dbl_cost"] > 0
        spread_type = "double" if has_double else "single"
        cps = sig["dbl_cost"] if has_double else sig["call_cost"]

        # Read put_strike from signal (separate from call strike for 35-delta)
        sig_put_strike = sig.get("put_strike")
        if pd.isna(sig_put_strike):
            sig_put_strike = None

        ps_str = " PutK=%.0f" % sig_put_strike if sig_put_strike else ""
        log.info("    %s %s CallK=%.0f%s FF=%.1f%% $%.2f (%s) ...",
                 ticker, sig['combo'], sig['strike'], ps_str,
                 sig['ff'], cps, spread_type)

        legs, n_legs, actual_strike, actual_put_strike = create_calendar_legs(
            ib, ticker, sig["strike"],
            sig["front_exp"], sig["back_exp"],
            double=has_double,
            put_strike=sig_put_strike
        )
        if not legs:
            log.info("    -> SKIP")
            skipped += 1
            continue

        log.info("    -> OK")
        qualified.append((sig, legs, n_legs, actual_strike, actual_put_strike, cps, spread_type))

    log.info("  Qualified: %d/%d (%d skipped)",
             len(qualified), len(qualified) + skipped, skipped)

    if not qualified:
        return

    # ── PASS 2: Global Kelly sizing ──
    log.info("  --- PASS 2: Kelly sizing (%d positions) ---", len(qualified))
    # qualified tuple: (sig, legs, n_legs, actual_strike, actual_put_strike, cps, spread_type)
    signals_info = [(q[0]["ticker"], q[5], q[2]) for q in qualified]
    sizing = size_portfolio(signals_info, kelly_f, account_value)

    total_deployed = sum(d for _, _, d in sizing)
    log.info("  Kelly target:  $%,.0f", kelly_target)
    log.info("  Total sized:   $%,.0f", total_deployed)
    gap = kelly_target - total_deployed
    gap_pct = (total_deployed / kelly_target - 1) * 100 if kelly_target else 0
    log.info("  Gap:           $%+,.0f (%+.1f%%)", gap, gap_pct)

    log.info("  %6s %6s %6s %4s %9s", "Ticker", "FF%", "Cost", "Ctr", "Deployed")
    log.info("  %s", "-" * 38)
    for (sig, _legs, _nl, _as, _aps, cps_q, _st), (ticker, n_ctr, deployed) in zip(qualified, sizing):
        log.info("  %6s %5.1f%% $%5.2f %4d $%8,.0f",
                 ticker, sig['ff'], cps_q, n_ctr, deployed)

    # ── Time-of-day check ──
    is_optimal, time_msg = check_optimal_window()
    log.info("  %s", time_msg)
    if ENFORCE_WINDOW and not is_optimal:
        log.warning("  Orders blocked -- retry between %s-%s ET.",
                    OPTIMAL_START_ET, OPTIMAL_END_ET)
        return

    # ── PASS 3: Optimal execution (combo-first, legs-fallback) ──
    log.info("  --- PASS 3: Optimal execution (%d spreads) ---", len(qualified))
    log.info("  Strategy: combo LMT first (%ds) -> individual legs LMT+walk (%ds/leg)",
             COMBO_TIMEOUT, LEG_TIMEOUT)
    entered = 0
    partial = 0
    not_filled = 0
    slippage_log = []

    for (sig, legs, n_legs, actual_strike, actual_put_strike, cps, spread_type), \
        (ticker, contracts, deployed) in zip(qualified, sizing):
        if contracts <= 0:
            continue

        if deployed > buying_power * 0.9:
            log.warning("    %s: SKIP ($%,.0f > buying power)", ticker, deployed)
            continue

        log.info("    %s: %dx %s (%d legs, EODHD mid=$%.2f)",
                 ticker, contracts, spread_type, n_legs, cps)

        result, fill_cost, slippage, details = execute_spread_optimal(
            ib, ticker, legs, n_legs, contracts, cps, spread_type
        )

        slippage_log.append({
            "ticker": ticker,
            "eodhd_mid": cps,
            "fill_cost": fill_cost,
            "slippage": slippage,
            "method": details.get("method"),
            "result": result,
        })

        if result == "full":
            pos = add_position(
                portfolio, ticker, sig["combo"], actual_strike,
                sig["front_exp"], sig["back_exp"],
                contracts, fill_cost, spread_type, sig["ff"], n_legs,
                put_strike=actual_put_strike
            )
            pos["execution_method"] = details.get("method")
            pos["slippage"] = round(slippage, 4)
            active_tickers.add(ticker)
            entered += 1
            buying_power -= deployed
        elif result == "partial":
            partial += 1
        else:
            not_filled += 1

    save_portfolio(portfolio)

    # ── Slippage Report ──
    if slippage_log:
        log.info("  %s", "=" * 55)
        log.info("  SLIPPAGE REPORT")
        log.info("  %6s %6s %7s %7s %7s %8s",
                 "Ticker", "Method", "EODHD", "Fill", "Slip", "Result")
        log.info("  %s", "-" * 55)
        for s in slippage_log:
            log.info("  %6s %6s $%6.2f $%6.2f $%+6.2f %8s",
                     s['ticker'], s['method'] or '-',
                     s['eodhd_mid'], s['fill_cost'],
                     s['slippage'], s['result'])

        filled_slips = [s["slippage"] for s in slippage_log
                        if s["result"] == "full"]
        if filled_slips:
            avg_slip = np.mean(filled_slips)
            log.info("  Avg slippage: $%+.3f/sh (%d fills)",
                     avg_slip, len(filled_slips))

    log.info("  %s", "=" * 40)
    log.info("  SUMMARY")
    log.info("  Kelly f:       %.4f (%.1f%%)", kelly_f, kelly_f * 100)
    log.info("  Kelly target:  $%,.0f", kelly_target)
    log.info("  Total sized:   $%,.0f", total_deployed)
    log.info("  Full fills:    %d/%d", entered, len(qualified))
    log.info("  Partial fills: %d", partial)
    log.info("  Not filled:    %d", not_filled)
    log.info("  Skipped IBKR:  %d", skipped)
    log.info("  Total active:  %d/%d", n_active + entered, MAX_POSITIONS)

    return {
        "entered": entered,
        "partial": partial,
        "not_filled": not_filled,
        "skipped": skipped,
        "slippage_log": slippage_log,
        "is_optimal_window": is_optimal,
    }


# ═══════════════════════════════════════════════════════════════
#  CLOSE EXPIRING POSITIONS (--close)
# ═══════════════════════════════════════════════════════════════

def close_expiring_positions(ib: IB, acct: str) -> None:
    """Close positions where front expiry is within CLOSE_DAYS."""
    log.info("=" * 60)
    log.info("CLOSING EXPIRING POSITIONS")
    log.info("=" * 60)

    portfolio = load_portfolio()
    active = [p for p in portfolio["positions"] if "exit_date" not in p]
    today = datetime.now()

    to_close = []
    for pos in active:
        front_exp = pd.Timestamp(pos["front_exp"])
        days_left = (front_exp - pd.Timestamp(today)).days
        if days_left <= CLOSE_DAYS:
            to_close.append(pos)

    if not to_close:
        log.info("  No positions to close (next expiry not within %d days)", CLOSE_DAYS)
        for pos in active:
            front_exp = pd.Timestamp(pos["front_exp"])
            days_left = (front_exp - pd.Timestamp(today)).days
            log.info("    %s %s: front exp %s (%dd)",
                     pos['ticker'], pos['combo'], pos['front_exp'], days_left)
        return

    log.info("  Positions to close: %d", len(to_close))

    # ── Time-of-day check for closing ──
    is_optimal, time_msg = check_optimal_window()
    log.info("  %s", time_msg)
    if ENFORCE_WINDOW and not is_optimal:
        log.warning("  Close orders blocked -- retry between %s-%s ET.",
                    OPTIMAL_START_ET, OPTIMAL_END_ET)
        return

    for pos in to_close:
        ticker = pos["ticker"]
        contracts = pos["contracts"]
        pos_put_strike = pos.get("put_strike", pos["strike"])
        log.info("  Closing %s %s CallK=%.0f PutK=%.0f x%d (%s)",
                 ticker, pos['combo'], pos['strike'],
                 pos_put_strike, contracts, pos['spread_type'])

        # Rebuild individual leg contracts (separate call/put strikes)
        is_double = pos["spread_type"] == "double"
        legs, n_legs, _, _ = create_calendar_legs(
            ib, ticker, pos["strike"],
            pos["front_exp"], pos["back_exp"],
            double=is_double,
            put_strike=pos_put_strike
        )
        if not legs:
            log.error("    Could not rebuild contract, manual close needed")
            continue

        # Close = reverse the entry actions (BUY->SELL, SELL->BUY)
        # Close SELL (short front) first, then close BUY (long back)
        close_legs = []
        for leg_contract, entry_action in reversed(legs):
            close_action = "SELL" if entry_action == "BUY" else "BUY"
            close_legs.append((leg_contract, close_action))

        # Try combo close first, then fallback to individual legs
        log.info("    Closing %d legs (combo-first, legs-fallback):", n_legs)
        total_exit_price = 0.0
        close_success = False

        # Step A: Try combo close
        combo = create_calendar_combo(close_legs)
        if combo is not None:
            bid, ask, mid = get_combo_mid_price(ib, combo)
            if mid > 0:
                log.info("      Combo close: bid=%.2f ask=%.2f mid=%.2f", bid, ask, mid)
                limit_price = round(mid, 2)
                order = LimitOrder("SELL", contracts, limit_price)
                order.outsideRth = False
                order.tif = "DAY"
                trade = ib.placeOrder(combo, order)
                for _ in range(COMBO_TIMEOUT // 2):  # 2.5 min for close
                    ib.sleep(1)
                    if trade.orderStatus.status == "Filled":
                        total_exit_price = trade.orderStatus.avgFillPrice
                        close_success = True
                        log.info("      Combo close FILLED @ $%.2f", total_exit_price)
                        break
                    elif trade.orderStatus.status in ("Cancelled", "ApiCancelled", "Inactive"):
                        break
                if not close_success:
                    ib.cancelOrder(order)
                    ib.sleep(2)
                    log.info("      Combo close failed, fallback to legs")

        # Step B: Individual legs fallback
        if not close_success:
            filled_legs = []
            for leg_contract, action in close_legs:
                filled, fill_px = execute_leg_order(
                    ib, leg_contract, action, contracts
                )
                if filled:
                    if action == "SELL":
                        total_exit_price += fill_px
                    else:
                        total_exit_price -= fill_px
                    filled_legs.append((leg_contract, action, fill_px))
                else:
                    break

            close_success = len(filled_legs) == n_legs

        if close_success:
            exit_price = total_exit_price
            commission = n_legs * COMMISSION_LEG * contracts
            pnl = ((exit_price - pos["cost_per_share"]) *
                   CONTRACT_MULT * contracts - commission)
            ret_pct = (exit_price - pos["cost_per_share"]) / pos["cost_per_share"] \
                      if pos["cost_per_share"] != 0 else 0

            log.info("    CLOSED net=$%.2f/sh, P&L=$%+,.2f (%+.1f%%)",
                     exit_price, pnl, ret_pct * 100)

            pos["exit_date"] = today.strftime("%Y-%m-%d")
            pos["exit_price"] = exit_price
            pos["pnl"] = round(pnl, 2)
            record_trade(pos, exit_price, pnl, ret_pct)
        else:
            log.warning("    PARTIAL CLOSE: %d/%d legs.", len(filled_legs), n_legs)
            log.warning("    Manual cleanup needed for remaining legs.")

    save_portfolio(portfolio)


# ═══════════════════════════════════════════════════════════════
#  STATUS DISPLAY
# ═══════════════════════════════════════════════════════════════

def show_basic_status(ib: IB, acct: str) -> None:
    """Show portfolio overview."""
    log.info("=" * 60)
    log.info("PORTFOLIO STATUS")
    log.info("=" * 60)

    info = get_account_info(ib, acct)
    ccy = info.get("base_currency", "USD")
    log.info("  Account:         %s (%s)", acct, ccy)
    log.info("  Net Liquidation: %12,.2f %s", info.get('NetLiquidation', 0), ccy)
    log.info("  Buying Power:    %12,.2f %s", info.get('BuyingPower', 0), ccy)
    log.info("  Cash:            %12,.2f %s", info.get('TotalCashValue', 0), ccy)
    log.info("  Gross Position:  %12,.2f %s", info.get('GrossPositionValue', 0), ccy)

    portfolio = load_portfolio()
    active = [p for p in portfolio["positions"] if "exit_date" not in p]
    closed = [p for p in portfolio["positions"] if "exit_date" in p]

    log.info("  Active positions: %d/%d", len(active), MAX_POSITIONS)

    if active:
        total_deployed = sum(p["total_deployed"] for p in active)
        avg_ff = np.mean([p["ff"] for p in active])
        log.info("  Total deployed:   $%12,.2f", total_deployed)
        log.info("  Avg FF:           %11.1f%%", avg_ff)

        log.info("  %6s  %5s  %6s  %6s  %6s  %3s  %7s  %9s  %5s  %10s  %10s",
                 "Ticker", "Combo", "CallK", "PutK", "Type",
                 "Ctr", "Cost", "Deployed", "FF", "FrontExp", "Entry")
        log.info("  %s", "-" * 95)

        for p in sorted(active, key=lambda x: -x["ff"]):
            ps = p.get("put_strike", p["strike"])
            log.info("  %6s  %5s  $%5.0f  $%5.0f  %6s  %3d  $%6.2f  $%8.2f  %4.1f%%  %10s  %10s",
                     p['ticker'], p['combo'], p['strike'], ps,
                     p['spread_type'], p['contracts'], p['cost_per_share'],
                     p['total_deployed'], p['ff'], p['front_exp'], p['entry_date'])

    if closed:
        total_pnl = sum(p.get("pnl", 0) for p in closed)
        n_wins = sum(1 for p in closed if p.get("pnl", 0) > 0)
        wr = n_wins / len(closed) * 100 if closed else 0
        log.info("  Closed trades: %d", len(closed))
        log.info("  Total P&L:     $%+12,.2f", total_pnl)
        log.info("  Win rate:      %11.1f%%", wr)


def show_detailed_status(ib: IB, acct: str) -> None:
    """Show detailed status with live P&L from IBKR."""
    show_basic_status(ib, acct)

    # Show IBKR portfolio items
    items = ib.portfolio(acct)
    if items:
        log.info("  IBKR Live Positions:")
        log.info("  %8s  %7s  %5s  %9s  %10s  %10s",
                 "Symbol", "SecType", "Pos", "MktPrice", "MktValue", "UnrlzPnL")
        log.info("  %s", "-" * 65)
        for item in items:
            c = item.contract
            log.info("  %8s  %7s  %5.0f  $%8.2f  $%9.2f  $%9.2f",
                     c.symbol, c.secType, item.position,
                     item.marketPrice, item.marketValue, item.unrealizedPNL)

    # Trade history
    if TRADES_FILE.exists():
        with open(TRADES_FILE) as f:
            data = json.load(f)
        trades = data.get("trades", [])
        if trades:
            log.info("  Trade History (last 10):")
            log.info("  %6s  %5s  %10s  %10s  %3s  %9s  %7s  %5s",
                     "Ticker", "Combo", "Entry", "Exit", "Ctr", "P&L", "Ret%", "FF")
            log.info("  %s", "-" * 70)
            for t in trades[-10:]:
                log.info("  %6s  %5s  %10s  %10s  %3d  $%+8.2f  %+6.1f%%  %4.1f%%",
                         t['ticker'], t['combo'],
                         t['entry_date'], t['exit_date'],
                         t['contracts'], t['pnl'],
                         t['return_pct'] * 100, t['ff'])


# ═══════════════════════════════════════════════════════════════
#  SYNC PORTFOLIO WITH IBKR (--sync)
# ═══════════════════════════════════════════════════════════════

def sync_portfolio(ib: IB, acct: str) -> None:
    """Reconcile portfolio.json with actual IBKR positions.

    - Removes positions not on IBKR (orders that were rejected/cancelled)
    - Updates cost with actual fill prices
    """
    log.info("=" * 60)
    log.info("SYNCING PORTFOLIO WITH IBKR")
    log.info("=" * 60)

    portfolio = load_portfolio()
    positions = ib.positions()

    # Build IBKR position map: ticker -> cost info
    ibkr_tickers = {}
    for p in positions:
        sym = p.contract.symbol
        if sym not in ibkr_tickers:
            ibkr_tickers[sym] = {"debit": 0, "credit": 0, "contracts": 0}
        if p.position > 0:
            ibkr_tickers[sym]["debit"] += abs(p.position) * p.avgCost
            ibkr_tickers[sym]["contracts"] = max(
                ibkr_tickers[sym]["contracts"], int(abs(p.position))
            )
        else:
            ibkr_tickers[sym]["credit"] += abs(p.position) * p.avgCost

    log.info("  IBKR positions: %d tickers", len(ibkr_tickers))
    log.info("  Portfolio file: %d entries", len(portfolio['positions']))

    # Reconcile
    kept = []
    removed = []
    updated = []

    for pos in portfolio["positions"]:
        t = pos["ticker"]
        if t not in ibkr_tickers:
            removed.append(t)
            continue

        tc = ibkr_tickers[t]
        net = tc["debit"] - tc["credit"]
        n = tc["contracts"]
        new_cps = round(net / n / CONTRACT_MULT, 2) if n > 0 else 0

        if pos["contracts"] != n or abs(pos["cost_per_share"] - new_cps) > 0.01:
            updated.append(f"{t}: {pos['contracts']}x@${pos['cost_per_share']:.2f}"
                           f" -> {n}x@${new_cps:.2f}")
            pos["contracts"] = n
            pos["cost_per_share"] = new_cps
            pos["total_deployed"] = round(net, 2)

        kept.append(pos)

    portfolio["positions"] = kept
    save_portfolio(portfolio)

    if removed:
        log.info("  REMOVED (%d not on IBKR):", len(removed))
        for t in removed:
            log.info("    %s", t)

    if updated:
        log.info("  UPDATED (%d cost corrections):", len(updated))
        for u in updated:
            log.info("    %s", u)

    total_deployed = sum(p["total_deployed"] for p in kept)
    log.info("  Final: %d positions, $%,.2f deployed", len(kept), total_deployed)


# ═══════════════════════════════════════════════════════════════
#  MAIN / CLI
# ═══════════════════════════════════════════════════════════════

def main() -> None:
    parser = argparse.ArgumentParser(
        description="IBKR Paper Trading — Calendar Spreads"
    )
    parser.add_argument("--enter", action="store_true",
                        help="Place new orders from latest scanner signals")
    parser.add_argument("--close", action="store_true",
                        help="Close positions expiring soon (J-1)")
    parser.add_argument("--status", action="store_true",
                        help="Detailed portfolio status with live P&L")
    parser.add_argument("--sync", action="store_true",
                        help="Reconcile portfolio.json with actual IBKR positions")
    parser.add_argument("--port", type=int, default=TWS_PAPER,
                        help=f"IBKR port (TWS paper={TWS_PAPER}, GW={GW_PAPER})")
    parser.add_argument("--host", default=DEFAULT_HOST,
                        help="IBKR host IP")
    parser.add_argument("--max-new", type=int, default=None,
                        help="Max new positions to enter")

    args = parser.parse_args()

    ib = None
    try:
        ib = connect_ibkr(args.host, args.port)
        acct = verify_paper(ib)

        if args.enter:
            enter_new_positions(ib, acct, max_new=args.max_new)
        elif args.close:
            close_expiring_positions(ib, acct)
        elif args.sync:
            sync_portfolio(ib, acct)
        elif args.status:
            show_detailed_status(ib, acct)
        else:
            show_basic_status(ib, acct)

    except (ConnectionRefusedError, ConnectionError, OSError) as ex:
        log.error("Cannot connect to IBKR (%s).", type(ex).__name__)
        log.error("Make sure TWS or IB Gateway is running with API enabled:")
        log.error("  1. Open TWS or IB Gateway")
        log.error("  2. Login to PAPER TRADING account")
        log.error("  3. Enable API on port %d:", args.port)
        log.error("     Edit -> Global Configuration -> API -> Settings")
        log.error("     [x] Enable ActiveX and Socket Clients")
        log.error("     Socket port: %d", args.port)
    except KeyboardInterrupt:
        log.info("Interrupted")
    except Exception as ex:
        log.error("ERROR: %s", ex, exc_info=True)
    finally:
        if ib and ib.isConnected():
            ib.disconnect()
            log.info("Disconnected from IBKR")


if __name__ == "__main__":
    main()
