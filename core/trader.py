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
import asyncio
import argparse
import numpy as np
import pandas as pd
from datetime import datetime

# Fix Python 3.14 asyncio event loop before importing ib_insync
try:
    asyncio.get_event_loop()
except RuntimeError:
    asyncio.set_event_loop(asyncio.new_event_loop())

from ib_insync import IB, Stock, Option, MarketOrder

from core.config import (
    TRADES_FILE,
    DEFAULT_HOST, TWS_PAPER, GW_PAPER, CLIENT_ID,
    MAX_POSITIONS, CONTRACT_MULT,
    COMMISSION_LEG, CLOSE_DAYS,
    OPTIMAL_START_ET, OPTIMAL_END_ET, ENFORCE_WINDOW,
    MIN_OI_LEG, MAX_BA_PCT,
    CLOSE_PAUSE, SWEET_SPOT_START_ET, SWEET_SPOT_END_ET,
    SWEET_SPOT_GATE, SWEET_SPOT_AGGRESSION_BOOST,
    ENABLE_NETTING, OI_LEG_HARD_GATE,
    FF_THRESHOLD_DEFAULT,
    BA_WIDEN_ABORT, ENABLE_BA_CIRCUIT_BREAKER,
    MAX_PARTICIPATION_RATE, ENABLE_PARTICIPATION_CAP,
    INTER_SIGNAL_PAUSE,
    get_logger,
)
from core.portfolio import (
    load_latest_signals, load_portfolio, save_portfolio,
    add_position, record_trade,
    load_trade_history, compute_kelly, cost_per_contract, size_portfolio,
    load_pending_signals, save_pending_signals, merge_signals_with_pending,
)
from core.execution import (
    execute_spread, execute_spread_close, get_leg_price,
)
from core.pricing import implied_vol_scalar, compute_ff

log = get_logger(__name__)


def _safe_int(val, default: int = 0) -> int:
    """Convert a value to int, treating NaN/None/empty as *default*."""
    if val is None:
        return default
    try:
        if pd.notna(val) and val:
            return int(float(val))
    except (ValueError, TypeError):
        pass
    return default


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
                       "TotalCashValue", "GrossPositionValue",
                       "MaintMarginReq", "InitMarginReq", "Cushion",
                       "ExcessLiquidity"):
            try:
                info[s.tag] = float(s.value)
            except (ValueError, TypeError):
                pass
    return info


def get_ibkr_positions(ib: IB, acct: str = "") -> list[dict]:
    """Get all IBKR positions as a list of dicts."""
    items = ib.portfolio(acct)
    result = []
    for item in items:
        c = item.contract
        result.append({
            "symbol": c.symbol,
            "secType": c.secType,
            "right": getattr(c, "right", ""),
            "strike": float(getattr(c, "strike", 0)),
            "expiry": getattr(c, "lastTradeDateOrContractMonth", ""),
            "position": float(item.position),
            "marketPrice": float(item.marketPrice),
            "marketValue": float(item.marketValue),
            "avgCost": float(item.averageCost),
            "unrealizedPnl": float(item.unrealizedPNL),
            "realizedPnl": float(item.realizedPNL),
        })
    return result


def liquidate_stocks(ib: IB, acct: str = "", symbols: list[str] | None = None) -> list[dict]:
    """Sell all STK positions at market. Optionally filter by symbols list.

    Returns list of results per symbol with fill info.
    """
    items = ib.portfolio(acct)
    stock_positions = [
        item for item in items
        if item.contract.secType == "STK" and item.position != 0
    ]
    if symbols:
        symbols_upper = {s.upper() for s in symbols}
        stock_positions = [
            item for item in stock_positions
            if item.contract.symbol.upper() in symbols_upper
        ]

    results = []
    for item in stock_positions:
        contract = item.contract
        raw_qty = abs(item.position)
        qty = int(raw_qty)                       # IBKR API rejects fractional
        if qty == 0:
            log.info(f"SKIP {contract.symbol}: fractional only ({raw_qty:.2f} shares)")
            results.append({
                "symbol": contract.symbol, "action": "SKIP", "qty": float(raw_qty),
                "filled": False, "fill_price": 0, "status": "Skipped (fractional)",
                "pnl": round(float(item.unrealizedPNL), 2),
            })
            continue
        action = "SELL" if item.position > 0 else "BUY"

        ib.qualifyContracts(contract)
        order = MarketOrder(action, qty)
        order.outsideRth = False
        order.tif = "DAY"
        trade = ib.placeOrder(contract, order)

        # Brief pause for order acknowledgement, then check status
        ib.sleep(2)
        for _ in range(8):
            if trade.orderStatus.status in ("Filled", "Cancelled", "Inactive",
                                            "PreSubmitted", "Submitted"):
                break
            ib.sleep(1)

        filled = trade.orderStatus.status == "Filled"
        fill_price = trade.orderStatus.avgFillPrice if filled else 0
        results.append({
            "symbol": contract.symbol,
            "action": action,
            "qty": float(qty),
            "filled": filled,
            "fill_price": round(fill_price, 2),
            "status": trade.orderStatus.status,
            "pnl": round(float(item.unrealizedPNL), 2),
        })
        log.info(f"LIQUIDATE {action} {qty} {contract.symbol}: "
                 f"{'FILLED @' + str(round(fill_price, 2)) if filled else trade.orderStatus.status}")

    return results


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


def recompute_ff_ibkr(ib: IB, legs: list, stock_px: float) -> tuple[float | None, float, float]:
    """Recompute Forward Factor using IBKR live option quotes.

    Queries IBKR for call leg bid/ask, computes IV from mid,
    then computes FF = (front_iv - fwd_iv) / fwd_iv.

    Returns (ff, front_iv, back_iv) or (None, 0, 0) if unavailable.
    """
    # Find front and back call legs
    front_call = back_call = None
    for option_contract, action in legs:
        if option_contract.right == "C":
            if action == "SELL":   # front month
                front_call = option_contract
            elif action == "BUY":  # back month
                back_call = option_contract

    if not front_call or not back_call:
        return None, 0.0, 0.0

    # Query IBKR for both call legs in parallel
    ib.reqMktData(front_call, "", False, False)
    ib.reqMktData(back_call, "", False, False)
    ib.sleep(3)

    front_tk = ib.ticker(front_call)
    back_tk = ib.ticker(back_call)

    front_mid = back_mid = 0.0
    if front_tk:
        fb = front_tk.bid
        fa = front_tk.ask
        if fb and fa and fb > 0 and fa > 0 and not np.isinf(fb) and not np.isinf(fa):
            front_mid = (float(fb) + float(fa)) / 2
    if back_tk:
        bb = back_tk.bid
        ba = back_tk.ask
        if bb and ba and bb > 0 and ba > 0 and not np.isinf(bb) and not np.isinf(ba):
            back_mid = (float(bb) + float(ba)) / 2

    ib.cancelMktData(front_call)
    ib.cancelMktData(back_call)

    if front_mid <= 0 or back_mid <= 0:
        return None, 0.0, 0.0

    # Compute DTE
    today = datetime.now()
    front_exp_dt = datetime.strptime(front_call.lastTradeDateOrContractMonth, "%Y%m%d")
    back_exp_dt = datetime.strptime(back_call.lastTradeDateOrContractMonth, "%Y%m%d")
    front_dte = max(1, (front_exp_dt - today).days)
    back_dte = max(1, (back_exp_dt - today).days)

    strike = float(front_call.strike)

    # Compute IV from mid prices
    front_iv = implied_vol_scalar(front_mid, stock_px, strike, front_dte / 365.0, right="C")
    back_iv = implied_vol_scalar(back_mid, stock_px, strike, back_dte / 365.0, right="C")

    if np.isnan(front_iv) or np.isnan(back_iv):
        return None, 0.0, 0.0

    ff = compute_ff(front_iv, back_iv, front_dte, back_dte)
    if np.isnan(ff):
        return None, 0.0, 0.0

    return ff, front_iv, back_iv


def create_calendar_legs(ib: IB, ticker: str, strike: float, front_exp: str, back_exp: str,
                         double: bool = False, put_strike: float | None = None) -> tuple[list, int, float | None, float | None]:
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

    # Step 3: Snap call strike + qualify (try nearest strikes on failure)
    call_candidates = sorted(valid_strikes, key=lambda s: abs(float(s) - strike))
    call_candidates = [s for s in call_candidates if abs(float(s) - strike) <= strike * 0.05]
    if not call_candidates:
        log.warning("    No valid IBKR call strike near %.0f for %s", strike, ticker)
        return [], 0, None, None

    # Step 4: Try qualifying call options with nearest strikes
    ibkr_call_strike = None
    front_call = None
    back_call = None

    for candidate in call_candidates[:5]:  # Try up to 5 nearest strikes
        fc = Option(ticker, ibkr_front, float(candidate), "C", "SMART", "100", "USD")
        bc = Option(ticker, ibkr_back,  float(candidate), "C", "SMART", "100", "USD")
        try:
            ib.qualifyContracts(fc, bc)
            if fc.conId > 0 and bc.conId > 0:
                ibkr_call_strike = float(candidate)
                front_call = fc
                back_call = bc
                if ibkr_call_strike != strike:
                    log.info("    Snapped call strike: %.1f -> %.1f", strike, ibkr_call_strike)
                break
            else:
                log.info("    Call K=%.1f not available for %s/%s, trying next...",
                         float(candidate), ibkr_front, ibkr_back)
        except Exception:
            log.info("    Call K=%.1f qualification error, trying next...", float(candidate))

    if ibkr_call_strike is None:
        log.error("    No valid call strike found for %s near %.0f after trying %d candidates",
                  ticker, strike, min(5, len(call_candidates)))
        return [], 0, None, None

    # BUY first, then SELL (never naked short)
    legs = [
        (back_call,  "BUY"),
        (front_call, "SELL"),
    ]

    actual_put_strike = None

    # Put legs for double calendar (separate put strike, retry nearest on failure)
    if double:
        ps = float(put_strike) if put_strike is not None else strike
        put_candidates = sorted(valid_strikes, key=lambda s: abs(float(s) - ps))
        put_candidates = [s for s in put_candidates if abs(float(s) - ps) <= ps * 0.05]

        ibkr_put_strike = None
        for candidate in put_candidates[:5]:
            fp = Option(ticker, ibkr_front, float(candidate), "P", "SMART", "100", "USD")
            bp = Option(ticker, ibkr_back,  float(candidate), "P", "SMART", "100", "USD")
            try:
                ib.qualifyContracts(fp, bp)
                if fp.conId > 0 and bp.conId > 0:
                    ibkr_put_strike = float(candidate)
                    if ibkr_put_strike != ps:
                        log.info("    Snapped put strike: %.1f -> %.1f", ps, ibkr_put_strike)
                    legs.extend([
                        (bp, "BUY"),
                        (fp, "SELL"),
                    ])
                    actual_put_strike = ibkr_put_strike
                    break
                else:
                    log.info("    Put K=%.1f not available, trying next...", float(candidate))
            except Exception:
                log.info("    Put K=%.1f qualification error, trying next...", float(candidate))

        if ibkr_put_strike is None:
            log.warning("    No valid put strike for %s near %.0f, using single calendar", ticker, ps)

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


def check_sweet_spot() -> tuple[bool, str]:
    """Check if we're in the sweet spot 10:30-11:30 ET (Muravyev & Pearson 2020).

    Returns (in_sweet_spot: bool, message: str).
    """
    try:
        from zoneinfo import ZoneInfo
    except ImportError:
        from backports.zoneinfo import ZoneInfo
    now_et = datetime.now(ZoneInfo("America/New_York"))
    current_time = now_et.strftime("%H:%M")

    in_sweet = SWEET_SPOT_START_ET <= current_time <= SWEET_SPOT_END_ET
    if in_sweet:
        msg = f"In sweet spot {SWEET_SPOT_START_ET}-{SWEET_SPOT_END_ET} ET ({current_time} ET) — tightest spreads"
    else:
        msg = f"Outside sweet spot ({SWEET_SPOT_START_ET}-{SWEET_SPOT_END_ET} ET). Current: {current_time} ET"
    return in_sweet, msg


def _find_nettable_positions(to_close: list[dict], qualified: list) -> list[str]:
    """Detect positions that could be netted (Man Group 2024).

    Legacy V1: log-only — identifies tickers present in both close and enter lists.
    Returns list of nettable tickers.
    """
    close_tickers = {pos["ticker"] for pos in to_close}
    enter_tickers = {q[0]["ticker"] for q in qualified} if qualified else set()
    return sorted(close_tickers & enter_tickers)


def compute_netting(to_close: list[dict], qualified: list) -> list[dict]:
    """Active Netting V2 — detect roll opportunities (Man Group 2024).

    Input:
        to_close: positions expiring soon
        qualified: list of (sig, legs, n_legs, strike, put_strike, cps, spread_type)

    Detects:
        - same ticker + same back_exp + same strike → "roll_front" (only close/open front legs)
        - otherwise → "none" (close and re-enter fully)

    Returns list of NettingInstruction dicts:
        {ticker, action, savings_estimate, close_pos, enter_sig}
    """
    close_by_ticker = {pos["ticker"]: pos for pos in to_close}
    enter_by_ticker = {}
    for q in qualified:
        sig = q[0]
        enter_by_ticker[sig["ticker"]] = q

    instructions = []
    overlap = sorted(set(close_by_ticker.keys()) & set(enter_by_ticker.keys()))

    for ticker in overlap:
        close_pos = close_by_ticker[ticker]
        enter_q = enter_by_ticker[ticker]
        enter_sig = enter_q[0]

        # Check if back_exp and strike match → roll_front
        same_back = close_pos.get("back_exp") == str(enter_sig.get("back_exp", ""))
        same_strike = abs(close_pos.get("strike", 0) - float(enter_sig.get("strike", 0))) < 0.01

        if same_back and same_strike:
            # Roll front: only close/open front legs → save 2 legs × $0.65 × contracts
            contracts = close_pos.get("contracts", 0)
            savings = 2 * COMMISSION_LEG * contracts
            instructions.append({
                "ticker": ticker,
                "action": "roll_front",
                "savings_estimate": round(savings, 2),
                "close_back_exp": close_pos.get("back_exp"),
                "close_strike": close_pos.get("strike"),
            })
        else:
            instructions.append({
                "ticker": ticker,
                "action": "none",
                "savings_estimate": 0.0,
                "close_back_exp": close_pos.get("back_exp"),
                "close_strike": close_pos.get("strike"),
            })

    return instructions


# Execution engine delegated to core.execution module
# Backward-compat alias for api/routes_trading.py
execute_spread_optimal = execute_spread


def _distribute_across_combos(
    ib, ticker, total_contracts, primary_sig, primary_legs, primary_n_legs,
    primary_strike, primary_put_strike, primary_cps, primary_spread_type,
    alternatives, exec_priority,
    portfolio, active_tickers, slippage_log,
    buying_power, total_deployed,
):
    """Distribute contracts across multiple combos for the same ticker when OI is tight.

    Splits contracts proportionally to each combo's min leg OI, respecting the
    10% OI gate per combo. Executes each chunk independently.

    Returns dict with {entered, deployed} or None if distribution failed.
    """
    # Build list of all combos: primary + alternatives (FF >= threshold)
    all_combos = [(primary_sig, primary_legs, primary_n_legs,
                   primary_strike, primary_put_strike, primary_cps, primary_spread_type)]
    for alt in alternatives:
        alt_ff = float(alt[0].get("ff", 0) or 0)
        if alt_ff >= FF_THRESHOLD_DEFAULT:
            all_combos.append(alt)
        else:
            log.info("    %s: alt combo %s skipped (FF=%.1f%% < %.0f%%)",
                     ticker, alt[0].get("combo", "?"), alt_ff * 100,
                     FF_THRESHOLD_DEFAULT * 100)

    # Compute OI capacity per combo (10% of min leg OI)
    capacities = []
    for (sig, legs, n_legs, strike, put_strike, cps, stype) in all_combos:
        f_oi = _safe_int(sig.get("front_oi"))
        b_oi = _safe_int(sig.get("back_oi"))
        leg_ois = [oi for oi in (f_oi, b_oi) if oi > 0]
        cap = max(1, int(min(leg_ois) * OI_LEG_HARD_GATE)) if leg_ois else 0
        capacities.append(cap)

    total_capacity = sum(capacities)
    if total_capacity <= 0:
        log.warning("    %s: No OI capacity across %d combos — SKIP", ticker, len(all_combos))
        return None

    # Distribute proportionally, capped at each combo's capacity
    remaining = min(total_contracts, total_capacity)
    allocations = [0] * len(all_combos)
    for i in range(len(all_combos)):
        if remaining <= 0:
            break
        alloc = min(capacities[i], remaining)
        allocations[i] = alloc
        remaining -= alloc

    actual_total = sum(allocations)
    if actual_total <= 0:
        return None

    log.info("    %s: OI DISTRIBUTION across %d combos: %s (total %d/%d)",
             ticker, sum(1 for a in allocations if a > 0),
             " + ".join(f"{combo_entry[0]['combo']}={alloc}"
                        for combo_entry, alloc in zip(all_combos, allocations) if alloc > 0),
             actual_total, total_contracts)

    total_entered = 0
    total_bp_used = 0.0

    for i, ((sig, legs, n_legs, strike, put_strike, cps, stype), n) in enumerate(
            zip(all_combos, allocations)):
        if n <= 0:
            continue

        cpc = cost_per_contract(cps, n_legs)
        chunk_deployed = n * cpc
        if chunk_deployed > buying_power * 0.9:
            log.warning("    %s %s: SKIP chunk ($%.0f > BP)", ticker, sig["combo"], chunk_deployed)
            continue

        min_oi = min(_safe_int(sig.get("front_oi")), _safe_int(sig.get("back_oi")))
        log.info("    %s %s: %dx (OI chunk %d/%d)",
                 ticker, sig["combo"], n, i + 1, len(all_combos))

        result, fill_cost, slippage, details = execute_spread(
            ib, ticker, legs, n_legs, n, cps, stype,
            min_leg_oi=min_oi,
            priority=exec_priority,
        )

        slippage_log.append({
            "ticker": ticker, "eodhd_mid": cps,
            "fill_cost": fill_cost, "slippage": slippage,
            "method": details.get("method"), "result": result,
        })

        if result == "full":
            pos = add_position(
                portfolio, ticker, sig["combo"], strike,
                sig["front_exp"], sig["back_exp"],
                n, fill_cost, stype, sig["ff"], n_legs,
                put_strike=put_strike, min_leg_oi=min_oi,
            )
            pos["execution_method"] = details.get("method")
            pos["slippage"] = round(slippage, 4)
            active_tickers.add(ticker)
            total_entered += 1
            total_bp_used += chunk_deployed
            buying_power -= chunk_deployed

            try:
                from core.tca import ExecutionRecord, record_execution
                record_execution(ExecutionRecord(
                    ticker=ticker, direction="entry", scanner_mid=cps,
                    theta_bid=details.get("theta_bid", 0.0),
                    theta_ask=details.get("theta_ask", 0.0),
                    theta_mid=details.get("theta_mid", 0.0),
                    method=details.get("method", ""),
                    combo_attempted=details.get("combo_attempted", False),
                    combo_result=details.get("combo_result"),
                    walk_steps=details.get("walk_steps", 0),
                    initial_limit=details.get("initial_limit", 0.0),
                    final_limit=details.get("final_limit", 0.0),
                    exec_seconds=details.get("exec_seconds", 0.0),
                    priority=details.get("priority", exec_priority),
                    fill_price=fill_cost, contracts=n, slippage=slippage,
                    min_leg_oi=min_oi, spread_type=stype, n_legs=n_legs,
                ))
            except Exception:
                pass

    if total_entered > 0:
        return {"entered": total_entered, "deployed": total_bp_used}
    return None


# ═══════════════════════════════════════════════════════════════
#  ENTER NEW POSITIONS (--enter)
# ═══════════════════════════════════════════════════════════════

def enter_new_positions(ib: IB, acct: str, max_new: int | None = None,
                        retry_signals: list | None = None) -> dict | None:
    """Place new calendar spread orders from scanner signals.

    Two-pass approach:
      Pass 1: Qualify contracts on IBKR (identify valid positions)
      Pass 2: Size all positions globally via Kelly to hit target allocation
      Pass 3: Place orders

    If retry_signals is provided, uses those pre-filtered signals instead
    of loading fresh signals (intra-day retry mode).
    """
    log.info("=" * 60)
    log.info("ENTERING NEW POSITIONS")
    log.info("=" * 60)

    # Load signals: either from retry list or fresh + pending
    if retry_signals is not None:
        log.info("  INTRA-DAY RETRY: %d pre-filtered signals", len(retry_signals))
        signals = pd.DataFrame(retry_signals)
        pending = []
        if signals.empty:
            return
    else:
        fresh_signals = load_latest_signals()
        pending = load_pending_signals()
        signals = merge_signals_with_pending(fresh_signals, pending)
        if signals.empty:
            return

    pending_drop_tickers = set()   # tickers whose FF decayed → remove from pending

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
    log.info(f"  Account:       ${account_value:,.0f}")
    log.info(f"  Buying power:  ${buying_power:,.0f}")
    log.info("  Kelly history: %d trades", len(returns))
    log.info("  Half Kelly f:  %.4f (%.1f%%)", kelly_f, kelly_f * 100)
    log.info(f"  Kelly target:  ${kelly_target:,.0f} (f * W)")

    # ── PASS 1: Qualify all contracts (individual legs) ──
    # Allow multiple combos per ticker for OI distribution across expirations
    log.info("  --- PASS 1: Qualifying contracts on IBKR ---")
    qualified = []  # (sig, legs, n_legs, actual_strike, actual_put_strike, cost_per_share, spread_type)
    qualified_tickers = set()   # tickers with at least one qualified combo
    alt_combos = {}             # ticker -> list of alternative qualified combos
    skipped = 0

    for _, sig in signals.iterrows():
        ticker = sig["ticker"]
        if ticker in active_tickers:
            continue

        # Primary combo: first seen per ticker fills a slot
        # Alt combos: subsequent combos for same ticker stored for OI distribution
        if ticker in qualified_tickers:
            # Already have primary — store as alternative (no slot cost)
            if ticker not in alt_combos:
                alt_combos[ticker] = []
            # Still need to qualify on IBKR though
        elif len(qualified) >= slots:
            break

        spread_type = "single"
        cps = sig["call_cost"]

        # ── Liquidity filters ──
        front_oi = _safe_int(sig.get("front_oi"))
        back_oi = _safe_int(sig.get("back_oi"))
        min_leg_oi = min(front_oi, back_oi) if (front_oi > 0 and back_oi > 0) else max(front_oi, back_oi)
        ba_pct = float(sig.get("ba_pct", 0) or 0)

        # Filter MIN_OI_LEG: skip if OI is known but below threshold
        if min_leg_oi > 0 and min_leg_oi < MIN_OI_LEG:
            log.info("    %s: SKIP — min_leg_oi=%d < %d",
                     ticker, min_leg_oi, MIN_OI_LEG)
            skipped += 1
            continue

        # Filter MAX_BA_PCT: skip if bid-ask spread too wide
        if ba_pct > MAX_BA_PCT:
            log.info("    %s: SKIP — ba_pct=%.1f%% > %.0f%%",
                     ticker, ba_pct * 100, MAX_BA_PCT * 100)
            skipped += 1
            continue

        scanner_ff = float(sig.get("ff", 0) or 0)
        log.info("    %s %s K=%.0f scanFF=%.1f%% $%.2f (%s) OI=%d/%d BA=%.1f%% ...",
                 ticker, sig['combo'], sig['strike'],
                 scanner_ff * 100, cps, spread_type, front_oi, back_oi, ba_pct * 100)

        legs, n_legs, actual_strike, actual_put_strike = create_calendar_legs(
            ib, ticker, sig["strike"],
            sig["front_exp"], sig["back_exp"],
        )
        if not legs:
            log.info("    -> SKIP (no legs)")
            skipped += 1
            continue

        # ── Recompute FF with IBKR live IV data ──
        stock_px = float(sig.get("stock_px", 0) or 0)
        ibkr_ff, front_iv, back_iv = recompute_ff_ibkr(ib, legs, stock_px)

        if ibkr_ff is not None:
            sig_ff = ibkr_ff
            log.info("    %s: IBKR FF=%.1f%% (front_iv=%.1f%% back_iv=%.1f%%)",
                     ticker, ibkr_ff * 100, front_iv * 100, back_iv * 100)
        else:
            sig_ff = scanner_ff
            log.info("    %s: IBKR IV unavailable, using scanner FF=%.1f%%",
                     ticker, scanner_ff * 100)

        # Store live FF in signal for downstream use
        sig = sig.copy()
        sig["ff"] = sig_ff

        if sig_ff < FF_THRESHOLD_DEFAULT:
            log.info("    %s: SKIP — FF=%.1f%% < %.0f%% threshold",
                     ticker, sig_ff * 100, FF_THRESHOLD_DEFAULT * 100)
            skipped += 1
            # If this was a pending signal, mark for removal (FF decayed)
            if sig.get("_pending"):
                pending_drop_tickers.add(ticker)
            continue

        log.info("    -> OK")
        entry = (sig, legs, n_legs, actual_strike, actual_put_strike, cps, spread_type)
        if ticker not in qualified_tickers:
            qualified.append(entry)
            qualified_tickers.add(ticker)
        else:
            alt_combos.setdefault(ticker, []).append(entry)

    n_alts = sum(len(v) for v in alt_combos.values())
    log.info("  Qualified: %d/%d (%d skipped, %d alt combos for %d tickers)",
             len(qualified), len(qualified) + skipped, skipped,
             n_alts, len(alt_combos))

    if not qualified:
        return

    # ── PASS 2: Global Kelly sizing ──
    log.info("  --- PASS 2: Kelly sizing (%d positions) ---", len(qualified))
    # qualified tuple: (sig, legs, n_legs, actual_strike, actual_put_strike, cps, spread_type)
    # signals_info: (ticker, cps, n_legs, ff, ba_pct, min_leg_oi)
    def _min_leg_oi(sig):
        f_oi = _safe_int(sig.get("front_oi"))
        b_oi = _safe_int(sig.get("back_oi"))
        if f_oi > 0 and b_oi > 0:
            return min(f_oi, b_oi)
        return max(f_oi, b_oi)

    signals_info = [(q[0]["ticker"], q[5], q[2], float(q[0]["ff"]),
                     float(q[0].get("ba_pct", 0)), _min_leg_oi(q[0]))
                    for q in qualified]
    sizing = size_portfolio(signals_info, kelly_f, account_value)

    total_deployed = sum(d for _, _, d in sizing)
    log.info(f"  Kelly target:  ${kelly_target:,.0f}")
    log.info(f"  Total sized:   ${total_deployed:,.0f}")
    gap = kelly_target - total_deployed
    gap_pct = (total_deployed / kelly_target - 1) * 100 if kelly_target else 0
    log.info(f"  Gap:           ${gap:+,.0f} ({gap_pct:+.1f}%)")

    log.info("  %6s %6s %6s %4s %9s", "Ticker", "FF%", "Cost", "Ctr", "Deployed")
    log.info("  %s", "-" * 38)
    for (sig, _legs, _nl, _as, _aps, cps_q, _st), (ticker, n_ctr, deployed) in zip(qualified, sizing):
        log.info("  %6s %5.1f%% $%5.2f %4d $%8.0f",
                 ticker, sig['ff'], cps_q, n_ctr, deployed)

    # ── Time-of-day check ──
    is_optimal, time_msg = check_optimal_window()
    log.info("  %s", time_msg)
    if ENFORCE_WINDOW and not is_optimal:
        log.warning("  Orders blocked -- retry between %s-%s ET.",
                    OPTIMAL_START_ET, OPTIMAL_END_ET)
        return

    # ── PASS 3: Optimal execution (BAG-only, no legs fallback) ──
    log.info("  --- PASS 3: Optimal execution (%d spreads) ---", len(qualified))
    log.info("  Strategy: BAG-only combo (no legs fallback)")

    # Sweet spot check (Muravyev & Pearson 2020)
    in_sweet, sweet_msg = check_sweet_spot()
    log.info("  %s", sweet_msg)

    # ── Sweet Spot Gate (Muravyev & Pearson 2020) ──
    if SWEET_SPOT_GATE and not in_sweet:
        log.warning("  SWEET SPOT GATE: deferring execution until %s-%s ET",
                    SWEET_SPOT_START_ET, SWEET_SPOT_END_ET)
        return

    # Adaptive algo priority: patient in sweet spot, urgent outside
    exec_priority = "Normal" if in_sweet else "Urgent"
    if SWEET_SPOT_AGGRESSION_BOOST and not in_sweet:
        log.info("  Priority: %s (outside sweet spot)", exec_priority)

    # Active Netting V2 (Man Group 2024)
    today = datetime.now()
    expiring_positions = [
        p for p in active
        if p.get("front_exp")
        and (datetime.strptime(p["front_exp"], "%Y-%m-%d") - today).days <= CLOSE_DAYS
    ]
    netting_instructions = []
    if expiring_positions and qualified and ENABLE_NETTING:
        netting_instructions = compute_netting(expiring_positions, qualified)
        rollable = [ni for ni in netting_instructions if ni["action"] == "roll_front"]
        if rollable:
            tickers = [ni["ticker"] for ni in rollable]
            total_savings = sum(ni["savings_estimate"] for ni in rollable)
            log.info("  ACTIVE NETTING: %d ticker(s) will be rolled: %s (est. savings $%.2f)",
                     len(rollable), ", ".join(tickers), total_savings)
    elif expiring_positions and qualified:
        # Legacy netting log
        nettable = _find_nettable_positions(expiring_positions, qualified)
        if nettable:
            log.info("  NETTING: %d ticker(s) in both close & enter: %s",
                     len(nettable), ", ".join(nettable))

    # Sort by most liquid first (fill easy names before IBKR event loop saturates)
    exec_order = list(zip(qualified, sizing))
    exec_order.sort(key=lambda x: _min_leg_oi(x[0][0]), reverse=True)
    log.info("  Execution order (most liquid first): %s",
             ", ".join(f"{x[0][0]['ticker']}(OI={_min_leg_oi(x[0][0])})"
                       for x in exec_order))

    entered = 0
    partial = 0
    not_filled = 0
    skipped_exec = 0
    slippage_log = []
    filled_tickers = set()
    unfilled_signals = []          # signals that failed to fill → pending

    for (sig, legs, n_legs, actual_strike, actual_put_strike, cps, spread_type), \
        (ticker, contracts, deployed) in exec_order:
        if contracts <= 0:
            continue

        if deployed > buying_power * 0.9:
            log.warning("    %s: SKIP ($%.0f > buying power $%.0f)", ticker, deployed, buying_power)
            continue

        pos_min_leg_oi = _min_leg_oi(sig)

        # ── Spread circuit breaker: abort if live BA widened (Hasbrouck 2007) ──
        if ENABLE_BA_CIRCUIT_BREAKER:
            try:
                worst_live_ba = 0.0
                for leg_contract, _ in legs[:2]:  # back + front call legs
                    _cb_bid, _cb_ask, _cb_mid = get_leg_price(ib, leg_contract)
                    if _cb_bid > 0 and _cb_ask > _cb_bid:
                        leg_ba = _cb_ask - _cb_bid
                        worst_live_ba = max(worst_live_ba, leg_ba)
                signal_ba_pct = float(sig.get("ba_pct", 0) or 0)
                signal_ba = abs(cps) * signal_ba_pct if signal_ba_pct > 0 else 0
                if signal_ba > 0 and worst_live_ba > signal_ba * BA_WIDEN_ABORT:
                    log.warning("    %s: CIRCUIT BREAKER — worst leg BA $%.2f > %.0fx signal BA $%.2f, SKIP",
                                ticker, worst_live_ba, BA_WIDEN_ABORT, signal_ba)
                    skipped_exec += 1
                    continue
            except Exception as ex:
                log.warning("    %s: Circuit breaker price check failed: %s — proceeding cautiously", ticker, ex)

        # ── Participation rate cap: refuse if > 5% of daily volume (Almgren et al. 2005) ──
        if ENABLE_PARTICIPATION_CAP:
            try:
                from core.portfolio import _get_daily_volume
                daily_vol = _get_daily_volume(ticker)
                if daily_vol > 0:
                    participation = contracts / daily_vol
                    if participation > MAX_PARTICIPATION_RATE:
                        capped = max(1, int(daily_vol * MAX_PARTICIPATION_RATE))
                        log.warning("    %s: PARTICIPATION CAP — %d contracts = %.1f%% of daily vol %d (cap → %d)",
                                    ticker, contracts, participation * 100, daily_vol, capped)
                        contracts = capped
                        deployed = contracts * cost_per_contract(cps, n_legs)
            except Exception:
                pass

        # ── Per-leg OI hard gate: refuse if contracts > 10% of ANY leg's OI ──
        front_oi = _safe_int(sig.get("front_oi"))
        back_oi = _safe_int(sig.get("back_oi"))
        leg_ois = [oi for oi in (front_oi, back_oi) if oi > 0]
        if leg_ois:
            min_oi = min(leg_ois)
            oi_cap = max(1, int(min_oi * OI_LEG_HARD_GATE))
            if contracts > oi_cap:
                log.warning("    %s: OI GATE — %d contracts > %d%% of min leg OI %d (cap=%d)",
                            ticker, contracts, int(OI_LEG_HARD_GATE * 100), min_oi, oi_cap)
                # Try to distribute across alternative combos
                if ticker in alt_combos and alt_combos[ticker]:
                    distributed = _distribute_across_combos(
                        ib, ticker, contracts, sig, legs, n_legs,
                        actual_strike, actual_put_strike, cps, spread_type,
                        alt_combos[ticker], exec_priority,
                        portfolio, active_tickers, slippage_log,
                        buying_power, deployed,
                    )
                    if distributed:
                        entered += distributed["entered"]
                        buying_power -= distributed["deployed"]
                        continue
                # No alternatives or distribution failed — cap at OI limit
                if oi_cap <= 0:
                    log.warning("    %s: SKIP — OI too low for any contracts", ticker)
                    skipped_exec += 1
                    continue
                old_n = contracts
                contracts = oi_cap
                deployed = contracts * cost_per_contract(cps, n_legs)
                log.info("    %s: Reduced %d → %d contracts (OI gate)", ticker, old_n, contracts)

        log.info("    %s: %dx %s (%d legs, mid=$%.2f, OI=%d/%d)",
                 ticker, contracts, spread_type, n_legs, cps, front_oi, back_oi)

        result, fill_cost, slippage, details = execute_spread(
            ib, ticker, legs, n_legs, contracts, cps, spread_type,
            min_leg_oi=pos_min_leg_oi,
            priority=exec_priority
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
                put_strike=actual_put_strike,
                min_leg_oi=pos_min_leg_oi
            )
            pos["execution_method"] = details.get("method")
            pos["slippage"] = round(slippage, 4)
            active_tickers.add(ticker)
            filled_tickers.add(ticker)
            entered += 1
            buying_power -= deployed

            # ── TCA Recording (Kissell & Glantz 2003) ──
            try:
                from core.tca import ExecutionRecord, record_execution
                record_execution(ExecutionRecord(
                    ticker=ticker,
                    direction="entry",
                    scanner_mid=cps,
                    theta_bid=details.get("theta_bid", 0.0),
                    theta_ask=details.get("theta_ask", 0.0),
                    theta_mid=details.get("theta_mid", 0.0),
                    method=details.get("method", ""),
                    combo_attempted=details.get("combo_attempted", False),
                    combo_result=details.get("combo_result"),
                    walk_steps=details.get("walk_steps", 0),
                    initial_limit=details.get("initial_limit", 0.0),
                    final_limit=details.get("final_limit", 0.0),
                    exec_seconds=details.get("exec_seconds", 0.0),
                    priority=details.get("priority", exec_priority),
                    fill_price=fill_cost,
                    contracts=contracts,
                    slippage=slippage,
                    min_leg_oi=pos_min_leg_oi,
                    option_ba=float(sig.get("ba_pct", 0) or 0),
                    spread_type=spread_type,
                    n_legs=n_legs,
                ))
            except Exception as ex:
                log.debug("TCA recording failed: %s", ex)
        elif result == "partial":
            partial += 1
            # Register partially filled position (some slices succeeded)
            filled_cts = details.get("filled_contracts", 0)
            if filled_cts > 0:
                partial_deployed = filled_cts * cost_per_contract(cps, n_legs)
                pos = add_position(
                    portfolio, ticker, sig["combo"], actual_strike,
                    sig["front_exp"], sig["back_exp"],
                    filled_cts, fill_cost, spread_type, sig["ff"], n_legs,
                    put_strike=actual_put_strike,
                    min_leg_oi=pos_min_leg_oi
                )
                pos["execution_method"] = details.get("method")
                pos["slippage"] = round(slippage, 4)
                pos["partial"] = True
                active_tickers.add(ticker)
                filled_tickers.add(ticker)
                buying_power -= partial_deployed
                log.info("    %s: Partial fill registered (%d/%d contracts)",
                         ticker, filled_cts, contracts)
        else:
            not_filled += 1
            # Add to pending for retry next day
            raw_rc = sig.get("retry_count", 0)
            retry_count = int(raw_rc if pd.notna(raw_rc) else 0) + 1
            raw_ps = sig.get("pending_since", None)
            pending_since = str(raw_ps) if pd.notna(raw_ps) and raw_ps else datetime.now().strftime("%Y-%m-%d")
            unfilled_signals.append({
                "ticker": ticker,
                "combo": str(sig.get("combo", "")),
                "strike": float(sig.get("strike", 0)),
                "front_exp": str(sig.get("front_exp", "")),
                "back_exp": str(sig.get("back_exp", "")),
                "ff": float(sig.get("ff", 0)),
                "call_cost": float(sig.get("call_cost", cps)),
                "front_oi": _safe_int(sig.get("front_oi")),
                "back_oi": _safe_int(sig.get("back_oi")),
                "ba_pct": float(sig.get("ba_pct", 0) or 0) if pd.notna(sig.get("ba_pct", 0)) else 0.0,
                "pending_since": pending_since,
                "retry_count": retry_count,
            })
            log.info("    %s: Added to pending (retry #%d)", ticker, retry_count)

        # Pause between signal executions (let IBKR event loop settle)
        if INTER_SIGNAL_PAUSE > 0:
            ib.sleep(INTER_SIGNAL_PAUSE)

    save_portfolio(portfolio)

    # ── Save pending signals ──
    # Keep: existing pending that weren't filled and weren't dropped
    # Add: newly unfilled signals
    remaining_pending = [
        s for s in pending
        if s.get("ticker") not in filled_tickers
        and s.get("ticker") not in pending_drop_tickers
    ]
    all_pending = remaining_pending + unfilled_signals
    save_pending_signals(all_pending)

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
    log.info(f"  Kelly target:  ${kelly_target:,.0f}")
    log.info(f"  Total sized:   ${total_deployed:,.0f}")
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
        if not pos.get("front_exp"):
            continue
        front_exp = pd.Timestamp(pos["front_exp"])
        days_left = (front_exp - pd.Timestamp(today)).days
        if days_left <= CLOSE_DAYS:
            to_close.append(pos)

    if not to_close:
        log.info("  No positions to close (next expiry not within %d days)", CLOSE_DAYS)
        for pos in active:
            if not pos.get("front_exp"):
                log.info("    %s: SKIP — no front_exp", pos['ticker'])
                continue
            front_exp = pd.Timestamp(pos["front_exp"])
            days_left = (front_exp - pd.Timestamp(today)).days
            log.info("    %s %s: front exp %s (%dd)",
                     pos['ticker'], pos['combo'], pos['front_exp'], days_left)
        return

    log.info("  Positions to close: %d", len(to_close))

    # Sort by least liquid first (Le Coz et al. 2024)
    to_close.sort(key=lambda p: p.get("min_leg_oi", 0))
    log.info("  Close order (least liquid first): %s",
             ", ".join(f"{p['ticker']}(OI={p.get('min_leg_oi', 0)})" for p in to_close))

    # ── Time-of-day check for closing ──
    is_optimal, time_msg = check_optimal_window()
    log.info("  %s", time_msg)
    if ENFORCE_WINDOW and not is_optimal:
        log.warning("  Close orders blocked -- retry between %s-%s ET.",
                    OPTIMAL_START_ET, OPTIMAL_END_ET)
        return

    for i, pos in enumerate(to_close):
        ticker = pos["ticker"]
        contracts = pos["contracts"]
        is_double = pos["spread_type"] == "double"
        pos_put_strike = pos.get("put_strike", pos["strike"]) if is_double else None
        log.info("  Closing %s %s K=%.0f x%d (%s)",
                 ticker, pos['combo'], pos['strike'],
                 contracts, pos['spread_type'])

        # Rebuild individual leg contracts (double=True only for legacy double positions)
        legs, n_legs, _, _ = create_calendar_legs(
            ib, ticker, pos["strike"],
            pos["front_exp"], pos["back_exp"],
            double=is_double,
            put_strike=pos_put_strike,
        )
        if not legs:
            log.error("    Could not rebuild contract, manual close needed")
            continue

        # Execute close via execution module (combo-first, legs-fallback)
        close_success, total_exit_price, method, filled_legs = execute_spread_close(
            ib, ticker, legs, n_legs, contracts
        )

        if close_success:
            exit_price = total_exit_price
            commission = n_legs * COMMISSION_LEG * contracts
            pnl = ((exit_price - pos["cost_per_share"]) *
                   CONTRACT_MULT * contracts - commission)
            deployed = pos.get("total_deployed", pos["cost_per_share"] * CONTRACT_MULT * contracts)
            ret_pct = pnl / deployed if deployed > 0 else 0

            log.info("    CLOSED net=$%.2f/sh, P&L=$%+,.2f (%+.1f%%)",
                     exit_price, pnl, ret_pct * 100)

            pos["exit_date"] = today.strftime("%Y-%m-%d")
            pos["exit_price"] = exit_price
            pos["pnl"] = round(pnl, 2)
            pos["close_method"] = method
            record_trade(pos, exit_price, pnl, ret_pct)

            # ── TCA Recording for close (Kissell & Glantz 2003) ──
            try:
                from core.tca import ExecutionRecord, record_execution
                record_execution(ExecutionRecord(
                    ticker=ticker,
                    direction="close",
                    scanner_mid=pos.get("cost_per_share", 0.0),
                    method=method,
                    fill_price=exit_price,
                    contracts=contracts,
                    slippage=exit_price - pos.get("cost_per_share", 0.0),
                    min_leg_oi=pos.get("min_leg_oi", 0),
                    spread_type=pos.get("spread_type", ""),
                    n_legs=n_legs,
                ))
            except Exception as ex:
                log.debug("TCA recording (close) failed: %s", ex)
        else:
            log.warning("    PARTIAL CLOSE: %d/%d legs.", len(filled_legs), n_legs)
            log.warning("    Manual cleanup needed for remaining legs.")

        # Stagger closes (Moallemi & Park 2018)
        if i < len(to_close) - 1:
            log.info("    Pausing %ds between close executions", CLOSE_PAUSE)
            ib.sleep(CLOSE_PAUSE)

    save_portfolio(portfolio)


# ═══════════════════════════════════════════════════════════════
#  CLOSE SINGLE POSITION ON IBKR (user-triggered via UI)
# ═══════════════════════════════════════════════════════════════

def close_position_ibkr(ib: IB, pos: dict) -> dict:
    """Close a single position on IBKR with real order execution.

    Reuses the same execution logic as close_expiring_positions() but for
    a single user-triggered close. Does NOT mutate pos or save portfolio —
    the API route handles that.

    Returns dict: {success, exit_price, pnl, return_pct, method, details, n_filled, n_legs}
    """
    ticker = pos["ticker"]
    contracts = pos["contracts"]

    # 0-contract positions can't be closed on IBKR
    if contracts == 0:
        return {
            "success": False,
            "error": "Position has 0 contracts (failed entry). Use paper close.",
            "method": "skip",
        }

    is_double = pos["spread_type"] == "double"
    pos_put_strike = pos.get("put_strike", pos["strike"]) if is_double else None

    log.info("  IBKR CLOSE: %s %s K=%.0f x%d (%s)",
             ticker, pos['combo'], pos['strike'],
             contracts, pos['spread_type'])

    # Rebuild individual leg contracts (double=True only for legacy double positions)
    legs, n_legs, _, _ = create_calendar_legs(
        ib, ticker, pos["strike"],
        pos["front_exp"], pos["back_exp"],
        double=is_double,
        put_strike=pos_put_strike,
    )
    if not legs:
        return {
            "success": False,
            "error": "Could not rebuild contracts on IBKR. Manual close needed.",
            "method": "failed",
        }

    # Execute close via execution module (combo-first, legs-fallback)
    close_success, total_exit_price, method, filled_legs = execute_spread_close(
        ib, ticker, legs, n_legs, contracts
    )

    if not close_success:
        n_filled = len(filled_legs)
        return {
            "success": False,
            "error": f"Partial close: {n_filled}/{n_legs} legs filled. Manual cleanup needed.",
            "method": method,
            "n_filled": n_filled,
            "n_legs": n_legs,
            "filled_legs": filled_legs,
        }

    # Compute P&L from actual fills (no estimated slippage — baked into fills)
    entry_price = pos.get("cost_per_share", 0)
    commission = n_legs * COMMISSION_LEG * contracts
    pnl = ((total_exit_price - entry_price) *
           CONTRACT_MULT * contracts - commission)
    ret_pct = ((total_exit_price - entry_price) / entry_price
               if entry_price != 0 else 0)

    log.info("    CLOSED %s: exit=$%.2f, P&L=$%+,.2f (%+.1f%%), method=%s",
             ticker, total_exit_price, pnl, ret_pct * 100, method)

    return {
        "success": True,
        "exit_price": round(total_exit_price, 4),
        "pnl": round(pnl, 2),
        "return_pct": round(ret_pct, 4),
        "method": method,
        "n_filled": n_legs,
        "n_legs": n_legs,
    }


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
    log.info(f"  Net Liquidation: {info.get('NetLiquidation', 0):>12,.2f} {ccy}")
    log.info(f"  Buying Power:    {info.get('BuyingPower', 0):>12,.2f} {ccy}")
    log.info(f"  Cash:            {info.get('TotalCashValue', 0):>12,.2f} {ccy}")
    log.info(f"  Gross Position:  {info.get('GrossPositionValue', 0):>12,.2f} {ccy}")

    portfolio = load_portfolio()
    active = [p for p in portfolio["positions"] if "exit_date" not in p]
    closed = [p for p in portfolio["positions"] if "exit_date" in p]

    log.info("  Active positions: %d/%d", len(active), MAX_POSITIONS)

    if active:
        total_deployed = sum(p["total_deployed"] for p in active)
        avg_ff = np.mean([p["ff"] for p in active])
        log.info(f"  Total deployed:   ${total_deployed:>12,.2f}")
        log.info("  Avg FF:           %11.1f%%", avg_ff)

        log.info("  %6s  %5s  %6s  %6s  %3s  %7s  %9s  %5s  %10s  %10s",
                 "Ticker", "Combo", "Strike", "Type",
                 "Ctr", "Cost", "Deployed", "FF", "FrontExp", "Entry")
        log.info("  %s", "-" * 85)

        for p in sorted(active, key=lambda x: -x["ff"]):
            log.info("  %6s  %5s  $%5.0f  %6s  %3d  $%6.2f  $%8.2f  %4.1f%%  %10s  %10s",
                     p['ticker'], p['combo'], p['strike'],
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

def sync_portfolio(ib: IB, acct: str) -> dict:
    """Rebuild portfolio.json from actual IBKR option positions.

    Groups option legs by ticker into calendar spreads, computes net cost
    from IBKR avgCost, preserves closed positions from the existing file.
    """
    from collections import defaultdict

    log.info("=" * 60)
    log.info("SYNCING PORTFOLIO WITH IBKR")
    log.info("=" * 60)

    portfolio = load_portfolio()
    ibkr_positions = ib.positions()

    # Keep closed positions from existing portfolio
    closed = [p for p in portfolio["positions"] if "exit_date" in p]

    # Existing open positions — index by ticker to preserve metadata
    existing = {}
    for p in portfolio["positions"]:
        if "exit_date" not in p:
            existing[p["ticker"]] = p

    # Group IBKR option positions by ticker (skip qty=0 and non-OPT)
    by_ticker = defaultdict(list)
    for p in ibkr_positions:
        if p.contract.secType == "OPT" and p.position != 0:
            by_ticker[p.contract.symbol].append(p)

    log.info("  IBKR: %d tickers with live option positions", len(by_ticker))
    log.info("  Portfolio file: %d open, %d closed",
             len(existing), len(closed))

    # Build positions from IBKR data
    synced = []
    added = []
    updated = []
    removed = [t for t in existing if t not in by_ticker]

    now = datetime.now()

    for ticker in sorted(by_ticker.keys()):
        legs = by_ticker[ticker]

        longs = [p for p in legs if p.position > 0]
        shorts = [p for p in legs if p.position < 0]

        calls_long = [p for p in longs if p.contract.right == "C"]
        puts_long = [p for p in longs if p.contract.right == "P"]
        calls_short = [p for p in shorts if p.contract.right == "C"]
        puts_short = [p for p in shorts if p.contract.right == "P"]

        has_call_cal = len(calls_long) > 0 and len(calls_short) > 0
        has_put_cal = len(puts_long) > 0 and len(puts_short) > 0
        is_double = has_call_cal and has_put_cal
        n_legs = len(legs)

        # Expirations
        back_exps = sorted(set(
            p.contract.lastTradeDateOrContractMonth for p in longs
        ))
        front_exps = sorted(set(
            p.contract.lastTradeDateOrContractMonth for p in shorts
        ))
        back_exp = back_exps[0] if back_exps else ""
        front_exp = front_exps[0] if front_exps else ""

        def fmt_exp(e):
            return f"{e[:4]}-{e[4:6]}-{e[6:8]}" if len(e) >= 8 else e

        # Strikes
        call_k = (calls_long[0].contract.strike if calls_long
                  else calls_short[0].contract.strike if calls_short else 0)
        put_k = (puts_long[0].contract.strike if puts_long
                 else puts_short[0].contract.strike if puts_short else call_k)

        # Contracts = max qty across legs
        contracts = max(abs(int(p.position)) for p in legs)

        # Net cost from IBKR avgCost
        total_cost = 0.0
        for p in legs:
            qty = abs(int(p.position))
            if p.position > 0:
                total_cost += p.avgCost * qty
            else:
                total_cost -= p.avgCost * qty
        cost_per_share = round(total_cost / (contracts * CONTRACT_MULT), 2)

        # DTE combo label
        if front_exp and back_exp:
            front_dte = (datetime.strptime(front_exp, "%Y%m%d") - now).days
            back_dte = (datetime.strptime(back_exp, "%Y%m%d") - now).days
            combo = f"{front_dte}-{back_dte}"
        else:
            combo = ""

        # Preserve metadata from existing position if it exists
        old = existing.get(ticker, {})

        pos = {
            "id": old.get("id", f"{ticker}_{combo}_{now.strftime('%Y%m%d')}_SYNC"),
            "ticker": ticker,
            "combo": combo,
            "strike": call_k,
            "put_strike": put_k,
            "spread_type": "double" if is_double else "single",
            "front_exp": fmt_exp(front_exp),
            "back_exp": fmt_exp(back_exp),
            "entry_date": old.get("entry_date", now.strftime("%Y-%m-%d")),
            "contracts": contracts,
            "cost_per_share": cost_per_share,
            "total_deployed": round(total_cost, 2),
            "n_legs": n_legs,
            "ff": old.get("ff", 0.0),
            "execution_method": old.get("execution_method", "market"),
            "slippage": old.get("slippage", 0.0),
        }

        synced.append(pos)

        if ticker in existing:
            old_p = existing[ticker]
            changes = []
            if old_p["contracts"] != contracts:
                changes.append(f"cts {old_p['contracts']}->{contracts}")
            if abs(old_p["cost_per_share"] - cost_per_share) > 0.01:
                changes.append(f"cps ${old_p['cost_per_share']:.2f}->${cost_per_share:.2f}")
            if changes:
                updated.append(f"{ticker}: {', '.join(changes)}")
        else:
            added.append(ticker)

        log.info("  %s: %s %dx K=%g/%g %s/%s cps=$%.2f total=$%.2f",
                 ticker, pos["spread_type"], contracts, call_k, put_k,
                 fmt_exp(front_exp), fmt_exp(back_exp),
                 cost_per_share, total_cost)

    # Merge: synced (active from IBKR) + closed (preserved)
    portfolio["positions"] = synced + closed
    save_portfolio(portfolio)

    if added:
        log.info("  ADDED (%d new from IBKR): %s", len(added), ", ".join(added))
    if updated:
        log.info("  UPDATED (%d):", len(updated))
        for u in updated:
            log.info("    %s", u)
    if removed:
        log.info("  REMOVED (%d not on IBKR): %s", len(removed), ", ".join(removed))

    total_deployed = sum(p["total_deployed"] for p in synced)
    log.info("  Final: %d active, %d closed, $%.2f deployed",
             len(synced), len(closed), total_deployed)

    return {
        "added": added,
        "updated": updated,
        "removed": removed,
        "active_count": len(synced),
        "closed_count": len(closed),
        "total_deployed": round(total_deployed, 2),
    }


def convert_pending_to_market(ib: IB) -> list[dict]:
    """Cancel all open limit orders and replace them with market orders.

    Uses reqAllOpenOrders() to see orders from ALL client IDs.
    Returns a list of dicts describing each conversion result.
    """
    log.info("=" * 60)
    log.info("CONVERTING PENDING ORDERS TO MARKET")
    log.info("=" * 60)

    # Fetch orders from ALL client IDs
    ib.reqAllOpenOrders()
    ib.sleep(2)

    all_trades = ib.trades()
    pending = [
        t for t in all_trades
        if t.orderStatus.status in ("PreSubmitted", "Submitted")
        and t.order.orderType != "MKT"
    ]

    if not pending:
        log.info("  No pending limit orders found.")
        return []

    log.info("  Found %d pending order(s) to convert", len(pending))
    results = []

    for trade in pending:
        contract = trade.contract
        order = trade.order
        action = order.action
        qty = int(order.totalQuantity)
        old_type = order.orderType
        old_limit = getattr(order, "lmtPrice", 0)

        label = getattr(contract, "symbol", str(contract))
        log.info("  %s %s %s x%d — %s $%.2f → MKT (clientId=%d)",
                 action, label, contract.secType, qty,
                 old_type, old_limit, order.clientId)

        # Cancel the existing order
        ib.cancelOrder(order)
        ib.sleep(1)

        # Skip if qty is 0 (ghost order — cancel is enough)
        if qty == 0:
            log.info("    → Cancelled (qty=0, no replacement needed)")
            results.append({
                "contract": label,
                "action": action,
                "qty": qty,
                "old_type": old_type,
                "old_limit": round(old_limit, 2),
                "filled": False,
                "fill_price": 0,
                "status": "Cancelled (qty=0)",
            })
            continue

        # Place market replacement
        mkt_order = MarketOrder(action, qty)
        mkt_order.outsideRth = False
        mkt_order.tif = "DAY"
        new_trade = ib.placeOrder(contract, mkt_order)

        # Wait for fill
        for _ in range(15):
            ib.sleep(1)
            if new_trade.orderStatus.status in ("Filled", "Cancelled", "Inactive"):
                break

        filled = new_trade.orderStatus.status == "Filled"
        fill_px = new_trade.orderStatus.avgFillPrice if filled else 0

        status_str = new_trade.orderStatus.status
        log.info("    → %s%s", status_str,
                 f" @ ${fill_px:.2f}" if filled else "")

        results.append({
            "contract": label,
            "action": action,
            "qty": qty,
            "old_type": old_type,
            "old_limit": round(old_limit, 2),
            "filled": filled,
            "fill_price": round(fill_px, 2),
            "status": status_str,
        })

    n_filled = sum(1 for r in results if r["filled"])
    log.info("  Done: %d/%d converted and filled", n_filled, len(results))
    return results


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
    parser.add_argument("--to-market", action="store_true",
                        help="Convert all pending limit orders to market orders")
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
        elif args.to_market:
            convert_pending_to_market(ib)
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
        # Shut down ThetaData WebSocket if it was used
        try:
            from core.theta_ws import theta_ws_shutdown
            theta_ws_shutdown()
        except Exception:
            pass
        if ib and ib.isConnected():
            ib.disconnect()
            log.info("Disconnected from IBKR")


if __name__ == "__main__":
    main()
