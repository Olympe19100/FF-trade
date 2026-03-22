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

# Force unbuffered output so user sees progress in real time
sys.stdout.reconfigure(line_buffering=True)

# Fix Python 3.14 asyncio event loop before importing ib_insync
try:
    asyncio.get_event_loop()
except RuntimeError:
    asyncio.set_event_loop(asyncio.new_event_loop())

from ib_insync import IB, Stock, Option, Bag, ComboLeg, LimitOrder, MarketOrder, util

# ── Paths ──
ROOT   = Path(r"C:\Users\ANTEC MSI\Desktop\pro\Option trading")
OUTPUT = ROOT / "output"
STATE  = ROOT / "state"
STATE.mkdir(exist_ok=True)

PORTFOLIO_FILE = STATE / "portfolio.json"
TRADES_FILE    = STATE / "trades.json"

# ── IBKR Connection ──
DEFAULT_HOST = "127.0.0.1"
TWS_PAPER    = 7497
GW_PAPER     = 4002
CLIENT_ID    = 3

# ── Strategy parameters (match backtest.py) ──
MAX_POSITIONS    = 20
MAX_CONTRACTS    = 10
DEFAULT_ALLOC    = 0.04      # 4% per name
KELLY_FRAC       = 0.5       # Half Kelly
MIN_KELLY_TRADES = 50
CONTRACT_MULT    = 100
COMMISSION_LEG   = 0.65      # $/leg
SLIPPAGE_BUFFER  = 0.03      # Extra $/share on limit price (Muravyev & Pearson 2020: ~40% of quoted spread)
CLOSE_DAYS       = 1         # Close J-1 before front expiry
FILL_TIMEOUT     = 30        # Seconds to wait per price level
LMT_WALK_STEP    = 0.05      # $/share step when walking limit price
LMT_WALK_MAX     = 10        # Max price walk iterations
LMT_WALK_WAIT    = 15        # Seconds to wait at each price level

# ── Optimal Execution (Muravyev & Pearson 2020, Cont & Kukanov 2013) ──
COMBO_TIMEOUT    = 300       # 5 min combo attempt (CBOE COB)
COMBO_WALK_STEP  = 0.05      # $/share step for combo walk
COMBO_WALK_WAIT  = 30        # Seconds between combo walks
LEG_WALK_STEP    = 0.02      # $/share step for individual leg walk
LEG_WALK_WAIT    = 20        # Seconds between leg walks
LEG_MAX_WALK     = 0.15      # 15% max deviation from mid
LEG_TIMEOUT      = 120       # 2 min per individual leg
OPTIMAL_START_ET = "10:00"   # ET optimal window start (Muravyev & Pearson 2020)
OPTIMAL_END_ET   = "15:00"   # ET optimal window end (avoid first/last 30 min)
ENFORCE_WINDOW   = True      # Block orders outside optimal window


# ═══════════════════════════════════════════════════════════════
#  CONNECTION
# ═══════════════════════════════════════════════════════════════

def connect_ibkr(host=DEFAULT_HOST, port=TWS_PAPER, client_id=CLIENT_ID):
    """Connect to IBKR TWS/Gateway. Returns IB instance."""
    ib = IB()
    print(f"Connecting to IBKR at {host}:{port} ...")
    ib.connect(host, port, clientId=client_id, timeout=10, readonly=False)
    print(f"  Connected: {ib.isConnected()}")
    return ib


def verify_paper(ib):
    """Verify we're on a paper trading account. Returns account ID."""
    accounts = ib.managedAccounts()
    if not accounts:
        raise RuntimeError("No accounts found")
    acct = accounts[0]
    # Paper accounts typically start with 'D' or contain 'PAPER'
    is_paper = acct.startswith("D") or "PAPER" in acct.upper()
    if not is_paper:
        print(f"  WARNING: Account {acct} may be LIVE (not paper).")
        print(f"  Paper accounts usually start with 'D'.")
        resp = input("  Continue anyway? (yes/no): ").strip().lower()
        if resp != "yes":
            raise RuntimeError("Aborted: not a paper account")
    else:
        print(f"  Paper account: {acct}")
    return acct


def get_account_info(ib, acct=""):
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
#  SIGNAL READER
# ═══════════════════════════════════════════════════════════════

def load_latest_signals(top_n=None):
    """Load the most recent scanner signals CSV."""
    # Find latest signals file
    files = sorted(OUTPUT.glob("signals_*.csv"), reverse=True)
    if not files:
        print("  No signal files found in output/")
        return pd.DataFrame()

    latest = files[0]
    print(f"  Loading signals: {latest.name}")
    df = pd.read_csv(str(latest))

    # Sort by FF descending
    df = df.sort_values("ff", ascending=False).reset_index(drop=True)

    if top_n:
        df = df.head(top_n)

    return df


# ═══════════════════════════════════════════════════════════════
#  CONTRACT BUILDER
# ═══════════════════════════════════════════════════════════════

def fmt_exp(date_str):
    """Convert 'YYYY-MM-DD' to 'YYYYMMDD' for IBKR."""
    return str(date_str).replace("-", "")[:8]


def get_ibkr_option_params(ib, ticker):
    """Query IBKR for valid option expirations and strikes.

    Returns (expirations_set, strikes_set) or (None, None) on failure.
    """
    stock = Stock(ticker, "SMART", "USD")
    try:
        ib.qualifyContracts(stock)
    except Exception as ex:
        print(f"    Cannot qualify stock {ticker}: {ex}")
        return None, None

    if stock.conId == 0:
        print(f"    Stock {ticker} not found on IBKR")
        return None, None

    params_list = ib.reqSecDefOptParams(
        stock.symbol, "", stock.secType, stock.conId
    )
    ib.sleep(1)

    if not params_list:
        print(f"    No option params returned for {ticker}")
        return None, None

    # Merge all exchanges — take the one with most strikes (usually SMART)
    best = max(params_list, key=lambda p: len(p.strikes))
    return set(best.expirations), set(best.strikes)


def snap_to_valid(value, valid_set, max_diff=None):
    """Find the closest valid value. Returns None if outside max_diff."""
    if not valid_set:
        return None
    closest = min(valid_set, key=lambda v: abs(float(v) - float(value)))
    if max_diff is not None and abs(float(closest) - float(value)) > max_diff:
        return None
    return closest


def create_calendar_legs(ib, ticker, strike, front_exp, back_exp, double=True):
    """Build individual option contracts for a calendar spread.

    Returns list of (Option, action) tuples ordered BUY-first (no naked short risk),
    plus (n_legs, actual_strike) or ([], 0, None) on failure.

    Execution order: BUY back-month first, then SELL front-month.
    """
    strike = float(strike)
    front_str = fmt_exp(front_exp)
    back_str = fmt_exp(back_exp)

    # Step 1: Get valid expirations and strikes from IBKR
    valid_exps, valid_strikes = get_ibkr_option_params(ib, ticker)
    if valid_exps is None:
        return [], 0, None

    # Step 2: Snap expiration dates
    ibkr_front = snap_to_valid(front_str, valid_exps)
    ibkr_back = snap_to_valid(back_str, valid_exps)

    if ibkr_front is None or ibkr_back is None:
        print(f"    No matching IBKR expirations for {ticker}: "
              f"want {front_str}/{back_str}")
        return [], 0, None

    if ibkr_front == ibkr_back:
        print(f"    Front and back snap to same expiration for {ticker}")
        return [], 0, None

    if ibkr_front != front_str or ibkr_back != back_str:
        print(f"    Snapped exps: {front_str}->{ibkr_front}, {back_str}->{ibkr_back}")

    # Step 3: Snap strike
    max_strike_diff = strike * 0.03
    ibkr_strike = snap_to_valid(strike, valid_strikes, max_diff=max_strike_diff)
    if ibkr_strike is None:
        print(f"    No valid IBKR strike near {strike:.0f} for {ticker}")
        return [], 0, None

    if ibkr_strike != strike:
        print(f"    Snapped strike: {strike:.1f} -> {ibkr_strike}")

    # Step 4: Create and qualify call options
    front_call = Option(ticker, ibkr_front, ibkr_strike, "C", "SMART", "100", "USD")
    back_call  = Option(ticker, ibkr_back,  ibkr_strike, "C", "SMART", "100", "USD")

    try:
        ib.qualifyContracts(front_call, back_call)
    except Exception as ex:
        print(f"    Failed to qualify call options for {ticker}: {ex}")
        return [], 0, None

    if front_call.conId == 0 or back_call.conId == 0:
        print(f"    Could not resolve call contracts for {ticker} "
              f"K={ibkr_strike} {ibkr_front}/{ibkr_back}")
        return [], 0, None

    # BUY first, then SELL (never naked short)
    legs = [
        (back_call,  "BUY"),
        (front_call, "SELL"),
    ]

    # Put legs for double calendar
    if double:
        front_put = Option(ticker, ibkr_front, ibkr_strike, "P", "SMART", "100", "USD")
        back_put  = Option(ticker, ibkr_back,  ibkr_strike, "P", "SMART", "100", "USD")
        try:
            ib.qualifyContracts(front_put, back_put)
            if front_put.conId > 0 and back_put.conId > 0:
                legs.extend([
                    (back_put,  "BUY"),
                    (front_put, "SELL"),
                ])
            else:
                print(f"    Put contracts not found for {ticker}, using single calendar")
        except Exception:
            print(f"    Put qualification failed for {ticker}, using single calendar")

    return legs, len(legs), ibkr_strike


# ═══════════════════════════════════════════════════════════════
#  OPTIMAL EXECUTION (Muravyev & Pearson 2020, Cont & Kukanov 2013)
# ═══════════════════════════════════════════════════════════════

def check_optimal_window():
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


def create_calendar_combo(legs):
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


def get_combo_mid_price(ib, combo):
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


def get_leg_mid_price(ib, option_contract):
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


def execute_combo_order(ib, combo, contracts, eodhd_mid):
    """Try to fill a combo order with price walking.

    Strategy (ORATS methodology, 56% spread slippage):
      1. Start LMT at mid
      2. Walk toward ask by COMBO_WALK_STEP every COMBO_WALK_WAIT seconds
      3. Give up after COMBO_TIMEOUT

    Returns (filled: bool, fill_price: float, slippage: float).
    """
    bid, ask, mid = get_combo_mid_price(ib, combo)

    if mid <= 0:
        print(f"      Combo: no market data (bid={bid}, ask={ask}), skipping")
        return False, 0.0, 0.0

    # Calendar spread is a debit: we BUY the combo
    limit_price = round(mid, 2)
    max_price = round(ask, 2) if ask > 0 else round(mid * 1.15, 2)

    print(f"      Combo: bid={bid:.2f} ask={ask:.2f} mid={mid:.2f}, "
          f"LMT start @ {limit_price:.2f}")

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
                print(f"      Combo FILLED @ ${fill_px:.2f} "
                      f"(slip={slippage:+.2f} vs EODHD ${eodhd_mid:.2f})")
                return True, fill_px, slippage
            elif status in ("Cancelled", "ApiCancelled", "Inactive"):
                print(f"      Combo rejected: {status}")
                return False, 0.0, 0.0

        # Not filled — cancel and walk price
        ib.cancelOrder(order)
        ib.sleep(2)

        new_price = round(limit_price + COMBO_WALK_STEP, 2)
        if new_price > max_price:
            print(f"      Combo: hit ceiling ${max_price:.2f}, giving up")
            break

        limit_price = new_price
        print(f"      Combo: walk -> ${limit_price:.2f} "
              f"(step {walk+1}/{n_walks})", flush=True)

    print(f"      Combo: not filled after {COMBO_TIMEOUT}s")
    return False, 0.0, 0.0


def execute_leg_order(ib, leg_contract, action, contracts, eodhd_mid=0):
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
        print(f"        No live data, using EODHD mid=${eodhd_mid:.2f}")
        mid = eodhd_mid
        bid = eodhd_mid * 0.90
        ask = eodhd_mid * 1.10

    if mid <= 0:
        print(f"        No price data at all, using MKT")
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
    print(f"      {action} {right_label} {exp_label} "
          f"K={leg_contract.strike:.0f} x{contracts} "
          f"LMT ${limit_price:.2f} (bid={bid:.2f} ask={ask:.2f})",
          end="", flush=True)

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
                print(f" -> FILLED @ ${fill_px:.2f}")
                return True, fill_px
            elif status in ("Cancelled", "ApiCancelled", "Inactive"):
                print(f" -> REJECTED ({status})")
                return False, 0.0

        if elapsed >= LEG_TIMEOUT:
            break

        # Cancel and walk
        ib.cancelOrder(order)
        ib.sleep(1)
        elapsed += 1

        new_price = round(limit_price + walk_dir, 2)
        if action == "BUY" and new_price > max_limit:
            print(f" -> hit ceiling ${max_limit:.2f}")
            break
        elif action == "SELL" and new_price < max_limit:
            print(f" -> hit floor ${max_limit:.2f}")
            break

        limit_price = new_price
        print(f" ${limit_price:.2f}", end="", flush=True)

    # Final cancel
    if order:
        try:
            ib.cancelOrder(order)
            ib.sleep(1)
        except Exception:
            pass

    print(f" -> NOT FILLED ({elapsed}s)")
    return False, 0.0


def execute_spread_optimal(ib, ticker, legs, n_legs, contracts,
                           eodhd_cps, spread_type):
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
    print(f"    {ticker}: Step A — Combo ({n_legs} legs as BAG)")
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
            print(f"    {ticker}: Combo failed -> fallback to individual legs")
    else:
        print(f"    {ticker}: Cannot build combo -> individual legs")

    # ── Step B: Individual legs (BUY back-month first) ──
    print(f"    {ticker}: Step B — Individual legs (LMT + walk)")
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
            print(f"    {ticker}: Leg failed, stopping (no naked short risk)")
            break

    if len(filled_legs) == n_legs:
        slippage = total_fill_cost - eodhd_cps
        print(f"    {ticker}: ALL {n_legs} LEGS FILLED, "
              f"net=${total_fill_cost:.2f}/sh "
              f"(slip={slippage:+.2f} vs EODHD ${eodhd_cps:.2f})")
        return "full", total_fill_cost, slippage, details
    elif filled_legs:
        print(f"    {ticker}: PARTIAL {len(filled_legs)}/{n_legs} legs. "
              f"Manual cleanup needed.")
        return "partial", total_fill_cost, 0.0, details
    else:
        return "failed", 0.0, 0.0, details


# ═══════════════════════════════════════════════════════════════
#  POSITION SIZING — Half Kelly (f/2), same as backtest.py
# ═══════════════════════════════════════════════════════════════
#
# Kelly fractionnaire a 1/2 : f = 0.5 * mu / var
# Minimise la variance du portefeuille global.
# Bootstrap from backtest trades (347 trades), then walk-forward
# with live trades as they accumulate.

BACKTEST_TRADES_FILE = OUTPUT / "backtest_trades.csv"


def load_trade_history():
    """Load return history: backtest + live trades for Kelly."""
    returns = []

    # 1. Bootstrap from backtest trades (347 trades, 2016-2025)
    if BACKTEST_TRADES_FILE.exists():
        try:
            bt = pd.read_csv(str(BACKTEST_TRADES_FILE))
            if "return_pct" in bt.columns:
                returns.extend(bt["return_pct"].dropna().tolist())
        except Exception:
            pass

    # 2. Append live trades (walk-forward)
    if TRADES_FILE.exists():
        try:
            with open(TRADES_FILE) as f:
                data = json.load(f)
            for t in data.get("trades", []):
                if t.get("return_pct") is not None:
                    returns.append(t["return_pct"])
        except Exception:
            pass

    return returns


def compute_kelly(returns):
    """Compute Half Kelly fraction from trade returns.

    f = 0.5 * mu / var  (fractionnaire a 1/2)
    Same formula as backtest.py lines 154-163.
    """
    if len(returns) < MIN_KELLY_TRADES:
        return DEFAULT_ALLOC

    arr = np.array(returns)
    mu = arr.mean()
    var = arr.var()

    if var > 0 and mu > 0:
        return min(KELLY_FRAC * mu / var, 1.0)
    return DEFAULT_ALLOC


def cost_per_contract(cost_per_share, n_legs=4):
    """Cost per contract including slippage + commission."""
    return (cost_per_share + SLIPPAGE_BUFFER) * CONTRACT_MULT + COMMISSION_LEG * n_legs


def size_portfolio(signals_info, kelly_f, account_value):
    """Two-pass sizing: guarantee all positions get 1 contract, then add Kelly extras.

    Pass 1: Reserve 1 contract per position (minimum deployment).
    Pass 2: Distribute remaining Kelly budget as extra contracts
            (cheapest positions get extras first).

    Args:
        signals_info: list of (ticker, cost_per_share, n_legs) for each position
        kelly_f: Kelly fraction (e.g. 0.041)
        account_value: total account value

    Returns:
        list of (ticker, contracts, deployed) tuples
    """
    n_pos = len(signals_info)
    if n_pos == 0:
        return []

    kelly_target = kelly_f * account_value

    # Pass 1: 1 contract per position (guaranteed entry)
    sizing = []
    for ticker, cps, n_legs in signals_info:
        cpc = cost_per_contract(cps, n_legs)
        if cpc <= 0:
            sizing.append((ticker, 0, cpc))
            continue
        sizing.append((ticker, 1, cpc))

    # Pass 2: distribute remaining budget as extra contracts
    min_deployed = sum(n * cpc for _, n, cpc in sizing)
    extra_budget = max(0, kelly_target - min_deployed)

    if extra_budget > 0:
        # Sort by cheapest cpc first — fill cheapest positions first
        order = sorted(range(len(sizing)), key=lambda i: sizing[i][2])
        for idx in order:
            ticker, n, cpc = sizing[idx]
            if cpc <= 0 or n >= MAX_CONTRACTS:
                continue
            add = min(int(extra_budget / cpc), MAX_CONTRACTS - n)
            if add > 0:
                sizing[idx] = (ticker, n + add, cpc)
                extra_budget -= add * cpc
            if extra_budget <= 0:
                break

    result = []
    for ticker, n, cpc in sizing:
        result.append((ticker, n, n * cpc))
    return result


# ═══════════════════════════════════════════════════════════════
#  STATE MANAGEMENT
# ═══════════════════════════════════════════════════════════════

def load_portfolio():
    """Load active positions from state file."""
    if PORTFOLIO_FILE.exists():
        with open(PORTFOLIO_FILE) as f:
            return json.load(f)
    return {"positions": [], "last_updated": None}


def save_portfolio(portfolio):
    """Save positions to state file."""
    portfolio["last_updated"] = datetime.now().isoformat()
    with open(PORTFOLIO_FILE, "w") as f:
        json.dump(portfolio, f, indent=2)


def add_position(portfolio, ticker, combo, strike, front_exp, back_exp,
                 contracts, cost_per_share, spread_type, ff, n_legs):
    """Add a new position to portfolio state."""
    commission = n_legs * COMMISSION_LEG * contracts
    total_cost = cost_per_share * CONTRACT_MULT * contracts + commission

    pos = {
        "id": f"{ticker}_{combo}_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
        "ticker": ticker,
        "combo": combo,
        "strike": float(strike),
        "spread_type": spread_type,
        "front_exp": str(front_exp),
        "back_exp": str(back_exp),
        "entry_date": datetime.now().strftime("%Y-%m-%d"),
        "contracts": contracts,
        "cost_per_share": float(cost_per_share),
        "total_deployed": round(total_cost, 2),
        "n_legs": n_legs,
        "ff": float(ff),
    }
    portfolio["positions"].append(pos)
    return pos


def record_trade(position, exit_price, pnl, return_pct):
    """Append closed trade to trades.json."""
    if TRADES_FILE.exists():
        with open(TRADES_FILE) as f:
            data = json.load(f)
    else:
        data = {"trades": []}

    data["trades"].append({
        "id": position["id"],
        "ticker": position["ticker"],
        "combo": position["combo"],
        "entry_date": position["entry_date"],
        "exit_date": datetime.now().strftime("%Y-%m-%d"),
        "contracts": position["contracts"],
        "cost_per_share": position["cost_per_share"],
        "exit_price": round(exit_price, 4),
        "pnl": round(pnl, 2),
        "return_pct": round(return_pct, 4),
        "ff": position["ff"],
    })

    with open(TRADES_FILE, "w") as f:
        json.dump(data, f, indent=2)


# ═══════════════════════════════════════════════════════════════
#  ENTER NEW POSITIONS (--enter)
# ═══════════════════════════════════════════════════════════════

def enter_new_positions(ib, acct, max_new=None):
    """Place new calendar spread orders from scanner signals.

    Two-pass approach:
      Pass 1: Qualify contracts on IBKR (identify valid positions)
      Pass 2: Size all positions globally via Kelly to hit target allocation
      Pass 3: Place orders
    """
    print("\n" + "=" * 60)
    print("ENTERING NEW POSITIONS")
    print("=" * 60)

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
        print(f"  Portfolio full: {n_active}/{MAX_POSITIONS} positions")
        return

    # Account info
    info = get_account_info(ib, acct)
    account_value = info.get("NetLiquidation", 0)
    buying_power = info.get("BuyingPower", 0)

    # Walk-forward Kelly (bootstrap backtest + live trades)
    returns = load_trade_history()
    kelly_f = compute_kelly(returns)
    kelly_target = kelly_f * account_value

    print(f"  Active:        {n_active}/{MAX_POSITIONS}")
    print(f"  Slots:         {slots}")
    print(f"  Signals:       {len(signals)}")
    print(f"  Account:       ${account_value:,.0f}")
    print(f"  Buying power:  ${buying_power:,.0f}")
    print(f"  Kelly history: {len(returns)} trades")
    print(f"  Half Kelly f:  {kelly_f:.4f} ({kelly_f:.1%})")
    print(f"  Kelly target:  ${kelly_target:,.0f} (f * W)")

    # ── PASS 1: Qualify all contracts (individual legs) ──
    print(f"\n  --- PASS 1: Qualifying contracts on IBKR ---")
    qualified = []  # (sig, legs, n_legs, actual_strike, cost_per_share, spread_type)
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

        print(f"    {ticker} {sig['combo']} K={sig['strike']:.0f} "
              f"FF={sig['ff']:.1f}% ${cps:.2f} ({spread_type}) ... ", end="")

        legs, n_legs, actual_strike = create_calendar_legs(
            ib, ticker, sig["strike"],
            sig["front_exp"], sig["back_exp"],
            double=has_double
        )
        if not legs:
            print("SKIP")
            skipped += 1
            continue

        print("OK")
        qualified.append((sig, legs, n_legs, actual_strike, cps, spread_type))

    print(f"  Qualified: {len(qualified)}/{len(qualified)+skipped} "
          f"({skipped} skipped)")

    if not qualified:
        return

    # ── PASS 2: Global Kelly sizing ──
    print(f"\n  --- PASS 2: Kelly sizing ({len(qualified)} positions) ---")
    signals_info = [(q[0]["ticker"], q[4], q[2]) for q in qualified]
    sizing = size_portfolio(signals_info, kelly_f, account_value)

    total_deployed = sum(d for _, _, d in sizing)
    print(f"  Kelly target:  ${kelly_target:,.0f}")
    print(f"  Total sized:   ${total_deployed:,.0f}")
    print(f"  Gap:           ${kelly_target - total_deployed:+,.0f} "
          f"({(total_deployed/kelly_target - 1)*100:+.1f}%)")

    print(f"\n  {'Ticker':>6s} {'FF%':>6s} {'Cost':>6s} {'Ctr':>4s} {'Deployed':>9s}")
    print(f"  {'-'*38}")
    for (sig, *_rest), (ticker, n_ctr, deployed) in zip(qualified, sizing):
        print(f"  {ticker:>6s} {sig['ff']:>5.1f}% ${_rest[3]:>5.2f} {n_ctr:>4d} ${deployed:>8,.0f}")

    # ── Time-of-day check ──
    is_optimal, time_msg = check_optimal_window()
    print(f"\n  {time_msg}")
    if ENFORCE_WINDOW and not is_optimal:
        print(f"  Orders blocked — retry between {OPTIMAL_START_ET}-{OPTIMAL_END_ET} ET.")
        return

    # ── PASS 3: Optimal execution (combo-first, legs-fallback) ──
    print(f"\n  --- PASS 3: Optimal execution ({len(qualified)} spreads) ---")
    print(f"  Strategy: combo LMT first ({COMBO_TIMEOUT}s) -> "
          f"individual legs LMT+walk ({LEG_TIMEOUT}s/leg)")
    entered = 0
    partial = 0
    not_filled = 0
    slippage_log = []

    for (sig, legs, n_legs, actual_strike, cps, spread_type), \
        (ticker, contracts, deployed) in zip(qualified, sizing):
        if contracts <= 0:
            continue

        if deployed > buying_power * 0.9:
            print(f"    {ticker}: SKIP (${deployed:,.0f} > buying power)")
            continue

        print(f"\n    {ticker}: {contracts}x {spread_type} "
              f"({n_legs} legs, EODHD mid=${cps:.2f})")

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
                contracts, fill_cost, spread_type, sig["ff"], n_legs
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
        print(f"\n  {'='*55}")
        print(f"  SLIPPAGE REPORT")
        print(f"  {'Ticker':>6s} {'Method':>6s} {'EODHD':>7s} "
              f"{'Fill':>7s} {'Slip':>7s} {'Result':>8s}")
        print(f"  {'-'*55}")
        for s in slippage_log:
            print(f"  {s['ticker']:>6s} {(s['method'] or '-'):>6s} "
                  f"${s['eodhd_mid']:>6.2f} ${s['fill_cost']:>6.2f} "
                  f"${s['slippage']:>+6.2f} {s['result']:>8s}")

        filled_slips = [s["slippage"] for s in slippage_log
                        if s["result"] == "full"]
        if filled_slips:
            avg_slip = np.mean(filled_slips)
            print(f"\n  Avg slippage: ${avg_slip:+.3f}/sh "
                  f"({len(filled_slips)} fills)")

    print(f"\n  {'='*40}")
    print(f"  SUMMARY")
    print(f"  Kelly f:       {kelly_f:.4f} ({kelly_f:.1%})")
    print(f"  Kelly target:  ${kelly_target:,.0f}")
    print(f"  Total sized:   ${total_deployed:,.0f}")
    print(f"  Full fills:    {entered}/{len(qualified)}")
    print(f"  Partial fills: {partial}")
    print(f"  Not filled:    {not_filled}")
    print(f"  Skipped IBKR:  {skipped}")
    print(f"  Total active:  {n_active + entered}/{MAX_POSITIONS}")

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

def close_expiring_positions(ib, acct):
    """Close positions where front expiry is within CLOSE_DAYS."""
    print("\n" + "=" * 60)
    print("CLOSING EXPIRING POSITIONS")
    print("=" * 60)

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
        print(f"  No positions to close (next expiry not within {CLOSE_DAYS} days)")
        for pos in active:
            front_exp = pd.Timestamp(pos["front_exp"])
            days_left = (front_exp - pd.Timestamp(today)).days
            print(f"    {pos['ticker']} {pos['combo']}: "
                  f"front exp {pos['front_exp']} ({days_left}d)")
        return

    print(f"  Positions to close: {len(to_close)}")

    # ── Time-of-day check for closing ──
    is_optimal, time_msg = check_optimal_window()
    print(f"  {time_msg}")
    if ENFORCE_WINDOW and not is_optimal:
        print(f"  Close orders blocked — retry between {OPTIMAL_START_ET}-{OPTIMAL_END_ET} ET.")
        return

    for pos in to_close:
        ticker = pos["ticker"]
        contracts = pos["contracts"]
        print(f"\n  Closing {ticker} {pos['combo']} K={pos['strike']:.0f} "
              f"x{contracts} ({pos['spread_type']})")

        # Rebuild individual leg contracts
        is_double = pos["spread_type"] == "double"
        legs, n_legs, _ = create_calendar_legs(
            ib, ticker, pos["strike"],
            pos["front_exp"], pos["back_exp"],
            double=is_double
        )
        if not legs:
            print(f"    ERROR: could not rebuild contract, manual close needed")
            continue

        # Close = reverse the entry actions (BUY->SELL, SELL->BUY)
        # Close SELL (short front) first, then close BUY (long back)
        close_legs = []
        for leg_contract, entry_action in reversed(legs):
            close_action = "SELL" if entry_action == "BUY" else "BUY"
            close_legs.append((leg_contract, close_action))

        # Try combo close first, then fallback to individual legs
        print(f"    Closing {n_legs} legs (combo-first, legs-fallback):")
        total_exit_price = 0.0
        close_success = False

        # Step A: Try combo close
        combo = create_calendar_combo(close_legs)
        if combo is not None:
            bid, ask, mid = get_combo_mid_price(ib, combo)
            if mid > 0:
                print(f"      Combo close: bid={bid:.2f} ask={ask:.2f} mid={mid:.2f}")
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
                        print(f"      Combo close FILLED @ ${total_exit_price:.2f}")
                        break
                    elif trade.orderStatus.status in ("Cancelled", "ApiCancelled", "Inactive"):
                        break
                if not close_success:
                    ib.cancelOrder(order)
                    ib.sleep(2)
                    print(f"      Combo close failed, fallback to legs")

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

            print(f"    CLOSED net=${exit_price:.2f}/sh, "
                  f"P&L=${pnl:+,.2f} ({ret_pct:+.1%})")

            pos["exit_date"] = today.strftime("%Y-%m-%d")
            pos["exit_price"] = exit_price
            pos["pnl"] = round(pnl, 2)
            record_trade(pos, exit_price, pnl, ret_pct)
        else:
            print(f"    PARTIAL CLOSE: {len(filled_legs)}/{n_legs} legs.")
            print(f"    Manual cleanup needed for remaining legs.")

    save_portfolio(portfolio)


# ═══════════════════════════════════════════════════════════════
#  STATUS DISPLAY
# ═══════════════════════════════════════════════════════════════

def show_basic_status(ib, acct):
    """Show portfolio overview."""
    print("\n" + "=" * 60)
    print("PORTFOLIO STATUS")
    print("=" * 60)

    info = get_account_info(ib, acct)
    ccy = info.get("base_currency", "USD")
    print(f"  Account:         {acct} ({ccy})")
    print(f"  Net Liquidation: {info.get('NetLiquidation', 0):>12,.2f} {ccy}")
    print(f"  Buying Power:    {info.get('BuyingPower', 0):>12,.2f} {ccy}")
    print(f"  Cash:            {info.get('TotalCashValue', 0):>12,.2f} {ccy}")
    print(f"  Gross Position:  {info.get('GrossPositionValue', 0):>12,.2f} {ccy}")

    portfolio = load_portfolio()
    active = [p for p in portfolio["positions"] if "exit_date" not in p]
    closed = [p for p in portfolio["positions"] if "exit_date" in p]

    print(f"\n  Active positions: {len(active)}/{MAX_POSITIONS}")

    if active:
        total_deployed = sum(p["total_deployed"] for p in active)
        avg_ff = np.mean([p["ff"] for p in active])
        print(f"  Total deployed:   ${total_deployed:>12,.2f}")
        print(f"  Avg FF:           {avg_ff:>11.1f}%")

        print(f"\n  {'Ticker':>6s}  {'Combo':>5s}  {'K':>6s}  {'Type':>6s}  "
              f"{'Ctr':>3s}  {'Cost':>7s}  {'Deployed':>9s}  {'FF':>5s}  "
              f"{'FrontExp':>10s}  {'Entry':>10s}")
        print("  " + "-" * 85)

        for p in sorted(active, key=lambda x: -x["ff"]):
            print(f"  {p['ticker']:>6s}  {p['combo']:>5s}  "
                  f"${p['strike']:>5.0f}  {p['spread_type']:>6s}  "
                  f"{p['contracts']:>3d}  ${p['cost_per_share']:>6.2f}  "
                  f"${p['total_deployed']:>8.2f}  {p['ff']:>4.1f}%  "
                  f"{p['front_exp']:>10s}  {p['entry_date']:>10s}")

    if closed:
        total_pnl = sum(p.get("pnl", 0) for p in closed)
        n_wins = sum(1 for p in closed if p.get("pnl", 0) > 0)
        wr = n_wins / len(closed) * 100 if closed else 0
        print(f"\n  Closed trades: {len(closed)}")
        print(f"  Total P&L:     ${total_pnl:>+12,.2f}")
        print(f"  Win rate:      {wr:>11.1f}%")


def show_detailed_status(ib, acct):
    """Show detailed status with live P&L from IBKR."""
    show_basic_status(ib, acct)

    # Show IBKR portfolio items
    items = ib.portfolio(acct)
    if items:
        print(f"\n  IBKR Live Positions:")
        print(f"  {'Symbol':>8s}  {'SecType':>7s}  {'Pos':>5s}  "
              f"{'MktPrice':>9s}  {'MktValue':>10s}  {'UnrlzPnL':>10s}")
        print("  " + "-" * 65)
        for item in items:
            c = item.contract
            print(f"  {c.symbol:>8s}  {c.secType:>7s}  {item.position:>5.0f}  "
                  f"${item.marketPrice:>8.2f}  ${item.marketValue:>9.2f}  "
                  f"${item.unrealizedPNL:>9.2f}")

    # Trade history
    if TRADES_FILE.exists():
        with open(TRADES_FILE) as f:
            data = json.load(f)
        trades = data.get("trades", [])
        if trades:
            print(f"\n  Trade History (last 10):")
            print(f"  {'Ticker':>6s}  {'Combo':>5s}  {'Entry':>10s}  {'Exit':>10s}  "
                  f"{'Ctr':>3s}  {'P&L':>9s}  {'Ret%':>7s}  {'FF':>5s}")
            print("  " + "-" * 70)
            for t in trades[-10:]:
                print(f"  {t['ticker']:>6s}  {t['combo']:>5s}  "
                      f"{t['entry_date']:>10s}  {t['exit_date']:>10s}  "
                      f"{t['contracts']:>3d}  ${t['pnl']:>+8.2f}  "
                      f"{t['return_pct']:>+6.1%}  {t['ff']:>4.1f}%")


# ═══════════════════════════════════════════════════════════════
#  SYNC PORTFOLIO WITH IBKR (--sync)
# ═══════════════════════════════════════════════════════════════

def sync_portfolio(ib, acct):
    """Reconcile portfolio.json with actual IBKR positions.

    - Removes positions not on IBKR (orders that were rejected/cancelled)
    - Updates cost with actual fill prices
    """
    print("\n" + "=" * 60)
    print("SYNCING PORTFOLIO WITH IBKR")
    print("=" * 60)

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

    print(f"  IBKR positions: {len(ibkr_tickers)} tickers")
    print(f"  Portfolio file: {len(portfolio['positions'])} entries")

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
        print(f"\n  REMOVED ({len(removed)} not on IBKR):")
        for t in removed:
            print(f"    {t}")

    if updated:
        print(f"\n  UPDATED ({len(updated)} cost corrections):")
        for u in updated:
            print(f"    {u}")

    total_deployed = sum(p["total_deployed"] for p in kept)
    print(f"\n  Final: {len(kept)} positions, ${total_deployed:,.2f} deployed")


# ═══════════════════════════════════════════════════════════════
#  MAIN / CLI
# ═══════════════════════════════════════════════════════════════

def main():
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
        print(f"\nERROR: Cannot connect to IBKR ({type(ex).__name__}).")
        print("Make sure TWS or IB Gateway is running with API enabled:")
        print("  1. Open TWS or IB Gateway")
        print("  2. Login to PAPER TRADING account")
        print(f"  3. Enable API on port {args.port}:")
        print("     Edit -> Global Configuration -> API -> Settings")
        print("     [x] Enable ActiveX and Socket Clients")
        print(f"     Socket port: {args.port}")
    except KeyboardInterrupt:
        print("\nInterrupted")
    except Exception as ex:
        print(f"\nERROR: {ex}")
        import traceback
        traceback.print_exc()
    finally:
        if ib and ib.isConnected():
            ib.disconnect()
            print("\nDisconnected from IBKR")


if __name__ == "__main__":
    main()
