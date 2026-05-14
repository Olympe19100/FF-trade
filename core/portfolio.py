"""
Portfolio State Management — Load, Save, Size, Record

Extracted from trader.py. No IBKR dependency — pure JSON/CSV file operations.
Can be imported by autopilot.py, app.py, etc. without pulling in ib_insync.
"""

import json
import numpy as np
import pandas as pd
from datetime import datetime

from core.config import (
    STATE, OUTPUT, CACHE, PORTFOLIO_FILE, TRADES_FILE, BACKTEST_TRADES_FILE,
    PENDING_SIGNALS_FILE, MAX_PENDING_AGE_DAYS, MIN_FRONT_DTE_PENDING,
    DEFAULT_ALLOC,
    KELLY_FRAC, MIN_KELLY_TRADES, CONTRACT_MULT,
    COMMISSION_LEG, SLIPPAGE_PER_LEG,
    MAX_PCT_OF_OI, OI_FULL_SIZE,
    USE_ALMGREN_SIZING,
    get_logger,
)

log = get_logger(__name__)


# ═══════════════════════════════════════════════════════════════
#  SIGNAL READER
# ═══════════════════════════════════════════════════════════════

def load_latest_signals(top_n: int | None = None) -> pd.DataFrame:
    """Load the most recent scanner signals CSV."""
    files = sorted(OUTPUT.glob("signals_*.csv"), reverse=True)
    if not files:
        log.info("No signal files found in output/")
        return pd.DataFrame()

    latest = files[0]
    log.info("Loading signals: %s", latest.name)
    df = pd.read_csv(str(latest))
    df = df.sort_values("ff", ascending=False).reset_index(drop=True)

    if top_n:
        df = df.head(top_n)

    return df


# ═══════════════════════════════════════════════════════════════
#  POSITION SIZING — Half Kelly (f/2), same as backtest.py
# ═══════════════════════════════════════════════════════════════

def load_trade_history() -> list[float]:
    """Load return history: backtest + live trades for Kelly."""
    returns: list[float] = []

    # 1. Bootstrap from backtest trades
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


def compute_kelly(returns: list[float]) -> float:
    """Compute Half Kelly fraction from trade returns.

    f = 0.5 * mu / var  (fractionnaire a 1/2)
    """
    if len(returns) < MIN_KELLY_TRADES:
        return DEFAULT_ALLOC

    arr = np.array(returns)
    mu = arr.mean()
    var = arr.var()

    if var > 0 and mu > 0:
        return min(KELLY_FRAC * mu / var, 1.0)
    return DEFAULT_ALLOC


def _get_daily_volume(ticker: str) -> int:
    """Read daily option volume from cache/daily_volumes.pkl.

    Returns 0 if data unavailable.
    """
    vol_file = CACHE / "daily_volumes.pkl"
    if not vol_file.exists():
        return 0
    try:
        import pickle
        with open(vol_file, "rb") as f:
            volumes = pickle.load(f)
        return int(volumes.get(ticker, 0))
    except Exception:
        return 0


def estimate_slippage(mid: float, contracts: int, daily_volume: int,
                      underlying_vol: float) -> float:
    """Dynamic slippage model (Almgren et al. 2005).

    Formula: mid × (contracts/volume)^0.6 × underlying_vol
    Clamped to [0.005, 10% of mid].

    Falls back to SLIPPAGE_PER_LEG if inputs are missing/invalid.
    """
    if mid <= 0 or contracts <= 0 or daily_volume <= 0 or underlying_vol <= 0:
        return SLIPPAGE_PER_LEG

    participation = contracts / daily_volume
    slip = mid * (participation ** 0.6) * underlying_vol
    # Clamp
    slip = max(0.005, min(slip, mid * 0.10))
    return round(slip, 4)


def cost_per_contract(cost_per_share: float, n_legs: int = 2,
                      slippage_override: float | None = None) -> float:
    """Cost per contract including slippage + commission (matches backtest.py).

    Args:
        slippage_override: if provided, use this instead of SLIPPAGE_PER_LEG * n_legs.
    """
    if slippage_override is not None:
        slippage = slippage_override
    else:
        slippage = SLIPPAGE_PER_LEG * n_legs
    return (cost_per_share + slippage) * CONTRACT_MULT + COMMISSION_LEG * n_legs


def size_portfolio(
    signals_info: list[tuple[str, float, int]],
    kelly_f: float,
    account_value: float,
) -> list[tuple[str, int, float]]:
    """Liquidity-weighted sizing: budget proportional to OI, hard-capped at 10% OI.

    Names with OI >= OI_FULL_SIZE (1000) get full Kelly weight.
    Names with lower OI get proportionally less budget.
    Hard cap: never exceed 10% of the least-liquid leg's OI.

    Args:
        signals_info: list of (ticker, cost_per_share, n_legs[, ff, ba_pct[, min_leg_oi]])
                      ff, ba_pct, min_leg_oi are optional (backward compatible).
        kelly_f: Kelly fraction (e.g. 0.041)
        account_value: total account value

    Returns:
        list of (ticker, contracts, deployed) tuples
    """
    n_pos = len(signals_info)
    if n_pos == 0:
        return []

    kelly_target = kelly_f * account_value

    # Parse optional fields from variable-length tuples
    # Support 3-tuple (ticker, cps, n_legs), 5-tuple (..., ff, ba_pct), 6-tuple (..., min_leg_oi)
    parsed: list[tuple[str, float, int, float, float, int]] = []
    for item in signals_info:
        ticker, cps, n_legs = item[0], item[1], item[2]
        ff = float(item[3]) if len(item) > 3 else 1.0
        ba_pct = float(item[4]) if len(item) > 4 else 0.0
        min_leg_oi = int(item[5]) if len(item) > 5 else 0
        parsed.append((ticker, cps, n_legs, ff, ba_pct, min_leg_oi))

    # ── Liquidity-weighted allocation ──
    # Weight = ff * liquidity_factor * (1 - ba_pct)
    # liquidity_factor: scales linearly from 0 to 1 based on OI vs OI_FULL_SIZE
    #   OI >= 1000 → 1.0 (full weight)
    #   OI = 500   → 0.5
    #   OI = 100   → 0.1
    #   OI = 0     → 1.0 (unknown OI = no penalty, rely on hard cap)
    weights: list[float] = []
    for ticker, cps, n_legs, ff, ba_pct, min_leg_oi in parsed:
        if min_leg_oi > 0:
            liq_factor = min(1.0, min_leg_oi / OI_FULL_SIZE)
        else:
            liq_factor = 1.0  # OI unknown — no discount, hard cap will handle

        ba_discount = max(0.3, 1.0 - ba_pct)  # wide BA → less allocation
        w = max(ff, 0.01) * liq_factor * ba_discount
        weights.append(w)

    total_weight = sum(weights)
    if total_weight <= 0:
        total_weight = 1.0

    # Size each position: budget proportional to weight, min 1 contract
    sizing: list[tuple[str, int, float]] = []
    for i, (ticker, cps, n_legs, ff, ba_pct, min_leg_oi) in enumerate(parsed):
        # ── Almgren slippage integration ──
        slippage_override = None
        if USE_ALMGREN_SIZING:
            daily_vol = _get_daily_volume(ticker)
            if daily_vol > 0 and cps > 0:
                # Rough initial estimate: use Kelly target / cpc as contract count
                est_n = max(1, int((weights[i] / total_weight) * kelly_target
                                   / max(cost_per_contract(cps, n_legs), 1)))
                almgren_slip = estimate_slippage(
                    mid=cps, contracts=est_n, daily_volume=daily_vol,
                    underlying_vol=0.30  # default
                )
                static_slip = SLIPPAGE_PER_LEG * n_legs
                log.info("  Almgren slip=$%.4f vs static=$%.4f for %s",
                         almgren_slip, static_slip, ticker)
                slippage_override = almgren_slip

        cpc = cost_per_contract(cps, n_legs, slippage_override=slippage_override)
        if cpc <= 0:
            sizing.append((ticker, 0, cpc))
            continue

        budget_i = (weights[i] / total_weight) * kelly_target
        n = max(1, int(budget_i / cpc))

        # Hard cap by OI: max 5% of the least-liquid leg (skip if OI=0)
        if min_leg_oi > 0:
            oi_cap = max(1, int(min_leg_oi * MAX_PCT_OF_OI))
            if n > oi_cap:
                log.info("  %s: OI cap %d -> %d (min_leg_oi=%d, liq_w=%.2f)",
                         ticker, n, oi_cap, min_leg_oi, weights[i])
                n = oi_cap

        sizing.append((ticker, n, cpc))

    result: list[tuple[str, int, float]] = []
    for ticker, n, cpc in sizing:
        result.append((ticker, n, n * cpc))
    return result


# ═══════════════════════════════════════════════════════════════
#  STATE MANAGEMENT
# ═══════════════════════════════════════════════════════════════

def load_portfolio() -> dict:
    """Load active positions from state file."""
    if PORTFOLIO_FILE.exists():
        with open(PORTFOLIO_FILE) as f:
            return json.load(f)
    return {"positions": [], "last_updated": None}


def save_portfolio(portfolio: dict) -> None:
    """Save positions to state file."""
    portfolio["last_updated"] = datetime.now().isoformat()
    with open(PORTFOLIO_FILE, "w") as f:
        json.dump(portfolio, f, indent=2)


def add_position(
    portfolio: dict,
    ticker: str,
    combo: str,
    strike: float,
    front_exp: str,
    back_exp: str,
    contracts: int,
    cost_per_share: float,
    spread_type: str,
    ff: float,
    n_legs: int,
    put_strike: float | None = None,
    min_leg_oi: int = 0,
) -> dict:
    """Add a new position to portfolio state."""
    slippage = SLIPPAGE_PER_LEG * n_legs  # Entry slippage per share
    commission = n_legs * COMMISSION_LEG * contracts
    total_cost = (cost_per_share + slippage) * CONTRACT_MULT * contracts + commission

    pos = {
        "id": f"{ticker}_{combo}_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
        "ticker": ticker,
        "combo": combo,
        "strike": float(strike),
        "put_strike": float(put_strike) if put_strike is not None else None,
        "spread_type": spread_type,
        "front_exp": str(front_exp),
        "back_exp": str(back_exp),
        "entry_date": datetime.now().strftime("%Y-%m-%d"),
        "contracts": contracts,
        "cost_per_share": float(cost_per_share),
        "total_deployed": round(total_cost, 2),
        "n_legs": n_legs,
        "ff": float(ff),
        "min_leg_oi": min_leg_oi,
    }
    portfolio["positions"].append(pos)
    return pos


def record_trade(
    position: dict,
    exit_price: float,
    pnl: float,
    return_pct: float,
) -> None:
    """Append closed trade to trades.json."""
    if TRADES_FILE.exists():
        with open(TRADES_FILE) as f:
            data = json.load(f)
    else:
        data = {"trades": []}

    trade_record = {
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
        "entry_method": position.get("execution_method", ""),
        "entry_slippage": position.get("slippage", 0.0),
        "close_method": position.get("close_method", ""),
        "close_slippage": round(exit_price - position.get("cost_per_share", 0), 4)
                         if exit_price else 0.0,
    }
    data["trades"].append(trade_record)

    with open(TRADES_FILE, "w") as f:
        json.dump(data, f, indent=2)


# ═══════════════════════════════════════════════════════════════
#  PENDING SIGNALS — Save/Load/Merge for retry
# ═══════════════════════════════════════════════════════════════

def load_pending_signals() -> list[dict]:
    """Load pending signals from state/pending_signals.json."""
    if not PENDING_SIGNALS_FILE.exists():
        return []
    try:
        with open(PENDING_SIGNALS_FILE) as f:
            data = json.load(f)
        return data.get("signals", [])
    except Exception:
        return []


def save_pending_signals(signals: list[dict]) -> None:
    """Save pending signals to state/pending_signals.json."""
    data = {
        "last_updated": datetime.now().isoformat(),
        "signals": signals,
    }
    with open(PENDING_SIGNALS_FILE, "w") as f:
        json.dump(data, f, indent=2)
    log.info("  Saved %d pending signal(s) to %s", len(signals),
             PENDING_SIGNALS_FILE.name)


def merge_signals_with_pending(fresh: pd.DataFrame,
                                pending: list[dict]) -> pd.DataFrame:
    """Merge fresh scanner signals with pending (unfilled) signals.

    - Dedup: if ticker present in both fresh AND pending, keep fresh
    - Age check: drop pending if > MAX_PENDING_AGE_DAYS old
    - DTE check: drop pending if front DTE < MIN_FRONT_DTE_PENDING
    - Return combined DataFrame sorted by FF descending
    """
    if not pending:
        return fresh

    today = datetime.now()
    fresh_tickers = set(fresh["ticker"].tolist()) if not fresh.empty else set()

    kept = []
    for sig in pending:
        ticker = sig.get("ticker", "")
        # Dedup: fresh takes priority
        if ticker in fresh_tickers:
            log.info("  Pending %s: replaced by fresh signal", ticker)
            continue
        # Age check
        pending_since = sig.get("pending_since", "")
        if pending_since:
            try:
                age = (today - datetime.strptime(pending_since, "%Y-%m-%d")).days
            except ValueError:
                age = 999
            if age > MAX_PENDING_AGE_DAYS:
                log.info("  Pending %s: dropped (age %dd > %dd)",
                         ticker, age, MAX_PENDING_AGE_DAYS)
                continue
        # DTE check
        front_exp = sig.get("front_exp", "")
        if front_exp:
            try:
                front_dt = datetime.strptime(str(front_exp), "%Y-%m-%d")
                front_dte = (front_dt - today).days
            except ValueError:
                front_dte = 999
            if front_dte < MIN_FRONT_DTE_PENDING:
                log.info("  Pending %s: dropped (front DTE %d < %d)",
                         ticker, front_dte, MIN_FRONT_DTE_PENDING)
                continue
        kept.append(sig)

    if not kept:
        return fresh

    # Convert pending to DataFrame rows matching fresh columns
    pending_df = pd.DataFrame(kept)
    # Mark source for downstream identification
    pending_df["_pending"] = True
    if not fresh.empty:
        fresh = fresh.copy()
        fresh["_pending"] = False
        combined = pd.concat([fresh, pending_df], ignore_index=True)
    else:
        combined = pending_df
        combined["_pending"] = True

    combined = combined.sort_values("ff", ascending=False).reset_index(drop=True)
    n_pending = len(kept)
    log.info("  Merged %d fresh + %d pending = %d total signals",
             len(fresh), n_pending, len(combined))
    return combined


# ═══════════════════════════════════════════════════════════════
#  CACHED MONITOR PRICES & P&L HISTORY
# ═══════════════════════════════════════════════════════════════

def ibkr_portfolio_to_positions(
    ibkr_items: list[dict],
    portfolio_positions: list[dict],
) -> tuple[list[dict], list[str]]:
    """Convert raw ib.portfolio() items to monitor position dicts.

    Groups IBKR portfolio items (options) by ticker, matches with active
    positions from portfolio.json, and computes P&L from IBKR marketValue
    and unrealizedPNL.

    Args:
        ibkr_items: list of dicts from ib.portfolio(), each with:
            contract (symbol, secType, strike, right, lastTradeDateOrContractMonth),
            position, marketPrice, marketValue, averageCost, unrealizedPNL
        portfolio_positions: active positions from portfolio.json

    Returns:
        (positions_list, errors_list) — same JSON shape as _price_position()
    """
    from datetime import datetime

    # 1. Index all IBKR items by a unique contract key for fast lookup
    # Key: (symbol, strike, right, expiration_YYYYMMDD)
    ibkr_map = {}
    for item in ibkr_items:
        if hasattr(item, "contract"):
            c = item.contract
            key = (c.symbol, float(c.strike), c.right, c.lastTradeDateOrContractMonth)
        elif isinstance(item, dict):
            c = item.get("contract", item)
            key = (c.get("symbol"), float(c.get("strike", 0)), c.get("right"), c.get("lastTradeDateOrContractMonth"))
        else:
            continue
        ibkr_map[key] = item

    positions = []
    errors = []

    for pos in portfolio_positions:
        ticker = pos["ticker"]
        strike = float(pos["strike"])
        put_strike = float(pos.get("put_strike") or strike)
        right = pos.get("right", "C") # Fallback for single legs
        f_exp = pos["front_exp"].replace("-", "")
        b_exp = pos["back_exp"].replace("-", "")

        # Identify the legs for this specific position
        legs_found = []
        
        # Standard 2-leg or 4-leg (double) logic
        # For a double (straddle calendar), we have 2 calls and 2 puts
        if pos.get("spread_type") == "double":
            # Call legs
            k_c_f = (ticker, strike, "C", f_exp)
            k_c_b = (ticker, strike, "C", b_exp)
            # Put legs
            k_p_f = (ticker, put_strike, "P", f_exp)
            k_p_b = (ticker, put_strike, "P", b_exp)
            
            for k in [k_c_f, k_c_b, k_p_f, k_p_b]:
                if k in ibkr_map:
                    legs_found.append(ibkr_map[k])
        else:
            # Single leg (calendar call or put)
            k_f = (ticker, strike, right, f_exp)
            k_b = (ticker, strike, right, b_exp)
            for k in [k_f, k_b]:
                if k in ibkr_map:
                    legs_found.append(ibkr_map[k])

        if not legs_found:
            errors.append(ticker)
            continue

        # Sum marketValue and unrealizedPNL ONLY for these matched legs
        total_market_value = 0.0
        total_unrealized_pnl = 0.0
        for item in legs_found:
            if isinstance(item, dict):
                total_market_value += item.get("marketValue", 0) or 0
                total_unrealized_pnl += item.get("unrealizedPNL", 0) or 0
            else:
                total_market_value += getattr(item, "marketValue", 0) or 0
                total_unrealized_pnl += getattr(item, "unrealizedPNL", 0) or 0

        contracts = pos["contracts"]
        entry_cost = pos["cost_per_share"]
        deployed = pos.get("total_deployed", entry_cost * CONTRACT_MULT * contracts)

        # current_cost = total market value per share
        divisor = contracts * CONTRACT_MULT
        current_cost = total_market_value / divisor if divisor > 0 else 0

        return_pct = total_unrealized_pnl / deployed if deployed > 0 else 0

        # Front DTE
        try:
            front_dt = datetime.strptime(pos["front_exp"], "%Y-%m-%d")
            front_dte = (front_dt - datetime.now()).days
        except (ValueError, KeyError):
            front_dte = -1

        positions.append({
            "ticker": ticker,
            "combo": pos.get("combo", ""),
            "contracts": contracts,
            "strike": strike,
            "put_strike": put_strike,
            "entry_cost": round(entry_cost, 2),
            "current_cost": round(current_cost, 2),
            "unrealized_pnl": round(total_unrealized_pnl, 2),
            "return_pct": round(return_pct, 4),
            "front_dte": front_dte,
            "stock_px": 0,
            "source": "ibkr",
        })

    return positions, errors


def load_cached_monitor_prices() -> tuple[dict, str | None]:
    """Load the latest monitor_*.json snapshot (not sim_*).

    Returns:
        ({ticker: pricing_dict}, cached_date_str | None)
    """
    monitor_files = sorted(STATE.glob("monitor_*.json"), reverse=True)
    # Filter out sim_ files
    monitor_files = [f for f in monitor_files if not f.name.startswith("sim_")]
    if not monitor_files:
        return {}, None

    try:
        with open(monitor_files[0]) as f:
            snapshot = json.load(f)
        cached_prices = {}
        for pos in snapshot.get("positions", []):
            cached_prices[pos["ticker"]] = pos
        return cached_prices, snapshot.get("date")
    except Exception:
        return {}, None


def load_pnl_history() -> list[dict]:
    """Load P&L history from all monitor_*.json snapshots (not sim_*).

    Returns:
        [{date, total_pnl, n_positions}, ...]
    """
    history = []
    for f in sorted(STATE.glob("monitor_*.json")):
        if f.name.startswith("sim_"):
            continue
        try:
            with open(f) as fh:
                data = json.load(fh)
            positions = data.get("positions", [])
            history.append({
                "date": data.get("date", f.stem.replace("monitor_", "")),
                "total_pnl": round(data.get("total_unrealized_pnl", 0), 2),
                "n_positions": len(positions),
            })
        except Exception:
            continue
    return history
