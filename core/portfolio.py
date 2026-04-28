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
    STATE, OUTPUT, PORTFOLIO_FILE, TRADES_FILE, BACKTEST_TRADES_FILE,
    MAX_POSITIONS, MAX_CONTRACTS, DEFAULT_ALLOC,
    KELLY_FRAC, MIN_KELLY_TRADES, CONTRACT_MULT,
    COMMISSION_LEG, SLIPPAGE_PER_LEG, SLIPPAGE_BUFFER,
    FF_THRESHOLD_DEFAULT,
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


def cost_per_contract(cost_per_share: float, n_legs: int = 4) -> float:
    """Cost per contract including slippage + commission (matches backtest.py)."""
    slippage = SLIPPAGE_PER_LEG * n_legs  # $0.12/share for 4-leg double calendar
    return (cost_per_share + slippage) * CONTRACT_MULT + COMMISSION_LEG * n_legs


def size_portfolio(
    signals_info: list[tuple[str, float, int]],
    kelly_f: float,
    account_value: float,
) -> list[tuple[str, int, float]]:
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
    sizing: list[tuple[str, int, float]] = []
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
) -> dict:
    """Add a new position to portfolio state."""
    slippage = SLIPPAGE_PER_LEG * n_legs  # Entry slippage: $0.12/share for 4-leg
    commission = n_legs * COMMISSION_LEG * contracts
    total_cost = (cost_per_share + slippage) * CONTRACT_MULT * contracts + commission

    pos = {
        "id": f"{ticker}_{combo}_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
        "ticker": ticker,
        "combo": combo,
        "strike": float(strike),
        "put_strike": float(put_strike) if put_strike is not None else float(strike),
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
#  CACHED MONITOR PRICES & P&L HISTORY
# ═══════════════════════════════════════════════════════════════

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
