"""Track Record — equity curves + quantstats-style metrics for backtest & live.

Data sources:
  Backtest: output/backtest_daily.csv (true daily equity from backtest.py)
            Falls back to output/backtest_trades.csv with portfolio-return approx
  Live:     state/trades.json

The equity curve always extends to today's date (forward-fill) so the
chart shows the full timeline up to now.
"""

import json
import math
from datetime import datetime
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
import pandas as pd

from core.config import (BACKTEST_TRADES_FILE, TRADES_FILE, PORTFOLIO_FILE,
                         OUTPUT, STATE, DEFAULT_ALLOC)

CHART_PATH = OUTPUT / "track_record.png"
BACKTEST_DAILY_FILE = OUTPUT / "backtest_daily.csv"

# Outlier filters (only used for trade-level fallback)
MIN_ENTRY_COST = 1.00
MAX_RETURN_PCT = 10.0


# ── Data loaders ──

def _load_backtest_daily() -> pd.DataFrame | None:
    """Load true daily equity curve from backtest.py output.

    Returns DataFrame with columns: date, account  (sorted by date).
    Returns None if file doesn't exist.
    """
    if not BACKTEST_DAILY_FILE.exists():
        return None
    try:
        df = pd.read_csv(str(BACKTEST_DAILY_FILE))
        if "date" not in df.columns or "account" not in df.columns:
            return None
        df["date"] = pd.to_datetime(df["date"])
        return df.sort_values("date").reset_index(drop=True)
    except Exception:
        return None


def _load_backtest_trades_fallback() -> tuple[np.ndarray, np.ndarray, int] | None:
    """Fallback: build portfolio returns from per-trade data.

    Uses size_pct * return_pct grouped by exit_date.
    Returns (dates, portfolio_returns, n_trades) or None.
    """
    if not BACKTEST_TRADES_FILE.exists():
        return None
    try:
        df = pd.read_csv(str(BACKTEST_TRADES_FILE))
        df = df.dropna(subset=["return_pct", "exit_date"])
        # Filter outliers
        if "entry_cost" in df.columns:
            df = df[df["entry_cost"] >= MIN_ENTRY_COST]
        df = df[df["return_pct"] <= MAX_RETURN_PCT]
        if df.empty:
            return None
        df["exit_date"] = pd.to_datetime(df["exit_date"])
        if "size_pct" not in df.columns:
            df["size_pct"] = DEFAULT_ALLOC
        n_trades = len(df)
        df["portfolio_contrib"] = df["size_pct"] * df["return_pct"]
        daily = df.groupby("exit_date")["portfolio_contrib"].sum().sort_index()
        return daily.index.values, daily.values, n_trades
    except Exception:
        return None


def load_live_trades() -> pd.DataFrame:
    """Load live trades from state/trades.json."""
    cols = ["exit_date", "return_pct", "dollar_pnl", "ticker", "size_pct"]
    empty = pd.DataFrame(columns=cols)
    if not TRADES_FILE.exists():
        return empty
    try:
        with open(TRADES_FILE) as f:
            data = json.load(f)
        trades = data.get("trades", [])
        if not trades:
            return empty
        rows = []
        for t in trades:
            if t.get("return_pct") is not None and t.get("exit_date"):
                rows.append({
                    "exit_date": t["exit_date"],
                    "return_pct": t["return_pct"],
                    "dollar_pnl": t.get("pnl", t.get("dollar_pnl", 0)),
                    "ticker": t.get("ticker", ""),
                    "size_pct": t.get("size_pct", DEFAULT_ALLOC),
                })
        if not rows:
            return empty
        df = pd.DataFrame(rows)
        df["exit_date"] = pd.to_datetime(df["exit_date"])
        return df.sort_values("exit_date").reset_index(drop=True)
    except Exception:
        return empty


def _build_live_equity(start_equity: float, today: pd.Timestamp) -> dict | None:
    """Build live MTM equity curve from monitor snapshots + closed trades.

    Data sources:
    - state/monitor_YYYYMMDD.json : daily snapshots with total_unrealized_pnl
    - state/trades.json : closed trades with realized pnl
    - state/portfolio.json : active/closed positions

    Equity = start_equity + cumulative_realized_pnl + unrealized_pnl(snapshot)

    Returns dict with dates, equity, n_closed, n_open or None.
    """
    # Load portfolio for position counts
    portfolio = {}
    if PORTFOLIO_FILE.exists():
        try:
            with open(PORTFOLIO_FILE) as f:
                portfolio = json.load(f)
        except Exception:
            pass

    positions = portfolio.get("positions", [])
    active = [p for p in positions if "exit_date" not in p]
    closed_pos = [p for p in positions if "exit_date" in p]

    # Load closed trades
    closed_trades = []
    if TRADES_FILE.exists():
        try:
            with open(TRADES_FILE) as f:
                closed_trades = json.load(f).get("trades", [])
        except Exception:
            pass

    # Load monitor snapshots (MTM data)
    snapshots = {}
    for snap_file in sorted(STATE.glob("monitor_*.json")):
        try:
            with open(snap_file) as f:
                snap = json.load(f)
            snap_date = pd.Timestamp(snap["date"])
            snapshots[snap_date] = snap
        except Exception:
            continue

    # If no live activity at all, nothing to show
    if not active and not closed_pos and not closed_trades and not snapshots:
        return None

    # Build realized P&L timeline: (date → cumulative realized pnl)
    realized_events = []
    for t in closed_trades:
        if t.get("exit_date") and t.get("pnl") is not None:
            realized_events.append((pd.Timestamp(t["exit_date"]), float(t["pnl"])))
    for p in closed_pos:
        if p.get("exit_date") and p.get("pnl") is not None:
            realized_events.append((pd.Timestamp(p["exit_date"]), float(p["pnl"])))
    realized_events.sort(key=lambda x: x[0])

    # Cumulative realized P&L by date
    cum_realized = {}
    running_realized = 0.0
    for dt, pnl in realized_events:
        running_realized += pnl
        cum_realized[dt] = running_realized

    # Find the first live date
    all_dates = []
    for p in active + closed_pos:
        if p.get("entry_date"):
            all_dates.append(pd.Timestamp(p["entry_date"]))
    all_dates.extend(snapshots.keys())
    for dt in cum_realized:
        all_dates.append(dt)

    if not all_dates:
        return None

    first_date = min(all_dates)

    # Build equity timeline using snapshots for MTM
    timeline_dates = []
    timeline_equity = []

    # Collect all dates we have data for (sorted)
    data_dates = sorted(set([first_date] + list(snapshots.keys())
                            + list(cum_realized.keys()) + [today]))

    last_unrealized = 0.0
    last_cum_realized = 0.0

    for dt in data_dates:
        # Update cumulative realized if we have an event on this date
        if dt in cum_realized:
            last_cum_realized = cum_realized[dt]

        # Update unrealized from snapshot if available
        if dt in snapshots:
            last_unrealized = snapshots[dt].get("total_unrealized_pnl", 0.0)

        equity = start_equity + last_cum_realized + last_unrealized
        timeline_dates.append(dt)
        timeline_equity.append(equity)

    # Ensure we extend to today
    if timeline_dates[-1] < today:
        timeline_dates.append(today)
        timeline_equity.append(timeline_equity[-1])

    # ── Portfolio summary from latest snapshot + positions ──
    total_deployed = sum(float(p.get("total_deployed", 0)) for p in active)

    # Latest snapshot position-level stats
    latest_snap = snapshots[max(snapshots)] if snapshots else None
    snap_positions = latest_snap.get("positions", []) if latest_snap else []
    n_priced = len(snap_positions)
    total_unrealized = latest_snap.get("total_unrealized_pnl", 0.0) if latest_snap else 0.0

    # Per-position returns from snapshot
    n_win = sum(1 for sp in snap_positions if (sp.get("unrealized_pnl") or 0) > 0)
    snap_date = latest_snap.get("date") if latest_snap else None

    # First entry date
    entry_dates = [p["entry_date"] for p in active if p.get("entry_date")]
    first_entry = min(entry_dates) if entry_dates else None
    days_active = (today - pd.Timestamp(first_entry)).days if first_entry else 0

    portfolio_summary = {
        "n_open": len(active),
        "n_priced": n_priced,
        "n_win": n_win,
        "total_deployed": round(total_deployed, 2),
        "total_unrealized_pnl": round(total_unrealized, 2),
        "return_on_deployed": round(total_unrealized / total_deployed, 4) if total_deployed > 0 else 0.0,
        "days_active": days_active,
        "first_entry": first_entry,
        "snap_date": snap_date,
    }

    return {
        "dates": timeline_dates,
        "equity": timeline_equity,
        "n_closed": len(closed_trades) + len(closed_pos),
        "n_open": len(active),
        "portfolio_summary": portfolio_summary,
    }


# ── Metrics ──

def _metrics_from_equity(equity: np.ndarray, dates: np.ndarray,
                         n_trades: int) -> dict | None:
    """Compute metrics from a daily equity curve."""
    if len(equity) < 2:
        return None

    initial = float(equity[0])
    final = float(equity[-1])

    # No change in equity → no meaningful metrics
    if initial == final and len(set(equity)) == 1:
        return {
            "cagr": 0.0, "sharpe": 0.0, "sortino": 0.0, "max_dd": 0.0,
            "win_rate": 0.0, "profit_factor": 0.0,
            "avg_win": 0.0, "avg_loss": 0.0,
            "total_trades": n_trades, "total_pnl": 0.0,
            "final_equity": round(final, 2),
        }

    # Time span
    date_range = (dates[-1] - dates[0]) / np.timedelta64(1, "D")
    years = max(date_range / 365.25, 0.01)

    # CAGR
    cagr = (final / initial) ** (1 / years) - 1

    # Daily returns
    returns = np.diff(equity) / equity[:-1]

    # Annualization (business days per year from data)
    periods_per_year = len(returns) / years if years > 0 else 252
    ann_factor = math.sqrt(periods_per_year)

    # Sharpe
    mu = float(np.mean(returns))
    std = float(np.std(returns, ddof=1)) if len(returns) > 1 else 0.0
    sharpe = (mu / std * ann_factor) if std > 0 else 0.0

    # Sortino
    downside = returns[returns < 0]
    down_std = float(np.std(downside, ddof=1)) if len(downside) > 1 else 0.0
    sortino = (mu / down_std * ann_factor) if down_std > 0 else 0.0

    # Max drawdown
    running_max = np.maximum.accumulate(equity)
    dd = (equity - running_max) / running_max
    max_dd = float(np.min(dd))

    # Win rate (on daily returns)
    wins = returns[returns > 0]
    losses = returns[returns <= 0]
    win_rate = len(wins) / len(returns) if len(returns) > 0 else 0.0

    # Avg win / avg loss
    avg_win = float(np.mean(wins)) if len(wins) > 0 else 0.0
    avg_loss = float(np.mean(losses)) if len(losses) > 0 else 0.0

    # Profit factor
    gross_win = float(np.sum(wins)) if len(wins) > 0 else 0.0
    gross_loss = float(np.abs(np.sum(losses))) if len(losses) > 0 else 0.0
    profit_factor = gross_win / gross_loss if gross_loss > 0 else 0.0

    total_pnl = final - initial

    return {
        "cagr": round(cagr, 4),
        "sharpe": round(sharpe, 2),
        "sortino": round(sortino, 2),
        "max_dd": round(max_dd, 4),
        "win_rate": round(win_rate, 4),
        "profit_factor": round(profit_factor, 2),
        "avg_win": round(avg_win, 4),
        "avg_loss": round(avg_loss, 4),
        "total_trades": n_trades,
        "total_pnl": round(total_pnl, 2),
        "final_equity": round(final, 2),
    }


# ── Chart generation ──

def generate_chart(
    bt_dates, bt_equity, live_dates, live_equity, initial: float
) -> str:
    """Generate dark-themed equity + drawdown chart. Returns path to PNG."""
    plt.style.use("dark_background")
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 7), height_ratios=[3, 1],
                                    gridspec_kw={"hspace": 0.15})
    fig.patch.set_facecolor("#0d1117")
    for ax in (ax1, ax2):
        ax.set_facecolor("#0d1117")
        ax.tick_params(colors="#8b949e", labelsize=9)
        for spine in ax.spines.values():
            spine.set_color("#30363d")

    # ── Equity curve ──
    transition_date = None

    if len(bt_dates) > 0:
        ax1.plot(bt_dates, bt_equity, color="#58a6ff", linewidth=1.5, label="Backtest")
        ax1.fill_between(bt_dates, initial, bt_equity, alpha=0.08, color="#58a6ff")
        transition_date = bt_dates[-1]

    if len(live_dates) > 0:
        ax1.plot(live_dates, live_equity, color="#3fb950", linewidth=1.5, label="Live")
        ax1.fill_between(live_dates, initial, live_equity, alpha=0.08, color="#3fb950")
        if transition_date is None:
            transition_date = live_dates[0]

    if transition_date is not None and len(bt_dates) > 0 and len(live_dates) > 0:
        ax1.axvline(transition_date, color="#d29922", linestyle="--", alpha=0.6, linewidth=1)
        ax1.text(transition_date, ax1.get_ylim()[1] * 0.95, " Live start",
                 color="#d29922", fontsize=9, alpha=0.8)

    ax1.set_ylabel("Equity ($)", color="#8b949e", fontsize=10)
    ax1.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"${x:,.0f}"))
    ax1.legend(loc="upper left", fontsize=9, framealpha=0.3)
    ax1.grid(True, alpha=0.1)
    ax1.set_title("Track Record — Equity Curve", color="#e6edf3", fontsize=14,
                   fontweight="bold", pad=10)

    # ── Drawdown ──
    def _drawdown(equity_arr):
        running_max = np.maximum.accumulate(equity_arr)
        return (equity_arr - running_max) / running_max * 100

    if len(bt_dates) > 0:
        bt_dd = _drawdown(np.array(bt_equity))
        ax2.fill_between(bt_dates, 0, bt_dd, alpha=0.3, color="#58a6ff")
        ax2.plot(bt_dates, bt_dd, color="#58a6ff", linewidth=0.8)

    if len(live_dates) > 0:
        live_dd = _drawdown(np.array(live_equity))
        ax2.fill_between(live_dates, 0, live_dd, alpha=0.3, color="#3fb950")
        ax2.plot(live_dates, live_dd, color="#3fb950", linewidth=0.8)

    if transition_date is not None and len(bt_dates) > 0 and len(live_dates) > 0:
        ax2.axvline(transition_date, color="#d29922", linestyle="--", alpha=0.6, linewidth=1)

    ax2.set_ylabel("Drawdown (%)", color="#8b949e", fontsize=10)
    ax2.set_xlabel("")
    ax2.grid(True, alpha=0.1)

    # Format x-axis dates
    for ax in (ax1, ax2):
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
        ax.xaxis.set_major_locator(mdates.YearLocator())

    fig.savefig(str(CHART_PATH), dpi=120, bbox_inches="tight",
                facecolor="#0d1117", edgecolor="none")
    plt.close(fig)

    return f"/output/{CHART_PATH.name}"


# ── Main entry point ──

def compute_track_record(initial: float = 100_000) -> dict:
    """Compute full track record: backtest + live equity curves and metrics."""
    result: dict = {"initial": initial, "backtest": None, "live": None, "chart": None}

    bt_dates_list, bt_equity_list = [], []
    bt_final = initial
    bt_n_trades = 0

    # ── Backtest: prefer daily equity curve, fall back to trade-level approx ──
    daily = _load_backtest_daily()
    if daily is not None and len(daily) > 1:
        bt_dates_list = daily["date"].tolist()
        bt_equity_list = daily["account"].tolist()
        bt_final = float(daily["account"].iloc[-1])

        # Count trades from backtest_trades.csv if it exists
        if BACKTEST_TRADES_FILE.exists():
            try:
                bt_n_trades = len(pd.read_csv(str(BACKTEST_TRADES_FILE)))
            except Exception:
                bt_n_trades = 0

        metrics = _metrics_from_equity(
            daily["account"].values, daily["date"].values, bt_n_trades)
        result["backtest"] = {
            "metrics": metrics,
            "final_equity": round(bt_final, 2),
            "n_trades": bt_n_trades,
        }
    else:
        # Fallback: approximate from per-trade returns
        fallback = _load_backtest_trades_fallback()
        if fallback is not None:
            dates, port_returns, bt_n_trades = fallback
            equity = initial * np.cumprod(1 + port_returns)
            bt_dates_list = list(dates)
            bt_equity_list = equity.tolist()
            bt_final = float(equity[-1])

            metrics = _metrics_from_equity(
                np.insert(equity, 0, initial),
                np.insert(dates, 0, dates[0] - np.timedelta64(1, "D")),
                bt_n_trades)
            result["backtest"] = {
                "metrics": metrics,
                "final_equity": round(bt_final, 2),
                "n_trades": bt_n_trades,
                "approximate": True,
            }

    # ── Live: build from portfolio + closed trades ──
    live_dates_list, live_equity_list = [], []
    live_start = bt_final
    today = pd.Timestamp(datetime.now().date())

    live_data = _build_live_equity(live_start, today)
    if live_data is not None:
        live_dates_list = live_data["dates"]
        live_equity_list = live_data["equity"]
        n_closed = live_data["n_closed"]
        n_open = live_data["n_open"]
        portfolio_summary = live_data.get("portfolio_summary", {})

        if len(live_equity_list) >= 2:
            eq_arr = np.array(live_equity_list)
            dt_arr = np.array(live_dates_list, dtype="datetime64[ns]")
            live_metrics = _metrics_from_equity(eq_arr, dt_arr, n_trades=n_closed)
            result["live"] = {
                "metrics": live_metrics,
                "final_equity": round(live_equity_list[-1], 2),
                "n_trades": n_closed,
                "n_open": n_open,
                "portfolio": portfolio_summary,
            }
        elif len(live_equity_list) == 1:
            result["live"] = {
                "metrics": None,
                "final_equity": round(live_equity_list[-1], 2),
                "n_trades": n_closed,
                "n_open": n_open,
                "portfolio": portfolio_summary,
            }

    # Extend backtest to today if no live data
    if not live_dates_list and bt_dates_list:
        last_date = bt_dates_list[-1]
        if last_date < today:
            bt_dates_list.append(today)
            bt_equity_list.append(bt_equity_list[-1])

    # ── Generate chart ──
    if bt_dates_list or live_dates_list:
        result["chart"] = generate_chart(
            bt_dates_list, bt_equity_list,
            live_dates_list, live_equity_list,
            initial,
        )

    return result
