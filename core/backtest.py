"""
Portfolio Backtest — Forward Factor Double Calendar Spreads

Single combined portfolio:
  - $100K starting capital
  - Max 20 concurrent positions (top FF across all combos)
  - Half Kelly (f/2) position sizing
  - Slippage + commissions
  - FF >= 20% entry signal (PDF/Campasano formula)
  - Close as spread 1 day before front expiry (ex-earn filtered)

Usage:
    python backtest.py
"""

import numpy as np
import pandas as pd
import sqlite3
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from pathlib import Path

from core.config import CACHE, OUTPUT, DB, get_logger
log = get_logger(__name__)
OUT = OUTPUT

# ── Parameters ──
INITIAL_CAPITAL  = 100_000
MAX_POSITIONS    = 20
KELLY_FRAC       = 0.5          # Half Kelly (f/2)
# FF threshold in PDF formula: (front_iv - fwd_iv) / fwd_iv >= 0.20
FF_THRESHOLD     = 0.200        # Campasano/PDF threshold (all combos)
FF_THRESHOLD_MAP = {"30-60": 0.200, "30-90": 0.200, "60-90": 0.200}
MIN_SPREAD_COST  = 1.00         # $1.00/share minimum
MAX_BA_PCT       = 0.10         # 10% max bid-ask spread (university liquidity filter)
# Per-leg costs (ORATS methodology: ~66% of quoted spread for 2-leg, ~53% for 4-leg)
# Muravyev & Pearson (2020): timed execution ~1-2c/leg on liquid names
# Conservative: $0.05/leg covers ~50-75% of typical ATM bid-ask on S&P 500 names
SLIPPAGE_PER_LEG = 0.03         # $0.03/leg/share (Muravyev & Pearson 2020: effective spread ~40% of quoted)
COMMISSION_PER_LEG = 0.65       # $0.65/leg/contract (IBKR US options)
CONTRACT_MULT    = 100          # 1 option contract = 100 shares
MAX_CONTRACTS    = 10           # Max contracts per position
DEFAULT_ALLOC    = 0.04         # 4% per name default (before Kelly kicks in)
MIN_KELLY_TRADES = 50           # Need 50 trades before using Kelly


def _load_earnings():
    """Load ALL earnings dates from DB, return dict: root -> sorted int array."""
    conn = sqlite3.connect(str(DB))
    edf = pd.read_sql_query("SELECT root, report_date FROM earnings", conn)
    conn.close()
    earn_by_root = {}
    if not edf.empty:
        edf["report_date"] = edf["report_date"].astype(int)
        for root, grp in edf.groupby("root"):
            earn_by_root[root] = np.sort(grp["report_date"].values)
    return earn_by_root


def _has_earnings_between(root, front_exp_int, back_exp_int, earn_by_root):
    """Check if any earnings date falls between front and back expiry."""
    if root not in earn_by_root:
        return False
    edates = earn_by_root[root]
    idx = np.searchsorted(edates, front_exp_int, side="left")
    return idx < len(edates) and edates[idx] <= back_exp_int


def load_data():
    """Load spread returns. Convert FF, filter earnings."""
    df = pd.read_pickle(str(CACHE / "spread_returns.pkl"))

    # Stored FF uses old formula: fwd_var/front_var - 1
    # Convert to PDF/Campasano: (front_iv - fwd_iv) / fwd_iv = 1/sqrt(1+ff_old) - 1
    df = df[np.isfinite(df["ff"])].copy()
    ff_old = df["ff"].values
    mask = ff_old > -1
    ff_pdf = np.where(mask, 1.0 / np.sqrt(1.0 + ff_old) - 1.0, np.nan)
    df["ff"] = ff_pdf
    df = df[np.isfinite(df["ff"])].copy()

    # Filter: no earnings between obs_date (entry) and back_exp (ex-earn IVs)
    earn_by_root = _load_earnings()
    n_before = len(df)
    keep = np.array([
        not _has_earnings_between(row["root"], int(row["obs_date"]),
                                  int(row["back_exp"]), earn_by_root)
        for _, row in df.iterrows()
    ])
    df = df[keep].copy()
    n_removed = n_before - len(df)
    log.info("Earnings filter: removed %s trades (%.1f%%), kept %s",
             f"{n_removed:,}", n_removed / n_before * 100, f"{len(df):,}")

    # Parse dates
    df["entry_dt"] = pd.to_datetime(df["obs_date"].astype(str), format="%Y%m%d")
    # Exit = 1 day before front expiry (already computed in returns.py as exit_date)
    df["exit_dt"] = pd.to_datetime(df["exit_date"].astype(str), format="%Y%m%d")

    log.info("Loaded %s trades (%s to %s)",
             f"{len(df):,}", df["entry_dt"].min().date(), df["entry_dt"].max().date())
    return df


def _build_contract_id_cache(conn, sub, mode):
    """Pre-resolve all contract_ids needed for MTM lookups.

    Returns dict: (root, exp_int, strike_millis, right) -> contract_id
    """
    # Collect unique (root, expiration, strike) tuples from all trades
    tuples = set()
    for _, row in sub.iterrows():
        root = row["root"]
        # Call legs use front_strike/back_strike
        fk = (root, int(row["front_exp"]), int(round(row["front_strike"] * 1000)))
        bk = (root, int(row["back_exp"]), int(round(row["back_strike"] * 1000)))
        tuples.add(fk)
        tuples.add(bk)
        # Put legs may use different strike
        if mode == "double":
            ps = row.get("put_strike", np.nan)
            pbs = row.get("put_back_strike", np.nan)
            if not pd.isna(ps):
                tuples.add((root, int(row["front_exp"]), int(round(ps * 1000))))
            if not pd.isna(pbs):
                tuples.add((root, int(row["back_exp"]), int(round(pbs * 1000))))

    # Group by root for batched queries
    by_root = {}
    for root, exp, strike in tuples:
        by_root.setdefault(root, []).append((exp, strike))

    rights = ["C", "P"] if mode == "double" else ["C"]
    cache = {}
    cur = conn.cursor()

    BATCH_SIZE = 400  # Keep well under SQLite expression depth limit of 1000
    for root, pairs in by_root.items():
        for right in rights:
            for i in range(0, len(pairs), BATCH_SIZE):
                batch = pairs[i:i + BATCH_SIZE]
                placeholders = " OR ".join(
                    ["(expiration = ? AND strike = ?)"] * len(batch))
                params = []
                for exp, strike in batch:
                    params.extend([exp, strike])
                query = (f"SELECT contract_id, expiration, strike FROM contracts "
                         f"WHERE root = ? AND right = ? AND ({placeholders})")
                cur.execute(query, [root, right] + params)
                for cid, exp, strike in cur.fetchall():
                    cache[(root, exp, strike, right)] = cid

    log.info("  MTM: resolved %s contract IDs", f"{len(cache):,}")
    return cache


def _get_eod_dates(conn):
    """Load all available EOD dates into a set."""
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT date FROM eod_history")
    return {r[0] for r in cur.fetchall()}


def _compute_mtm_value(positions, date_int, conn, cid_cache, mode):
    """Compute mark-to-market value of all open positions on a given date.

    Returns total MTM value in dollars, or None if no data available.
    """
    if not positions:
        return 0.0

    # Collect needed contract_ids
    needed = {}  # contract_id -> (pos_idx, leg_label)
    for i, pos in enumerate(positions):
        root = pos["ticker"]
        fexp = int(pos["front_exp"])
        bexp = int(pos["back_exp"])
        call_strike_millis = int(round(pos["front_strike"] * 1000))

        # Call legs
        fc_key = (root, fexp, call_strike_millis, "C")
        bc_key = (root, bexp, call_strike_millis, "C")
        fc_cid = cid_cache.get(fc_key)
        bc_cid = cid_cache.get(bc_key)
        if fc_cid is not None:
            needed[fc_cid] = needed.get(fc_cid, [])
            needed[fc_cid].append((i, "front_call"))
        if bc_cid is not None:
            needed[bc_cid] = needed.get(bc_cid, [])
            needed[bc_cid].append((i, "back_call"))

        # Put legs (double calendar only) — use put_strike if available
        if mode == "double":
            put_front_millis = int(round(pos.get("put_strike", pos["front_strike"]) * 1000))
            put_back_millis = int(round(pos.get("put_back_strike", pos["front_strike"]) * 1000))
            fp_key = (root, fexp, put_front_millis, "P")
            bp_key = (root, bexp, put_back_millis, "P")
            fp_cid = cid_cache.get(fp_key)
            bp_cid = cid_cache.get(bp_key)
            if fp_cid is not None:
                needed[fp_cid] = needed.get(fp_cid, [])
                needed[fp_cid].append((i, "front_put"))
            if bp_cid is not None:
                needed[bp_cid] = needed.get(bp_cid, [])
                needed[bp_cid].append((i, "back_put"))

    if not needed:
        return None

    # Batch query EOD data
    cid_list = list(needed.keys())
    placeholders = ",".join(["?"] * len(cid_list))
    cur = conn.cursor()
    cur.execute(
        f"SELECT contract_id, bid, ask, close FROM eod_history "
        f"WHERE date = ? AND contract_id IN ({placeholders})",
        [date_int] + cid_list)

    prices = {}
    for cid, bid, ask, close_px in cur.fetchall():
        if bid is not None and ask is not None and bid > 0 and ask > 0:
            prices[cid] = (bid + ask) / 2
        elif close_px is not None and close_px > 0:
            prices[cid] = close_px

    # Compute per-position MTM
    n_legs = 4 if mode == "double" else 2
    leg_prices = {}  # (pos_idx, leg_label) -> mid
    for cid, mappings in needed.items():
        mid = prices.get(cid)
        if mid is not None:
            for pos_idx, leg_label in mappings:
                leg_prices[(pos_idx, leg_label)] = mid

    total_mtm = 0.0
    any_priced = False
    for i, pos in enumerate(positions):
        fc = leg_prices.get((i, "front_call"))
        bc = leg_prices.get((i, "back_call"))

        if mode == "double":
            fp = leg_prices.get((i, "front_put"))
            bp = leg_prices.get((i, "back_put"))
            if fc is not None and bc is not None and fp is not None and bp is not None:
                spread_val = (bc - fc + bp - fp)
                total_mtm += spread_val * pos["contracts"] * CONTRACT_MULT
                any_priced = True
            else:
                # Fallback: use deployed (at-cost)
                total_mtm += pos["deployed"]
        else:
            if fc is not None and bc is not None:
                spread_val = bc - fc
                total_mtm += spread_val * pos["contracts"] * CONTRACT_MULT
                any_priced = True
            else:
                total_mtm += pos["deployed"]

    return total_mtm if any_priced else None


def run_portfolio(df, mode="double"):
    """
    Run single combined portfolio across all combos.
    mode: "double" (call+put) or "single" (call only)
    """
    if mode == "double":
        sub = df.dropna(subset=["double_ret", "combined_cost"]).copy()
        sub = sub[sub["combined_cost"] > 0]
        ret_col = "double_ret"
        cost_col = "combined_cost"
        n_legs = 4                # call front + call back + put front + put back
        label = "Double Calendar"
    else:
        sub = df.copy()
        ret_col = "ret"
        cost_col = "spread_cost"
        n_legs = 2                # call front + call back
        label = "Single Calendar (Call)"

    # Per-trade slippage & commission scaled by number of legs
    slippage_entry = SLIPPAGE_PER_LEG * n_legs    # $0.10 (2-leg) or $0.20 (4-leg)
    slippage_exit  = SLIPPAGE_PER_LEG * n_legs
    comm_entry     = COMMISSION_PER_LEG * n_legs   # $1.30 (2-leg) or $2.60 (4-leg)
    comm_exit      = COMMISSION_PER_LEG * n_legs

    # Filters (keep all FF > 0 for fill pool; FF >= threshold gets priority)
    sub = sub[sub[cost_col] >= MIN_SPREAD_COST].copy()
    # BA% liquidity filter — match live execution quality (10% max)
    if "ba_pct" in sub.columns:
        sub = sub[sub["ba_pct"].fillna(1.0) <= MAX_BA_PCT].copy()
    # Keep any positive FF (backwardation) — threshold applied as priority at entry
    sub = sub[sub["ff"] > 0].copy()

    if len(sub) == 0:
        log.warning("  No trades after filters for %s", label)
        return None

    # Tag trades above threshold for priority
    ff_above = pd.Series(False, index=sub.index)
    for combo_name, thresh in FF_THRESHOLD_MAP.items():
        ff_above |= (sub["combo"] == combo_name) & (sub["ff"] >= thresh)
    known = set(FF_THRESHOLD_MAP.keys())
    ff_above |= (~sub["combo"].isin(known)) & (sub["ff"] >= FF_THRESHOLD)
    sub["above_thresh"] = ff_above

    n_above = ff_above.sum()
    n_below = len(sub) - n_above
    log.info("  FF >= threshold: %s trades (priority)", f"{n_above:,}")
    log.info("  FF > 0 (fill):   %s trades", f"{n_below:,}")

    # Stats per combo (above threshold only)
    above = sub[sub["above_thresh"]]
    for combo in sorted(above["combo"].unique()):
        c = above[above["combo"] == combo]
        log.info("  %s (>=thresh): %s trades, mean ret=%.3f, wr=%.1f%%",
                 combo, f"{len(c):,}", c[ret_col].mean(), (c[ret_col] > 0).mean() * 100)
    log.info("  TOTAL pool: %s trades", f"{len(sub):,}")

    # Build date-indexed lookup (all combos together)
    trades_by_entry = {d: g for d, g in sub.groupby("entry_dt")}

    # ── MTM setup ──
    conn = sqlite3.connect(str(DB))
    cid_cache = _build_contract_id_cache(conn, sub, mode)
    eod_dates = _get_eod_dates(conn)

    # ── Day-by-day simulation ──
    cash = INITIAL_CAPITAL
    positions = []
    daily_log = []
    trade_log = []
    kelly_history = []
    last_mtm_invested = 0.0   # carry-forward for holidays/missing data

    # Continuous business day calendar
    all_event_dates = sorted(
        set(sub["entry_dt"].unique()) | set(sub["exit_dt"].unique()))
    d0 = all_event_dates[0]
    d1 = all_event_dates[-1]
    all_dates = pd.bdate_range(d0, d1).tolist()

    for d in all_dates:
        # ─── 1. CLOSE expired positions ───
        still_open = []
        for pos in positions:
            if pos["exit_dt"] <= d:
                n_cts = pos["contracts"]
                # True P&L = (mid_exit - mid_entry - slip_entry - slip_exit) * shares
                #            - (comm_entry + comm_exit) * contracts
                pnl_per_share = (pos["exit_val_per_share"] - pos["cost_per_share"]
                                 - slippage_entry - slippage_exit)
                pnl = (pnl_per_share * n_cts * CONTRACT_MULT
                       - n_cts * (comm_entry + comm_exit))

                deployed = pos["deployed"]
                cash += deployed + pnl

                ret_pct = pnl / deployed if deployed > 0 else 0
                trade_log.append({
                    "entry_date": pos["entry_dt"],
                    "exit_date": pos["exit_dt"],
                    "ticker": pos["ticker"],
                    "combo": pos["combo"],
                    "ff": pos["ff"],
                    "contracts": n_cts,
                    "cost_per_contract": deployed / n_cts,
                    "pnl": pnl,
                    "ret_pct": ret_pct,
                })
                kelly_history.append(ret_pct)
            else:
                still_open.append(pos)
        positions = still_open

        # ─── 2. ENTER new positions (priority: FF>=thresh, then fill) ───
        slots = MAX_POSITIONS - len(positions)

        if slots > 0 and d in trades_by_entry:
            candidates = trades_by_entry[d]
            # No duplicate tickers: skip already-open, keep best FF per ticker
            open_tickers = {p["ticker"] for p in positions}
            candidates = candidates[~candidates["root"].isin(open_tickers)]
            candidates = candidates.sort_values("ff", ascending=False).drop_duplicates(subset=["root"], keep="first")
            # Two-pass entry: 1) above-threshold first, 2) fill remaining slots
            priority = candidates[candidates["above_thresh"]].nlargest(slots, "ff")
            remaining_slots = slots - len(priority)
            if remaining_slots > 0:
                fill = candidates[~candidates.index.isin(priority.index)].nlargest(remaining_slots, "ff")
                candidates = pd.concat([priority, fill])
            else:
                candidates = priority

            account_value = cash + sum(p["deployed"] for p in positions)

            # Walk-forward Kelly
            if len(kelly_history) >= MIN_KELLY_TRADES:
                kh = np.array(kelly_history)
                mu_k = kh.mean()
                var_k = kh.var()
                if var_k > 0 and mu_k > 0:
                    kelly_f = min(KELLY_FRAC * mu_k / var_k, 1.0)
                else:
                    kelly_f = DEFAULT_ALLOC
            else:
                kelly_f = DEFAULT_ALLOC

            # Two-pass sizing: guarantee all positions, then add Kelly extras
            slots = MAX_POSITIONS - len(positions)
            n_candidates = min(len(candidates), slots)
            kelly_budget = kelly_f * account_value

            # Pre-compute cost per contract for each candidate
            cand_list = []
            for _, trade in candidates.head(n_candidates).iterrows():
                cpc = ((trade[cost_col] + slippage_entry) * CONTRACT_MULT
                       + comm_entry)
                if cpc > 0:
                    cand_list.append((trade, cpc))

            # Pass 1: reserve 1 contract per position (minimum)
            min_total = sum(cpc for _, cpc in cand_list)
            # Pass 2: distribute remaining Kelly budget as extra contracts
            #   Candidates already sorted by FF desc — highest FF gets extras first
            extra_budget = max(0, kelly_budget - min_total)
            extras = [0] * len(cand_list)

            if extra_budget > 0:
                for idx in range(len(cand_list)):
                    _, cpc_i = cand_list[idx]
                    add = min(int(extra_budget / cpc_i), MAX_CONTRACTS - 1)
                    if add > 0:
                        extras[idx] = add
                        extra_budget -= add * cpc_i
                    if extra_budget <= 0:
                        break

            for i, (trade, cost_per_contract) in enumerate(cand_list):
                if len(positions) >= MAX_POSITIONS:
                    break

                contracts = 1 + extras[i]

                total_deployed = contracts * cost_per_contract

                if total_deployed > cash:
                    contracts = max(1, int(cash / cost_per_contract))
                    if contracts < 1:
                        continue
                    total_deployed = contracts * cost_per_contract

                cash -= total_deployed

                # Exit value per share from stored return
                exit_val_per_share = trade[cost_col] * (1 + trade[ret_col])

                positions.append({
                    "ticker": trade["root"],
                    "combo": trade["combo"],
                    "entry_dt": d,
                    "exit_dt": trade["exit_dt"],
                    "contracts": contracts,
                    "cost_per_share": trade[cost_col],
                    "exit_val_per_share": exit_val_per_share,
                    "deployed": total_deployed,
                    "ff": trade["ff"],
                    "front_exp": int(trade["front_exp"]),
                    "back_exp": int(trade["back_exp"]),
                    "front_strike": trade["front_strike"],
                    "put_strike": trade.get("put_strike", trade["front_strike"]),
                    "put_back_strike": trade.get("put_back_strike", trade["front_strike"]),
                })

        # ─── 3. RECORD daily state ───
        invested = sum(p["deployed"] for p in positions)
        account_value = cash + invested

        # MTM valuation
        date_int = int(d.strftime("%Y%m%d"))
        if positions and date_int in eod_dates:
            mtm_val = _compute_mtm_value(positions, date_int, conn,
                                         cid_cache, mode)
            if mtm_val is not None:
                mtm_invested = mtm_val
                last_mtm_invested = mtm_invested
            else:
                mtm_invested = last_mtm_invested
        else:
            mtm_invested = last_mtm_invested if positions else 0.0

        account_mtm = cash + mtm_invested

        daily_log.append({
            "date": d,
            "account": account_value,
            "account_mtm": account_mtm,
            "cash": cash,
            "invested": invested,
            "invested_mtm": mtm_invested,
            "n_positions": len(positions),
        })

    conn.close()

    daily = pd.DataFrame(daily_log)
    trades = pd.DataFrame(trade_log) if trade_log else pd.DataFrame()

    return {
        "daily": daily,
        "trades": trades,
        "label": label,
        "mode": mode,
    }


def print_stats(result):
    """Print performance statistics."""
    daily = result["daily"]
    trades = result["trades"]
    label = result["label"]

    log.info("\n%s", "=" * 60)
    log.info("PORTFOLIO: %s", label)
    log.info("$%s | %d max positions | Half Kelly | slippage+commission",
             f"{INITIAL_CAPITAL:,}", MAX_POSITIONS)
    log.info("%s", "=" * 60)

    if trades.empty:
        log.warning("  No trades executed")
        return

    # Period
    d0, d1 = daily["date"].min(), daily["date"].max()
    n_years = max((d1 - d0).days / 365.25, 0.1)

    # Account
    final = daily["account"].iloc[-1]
    cagr = (final / INITIAL_CAPITAL) ** (1 / n_years) - 1
    log.info("  Period: %s -> %s (%.1fy)", d0.date(), d1.date(), n_years)
    log.info("  Account: $%s -> $%s (CAGR=%.1f%%)",
             f"{INITIAL_CAPITAL:,.0f}", f"{final:,.0f}", cagr * 100)

    # Sharpe
    daily_ret = daily["account"].pct_change().dropna()
    n_periods_per_year = len(daily_ret) / n_years
    sharpe = 0
    if len(daily_ret) > 20 and daily_ret.std() > 0:
        sharpe = daily_ret.mean() / daily_ret.std() * np.sqrt(n_periods_per_year)
        log.info("  Sharpe (daily, ann): %.2f", sharpe)

    # Max Drawdown
    peak = daily["account"].cummax()
    dd = (daily["account"] - peak) / peak
    max_dd = dd.min()
    log.info("  Max Drawdown: %.1f%%", max_dd * 100)

    # ── Mark-to-Market stats ──
    if "account_mtm" in daily.columns:
        final_mtm = daily["account_mtm"].iloc[-1]
        cagr_mtm = (final_mtm / INITIAL_CAPITAL) ** (1 / n_years) - 1
        daily_ret_mtm = daily["account_mtm"].pct_change().dropna()
        sharpe_mtm = 0
        if len(daily_ret_mtm) > 20 and daily_ret_mtm.std() > 0:
            sharpe_mtm = (daily_ret_mtm.mean() / daily_ret_mtm.std()
                          * np.sqrt(n_periods_per_year))
        peak_mtm = daily["account_mtm"].cummax()
        dd_mtm = (daily["account_mtm"] - peak_mtm) / peak_mtm
        max_dd_mtm = dd_mtm.min()
        log.info("\n  -- Mark-to-Market --")
        log.info("  MTM Account: $%s -> $%s (CAGR=%.1f%%)",
                 f"{INITIAL_CAPITAL:,.0f}", f"{final_mtm:,.0f}", cagr_mtm * 100)
        log.info("  MTM Sharpe (daily, ann): %.2f", sharpe_mtm)
        log.info("  MTM Max Drawdown: %.1f%%", max_dd_mtm * 100)

    # Trade stats
    n_trades = len(trades)
    wins = (trades["pnl"] > 0).sum()
    win_rate = wins / n_trades if n_trades > 0 else 0
    log.info("\n  Trades: %s (%d/year)", f"{n_trades:,}", n_trades / n_years)
    log.info("  Win rate: %.1f%%", win_rate * 100)
    log.info("  Avg P&L/trade: $%s", f"{trades['pnl'].mean():,.0f}")
    log.info("  Median P&L/trade: $%s", f"{trades['pnl'].median():,.0f}")
    log.info("  Avg contracts: %.1f", trades["contracts"].mean())
    log.info("  Total P&L: $%s", f"{trades['pnl'].sum():,.0f}")

    # P&L distribution
    log.info("\n  P&L distribution:")
    for pct in [0.05, 0.25, 0.50, 0.75, 0.95]:
        log.info("    %.0f%%: $%s", pct * 100, f"{trades['pnl'].quantile(pct):,.0f}")

    # Positions & Capital Deployed
    log.info("\n  Positions: mean=%.1f, max=%d",
             daily["n_positions"].mean(), daily["n_positions"].max())
    deployed_pct = daily["invested"] / daily["account"] * 100
    log.info("  Capital deployed: mean=$%s (%.1f%%), max=$%s (%.1f%%)",
             f"{daily['invested'].mean():,.0f}", deployed_pct.mean(),
             f"{daily['invested'].max():,.0f}", deployed_pct.max())
    log.info("  Cash reserve: mean=$%s (%.1f%%)",
             f"{daily['cash'].mean():,.0f}",
             (daily["cash"] / daily["account"] * 100).mean())

    # Combo breakdown
    if "combo" in trades.columns:
        log.info("\n  By combo:")
        for combo, grp in trades.groupby("combo"):
            wr = (grp["pnl"] > 0).mean()
            log.info("    %s: %4d trades, P&L=$%10s, wr=%.0f%%",
                     combo, len(grp), f"{grp['pnl'].sum():,.0f}", wr * 100)

    # Monthly returns
    daily_cp = daily.copy()
    daily_cp["month"] = daily_cp["date"].dt.to_period("M")
    monthly = daily_cp.groupby("month")["account"].last()
    monthly_ret = monthly.pct_change().dropna()
    if len(monthly_ret) > 0:
        log.info("\n  Monthly returns:")
        log.info("    mean=%.2f%%, std=%.2f%%",
                 monthly_ret.mean() * 100, monthly_ret.std() * 100)
        log.info("    worst=%.2f%%, best=%.2f%%",
                 monthly_ret.min() * 100, monthly_ret.max() * 100)
        n_pos = (monthly_ret > 0).sum()
        log.info("    positive months: %d/%d (%.0f%%)",
                 n_pos, len(monthly_ret), n_pos / len(monthly_ret) * 100)

    # Year-by-year
    trades_cp = trades.copy()
    trades_cp["exit_year"] = pd.to_datetime(trades_cp["exit_date"]).dt.year
    log.info("\n  Year-by-year:")
    for year, grp in trades_cp.groupby("exit_year"):
        wr = (grp["pnl"] > 0).mean()
        log.info("    %s: %4d trades, P&L=$%10s, avg=$%7s, wr=%.0f%%",
                 year, len(grp), f"{grp['pnl'].sum():,.0f}",
                 f"{grp['pnl'].mean():,.0f}", wr * 100)


def plot_results(result):
    """Generate backtest charts."""
    daily = result["daily"]
    trades = result["trades"]
    label = result["label"]
    tag = result["mode"]

    if trades.empty:
        return

    # 1. Equity Curve + Drawdown (At-Cost and MTM overlay)
    has_mtm = "account_mtm" in daily.columns
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 8), height_ratios=[3, 1],
                                    sharex=True)
    ax1.plot(daily["date"], daily["account"] / 1000, color="C0", linewidth=1.5,
             label="At-Cost")
    if has_mtm:
        ax1.plot(daily["date"], daily["account_mtm"] / 1000, color="C1",
                 linewidth=1.5, alpha=0.85, label="Mark-to-Market")
    ax1.axhline(INITIAL_CAPITAL / 1000, color="gray", linestyle="--", alpha=0.5)
    ax1.set_ylabel("Account ($K)")
    ax1.set_title(f"Portfolio: {label}\n"
                  f"${INITIAL_CAPITAL/1000:.0f}K start | {MAX_POSITIONS} max pos | "
                  f"Half Kelly | slippage + commissions")
    ax1.legend(loc="upper left")
    ax1.grid(True, alpha=0.3)

    peak = daily["account"].cummax()
    dd = (daily["account"] - peak) / peak * 100
    ax2.fill_between(daily["date"], dd, 0, color="C0", alpha=0.25,
                     label="At-Cost DD")
    if has_mtm:
        peak_mtm = daily["account_mtm"].cummax()
        dd_mtm = (daily["account_mtm"] - peak_mtm) / peak_mtm * 100
        ax2.fill_between(daily["date"], dd_mtm, 0, color="C1", alpha=0.25,
                         label="MTM DD")
    ax2.set_ylabel("Drawdown (%)")
    ax2.set_xlabel("Date")
    ax2.legend(loc="lower left", fontsize=8)
    ax2.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(str(OUT / f"bt_portfolio_{tag}.png"), dpi=150)
    plt.close(fig)

    # 2. Position count + Capital deployed
    fig, (ax_pos, ax_dep) = plt.subplots(2, 1, figsize=(14, 6), sharex=True)

    ax_pos.plot(daily["date"], daily["n_positions"], color="C1", alpha=0.8)
    ax_pos.axhline(MAX_POSITIONS, color="red", linestyle="--", alpha=0.5,
                   label=f"Max = {MAX_POSITIONS}")
    ax_pos.set_ylabel("Active Positions")
    ax_pos.set_title(f"Portfolio: {label} — Positions & Capital Deployed")
    ax_pos.legend(loc="upper left")
    ax_pos.grid(True, alpha=0.3)

    deployed_pct = daily["invested"] / daily["account"] * 100
    ax_dep.fill_between(daily["date"], deployed_pct, 0,
                        color="C0", alpha=0.3, label="Deployed %")
    ax_dep.plot(daily["date"], deployed_pct, color="C0", linewidth=1)
    ax_dep.set_ylabel("Capital Deployed (%)")
    ax_dep.set_xlabel("Date")
    ax_dep.set_ylim(0, 105)
    ax_dep.legend(loc="upper left")
    ax_dep.grid(True, alpha=0.3)

    fig.tight_layout()
    fig.savefig(str(OUT / f"bt_positions_{tag}.png"), dpi=150)
    plt.close(fig)

    # 3. Monthly P&L bars
    trades_cp = trades.copy()
    trades_cp["exit_month"] = pd.to_datetime(trades_cp["exit_date"]).dt.to_period("M")
    monthly_pnl = trades_cp.groupby("exit_month")["pnl"].sum()
    fig, ax = plt.subplots(figsize=(14, 5))
    colors = ["C2" if v > 0 else "C3" for v in monthly_pnl.values]
    ax.bar(range(len(monthly_pnl)), monthly_pnl.values / 1000,
           color=colors, edgecolor="white", alpha=0.8)
    step = max(1, len(monthly_pnl) // 12)
    ax.set_xticks(range(0, len(monthly_pnl), step))
    labels = [str(m) for m in monthly_pnl.index]
    ax.set_xticklabels([labels[i] for i in range(0, len(labels), step)],
                       rotation=45, ha="right")
    ax.axhline(0, color="gray", linestyle="--", alpha=0.5)
    ax.set_ylabel("Monthly P&L ($K)")
    ax.set_title(f"Portfolio: {label} — Monthly P&L")
    ax.grid(True, alpha=0.3, axis="y")
    fig.tight_layout()
    fig.savefig(str(OUT / f"bt_monthly_{tag}.png"), dpi=150)
    plt.close(fig)

    log.info("  Charts saved: bt_portfolio_%s.png, bt_positions_%s.png, bt_monthly_%s.png",
             tag, tag, tag)


def save_track_record(result):
    """Save daily equity curve + trades to CSV for track_record.py to load."""
    daily = result["daily"]
    trades = result["trades"]

    # Daily equity curve: date, account, account_mtm, n_positions
    daily_out = daily[["date", "account", "n_positions"]].copy()
    if "account_mtm" in daily.columns:
        daily_out["account_mtm"] = daily["account_mtm"]
    daily_out.to_csv(str(OUT / "backtest_daily.csv"), index=False)
    log.info("  Saved: backtest_daily.csv (%d days)", len(daily_out))

    # Trades with backtest-computed sizing
    if not trades.empty:
        trades.to_csv(str(OUT / "backtest_trades.csv"), index=False)
        log.info("  Saved: backtest_trades.csv (%d trades)", len(trades))


# ══════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    df = load_data()

    log.info("\nParameters:")
    log.info("  Capital:     $%s", f"{INITIAL_CAPITAL:,}")
    log.info("  Max pos:     %d", MAX_POSITIONS)
    log.info("  Kelly:       %s (Half Kelly)", KELLY_FRAC)
    log.info("  FF thresh:   %s (PDF formula, per combo)", FF_THRESHOLD_MAP)
    log.info("  Min cost:    $%s/share", MIN_SPREAD_COST)
    log.info("  Slippage:    $%s/leg/share (2-leg=$%s, 4-leg=$%s)",
             SLIPPAGE_PER_LEG, SLIPPAGE_PER_LEG * 2, SLIPPAGE_PER_LEG * 4)
    log.info("  Commission:  $%s/leg/contract (2-leg=$%s, 4-leg=$%s)",
             COMMISSION_PER_LEG, COMMISSION_PER_LEG * 2, COMMISSION_PER_LEG * 4)
    log.info("  Max cts:     %d/position", MAX_CONTRACTS)

    # ── Double Calendar Portfolio (all combos combined) ──
    log.info("\n%s", "#" * 60)
    log.info("# DOUBLE CALENDAR PORTFOLIO (all combos, top 20 by FF)")
    log.info("%s", "#" * 60)
    result_dbl = run_portfolio(df, mode="double")
    if result_dbl:
        print_stats(result_dbl)
        plot_results(result_dbl)
        save_track_record(result_dbl)

    # ── Single Calendar Portfolio for comparison ──
    log.info("\n%s", "#" * 60)
    log.info("# SINGLE CALENDAR PORTFOLIO (all combos, top 20 by FF)")
    log.info("%s", "#" * 60)
    result_sgl = run_portfolio(df, mode="single")
    if result_sgl:
        print_stats(result_sgl)
        plot_results(result_sgl)

    log.info("\n%s", "=" * 60)
    log.info("BACKTEST COMPLETE")
    log.info("%s", "=" * 60)
