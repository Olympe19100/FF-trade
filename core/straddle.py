"""
Earnings Vol Ramp — Pre-Earnings Long Straddle Strategy

Replicates OQuants methodology (21K+ trades):
  Buy ATM straddle ~14 days before earnings,
  sell day before announcement.
  Edge = event vol repricing, not the actual earnings move.

Walk-forward OLS regression with 4 predictors:
  1. implied / last_implied
  2. implied - last_realized
  3. implied - avg_realized
  4. implied / avg_implied

Usage:
    from core.straddle import compute_straddle_analytics
    result = compute_straddle_analytics()
"""

import sqlite3
import pickle
import hashlib
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from pathlib import Path
from datetime import date, datetime, timedelta
from sklearn.linear_model import LinearRegression
from scipy import stats as sp_stats

from core.config import (
    DB, CACHE, OUTPUT,
    COMMISSION_LEG, CONTRACT_MULT, MIN_KELLY_TRADES, DEFAULT_ALLOC,
    SLIPPAGE_BUFFER, get_logger,
)
from core.pricing import bs_price, implied_vol_scalar

log = get_logger(__name__)

# ── Straddle-specific parameters (OQuants PPTX slides 19, 36, 41) ──
ENTRY_WINDOW     = (10, 18)    # calendar days before earnings (slide 19: +/-4 days)
BA_FILL_FRACTION = 0.10             # fraction of bid-ask spread paid as slippage (10%)
MAX_POSITIONS    = 10          # straddle-specific (different from calendar's 20)
MIN_ALLOC_TRADE  = 0.02        # 2% min per trade (slide 41)
MAX_ALLOC_TRADE  = 0.06        # 6% max per trade (slide 41)
MAX_CONTRACTS    = 10
INITIAL_CAPITAL  = 100_000
MIN_OPT_VOLUME   = 20_000      # 20K avg daily option volume (slide 19)
AVG_VOL_LOOKBACK = 20          # 20 trading days for avg daily volume

# ── Dark theme colors ──
DARK_BG    = "#0d1117"
DARK_CARD  = "#1c2333"
DARK_TEXT  = "#e6edf3"
DARK_GRID  = "#30363d"
ACCENT     = "#58a6ff"
GREEN      = "#3fb950"
RED        = "#f85149"
YELLOW     = "#d29922"


# ═══════════════════════════════════════════════════════════════
# Phase 1 — Data Pipeline
# ═══════════════════════════════════════════════════════════════

def third_friday(year: int, month: int) -> date:
    """Return the 3rd Friday of the given month/year."""
    # First day of month
    d = date(year, month, 1)
    # Day of week: Monday=0, Friday=4
    first_friday = d.day + (4 - d.weekday()) % 7
    return date(year, month, first_friday + 14)


def next_monthly_expiration(after_date_int: int) -> int:
    """Find nearest monthly expiration (3rd Friday) on or after date."""
    d = datetime.strptime(str(after_date_int), "%Y%m%d").date()
    tf = third_friday(d.year, d.month)
    if tf >= d:
        return int(tf.strftime("%Y%m%d"))
    # Next month
    if d.month == 12:
        tf = third_friday(d.year + 1, 1)
    else:
        tf = third_friday(d.year, d.month + 1)
    return int(tf.strftime("%Y%m%d"))


def _load_prices() -> pd.DataFrame:
    """Load underlying prices from bt_prices.pkl."""
    prices = pd.read_pickle(str(CACHE / "bt_prices.pkl"))
    # Ensure index is DatetimeIndex
    if not isinstance(prices.index, pd.DatetimeIndex):
        prices.index = pd.to_datetime(prices.index)
    return prices


def _int_to_date(d: int) -> date:
    return datetime.strptime(str(d), "%Y%m%d").date()


def _date_to_int(d) -> int:
    if isinstance(d, (date, datetime)):
        return int(d.strftime("%Y%m%d"))
    return int(pd.Timestamp(d).strftime("%Y%m%d"))


def build_earnings_straddle_history(
    min_date: int = 20160101,
    max_date: int = 20260401,
    force_rebuild: bool = False,
) -> pd.DataFrame:
    """Build complete ATM straddle history around all earnings events.

    Uses DB volume data to identify ATM strike (no bt_prices for strikes).
    Underlying inferred via put-call parity: S = K + C_mid - P_mid.
    Realized move from bt_prices (split-adjusted ratios are invariant).

    Cached to cache/straddle_history.pkl.
    """
    cache_path = CACHE / "straddle_history.pkl"
    if cache_path.exists() and not force_rebuild:
        log.info("Loading cached straddle history...")
        return pd.read_pickle(str(cache_path))

    log.info("Building earnings straddle history (first run, ~5-10 min)...")
    prices = _load_prices()

    # Trading dates index for lookups
    trading_dates = sorted([_date_to_int(d) for d in prices.index])
    trading_dates_arr = np.array(trading_dates)

    conn = sqlite3.connect(str(DB))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA cache_size=-512000")  # 512MB cache

    # Load all earnings events
    earnings = pd.read_sql_query(
        f"SELECT root, report_date, before_after FROM earnings "
        f"WHERE report_date >= {min_date} AND report_date <= {max_date} "
        f"ORDER BY root, report_date",
        conn
    )
    log.info("%d earnings events in range", len(earnings))

    # Get all tickers with options
    tickers_with_options = set(
        pd.read_sql_query("SELECT DISTINCT root FROM contracts", conn)["root"]
    )

    results = []
    n_skip_no_options = 0
    n_skip_no_price = 0
    n_skip_no_contracts = 0
    n_skip_no_data = 0
    n_skip_low_volume = 0
    processed = 0

    for root, grp in earnings.groupby("root"):
        if root not in tickers_with_options:
            n_skip_no_options += len(grp)
            continue

        # bt_prices needed for realized move only (ratios are split-invariant)
        has_prices = root in prices.columns
        root_prices = prices[root].dropna() if has_prices else pd.Series(dtype=float)

        for _, row in grp.iterrows():
            report_date = int(row["report_date"])
            before_after = row.get("before_after", "")

            # --- Entry/exit dates ---
            target_entry = _int_to_date(report_date) - timedelta(days=14)
            target_entry_int = _date_to_int(target_entry)
            window_start = _date_to_int(
                _int_to_date(report_date) - timedelta(days=ENTRY_WINDOW[1])
            )
            window_end = _date_to_int(
                _int_to_date(report_date) - timedelta(days=ENTRY_WINDOW[0])
            )
            idx_s = np.searchsorted(trading_dates_arr, window_start, side="left")
            idx_e = np.searchsorted(trading_dates_arr, window_end, side="right")
            window_trading = trading_dates_arr[idx_s:idx_e]

            if len(window_trading) == 0:
                n_skip_no_data += 1
                continue

            diffs = np.abs(window_trading - target_entry_int)
            entry_date = int(window_trading[diffs.argmin()])

            # Exit = last trading session BEFORE the announcement
            # AMC (after market close): can sell on report_date (results after close)
            # BMO (before market open): must sell on report_date - 1 (results before open)
            is_amc = str(before_after).lower().startswith("after")
            idx_report = np.searchsorted(trading_dates_arr, report_date, side="left")
            if is_amc:
                # AMC: exit on report_date itself (sell during session, results after close)
                if idx_report < len(trading_dates_arr) and trading_dates_arr[idx_report] == report_date:
                    exit_date = report_date
                elif idx_report > 0:
                    exit_date = int(trading_dates_arr[idx_report - 1])
                else:
                    n_skip_no_data += 1
                    continue
            else:
                # BMO or unknown: exit day before report_date
                if idx_report == 0:
                    n_skip_no_data += 1
                    continue
                exit_date = int(trading_dates_arr[idx_report - 1])

            if exit_date <= entry_date:
                n_skip_no_data += 1
                continue

            # --- Find nearest monthly expiration after the earnings event ---
            # OQuants slide 19: "Use nearest monthly (third friday) expiry after event"
            exp_int = next_monthly_expiration(report_date)

            # Verify this expiration exists in the DB for this ticker
            exp_check = conn.execute(
                "SELECT 1 FROM contracts WHERE root = ? AND expiration = ? LIMIT 1",
                (root, exp_int)
            ).fetchone()
            if not exp_check:
                n_skip_no_contracts += 1
                continue

            # --- Find ATM strike: where |call_mid - put_mid| is minimal ---
            # Get top 10 most liquid calls, then check put at each strike
            top_calls = conn.execute(
                "SELECT c.contract_id, c.strike, e.bid, e.ask "
                "FROM contracts c "
                "JOIN eod_history e ON c.contract_id = e.contract_id "
                "WHERE c.root = ? AND c.expiration = ? AND c.right = 'C' "
                "AND e.date = ? AND e.bid > 0 AND e.ask > 0 "
                "ORDER BY e.volume DESC LIMIT 10",
                (root, exp_int, entry_date)
            ).fetchall()

            if not top_calls:
                n_skip_no_data += 1
                continue

            # For each candidate, find matching put and pick min |C-P|
            best_atm = None
            best_diff = float("inf")
            for c_row in top_calls:
                c_id, c_strike, c_bid, c_ask = c_row
                c_mid = (c_bid + c_ask) / 2

                p_row = conn.execute(
                    "SELECT c.contract_id, e.bid, e.ask "
                    "FROM contracts c "
                    "JOIN eod_history e ON c.contract_id = e.contract_id "
                    "WHERE c.root = ? AND c.expiration = ? AND c.right = 'P' "
                    "AND c.strike = ? AND e.date = ? AND e.bid > 0 AND e.ask > 0 "
                    "LIMIT 1",
                    (root, exp_int, c_strike, entry_date)
                ).fetchone()

                if not p_row:
                    continue

                p_id, p_bid, p_ask = p_row
                p_mid = (p_bid + p_ask) / 2
                diff = abs(c_mid - p_mid)

                if diff < best_diff:
                    best_diff = diff
                    best_atm = {
                        "strike": c_strike,
                        "call_id": c_id, "call_mid": c_mid,
                        "call_bid": c_bid, "call_ask": c_ask,
                        "put_id": p_id, "put_mid": p_mid,
                        "put_bid": p_bid, "put_ask": p_ask,
                    }

            if not best_atm:
                n_skip_no_data += 1
                continue

            # --- Volume filter: >= 20K avg daily option volume (slide 19) ---
            # Compute average daily total option volume over last 20 trading days
            idx_entry = np.searchsorted(trading_dates_arr, entry_date, side="right")
            lookback_start = max(0, idx_entry - AVG_VOL_LOOKBACK)
            lookback_dates = trading_dates_arr[lookback_start:idx_entry]
            if len(lookback_dates) > 0:
                placeholders = ",".join(str(int(d)) for d in lookback_dates)
                vol_row = conn.execute(
                    f"SELECT SUM(e.volume), COUNT(DISTINCT e.date) "
                    f"FROM eod_history e "
                    f"JOIN contracts c ON e.contract_id = c.contract_id "
                    f"WHERE c.root = ? AND e.date IN ({placeholders})",
                    (root,)
                ).fetchone()
                total_vol = vol_row[0] if vol_row and vol_row[0] else 0
                n_days = vol_row[1] if vol_row[1] else 1
                avg_daily_vol = total_vol / n_days
            else:
                avg_daily_vol = 0
            if avg_daily_vol < MIN_OPT_VOLUME:
                n_skip_low_volume += 1
                continue

            atm_strike_raw = best_atm["strike"]      # millidollars in DB
            actual_strike = atm_strike_raw / 1000.0   # convert to dollars
            call_mid_entry = best_atm["call_mid"]
            put_mid_entry = best_atm["put_mid"]
            call_cid = best_atm["call_id"]
            put_cid = best_atm["put_id"]

            if not call_cid or not put_cid:
                n_skip_no_contracts += 1
                continue

            # --- Get EXIT prices ---
            call_exit = conn.execute(
                "SELECT bid, ask FROM eod_history "
                "WHERE contract_id = ? AND date = ? AND bid > 0 AND ask > 0",
                (call_cid, exit_date)
            ).fetchone()
            put_exit = conn.execute(
                "SELECT bid, ask FROM eod_history "
                "WHERE contract_id = ? AND date = ? AND bid > 0 AND ask > 0",
                (put_cid, exit_date)
            ).fetchone()

            if not call_exit or not put_exit:
                n_skip_no_data += 1
                continue

            call_mid_exit = (call_exit[0] + call_exit[1]) / 2
            put_mid_exit = (put_exit[0] + put_exit[1]) / 2

            straddle_entry = call_mid_entry + put_mid_entry
            straddle_exit = call_mid_exit + put_mid_exit

            # Real bid/ask straddle prices (buy at ask, sell at bid)
            call_bid_entry = best_atm["call_bid"]
            call_ask_entry = best_atm["call_ask"]
            put_bid_entry = best_atm["put_bid"]
            put_ask_entry = best_atm["put_ask"]
            call_bid_exit, call_ask_exit = call_exit[0], call_exit[1]
            put_bid_exit, put_ask_exit = put_exit[0], put_exit[1]

            straddle_entry_ask = call_ask_entry + put_ask_entry  # what we pay
            straddle_exit_bid = call_bid_exit + put_bid_exit      # what we get

            if straddle_entry <= 1.00:  # Minimum $1 straddle price
                n_skip_no_data += 1
                continue

            # --- Underlying via put-call parity: S = K + C - P ---
            underlying_entry = actual_strike + call_mid_entry - put_mid_entry
            underlying_exit_pc = actual_strike + call_mid_exit - put_mid_exit
            underlying_exit = underlying_exit_pc if underlying_exit_pc > 0 else underlying_entry

            # --- ATM Implied Volatility via Black-Scholes inversion ---
            exp_dt = _int_to_date(exp_int)
            entry_dt = _int_to_date(entry_date)
            exit_dt = _int_to_date(exit_date)
            T_entry = max((exp_dt - entry_dt).days, 1) / 365.0
            T_exit = max((exp_dt - exit_dt).days, 1) / 365.0

            call_iv_entry = implied_vol_scalar(call_mid_entry, underlying_entry, actual_strike,
                                   T_entry, r=0.0, right='C')
            put_iv_entry = implied_vol_scalar(put_mid_entry, underlying_entry, actual_strike,
                                  T_entry, r=0.0, right='P')
            # Average call/put IV (skip NaN legs)
            ivs_entry = [v for v in [call_iv_entry, put_iv_entry] if not np.isnan(v)]
            if not ivs_entry:
                n_skip_no_data += 1
                continue
            atm_iv_entry = np.mean(ivs_entry)

            # Exit IV (for tracking IV change over holding period)
            if underlying_exit_pc > 0:
                call_iv_exit = implied_vol_scalar(call_mid_exit, underlying_exit_pc,
                                      actual_strike, T_exit, r=0.0, right='C')
                put_iv_exit = implied_vol_scalar(put_mid_exit, underlying_exit_pc,
                                     actual_strike, T_exit, r=0.0, right='P')
                ivs_exit = [v for v in [call_iv_exit, put_iv_exit] if not np.isnan(v)]
                atm_iv_exit = np.mean(ivs_exit) if ivs_exit else np.nan
            else:
                atm_iv_exit = np.nan

            # --- Event vol decomposition (OQuants slides 9-12) ---
            # Find second monthly expiry (next after exp1) for forward vol
            # Forward vol between exp1 and exp2 "cancels" the shared event,
            # yielding ambient vol. Then event_var = total_var - ambient_var.
            exp2_dt = third_friday(exp_dt.year + (1 if exp_dt.month == 12 else 0),
                                   (exp_dt.month % 12) + 1)
            exp2_int = _date_to_int(exp2_dt)
            T2_entry = max((exp2_dt - entry_dt).days, 1) / 365.0

            # Try to get IV at exp2 for the same ATM strike
            c2_row = conn.execute(
                "SELECT e.bid, e.ask FROM contracts c "
                "JOIN eod_history e ON c.contract_id = e.contract_id "
                "WHERE c.root = ? AND c.expiration = ? AND c.right = 'C' "
                "AND c.strike = ? AND e.date = ? AND e.bid > 0 AND e.ask > 0 "
                "LIMIT 1",
                (root, exp2_int, atm_strike_raw, entry_date)
            ).fetchone()
            p2_row = conn.execute(
                "SELECT e.bid, e.ask FROM contracts c "
                "JOIN eod_history e ON c.contract_id = e.contract_id "
                "WHERE c.root = ? AND c.expiration = ? AND c.right = 'P' "
                "AND c.strike = ? AND e.date = ? AND e.bid > 0 AND e.ask > 0 "
                "LIMIT 1",
                (root, exp2_int, atm_strike_raw, entry_date)
            ).fetchone()

            if c2_row and p2_row:
                c2_mid = (c2_row[0] + c2_row[1]) / 2
                p2_mid = (p2_row[0] + p2_row[1]) / 2
                civ2 = implied_vol_scalar(c2_mid, underlying_entry, actual_strike,
                                          T2_entry, r=0.0, right='C')
                piv2 = implied_vol_scalar(p2_mid, underlying_entry, actual_strike,
                                          T2_entry, r=0.0, right='P')
                ivs2 = [v for v in [civ2, piv2] if not np.isnan(v) and v > 0]
                if ivs2 and T2_entry > T_entry:
                    iv2 = np.mean(ivs2)
                    # Forward vol = ambient vol (slides 9-10)
                    fwd_var = (iv2**2 * T2_entry - atm_iv_entry**2 * T_entry) / (T2_entry - T_entry)
                    if fwd_var > 0:
                        ambient_iv = np.sqrt(fwd_var)
                        # Event variance (slide 11): total - ambient for non-event days
                        total_var = atm_iv_entry**2 * T_entry
                        ambient_var = ambient_iv**2 * max(T_entry - 1.0/365.0, 0)
                        event_var = max(total_var - ambient_var, 0)
                        # Implied move (slide 12): sqrt(event_var) × E[|Z|]
                        implied_move = np.sqrt(event_var) * np.sqrt(2.0 / np.pi)
                    else:
                        # Negative forward var → fallback to total IV
                        implied_move = atm_iv_entry * np.sqrt(T_entry)
                else:
                    implied_move = atm_iv_entry * np.sqrt(T_entry)
            else:
                # No second expiry data → fallback to total IV move
                implied_move = atm_iv_entry * np.sqrt(T_entry)

            # --- Realized move (from bt_prices, ratios are split-invariant) ---
            realized_move = np.nan
            if has_prices:
                if str(before_after).lower().startswith("after"):
                    pre_ts = pd.Timestamp(_int_to_date(report_date))
                    idx_r = np.searchsorted(trading_dates_arr, report_date, side="right")
                    if idx_r < len(trading_dates_arr):
                        post_ts = pd.Timestamp(_int_to_date(int(trading_dates_arr[idx_r])))
                        pre_c = root_prices.get(pre_ts, np.nan)
                        post_c = root_prices.get(post_ts, np.nan)
                        if not pd.isna(pre_c) and not pd.isna(post_c) and pre_c > 0:
                            realized_move = abs(post_c / pre_c - 1)
                else:
                    idx_r = np.searchsorted(trading_dates_arr, report_date, side="left")
                    if idx_r > 0:
                        pre_ts = pd.Timestamp(_int_to_date(int(trading_dates_arr[idx_r - 1])))
                        post_ts = pd.Timestamp(_int_to_date(report_date))
                        pre_c = root_prices.get(pre_ts, np.nan)
                        post_c = root_prices.get(post_ts, np.nan)
                        if not pd.isna(pre_c) and not pd.isna(post_c) and pre_c > 0:
                            realized_move = abs(post_c / pre_c - 1)

            # --- Returns ---
            gross_return = (straddle_exit - straddle_entry) / straddle_entry
            # Net return using fractional spread: realistic limit order fill
            # BA_FILL_FRACTION of the spread is the expected slippage
            entry_spread = straddle_entry_ask - straddle_entry  # full entry spread
            exit_spread = straddle_exit - straddle_exit_bid      # full exit spread
            slippage_cost = BA_FILL_FRACTION * (entry_spread + exit_spread)
            commission_cost = COMMISSION_LEG * 4 / CONTRACT_MULT  # $0.026/share
            actual_entry = straddle_entry + BA_FILL_FRACTION * entry_spread
            net_return = (straddle_exit - actual_entry - BA_FILL_FRACTION * exit_spread - commission_cost) / actual_entry

            results.append({
                "root": root,
                "report_date": report_date,
                "before_after": before_after,
                "expiration": exp_int,
                "strike": actual_strike,
                "entry_date": entry_date,
                "exit_date": exit_date,
                "entry_straddle": round(straddle_entry, 4),
                "exit_straddle": round(straddle_exit, 4),
                "entry_straddle_ask": round(straddle_entry_ask, 4),
                "exit_straddle_bid": round(straddle_exit_bid, 4),
                "entry_underlying": round(underlying_entry, 2),
                "exit_underlying": round(underlying_exit, 2),
                "implied_move": round(implied_move, 6),
                "realized_move": round(realized_move, 6) if not np.isnan(realized_move) else np.nan,
                "atm_iv_entry": round(atm_iv_entry, 6),
                "atm_iv_exit": round(atm_iv_exit, 6) if not np.isnan(atm_iv_exit) else np.nan,
                "gross_return": round(gross_return, 6),
                "net_return": round(net_return, 6),
                "entry_call_mid": round(call_mid_entry, 4),
                "entry_put_mid": round(put_mid_entry, 4),
                "exit_call_mid": round(call_mid_exit, 4),
                "exit_put_mid": round(put_mid_exit, 4),
            })

        processed += 1
        if processed % 50 == 0:
            log.info("[%d] %s... %d events so far", processed, root, len(results))

    conn.close()

    df = pd.DataFrame(results)
    log.info("Done: %d valid events", len(df))
    log.info("Skipped: %d no options, %d no price, %d no contracts, "
             "%d low volume, %d no data",
             n_skip_no_options, n_skip_no_price, n_skip_no_contracts,
             n_skip_low_volume, n_skip_no_data)

    if not df.empty:
        df = df.sort_values(["report_date", "root"]).reset_index(drop=True)
        df.to_pickle(str(cache_path))
        log.info("Cached to %s", cache_path)

    return df


# ═══════════════════════════════════════════════════════════════
# Phase 2 — Signal Computation
# ═══════════════════════════════════════════════════════════════

def compute_signals(history: pd.DataFrame) -> pd.DataFrame:
    """Compute the 4 OQuants regression signals (slides 24-27).

    Per-ticker expanding window:
    1. implied_move / last_implied_move   — move ratio vs previous earnings
    2. implied_move - last_realized       — gap vs previous realized
    3. implied_move / avg_implied_move    — move ratio vs expanding avg
    4. implied_move - avg_realized        — gap vs expanding avg realized
    """
    df = history.copy()
    df["sig_impl_vs_last_impl"] = np.nan
    df["sig_impl_minus_last_real"] = np.nan
    df["sig_impl_vs_avg_impl"] = np.nan
    df["sig_impl_minus_avg_real"] = np.nan

    for root, grp in df.groupby("root"):
        idx = grp.index
        implied = grp["implied_move"].values   # IV × sqrt(T) — OQuants "implied move"
        realized = grp["realized_move"].values

        for i in range(len(grp)):
            if i == 0:
                continue  # All 4 signals need at least 1 prior event

            cur_impl = implied[i]

            # Signal 1: implied_move / last_implied_move (slide 24)
            last_impl = implied[i - 1]
            if last_impl > 0:
                df.loc[idx[i], "sig_impl_vs_last_impl"] = cur_impl / last_impl

            # Signal 2: implied_move - last_realized (slide 25)
            last_real = realized[i - 1]
            if not np.isnan(last_real):
                df.loc[idx[i], "sig_impl_minus_last_real"] = cur_impl - last_real

            # Signal 3: implied_move / avg_implied_move (slide 26)
            past_impl = implied[:i]
            valid_impl = past_impl[~np.isnan(past_impl)]
            avg_impl = valid_impl.mean() if len(valid_impl) > 0 else 0
            if avg_impl > 0:
                df.loc[idx[i], "sig_impl_vs_avg_impl"] = cur_impl / avg_impl

            # Signal 4: implied_move - avg_realized (slide 27)
            past_realized = realized[:i]
            valid_real = past_realized[~np.isnan(past_realized)]
            if len(valid_real) > 0:
                df.loc[idx[i], "sig_impl_minus_avg_real"] = cur_impl - valid_real.mean()

    # Drop rows without signals
    sig_cols = ["sig_impl_vs_last_impl",
                "sig_impl_minus_last_real",
                "sig_impl_vs_avg_impl",
                "sig_impl_minus_avg_real"]
    before = len(df)
    df["sig_impl_raw"] = df["implied_move"]
    df = df.dropna(subset=sig_cols).reset_index(drop=True)
    log.info("Signals computed: %d events (%d dropped for insufficient history)",
             len(df), before - len(df))

    return df


# ═══════════════════════════════════════════════════════════════
# Phase 3 — Walk-Forward Regression + Backtest
# ═══════════════════════════════════════════════════════════════

SIG_COLS = ["sig_impl_vs_last_impl",
            "sig_impl_minus_last_real", "sig_impl_vs_avg_impl",
            "sig_impl_minus_avg_real"]

# ── Academic Study "Master Model" (fitted on 21,000+ trades) ──
MASTER_MODEL = {
    "coefficients": {
        "sig_impl_vs_last_impl": -0.9596,
        "sig_impl_minus_last_real": -0.1880,      # applied to diff in %
        "sig_impl_vs_avg_impl": -1.1505,
        "sig_impl_minus_avg_real": -0.6233        # applied to diff in %
    },
    "intercept": 3.3773,  # result in %
    "is_master": True
}


def walk_forward_regression(
    signals_df: pd.DataFrame,
    min_train: int = 200,
) -> pd.DataFrame:
    """Expanding-window OLS regression.

    Train on gross_return (market signal), not net_return.
    The model predicts the gross move; transaction costs are applied in the
    backtest.  Filter: predicted_return > 0 (positive gross move expected).
    """
    df = signals_df.sort_values("report_date").reset_index(drop=True)
    df["predicted_return"] = np.nan
    df["is_oos"] = False

    X = df[SIG_COLS].values
    y = df["gross_return"].values

    for t in range(min_train, len(df)):
        X_train = X[:t]
        y_train = y[:t]

        # Remove NaN/inf
        mask = np.isfinite(X_train).all(axis=1) & np.isfinite(y_train)
        if mask.sum() < 50:
            continue

        model = LinearRegression()
        model.fit(X_train[mask], y_train[mask])

        x_t = X[t:t+1]
        if np.isfinite(x_t).all():
            df.loc[t, "predicted_return"] = float(model.predict(x_t)[0])
            df.loc[t, "is_oos"] = True

    oos = df[df["is_oos"]].copy()
    tradeable = oos[oos["predicted_return"] > 0].copy()

    log.info("Walk-forward: %d OOS predictions, %d tradeable (pred > 0)",
             len(oos), len(tradeable))

    # Final model on all data (for display + live scanner)
    mask_all = np.isfinite(X).all(axis=1) & np.isfinite(y)
    final_model = LinearRegression()
    final_model.fit(X[mask_all], y[mask_all])

    model_info = {
        "coefficients": dict(zip(SIG_COLS, final_model.coef_.tolist())),
        "intercept": float(final_model.intercept_),
        "r2": float(final_model.score(X[mask_all], y[mask_all])),
        "n_train": int(mask_all.sum()),
    }

    # OOS correlation (vs gross_return, since model predicts gross)
    oos_valid = oos.dropna(subset=["predicted_return", "gross_return"])
    if len(oos_valid) > 10:
        corr, pval = sp_stats.pearsonr(oos_valid["predicted_return"],
                                        oos_valid["gross_return"])
        model_info["oos_correlation"] = round(corr, 4)
        model_info["oos_pvalue"] = round(pval, 4)

    return df, tradeable, model_info


def run_backtest(
    tradeable_df: pd.DataFrame,
    initial: float = INITIAL_CAPITAL,
    max_pos: int = MAX_POSITIONS,
) -> dict:
    """Walk-forward backtest with OQuants sizing (2-6% per trade, slide 41).

    Entry: buy straddle at mid + BA_FILL_FRACTION of spread.
    Exit: sell straddle at mid - BA_FILL_FRACTION of spread.
    Commission: $0.65/leg x 4 = $2.60/contract.
    """
    df = tradeable_df.sort_values("entry_date").reset_index(drop=True)

    account = initial
    positions = []  # {root, entry_date, exit_date, entry_price, contracts, ...}
    trade_log = []
    equity_log = []

    # Group by entry_date for batch processing
    all_dates = sorted(set(df["entry_date"].tolist() + df["exit_date"].tolist()))

    for day in all_dates:
        # Close positions expiring today or earlier
        closed = [p for p in positions if p["exit_date"] <= day]
        for p in closed:
            exit_price = p["exit_price"]
            pnl = (exit_price - p["entry_price"]) * p["contracts"] * CONTRACT_MULT
            pnl -= COMMISSION_LEG * 4 * p["contracts"]  # close commissions
            account += pnl + p["invested"]  # return invested capital + pnl

            trade_log.append({
                "root": p["root"],
                "entry_date": p["entry_date"],
                "exit_date": p["exit_date"],
                "entry_price": p["entry_price"],
                "exit_price": exit_price,
                "contracts": p["contracts"],
                "invested": p["invested"],
                "pnl": round(pnl, 2),
                "return_pct": round(pnl / p["invested"] * 100, 2) if p["invested"] > 0 else 0,
                "net_return": p["net_return"],
                "predicted_return": p.get("predicted_return", np.nan),
                "implied_move": p["implied_move"],
            })

        positions = [p for p in positions if p["exit_date"] > day]

        # Enter new positions
        entries = df[df["entry_date"] == day].sort_values("predicted_return", ascending=False)
        slots = max_pos - len(positions)

        if slots > 0 and not entries.empty:
            # OQuants sizing: 2-6% of portfolio per trade (slide 41)
            if len(trade_log) >= MIN_KELLY_TRADES:
                rets = np.array([t["return_pct"] / 100 for t in trade_log])
                mu = rets.mean()
                var = rets.var()
                kelly_full = mu / var if var > 0 else 0
                # Scale Kelly into 2-6% range
                alloc_pct = max(MIN_ALLOC_TRADE, min(MAX_ALLOC_TRADE,
                                                     kelly_full * 0.1))
            else:
                alloc_pct = DEFAULT_ALLOC

            alloc_per_pos = alloc_pct * account

            for _, row in entries.head(slots).iterrows():
                # Fractional spread slippage at entry
                mid_entry = row["entry_straddle"]
                ask_entry = row.get("entry_straddle_ask", mid_entry)
                entry_cost = mid_entry + BA_FILL_FRACTION * (ask_entry - mid_entry)

                # Number of contracts
                contracts = int(alloc_per_pos / (entry_cost * CONTRACT_MULT))
                contracts = max(1, min(contracts, MAX_CONTRACTS))

                invested = contracts * entry_cost * CONTRACT_MULT
                invested += COMMISSION_LEG * 2 * contracts  # entry commissions

                if invested > account * 0.5:
                    continue

                account -= invested

                # Fractional spread slippage at exit
                mid_exit = row["exit_straddle"]
                bid_exit = row.get("exit_straddle_bid", mid_exit)
                exit_price = mid_exit - BA_FILL_FRACTION * (mid_exit - bid_exit)

                positions.append({
                    "root": row["root"],
                    "entry_date": day,
                    "exit_date": row["exit_date"],
                    "entry_price": entry_cost,
                    "exit_price": exit_price,
                    "contracts": contracts,
                    "invested": invested,
                    "net_return": row["net_return"],
                    "predicted_return": row.get("predicted_return", np.nan),
                    "implied_move": row["implied_move"],
                })

        # Log equity
        invested_total = sum(p["invested"] for p in positions)
        equity_log.append({
            "date": day,
            "account": round(account + invested_total, 2),
            "cash": round(account, 2),
            "invested": round(invested_total, 2),
            "n_positions": len(positions),
        })

    # Close any remaining positions
    for p in positions:
        exit_price = p["exit_price"]
        pnl = (exit_price - p["entry_price"]) * p["contracts"] * CONTRACT_MULT
        pnl -= COMMISSION_LEG * 4 * p["contracts"]
        account += pnl + p["invested"]
        trade_log.append({
            "root": p["root"],
            "entry_date": p["entry_date"],
            "exit_date": p["exit_date"],
            "entry_price": p["entry_price"],
            "exit_price": exit_price,
            "contracts": p["contracts"],
            "invested": p["invested"],
            "pnl": round(pnl, 2),
            "return_pct": round(pnl / p["invested"] * 100, 2) if p["invested"] > 0 else 0,
            "net_return": p["net_return"],
            "predicted_return": p.get("predicted_return", np.nan),
            "implied_move": p["implied_move"],
        })

    trades_df = pd.DataFrame(trade_log)
    equity_df = pd.DataFrame(equity_log)

    # Compute stats
    stats = _compute_stats(trades_df, equity_df, initial)

    return {
        "trades": trades_df,
        "equity": equity_df,
        "stats": stats,
    }


def _compute_stats(trades_df, equity_df, initial):
    """Compute backtest performance statistics."""
    if trades_df.empty:
        return {}

    rets = trades_df["return_pct"].values / 100
    pnls = trades_df["pnl"].values

    final_equity = equity_df["account"].iloc[-1] if not equity_df.empty else initial
    n_years = len(equity_df) / 252 if len(equity_df) > 0 else 1

    # Drawdown
    equity_arr = equity_df["account"].values if not equity_df.empty else [initial]
    peak = np.maximum.accumulate(equity_arr)
    dd = (equity_arr - peak) / peak
    max_dd = float(dd.min())

    # CAGR
    cagr = (final_equity / initial) ** (1 / max(n_years, 0.1)) - 1

    # Sharpe (per-trade)
    sharpe = rets.mean() / rets.std() * np.sqrt(252 / 14) if rets.std() > 0 else 0

    return {
        "n_trades": len(trades_df),
        "win_rate": round(float((rets > 0).mean()) * 100, 1),
        "mean_return": round(float(rets.mean()) * 100, 2),
        "median_return": round(float(np.median(rets)) * 100, 2),
        "std_return": round(float(rets.std()) * 100, 2),
        "skewness": round(float(sp_stats.skew(rets)), 2),
        "total_pnl": round(float(pnls.sum()), 2),
        "initial_capital": initial,
        "final_equity": round(float(final_equity), 2),
        "cagr": round(float(cagr * 100), 2),
        "sharpe": round(float(sharpe), 2),
        "max_drawdown": round(max_dd * 100, 2),
        "avg_holding_days": round(float(
            trades_df.apply(
                lambda r: (_int_to_date(int(r["exit_date"])) - _int_to_date(int(r["entry_date"]))).days,
                axis=1
            ).mean()
        ), 1) if not trades_df.empty else 0,
    }


# ═══════════════════════════════════════════════════════════════
# Phase 4 — Charts
# ═══════════════════════════════════════════════════════════════

def _apply_dark_theme(ax, fig=None):
    """Apply dark theme to matplotlib axes."""
    ax.set_facecolor(DARK_BG)
    if fig:
        fig.patch.set_facecolor(DARK_BG)
    ax.tick_params(colors=DARK_TEXT, which="both")
    ax.xaxis.label.set_color(DARK_TEXT)
    ax.yaxis.label.set_color(DARK_TEXT)
    ax.title.set_color(DARK_TEXT)
    for spine in ax.spines.values():
        spine.set_color(DARK_GRID)
    ax.grid(True, color=DARK_GRID, alpha=0.3)


def generate_straddle_charts(backtest_result: dict, signals_df: pd.DataFrame,
                              history: pd.DataFrame) -> list:
    """Generate 5 dark-themed charts."""
    charts = []
    trades = backtest_result["trades"]
    equity = backtest_result["equity"]

    # 1. Equity curve + drawdown
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 6), height_ratios=[3, 1],
                                     gridspec_kw={"hspace": 0.05})
    _apply_dark_theme(ax1, fig)
    _apply_dark_theme(ax2)

    if not equity.empty:
        dates = [_int_to_date(int(d)) for d in equity["date"]]
        vals = equity["account"].values
        ax1.plot(dates, vals, color=ACCENT, linewidth=1.5)
        ax1.fill_between(dates, INITIAL_CAPITAL, vals,
                         where=vals >= INITIAL_CAPITAL, alpha=0.1, color=GREEN)
        ax1.fill_between(dates, INITIAL_CAPITAL, vals,
                         where=vals < INITIAL_CAPITAL, alpha=0.1, color=RED)
        ax1.set_ylabel("Equity ($)")
        ax1.set_title("Pre-Earnings Straddle — Equity Curve")
        ax1.axhline(INITIAL_CAPITAL, color=DARK_GRID, linestyle="--", alpha=0.5)
        ax1.set_xticklabels([])

        # Drawdown
        peak = np.maximum.accumulate(vals)
        dd = (vals - peak) / peak * 100
        ax2.fill_between(dates, dd, 0, color=RED, alpha=0.4)
        ax2.set_ylabel("DD (%)")
        ax2.set_xlabel("Date")

    path = str(OUTPUT / "straddle_equity.png")
    fig.savefig(path, dpi=120, bbox_inches="tight", facecolor=DARK_BG)
    plt.close(fig)
    charts.append("straddle_equity.png")

    # 2. Return distribution
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(10, 4))
    _apply_dark_theme(ax1, fig)
    _apply_dark_theme(ax2)

    if not trades.empty:
        rets = trades["return_pct"].values
        rets_clipped = np.clip(rets, -100, 200)
        ax1.hist(rets_clipped, bins=50, color=ACCENT, alpha=0.7, edgecolor=DARK_GRID)
        ax1.axvline(0, color=RED, linestyle="--", alpha=0.7)
        ax1.axvline(np.mean(rets), color=GREEN, linestyle="--", alpha=0.7,
                    label=f"Mean: {np.mean(rets):.1f}%")
        ax1.set_xlabel("Return (%)")
        ax1.set_ylabel("Count")
        ax1.set_title("Return Distribution")
        ax1.legend(facecolor=DARK_CARD, edgecolor=DARK_GRID, labelcolor=DARK_TEXT)

        # QQ plot
        sorted_rets = np.sort(rets)
        n = len(sorted_rets)
        theoretical = sp_stats.norm.ppf(np.linspace(0.01, 0.99, n))
        ax2.scatter(theoretical, sorted_rets, s=8, alpha=0.5, color=ACCENT)
        ax2.plot([theoretical[0], theoretical[-1]], [theoretical[0]*np.std(rets)+np.mean(rets),
                  theoretical[-1]*np.std(rets)+np.mean(rets)],
                 color=RED, linestyle="--", linewidth=1)
        ax2.set_xlabel("Theoretical Quantiles")
        ax2.set_ylabel("Sample Quantiles (%)")
        ax2.set_title("Q-Q Plot")

    path = str(OUTPUT / "straddle_distribution.png")
    fig.savefig(path, dpi=120, bbox_inches="tight", facecolor=DARK_BG)
    plt.close(fig)
    charts.append("straddle_distribution.png")

    # 3. Signal scatter plots (2x2 for 4 signals)
    fig, axes = plt.subplots(2, 2, figsize=(10, 8))
    _apply_dark_theme(axes[0, 0], fig)

    sig_labels = [
        ("sig_impl_vs_last_impl", "Implied / Last Implied"),
        ("sig_impl_minus_last_real", "Implied - Last Realized"),
        ("sig_impl_vs_avg_impl", "Implied / Avg Implied"),
        ("sig_impl_minus_avg_real", "Implied - Avg Realized"),
    ]

    for ax, (col, label) in zip(axes.flat, sig_labels):
        _apply_dark_theme(ax)
        if col in signals_df.columns:
            valid = signals_df.dropna(subset=[col, "net_return"])
            x = valid[col].values
            y = valid["net_return"].values * 100
            # Clip for visibility
            x_clip = np.clip(x, np.percentile(x, 1), np.percentile(x, 99))
            y_clip = np.clip(y, -100, 200)
            ax.scatter(x_clip, y_clip, s=4, alpha=0.2, color=ACCENT)
            # Regression line
            if len(x_clip) > 10:
                z = np.polyfit(x_clip, y_clip, 1)
                x_line = np.linspace(x_clip.min(), x_clip.max(), 100)
                ax.plot(x_line, np.polyval(z, x_line), color=RED, linewidth=2)
                ax.set_title(f"{label}\nslope={z[0]:.3f}", fontsize=9)
            else:
                ax.set_title(label, fontsize=9)
            ax.set_ylabel("Return (%)")

    # Hide unused 6th subplot
    if len(sig_labels) < len(axes.flat):
        for ax_unused in list(axes.flat)[len(sig_labels):]:
            ax_unused.set_visible(False)

    fig.suptitle("Signal vs Return (OOS)", color=DARK_TEXT, fontsize=12)
    fig.tight_layout()
    path = str(OUTPUT / "straddle_signals.png")
    fig.savefig(path, dpi=120, bbox_inches="tight", facecolor=DARK_BG)
    plt.close(fig)
    charts.append("straddle_signals.png")

    # 4. Vol ramp profile (average implied move T-14 to T-1)
    fig, ax = plt.subplots(figsize=(10, 4))
    _apply_dark_theme(ax, fig)

    # For this we need daily straddle prices — we'll use the entry/exit only
    # Show the average implied move at entry vs exit
    if not history.empty:
        valid = history.dropna(subset=["implied_move", "realized_move"])
        if not valid.empty:
            entry_im = valid["implied_move"].values * 100
            # Compute exit implied move
            exit_im = (valid["exit_straddle"] / valid["exit_underlying"]).values * 100

            bins = np.linspace(0, 15, 31)
            ax.hist(entry_im, bins=bins, alpha=0.5, color=ACCENT, label="Entry (T-14)")
            ax.hist(exit_im, bins=bins, alpha=0.5, color=YELLOW, label="Exit (T-1)")
            ax.axvline(np.median(entry_im), color=ACCENT, linestyle="--", linewidth=1.5)
            ax.axvline(np.median(exit_im), color=YELLOW, linestyle="--", linewidth=1.5)
            ax.set_xlabel("Implied Move (%)")
            ax.set_ylabel("Count")
            ax.set_title("Implied Move Distribution: Entry (T-14) vs Exit (T-1)")
            ax.legend(facecolor=DARK_CARD, edgecolor=DARK_GRID, labelcolor=DARK_TEXT)

    path = str(OUTPUT / "straddle_vol_ramp.png")
    fig.savefig(path, dpi=120, bbox_inches="tight", facecolor=DARK_BG)
    plt.close(fig)
    charts.append("straddle_vol_ramp.png")

    # 5. Monthly P&L bars
    fig, ax = plt.subplots(figsize=(10, 4))
    _apply_dark_theme(ax, fig)

    if not trades.empty:
        trades_copy = trades.copy()
        trades_copy["month"] = trades_copy["exit_date"].apply(
            lambda d: str(d)[:6]
        )
        monthly = trades_copy.groupby("month")["pnl"].sum()
        colors = [GREEN if v >= 0 else RED for v in monthly.values]
        ax.bar(range(len(monthly)), monthly.values, color=colors, alpha=0.8)
        # Show every 12th label
        step = max(1, len(monthly) // 15)
        ax.set_xticks(range(0, len(monthly), step))
        ax.set_xticklabels([monthly.index[i] for i in range(0, len(monthly), step)],
                           rotation=45, fontsize=7)
        ax.set_ylabel("P&L ($)")
        ax.set_title("Monthly P&L")
        ax.axhline(0, color=DARK_GRID, linestyle="--", alpha=0.5)

    path = str(OUTPUT / "straddle_monthly.png")
    fig.savefig(path, dpi=120, bbox_inches="tight", facecolor=DARK_BG)
    plt.close(fig)
    charts.append("straddle_monthly.png")

    return charts


# ═══════════════════════════════════════════════════════════════
# Phase 5 — Live Scanner
# ═══════════════════════════════════════════════════════════════

def _fetch_ibkr_earnings(tickers, days_min=10, days_max=18):
    """Fetch upcoming earnings dates from IBKR (Wall Street Horizon).

    Uses reqFundamentalData with 'CalendarReport' to get next earnings date
    for each ticker. Returns DataFrame with root, report_date, before_after.
    """
    try:
        from app import ib_state, _run_in_ib_thread
    except ImportError:
        return pd.DataFrame()

    if not ib_state.get("connected") or not ib_state.get("ib"):
        return pd.DataFrame()

    from ib_insync import Stock
    import xml.etree.ElementTree as ET

    today = datetime.now().date()
    start_date = today + timedelta(days=days_min)
    end_date = today + timedelta(days=days_max)

    results = []

    def _query_earnings():
        ib = ib_state["ib"]
        for ticker in tickers:
            try:
                contract = Stock(ticker, "SMART", "USD")
                ib.qualifyContracts(contract)
                xml_data = ib.reqFundamentalData(contract, "CalendarReport")
                if not xml_data:
                    continue
                root_el = ET.fromstring(xml_data)
                for event in root_el.iter("Event"):
                    etype = event.findtext("Type", "")
                    if "earnings" not in etype.lower() and "report" not in etype.lower():
                        continue
                    date_str = event.findtext("Date", "")
                    if not date_str:
                        continue
                    # Parse date (IBKR format: MM/DD/YYYY or YYYY-MM-DD)
                    try:
                        if "/" in date_str:
                            ed = datetime.strptime(date_str, "%m/%d/%Y").date()
                        else:
                            ed = datetime.strptime(date_str, "%Y-%m-%d").date()
                    except ValueError:
                        continue
                    if start_date <= ed <= end_date:
                        timing = event.findtext("Timing", "")
                        ba = ""
                        if "before" in timing.lower() or "bmo" in timing.lower():
                            ba = "BeforeMarket"
                        elif "after" in timing.lower() or "amc" in timing.lower():
                            ba = "AfterMarket"
                        results.append({
                            "root": ticker,
                            "report_date": int(ed.strftime("%Y%m%d")),
                            "before_after": ba,
                        })
                ib.sleep(0.1)  # rate limit
            except Exception:
                continue

    try:
        _run_in_ib_thread(_query_earnings)
    except Exception:
        pass

    if results:
        return pd.DataFrame(results)
    return pd.DataFrame()


def _fetch_live_implied_move(ticker: str, report_date: int) -> float | None:
    """Fetch current ATM implied move from ThetaData for a ticker.

    Finds the nearest monthly expiration after earnings, identifies the ATM
    strike, computes IV via Newton-Raphson, and returns implied_move = IV * sqrt(T).

    Returns None on failure.
    """
    try:
        from core.scanner import fetch_option_chain_thetadata

        stock_px, chain = fetch_option_chain_thetadata(ticker)
        if stock_px <= 0 or chain.empty:
            return None

        # Find nearest monthly expiration after earnings
        exp_int = next_monthly_expiration(report_date)
        exp_str = _int_to_date(exp_int).strftime("%Y-%m-%d")

        # Filter chain for this expiration
        exp_chain = chain[chain["exp_date"] == exp_str]
        if exp_chain.empty:
            return None

        calls = exp_chain[exp_chain["type"] == "call"]
        puts = exp_chain[exp_chain["type"] == "put"]
        if calls.empty or puts.empty:
            return None

        # Find ATM: strike closest to stock price with both call and put
        calls_valid = calls[(calls["bid"] > 0) & (calls["ask"] > 0)].copy()
        puts_valid = puts[(puts["bid"] > 0) & (puts["ask"] > 0)].copy()
        if calls_valid.empty or puts_valid.empty:
            return None

        # Common strikes
        common_strikes = set(calls_valid["strike"]) & set(puts_valid["strike"])
        if not common_strikes:
            return None

        # Closest to stock_px
        atm_strike = min(common_strikes, key=lambda k: abs(k - stock_px))

        call_row = calls_valid[calls_valid["strike"] == atm_strike].iloc[0]
        put_row = puts_valid[puts_valid["strike"] == atm_strike].iloc[0]

        call_mid = (call_row["bid"] + call_row["ask"]) / 2
        put_mid = (put_row["bid"] + put_row["ask"]) / 2

        # Time to expiration
        today = datetime.now().date()
        exp_date = _int_to_date(exp_int)
        T = max((exp_date - today).days, 1) / 365.0

        # IV via Newton-Raphson (average call + put IV)
        call_iv = implied_vol_scalar(call_mid, stock_px, atm_strike, T, r=0.0, right='C')
        put_iv = implied_vol_scalar(put_mid, stock_px, atm_strike, T, r=0.0, right='P')

        ivs = [v for v in [call_iv, put_iv] if not np.isnan(v) and v > 0]
        if not ivs:
            return None

        atm_iv = np.mean(ivs)
        
        # --- Extract Ambient Volatility & Calculate Implied Move (OQuants method) ---
        exp_dt = _int_to_date(exp_int)
        if exp_dt.month == 12:
            exp2_date = third_friday(exp_dt.year + 1, 1)
        else:
            exp2_date = third_friday(exp_dt.year, exp_dt.month + 1)
        exp2_int = _date_to_int(exp2_date)
        
        c2 = df_chain[(df_chain["right"] == "C") & (df_chain["expiration"] == exp2_int) & (df_chain["strike"] == atm_strike)]
        p2 = df_chain[(df_chain["right"] == "P") & (df_chain["expiration"] == exp2_int) & (df_chain["strike"] == atm_strike)]
        
        ambient_iv = atm_iv  # fallback
        if not c2.empty and not p2.empty:
            c2_mid = (c2.iloc[0]["bid"] + c2.iloc[0]["ask"]) / 2
            p2_mid = (p2.iloc[0]["bid"] + p2.iloc[0]["ask"]) / 2
            T2 = max((exp2_date - today).days, 1) / 365.0
            c2_iv = implied_vol_scalar(c2_mid, stock_px, atm_strike, T2, r=0.0, right='C')
            p2_iv = implied_vol_scalar(p2_mid, stock_px, atm_strike, T2, r=0.0, right='P')
            ivs2 = [v for v in [c2_iv, p2_iv] if not np.isnan(v)]
            if ivs2:
                ambient_iv = np.mean(ivs2)
                
        total_variance = (atm_iv ** 2) * T
        ambient_variance = (ambient_iv ** 2) * max(T - 1.0/365.0, 0)
        event_variance = max(total_variance - ambient_variance, 0)
        implied_move = np.sqrt(event_variance) * np.sqrt(2.0 / np.pi)

        log.debug("Scanner live IV: %s ATM_K=%.0f IV=%.1f%% impl_move=%.1f%%",
                  ticker, atm_strike, atm_iv * 100, implied_move * 100)
        return float(implied_move)

    except Exception as ex:
        log.debug("Scanner: live IV fetch failed for %s: %s", ticker, ex)
        return None


def scan_upcoming_earnings(
    days_min: int = 10,
    days_max: int = 18,
    history: pd.DataFrame = None,
    model_info: dict = None,
) -> list:
    """Find upcoming earnings opportunities.

    Priority: IBKR (live, most accurate) -> ThetaData DB (fallback).
    1. Query earnings 10-18 days from today
    2. Compute signals using historical data
    3. Predict return using latest model
    4. Return ranked candidates
    """
    today = datetime.now().date()
    start_date = today + timedelta(days=days_min)
    end_date = today + timedelta(days=days_max)
    start_int = _date_to_int(start_date)
    end_int = _date_to_int(end_date)

    # Source 1: IBKR (live earnings calendar)
    all_tickers = sorted(history["root"].unique()) if history is not None else []
    ibkr_df = _fetch_ibkr_earnings(all_tickers, days_min, days_max)

    # Source 2: ThetaData DB (fallback / complement)
    conn = sqlite3.connect(str(DB))
    db_df = pd.read_sql_query(
        "SELECT root, report_date, before_after FROM earnings "
        f"WHERE report_date >= {start_int} AND report_date <= {end_int} "
        "ORDER BY report_date",
        conn
    )
    conn.close()

    # Merge: IBKR takes priority, DB fills gaps
    if not ibkr_df.empty:
        log.info("Scanner: %d earnings from IBKR", len(ibkr_df))
        upcoming = ibkr_df
        # Add DB entries for tickers not in IBKR
        if not db_df.empty:
            ibkr_tickers = set(ibkr_df["root"])
            extra = db_df[~db_df["root"].isin(ibkr_tickers)]
            if not extra.empty:
                upcoming = pd.concat([upcoming, extra], ignore_index=True)
                log.info("Scanner: +%d from DB (complement)", len(extra))
    else:
        upcoming = db_df
        if not upcoming.empty:
            log.info("Scanner: %d earnings from DB (IBKR not connected)", len(upcoming))

    if upcoming.empty or history is None or model_info is None:
        return []

    results = []
    for _, row in upcoming.iterrows():
        root = row["root"]
        report_date = int(row["report_date"])

        # Get this ticker's history
        root_hist = history[history["root"] == root].sort_values("report_date")
        if root_hist.empty:
            continue

        last = root_hist.iloc[-1]
        last_impl = last["implied_move"]
        last_real = last.get("realized_move", np.nan)

        # Expanding averages from history
        avg_real = root_hist["realized_move"].mean()
        avg_impl = root_hist["implied_move"].mean()

        # Fetch LIVE implied move from ThetaData option chain
        cur_impl = _fetch_live_implied_move(root, report_date)
        if cur_impl is None or cur_impl <= 0:
            log.debug("Scanner: no live IV for %s, using last known", root)
            cur_impl = last_impl  # fallback to last known

        # Build signals (4 predictors, OQuants slides 24-27)
        sigs = {}
        sigs["sig_impl_vs_last_impl"] = cur_impl / last_impl if last_impl > 0 else np.nan
        sigs["sig_impl_minus_last_real"] = cur_impl - last_real if not np.isnan(last_real) else np.nan
        sigs["sig_impl_vs_avg_impl"] = cur_impl / avg_impl if avg_impl > 0 else np.nan
        sigs["sig_impl_minus_avg_real"] = cur_impl - avg_real if not np.isnan(avg_real) else np.nan

        if any(np.isnan(v) for v in sigs.values()):
            continue

        # Predict using Master Model (Study coefficients)
        # Scaling: differences must be in % for these coefficients
        pred = MASTER_MODEL["intercept"]
        pred += MASTER_MODEL["coefficients"]["sig_impl_vs_last_impl"] * sigs["sig_impl_vs_last_impl"]
        pred += MASTER_MODEL["coefficients"]["sig_impl_minus_last_real"] * (sigs["sig_impl_minus_last_real"] * 100)
        pred += MASTER_MODEL["coefficients"]["sig_impl_vs_avg_impl"] * sigs["sig_impl_vs_avg_impl"]
        pred += MASTER_MODEL["coefficients"]["sig_impl_minus_avg_real"] * (sigs["sig_impl_minus_avg_real"] * 100)

        if pred <= 0:
            continue

        exp_int = next_monthly_expiration(report_date)

        results.append({
            "root": root,
            "report_date": report_date,
            "report_date_str": _int_to_date(report_date).strftime("%Y-%m-%d"),
            "days_to_earnings": (datetime.strptime(str(report_date), "%Y%m%d").date() - today).days,
            "before_after": row.get("before_after", ""),
            "expiration": exp_int,
            "expiration_str": _int_to_date(exp_int).strftime("%Y-%m-%d"),
            "n_historical_events": len(root_hist),
            "avg_implied_move": round(float(avg_impl * 100), 2),
            "avg_realized_move": round(float(avg_real * 100), 2),
            "predicted_return": round(float(pred), 2),
            **{k: round(float(v), 4) for k, v in sigs.items()},
        })

    # Sort by predicted return descending
    results.sort(key=lambda x: -x["predicted_return"])
    return results


# ═══════════════════════════════════════════════════════════════
# Main Entry Point
# ═══════════════════════════════════════════════════════════════

def compute_straddle_analytics(force_rebuild: bool = False) -> dict:
    """Main entry point — compute all straddle analytics.

    Returns JSON-serializable dict for the dashboard.
    Cached with pickle.
    """
    cache_path = CACHE / "straddle_analytics.pkl"

    if cache_path.exists() and not force_rebuild:
        try:
            with open(cache_path, "rb") as f:
                cached = pickle.load(f)
            # Check if it's still valid
            if "stats" in cached:
                log.info("Using cached straddle analytics")
                return cached
        except Exception:
            pass

    log.info("Computing straddle analytics...")

    # Phase 1: Build history
    history = build_earnings_straddle_history(force_rebuild=force_rebuild)
    if history.empty:
        return {"error": "No straddle history available"}

    # Phase 2: Compute signals
    signals = compute_signals(history)
    if signals.empty:
        return {"error": "No valid signals computed"}

    # Phase 3: Walk-forward regression + backtest
    full_df, tradeable, model_info = walk_forward_regression(signals)

    if tradeable.empty:
        return {"error": "No tradeable signals after walk-forward filter"}

    backtest = run_backtest(tradeable)

    # Phase 4: Charts
    charts = generate_straddle_charts(backtest, signals, history)

    # Phase 5: Live scanner
    scanner_results = scan_upcoming_earnings(
        history=history, model_info=model_info
    )

    # Assemble result
    result = {
        "stats": backtest["stats"],
        "model": model_info,
        "charts": charts,
        "scanner": scanner_results,
        "n_scanner": len(scanner_results),
        "history_stats": {
            "n_total_events": len(history),
            "n_with_signals": len(signals),
            "n_tradeable": len(tradeable),
            "date_range": f"{history['report_date'].min()} - {history['report_date'].max()}",
            "tickers": int(history["root"].nunique()),
            "avg_implied_move": round(float(history["implied_move"].mean() * 100), 2),
            "avg_realized_move": round(float(history["realized_move"].dropna().mean() * 100), 2),
        },
        # Recent trades for display (last 20)
        "recent_trades": backtest["trades"].tail(20).to_dict(orient="records")
            if not backtest["trades"].empty else [],
    }

    # Cache
    with open(cache_path, "wb") as f:
        pickle.dump(result, f)
    log.info("Cached to %s", cache_path)

    return result


if __name__ == "__main__":
    import sys
    force = "--force" in sys.argv
    result = compute_straddle_analytics(force_rebuild=force)

    if "error" in result:
        log.error("Error: %s", result['error'])
    else:
        stats = result["stats"]
        log.info("=" * 60)
        log.info("EARNINGS VOL RAMP BACKTEST RESULTS")
        log.info("=" * 60)
        log.info("Events:        %d", result['history_stats']['n_total_events'])
        log.info("Tradeable:     %d", result['history_stats']['n_tradeable'])
        log.info("Trades:        %d", stats['n_trades'])
        log.info("Win Rate:      %s%%", stats['win_rate'])
        log.info("Mean Return:   %s%%", stats['mean_return'])
        log.info("Median Return: %s%%", stats['median_return'])
        log.info("Skewness:      %s", stats['skewness'])
        log.info("CAGR:          %s%%", stats['cagr'])
        log.info("Sharpe:        %s", stats['sharpe'])
        log.info("Max DD:        %s%%", stats['max_drawdown'])
        log.info("Final Equity:  $%s", f"{stats['final_equity']:,.0f}")

        model = result["model"]
        log.info("Model R2:      %.4f", model['r2'])
        log.info("OOS Corr:      %s", model.get('oos_correlation', 'N/A'))
        log.info("OOS p-value:   %s", model.get('oos_pvalue', 'N/A'))
        log.info("Coefficients:")
        for k, v in model["coefficients"].items():
            log.info("  %-35s %+.4f", k, v)

        if result["scanner"]:
            log.info("Upcoming opportunities: %d", len(result['scanner']))
            for s in result["scanner"][:5]:
                log.info("  %6s earnings %s pred=%+.1f%%",
                         s['root'], s['report_date_str'], s['predicted_return'])
