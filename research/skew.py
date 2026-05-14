"""
Skew Mean-Reversion — Vertical Spread Strategy

Observations:
  1. Skew (25D put IV - ATM IV) is mean-reverting
  2. IV vs RV as filter: IV > RV => credit spread, IV < RV => debit spread

Trading logic:
  - Credit spread (bull put): sell 25D put, buy 10D put when skew steep + IV > RV
  - Debit spread (bear put): buy ATM put, sell 25D put when skew flat + IV < RV

Three phases (same pattern as core/straddle.py):
  Phase 1: build_skew_history()     — data pipeline from sp500_options.db
  Phase 2: compute_skew_signals()   — z-scores + IV-RV gap
  Phase 3: run_skew_backtest()      — simulate credit/debit spreads

Usage:
    from research.skew import compute_skew_analytics
    result = compute_skew_analytics()
"""

import sqlite3
import pickle
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from datetime import datetime, date
from scipy import stats as sp_stats

from sklearn.linear_model import LinearRegression
from core.config import DB, CACHE, OUTPUT, COMMISSION_LEG, CONTRACT_MULT, get_logger
from core.pricing import (
    implied_vol_vec, bs_delta_vec, put_call_parity_call_equiv, RISK_FREE_RATE,
)

log = get_logger(__name__)

# ── Constants ──
TARGET_DTE_MIN   = 25     # monthly expiration 25-45 DTE out
TARGET_DTE_MAX   = 45
RV_LOOKBACK      = 20     # 20 trading days for realized vol
SKEW_LOOKBACK    = 60     # 60 days for z-score
MIN_STRIKES_SIDE = 3      # need >= 3 valid strikes per side
MIN_OPT_VOLUME   = 20_000 # same as straddle
BA_FILL_FRACTION = 0.10
INITIAL_CAPITAL  = 100_000
MAX_POSITIONS    = 15
MAX_CONTRACTS    = 10
DEFAULT_ALLOC    = 0.04   # 4% per trade

# Signal thresholds (optimized via parameter scan)
CREDIT_SKEW_Z    = 2.0    # z-score above which skew is "steep"
CREDIT_IVRV_MIN  = 0.10   # IV-RV gap minimum for credit signal (10%)
DEBIT_ENABLED    = False   # debit spreads disabled — no edge found
DEBIT_SKEW_Z     = -1.0   # z-score below which skew is "flat"
DEBIT_IVRV_MAX   = -0.03  # IV-RV gap maximum for debit signal

# Mean-reversion exit parameters
EXIT_Z_THRESHOLD = 0.5    # exit when skew z drops below this
MIN_HOLD_DAYS    = 5      # minimum holding period (trading days)

# Dark theme colors (match straddle.py)
DARK_BG   = "#0d1117"
DARK_CARD = "#1c2333"
DARK_TEXT = "#e6edf3"
DARK_GRID = "#30363d"
ACCENT    = "#58a6ff"
GREEN     = "#3fb950"
RED       = "#f85149"
YELLOW    = "#d29922"


# ═══════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════

def _int_to_date(d: int) -> date:
    return datetime.strptime(str(d), "%Y%m%d").date()


def _date_to_int(d) -> int:
    if isinstance(d, (date, datetime)):
        return int(d.strftime("%Y%m%d"))
    return int(pd.Timestamp(d).strftime("%Y%m%d"))


def third_friday(year: int, month: int) -> date:
    """Return the 3rd Friday of the given month/year."""
    d = date(year, month, 1)
    first_friday = d.day + (4 - d.weekday()) % 7
    return date(year, month, first_friday + 14)


def _find_target_monthly(date_int: int, min_dte: int, max_dte: int) -> int | None:
    """Find monthly expiration (3rd Friday) within [min_dte, max_dte] DTE.

    Returns expiration as int (YYYYMMDD), or None if no valid monthly found.
    """
    obs = _int_to_date(date_int)

    # Check this month and next 3 months
    for m_offset in range(4):
        y = obs.year + (obs.month + m_offset - 1) // 12
        m = (obs.month + m_offset - 1) % 12 + 1
        tf = third_friday(y, m)
        dte = (tf - obs).days
        if min_dte <= dte <= max_dte:
            return _date_to_int(tf)

    return None


def _load_daily_volumes() -> dict:
    """Load cached daily volumes (built by straddle.py)."""
    cache_path = CACHE / "daily_volumes.pkl"
    if cache_path.exists():
        return pd.read_pickle(str(cache_path))
    log.warning("daily_volumes.pkl not found — volume filter disabled")
    return {}


def _avg_daily_volume(vol_dict: dict, root: str, date_int: int,
                      trading_dates_arr: np.ndarray, lookback: int = 20) -> float:
    """Average daily option volume over lookback trading days before date_int."""
    idx = np.searchsorted(trading_dates_arr, date_int, side="left")
    start = max(idx - lookback, 0)
    dates = trading_dates_arr[start:idx]
    vols = [vol_dict.get((root, int(d)), 0) for d in dates]
    return np.mean(vols) if vols else 0


def _bs_put_price(S, K, T, sigma, r=RISK_FREE_RATE):
    """Black-Scholes put price (scalar)."""
    if T <= 0 or sigma <= 0:
        return max(K - S, 0)
    sqrt_T = np.sqrt(T)
    d1 = (np.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * sqrt_T)
    d2 = d1 - sigma * sqrt_T
    return K * np.exp(-r * T) * sp_stats.norm.cdf(-d2) - S * sp_stats.norm.cdf(-d1)


def _bs_call_price(S, K, T, sigma, r=RISK_FREE_RATE):
    """Black-Scholes call price (scalar)."""
    if T <= 0 or sigma <= 0:
        return max(S - K, 0)
    sqrt_T = np.sqrt(T)
    d1 = (np.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * sqrt_T)
    d2 = d1 - sigma * sqrt_T
    return S * sp_stats.norm.cdf(d1) - K * np.exp(-r * T) * sp_stats.norm.cdf(d2)


def _interp_iv(K_target, S_exit, atm_strike, atm_iv,
               put25d_strike, put25d_iv, put10d_strike, put10d_iv,
               call25d_strike=None, call25d_iv=None,
               call10d_strike=None, call10d_iv=None):
    """Interpolate IV at K_target from the exit vol surface.

    5-point surface: put10D, put25D, ATM, call25D, call10D.
    Linear interpolation between adjacent anchors.
    """
    if K_target <= put10d_strike:
        return put10d_iv  # extrapolate flat below 10D put
    elif K_target <= put25d_strike:
        if put25d_strike == put10d_strike:
            return put10d_iv
        t = (K_target - put10d_strike) / (put25d_strike - put10d_strike)
        return put10d_iv + t * (put25d_iv - put10d_iv)
    elif K_target <= atm_strike:
        if atm_strike == put25d_strike:
            return atm_iv
        t = (K_target - put25d_strike) / (atm_strike - put25d_strike)
        return put25d_iv + t * (atm_iv - put25d_iv)
    elif call25d_strike is not None and K_target <= call25d_strike:
        if call25d_strike == atm_strike:
            return call25d_iv
        t = (K_target - atm_strike) / (call25d_strike - atm_strike)
        return atm_iv + t * (call25d_iv - atm_iv)
    elif call10d_strike is not None and call25d_strike is not None and K_target <= call10d_strike:
        if call10d_strike == call25d_strike:
            return call10d_iv
        t = (K_target - call25d_strike) / (call10d_strike - call25d_strike)
        return call25d_iv + t * (call10d_iv - call25d_iv)
    elif call10d_iv is not None:
        return call10d_iv  # extrapolate flat above 10D call
    else:
        return atm_iv  # fallback


# ═══════════════════════════════════════════════════════════════
# Phase 1 — Data Pipeline
# ═══════════════════════════════════════════════════════════════

def _compute_rv(prices: pd.DataFrame) -> pd.DataFrame:
    """Compute 20-day rolling realized vol (annualized) for all tickers.

    Returns DataFrame with same index/columns as prices, values = RV.
    """
    log_ret = np.log(prices / prices.shift(1))
    rv = log_ret.rolling(RV_LOOKBACK, min_periods=RV_LOOKBACK).std() * np.sqrt(252)
    return rv


def _extract_skew_for_root(
    root_df: pd.DataFrame,
    underlying: float,
    dte: int,
) -> dict | None:
    """For one ticker on one date: compute IV surface and extract skew metrics.

    Uses OTM options only for reliable IV:
      - K >= S: OTM calls  → IV from call mid directly
      - K <  S: OTM puts   → IV via put-call parity (put mid → call equiv → IV)

    Delta identification (from call-equivalent IV):
      - ATM:      call_delta ≈ 0.50
      - 25D call: call_delta ≈ 0.25  (OTM call, K > S)
      - 25D put:  call_delta ≈ 0.75  (≡ |put delta| = 0.25, K < S)
      - 10D put:  call_delta ≈ 0.90  (≡ |put delta| = 0.10, K << S)
      - 10D call: call_delta ≈ 0.10  (OTM call, K >> S)

    root_df: DataFrame with columns [strike, right, bid, ask]
             strike already in dollars (divided by 1000)
    underlying: stock price
    dte: days to expiration
    """
    if underlying <= 0 or dte <= 0:
        return None

    T = dte / 365.0
    S = underlying

    # Split calls and puts
    calls = root_df[root_df["right"] == "C"].copy()
    puts = root_df[root_df["right"] == "P"].copy()

    if calls.empty and puts.empty:
        return None

    # Compute mid prices
    if not calls.empty:
        calls["mid"] = (calls["bid"] + calls["ask"]) / 2
    if not puts.empty:
        puts["mid"] = (puts["bid"] + puts["ask"]) / 2

    # Filter strikes within reasonable range
    strike_lo = S * 0.70
    strike_hi = S * 1.30
    calls = calls[(calls["strike"] >= strike_lo) & (calls["strike"] <= strike_hi)]
    puts = puts[(puts["strike"] >= strike_lo) & (puts["strike"] <= strike_hi)]

    # ── Compute implied forward from put-call parity ──
    # bt_prices.pkl contains dividend-adjusted prices (10-20% below actual for
    # older dates). Options price off the actual (unadjusted) stock, so derive
    # the correct forward from C - P = (F - K)*exp(-rT) at common strikes.
    if not calls.empty and not puts.empty:
        calls_idx = calls.set_index("strike")["mid"]
        puts_idx = puts.set_index("strike")["mid"]
        common_K = calls_idx.index.intersection(puts_idx.index)
        # Use wider range (30%) since bt_prices can be 10-20% off for old dates
        near_atm = common_K[(common_K >= S * 0.70) & (common_K <= S * 1.30)]
        if len(near_atm) >= 3:
            fwd_arr = []
            for K_val in near_atm:
                C_m = calls_idx.loc[K_val]
                P_m = puts_idx.loc[K_val]
                if isinstance(C_m, pd.Series):
                    C_m = C_m.iloc[0]
                if isinstance(P_m, pd.Series):
                    P_m = P_m.iloc[0]
                F = K_val + np.exp(RISK_FREE_RATE * T) * (C_m - P_m)
                fwd_arr.append(F)
            # Use median of forwards near where C≈P (true ATM region)
            fwd_arr = np.array(fwd_arr)
            implied_fwd = np.median(fwd_arr)
            # Sanity: forward should be within 30% of stock price
            # (10-20% gaps are normal for dividend-adjusted historical prices)
            if 0.70 * S < implied_fwd < 1.30 * S:
                S = implied_fwd

    # ── Build IV surface using OTM options only ──
    # OTM calls: K >= S (reliable, direct Newton-Raphson on call prices)
    otm_calls = calls[calls["strike"] >= S].copy()
    # OTM puts: K < S (convert to call-equiv via put-call parity, then IV)
    otm_puts = puts[puts["strike"] < S].copy()

    iv_rows = []

    # IV from OTM calls
    if not otm_calls.empty:
        S_arr = np.full(len(otm_calls), S)
        K_arr = otm_calls["strike"].values
        T_arr = np.full(len(otm_calls), T)
        ivs = implied_vol_vec(otm_calls["mid"].values, S_arr, K_arr, T_arr)
        for j, (idx, row) in enumerate(otm_calls.iterrows()):
            if not np.isnan(ivs[j]) and 0.02 < ivs[j] < 3.0:
                iv_rows.append({
                    "strike": row["strike"], "iv": ivs[j],
                    "bid_c": row["bid"], "ask_c": row["ask"],
                    "bid_p": np.nan, "ask_p": np.nan,
                })

    # IV from OTM puts (via put-call parity)
    if not otm_puts.empty:
        call_equivs = put_call_parity_call_equiv(
            otm_puts["mid"].values,
            np.full(len(otm_puts), S),
            otm_puts["strike"].values,
            np.full(len(otm_puts), T),
        )
        S_arr = np.full(len(otm_puts), S)
        K_arr = otm_puts["strike"].values
        T_arr = np.full(len(otm_puts), T)
        ivs = implied_vol_vec(call_equivs, S_arr, K_arr, T_arr)
        # Also look up call bid/ask at same strike for possible trading
        call_ba = {}
        if not calls.empty:
            for _, cr in calls.iterrows():
                call_ba[cr["strike"]] = (cr["bid"], cr["ask"])
        for j, (idx, row) in enumerate(otm_puts.iterrows()):
            if not np.isnan(ivs[j]) and 0.02 < ivs[j] < 3.0:
                cb = call_ba.get(row["strike"], (np.nan, np.nan))
                iv_rows.append({
                    "strike": row["strike"], "iv": ivs[j],
                    "bid_c": cb[0], "ask_c": cb[1],
                    "bid_p": row["bid"], "ask_p": row["ask"],
                })

    if len(iv_rows) < MIN_STRIKES_SIDE * 2:
        return None

    surface = pd.DataFrame(iv_rows).sort_values("strike").reset_index(drop=True)

    # Compute call delta for all strikes (using their proper OTM-derived IV)
    cd = bs_delta_vec(
        np.full(len(surface), S),
        surface["strike"].values,
        np.full(len(surface), T),
        surface["iv"].values,
    )
    surface["call_delta"] = cd
    surface = surface.dropna(subset=["call_delta"])
    if len(surface) < MIN_STRIKES_SIDE * 2:
        return None

    # ── Find key strikes via call delta ──
    # ATM: call_delta ≈ 0.50
    atm_idx = (surface["call_delta"] - 0.50).abs().idxmin()
    atm_row = surface.loc[atm_idx]
    atm_strike = atm_row["strike"]
    atm_iv = atm_row["iv"]

    # 25D call: call_delta ≈ 0.25 (OTM call, K > S)
    otm_call_surface = surface[surface["strike"] >= S]
    if len(otm_call_surface) < 2:
        return None
    call25_idx = (otm_call_surface["call_delta"] - 0.25).abs().idxmin()
    call25_row = otm_call_surface.loc[call25_idx]
    call25d_strike = call25_row["strike"]
    call25d_iv = call25_row["iv"]
    call25d_bid = call25_row["bid_c"]
    call25d_ask = call25_row["ask_c"]

    # 10D call: call_delta ≈ 0.10
    call10_idx = (otm_call_surface["call_delta"] - 0.10).abs().idxmin()
    call10_row = otm_call_surface.loc[call10_idx]
    call10d_strike = call10_row["strike"]
    call10d_iv = call10_row["iv"]
    call10d_bid = call10_row["bid_c"]
    call10d_ask = call10_row["ask_c"]

    # 25D put: call_delta ≈ 0.75 (K < S, OTM put side)
    otm_put_surface = surface[surface["strike"] < S]
    if len(otm_put_surface) < 2:
        return None
    put25_idx = (otm_put_surface["call_delta"] - 0.75).abs().idxmin()
    put25_row = otm_put_surface.loc[put25_idx]
    put25d_strike = put25_row["strike"]
    put25d_iv = put25_row["iv"]
    put25d_bid = put25_row["bid_p"]
    put25d_ask = put25_row["ask_p"]

    # 10D put: call_delta ≈ 0.90 (K << S, deep OTM put)
    put10_idx = (otm_put_surface["call_delta"] - 0.90).abs().idxmin()
    put10_row = otm_put_surface.loc[put10_idx]
    put10d_strike = put10_row["strike"]
    put10d_iv = put10_row["iv"]
    put10d_bid = put10_row["bid_p"]
    put10d_ask = put10_row["ask_p"]

    # Sanity checks
    if atm_iv <= 0 or put25d_iv <= 0 or call25d_iv <= 0:
        return None
    if np.isnan(put25d_bid) or np.isnan(put10d_bid):
        return None
    if np.isnan(call25d_bid) or np.isnan(call10d_bid):
        return None

    # Strike ordering: put10 < put25 < ATM < call25 < call10
    if not (put10d_strike < put25d_strike <= atm_strike <= call25d_strike < call10d_strike):
        return None

    return {
        "implied_forward": S,  # corrected spot (from put-call parity)
        "atm_strike": atm_strike,
        "atm_iv": atm_iv,
        "put25d_strike": put25d_strike,
        "put25d_iv": put25d_iv,
        "put25d_bid": put25d_bid,
        "put25d_ask": put25d_ask,
        "call25d_strike": call25d_strike,
        "call25d_iv": call25d_iv,
        "call25d_bid": call25d_bid,
        "call25d_ask": call25d_ask,
        "put10d_strike": put10d_strike,
        "put10d_iv": put10d_iv,
        "put10d_bid": put10d_bid,
        "put10d_ask": put10d_ask,
        "call10d_strike": call10d_strike,
        "call10d_iv": call10d_iv,
        "call10d_bid": call10d_bid,
        "call10d_ask": call10d_ask,
        "skew_put": put25d_iv - atm_iv,
        "skew_call": atm_iv - call25d_iv,
    }


def build_skew_history(force_rebuild: bool = False) -> pd.DataFrame:
    """Build daily skew metrics for all tickers across all trading dates.

    For each date:
      1. Find monthly expiration in [25, 45] DTE
      2. Batch query all options for that expiration
      3. Per ticker: compute IV surface, extract skew at 25D/10D/ATM

    Cached to cache/skew_history.pkl.
    """
    cache_path = CACHE / "skew_history.pkl"
    if cache_path.exists() and not force_rebuild:
        log.info("Loading cached skew history...")
        return pd.read_pickle(str(cache_path))

    log.info("Building skew history (first run, ~30-45 min)...")

    # Load prices for RV computation
    prices = pd.read_pickle(str(CACHE / "bt_prices.pkl"))
    if not isinstance(prices.index, pd.DatetimeIndex):
        prices.index = pd.to_datetime(prices.index)

    # Pre-compute RV (vectorized, instant)
    rv_df = _compute_rv(prices)

    # Price lookup: date_int -> {root: price}
    prices_date_int = {}
    for ts in prices.index:
        d_int = _date_to_int(ts)
        row = prices.loc[ts].dropna()
        prices_date_int[d_int] = row[row > 0].to_dict()

    # RV lookup: date_int -> {root: rv}
    rv_date_int = {}
    for ts in rv_df.index:
        d_int = _date_to_int(ts)
        row = rv_df.loc[ts].dropna()
        rv_date_int[d_int] = row[row > 0].to_dict()

    # Volume filter
    vol_dict = _load_daily_volumes()
    trading_dates_arr = np.array(sorted(prices_date_int.keys()))

    conn = sqlite3.connect(str(DB))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA cache_size=-512000")

    # Get all trading dates from DB
    db_dates = [r[0] for r in conn.execute(
        "SELECT DISTINCT date FROM eod_history ORDER BY date"
    ).fetchall()]
    log.info("DB dates: %d, price dates: %d", len(db_dates), len(prices_date_int))

    results = []
    n_processed = 0
    n_no_exp = 0
    n_no_data = 0

    for i, date_int in enumerate(db_dates):
        # Need stock prices for this date
        if date_int not in prices_date_int:
            continue
        price_row = prices_date_int[date_int]
        rv_row = rv_date_int.get(date_int, {})

        # Find target monthly expiration
        target_exp = _find_target_monthly(date_int, TARGET_DTE_MIN, TARGET_DTE_MAX)
        if target_exp is None:
            n_no_exp += 1
            continue

        obs_date = _int_to_date(date_int)
        exp_date = _int_to_date(target_exp)
        dte = (exp_date - obs_date).days

        # Batch query: all options for this expiration on this date
        rows = conn.execute("""
            SELECT c.root, c.strike / 1000.0 AS strike, c.right,
                   e.bid, e.ask
            FROM eod_history e
            JOIN contracts c ON e.contract_id = c.contract_id
            WHERE e.date = ? AND c.expiration = ?
              AND e.bid > 0 AND e.ask > 0
        """, (date_int, target_exp)).fetchall()

        if not rows:
            n_no_data += 1
            continue

        # Convert to DataFrame
        chain = pd.DataFrame(rows, columns=["root", "strike", "right", "bid", "ask"])

        # Group by root and extract skew metrics
        for root, grp in chain.groupby("root"):
            # Check stock price
            S = price_row.get(root)
            if S is None or S <= 0:
                continue

            # Volume filter
            if vol_dict:
                avg_vol = _avg_daily_volume(vol_dict, root, date_int, trading_dates_arr)
                if avg_vol < MIN_OPT_VOLUME:
                    continue

            metrics = _extract_skew_for_root(grp, S, dte)
            if metrics is None:
                continue

            rv_20d = rv_row.get(root, np.nan)
            # Use implied forward as underlying (corrects for dividend adjustment)
            fwd = metrics.pop("implied_forward", S)

            results.append({
                "date": date_int,
                "root": root,
                "expiration": target_exp,
                "dte": dte,
                "underlying": fwd,
                **metrics,
                "rv_20d": rv_20d,
                "iv_rv_spread": metrics["atm_iv"] - rv_20d if not np.isnan(rv_20d) else np.nan,
            })

        n_processed += 1
        if (i + 1) % 100 == 0 or i == 0:
            log.info("[%d/%d] date=%d, %d records so far",
                     i + 1, len(db_dates), date_int, len(results))

    conn.close()

    df = pd.DataFrame(results)
    log.info("Done: %d records, %d dates processed, %d no expiration, %d no data",
             len(df), n_processed, n_no_exp, n_no_data)

    if not df.empty:
        df = df.sort_values(["date", "root"]).reset_index(drop=True)
        df.to_pickle(str(cache_path))
        log.info("Cached to %s", cache_path)

    return df


# ═══════════════════════════════════════════════════════════════
# Phase 2 — Signal Computation
# ═══════════════════════════════════════════════════════════════

def compute_skew_signals(history: pd.DataFrame) -> pd.DataFrame:
    """Compute skew z-scores and IV-RV gap, then generate trade signals.

    For each ticker:
      - skew_zscore: rolling 60-day z-score of skew_put
      - iv_rv_spread: atm_iv - rv_20d (positive = options expensive)

    Signals:
      - credit_signal: skew_zscore > 1.5 AND iv_rv_spread > 0.05
      - debit_signal:  skew_zscore < -1.0 AND iv_rv_spread < -0.03
    """
    df = history.copy()
    df = df.dropna(subset=["skew_put", "iv_rv_spread"])

    if df.empty:
        log.warning("No valid rows after dropping NaN skew/iv_rv_spread")
        return df

    # Rolling z-score of skew_put per ticker
    df = df.sort_values(["root", "date"]).reset_index(drop=True)
    df["skew_mean"] = np.nan
    df["skew_std"] = np.nan
    df["skew_zscore"] = np.nan

    for root, grp in df.groupby("root"):
        idx = grp.index
        skew = grp["skew_put"].values

        # Rolling stats (expanding up to SKEW_LOOKBACK)
        for j in range(len(grp)):
            window_start = max(0, j - SKEW_LOOKBACK + 1)
            window = skew[window_start:j + 1]
            if len(window) >= 20:  # need min 20 obs for stable z-score
                mu = window[:-1].mean()  # exclude current for signal purity
                sigma = window[:-1].std()
                if sigma > 1e-8:
                    df.loc[idx[j], "skew_mean"] = mu
                    df.loc[idx[j], "skew_std"] = sigma
                    df.loc[idx[j], "skew_zscore"] = (skew[j] - mu) / sigma

    df = df.dropna(subset=["skew_zscore"])
    log.info("Signals computed: %d rows with valid z-scores", len(df))

    # Generate binary signals
    df["credit_signal"] = (
        (df["skew_zscore"] > CREDIT_SKEW_Z) &
        (df["iv_rv_spread"] > CREDIT_IVRV_MIN)
    ).astype(int)

    if DEBIT_ENABLED:
        df["debit_signal"] = (
            (df["skew_zscore"] < DEBIT_SKEW_Z) &
            (df["iv_rv_spread"] < DEBIT_IVRV_MAX)
        ).astype(int)
    else:
        df["debit_signal"] = 0

    n_credit = df["credit_signal"].sum()
    n_debit = df["debit_signal"].sum()
    log.info("Credit signals: %d (%.1f%%), Debit signals: %d (%.1f%%)",
             n_credit, 100 * n_credit / len(df) if len(df) > 0 else 0,
             n_debit, 100 * n_debit / len(df) if len(df) > 0 else 0)

    return df


# ═══════════════════════════════════════════════════════════════
# Phase 2.5 — Walk-Forward Regression
# ═══════════════════════════════════════════════════════════════

# Regression features (predictors of credit spread return)
REG_FEATURES = [
    "skew_zscore",
    "iv_rv_spread",
    "credit_width_ratio",
    "width_pct",
    "rv_20d",
]
MIN_TRAIN = 500  # minimum training samples for first OOS prediction


def _compute_credit_features(df: pd.DataFrame) -> pd.DataFrame:
    """Enrich signals DataFrame with credit spread features for regression.

    Adds credit/width ratio, width as % of underlying, and the
    hold-to-expiry net_return (target variable) for each potential trade.
    """
    prices = pd.read_pickle(str(CACHE / "bt_prices.pkl"))
    if not isinstance(prices.index, pd.DatetimeIndex):
        prices.index = pd.to_datetime(prices.index)
    price_lookup = {}
    for ts in prices.index:
        d_int = _date_to_int(ts)
        row = prices.loc[ts].dropna()
        price_lookup[d_int] = row[row > 0].to_dict()
    trading_dates = np.array(sorted(price_lookup.keys()))

    out = df.copy()
    out["credit_per_share"] = np.nan
    out["width"] = np.nan
    out["credit_width_ratio"] = np.nan
    out["width_pct"] = np.nan
    out["net_return"] = np.nan
    out["max_loss"] = np.nan

    for i in out.index:
        r = out.loc[i]
        K_high = r["put25d_strike"]
        K_low = r["put10d_strike"]
        width = K_high - K_low
        if width < 1.0:
            continue

        sell = r["put25d_bid"] + BA_FILL_FRACTION * (r["put25d_ask"] - r["put25d_bid"])
        buy = r["put10d_ask"] - BA_FILL_FRACTION * (r["put10d_ask"] - r["put10d_bid"])
        credit = sell - buy
        if credit <= 0:
            continue

        max_loss = width - credit
        if max_loss <= 0:
            continue

        out.loc[i, "credit_per_share"] = credit
        out.loc[i, "width"] = width
        out.loc[i, "credit_width_ratio"] = credit / width
        out.loc[i, "width_pct"] = width / r["underlying"]
        out.loc[i, "max_loss"] = max_loss

        # Compute hold-to-expiry P&L (target variable)
        exp_int = r["expiration"]
        root = r["root"]
        idx = np.searchsorted(trading_dates, exp_int, side="right")
        S_exp = None
        for offset in [0, -1, -2, 1]:
            ci = idx + offset
            if 0 <= ci < len(trading_dates):
                d = int(trading_dates[ci])
                pr = price_lookup.get(d, {})
                if root in pr:
                    S_exp = pr[root]
                    break
        if S_exp is None:
            continue

        short_intr = max(K_high - S_exp, 0)
        long_intr = max(K_low - S_exp, 0)
        spread_val = max(0, min(short_intr - long_intr, width))
        pnl = credit - spread_val
        comm = COMMISSION_LEG * 4 / CONTRACT_MULT
        out.loc[i, "net_return"] = (pnl - comm) / max_loss

    # Drop rows without valid features/target
    before = len(out)
    out = out.dropna(subset=REG_FEATURES + ["net_return"]).reset_index(drop=True)
    log.info("Credit features: %d/%d rows with valid features + P&L", len(out), before)
    return out


def walk_forward_regression(
    featured_df: pd.DataFrame,
    min_train: int = MIN_TRAIN,
) -> tuple:
    """Expanding-window OLS regression on credit spread returns.

    Predicts net_return from REG_FEATURES. Filters: predicted_return > 0.
    Returns (full_df, tradeable_df, model_info).
    """
    df = featured_df.sort_values("date").reset_index(drop=True)
    df["predicted_return"] = np.nan
    df["is_oos"] = False

    X = df[REG_FEATURES].values
    y = df["net_return"].values

    for t in range(min_train, len(df)):
        X_train = X[:t]
        y_train = y[:t]

        mask = np.isfinite(X_train).all(axis=1) & np.isfinite(y_train)
        if mask.sum() < 100:
            continue

        model = LinearRegression()
        model.fit(X_train[mask], y_train[mask])

        x_t = X[t:t + 1]
        if np.isfinite(x_t).all():
            df.loc[t, "predicted_return"] = float(model.predict(x_t)[0])
            df.loc[t, "is_oos"] = True

    oos = df[df["is_oos"]].copy()
    tradeable = oos[oos["predicted_return"] > 0].copy()

    log.info("Walk-forward: %d OOS predictions, %d tradeable (pred > 0)",
             len(oos), len(tradeable))

    # Final model on all data (for display + live use)
    mask_all = np.isfinite(X).all(axis=1) & np.isfinite(y)
    final_model = LinearRegression()
    final_model.fit(X[mask_all], y[mask_all])

    model_info = {
        "coefficients": dict(zip(REG_FEATURES, final_model.coef_.tolist())),
        "intercept": float(final_model.intercept_),
        "r2": float(final_model.score(X[mask_all], y[mask_all])),
        "n_train": int(mask_all.sum()),
    }

    # OOS stats
    if len(tradeable) > 10:
        from scipy import stats as sp_stats
        oos_valid = oos.dropna(subset=["predicted_return", "net_return"])
        if len(oos_valid) > 10:
            corr, pval = sp_stats.pearsonr(
                oos_valid["predicted_return"], oos_valid["net_return"]
            )
            model_info["oos_correlation"] = round(corr, 4)
            model_info["oos_pvalue"] = round(pval, 6)
            model_info["oos_n"] = len(oos_valid)
            model_info["tradeable_n"] = len(tradeable)
            model_info["tradeable_wr"] = round(
                float((tradeable["net_return"] > 0).mean()) * 100, 1
            )
            model_info["tradeable_mean_return"] = round(
                float(tradeable["net_return"].mean()) * 100, 2
            )

    return df, tradeable, model_info


# ═══════════════════════════════════════════════════════════════
# Phase 3 — Backtest
# ═══════════════════════════════════════════════════════════════

def run_skew_backtest(
    signals_df: pd.DataFrame,
    initial: float = INITIAL_CAPITAL,
    max_pos: int = MAX_POSITIONS,
    z_track_df: pd.DataFrame | None = None,
) -> dict:
    """Backtest iron condors with skew mean-reversion exit.

    Delta-neutral structure: sell 25D put/call, buy 10D put/call.
    Positions are closed when the skew z-score drops below
    EXIT_Z_THRESHOLD (mean reversion captured).  Fallback to
    intrinsic-value P&L at expiration if skew never normalizes.

    signals_df:  rows eligible for entry (filtered by threshold)
    z_track_df:  full z-score data for ALL (root, date) pairs — used to
                 track skew daily and decide when to exit.
    """
    entry_df = signals_df.sort_values("date").reset_index(drop=True)

    # Z-score tracking: use full signals for exit decisions
    if z_track_df is None:
        z_track = entry_df
    else:
        z_track = z_track_df.sort_values("date").reset_index(drop=True)

    # Build info lookup: (root, date) -> vol surface info for exit pricing
    # Includes full 5-point surface (put10D, put25D, ATM, call25D, call10D)
    info_lookup: dict[tuple, dict] = {}
    for _, r in z_track.iterrows():
        info_lookup[(r["root"], r["date"])] = {
            "skew_zscore": r["skew_zscore"],
            "underlying": r["underlying"],
            "dte": r["dte"],
            "atm_strike": r["atm_strike"],
            "atm_iv": r["atm_iv"],
            "put25d_strike": r["put25d_strike"],
            "put25d_iv": r["put25d_iv"],
            "put10d_strike": r["put10d_strike"],
            "put10d_iv": r["put10d_iv"],
            "call25d_strike": r.get("call25d_strike"),
            "call25d_iv": r.get("call25d_iv"),
            "call10d_strike": r.get("call10d_strike"),
            "call10d_iv": r.get("call10d_iv"),
            "date": r["date"],
        }

    # All trading dates (from z_track, which has every date)
    all_dates = sorted(z_track["date"].unique())
    date_idx = {d: i for i, d in enumerate(all_dates)}

    # Entry lookup: date -> list of entry candidates
    entry_by_date: dict[int, list] = {}
    for _, r in entry_df.iterrows():
        if r.get("credit_signal", 0) == 1:
            entry_by_date.setdefault(r["date"], []).append(r)

    # Prices for expiry fallback
    prices = pd.read_pickle(str(CACHE / "bt_prices.pkl"))
    if not isinstance(prices.index, pd.DatetimeIndex):
        prices.index = pd.to_datetime(prices.index)
    price_lookup = {}
    for ts in prices.index:
        d_int = _date_to_int(ts)
        row = prices.loc[ts].dropna()
        price_lookup[d_int] = row[row > 0].to_dict()
    trading_dates = np.array(sorted(price_lookup.keys()))

    account = initial
    positions = []
    trade_log = []
    equity_log = []
    n_exit_skew = 0
    n_exit_expiry = 0

    for day in all_dates:
        # ── Check exits ──
        closed_idx = []
        for p_idx, pos in enumerate(positions):
            root = pos["root"]

            # 1) Expiry check
            if day >= pos["expiration"]:
                _close_at_expiry(pos, price_lookup, trading_dates)
                closed_idx.append(p_idx)
                n_exit_expiry += 1
                continue

            # 2) Skew normalization check
            info = info_lookup.get((root, day))
            if info is None:
                continue
            entry_i = date_idx.get(pos["entry_date"], 0)
            day_i = date_idx.get(day, 0)
            days_held = day_i - entry_i
            if days_held >= MIN_HOLD_DAYS and info["skew_zscore"] < EXIT_Z_THRESHOLD:
                _close_at_skew_normal(pos, info)
                closed_idx.append(p_idx)
                n_exit_skew += 1

        for idx in sorted(closed_idx, reverse=True):
            p = positions.pop(idx)
            trade_log.append(p)
            account += p["invested"] + p["pnl"]

        # ── Enter new positions ──
        slots = max_pos - len(positions)
        if slots <= 0:
            _log_equity(equity_log, day, account, positions)
            continue

        held_tickers = {p["root"] for p in positions}
        candidates = entry_by_date.get(day, [])
        candidates = [r for r in candidates if r["root"] not in held_tickers]
        candidates.sort(key=lambda r: r["skew_zscore"], reverse=True)

        for row in candidates:
            if slots <= 0:
                break
            pos = _build_iron_condor(row, account, day)
            if pos is not None:
                account -= pos["invested"]
                positions.append(pos)
                held_tickers.add(pos["root"])
                slots -= 1

        _log_equity(equity_log, day, account, positions)

    # Close remaining at last available date
    for pos in positions:
        _close_at_expiry(pos, price_lookup, trading_dates)
        trade_log.append(pos)
        account += pos["invested"] + pos["pnl"]
        n_exit_expiry += 1

    log.info("Exits: %d skew-normal, %d expiry", n_exit_skew, n_exit_expiry)

    trades_df = pd.DataFrame(trade_log) if trade_log else pd.DataFrame()
    equity_df = pd.DataFrame(equity_log) if equity_log else pd.DataFrame()
    stats = _compute_stats(trades_df, equity_df, initial)

    return {"trades": trades_df, "equity": equity_df, "stats": stats}


def _build_credit_position(row, account, day) -> dict | None:
    """Build a credit spread: sell 25D put, buy 10D put.

    Margin = width * contracts * multiplier (collateral held by broker).
    """
    put25d_bid = row["put25d_bid"]
    put25d_ask = row["put25d_ask"]
    put10d_bid = row["put10d_bid"]
    put10d_ask = row["put10d_ask"]
    put25d_strike = row["put25d_strike"]
    put10d_strike = row["put10d_strike"]

    width = put25d_strike - put10d_strike
    if width < 1.0:  # minimum $1 spread width
        return None

    # Credit: sell 25D put (receive), buy 10D put (pay)
    sell_price = put25d_bid + BA_FILL_FRACTION * (put25d_ask - put25d_bid)
    buy_price = put10d_ask - BA_FILL_FRACTION * (put10d_ask - put10d_bid)
    credit = sell_price - buy_price
    if credit <= 0:
        return None

    max_loss = width - credit
    if max_loss <= 0:
        return None

    # Sizing: limit risk to DEFAULT_ALLOC of account
    alloc = DEFAULT_ALLOC * account
    contracts = int(alloc / (max_loss * CONTRACT_MULT))
    contracts = max(1, min(contracts, MAX_CONTRACTS))

    # Invested = margin held (width of spread as collateral)
    invested = width * contracts * CONTRACT_MULT

    if invested > account * 0.5:
        return None

    return {
        "root": row["root"],
        "side": "credit",
        "entry_date": day,
        "expiration": row["expiration"],
        "underlying_entry": row["underlying"],
        "put25d_strike": put25d_strike,
        "put10d_strike": put10d_strike,
        "credit_per_share": credit,
        "width": width,
        "contracts": contracts,
        "invested": invested,
        "pnl": 0.0,
        "entry_skew_z": row["skew_zscore"],
        "entry_iv_rv": row["iv_rv_spread"],
        "ba_25d": put25d_ask - put25d_bid,
        "ba_10d": put10d_ask - put10d_bid,
        "exit_date": None,
        "exit_reason": None,
    }


def _build_debit_position(row, account, day) -> dict | None:
    """Build a debit spread: buy 25D put, sell 10D put."""
    put25d_bid = row["put25d_bid"]
    put25d_ask = row["put25d_ask"]
    put10d_bid = row["put10d_bid"]
    put10d_ask = row["put10d_ask"]
    put25d_strike = row["put25d_strike"]
    put10d_strike = row["put10d_strike"]

    width = put25d_strike - put10d_strike
    if width < 1.0:
        return None

    # Debit: buy 25D put (pay), sell 10D put (receive)
    buy_price = put25d_ask - BA_FILL_FRACTION * (put25d_ask - put25d_bid)
    sell_price = put10d_bid + BA_FILL_FRACTION * (put10d_ask - put10d_bid)
    debit = buy_price - sell_price
    if debit <= 0 or debit >= width:
        return None

    # Sizing: limit risk to DEFAULT_ALLOC of account
    alloc = DEFAULT_ALLOC * account
    contracts = int(alloc / (debit * CONTRACT_MULT))
    contracts = max(1, min(contracts, MAX_CONTRACTS))

    invested = debit * contracts * CONTRACT_MULT

    if invested > account * 0.5:
        return None

    return {
        "root": row["root"],
        "side": "debit",
        "entry_date": day,
        "expiration": row["expiration"],
        "underlying_entry": row["underlying"],
        "put25d_strike": put25d_strike,
        "put10d_strike": put10d_strike,
        "debit_per_share": debit,
        "width": width,
        "contracts": contracts,
        "invested": invested,
        "pnl": 0.0,
        "entry_skew_z": row["skew_zscore"],
        "entry_iv_rv": row["iv_rv_spread"],
        "exit_date": None,
        "exit_reason": None,
    }


def _build_iron_condor(row, account, day) -> dict | None:
    """Build iron condor: sell 25D put/call, buy 10D put/call.

    Put wing: sell 25D put, buy 10D put (bull put spread)
    Call wing: sell 25D call, buy 10D call (bear call spread)
    Combined: delta-neutral, short skew premium.
    """
    # Check call10d data exists
    if "call10d_bid" not in row or pd.isna(row.get("call10d_bid")):
        return None

    # ── Put leg (same as credit spread) ──
    p25_bid, p25_ask = row["put25d_bid"], row["put25d_ask"]
    p10_bid, p10_ask = row["put10d_bid"], row["put10d_ask"]
    K_p25, K_p10 = row["put25d_strike"], row["put10d_strike"]

    put_width = K_p25 - K_p10
    if put_width < 1.0:
        return None
    put_sell = p25_bid + BA_FILL_FRACTION * (p25_ask - p25_bid)
    put_buy = p10_ask - BA_FILL_FRACTION * (p10_ask - p10_bid)
    put_credit = put_sell - put_buy
    if put_credit <= 0:
        return None

    # ── Call leg ──
    c25_bid, c25_ask = row["call25d_bid"], row["call25d_ask"]
    c10_bid, c10_ask = row["call10d_bid"], row["call10d_ask"]
    K_c25, K_c10 = row["call25d_strike"], row["call10d_strike"]

    call_width = K_c10 - K_c25
    if call_width < 1.0:
        return None
    call_sell = c25_bid + BA_FILL_FRACTION * (c25_ask - c25_bid)
    call_buy = c10_ask - BA_FILL_FRACTION * (c10_ask - c10_bid)
    call_credit = call_sell - call_buy
    if call_credit <= 0:
        return None

    # ── Combined ──
    total_credit = put_credit + call_credit
    max_width = max(put_width, call_width)
    max_loss = max_width - total_credit
    if max_loss <= 0:
        return None

    # Sizing: margin = wider side only (only one side can lose at expiry)
    alloc = DEFAULT_ALLOC * account
    contracts = int(alloc / (max_loss * CONTRACT_MULT))
    contracts = max(1, min(contracts, MAX_CONTRACTS))
    invested = max_width * contracts * CONTRACT_MULT

    if invested > account * 0.5:
        return None

    return {
        "root": row["root"],
        "side": "iron_condor",
        "entry_date": day,
        "expiration": row["expiration"],
        "underlying_entry": row["underlying"],
        # Put leg
        "put25d_strike": K_p25,
        "put10d_strike": K_p10,
        "put_credit": put_credit,
        "put_width": put_width,
        # Call leg
        "call25d_strike": K_c25,
        "call10d_strike": K_c10,
        "call_credit": call_credit,
        "call_width": call_width,
        # Combined
        "credit_per_share": total_credit,
        "width": max_width,
        "contracts": contracts,
        "invested": invested,
        "pnl": 0.0,
        "entry_skew_z": row["skew_zscore"],
        "entry_iv_rv": row["iv_rv_spread"],
        "ba_p25": p25_ask - p25_bid,
        "ba_p10": p10_ask - p10_bid,
        "ba_c25": c25_ask - c25_bid,
        "ba_c10": c10_ask - c10_bid,
        "exit_date": None,
        "exit_reason": None,
    }


def _close_at_expiry(pos, price_lookup, trading_dates):
    """Compute P&L at expiration using intrinsic value.

    Handles credit spreads, debit spreads, and iron condors.
    """
    exp_int = pos["expiration"]
    root = pos["root"]

    # Find underlying price at expiration (or closest trading date)
    idx = np.searchsorted(trading_dates, exp_int, side="right")
    S_exp = None
    for offset in [0, -1, -2, 1]:
        check_idx = idx + offset
        if 0 <= check_idx < len(trading_dates):
            d = int(trading_dates[check_idx])
            pr = price_lookup.get(d, {})
            if root in pr:
                S_exp = pr[root]
                pos["exit_date"] = d
                break

    if S_exp is None:
        pos["exit_date"] = exp_int
        pos["exit_reason"] = "no_data"
        n_legs = 8 if pos["side"] == "iron_condor" else 4
        pos["pnl"] = -COMMISSION_LEG * n_legs * pos["contracts"]
        return

    pos["exit_reason"] = "expiry"
    pos["underlying_exit"] = S_exp
    contracts = pos["contracts"]

    if pos["side"] == "iron_condor":
        # Put wing: short 25D put, long 10D put
        short_put = max(pos["put25d_strike"] - S_exp, 0)
        long_put = max(pos["put10d_strike"] - S_exp, 0)
        put_loss = max(0, min(short_put - long_put, pos["put_width"]))

        # Call wing: short 25D call, long 10D call
        short_call = max(S_exp - pos["call25d_strike"], 0)
        long_call = max(S_exp - pos["call10d_strike"], 0)
        call_loss = max(0, min(short_call - long_call, pos["call_width"]))

        pnl_per_share = pos["credit_per_share"] - put_loss - call_loss
        n_legs = 8  # 4 legs × open + close

    elif pos["side"] == "credit":
        K_high = pos["put25d_strike"]
        K_low = pos["put10d_strike"]
        short_put_intrinsic = max(K_high - S_exp, 0)
        long_put_intrinsic = max(K_low - S_exp, 0)
        spread_value = max(0, min(short_put_intrinsic - long_put_intrinsic, pos["width"]))
        pnl_per_share = pos["credit_per_share"] - spread_value
        n_legs = 4

    else:  # debit
        K_high = pos["put25d_strike"]
        K_low = pos["put10d_strike"]
        short_put_intrinsic = max(K_high - S_exp, 0)
        long_put_intrinsic = max(K_low - S_exp, 0)
        spread_value = max(0, min(short_put_intrinsic - long_put_intrinsic, pos["width"]))
        pnl_per_share = spread_value - pos["debit_per_share"]
        n_legs = 4

    pos["pnl"] = round(
        pnl_per_share * contracts * CONTRACT_MULT
        - COMMISSION_LEG * n_legs * contracts,
        2
    )


def _close_at_skew_normal(pos, exit_info):
    """Close position when skew normalizes, using BS pricing at entry strikes.

    Interpolates IV at entry strikes from the exit-date 5-point vol surface,
    then prices legs to compute the exit cost.

    exit_info: dict with underlying, atm_strike, atm_iv,
               put25d_strike, put25d_iv, put10d_strike, put10d_iv,
               call25d_strike, call25d_iv, call10d_strike, call10d_iv, date
    """
    S = exit_info["underlying"]
    # Use POSITION's expiration, not the skew surface's current monthly
    exp_date = _int_to_date(pos["expiration"])
    exit_date_obj = _int_to_date(exit_info["date"])
    T = max((exp_date - exit_date_obj).days / 365.0, 1 / 365)

    # Common vol surface args for interpolation
    vs = dict(
        S_exit=S,
        atm_strike=exit_info["atm_strike"],
        atm_iv=exit_info["atm_iv"],
        put25d_strike=exit_info["put25d_strike"],
        put25d_iv=exit_info["put25d_iv"],
        put10d_strike=exit_info["put10d_strike"],
        put10d_iv=exit_info["put10d_iv"],
        call25d_strike=exit_info.get("call25d_strike"),
        call25d_iv=exit_info.get("call25d_iv"),
        call10d_strike=exit_info.get("call10d_strike"),
        call10d_iv=exit_info.get("call10d_iv"),
    )

    contracts = pos["contracts"]

    if pos["side"] == "iron_condor":
        # Put leg: buy back short 25D put, sell long 10D put
        K_p25 = pos["put25d_strike"]
        K_p10 = pos["put10d_strike"]
        sig_p25 = _interp_iv(K_p25, **vs)
        sig_p10 = _interp_iv(K_p10, **vs)
        exit_p25_mid = _bs_put_price(S, K_p25, T, sig_p25)
        exit_p10_mid = _bs_put_price(S, K_p10, T, sig_p10)

        # Call leg: buy back short 25D call, sell long 10D call
        K_c25 = pos["call25d_strike"]
        K_c10 = pos["call10d_strike"]
        sig_c25 = _interp_iv(K_c25, **vs)
        sig_c10 = _interp_iv(K_c10, **vs)
        exit_c25_mid = _bs_call_price(S, K_c25, T, sig_c25)
        exit_c10_mid = _bs_call_price(S, K_c10, T, sig_c10)

        # BA crossing cost (use entry BA as proxy)
        ba_p25 = pos.get("ba_p25", 0.10)
        ba_p10 = pos.get("ba_p10", 0.10)
        ba_c25 = pos.get("ba_c25", 0.10)
        ba_c10 = pos.get("ba_c10", 0.10)

        # Close put leg: buy back 25D put (pay), sell 10D put (receive)
        put_exit_cost = (exit_p25_mid + BA_FILL_FRACTION * ba_p25) - \
                        (exit_p10_mid - BA_FILL_FRACTION * ba_p10)
        # Close call leg: buy back 25D call (pay), sell 10D call (receive)
        call_exit_cost = (exit_c25_mid + BA_FILL_FRACTION * ba_c25) - \
                         (exit_c10_mid - BA_FILL_FRACTION * ba_c10)

        total_exit_cost = put_exit_cost + call_exit_cost
        pnl_per_share = pos["credit_per_share"] - total_exit_cost
        n_legs = 8

    else:
        # Credit/debit spread (put side only)
        K_high = pos["put25d_strike"]
        K_low = pos["put10d_strike"]
        sig_high = _interp_iv(K_high, **vs)
        sig_low = _interp_iv(K_low, **vs)
        exit_25d_mid = _bs_put_price(S, K_high, T, sig_high)
        exit_10d_mid = _bs_put_price(S, K_low, T, sig_low)

        ba_25d = pos.get("ba_25d", 0.10)
        ba_10d = pos.get("ba_10d", 0.10)
        buyback = exit_25d_mid + BA_FILL_FRACTION * ba_25d
        sell_back = exit_10d_mid - BA_FILL_FRACTION * ba_10d
        exit_cost = buyback - sell_back
        pnl_per_share = pos["credit_per_share"] - exit_cost
        n_legs = 4

    pos["exit_date"] = exit_info["date"]
    pos["exit_reason"] = "skew_normal"
    pos["underlying_exit"] = S
    pos["exit_z"] = exit_info.get("skew_zscore", np.nan)
    pos["pnl"] = round(
        pnl_per_share * contracts * CONTRACT_MULT
        - COMMISSION_LEG * n_legs * contracts,
        2,
    )


def _log_equity(equity_log, day, account, positions):
    """Log daily equity snapshot."""
    invested_total = sum(p["invested"] for p in positions)
    equity_log.append({
        "date": day,
        "account": round(account + invested_total, 2),
        "cash": round(account, 2),
        "invested": round(invested_total, 2),
        "n_positions": len(positions),
    })


def _compute_stats(trades_df, equity_df, initial):
    """Compute backtest performance statistics."""
    if trades_df.empty:
        return {"n_trades": 0, "error": "No trades generated"}

    pnls = trades_df["pnl"].values
    invested = trades_df["invested"].values
    rets = np.where(invested > 0, pnls / invested, 0)

    final_equity = equity_df["account"].iloc[-1] if not equity_df.empty else initial

    # Years
    if not equity_df.empty:
        first_date = _int_to_date(int(equity_df["date"].iloc[0]))
        last_date = _int_to_date(int(equity_df["date"].iloc[-1]))
        n_years = max((last_date - first_date).days / 365.25, 0.1)
    else:
        n_years = 1

    # Drawdown
    equity_arr = equity_df["account"].values if not equity_df.empty else np.array([initial])
    peak = np.maximum.accumulate(equity_arr)
    dd = (equity_arr - peak) / peak
    max_dd = float(dd.min())

    # CAGR
    cagr = (final_equity / initial) ** (1 / n_years) - 1

    # Sharpe (annualized from per-trade)
    if "exit_date" in trades_df.columns and "entry_date" in trades_df.columns:
        holds = trades_df["exit_date"].astype(float) - trades_df["entry_date"].astype(float)
        # Approximate trading days from date_int diff (rough but good enough)
        avg_hold = max(holds.median() / 1.5, 1)  # date_int diff / ~1.5 ≈ trading days
    else:
        avg_hold = 30
    trades_per_year = 252 / avg_hold
    sharpe = (rets.mean() / rets.std() * np.sqrt(trades_per_year)
              if rets.std() > 0 else 0)

    # By side
    credit_mask = trades_df["side"] == "credit"
    debit_mask = trades_df["side"] == "debit"
    ic_mask = trades_df["side"] == "iron_condor"

    # Exit reason breakdown
    n_skew_normal = 0
    n_expiry = 0
    if "exit_reason" in trades_df.columns:
        n_skew_normal = int((trades_df["exit_reason"] == "skew_normal").sum())
        n_expiry = int((trades_df["exit_reason"] == "expiry").sum())

    stats = {
        "n_trades": len(trades_df),
        "n_credit": int(credit_mask.sum()),
        "n_debit": int(debit_mask.sum()),
        "n_iron_condor": int(ic_mask.sum()),
        "n_exit_skew": n_skew_normal,
        "n_exit_expiry": n_expiry,
        "win_rate": round(float((pnls > 0).mean()) * 100, 1),
        "mean_return": round(float(rets.mean()) * 100, 2),
        "median_return": round(float(np.median(rets)) * 100, 2),
        "std_return": round(float(rets.std()) * 100, 2),
        "total_pnl": round(float(pnls.sum()), 2),
        "initial_capital": initial,
        "final_equity": round(float(final_equity), 2),
        "cagr": round(float(cagr * 100), 2),
        "sharpe": round(float(sharpe), 2),
        "max_drawdown": round(max_dd * 100, 2),
    }

    # Credit-specific stats
    if credit_mask.any():
        c_pnls = pnls[credit_mask]
        c_rets = rets[credit_mask]
        stats["credit_win_rate"] = round(float((c_pnls > 0).mean()) * 100, 1)
        stats["credit_mean_return"] = round(float(c_rets.mean()) * 100, 2)
        stats["credit_total_pnl"] = round(float(c_pnls.sum()), 2)

    # Debit-specific stats
    if debit_mask.any():
        d_pnls = pnls[debit_mask]
        d_rets = rets[debit_mask]
        stats["debit_win_rate"] = round(float((d_pnls > 0).mean()) * 100, 1)
        stats["debit_mean_return"] = round(float(d_rets.mean()) * 100, 2)
        stats["debit_total_pnl"] = round(float(d_pnls.sum()), 2)

    # Iron condor stats
    if ic_mask.any():
        ic_pnls = pnls[ic_mask]
        ic_rets = rets[ic_mask]
        stats["ic_win_rate"] = round(float((ic_pnls > 0).mean()) * 100, 1)
        stats["ic_mean_return"] = round(float(ic_rets.mean()) * 100, 2)
        stats["ic_total_pnl"] = round(float(ic_pnls.sum()), 2)

        # Exit breakdown for iron condors
        if "exit_reason" in trades_df.columns:
            ic_trades = trades_df[ic_mask]
            ic_skew_exits = ic_trades[ic_trades["exit_reason"] == "skew_normal"]
            ic_exp_exits = ic_trades[ic_trades["exit_reason"] == "expiry"]
            if len(ic_skew_exits) > 0:
                stats["ic_skew_exit_wr"] = round(
                    float((ic_skew_exits["pnl"] > 0).mean()) * 100, 1)
                stats["ic_skew_exit_mean"] = round(
                    float(np.where(ic_skew_exits["invested"] > 0,
                                   ic_skew_exits["pnl"] / ic_skew_exits["invested"], 0).mean()) * 100, 2)
            if len(ic_exp_exits) > 0:
                stats["ic_exp_exit_wr"] = round(
                    float((ic_exp_exits["pnl"] > 0).mean()) * 100, 1)
                stats["ic_exp_exit_mean"] = round(
                    float(np.where(ic_exp_exits["invested"] > 0,
                                   ic_exp_exits["pnl"] / ic_exp_exits["invested"], 0).mean()) * 100, 2)

    return stats


# ═══════════════════════════════════════════════════════════════
# Strategy A — Cross-Sectional Skew Ranking (L/S equity)
# ═══════════════════════════════════════════════════════════════

QUINTILE_HOLD_MONTHS = 1  # monthly rebalance


def run_crosssectional_backtest(
    signals_df: pd.DataFrame, initial: float = INITIAL_CAPITAL,
) -> dict:
    """Monthly long/short equity portfolio sorted by skew z-score.

    Xing-Zhang-Zhao (2010): steep smirk predicts stock underperformance.
    Short Q5 (steep skew), Long Q1 (flat skew).  Monthly rebalance.
    """
    prices = pd.read_pickle(str(CACHE / "bt_prices.pkl"))
    if not isinstance(prices.index, pd.DatetimeIndex):
        prices.index = pd.to_datetime(prices.index)

    # Monthly end-of-month prices
    monthly_prices = prices.resample("ME").last()

    # Build monthly signal: last observation per (month, ticker)
    df = signals_df.copy()
    df["date_dt"] = df["date"].apply(lambda d: _int_to_date(int(d)))
    df["ym"] = df["date_dt"].apply(lambda d: (d.year, d.month))
    monthly_sigs = df.sort_values("date").groupby(["ym", "root"]).last().reset_index()

    sorted_months = sorted(monthly_sigs["ym"].unique())
    port_rows = []

    for i in range(len(sorted_months) - 1):
        ym = sorted_months[i]
        ym_next = sorted_months[i + 1]

        grp = monthly_sigs[monthly_sigs["ym"] == ym].dropna(subset=["skew_zscore"])
        if len(grp) < 25:
            continue

        grp = grp.sort_values("skew_zscore")
        q = max(len(grp) // 5, 1)
        q1 = grp.head(q)["root"].tolist()  # flat skew → long
        q5 = grp.tail(q)["root"].tolist()  # steep skew → short

        # Month-end prices
        y0, m0 = ym
        y1, m1 = ym_next
        try:
            p0 = monthly_prices.loc[f"{y0}-{m0:02d}"].iloc[-1] if \
                len(monthly_prices.loc[f"{y0}-{m0:02d}"]) > 0 else None
            p1 = monthly_prices.loc[f"{y1}-{m1:02d}"].iloc[-1] if \
                len(monthly_prices.loc[f"{y1}-{m1:02d}"]) > 0 else None
        except KeyError:
            continue
        if p0 is None or p1 is None:
            continue

        # Long returns
        long_rets = []
        for t in q1:
            if t in p0.index and t in p1.index and p0[t] > 0 and p1[t] > 0:
                long_rets.append(p1[t] / p0[t] - 1)

        # Short returns
        short_rets = []
        for t in q5:
            if t in p0.index and t in p1.index and p0[t] > 0 and p1[t] > 0:
                short_rets.append(-(p1[t] / p0[t] - 1))

        if long_rets and short_rets:
            ls = (np.mean(long_rets) + np.mean(short_rets)) / 2
            port_rows.append({
                "ym": ym, "long_ret": np.mean(long_rets),
                "short_ret": np.mean(short_rets), "ls_ret": ls,
                "n_long": len(long_rets), "n_short": len(short_rets),
            })

    if not port_rows:
        return {"stats": {"error": "No portfolio months"}}

    pdf = pd.DataFrame(port_rows)
    rets = pdf["ls_ret"].values

    # Equity curve
    equity = initial * np.cumprod(1 + rets)
    equity = np.insert(equity, 0, initial)
    final = float(equity[-1])
    n_years = max(len(rets) / 12, 0.1)
    cagr = (final / initial) ** (1 / n_years) - 1

    peak = np.maximum.accumulate(equity)
    dd = (equity - peak) / peak
    max_dd = float(dd.min())

    sharpe = float(rets.mean() / rets.std() * np.sqrt(12)) if rets.std() > 0 else 0

    stats = {
        "strategy": "cross_sectional",
        "n_months": len(rets),
        "monthly_mean": round(float(rets.mean()) * 100, 3),
        "monthly_median": round(float(np.median(rets)) * 100, 3),
        "monthly_std": round(float(rets.std()) * 100, 3),
        "win_rate": round(float((rets > 0).mean()) * 100, 1),
        "cagr": round(cagr * 100, 2),
        "sharpe": round(sharpe, 2),
        "max_drawdown": round(max_dd * 100, 2),
        "final_equity": round(final, 2),
        "long_mean": round(float(pdf["long_ret"].mean()) * 100, 3),
        "short_mean": round(float(pdf["short_ret"].mean()) * 100, 3),
    }
    return {"stats": stats, "monthly_df": pdf, "equity": equity}


# ═══════════════════════════════════════════════════════════════
# Strategy B — Delta-Hedged Risk Reversal
# ═══════════════════════════════════════════════════════════════

HEDGE_SLIP_BPS  = 10     # 10 bps per share rebalance
HEDGE_BAND      = 0.05   # rehedge when delta drifts > 5 delta


def _bs_delta_scalar(S, K, T, sigma, r=RISK_FREE_RATE):
    """Call delta (scalar) using BS.  Returns N(d1)."""
    if T <= 0 or sigma <= 0:
        return 1.0 if S > K else 0.0
    d1 = (np.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * np.sqrt(T))
    return float(sp_stats.norm.cdf(d1))


def _build_risk_reversal(row, account, day) -> dict | None:
    """Build delta-hedged risk reversal: sell 25D put + buy 25D call.

    Entry premiums computed from bid/ask with BA_FILL_FRACTION.
    Initial delta hedge in shares.
    """
    p25_bid = row["put25d_bid"]
    p25_ask = row["put25d_ask"]
    c25_bid = row["call25d_bid"]
    c25_ask = row["call25d_ask"]

    K_put = row["put25d_strike"]
    K_call = row["call25d_strike"]
    S = row["underlying"]
    T = row["dte"] / 365.0

    if T <= 0 or S <= 0:
        return None
    if np.isnan(p25_bid) or np.isnan(c25_ask):
        return None

    # Sell put (receive), buy call (pay)
    put_sell = p25_bid + BA_FILL_FRACTION * (p25_ask - p25_bid)
    call_buy = c25_ask - BA_FILL_FRACTION * (c25_ask - c25_bid)
    net_premium = put_sell - call_buy  # positive = net credit

    # Compute initial deltas
    put_iv = row["put25d_iv"]
    call_iv = row["call25d_iv"]
    if put_iv <= 0 or call_iv <= 0:
        return None

    # Short put delta = |put delta| = 1 - N(d1 at put strike)
    short_put_delta = 1.0 - _bs_delta_scalar(S, K_put, T, put_iv)
    # Long call delta = N(d1 at call strike)
    long_call_delta = _bs_delta_scalar(S, K_call, T, call_iv)
    net_delta = short_put_delta + long_call_delta  # ≈ +0.50

    # Hedge: short shares to neutralize delta
    contracts = 1
    shares_per = CONTRACT_MULT  # 100
    shares_to_short = -round(net_delta * shares_per * contracts)

    # Margin: max of put assignment risk or call exercise risk
    # Simplified: collateral = max(put_strike, call_strike) * contracts * multiplier
    # Conservative: put_strike (the assignment risk if stock drops)
    margin = K_put * contracts * CONTRACT_MULT
    # Also need capital for initial share hedge
    hedge_capital = abs(shares_to_short) * S * 0.5  # 50% margin on short shares

    invested = margin + hedge_capital
    if invested > account * 0.5:
        return None
    if invested <= 0:
        return None

    alloc = DEFAULT_ALLOC * account
    # Scale contracts to fit allocation
    contracts = max(1, int(alloc / invested))
    contracts = min(contracts, MAX_CONTRACTS)
    shares_to_short = -round(net_delta * shares_per * contracts)
    invested = (K_put * contracts * CONTRACT_MULT) + abs(shares_to_short) * S * 0.5

    if invested > account * 0.5:
        contracts = 1
        shares_to_short = -round(net_delta * shares_per)
        invested = (K_put * CONTRACT_MULT) + abs(shares_to_short) * S * 0.5

    return {
        "root": row["root"],
        "side": "risk_reversal",
        "entry_date": day,
        "expiration": row["expiration"],
        "exp_date": _int_to_date(row["expiration"]),
        "underlying_entry": S,
        "put_strike": K_put,
        "call_strike": K_call,
        "put_sell_price": put_sell,
        "call_buy_price": call_buy,
        "net_premium": net_premium,
        "entry_put_iv": put_iv,
        "entry_call_iv": call_iv,
        "contracts": contracts,
        "shares_held": shares_to_short,
        "last_price": S,
        "hedge_pnl": 0.0,
        "invested": invested,
        "pnl": 0.0,
        "entry_skew_z": row["skew_zscore"],
        "entry_iv_rv": row["iv_rv_spread"],
        "ba_put": p25_ask - p25_bid,
        "ba_call": c25_ask - c25_bid,
        "exit_date": None,
        "exit_reason": None,
    }


def _close_rr_at_expiry(pos, S_exp):
    """Close risk reversal at expiry using intrinsic values."""
    if S_exp is None:
        pos["pnl"] = pos["hedge_pnl"] - COMMISSION_LEG * 4 * pos["contracts"]
        pos["exit_reason"] = "no_data"
        return

    K_put = pos["put_strike"]
    K_call = pos["call_strike"]
    contracts = pos["contracts"]

    # Short put P&L at expiry: received premium, pay intrinsic if ITM
    put_intrinsic = max(K_put - S_exp, 0)
    # Long call P&L at expiry: paid premium, receive intrinsic if ITM
    call_intrinsic = max(S_exp - K_call, 0)

    # Option P&L per share
    option_pnl = (pos["net_premium"] - put_intrinsic + call_intrinsic) * contracts * CONTRACT_MULT

    # Final hedge P&L from last rebalance to expiry
    final_hedge = pos["shares_held"] * (S_exp - pos["last_price"])

    pos["pnl"] = round(
        option_pnl + pos["hedge_pnl"] + final_hedge
        - COMMISSION_LEG * 4 * contracts,
        2,
    )
    pos["exit_reason"] = "expiry"
    pos["underlying_exit"] = S_exp


def _close_rr_at_skew_normal(pos, exit_info, T_remain):
    """Close risk reversal when skew normalizes, BS pricing for options."""
    S = exit_info["underlying"]
    contracts = pos["contracts"]

    # Interpolate IV at entry strikes from exit vol surface
    vs = dict(
        S_exit=S,
        atm_strike=exit_info["atm_strike"],
        atm_iv=exit_info["atm_iv"],
        put25d_strike=exit_info["put25d_strike"],
        put25d_iv=exit_info["put25d_iv"],
        put10d_strike=exit_info["put10d_strike"],
        put10d_iv=exit_info["put10d_iv"],
        call25d_strike=exit_info.get("call25d_strike"),
        call25d_iv=exit_info.get("call25d_iv"),
        call10d_strike=exit_info.get("call10d_strike"),
        call10d_iv=exit_info.get("call10d_iv"),
    )

    put_iv = _interp_iv(pos["put_strike"], **vs)
    call_iv = _interp_iv(pos["call_strike"], **vs)

    # BS prices at exit
    put_exit = _bs_put_price(S, pos["put_strike"], T_remain, put_iv)
    call_exit = _bs_call_price(S, pos["call_strike"], T_remain, call_iv)

    # Close: buy back put (pay), sell call (receive)
    ba_put = pos.get("ba_put", 0.10)
    ba_call = pos.get("ba_call", 0.10)
    buyback_put = put_exit + BA_FILL_FRACTION * ba_put
    sell_call = call_exit - BA_FILL_FRACTION * ba_call

    # Option P&L = entry credit/debit + exit credit/debit
    # Entry: +put_sell - call_buy = +net_premium
    # Exit: -buyback_put + sell_call
    option_pnl = (pos["net_premium"] - buyback_put + sell_call) * contracts * CONTRACT_MULT

    # Final hedge P&L
    final_hedge = pos["shares_held"] * (S - pos["last_price"])

    pos["pnl"] = round(
        option_pnl + pos["hedge_pnl"] + final_hedge
        - COMMISSION_LEG * 4 * contracts,
        2,
    )
    pos["exit_date"] = exit_info["date"]
    pos["exit_reason"] = "skew_normal"
    pos["underlying_exit"] = S
    pos["exit_z"] = exit_info.get("skew_zscore", np.nan)


def run_risk_reversal_backtest(
    signals_df: pd.DataFrame,
    z_track_df: pd.DataFrame,
    initial: float = INITIAL_CAPITAL,
    max_pos: int = MAX_POSITIONS,
) -> dict:
    """Backtest delta-hedged risk reversals (sell 25D put + buy 25D call).

    The canonical skew trade.  Daily delta-hedge with shares removes
    directional exposure, leaving P&L dominated by skew/vanna.
    """
    entry_df = signals_df[signals_df["credit_signal"] == 1].sort_values("date").reset_index(drop=True)
    z_track = z_track_df.sort_values("date").reset_index(drop=True)

    # Build info lookup (full 5-point surface)
    info_lookup: dict[tuple, dict] = {}
    for _, r in z_track.iterrows():
        info_lookup[(r["root"], r["date"])] = {
            "skew_zscore": r["skew_zscore"],
            "underlying": r["underlying"],
            "dte": r["dte"],
            "expiration": r["expiration"],
            "atm_strike": r["atm_strike"],
            "atm_iv": r["atm_iv"],
            "put25d_strike": r["put25d_strike"],
            "put25d_iv": r["put25d_iv"],
            "put10d_strike": r["put10d_strike"],
            "put10d_iv": r["put10d_iv"],
            "call25d_strike": r.get("call25d_strike"),
            "call25d_iv": r.get("call25d_iv"),
            "call10d_strike": r.get("call10d_strike"),
            "call10d_iv": r.get("call10d_iv"),
            "date": r["date"],
        }

    all_dates = sorted(z_track["date"].unique())
    date_idx = {d: i for i, d in enumerate(all_dates)}

    entry_by_date: dict[int, list] = {}
    for _, r in entry_df.iterrows():
        entry_by_date.setdefault(r["date"], []).append(r)

    # Price lookup for expiry
    prices = pd.read_pickle(str(CACHE / "bt_prices.pkl"))
    if not isinstance(prices.index, pd.DatetimeIndex):
        prices.index = pd.to_datetime(prices.index)
    price_lookup = {}
    for ts in prices.index:
        d_int = _date_to_int(ts)
        row = prices.loc[ts].dropna()
        price_lookup[d_int] = row[row > 0].to_dict()
    trading_dates = np.array(sorted(price_lookup.keys()))

    account = initial
    positions = []
    trade_log = []
    equity_log = []
    n_exit_skew = 0
    n_exit_expiry = 0

    for day in all_dates:
        # ── Daily rebalance + exit checks ──
        closed_idx = []
        for p_idx, pos in enumerate(positions):
            root = pos["root"]
            info = info_lookup.get((root, day))

            # Price lookup for underlying
            S = None
            if info:
                S = info["underlying"]
            else:
                pr = price_lookup.get(day, {})
                S = pr.get(root)

            # 1) Expiry check
            if day >= pos["expiration"]:
                # Find price at expiry
                idx = np.searchsorted(trading_dates, pos["expiration"], side="right")
                S_exp = S
                for offset in [0, -1, -2, 1]:
                    ci = idx + offset
                    if 0 <= ci < len(trading_dates):
                        d = int(trading_dates[ci])
                        if root in price_lookup.get(d, {}):
                            S_exp = price_lookup[d][root]
                            pos["exit_date"] = d
                            break
                # Final hedge P&L
                if S_exp and S_exp != pos["last_price"]:
                    pos["hedge_pnl"] += pos["shares_held"] * (S_exp - pos["last_price"])
                    pos["last_price"] = S_exp
                _close_rr_at_expiry(pos, S_exp)
                closed_idx.append(p_idx)
                n_exit_expiry += 1
                continue

            if S is None:
                continue

            # Daily hedge P&L from price movement
            price_change = S - pos["last_price"]
            pos["hedge_pnl"] += pos["shares_held"] * price_change
            pos["last_price"] = S

            # Recompute deltas & rebalance
            T_remain = max((pos["exp_date"] - _int_to_date(day)).days / 365.0, 1 / 365)
            put_iv = info["put25d_iv"] if info else pos["entry_put_iv"]
            call_iv = info["call25d_iv"] if info else pos["entry_call_iv"]

            short_put_delta = 1.0 - _bs_delta_scalar(S, pos["put_strike"], T_remain, put_iv)
            long_call_delta = _bs_delta_scalar(S, pos["call_strike"], T_remain, call_iv)
            net_delta = (short_put_delta + long_call_delta) * CONTRACT_MULT * pos["contracts"]
            target_shares = -round(net_delta)

            drift = abs(target_shares - pos["shares_held"])
            if drift > HEDGE_BAND * CONTRACT_MULT * pos["contracts"]:
                cost = abs(target_shares - pos["shares_held"]) * S * HEDGE_SLIP_BPS / 10000
                pos["hedge_pnl"] -= cost
                pos["shares_held"] = target_shares

            # 2) Skew normalization check
            if info is None:
                continue
            entry_i = date_idx.get(pos["entry_date"], 0)
            day_i = date_idx.get(day, 0)
            days_held = day_i - entry_i
            if days_held >= MIN_HOLD_DAYS and info["skew_zscore"] < EXIT_Z_THRESHOLD:
                _close_rr_at_skew_normal(pos, info, T_remain)
                closed_idx.append(p_idx)
                n_exit_skew += 1

        for idx in sorted(closed_idx, reverse=True):
            p = positions.pop(idx)
            trade_log.append(p)
            account += p["invested"] + p["pnl"]

        # ── Enter new positions ──
        slots = max_pos - len(positions)
        if slots > 0:
            held = {p["root"] for p in positions}
            cands = entry_by_date.get(day, [])
            cands = [r for r in cands if r["root"] not in held]
            cands.sort(key=lambda r: r["skew_zscore"], reverse=True)

            for row in cands:
                if slots <= 0:
                    break
                pos = _build_risk_reversal(row, account, day)
                if pos is not None:
                    account -= pos["invested"]
                    positions.append(pos)
                    held.add(pos["root"])
                    slots -= 1

        _log_equity(equity_log, day, account, positions)

    # Close remaining
    for pos in positions:
        S_final = price_lookup.get(all_dates[-1], {}).get(pos["root"])
        if S_final:
            pos["hedge_pnl"] += pos["shares_held"] * (S_final - pos["last_price"])
            pos["last_price"] = S_final
        _close_rr_at_expiry(pos, S_final)
        pos["exit_date"] = all_dates[-1]
        trade_log.append(pos)
        account += pos["invested"] + pos["pnl"]
        n_exit_expiry += 1

    log.info("Risk Reversal exits: %d skew-normal, %d expiry", n_exit_skew, n_exit_expiry)

    trades_df = pd.DataFrame(trade_log) if trade_log else pd.DataFrame()
    equity_df = pd.DataFrame(equity_log) if equity_log else pd.DataFrame()
    stats = _compute_rr_stats(trades_df, equity_df, initial)
    return {"trades": trades_df, "equity": equity_df, "stats": stats}


def _compute_rr_stats(trades_df, equity_df, initial):
    """Stats for risk reversal backtest."""
    if trades_df.empty:
        return {"n_trades": 0, "error": "No trades"}

    pnls = trades_df["pnl"].values
    invested = trades_df["invested"].values
    rets = np.where(invested > 0, pnls / invested, 0)

    final_equity = equity_df["account"].iloc[-1] if not equity_df.empty else initial

    if not equity_df.empty:
        first_date = _int_to_date(int(equity_df["date"].iloc[0]))
        last_date = _int_to_date(int(equity_df["date"].iloc[-1]))
        n_years = max((last_date - first_date).days / 365.25, 0.1)
    else:
        n_years = 1

    equity_arr = equity_df["account"].values if not equity_df.empty else np.array([initial])
    peak = np.maximum.accumulate(equity_arr)
    dd = (equity_arr - peak) / peak
    max_dd = float(dd.min())

    cagr = (final_equity / initial) ** (1 / n_years) - 1

    # Sharpe
    if "exit_date" in trades_df.columns and "entry_date" in trades_df.columns:
        holds = trades_df["exit_date"].astype(float) - trades_df["entry_date"].astype(float)
        avg_hold = max(holds.median() / 1.5, 1)
    else:
        avg_hold = 30
    tpy = 252 / avg_hold
    sharpe = rets.mean() / rets.std() * np.sqrt(tpy) if rets.std() > 0 else 0

    # Exit breakdown
    n_skew = n_exp = 0
    skew_wr = exp_wr = skew_mean = exp_mean = 0.0
    if "exit_reason" in trades_df.columns:
        skew_mask = trades_df["exit_reason"] == "skew_normal"
        exp_mask = trades_df["exit_reason"] == "expiry"
        n_skew = int(skew_mask.sum())
        n_exp = int(exp_mask.sum())
        if n_skew > 0:
            skew_wr = float((pnls[skew_mask] > 0).mean()) * 100
            skew_mean = float(rets[skew_mask].mean()) * 100
        if n_exp > 0:
            exp_wr = float((pnls[exp_mask] > 0).mean()) * 100
            exp_mean = float(rets[exp_mask].mean()) * 100

    return {
        "strategy": "risk_reversal",
        "n_trades": len(trades_df),
        "n_exit_skew": n_skew,
        "n_exit_expiry": n_exp,
        "win_rate": round(float((pnls > 0).mean()) * 100, 1),
        "mean_return": round(float(rets.mean()) * 100, 2),
        "std_return": round(float(rets.std()) * 100, 2),
        "total_pnl": round(float(pnls.sum()), 2),
        "initial_capital": initial,
        "final_equity": round(float(final_equity), 2),
        "cagr": round(float(cagr * 100), 2),
        "sharpe": round(float(sharpe), 2),
        "max_drawdown": round(max_dd * 100, 2),
        "skew_exit_wr": round(skew_wr, 1),
        "skew_exit_mean": round(skew_mean, 2),
        "exp_exit_wr": round(exp_wr, 1),
        "exp_exit_mean": round(exp_mean, 2),
    }


# ═══════════════════════════════════════════════════════════════
# Phase 4 — Charts
# ═══════════════════════════════════════════════════════════════

def _apply_dark_theme(ax, fig=None):
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


def generate_skew_charts(backtest_result: dict, signals_df: pd.DataFrame) -> list:
    """Generate dark-themed charts for the skew strategy."""
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
        ax1.set_title("Skew Mean-Reversion Iron Condors")
        ax1.axhline(INITIAL_CAPITAL, color=DARK_GRID, linestyle="--", alpha=0.5)
        ax1.set_xticklabels([])

        peak = np.maximum.accumulate(vals)
        dd = (vals - peak) / peak * 100
        ax2.fill_between(dates, dd, 0, color=RED, alpha=0.4)
        ax2.set_ylabel("DD (%)")
        ax2.set_xlabel("Date")

    path = str(OUTPUT / "skew_equity.png")
    fig.savefig(path, dpi=120, bbox_inches="tight", facecolor=DARK_BG)
    plt.close(fig)
    charts.append("skew_equity.png")

    # 2. Return distribution by exit reason
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(10, 4))
    _apply_dark_theme(ax1, fig)
    _apply_dark_theme(ax2)

    if not trades.empty and "exit_reason" in trades.columns:
        for ax, reason, color, title in [
            (ax1, "skew_normal", GREEN, "Skew Mean-Reversion Exits"),
            (ax2, "expiry", ACCENT, "Hold-to-Expiry Exits"),
        ]:
            sub = trades[trades["exit_reason"] == reason]
            if not sub.empty:
                rets = np.where(sub["invested"] > 0,
                                sub["pnl"] / sub["invested"] * 100, 0)
                rets_clip = np.clip(rets, -100, 200)
                ax.hist(rets_clip, bins=40, color=color, alpha=0.7, edgecolor=DARK_GRID)
                ax.axvline(0, color=RED, linestyle="--", alpha=0.7)
                ax.axvline(np.mean(rets), color=YELLOW, linestyle="--", alpha=0.7,
                           label=f"Mean: {np.mean(rets):.1f}%")
                ax.set_xlabel("Return (%)")
                ax.set_ylabel("Count")
                ax.set_title(f"{title} (N={len(sub)})")
                ax.legend(facecolor=DARK_CARD, edgecolor=DARK_GRID, labelcolor=DARK_TEXT)
            else:
                ax.set_title(f"{title} (N=0)")

    path = str(OUTPUT / "skew_distribution.png")
    fig.savefig(path, dpi=120, bbox_inches="tight", facecolor=DARK_BG)
    plt.close(fig)
    charts.append("skew_distribution.png")

    # 3. Skew z-score distribution with signal thresholds
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(10, 4))
    _apply_dark_theme(ax1, fig)
    _apply_dark_theme(ax2)

    if not signals_df.empty:
        zscores = signals_df["skew_zscore"].dropna().values
        ax1.hist(zscores, bins=80, color=ACCENT, alpha=0.7, edgecolor=DARK_GRID)
        ax1.axvline(CREDIT_SKEW_Z, color=GREEN, linestyle="--", linewidth=2,
                    label=f"Credit thresh ({CREDIT_SKEW_Z})")
        ax1.axvline(DEBIT_SKEW_Z, color=RED, linestyle="--", linewidth=2,
                    label=f"Debit thresh ({DEBIT_SKEW_Z})")
        ax1.set_xlabel("Skew Z-Score")
        ax1.set_ylabel("Count")
        ax1.set_title("Skew Z-Score Distribution")
        ax1.legend(facecolor=DARK_CARD, edgecolor=DARK_GRID, labelcolor=DARK_TEXT,
                   fontsize=8)

        # IV-RV spread
        ivrv = signals_df["iv_rv_spread"].dropna().values
        ax2.hist(ivrv * 100, bins=80, color=YELLOW, alpha=0.7, edgecolor=DARK_GRID)
        ax2.axvline(CREDIT_IVRV_MIN * 100, color=GREEN, linestyle="--", linewidth=2,
                    label=f"Credit min ({CREDIT_IVRV_MIN*100:.0f}%)")
        ax2.axvline(DEBIT_IVRV_MAX * 100, color=RED, linestyle="--", linewidth=2,
                    label=f"Debit max ({DEBIT_IVRV_MAX*100:.0f}%)")
        ax2.set_xlabel("IV - RV (%)")
        ax2.set_ylabel("Count")
        ax2.set_title("IV-RV Spread Distribution")
        ax2.legend(facecolor=DARK_CARD, edgecolor=DARK_GRID, labelcolor=DARK_TEXT,
                   fontsize=8)

    path = str(OUTPUT / "skew_signals.png")
    fig.savefig(path, dpi=120, bbox_inches="tight", facecolor=DARK_BG)
    plt.close(fig)
    charts.append("skew_signals.png")

    # 4. Monthly P&L bars
    fig, ax = plt.subplots(figsize=(10, 4))
    _apply_dark_theme(ax, fig)

    if not trades.empty and "exit_date" in trades.columns:
        trades_copy = trades.copy()
        trades_copy["month"] = trades_copy["exit_date"].apply(
            lambda d: str(int(d))[:6] if pd.notna(d) else ""
        )
        trades_copy = trades_copy[trades_copy["month"] != ""]
        if not trades_copy.empty:
            monthly = trades_copy.groupby("month")["pnl"].sum()
            colors = [GREEN if v >= 0 else RED for v in monthly.values]
            ax.bar(range(len(monthly)), monthly.values, color=colors, alpha=0.8)
            step = max(1, len(monthly) // 15)
            ax.set_xticks(range(0, len(monthly), step))
            ax.set_xticklabels([monthly.index[i] for i in range(0, len(monthly), step)],
                               rotation=45, fontsize=7)
            ax.set_ylabel("P&L ($)")
            ax.set_title("Monthly P&L")
            ax.axhline(0, color=DARK_GRID, linestyle="--", alpha=0.5)

    path = str(OUTPUT / "skew_monthly.png")
    fig.savefig(path, dpi=120, bbox_inches="tight", facecolor=DARK_BG)
    plt.close(fig)
    charts.append("skew_monthly.png")

    return charts


# ═══════════════════════════════════════════════════════════════
# Main Entry Point
# ═══════════════════════════════════════════════════════════════

def compute_skew_analytics(force_rebuild: bool = False) -> dict:
    """Main entry point — compute all skew analytics.

    Runs 3 strategies:
      A) Cross-sectional skew ranking (L/S equity)
      B) Delta-hedged risk reversal (canonical skew trade)
      C) Iron condor with mean-reversion exit (baseline)

    Returns JSON-serializable dict. Cached with pickle.
    """
    cache_path = CACHE / "skew_analytics.pkl"

    if cache_path.exists() and not force_rebuild:
        try:
            with open(cache_path, "rb") as f:
                cached = pickle.load(f)
            if "cross_sectional" in cached:
                log.info("Using cached skew analytics")
                return cached
        except Exception:
            pass

    log.info("Computing skew analytics...")

    # Phase 1: Build history
    history = build_skew_history(force_rebuild=force_rebuild)
    if history.empty:
        return {"error": "No skew history available"}

    # Phase 2: Compute signals (z-scores + IV-RV)
    signals = compute_skew_signals(history)
    if signals.empty:
        return {"error": "No valid signals computed"}

    n_credit = int(signals["credit_signal"].sum()) if "credit_signal" in signals.columns else 0

    # ── Strategy A: Cross-sectional skew ranking ──
    log.info("=" * 60)
    log.info("STRATEGY A: Cross-Sectional Skew Ranking (L/S equity)")
    log.info("=" * 60)
    cs_result = run_crosssectional_backtest(signals)

    # ── Strategy B: Delta-hedged risk reversal ──
    log.info("=" * 60)
    log.info("STRATEGY B: Delta-Hedged Risk Reversal")
    log.info("=" * 60)
    rr_result = run_risk_reversal_backtest(signals, z_track_df=signals)

    # ── Strategy C: Iron condor (baseline) ──
    log.info("=" * 60)
    log.info("STRATEGY C: Iron Condor (baseline)")
    log.info("=" * 60)
    bt_input = signals[signals["credit_signal"] == 1].copy()
    ic_result = run_skew_backtest(bt_input, z_track_df=signals)

    # Charts
    charts = generate_skew_charts(ic_result, signals)

    # Assemble result
    result = {
        "cross_sectional": cs_result.get("stats", {}),
        "risk_reversal": rr_result.get("stats", {}),
        "iron_condor": ic_result.get("stats", {}),
        "stats": rr_result.get("stats", {}),  # primary for backward compat
        "charts": charts,
        "history_stats": {
            "n_total_rows": len(history),
            "n_with_signals": len(signals),
            "n_credit_signals": n_credit,
            "date_range": f"{history['date'].min()} - {history['date'].max()}",
            "tickers": int(history["root"].nunique()),
            "avg_skew_put": round(float(history["skew_put"].mean()), 4),
            "avg_iv_rv_spread": round(float(history["iv_rv_spread"].dropna().mean()), 4),
        },
    }

    with open(cache_path, "wb") as f:
        pickle.dump(result, f)
    log.info("Cached to %s", cache_path)

    return result


def _print_strategy_stats(name, stats):
    """Pretty-print stats for one strategy."""
    log.info("-" * 60)
    log.info("  %s", name)
    log.info("-" * 60)
    for k, v in stats.items():
        if k == "strategy":
            continue
        label = k.replace("_", " ").title()
        if isinstance(v, float):
            if "rate" in k or "wr" in k or "cagr" in k or "drawdown" in k or "return" in k or "mean" in k or "median" in k or "std" in k:
                log.info("  %-22s %s%%", label, v)
            elif "equity" in k or "pnl" in k or "capital" in k:
                log.info("  %-22s $%s", label, f"{v:,.0f}")
            else:
                log.info("  %-22s %s", label, v)
        else:
            log.info("  %-22s %s", label, v)


if __name__ == "__main__":
    import sys
    force = "--force" in sys.argv
    result = compute_skew_analytics(force_rebuild=force)

    if "error" in result:
        log.error("Error: %s", result["error"])
    else:
        hs = result.get("history_stats", {})
        log.info("=" * 60)
        log.info("SKEW MEAN-REVERSION — 3 STRATEGIES COMPARISON")
        log.info("=" * 60)
        log.info("History: %d rows, %d tickers, %s",
                 hs.get("n_total_rows", 0), hs.get("tickers", 0),
                 hs.get("date_range", ""))
        log.info("Credit signals: %d", hs.get("n_credit_signals", 0))

        _print_strategy_stats("A) Cross-Sectional L/S Equity", result.get("cross_sectional", {}))
        _print_strategy_stats("B) Delta-Hedged Risk Reversal", result.get("risk_reversal", {}))
        _print_strategy_stats("C) Iron Condor (baseline)", result.get("iron_condor", {}))
