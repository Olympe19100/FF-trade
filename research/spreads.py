"""
Forward Factor Research — Step 1 (optimized for daily data)
Build calendar spread universe and compute Forward Factor.

Single Calendar = Sell front ATM call + Buy back ATM call (same strike)
Double Calendar = Single Call Calendar + Single Put Calendar (same strike)
Forward Factor  = Forward_Variance / Front_Variance - 1

DTE combos: 30-60, 30-90, 60-90
"""

import sqlite3
import numpy as np
import pandas as pd
import pickle
from scipy.stats import norm
from pathlib import Path
from datetime import datetime
import warnings
warnings.filterwarnings("ignore")

ROOT  = Path(r"C:\Users\ANTEC MSI\Desktop\pro\Option trading")
DB    = ROOT / "sp500_options.db"
CACHE = ROOT / "cache"

DTE_COMBOS = [(30, 60), (30, 90), (60, 90)]
DTE_TOL    = 5
STRIKE_PCT = 0.03   # max 3% from ATM for front leg
BACK_TOL   = 0.02   # max 2% strike diff front vs back


def implied_vol_vec(prices, S, K, T, r=0.04, n_iter=8):
    """Vectorized IV via Newton-Raphson with Brenner-Subrahmanyam start."""
    prices = np.asarray(prices, dtype=float)
    S = np.asarray(S, dtype=float)
    K = np.asarray(K, dtype=float)
    T = np.asarray(T, dtype=float)

    iv = np.full_like(prices, np.nan)
    ok = (T > 0) & (S > 0) & (K > 0) & (prices > 0)
    if not ok.any():
        return iv

    p, s, k, t = prices[ok], S[ok], K[ok], T[ok]

    # Brenner-Subrahmanyam ATM approx as starting guess
    sigma = p * np.sqrt(2 * np.pi / t) / s
    sigma = np.clip(sigma, 0.02, 3.0)
    sqrt_t = np.sqrt(t)

    for _ in range(n_iter):
        d1 = (np.log(s / k) + (r + 0.5 * sigma**2) * t) / (sigma * sqrt_t)
        d2 = d1 - sigma * sqrt_t
        bs = s * norm.cdf(d1) - k * np.exp(-r * t) * norm.cdf(d2)
        vega = s * sqrt_t * norm.pdf(d1)
        sigma = sigma - (bs - p) / (vega + 1e-10)
        sigma = np.clip(sigma, 0.02, 3.0)

    iv[ok] = sigma
    return iv


def load_prices():
    with open(CACHE / "bt_prices.pkl", "rb") as f:
        prices = pickle.load(f)
    prices.index = pd.to_datetime(prices.index).date
    return prices


def build_price_date_map(prices, eod_dates):
    """Pre-compute mapping: eod_date_int -> nearest prices.index date."""
    price_dates = sorted(prices.index)
    price_ts = np.array([pd.Timestamp(d).value for d in price_dates])

    mapping = {}
    for d_int in eod_dates:
        dt = datetime.strptime(str(d_int), "%Y%m%d").date()
        dt_ts = pd.Timestamp(dt).value
        idx = np.argmin(np.abs(price_ts - dt_ts))
        gap = abs((pd.Timestamp(price_dates[idx]) - pd.Timestamp(dt)).days)
        if gap <= 3:
            mapping[d_int] = price_dates[idx]
    return mapping


def build_spreads_for_date(chain, prices_row, short_dte, long_dte):
    """Build all call calendar spreads for one date + one combo, fully vectorized."""
    # ── Front leg ──
    front = chain[(chain["dte"] >= short_dte - DTE_TOL) &
                  (chain["dte"] <= short_dte + DTE_TOL)].copy()
    if front.empty:
        return pd.DataFrame()

    # Best front expiration per root (closest to short_dte)
    front["dte_diff"] = (front["dte"] - short_dte).abs()
    best_idx = front.groupby("root")["dte_diff"].idxmin()
    best_exp = front.loc[best_idx, ["root", "expiration"]]
    front = front.merge(best_exp, on=["root", "expiration"])

    # Add underlying price
    front["S"] = front["root"].map(prices_row)
    front = front.dropna(subset=["S"])
    front = front[front["S"] > 0]
    if front.empty:
        return pd.DataFrame()

    # ATM strike per root (closest to underlying)
    front["strike_pct"] = (front["strike"] - front["S"]).abs() / front["S"]
    atm_idx = front.groupby("root")["strike_pct"].idxmin()
    front_atm = front.loc[atm_idx].copy()
    front_atm = front_atm[front_atm["strike_pct"] <= STRIKE_PCT]
    if front_atm.empty:
        return pd.DataFrame()

    # ── Back leg ──
    back = chain[(chain["dte"] >= long_dte - DTE_TOL) &
                 (chain["dte"] <= long_dte + DTE_TOL)].copy()
    if back.empty:
        return pd.DataFrame()

    # Best back expiration per root
    back["dte_diff"] = (back["dte"] - long_dte).abs()
    best_idx = back.groupby("root")["dte_diff"].idxmin()
    best_exp = back.loc[best_idx, ["root", "expiration"]]
    back = back.merge(best_exp, on=["root", "expiration"])

    # ── Match front ATM strike → closest back strike ──
    m = front_atm[["root", "strike", "mid", "dte", "expiration", "S"]].merge(
        back[["root", "strike", "mid", "dte", "expiration"]],
        on="root", suffixes=("_f", "_b")
    )
    if m.empty:
        return pd.DataFrame()

    m["sdiff"] = (m["strike_b"] - m["strike_f"]).abs()
    m["sdiff_pct"] = m["sdiff"] / m["strike_f"]
    best_back = m.groupby("root")["sdiff"].idxmin()
    sp = m.loc[best_back].copy()
    sp = sp[sp["sdiff_pct"] <= BACK_TOL]
    if sp.empty:
        return pd.DataFrame()

    # ── Vectorized IV ──
    T_f = sp["dte_f"].values / 365.0
    T_b = sp["dte_b"].values / 365.0
    S = sp["S"].values

    iv_f = implied_vol_vec(sp["mid_f"].values, S, sp["strike_f"].values, T_f)
    iv_b = implied_vol_vec(sp["mid_b"].values, S, sp["strike_b"].values, T_b)

    spread_cost = sp["mid_b"].values - sp["mid_f"].values

    # Forward Factor
    var_f = iv_f ** 2
    var_b = iv_b ** 2
    dT = T_b - T_f
    fwd_var = (var_b * T_b - var_f * T_f) / dT
    ff = fwd_var / var_f - 1

    result = pd.DataFrame({
        "root": sp["root"].values,
        "front_exp": sp["expiration_f"].values,
        "front_strike": sp["strike_f"].values,
        "front_mid": sp["mid_f"].values,
        "front_dte": sp["dte_f"].values,
        "front_iv": iv_f,
        "back_exp": sp["expiration_b"].values.astype(int),
        "back_strike": sp["strike_b"].values,
        "back_mid": sp["mid_b"].values,
        "back_dte": sp["dte_b"].values.astype(int),
        "back_iv": iv_b,
        "spread_cost": spread_cost,
        "ff": ff,
        "underlying_price": S,
    })

    valid = (
        (result["front_iv"] > 0.02) & (result["front_iv"] < 3) &
        (result["back_iv"] > 0.02) & (result["back_iv"] < 3) &
        (result["spread_cost"] > 0) &
        np.isfinite(result["ff"])
    )
    return result[valid]


def build_universe():
    prices = load_prices()
    print(f"Prices: {prices.shape[0]} days, {prices.shape[1]} tickers")

    conn = sqlite3.connect(str(DB))
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT date FROM eod_history ORDER BY date")
    eod_dates = [r[0] for r in cur.fetchall()]
    print(f"EOD dates: {len(eod_dates)}")

    date_map = build_price_date_map(prices, eod_dates)
    print(f"Dates with stock prices: {len(date_map)}")

    # Query both calls and puts
    query = """
        SELECT c.root, c.expiration, c.strike / 1000.0 AS strike,
               c.right, e.bid, e.ask, e.close
        FROM eod_history e
        JOIN contracts c ON e.contract_id = c.contract_id
        WHERE e.date = ? AND c.right IN ('C', 'P')
    """

    all_dfs = []
    for i, d_int in enumerate(eod_dates):
        if d_int not in date_map:
            continue
        price_date = date_map[d_int]
        prices_row = prices.loc[price_date].dropna()
        prices_row = prices_row[prices_row > 0]

        chain_all = pd.read_sql_query(query, conn, params=(d_int,))
        if chain_all.empty:
            continue

        # Mid price
        chain_all["mid"] = np.where(
            (chain_all["bid"] > 0) & (chain_all["ask"] > 0),
            (chain_all["bid"] + chain_all["ask"]) / 2,
            chain_all["close"]
        )
        chain_all = chain_all[chain_all["mid"] > 0.01]

        # DTE
        obs_date = datetime.strptime(str(d_int), "%Y%m%d").date()
        exp_dates = pd.to_datetime(chain_all["expiration"].astype(str), format="%Y%m%d")
        chain_all["dte"] = (exp_dates - pd.Timestamp(obs_date)).dt.days
        chain_all = chain_all[chain_all["dte"] > 0]

        # Split calls / puts
        calls = chain_all[chain_all["right"] == "C"]
        puts = chain_all[chain_all["right"] == "P"]

        # Build put lookup: (root, exp_int, strike_x1000) -> put_mid
        put_lookup = dict(zip(
            zip(puts["root"],
                puts["expiration"].astype(int),
                (puts["strike"] * 1000).round().astype(int)),
            puts["mid"]
        ))

        n_total = 0
        for short_dte, long_dte in DTE_COMBOS:
            df_sp = build_spreads_for_date(calls, prices_row, short_dte, long_dte)
            if df_sp.empty:
                continue

            # Look up matching puts for double calendar
            fk = list(zip(df_sp["root"],
                          df_sp["front_exp"].astype(int),
                          (df_sp["front_strike"] * 1000).round().astype(int)))
            bk = list(zip(df_sp["root"],
                          df_sp["back_exp"].astype(int),
                          (df_sp["back_strike"] * 1000).round().astype(int)))

            df_sp["put_front_mid"] = [put_lookup.get(k, np.nan) for k in fk]
            df_sp["put_back_mid"] = [put_lookup.get(k, np.nan) for k in bk]
            df_sp["put_spread_cost"] = df_sp["put_back_mid"] - df_sp["put_front_mid"]

            has_puts = df_sp["put_front_mid"].notna() & df_sp["put_back_mid"].notna()
            df_sp["combined_cost"] = np.where(
                has_puts,
                df_sp["spread_cost"] + df_sp["put_spread_cost"],
                np.nan
            )
            df_sp["spread_type"] = np.where(has_puts, "double", "single")

            df_sp["obs_date"] = d_int
            df_sp["combo"] = f"{short_dte}-{long_dte}"
            all_dfs.append(df_sp)
            n_total += len(df_sp)

        if (i + 1) % 100 == 0 or i == 0:
            total = sum(len(x) for x in all_dfs)
            print(f"[{i+1}/{len(eod_dates)}] {d_int}: {n_total} spreads (total={total:,})")

    conn.close()

    if not all_dfs:
        print("No spreads found!")
        return pd.DataFrame()

    df = pd.concat(all_dfs, ignore_index=True)

    n_double = (df["spread_type"] == "double").sum()
    n_single = (df["spread_type"] == "single").sum()
    print(f"\n{'='*60}")
    print(f"Total spreads: {len(df):,}")
    print(f"  Double calendar (call+put): {n_double:,} ({n_double/len(df)*100:.1f}%)")
    print(f"  Single (call-only, no put): {n_single:,} ({n_single/len(df)*100:.1f}%)")

    for combo in sorted(df["combo"].unique()):
        sub = df[df["combo"] == combo]
        dbl = sub[sub["spread_type"] == "double"]
        print(f"  {combo}: {len(sub):,} total, {len(dbl):,} double, "
              f"FF mean={sub['ff'].mean():.4f}")

    out = CACHE / "calendar_spreads.pkl"
    df.to_pickle(str(out))
    print(f"\nSaved to {out}")
    return df


if __name__ == "__main__":
    print("=" * 60)
    print("BUILDING CALENDAR SPREAD UNIVERSE (daily, call+put)")
    print("=" * 60)
    build_universe()
