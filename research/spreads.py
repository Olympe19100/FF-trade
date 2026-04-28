"""
Forward Factor Research — Step 1 (optimized for daily data)
Build calendar spread universe and compute Forward Factor.

Single Calendar = Sell front 35Δ call + Buy back 35Δ call (same strike)
Double Calendar = Call calendar (35Δ call) + Put calendar (35Δ put)
  Call strike: where call delta ≈ 0.35 (OTM, above S)
  Put  strike: where |put delta| ≈ 0.35 (OTM, below S)
Forward Factor  = (Front_IV - Forward_IV) / Forward_IV  (Campasano/PDF)

DTE pairs: dynamic discovery from available expirations
  Front: [15, 75], Back: [40, 120], Gap >= 20 days
"""

import sqlite3
import numpy as np
import pandas as pd
import pickle
from pathlib import Path
from datetime import datetime
import warnings
warnings.filterwarnings("ignore")

from core.config import (
    ROOT, DB, CACHE,
    FRONT_DTE_MIN, FRONT_DTE_MAX, BACK_DTE_MIN, BACK_DTE_MAX, MIN_DTE_GAP,
)
from core.pricing import implied_vol_vec, bs_delta_vec, RISK_FREE_RATE
STRIKE_PCT = 0.10   # max 10% from ATM for front leg (wider for 35-delta OTM)
BACK_TOL   = 0.02   # max 2% strike diff front vs back
TARGET_DELTA = 0.35  # target call delta (paper uses +35Δ calls, -35Δ puts)


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


def build_spreads_for_date(chain, prices_row, front_exp_int, back_exp_int,
                           target_delta=TARGET_DELTA):
    """Build all call calendar spreads for one date + one exact expiration pair.

    front_exp_int / back_exp_int are exact expiration dates (int, YYYYMMDD).
    Selects strikes where call delta ≈ target_delta (OTM, above S).
    """
    # ── Front leg — exact expiration match ──
    front = chain[chain["expiration"] == front_exp_int].copy()
    if front.empty:
        return pd.DataFrame()

    # Add underlying price
    front["S"] = front["root"].map(prices_row)
    front = front.dropna(subset=["S"])
    front = front[front["S"] > 0]
    if front.empty:
        return pd.DataFrame()

    # Filter to strikes within STRIKE_PCT of ATM
    front["strike_pct"] = (front["strike"] - front["S"]).abs() / front["S"]
    front = front[front["strike_pct"] <= STRIKE_PCT].copy()
    if front.empty:
        return pd.DataFrame()

    # Compute IV for all front options, then compute call delta
    T_front = front["dte"].values / 365.0
    iv_all = implied_vol_vec(front["mid"].values, front["S"].values,
                             front["strike"].values, T_front)
    front["iv_est"] = iv_all
    front = front[front["iv_est"] > 0.02].copy()
    if front.empty:
        return pd.DataFrame()

    front["call_delta"] = bs_delta_vec(front["S"].values, front["strike"].values,
                                       front["dte"].values / 365.0,
                                       front["iv_est"].values)
    front = front.dropna(subset=["call_delta"])
    if front.empty:
        return pd.DataFrame()

    # Select strike closest to target_delta per root
    front["delta_diff"] = (front["call_delta"] - target_delta).abs()
    delta_idx = front.groupby("root")["delta_diff"].idxmin()
    front_sel = front.loc[delta_idx].copy()
    if front_sel.empty:
        return pd.DataFrame()

    # ── Back leg — exact expiration match ──
    back = chain[chain["expiration"] == back_exp_int].copy()
    if back.empty:
        return pd.DataFrame()

    # ── Match front selected strike → closest back strike ──
    m = front_sel[["root", "strike", "mid", "bid", "ask", "dte", "expiration", "S", "call_delta"]].merge(
        back[["root", "strike", "mid", "bid", "ask", "dte", "expiration"]],
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
    call_ba = (sp["ask_f"].values - sp["bid_f"].values) + (sp["ask_b"].values - sp["bid_b"].values)

    # Forward Factor (PDF/Campasano: (front_iv - fwd_iv) / fwd_iv)
    var_f = iv_f ** 2
    var_b = iv_b ** 2
    dT = T_b - T_f
    fwd_var = (var_b * T_b - var_f * T_f) / dT
    fwd_iv = np.where(fwd_var > 0, np.sqrt(fwd_var), np.nan)
    ff = np.where(fwd_iv > 0, (iv_f - fwd_iv) / fwd_iv, np.nan)

    result = pd.DataFrame({
        "root": sp["root"].values,
        "front_exp": sp["expiration_f"].values,
        "front_strike": sp["strike_f"].values,
        "front_mid": sp["mid_f"].values,
        "front_dte": sp["dte_f"].values,
        "front_iv": iv_f,
        "front_delta": sp["call_delta"].values,
        "back_exp": sp["expiration_b"].values.astype(int),
        "back_strike": sp["strike_b"].values,
        "back_mid": sp["mid_b"].values,
        "back_dte": sp["dte_b"].values.astype(int),
        "back_iv": iv_b,
        "spread_cost": spread_cost,
        "call_ba": call_ba,
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
        put_ba_lookup = dict(zip(
            zip(puts["root"],
                puts["expiration"].astype(int),
                (puts["strike"] * 1000).round().astype(int)),
            puts["ask"] - puts["bid"]
        ))

        # ── Dynamic DTE pair discovery from available expirations ──
        unique_exps = calls[["expiration", "dte"]].drop_duplicates("expiration").sort_values("dte")
        front_exps = unique_exps[
            (unique_exps["dte"] >= FRONT_DTE_MIN) & (unique_exps["dte"] <= FRONT_DTE_MAX)
        ]
        back_exps = unique_exps[
            (unique_exps["dte"] >= BACK_DTE_MIN) & (unique_exps["dte"] <= BACK_DTE_MAX)
        ]

        # Enumerate valid pairs
        pairs = []
        for _, fr in front_exps.iterrows():
            for _, bk in back_exps.iterrows():
                if bk["expiration"] == fr["expiration"]:
                    continue
                if bk["dte"] - fr["dte"] >= MIN_DTE_GAP:
                    pairs.append((int(fr["expiration"]), int(bk["expiration"]),
                                  int(fr["dte"]), int(bk["dte"])))

        n_total = 0
        puts_c = puts.copy()
        puts_c["exp_int"] = puts_c["expiration"].astype(int)

        for front_exp_int, back_exp_int, front_dte_val, back_dte_val in pairs:
            df_sp = build_spreads_for_date(calls, prices_row, front_exp_int, back_exp_int)
            if df_sp.empty:
                continue

            # ── Select 35-delta put strike per root (vectorized) ──
            ref = df_sp.drop_duplicates("root")[["root", "underlying_price", "front_exp", "back_exp"]].copy()
            ref["front_exp_int"] = ref["front_exp"].astype(int)
            ref["back_exp_int"] = ref["back_exp"].astype(int)

            rp = ref.merge(puts_c[["root", "exp_int", "strike", "mid", "dte"]],
                           left_on=["root", "front_exp_int"],
                           right_on=["root", "exp_int"], how="inner")
            if not rp.empty:
                rp["strike_pct"] = (rp["strike"] - rp["underlying_price"]).abs() / rp["underlying_price"]
                rp = rp[rp["strike_pct"] <= STRIKE_PCT].copy()

            if not rp.empty:
                T_p = rp["dte"].values / 365.0
                S_p = rp["underlying_price"].values
                call_equiv = rp["mid"].values + S_p - rp["strike"].values * np.exp(-RISK_FREE_RATE * T_p)
                call_equiv = np.maximum(call_equiv, 0.001)
                iv_p = implied_vol_vec(call_equiv, S_p, rp["strike"].values, T_p)

                cd = bs_delta_vec(S_p, rp["strike"].values, T_p, iv_p)
                rp["put_delta_abs"] = 1.0 - cd
                rp = rp[np.isfinite(rp["put_delta_abs"])].copy()

            if not rp.empty:
                rp["delta_diff"] = (rp["put_delta_abs"] - TARGET_DELTA).abs()
                best_put_idx = rp.groupby("root")["delta_diff"].idxmin()
                best_puts = rp.loc[best_put_idx, ["root", "strike"]].set_index("root")["strike"]
                df_sp["put_strike"] = df_sp["root"].map(best_puts)
            else:
                df_sp["put_strike"] = np.nan

            # ── Match back put strike (vectorized) ──
            has_ps = df_sp["put_strike"].notna()
            if has_ps.any():
                ps_ref = df_sp.loc[has_ps].drop_duplicates("root")[["root", "put_strike", "back_exp"]].copy()
                ps_ref["back_exp_int"] = ps_ref["back_exp"].astype(int)
                rb = ps_ref.merge(puts_c[["root", "exp_int", "strike"]],
                                  left_on=["root", "back_exp_int"],
                                  right_on=["root", "exp_int"], how="inner")
                if not rb.empty:
                    rb["sdiff"] = (rb["strike"] - rb["put_strike"]).abs()
                    rb["sdiff_pct"] = rb["sdiff"] / rb["put_strike"]
                    best_back_idx = rb.groupby("root")["sdiff"].idxmin()
                    rb_best = rb.loc[best_back_idx].copy()
                    rb_best = rb_best[rb_best["sdiff_pct"] <= BACK_TOL]
                    pbs_map = rb_best.set_index("root")["strike"]
                    df_sp["put_back_strike"] = df_sp["root"].map(pbs_map)
                else:
                    df_sp["put_back_strike"] = np.nan
            else:
                df_sp["put_back_strike"] = np.nan

            # Look up put mids/ba at the selected put strikes
            def _put_key(root, exp_int, strike):
                return (root, int(exp_int), int(round(strike * 1000)))

            df_sp["put_front_mid"] = [
                put_lookup.get(_put_key(r, e, s), np.nan)
                for r, e, s in zip(df_sp["root"], df_sp["front_exp"].astype(int),
                                   df_sp["put_strike"].fillna(0))
            ]
            df_sp["put_back_mid"] = [
                put_lookup.get(_put_key(r, e, s), np.nan)
                for r, e, s in zip(df_sp["root"], df_sp["back_exp"].astype(int),
                                   df_sp["put_back_strike"].fillna(0))
            ]
            put_front_ba = pd.Series([
                put_ba_lookup.get(_put_key(r, e, s), np.nan)
                for r, e, s in zip(df_sp["root"], df_sp["front_exp"].astype(int),
                                   df_sp["put_strike"].fillna(0))
            ], index=df_sp.index)
            put_back_ba = pd.Series([
                put_ba_lookup.get(_put_key(r, e, s), np.nan)
                for r, e, s in zip(df_sp["root"], df_sp["back_exp"].astype(int),
                                   df_sp["put_back_strike"].fillna(0))
            ], index=df_sp.index)
            df_sp["put_spread_cost"] = df_sp["put_back_mid"] - df_sp["put_front_mid"]

            has_puts = (df_sp["put_front_mid"].notna() & df_sp["put_back_mid"].notna()
                        & df_sp["put_strike"].notna() & df_sp["put_back_strike"].notna())
            df_sp["combined_cost"] = np.where(
                has_puts,
                df_sp["spread_cost"] + df_sp["put_spread_cost"],
                np.nan
            )
            df_sp["spread_type"] = np.where(has_puts, "double", "single")

            put_ba = put_front_ba.fillna(0) + put_back_ba.fillna(0)
            df_sp["ba_pct"] = np.where(
                has_puts & (df_sp["combined_cost"] > 0),
                (df_sp["call_ba"] + put_ba) / df_sp["combined_cost"],
                np.where(df_sp["spread_cost"] > 0,
                         df_sp["call_ba"] / df_sp["spread_cost"], np.nan)
            )

            df_sp["obs_date"] = d_int
            df_sp["combo"] = f"{front_dte_val}-{back_dte_val}"
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
