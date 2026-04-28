"""
Regime analysis: performance vs market conditions
Check if strategy decay is real or just regime-dependent.
"""
import sys
import numpy as np
import pandas as pd
import pickle
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from core.config import CACHE

# Load prices
with open(CACHE / "bt_prices.pkl", "rb") as f:
    prices = pickle.load(f)
prices.index = pd.to_datetime(prices.index)

# SPY as market proxy
if "SPY" in prices.columns:
    spy = prices["SPY"].dropna()
elif "SPXL" in prices.columns:
    spy = prices["SPXL"].dropna()
else:
    # Use equal-weight average
    spy = prices.mean(axis=1).dropna()

spy_ret = spy.pct_change().dropna()

# Load spread returns (FF in OLD formula, aligned with PDF)
df = pd.read_pickle(str(CACHE / "spread_returns.pkl"))
df = df[np.isfinite(df["ff"])].copy()
df["entry_dt"] = pd.to_datetime(df["obs_date"].astype(str), format="%Y%m%d")
df["exit_dt"] = pd.to_datetime(df["front_exp"].astype(str), format="%Y%m%d")

# FF thresholds in OLD formula (from PDF)
FF_THRESHOLD_OLD = {"30-60": 0.230, "30-90": 0.230, "60-90": 0.200}

print("=" * 70)
print("REGIME ANALYSIS")
print("=" * 70)

# ── 1. Market regime per year ──
print("\n--- MARKET REGIME (SPY) ---")
for year in [2022, 2023, 2024, 2025, 2026]:
    mask = spy_ret.index.year == year
    yr = spy_ret[mask]
    if len(yr) == 0:
        continue
    spy_yr = spy[spy.index.year == year]
    ytd_ret = (spy_yr.iloc[-1] / spy_yr.iloc[0] - 1) * 100
    vol_ann = yr.std() * np.sqrt(252) * 100
    print(f"  {year}: SPY return={ytd_ret:+.1f}%, "
          f"vol={vol_ann:.1f}%, "
          f"n_days={len(yr)}")

# ── 2. VIX proxy: rolling 20d realized vol of SPY ──
print("\n--- REALIZED VOL (20d rolling, annualized) ---")
rvol = spy_ret.rolling(20).std() * np.sqrt(252) * 100
for year in [2022, 2023, 2024, 2025, 2026]:
    mask = rvol.index.year == year
    yr = rvol[mask].dropna()
    if len(yr) == 0:
        continue
    print(f"  {year}: mean={yr.mean():.1f}%, "
          f"max={yr.max():.1f}%, min={yr.min():.1f}%")

# ── 3. Strategy by quarter ──
print("\n--- STRATEGY BY QUARTER (30-90 call, cost>=$1, FF>=threshold) ---")
sub = df[(df["combo"] == "30-90") & (df["spread_cost"] >= 1.0) & (df["ff"] >= FF_THRESHOLD_OLD["30-90"])].copy()
sub["quarter"] = sub["entry_dt"].dt.to_period("Q")
sub["ret_capped"] = sub["ret"].clip(upper=1.0)
sub["ret_eff"] = sub["ret_capped"] * 0.9 + 0.9 - 1  # after haircut

# Add SPY rvol at entry
sub["entry_rvol"] = sub["entry_dt"].map(
    lambda d: rvol.asof(d) if d in rvol.index or True else np.nan)

print(f"\n{'Qtr':>8s}  {'N':>5s}  {'eff_ret':>8s}  {'WR':>5s}  {'FF_med':>7s}  {'SPY_rvol':>9s}")
for q, grp in sub.groupby("quarter"):
    rvol_med = grp["entry_rvol"].median()
    print(f"  {q}  {len(grp):5d}  {grp['ret_eff'].mean():8.3f}  "
          f"{(grp['ret_eff'] > 0).mean():5.1%}  {grp['ff'].median():7.2f}  "
          f"{rvol_med:8.1f}%")

# ── 4. Correlation: quarterly returns vs SPY vol ──
print("\n--- CORRELATION: strategy return vs market vol ---")
q_stats = sub.groupby("quarter").agg(
    ret_mean=("ret_eff", "mean"),
    rvol_mean=("entry_rvol", "mean"),
    ff_mean=("ff", "mean"),
    n=("ret_eff", "count")
).dropna()

if len(q_stats) > 3:
    corr_vol = q_stats["ret_mean"].corr(q_stats["rvol_mean"])
    corr_ff = q_stats["ret_mean"].corr(q_stats["ff_mean"])
    print(f"  ret vs rvol:  rho={corr_vol:.3f} "
          f"({'strategy likes vol' if corr_vol > 0 else 'strategy dislikes vol'})")
    print(f"  ret vs FF:    rho={corr_ff:.3f}")

# ── 5. Data coverage check: 2025-2026 ──
print("\n--- DATA COVERAGE CHECK ---")
for year in [2022, 2023, 2024, 2025, 2026]:
    mask = df["entry_dt"].dt.year == year
    yr = df[mask]
    n_dates = yr["entry_dt"].nunique()
    n_tickers = yr["root"].nunique()
    n_total = len(yr)
    print(f"  {year}: {n_total:,} trades, {n_dates} dates, "
          f"{n_tickers} tickers, "
          f"avg {n_total/max(1,n_dates):.0f} trades/day")

# ── 6. FF distribution over time ──
print("\n--- FF DISTRIBUTION BY YEAR ---")
for year in [2022, 2023, 2024, 2025, 2026]:
    mask = (df["entry_dt"].dt.year == year) & (df["spread_cost"] >= 1.0)
    yr = df[mask]
    if len(yr) == 0:
        continue
    pct_above = {}
    for combo_name, thresh in FF_THRESHOLD_OLD.items():
        cm = yr[yr["combo"] == combo_name] if "combo" in yr.columns else yr
        pct_above[combo_name] = (cm["ff"] >= thresh).mean() * 100 if len(cm) > 0 else 0
    print(f"  {year}: FF mean={yr['ff'].mean():.3f}, "
          f"med={yr['ff'].median():.3f}, "
          f"pct>=thresh: {', '.join(f'{k}:{v:.0f}%' for k,v in pct_above.items())}, "
          f"n={len(yr):,}")

# ── 7. Spread cost by year (data quality) ──
print("\n--- SPREAD COST BY YEAR ---")
for combo in ["30-60", "30-90", "60-90"]:
    print(f"\n  {combo}:")
    ff_thresh = FF_THRESHOLD_OLD.get(combo, 0.230)
    c = df[(df["combo"] == combo) & (df["spread_cost"] >= 1.0) & (df["ff"] >= ff_thresh)]
    for year in [2022, 2023, 2024, 2025, 2026]:
        yr = c[c["entry_dt"].dt.year == year]
        if len(yr) == 0:
            continue
        print(f"    {year}: n={len(yr):5d}, "
              f"cost med=${yr['spread_cost'].median():.2f}, "
              f"ret med={yr['ret'].median():.2f}, "
              f"ret>100%: {(yr['ret'] > 1.0).mean()*100:.0f}%")

# ── 8. Exclude 2026 partial year: what's the real performance? ──
print("\n--- PERFORMANCE EXCLUDING 2026 (partial year) ---")
for combo in ["30-60", "30-90", "60-90"]:
    ff_thresh = FF_THRESHOLD_OLD.get(combo, 0.230)
    c = df[(df["combo"] == combo) & (df["spread_cost"] >= 1.0) & (df["ff"] >= ff_thresh)]
    full = c[c["entry_dt"].dt.year <= 2025]
    r = full["ret"].clip(upper=1.0)
    eff = r * 0.9 + 0.9 - 1
    print(f"  {combo}: n={len(full):,}, eff_ret mean={eff.mean():.3f}, "
          f"wr={(eff > 0).mean():.1%}")

print("\n" + "=" * 70)
print("REGIME ANALYSIS COMPLETE")
print("=" * 70)
