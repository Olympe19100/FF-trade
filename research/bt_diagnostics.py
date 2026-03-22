"""
Backtest Artifact Diagnostics
Check for biases and artifacts in the backtest.
"""
import numpy as np
import pandas as pd
import pickle
from pathlib import Path
from collections import Counter

ROOT  = Path(r"C:\Users\ANTEC MSI\Desktop\pro\Option trading")
CACHE = ROOT / "cache"

# ── Load data (aligned with PDF: FF in OLD formula) ──
df = pd.read_pickle(str(CACHE / "spread_returns.pkl"))
df = df[np.isfinite(df["ff"])].copy()
df["entry_dt"] = pd.to_datetime(df["obs_date"].astype(str), format="%Y%m%d")
df["exit_dt"] = pd.to_datetime(df["front_exp"].astype(str), format="%Y%m%d")

MIN_SPREAD_COST = 1.00
# FF thresholds in OLD formula (fwd_var/front_var - 1), from PDF filtered model
FF_THRESHOLD_OLD = {"30-60": 0.230, "30-90": 0.230, "60-90": 0.200}

print("=" * 70)
print("BACKTEST ARTIFACT DIAGNOSTICS")
print("=" * 70)

# ── 1. LOOK-AHEAD BIAS: Kelly on full dataset ──
print("\n--- 1. LOOK-AHEAD BIAS (Kelly) ---")
for combo in ["30-60", "30-90", "60-90"]:
    ff_thresh = FF_THRESHOLD_OLD.get(combo, 0.230)
    sub = df[(df["combo"] == combo) & (df["spread_cost"] >= MIN_SPREAD_COST)
             & (df["ff"] >= ff_thresh)].copy()
    if len(sub) == 0:
        continue

    # Walk-forward Kelly: first half vs second half vs full
    mid = sub["entry_dt"].quantile(0.5)
    first_half = sub[sub["entry_dt"] <= mid]
    second_half = sub[sub["entry_dt"] > mid]

    for name, s in [("Full", sub), ("1st half", first_half), ("2nd half", second_half)]:
        capped = s["ret"].clip(upper=1.0)
        eff_ret = capped * 0.9 + 0.9 - 1  # after 10% haircut
        mu = eff_ret.mean()
        var = eff_ret.var()
        k = 0.5 * mu / var if var > 0 and mu > 0 else 0
        wr = (eff_ret > 0).mean()
        print(f"  {combo} {name:10s}: n={len(s):5d}, mu={mu:.3f}, "
              f"std={eff_ret.std():.3f}, kelly={k:.3f}, wr={wr:.1%}")

# ── 2. RETURN DISTRIBUTION: are returns plausible? ──
print("\n--- 2. RETURN DISTRIBUTION (all trades, ret col) ---")
for combo in ["30-60", "30-90", "60-90"]:
    ff_thresh = FF_THRESHOLD_OLD.get(combo, 0.230)
    sub = df[(df["combo"] == combo) & (df["spread_cost"] >= MIN_SPREAD_COST)
             & (df["ff"] >= ff_thresh)]
    if len(sub) == 0:
        continue
    r = sub["ret"]
    cost = sub["spread_cost"]
    print(f"\n  {combo}: {len(sub):,} trades")
    print(f"    ret:  mean={r.mean():.2f}, med={r.median():.2f}, "
          f"p5={r.quantile(0.05):.2f}, p25={r.quantile(0.25):.2f}, "
          f"p75={r.quantile(0.75):.2f}, p95={r.quantile(0.95):.2f}")
    print(f"    cost: mean=${cost.mean():.2f}, med=${cost.median():.2f}, "
          f"min=${cost.min():.2f}, max=${cost.max():.2f}")
    # Return by cost bucket
    for lo, hi in [(1.0, 2.0), (2.0, 5.0), (5.0, 10.0), (10.0, 999)]:
        mask = (cost >= lo) & (cost < hi)
        if mask.sum() > 10:
            print(f"    cost ${lo:.0f}-${hi:.0f}: n={mask.sum():,}, "
                  f"ret mean={r[mask].mean():.2f}, med={r[mask].median():.2f}, "
                  f"wr={(r[mask] > 0).mean():.1%}")

# ── 3. SURVIVORSHIP BIAS ──
print("\n--- 3. SURVIVORSHIP / SELECTION BIAS ---")
ff_mask = pd.Series(False, index=df.index)
for combo_name, thresh in FF_THRESHOLD_OLD.items():
    ff_mask |= (df["combo"] == combo_name) & (df["ff"] >= thresh)
sub = df[(df["spread_cost"] >= MIN_SPREAD_COST) & ff_mask]
n_tickers = sub["root"].nunique()
total_tickers = df["root"].nunique()
print(f"  Tickers qualifying: {n_tickers} / {total_tickers}")
# Check if winning tickers dominate
ticker_stats = sub.groupby("root").agg(
    n=("ret", "count"),
    mean_ret=("ret", "mean"),
    win_rate=("ret", lambda x: (x > 0).mean())
).sort_values("n", ascending=False)
print(f"  Top 10 tickers by count:")
for _, row in ticker_stats.head(10).iterrows():
    print(f"    {row.name:6s}: {row['n']:4.0f} trades, "
          f"ret={row['mean_ret']:.2f}, wr={row['win_rate']:.1%}")
# Check if specific tickers drive all profit
top20 = ticker_stats.head(20).index.tolist()
in_top20 = sub[sub["root"].isin(top20)]
rest = sub[~sub["root"].isin(top20)]
print(f"  Top 20 tickers: {len(in_top20):,} trades, mean ret={in_top20['ret'].mean():.2f}")
print(f"  Remaining:      {len(rest):,} trades, mean ret={rest['ret'].mean():.2f}")

# ── 4. DAILY DATE COVERAGE ──
print("\n--- 4. DATE COVERAGE (Sharpe computation artifact) ---")
for combo in ["30-60", "30-90", "60-90"]:
    ff_thresh = FF_THRESHOLD_OLD.get(combo, 0.230)
    sub = df[(df["combo"] == combo) & (df["spread_cost"] >= MIN_SPREAD_COST)
             & (df["ff"] >= ff_thresh)]
    entry_dates = sorted(sub["entry_dt"].unique())
    exit_dates = sorted(sub["exit_dt"].unique())
    all_dates = sorted(set(entry_dates) | set(exit_dates))

    d0, d1 = all_dates[0], all_dates[-1]
    cal_days = (d1 - d0).days
    biz_days = np.busday_count(d0.date(), d1.date())

    print(f"  {combo}: {len(all_dates)} unique dates / {biz_days} business days "
          f"({len(all_dates)/biz_days*100:.0f}% coverage)")

    # Check gaps
    gaps = [(all_dates[i+1] - all_dates[i]).days
            for i in range(len(all_dates)-1)]
    print(f"    Gap days: mean={np.mean(gaps):.1f}, max={np.max(gaps)}, "
          f"median={np.median(gaps):.0f}")

# ── 5. DUPLICATE TRADES (same ticker, same day, same combo) ──
print("\n--- 5. DUPLICATE / CORRELATED ENTRIES ---")
ff_mask = pd.Series(False, index=df.index)
for combo_name, thresh in FF_THRESHOLD_OLD.items():
    ff_mask |= (df["combo"] == combo_name) & (df["ff"] >= thresh)
sub = df[(df["spread_cost"] >= MIN_SPREAD_COST) & ff_mask]
dupes = sub.groupby(["obs_date", "root", "combo"]).size()
multi = dupes[dupes > 1]
print(f"  Same (date, ticker, combo) with multiple entries: {len(multi):,}")
if len(multi) > 0:
    print(f"    Max entries for same (date,ticker,combo): {multi.max()}")
    # Check if we could accidentally open multiple positions in same ticker
    same_day_ticker = sub.groupby(["obs_date", "root"]).size()
    multi_ticker = same_day_ticker[same_day_ticker > 1]
    print(f"  Same (date, ticker) across combos: {len(multi_ticker):,}")

# ── 6. SLIPPAGE DOUBLE-COUNT BUG ──
print("\n--- 6. SLIPPAGE ACCOUNTING CHECK ---")
# Trace cash flow for a single hypothetical trade
cost = 2.00  # spread cost per share
ret = 0.50   # 50% return
contracts = 5
SLIP_E = 0.06   # 2-leg entry: 2 x $0.03 (Muravyev & Pearson 2020)
SLIP_X = 0.06   # 2-leg exit:  2 x $0.03
HAIRCUT = 0.10
MULT = 100

# At entry
cash_before = 100000
cash_deducted = contracts * (cost + SLIP_E) * MULT  # line 188
cash_after_entry = cash_before - cash_deducted

# At exit
capped_ret = min(ret, 1.0)
raw_exit = cost * (1 + capped_ret)
exit_val = raw_exit * (1 - HAIRCUT)
pnl_per_share = exit_val - cost - (SLIP_E + SLIP_X)  # line 126-127
pnl = pnl_per_share * contracts * MULT
cash_returned = contracts * cost * MULT + pnl  # line 129

net_pnl = (cash_after_entry + cash_returned) - cash_before
correct_pnl = (exit_val - cost - SLIP_X) * contracts * MULT - SLIP_E * contracts * MULT
# Correct: total cost = (cost + SLIP_E), proceeds = (exit_val - SLIP_X)
correct_pnl2 = ((exit_val - SLIP_X) - (cost + SLIP_E)) * contracts * MULT

print(f"  Example: cost={cost}, ret={ret}, contracts={contracts}")
print(f"  Cash deducted at entry:  ${cash_deducted:,.0f}")
print(f"  Exit value/share:        ${exit_val:.4f}")
print(f"  PnL per share:           ${pnl_per_share:.4f}")
print(f"  Cash returned at exit:   ${cash_returned:,.0f}")
print(f"  Net P&L (code):          ${net_pnl:,.0f}")
print(f"  Correct P&L:             ${correct_pnl2:,.0f}")
print(f"  Difference (bug):        ${net_pnl - correct_pnl2:,.0f}")
if abs(net_pnl - correct_pnl2) > 0.01:
    print(f"  ** SLIPPAGE DOUBLE-COUNT: entry slippage deducted twice!")
    print(f"     Impact per trade: ${abs(net_pnl - correct_pnl2)/contracts:.2f}/contract")

# ── 7. HOLDING PERIOD vs ASSUMED DTE ──
print("\n--- 7. HOLDING PERIOD CHECK ---")
for combo in ["30-60", "30-90", "60-90"]:
    ff_thresh = FF_THRESHOLD_OLD.get(combo, 0.230)
    sub = df[(df["combo"] == combo) & (df["spread_cost"] >= MIN_SPREAD_COST)
             & (df["ff"] >= ff_thresh)]
    holding = (sub["exit_dt"] - sub["entry_dt"]).dt.days
    print(f"  {combo}: holding days mean={holding.mean():.0f}, "
          f"med={holding.median():.0f}, min={holding.min()}, max={holding.max()}")

# ── 8. RETURNS BY YEAR (regime stability) ──
print("\n--- 8. RETURNS BY YEAR (regime stability) ---")
ff_mask = pd.Series(False, index=df.index)
for combo_name, thresh in FF_THRESHOLD_OLD.items():
    ff_mask |= (df["combo"] == combo_name) & (df["ff"] >= thresh)
sub = df[(df["spread_cost"] >= MIN_SPREAD_COST) & ff_mask]
sub["year"] = sub["entry_dt"].dt.year
for combo in ["30-60", "30-90", "60-90"]:
    c = sub[sub["combo"] == combo]
    print(f"\n  {combo}:")
    for year, grp in c.groupby("year"):
        r = grp["ret"].clip(upper=1.0)
        eff = r * 0.9 + 0.9 - 1
        print(f"    {year}: n={len(grp):4d}, eff_ret mean={eff.mean():.3f}, "
              f"wr={(eff > 0).mean():.1%}")

# ── 9. FF MONOTONICITY CHECK ──
print("\n--- 9. FF -> RETURN MONOTONICITY ---")
for combo in ["30-60", "30-90", "60-90"]:
    ff_thresh = FF_THRESHOLD_OLD.get(combo, 0.230)
    sub = df[(df["combo"] == combo) & (df["spread_cost"] >= MIN_SPREAD_COST)
             & (df["ff"] >= ff_thresh)]
    if len(sub) == 0:
        continue
    # Quintiles of FF
    sub = sub.copy()
    sub["ff_q"] = pd.qcut(sub["ff"], 5, labels=False, duplicates="drop")
    print(f"\n  {combo} FF quintiles (among FF >= {ff_thresh}):")
    for q in sorted(sub["ff_q"].unique()):
        grp = sub[sub["ff_q"] == q]
        print(f"    Q{q}: FF [{grp['ff'].min():.2f}-{grp['ff'].max():.2f}], "
              f"n={len(grp):,}, ret mean={grp['ret'].mean():.2f}, "
              f"wr={(grp['ret'] > 0).mean():.1%}")

# ── 10. CONCURRENT POSITION CORRELATION ──
print("\n--- 10. POSITION OVERLAP CHECK ---")
for combo in ["30-60", "30-90", "60-90"]:
    ff_thresh = FF_THRESHOLD_OLD.get(combo, 0.230)
    sub = df[(df["combo"] == combo) & (df["spread_cost"] >= MIN_SPREAD_COST)
             & (df["ff"] >= ff_thresh)]
    # Sort by FF desc, take top 20 per day (as backtest does)
    top = sub.sort_values("ff", ascending=False).groupby("entry_dt").head(20)
    # Check ticker concentration
    entry_ticker_counts = top.groupby("entry_dt")["root"].nunique()
    total_per_day = top.groupby("entry_dt").size()
    same_ticker_ratio = 1 - (entry_ticker_counts / total_per_day).mean()
    print(f"  {combo}: avg {total_per_day.mean():.0f} candidates/day, "
          f"{entry_ticker_counts.mean():.0f} unique tickers, "
          f"duplicate ratio={same_ticker_ratio:.1%}")

print("\n" + "=" * 70)
print("DIAGNOSTICS COMPLETE")
print("=" * 70)
