"""
Deep investigation of return calculation bug.
Our returns are ~20x higher than PDF reference:
  - Our mean: 2.90 (290%) vs PDF: 0.14 (14%)
  - Our win rate: 87.7% vs PDF: 47.8%
  - Our max: 116.1 vs PDF: 3.35
"""
import sys
import numpy as np
import pandas as pd
import sqlite3
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from core.config import CACHE, DB

# Load returns
df = pd.read_pickle(str(CACHE / "spread_returns.pkl"))
print(f"Total returns: {len(df):,}")
print(f"Columns: {list(df.columns)}")

# ══════════════════════════════════════════════════════════
# 1. RETURN DISTRIBUTION vs PDF
# ══════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("1. RETURN DISTRIBUTION (raw, no FF filter)")
print("=" * 70)

for combo in sorted(df["combo"].unique()):
    sub = df[df["combo"] == combo]
    r = sub["ret"]
    print(f"\n  {combo}: n={len(sub):,}")
    print(f"    mean={r.mean():.4f}  median={r.median():.4f}  std={r.std():.4f}")
    print(f"    min={r.min():.4f}  max={r.max():.4f}")
    print(f"    win_rate={(r > 0).mean():.4f}")
    print(f"    p5={r.quantile(0.05):.4f}  p25={r.quantile(0.25):.4f}  "
          f"p75={r.quantile(0.75):.4f}  p95={r.quantile(0.95):.4f}")

# PDF reference (unfiltered, Table 1):
# 30-60: mean=-0.16, wr=38%
# 30-90: mean=-0.09, wr=39%
# 60-90: mean=-0.07, wr=53%
print("\n  PDF Reference (unfiltered):")
print("    30-60: mean=-0.16, wr=38%")
print("    30-90: mean=-0.09, wr=39%")
print("    60-90: mean=-0.07, wr=53%")

# ══════════════════════════════════════════════════════════
# 2. EXTREME RETURNS — are these real?
# ══════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("2. TOP 20 EXTREME RETURNS")
print("=" * 70)

top20 = df.nlargest(20, "ret")
for _, row in top20.iterrows():
    print(f"  {row['root']:6s} {int(row['obs_date'])} -> {int(row['front_exp'])}  "
          f"combo={row['combo']}  cost={row['spread_cost']:.4f}  "
          f"exit_val={row['call_exit_value']:.4f}  ret={row['ret']:.2f}  "
          f"S_entry={row['underlying_price']:.2f}  S_exit={row['stock_price_exit']:.2f}  "
          f"K={row['front_strike']:.2f}")

# ══════════════════════════════════════════════════════════
# 3. SPREAD COST DISTRIBUTION — too low?
# ══════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("3. SPREAD COST DISTRIBUTION")
print("=" * 70)

for combo in sorted(df["combo"].unique()):
    sub = df[df["combo"] == combo]
    c = sub["spread_cost"]
    print(f"\n  {combo}: (in $/share)")
    print(f"    mean={c.mean():.4f}  median={c.median():.4f}")
    print(f"    min={c.min():.4f}  max={c.max():.4f}")
    print(f"    p5={c.quantile(0.05):.4f}  p10={c.quantile(0.10):.4f}  "
          f"p25={c.quantile(0.25):.4f}")
    # How many are < $0.50?
    cheap = (c < 0.50).sum()
    very_cheap = (c < 0.20).sum()
    print(f"    < $0.50: {cheap:,} ({cheap/len(sub)*100:.1f}%)")
    print(f"    < $0.20: {very_cheap:,} ({very_cheap/len(sub)*100:.1f}%)")

# ══════════════════════════════════════════════════════════
# 4. RETURN vs SPREAD COST — cheap spreads inflating?
# ══════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("4. MEAN RETURN BY SPREAD COST BUCKET")
print("=" * 70)

bins = [0, 0.10, 0.20, 0.50, 1.00, 2.00, 5.00, 999]
labels = ["<0.10", "0.10-0.20", "0.20-0.50", "0.50-1.00",
          "1.00-2.00", "2.00-5.00", ">5.00"]
df["cost_bucket"] = pd.cut(df["spread_cost"], bins=bins, labels=labels)

for combo in sorted(df["combo"].unique()):
    sub = df[df["combo"] == combo]
    print(f"\n  {combo}:")
    print(f"    {'Bucket':>12s}  {'N':>7s}  {'Mean':>8s}  {'Median':>8s}  "
          f"{'WR':>6s}  {'Max':>8s}")
    for bucket in labels:
        b = sub[sub["cost_bucket"] == bucket]
        if len(b) == 0:
            continue
        print(f"    {bucket:>12s}  {len(b):7,}  {b['ret'].mean():8.3f}  "
              f"{b['ret'].median():8.3f}  {(b['ret']>0).mean():6.1%}  "
              f"{b['ret'].max():8.2f}")

# ══════════════════════════════════════════════════════════
# 5. EXIT VALUE DECOMPOSITION
# ══════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("5. EXIT VALUE DECOMPOSITION")
print("=" * 70)

# exit_val = back_mid_exit - front_intrinsic
# front_intrinsic = max(S_exit - K_front, 0)
# We stored call_exit_value and stock_price_exit, front_strike
df["front_intrinsic"] = np.maximum(df["stock_price_exit"] - df["front_strike"], 0)
df["moneyness"] = (df["stock_price_exit"] - df["front_strike"]) / df["front_strike"]
df["back_mid_exit"] = df["call_exit_value"] + df["front_intrinsic"]

print("\n  front_intrinsic distribution:")
print(f"    mean={df['front_intrinsic'].mean():.4f}  "
      f"median={df['front_intrinsic'].median():.4f}")
pct_itm = (df["front_intrinsic"] > 0).mean()
print(f"    % ITM at exit: {pct_itm:.1%}")

print("\n  back_mid_exit (= call_exit_val + front_intrinsic):")
print(f"    mean={df['back_mid_exit'].mean():.4f}  "
      f"median={df['back_mid_exit'].median():.4f}")

print("\n  moneyness at exit (S/K - 1):")
print(f"    mean={df['moneyness'].mean():.4f}  "
      f"median={df['moneyness'].median():.4f}")

# ══════════════════════════════════════════════════════════
# 6. MANUAL VERIFICATION: Sample 10 trades against DB
# ══════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("6. MANUAL VERIFICATION (10 random trades)")
print("=" * 70)

conn = sqlite3.connect(str(DB))

# Pick 10 random trades with cost > $1.00
sample = df[df["spread_cost"] >= 1.0].sample(10, random_state=42)

for idx, row in sample.iterrows():
    ticker = row["root"]
    obs_int = int(row["obs_date"])
    front_exp = int(row["front_exp"])
    back_exp = int(row["back_exp"])
    strike = row["front_strike"]
    strike_k = int(round(strike * 1000))

    # Get entry data
    entry_q = """
        SELECT c.root, c.right, c.expiration, c.strike/1000.0 as strike,
               e.bid, e.ask, (e.bid+e.ask)/2.0 as mid
        FROM eod_history e JOIN contracts c ON e.contract_id = c.contract_id
        WHERE e.date = ? AND c.root = ?
          AND c.right = 'C'
          AND CAST(c.strike/1000.0 * 1000 + 0.5 AS INTEGER) = ?
        ORDER BY c.expiration
    """
    entry_chain = pd.read_sql_query(entry_q, conn,
                                     params=[obs_int, ticker, strike_k])

    # Get exit data (at front_exp, back option)
    # Find nearest available date
    avail = pd.read_sql_query(
        "SELECT DISTINCT date FROM eod_history WHERE date BETWEEN ? AND ? ORDER BY date",
        conn, params=[front_exp - 5, front_exp + 5]
    )["date"].tolist()
    exit_date = min(avail, key=lambda d: abs(d - front_exp)) if avail else None

    if exit_date:
        exit_q = """
            SELECT c.root, c.right, c.expiration, c.strike/1000.0 as strike,
                   e.bid, e.ask, (e.bid+e.ask)/2.0 as mid
            FROM eod_history e JOIN contracts c ON e.contract_id = c.contract_id
            WHERE e.date = ? AND c.root = ?
              AND c.right = 'C'
              AND c.expiration = ?
              AND CAST(c.strike/1000.0 * 1000 + 0.5 AS INTEGER) = ?
        """
        exit_chain = pd.read_sql_query(exit_q, conn,
                                        params=[exit_date, ticker,
                                                back_exp, strike_k])
    else:
        exit_chain = pd.DataFrame()

    print(f"\n  Trade: {ticker} {obs_int} K={strike:.2f}")
    print(f"    Entry: front_exp={front_exp}, back_exp={back_exp}")
    print(f"    Stored: cost={row['spread_cost']:.4f}, "
          f"exit_val={row['call_exit_value']:.4f}, ret={row['ret']:.4f}")
    print(f"    S_entry={row['underlying_price']:.2f}, "
          f"S_exit={row['stock_price_exit']:.2f}")

    if not entry_chain.empty:
        front_e = entry_chain[entry_chain["expiration"] == front_exp]
        back_e = entry_chain[entry_chain["expiration"] == back_exp]
        if not front_e.empty:
            print(f"    DB entry front: bid={front_e.iloc[0]['bid']:.4f}, "
                  f"ask={front_e.iloc[0]['ask']:.4f}, "
                  f"mid={front_e.iloc[0]['mid']:.4f}")
        if not back_e.empty:
            print(f"    DB entry back:  bid={back_e.iloc[0]['bid']:.4f}, "
                  f"ask={back_e.iloc[0]['ask']:.4f}, "
                  f"mid={back_e.iloc[0]['mid']:.4f}")
            if not front_e.empty:
                manual_cost = back_e.iloc[0]["mid"] - front_e.iloc[0]["mid"]
                print(f"    Manual spread_cost: {manual_cost:.4f} "
                      f"(stored: {row['spread_cost']:.4f})")

    if not exit_chain.empty:
        print(f"    DB exit back: bid={exit_chain.iloc[0]['bid']:.4f}, "
              f"ask={exit_chain.iloc[0]['ask']:.4f}, "
              f"mid={exit_chain.iloc[0]['mid']:.4f}")
        back_mid_exit = exit_chain.iloc[0]["mid"]
        front_intr = max(row["stock_price_exit"] - strike, 0)
        manual_exit_val = back_mid_exit - front_intr
        manual_ret = (manual_exit_val - row["spread_cost"]) / row["spread_cost"]
        print(f"    Manual: back_mid={back_mid_exit:.4f}, "
              f"front_intrinsic={front_intr:.4f}")
        print(f"    Manual: exit_val={manual_exit_val:.4f}, ret={manual_ret:.4f}")
        # Also compute with BID instead of MID for exit
        back_bid_exit = exit_chain.iloc[0]["bid"]
        bid_exit_val = back_bid_exit - front_intr
        bid_ret = (bid_exit_val - row["spread_cost"]) / row["spread_cost"]
        print(f"    Using BID: exit_val={bid_exit_val:.4f}, ret={bid_ret:.4f}")
    else:
        print(f"    Exit: no data at date {exit_date}")

conn.close()

# ══════════════════════════════════════════════════════════
# 7. SURVIVORSHIP BIAS — dropped trades
# ══════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("7. SURVIVORSHIP BIAS — what % of trades are we dropping?")
print("=" * 70)

# Load spreads (pre-return computation)
spreads = pd.read_pickle(str(CACHE / "calendar_spreads.pkl"))
spreads_filtered = spreads[
    ((spreads["back_strike"] - spreads["front_strike"]).abs() < 0.01) &
    (spreads["spread_cost"] >= 0.10)
]
print(f"  Spreads after basic filters: {len(spreads_filtered):,}")
print(f"  Returns computed:            {len(df):,}")
print(f"  Drop rate: {1 - len(df)/len(spreads_filtered):.1%}")
print(f"  (If dropped trades are mostly losers, this creates upward bias)")

# ══════════════════════════════════════════════════════════
# 8. KEY DIAGNOSTIC: What does the return look like if we
#    use ENTRY spread_cost correctly?
# ══════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("8. SPREAD_COST: is back_mid - front_mid the correct denominator?")
print("=" * 70)

# In spreads.py, spread_cost = mid_b - mid_f (back mid - front mid)
# This is the NET DEBIT to open the spread
# But... does the sign convention match the return formula?

# Check: is spread_cost always positive?
neg_cost = (df["spread_cost"] <= 0).sum()
print(f"  Negative/zero spread_cost: {neg_cost} (should be 0 after filter)")

# Check: typical spread_cost relative to stock price
df["cost_pct"] = df["spread_cost"] / df["underlying_price"] * 100
print(f"\n  spread_cost as % of stock price:")
print(f"    mean={df['cost_pct'].mean():.3f}%  "
      f"median={df['cost_pct'].median():.3f}%")
print(f"    p5={df['cost_pct'].quantile(0.05):.4f}%  "
      f"p95={df['cost_pct'].quantile(0.95):.3f}%")

# ══════════════════════════════════════════════════════════
# 9. CHECK: Are we exiting at the WRONG date?
# ══════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("9. EXIT TIMING: how far is exit_date from front_exp?")
print("=" * 70)

df["exp_gap"] = df["exit_date"] - df["front_exp"]
print(f"  exit_date - front_exp gap:")
print(f"    mean={df['exp_gap'].mean():.2f}  median={df['exp_gap'].median():.0f}")
print(f"    min={df['exp_gap'].min()}  max={df['exp_gap'].max()}")
# If gap > 0, we're exiting AFTER front expiry, which is wrong
# (back option has more time value = higher price = inflated exit)
after = (df["exp_gap"] > 0).sum()
before = (df["exp_gap"] < 0).sum()
exact = (df["exp_gap"] == 0).sum()
print(f"    Exact match:  {exact:,} ({exact/len(df)*100:.1f}%)")
print(f"    After expiry: {after:,} ({after/len(df)*100:.1f}%)")
print(f"    Before expiry:{before:,} ({before/len(df)*100:.1f}%)")

# ══════════════════════════════════════════════════════════
# 10. CRITICAL: Return by exit timing gap
# ══════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("10. RETURN BY EXIT DATE GAP (is earlier exit = higher return?)")
print("=" * 70)
for gap in sorted(df["exp_gap"].unique()):
    g = df[df["exp_gap"] == gap]
    if len(g) < 100:
        continue
    print(f"  gap={gap:+d}: n={len(g):,}, mean_ret={g['ret'].mean():.3f}, "
          f"wr={(g['ret']>0).mean():.1%}")

# ══════════════════════════════════════════════════════════
# 11. WHAT IF WE USE BID INSTEAD OF MID AT EXIT?
# ══════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("11. IMPACT ESTIMATE: using bid vs mid at exit")
print("=" * 70)
# If back option at exit has bid=X, ask=Y, mid=(X+Y)/2
# The difference is typically the spread / 2
# For options, bid-ask spread is often 5-20% of mid
# So using mid inflates exit by ~5-10%
# But that alone doesn't explain 20x difference
# Let's estimate: if bid = 0.85 * mid (15% discount)
for haircut in [0.05, 0.10, 0.15, 0.20, 0.30]:
    adj_exit = df["call_exit_value"] * (1 - haircut)
    adj_ret = (adj_exit - df["spread_cost"]) / df["spread_cost"]
    print(f"  Haircut={haircut:.0%}: mean={adj_ret.mean():.3f}, "
          f"wr={(adj_ret>0).mean():.1%}")

print("\n" + "=" * 70)
print("INVESTIGATION COMPLETE")
print("=" * 70)
