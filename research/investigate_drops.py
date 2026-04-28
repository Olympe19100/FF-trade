"""
Investigate the 73% drop rate in returns.py
and survivorship bias.
"""
import sys
import numpy as np
import pandas as pd
import sqlite3
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from core.config import CACHE, DB

# Load spreads and returns
spreads = pd.read_pickle(str(CACHE / "calendar_spreads.pkl"))
returns = pd.read_pickle(str(CACHE / "spread_returns.pkl"))

print(f"Spreads: {len(spreads):,}")
print(f"Returns: {len(returns):,}")

# Apply same filters as returns.py
strike_diff = (spreads["back_strike"] - spreads["front_strike"]).abs()
same_strike = strike_diff < 0.01
spreads_f1 = spreads[same_strike].copy()
print(f"After same-strike: {len(spreads_f1):,}")

spreads_f2 = spreads_f1[spreads_f1["spread_cost"] >= 0.10].copy()
print(f"After cost >= $0.10: {len(spreads_f2):,}")

# The volume filter removes more, but let's skip that for now and focus
# on what happens AFTER filters — the exit matching

# Returns have a unique key: (obs_date, root, front_exp, back_exp, front_strike)
returns["match_key"] = list(zip(
    returns["obs_date"], returns["root"],
    returns["front_exp"], returns["back_exp"],
    (returns["front_strike"] * 1000).round().astype(int)
))

# Check: how many of these returns actually came from the filtered spreads?
print(f"\nReturns: {len(returns):,}")
print(f"Unique match keys: {returns['match_key'].nunique():,}")

# ═══════════════════════════════════════════════════════════
# Run a mini-version of returns.py that tracks WHY each trade is dropped
# ═══════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("MINI RETURNS: track drop reasons")
print("=" * 70)

conn = sqlite3.connect(str(DB))

# Get available dates
cur = conn.cursor()
cur.execute("SELECT DISTINCT date FROM eod_history ORDER BY date")
avail_dates = sorted([r[0] for r in cur.fetchall()])

def find_nearest_date(target_int, avail_dates, max_gap=5):
    best, best_diff = None, 999999
    for d in avail_dates:
        diff = abs(d - target_int)
        if diff < best_diff:
            best_diff = diff
            best = d
    return best if best_diff <= max_gap else None

# Use filtered spreads (same strike, cost >= 0.10)
# Skip volume filter to see the full picture
test_spreads = spreads_f2.copy()
test_spreads["front_exp_int"] = test_spreads["front_exp"].astype(int)

import pickle
with open(CACHE / "bt_prices.pkl", "rb") as f:
    prices = pickle.load(f)
prices.index = pd.to_datetime(prices.index).date

# Pre-compute price date lookup
price_dates = sorted(prices.index)
price_ts = np.array([pd.Timestamp(d).value for d in price_dates])
price_date_map = {}
for ad in avail_dates:
    ad_obj = datetime.strptime(str(int(ad)), "%Y%m%d").date()
    ad_ts = pd.Timestamp(ad_obj).value
    idx = np.argmin(np.abs(price_ts - ad_ts))
    gap = abs((pd.Timestamp(price_dates[idx]) - pd.Timestamp(ad_obj)).days)
    if gap <= 5:
        price_date_map[ad] = price_dates[idx]

unique_exps = sorted(test_spreads["front_exp_int"].unique())
print(f"Unique expiries to check: {len(unique_exps)}")

# Sample: check first 50 expiries
sample_exps = unique_exps[:80]

stats = {"found": 0, "no_date": 0, "no_chain": 0,
         "no_price_map": 0, "no_stock": 0,
         "no_back_key": 0, "time_value_fail": 0}

# Also track stats for matched vs unmatched
matched_moves = []   # stock moves for matched trades
unmatched_moves = [] # stock moves for unmatched trades (where we have stock price)

for i, exp_int in enumerate(sample_exps):
    group = test_spreads[test_spreads["front_exp_int"] == exp_int]

    exit_date = find_nearest_date(exp_int, avail_dates)
    if exit_date is None:
        stats["no_date"] += len(group)
        continue

    if exit_date not in price_date_map:
        stats["no_price_map"] += len(group)
        continue
    price_date = price_date_map[exit_date]

    roots = group["root"].unique().tolist()
    placeholders = ",".join(["?"] * len(roots))

    # Query exit chain
    query = f"""
        SELECT c.root, c.expiration, c.strike / 1000.0 AS strike,
               c.right, e.bid, e.ask
        FROM eod_history e
        JOIN contracts c ON e.contract_id = c.contract_id
        WHERE e.date = ? AND c.right = 'C'
          AND c.root IN ({placeholders})
          AND e.bid > 0 AND e.ask > 0
    """
    chain = pd.read_sql_query(query, conn, params=[exit_date] + roots)

    if chain.empty:
        stats["no_chain"] += len(group)
        continue

    chain["mid"] = (chain["bid"] + chain["ask"]) / 2

    # Build call lookup
    call_lookup = {}
    for _, row in chain.iterrows():
        key = (row["root"], int(row["expiration"]),
               int(round(row["strike"] * 1000)))
        call_lookup[key] = row["mid"]

    for _, sp in group.iterrows():
        ticker = sp["root"]

        if ticker not in prices.columns:
            stats["no_stock"] += 1
            continue
        stock_px = prices.loc[price_date, ticker]
        if pd.isna(stock_px) or stock_px <= 0:
            stats["no_stock"] += 1
            continue

        back_key = (ticker, int(sp["back_exp"]),
                    int(round(sp["back_strike"] * 1000)))

        call_back_mid = call_lookup.get(back_key)
        if call_back_mid is None or call_back_mid <= 0:
            stats["no_back_key"] += 1
            # Track the stock move for this unmatched trade
            stock_move = (stock_px - sp["underlying_price"]) / sp["underlying_price"]
            unmatched_moves.append({
                "ticker": ticker,
                "stock_move": stock_move,
                "cost": sp["spread_cost"],
                "moneyness": (stock_px - sp["front_strike"]) / sp["front_strike"],
                "reason": "no_back_key"
            })
            continue

        # Time value check
        call_intrinsic_back = max(stock_px - sp["back_strike"], 0)
        call_tv = call_back_mid - call_intrinsic_back
        if call_tv > 0.15 * stock_px or call_tv < -0.01 * stock_px:
            stats["time_value_fail"] += 1
            # Track stock move
            stock_move = (stock_px - sp["underlying_price"]) / sp["underlying_price"]
            unmatched_moves.append({
                "ticker": ticker,
                "stock_move": stock_move,
                "cost": sp["spread_cost"],
                "moneyness": (stock_px - sp["front_strike"]) / sp["front_strike"],
                "reason": "time_value"
            })
            continue

        # MATCHED: compute return
        front_intrinsic_call = max(stock_px - sp["front_strike"], 0)
        call_exit_val = call_back_mid - front_intrinsic_call
        call_ret = (call_exit_val - sp["spread_cost"]) / sp["spread_cost"]
        stats["found"] += 1

        stock_move = (stock_px - sp["underlying_price"]) / sp["underlying_price"]
        matched_moves.append({
            "ticker": ticker,
            "stock_move": stock_move,
            "ret": call_ret,
            "cost": sp["spread_cost"],
            "moneyness": (stock_px - sp["front_strike"]) / sp["front_strike"],
        })

    if (i + 1) % 20 == 0:
        total = sum(stats.values())
        pct = stats["found"] / max(1, total) * 100
        print(f"  [{i+1}/{len(sample_exps)}] match rate={pct:.0f}%, "
              f"found={stats['found']:,}")

conn.close()

total = sum(stats.values())
print(f"\n  TOTAL PROCESSED: {total:,}")
print(f"  Found:           {stats['found']:,} ({stats['found']/max(1,total)*100:.1f}%)")
print(f"  No exit date:    {stats['no_date']:,}")
print(f"  No chain:        {stats['no_chain']:,}")
print(f"  No price map:    {stats['no_price_map']:,}")
print(f"  No stock px:     {stats['no_stock']:,}")
print(f"  No back key:     {stats['no_back_key']:,} ({stats['no_back_key']/max(1,total)*100:.1f}%)")
print(f"  Time value fail: {stats['time_value_fail']:,} ({stats['time_value_fail']/max(1,total)*100:.1f}%)")

# ═══════════════════════════════════════════════════════════
# KEY QUESTION: Are unmatched trades biased (losers)?
# ═══════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("SURVIVORSHIP BIAS: stock moves matched vs unmatched")
print("=" * 70)

if matched_moves:
    mm = pd.DataFrame(matched_moves)
    print(f"\n  MATCHED (n={len(mm):,}):")
    print(f"    Stock move: mean={mm['stock_move'].mean():.4f}, "
          f"std={mm['stock_move'].std():.4f}")
    print(f"    Abs move: mean={mm['stock_move'].abs().mean():.4f}")
    print(f"    Moneyness at exit: mean={mm['moneyness'].mean():.4f}")
    print(f"    Return: mean={mm['ret'].mean():.4f}, "
          f"median={mm['ret'].median():.4f}, "
          f"wr={(mm['ret']>0).mean():.1%}")

if unmatched_moves:
    um = pd.DataFrame(unmatched_moves)
    print(f"\n  UNMATCHED (n={len(um):,}):")
    print(f"    Stock move: mean={um['stock_move'].mean():.4f}, "
          f"std={um['stock_move'].std():.4f}")
    print(f"    Abs move: mean={um['stock_move'].abs().mean():.4f}")
    print(f"    Moneyness at exit: mean={um['moneyness'].mean():.4f}")

    for reason in um["reason"].unique():
        r = um[um["reason"] == reason]
        print(f"\n    Reason '{reason}' (n={len(r):,}):")
        print(f"      Stock move: mean={r['stock_move'].mean():.4f}, "
              f"std={r['stock_move'].std():.4f}")
        print(f"      Abs stock move: mean={r['stock_move'].abs().mean():.4f}")
        print(f"      Moneyness: mean={r['moneyness'].mean():.4f}")

# ═══════════════════════════════════════════════════════════
# WHAT IF UNMATCHED = TOTAL LOSS?
# ═══════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("SCENARIO: Unmatched trades = total loss (-100%)")
print("=" * 70)

if matched_moves:
    n_matched = len(mm)
    n_unmatched = len(um) if unmatched_moves else 0
    n_total = n_matched + n_unmatched
    mean_matched = mm["ret"].mean()

    # Scenario 1: unmatched = -100%
    combined_mean = (n_matched * mean_matched + n_unmatched * (-1.0)) / n_total
    combined_wr_numer = (mm["ret"] > 0).sum()
    combined_wr = combined_wr_numer / n_total

    print(f"  Matched: {n_matched:,} trades, mean={mean_matched:.4f}")
    print(f"  Unmatched: {n_unmatched:,} trades (assumed -100%)")
    print(f"  Combined mean return: {combined_mean:.4f}")
    print(f"  Combined win rate: {combined_wr:.4f}")
    print(f"  (PDF reference: mean~-0.10, wr~38-53%)")

    # Scenario 2: unmatched = -50%
    combined_mean_50 = (n_matched * mean_matched + n_unmatched * (-0.50)) / n_total
    print(f"\n  If unmatched = -50%: combined mean = {combined_mean_50:.4f}")

    # Scenario 3: unmatched = -80%
    combined_mean_80 = (n_matched * mean_matched + n_unmatched * (-0.80)) / n_total
    print(f"  If unmatched = -80%: combined mean = {combined_mean_80:.4f}")

print("\n" + "=" * 70)
print("INVESTIGATION COMPLETE")
print("=" * 70)
