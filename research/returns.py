"""
Forward Factor Research — Step 2
Compute calendar spread returns, closing as a spread 1 day before front expiry.

Aligned with Forward Factors Research PDF methodology:
  - Same strike front & back
  - Valid bid/ask at exit
  - NO volume filter, NO earnings filter, NO min cost filter
    (FF threshold handles signal quality in analysis.py)

Supports:
  - Single calendar (call only)
  - Double calendar (call ATM + put ATM at same strike)

Return (single) = (back_call_mid - front_call_mid - spread_cost) / spread_cost
Return (double) = (back_call+back_put - front_call-front_put - combined_cost) / combined_cost

Exit: close as a spread 1 day before front expiry (avoid assignment risk).

Usage:
    python returns.py          # Full run
    python returns.py --test   # First 5 expiries only
"""

import sqlite3
import numpy as np
import pandas as pd
import pickle
from pathlib import Path
from datetime import datetime

ROOT  = Path(r"C:\Users\ANTEC MSI\Desktop\pro\Option trading")
DB    = ROOT / "sp500_options.db"
CACHE = ROOT / "cache"


def int_to_date(d):
    return datetime.strptime(str(int(d)), "%Y%m%d").date()


def load_spreads():
    df = pd.read_pickle(str(CACHE / "calendar_spreads.pkl"))
    print(f"Loaded {len(df):,} spreads")
    return df


def load_prices():
    with open(CACHE / "bt_prices.pkl", "rb") as f:
        prices = pickle.load(f)
    prices.index = pd.to_datetime(prices.index).date
    return prices


def get_available_dates(conn):
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT date FROM eod_history ORDER BY date")
    return sorted([r[0] for r in cur.fetchall()])


def find_nearest_date(target_int, avail_dates, max_gap=5):
    best, best_diff = None, 999999
    for d in avail_dates:
        diff = abs(d - target_int)
        if diff < best_diff:
            best_diff = diff
            best = d
    return best if best_diff <= max_gap else None


def find_date_before(target_int, avail_dates, min_gap=1, max_gap=5):
    """Find the trading day 1 day before target (for closing before expiry)."""
    best, best_diff = None, 999999
    for d in avail_dates:
        diff = target_int - d  # positive = d is before target
        if min_gap <= diff <= max_gap and diff < best_diff:
            best_diff = diff
            best = d
    return best


def compute_returns(test_mode=False):
    spreads = load_spreads()

    # ── Filter: Same strike (fundamental to calendar spread definition) ──
    strike_diff = (spreads["back_strike"] - spreads["front_strike"]).abs()
    same_strike = strike_diff < 0.01
    print(f"  Same strike: {same_strike.sum():,} / {len(spreads):,}")
    spreads = spreads[same_strike].copy()

    # Check spread_type availability
    has_type = "spread_type" in spreads.columns
    if has_type:
        n_dbl = (spreads["spread_type"] == "double").sum()
        print(f"  Double calendar: {n_dbl:,} / {len(spreads):,}")
    print(f"After filters: {len(spreads):,} spreads")

    conn = sqlite3.connect(str(DB))
    prices = load_prices()
    avail_dates = get_available_dates(conn)

    print(f"Available EOD dates: {len(avail_dates)}")
    print(f"Stock prices: {prices.shape[0]} days, {prices.shape[1]} tickers")

    # Pre-compute price date lookup
    price_dates = sorted(prices.index)
    price_ts = np.array([pd.Timestamp(d).value for d in price_dates])
    price_date_map = {}
    for ad in avail_dates:
        ad_obj = int_to_date(ad)
        ad_ts = pd.Timestamp(ad_obj).value
        idx = np.argmin(np.abs(price_ts - ad_ts))
        gap = abs((pd.Timestamp(price_dates[idx]) - pd.Timestamp(ad_obj)).days)
        if gap <= 5:
            price_date_map[ad] = price_dates[idx]

    # Group by front_exp
    spreads["front_exp_int"] = spreads["front_exp"].astype(int)
    unique_exps = sorted(spreads["front_exp_int"].unique())
    print(f"Unique front expiries: {len(unique_exps)}")

    if test_mode:
        unique_exps = unique_exps[:5]
        print(f"TEST MODE: {len(unique_exps)} expiries")

    results = []
    stats = {"found": 0, "no_date": 0, "no_chain": 0,
             "no_price": 0, "no_back": 0, "no_front_exit": 0}

    for i, exp_int in enumerate(unique_exps):
        group = spreads[spreads["front_exp_int"] == exp_int]

        # EXIT 1 DAY BEFORE front expiry (avoid assignment risk)
        exit_date = find_date_before(exp_int, avail_dates, min_gap=1, max_gap=5)
        if exit_date is None:
            stats["no_date"] += len(group)
            continue

        if exit_date not in price_date_map:
            stats["no_price"] += len(group)
            continue
        price_date = price_date_map[exit_date]

        roots = group["root"].unique().tolist()
        placeholders = ",".join(["?"] * len(roots))

        # Query exit chain — BOTH calls and puts, valid bid/ask only
        query = f"""
            SELECT c.root, c.expiration, c.strike / 1000.0 AS strike,
                   c.right, e.bid, e.ask
            FROM eod_history e
            JOIN contracts c ON e.contract_id = c.contract_id
            WHERE e.date = ? AND c.right IN ('C', 'P')
              AND c.root IN ({placeholders})
              AND e.bid > 0 AND e.ask > 0
        """
        chain = pd.read_sql_query(query, conn, params=[exit_date] + roots)

        if chain.empty:
            stats["no_chain"] += len(group)
            continue

        chain["mid"] = (chain["bid"] + chain["ask"]) / 2

        # Build lookups: (root, expiration, strike_x1000) -> mid
        call_lookup = {}
        put_lookup = {}
        for _, row in chain.iterrows():
            key = (row["root"], int(row["expiration"]),
                   int(round(row["strike"] * 1000)))
            if row["right"] == "C":
                call_lookup[key] = row["mid"]
            else:
                put_lookup[key] = row["mid"]

        if (i + 1) % 20 == 0 or i == 0:
            pct = stats["found"] / max(1, sum(stats.values())) * 100
            print(f"[{i+1}/{len(unique_exps)}] exp={exp_int} -> exit={exit_date}, "
                  f"{len(group)} spreads, chain={len(chain)}, "
                  f"match rate={pct:.0f}%")

        for _, sp in group.iterrows():
            ticker = sp["root"]

            if ticker not in prices.columns:
                stats["no_price"] += 1
                continue
            stock_px = prices.loc[price_date, ticker]
            if pd.isna(stock_px) or stock_px <= 0:
                stats["no_price"] += 1
                continue

            front_key = (ticker, int(sp["front_exp"]),
                         int(round(sp["front_strike"] * 1000)))
            back_key = (ticker, int(sp["back_exp"]),
                        int(round(sp["back_strike"] * 1000)))

            # ── Call leg: close as a spread ──
            call_front_mid = call_lookup.get(front_key)
            call_back_mid = call_lookup.get(back_key)

            if call_front_mid is None or call_front_mid <= 0:
                stats["no_front_exit"] += 1
                continue
            if call_back_mid is None or call_back_mid <= 0:
                stats["no_back"] += 1
                continue

            # Exit value = sell back - buy front (close the spread)
            call_exit_val = call_back_mid - call_front_mid
            call_ret = (call_exit_val - sp["spread_cost"]) / sp["spread_cost"]

            row_data = {
                "obs_date": sp["obs_date"],
                "root": ticker,
                "combo": sp["combo"],
                "ff": sp["ff"],
                "spread_cost": sp["spread_cost"],
                "underlying_price": sp["underlying_price"],
                "front_exp": int(sp["front_exp"]),
                "front_strike": sp["front_strike"],
                "front_iv": sp["front_iv"],
                "back_exp": int(sp["back_exp"]),
                "back_strike": sp["back_strike"],
                "back_iv": sp["back_iv"],
                "exit_date": exit_date,
                "stock_price_exit": stock_px,
                "call_exit_value": call_exit_val,
                "ret": call_ret,
                "spread_type": sp.get("spread_type", "single"),
            }

            # ── Put leg (double calendar): close as a spread ──
            is_double = has_type and sp.get("spread_type") == "double"
            if is_double:
                put_front_mid = put_lookup.get(front_key)
                put_back_mid = put_lookup.get(back_key)
                combined_cost = sp.get("combined_cost", np.nan)

                if (put_front_mid is not None and put_front_mid > 0
                        and put_back_mid is not None and put_back_mid > 0
                        and not pd.isna(combined_cost) and combined_cost > 0):
                    put_exit_val = put_back_mid - put_front_mid
                    double_exit_val = call_exit_val + put_exit_val
                    double_ret = (double_exit_val - combined_cost) / combined_cost

                    row_data["put_exit_value"] = put_exit_val
                    row_data["combined_cost"] = combined_cost
                    row_data["double_ret"] = double_ret

            results.append(row_data)
            stats["found"] += 1

    conn.close()

    df = pd.DataFrame(results)
    total = sum(stats.values())
    print(f"\n{'='*60}")
    print(f"Results: {len(df):,} spreads with returns")
    print(f"  Found:      {stats['found']:,} ({stats['found']/max(1,total)*100:.1f}%)")
    print(f"  No date:    {stats['no_date']:,}")
    print(f"  No chain:   {stats['no_chain']:,}")
    print(f"  No price:   {stats['no_price']:,}")
    print(f"  No front:   {stats['no_front_exit']:,}")
    print(f"  No back:    {stats['no_back']:,}")

    if len(df) > 0:
        # Compute period length
        obs_min = int(df["obs_date"].min())
        obs_max = int(df["obs_date"].max())
        d0 = datetime.strptime(str(obs_min), "%Y%m%d")
        d1 = datetime.strptime(str(obs_max), "%Y%m%d")
        n_years = max((d1 - d0).days / 365.25, 0.1)

        # Single calendar stats
        for combo in sorted(df["combo"].unique()):
            sub = df[df["combo"] == combo]
            wins = (sub["ret"] > 0).mean()
            per_year = len(sub) / n_years
            per_month = per_year / 12
            print(f"\n{combo} (call-only): {len(sub):,} spreads")
            print(f"  mean={sub['ret'].mean():.6f}, std={sub['ret'].std():.6f}")
            print(f"  min={sub['ret'].min():.6f}, max={sub['ret'].max():.6f}")
            print(f"  25%={sub['ret'].quantile(0.25):.6f}, "
                  f"50%={sub['ret'].median():.6f}, "
                  f"75%={sub['ret'].quantile(0.75):.6f}")
            print(f"  win rate={wins:.6f}")
            print(f"  Averages ({n_years:.2f} years total):")
            print(f"    per year: {per_year:.2f}")
            print(f"    per month: {per_month:.2f}")

        # Double calendar stats
        if "double_ret" in df.columns:
            dbl = df.dropna(subset=["double_ret"])
            print(f"\n--- DOUBLE CALENDAR ---")
            for combo in sorted(dbl["combo"].unique()):
                sub = dbl[dbl["combo"] == combo]
                wins = (sub["double_ret"] > 0).mean()
                per_year = len(sub) / n_years
                per_month = per_year / 12
                print(f"\n{combo} (double): {len(sub):,} spreads")
                print(f"  mean={sub['double_ret'].mean():.6f}, "
                      f"std={sub['double_ret'].std():.6f}")
                print(f"  min={sub['double_ret'].min():.6f}, "
                      f"max={sub['double_ret'].max():.6f}")
                print(f"  win rate={wins:.6f}")
                print(f"    per year: {per_year:.2f}, per month: {per_month:.2f}")

        out = CACHE / "spread_returns.pkl"
        df.to_pickle(str(out))
        print(f"\nSaved to {out}")

    return df


if __name__ == "__main__":
    import sys
    test = "--test" in sys.argv
    if test:
        print("=" * 60)
        print("TEST MODE")
        print("=" * 60)
    else:
        print("=" * 60)
        print("COMPUTING SPREAD RETURNS (single + double)")
        print("=" * 60)
    compute_returns(test_mode=test)
