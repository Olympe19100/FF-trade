"""
Download EOD option data for ALL active tickers using ThetaData v3 API.

Resumable: tracks progress per ticker. Downloads all dates for each ticker
in one pass using the bulk EOD endpoint.

Usage:
    python core/download_v3.py                    # Download all tickers, all dates
    python core/download_v3.py --year 2024        # Single year only
    python core/download_v3.py --tickers AAPL,TSLA  # Specific tickers
    python core/download_v3.py --new-only         # Only tickers not yet in eod_history
    python core/download_v3.py --test             # Test: 3 tickers, 5 dates
"""

import sys
import sqlite3
import time
import argparse
import signal
import requests
from datetime import datetime, timedelta
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from core.config import DB, THETADATA_URL, ensure_theta_terminal

MAX_DTE = 150  # Max days to expiry to download

# ── Graceful shutdown ─────────────────────────────────────────────────
shutdown_requested = False

def signal_handler(sig, frame):
    global shutdown_requested
    print("\nShutdown requested, finishing current ticker...", flush=True)
    shutdown_requested = True

signal.signal(signal.SIGINT, signal_handler)


# ── Trading days ──────────────────────────────────────────────────────

def us_market_holidays(year):
    holidays = set()
    holidays.add((1, 1))
    holidays.add((6, 19))
    holidays.add((7, 4))
    holidays.add((12, 25))
    return holidays

def is_likely_holiday(d):
    fixed = us_market_holidays(d.year)
    if (d.month, d.day) in fixed:
        return True
    if d.weekday() == 4:
        sat = d + timedelta(days=1)
        if (sat.month, sat.day) in fixed:
            return True
    if d.weekday() == 0:
        sun = d - timedelta(days=1)
        if (sun.month, sun.day) in fixed:
            return True
    if d.month == 1 and d.weekday() == 0 and 15 <= d.day <= 21:
        return True
    if d.month == 2 and d.weekday() == 0 and 15 <= d.day <= 21:
        return True
    if d.month == 5 and d.weekday() == 0 and d.day >= 25:
        return True
    if d.month == 9 and d.weekday() == 0 and d.day <= 7:
        return True
    if d.month == 11 and d.weekday() == 3 and 22 <= d.day <= 28:
        return True
    return False

def generate_trading_days(start_year=2016, end_year=2026):
    days = []
    d = datetime(start_year, 1, 1)
    end = datetime(end_year, 12, 31)
    while d <= end:
        if d.weekday() < 5 and not is_likely_holiday(d):
            days.append(int(d.strftime("%Y%m%d")))
        d += timedelta(days=1)
    return days


# ── ThetaData API ─────────────────────────────────────────────────────

def download_ticker_range(root, start_date, end_date):
    """Download all option contracts for one ticker over a date RANGE.
    Returns list of (contract_key, eod_tuple) pairs.
    One API call fetches an entire year of data at once."""
    try:
        r = requests.get(
            f"{THETADATA_URL}/v3/option/history/eod",
            params={
                "symbol": root,
                "expiration": "*",
                "strike": "*",
                "right": "both",
                "start_date": str(start_date),
                "end_date": str(end_date),
                "max_dte": MAX_DTE,
                "format": "json",
            },
            timeout=300,  # Long timeout for large date ranges
        )
        if r.status_code == 472 or "No data" in r.text[:100]:
            return []
        if r.status_code != 200:
            return None

        data = r.json()
        records = data.get("response", [])
        if not records:
            return []

        results = []
        for rec in records:
            contract = rec["contract"]
            eod_list = rec.get("data", [])
            if not eod_list:
                continue

            symbol = contract["symbol"]
            exp_str = contract["expiration"].replace("-", "")
            exp_int = int(exp_str)
            strike_raw = contract["strike"]
            strike_milli = int(round(strike_raw * 1000))
            right = "C" if contract["right"] == "CALL" else "P"

            contract_key = (symbol, exp_int, strike_milli, right)

            # Each contract may have multiple dates in the range
            for eod in eod_list:
                # Parse date from 'created' or 'last_trade' field
                created = eod.get("created", "")
                if created:
                    date_int = int(created[:10].replace("-", ""))
                else:
                    continue

                bid = eod.get("bid")
                ask = eod.get("ask")
                close_px = eod.get("close")
                volume = eod.get("volume", 0)
                count = eod.get("count", 0)
                open_px = eod.get("open")
                high_px = eod.get("high")
                low_px = eod.get("low")
                bid_size = eod.get("bid_size", 0)
                ask_size = eod.get("ask_size", 0)

                eod_tuple = (date_int, None, open_px, high_px, low_px,
                             close_px, volume, count, bid_size, bid,
                             ask_size, ask)

                results.append((contract_key, eod_tuple))

        return results

    except requests.exceptions.Timeout:
        return None
    except Exception:
        return None


# ── Database operations ───────────────────────────────────────────────

def get_or_create_contracts(conn, contract_keys):
    """Bulk get or create contract IDs. Returns dict: key -> contract_id."""
    cur = conn.cursor()
    id_map = {}

    for key in contract_keys:
        root, exp, strike, right = key
        row = cur.execute(
            "SELECT contract_id FROM contracts "
            "WHERE root=? AND expiration=? AND strike=? AND right=?",
            (root, exp, strike, right),
        ).fetchone()

        if row:
            id_map[key] = row[0]
        else:
            cur.execute(
                "INSERT INTO contracts (root, expiration, strike, right) "
                "VALUES (?, ?, ?, ?)",
                (root, exp, strike, right),
            )
            id_map[key] = cur.lastrowid

    return id_map


def upsert_eod_rows(conn, rows):
    """Insert or replace EOD history rows. Each row = (contract_id, date, ...)."""
    cur = conn.cursor()
    cur.executemany(
        "INSERT OR REPLACE INTO eod_history "
        "(contract_id, date, ms_of_day, open, high, low, close, "
        " volume, count, bid_size, bid, ask_size, ask) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        rows,
    )


def get_tickers_with_data(conn):
    """Get set of tickers that already have contracts (fast: uses contracts table)."""
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT root FROM contracts")
    return {r[0] for r in cur.fetchall()}


def get_ticker_dates(conn, root):
    """Get set of dates already downloaded for a specific ticker.
    Uses contract_id range for fast lookup without full table scan."""
    cur = conn.cursor()
    # First get contract IDs for this ticker (fast: uses UNIQUE index)
    cids = cur.execute(
        "SELECT contract_id FROM contracts WHERE root = ?", (root,)
    ).fetchall()
    if not cids:
        return set()
    # Sample first contract to get dates (all contracts for same ticker
    # have same dates since we download all at once per date)
    sample_cid = cids[0][0]
    cur.execute(
        "SELECT DISTINCT date FROM eod_history WHERE contract_id = ?",
        (sample_cid,),
    )
    return {r[0] for r in cur.fetchall()}


# ── Main download loop ────────────────────────────────────────────────

def download_all(start_year=2016, end_year=2026, tickers=None,
                 new_only=False, n_workers=8, test_mode=False):
    """Download EOD option data for all active tickers."""

    ensure_theta_terminal()

    conn = sqlite3.connect(str(DB), timeout=60)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")

    # Get ticker list
    if tickers:
        ticker_list = tickers
    else:
        cur = conn.cursor()
        cur.execute("SELECT root FROM tickers WHERE is_active = 1 ORDER BY root")
        ticker_list = [r[0] for r in cur.fetchall()]

    if test_mode:
        ticker_list = ticker_list[:3]

    print(f"Tickers to process: {len(ticker_list):,}")

    # Skip tickers that already have data
    if new_only:
        existing_tickers = get_tickers_with_data(conn)
        ticker_list = [t for t in ticker_list if t not in existing_tickers]
        print(f"After filtering existing: {len(ticker_list):,} new tickers")

    if not ticker_list:
        print("Nothing to download!")
        conn.close()
        return

    # Build year chunks for bulk download
    years = list(range(start_year, end_year + 1))
    if test_mode:
        years = years[:1]
    all_days = generate_trading_days(start_year, end_year)

    total_api_calls = len(ticker_list) * len(years)
    print(f"Years: {years[0]}-{years[-1]} ({len(years)} years)")
    print(f"API calls: ~{total_api_calls:,} ({len(ticker_list):,} tickers x {len(years)} years)")
    print(f"Workers: {n_workers}")
    print(flush=True)

    total_rows = 0
    total_errors = 0
    total_skipped = 0
    t_start = time.time()

    for t_idx, root in enumerate(ticker_list):
        if shutdown_requested:
            print("Shutdown: committing and exiting...", flush=True)
            conn.commit()
            break

        # Quick check: skip if ticker already has data
        existing_dates = get_ticker_dates(conn, root)
        if len(existing_dates) >= len(all_days) * 0.9:
            total_skipped += 1
            continue

        ticker_rows = 0
        ticker_errors = 0
        ticker_t0 = time.time()

        # Download ALL years in parallel (one API call per year)
        with ThreadPoolExecutor(max_workers=min(n_workers, len(years))) as pool:
            futures = {}
            for year in years:
                sd = year * 10000 + 101    # Jan 1
                ed = year * 10000 + 1231   # Dec 31
                futures[pool.submit(download_ticker_range, root, sd, ed)] = year

            batch_keys = []
            batch_eods = []

            for future in as_completed(futures):
                if shutdown_requested:
                    break
                try:
                    result = future.result()
                    if result is None:
                        ticker_errors += 1
                    elif result:
                        for contract_key, eod_tuple in result:
                            batch_keys.append(contract_key)
                            batch_eods.append(eod_tuple)
                except Exception:
                    ticker_errors += 1

        if shutdown_requested:
            break

        # Batch insert
        if batch_keys:
            id_map = get_or_create_contracts(conn, batch_keys)
            final_rows = []
            for key, eod_tuple in zip(batch_keys, batch_eods):
                if key in id_map:
                    cid = id_map[key]
                    final_rows.append((cid,) + eod_tuple)
            if final_rows:
                upsert_eod_rows(conn, final_rows)
                ticker_rows = len(final_rows)

        conn.commit()
        total_rows += ticker_rows
        total_errors += ticker_errors

        elapsed = time.time() - t_start
        ticker_elapsed = time.time() - ticker_t0
        tickers_done = t_idx + 1 - total_skipped
        if tickers_done > 0:
            rate = tickers_done / elapsed
            remaining = len(ticker_list) - t_idx - 1
            eta_h = remaining / rate / 3600 if rate > 0 else 0
        else:
            eta_h = 0

        print(f"[{t_idx+1:,}/{len(ticker_list):,}] {root:>5s}: "
              f"{ticker_rows:,} rows, "
              f"{ticker_elapsed:.1f}s | "
              f"Total: {total_rows:,} rows, {total_errors} errs"
              f"{f', ETA {eta_h:.1f}h' if eta_h > 0.01 else ''}",
              flush=True)

    conn.close()

    elapsed = time.time() - t_start
    print(f"\n{'='*60}")
    print(f"Download complete in {elapsed/3600:.1f}h")
    print(f"  Rows inserted: {total_rows:,}")
    print(f"  Tickers skipped (already done): {total_skipped:,}")
    print(f"  Errors: {total_errors}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download EOD option data from ThetaData v3")
    parser.add_argument("--year", type=int, help="Single year to download")
    parser.add_argument("--tickers", type=str, help="Comma-separated ticker list")
    parser.add_argument("--new-only", action="store_true",
                        help="Only download tickers not yet in eod_history")
    parser.add_argument("--workers", type=int, default=8,
                        help="Parallel workers per ticker (default: 8)")
    parser.add_argument("--test", action="store_true", help="Test mode: 3 tickers, 5 dates")
    args = parser.parse_args()

    start_year = args.year or 2016
    end_year = args.year or 2026

    tickers = None
    if args.tickers:
        tickers = [t.strip().upper() for t in args.tickers.split(",")]

    print("=" * 60)
    print(f"THETADATA v3 EOD DOWNLOAD: {start_year}-{end_year}")
    print("=" * 60, flush=True)

    download_all(
        start_year=start_year,
        end_year=end_year,
        tickers=tickers,
        new_only=args.new_only,
        n_workers=args.workers,
        test_mode=args.test,
    )
