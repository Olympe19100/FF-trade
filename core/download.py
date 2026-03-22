"""
Download DAILY EOD option data for all S&P 500 tickers (2016-2026).
Uses existing Theta Data infrastructure from pyc modules.
Resumable: skips dates already in eod_history.

Usage:
    python download_daily.py              # Full run 2016-2026
    python download_daily.py --year 2024  # Single year
    python download_daily.py --test       # Test: 2 dates, 5 tickers
"""

import sys
import importlib.util
import sqlite3
import time
from datetime import datetime, timedelta
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import signal

# ── Load existing pyc modules ──────────────────────────────────────────
sys.path.insert(0, r"C:\Users\ANTEC MSI\Desktop\pro\Option trading")
for mod_name in ["config", "tickers", "database", "downloader"]:
    pyc = rf"C:\Users\ANTEC MSI\Desktop\pro\Option trading\__pycache__\{mod_name}.cpython-314.pyc"
    spec = importlib.util.spec_from_file_location(mod_name, pyc)
    m = importlib.util.module_from_spec(spec)
    sys.modules[mod_name] = m
    spec.loader.exec_module(m)

from config import MAX_DTE_DOWNLOAD
from database import (create_schema, bulk_get_or_create_contracts,
                       upsert_eod_history)
from downloader import _download_one_ticker, SharedRateLimiter

DB_PATH = Path(r"C:\Users\ANTEC MSI\Desktop\pro\Option trading\sp500_options.db")

# ── Graceful shutdown ──────────────────────────────────────────────────
shutdown_requested = False

def signal_handler(sig, frame):
    global shutdown_requested
    print("\nShutdown requested, finishing current batch...", flush=True)
    shutdown_requested = True

signal.signal(signal.SIGINT, signal_handler)

# ── US market holidays ─────────────────────────────────────────────────

def us_market_holidays(year):
    """Return set of (month, day) for fixed US market holidays.
    Approximate — covers the big ones that never move."""
    holidays = set()
    holidays.add((1, 1))   # New Year's Day
    holidays.add((6, 19))  # Juneteenth (from 2021, but skip anyway)
    holidays.add((7, 4))   # Independence Day
    holidays.add((12, 25)) # Christmas
    return holidays

def is_likely_holiday(d):
    """Check if a weekday is a known US market holiday or observed holiday."""
    fixed = us_market_holidays(d.year)
    if (d.month, d.day) in fixed:
        return True
    # Observed: if holiday falls on Saturday, Friday off; Sunday, Monday off
    if d.weekday() == 4:  # Friday
        sat = d + timedelta(days=1)
        if (sat.month, sat.day) in fixed:
            return True
    if d.weekday() == 0:  # Monday
        sun = d - timedelta(days=1)
        if (sun.month, sun.day) in fixed:
            return True
    # MLK Day: 3rd Monday of January
    if d.month == 1 and d.weekday() == 0 and 15 <= d.day <= 21:
        return True
    # Presidents Day: 3rd Monday of February
    if d.month == 2 and d.weekday() == 0 and 15 <= d.day <= 21:
        return True
    # Good Friday: hard to compute, skip (API returns 472 = handled)
    # Memorial Day: last Monday of May
    if d.month == 5 and d.weekday() == 0 and d.day >= 25:
        return True
    # Labor Day: 1st Monday of September
    if d.month == 9 and d.weekday() == 0 and d.day <= 7:
        return True
    # Thanksgiving: 4th Thursday of November
    if d.month == 11 and d.weekday() == 3 and 22 <= d.day <= 28:
        return True
    return False

# ── Generate trading days ──────────────────────────────────────────────

def generate_trading_days(start_year=2016, end_year=2026):
    """Generate US trading days (weekdays minus known holidays)."""
    days = []
    d = datetime(start_year, 1, 1)
    end = datetime(end_year, 12, 31)
    while d <= end:
        if d.weekday() < 5 and not is_likely_holiday(d):
            days.append(int(d.strftime("%Y%m%d")))
        d += timedelta(days=1)
    return days


def get_already_downloaded_dates(conn):
    """Get set of dates already in eod_history."""
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT date FROM eod_history")
    return {r[0] for r in cur.fetchall()}


def get_active_tickers(conn):
    """Get list of active tickers."""
    cur = conn.cursor()
    cur.execute("SELECT root FROM tickers WHERE is_active = 1 ORDER BY root")
    return [r[0] for r in cur.fetchall()]


# ── Main download loop ─────────────────────────────────────────────────

def download_daily(start_year=2016, end_year=2026, max_dte=150,
                   n_workers=16, test_mode=False):
    """Download daily EOD option data for all active tickers."""
    conn = sqlite3.connect(str(DB_PATH))
    create_schema(conn)

    tickers = get_active_tickers(conn)
    if not tickers:
        print("No active tickers. Run 'python main.py tickers --sync' first.")
        return

    if test_mode:
        tickers = tickers[:5]
        print(f"TEST MODE: {len(tickers)} tickers")

    all_days = generate_trading_days(start_year, end_year)
    existing_dates = get_already_downloaded_dates(conn)

    todo_days = [d for d in all_days if d not in existing_dates]

    if test_mode:
        todo_days = todo_days[:2]

    print(f"Trading days {start_year}-{end_year}: {len(all_days):,}")
    print(f"Already in DB: {len(existing_dates):,} dates")
    print(f"To download: {len(todo_days):,} dates")
    print(f"Tickers: {len(tickers):,}")
    total_calls = len(todo_days) * len(tickers)
    print(f"Estimated API calls: {total_calls:,}")
    print(f"Workers: {n_workers}", flush=True)

    if not todo_days:
        print("Nothing to download!")
        conn.close()
        return

    # Each worker has its own ThetaClient with its own HTTP connection.
    # The rate limiter is shared but the API is local (127.0.0.1),
    # so we can be very aggressive with pacing.
    rate_limiter = SharedRateLimiter(delay=0.005)

    rows_inserted = 0
    no_data_dates = 0
    real_errors = 0
    api_calls = 0
    t_start = time.time()

    for date_idx, date_int in enumerate(todo_days):
        if shutdown_requested:
            print("Shutdown: committing and exiting...", flush=True)
            conn.commit()
            break

        # Progress display
        elapsed = time.time() - t_start
        if api_calls > 100:
            rate = api_calls / elapsed
            remaining = (len(todo_days) - date_idx) * len(tickers)
            eta_h = remaining / rate / 3600
        else:
            rate = 0
            eta_h = 0

        print(f"[{date_idx+1}/{len(todo_days)}] {date_int} "
              f"| {rows_inserted:,} rows "
              f"| {no_data_dates} holidays, {real_errors} errs "
              f"| {rate:.0f} calls/s"
              f"{f', ETA {eta_h:.1f}h' if eta_h > 0 else ''}",
              flush=True)

        # Download all tickers in parallel
        all_contract_keys = []
        all_eod_rows = []
        date_has_data = False
        date_errors = 0

        with ThreadPoolExecutor(max_workers=n_workers) as pool:
            futures = {
                pool.submit(_download_one_ticker, root, date_int,
                           max_dte, rate_limiter): root
                for root in tickers
            }

            for future in as_completed(futures):
                root = futures[future]
                try:
                    _, contract_keys, eod_rows, error = future.result()
                    api_calls += 1

                    if error:
                        # BUG FIX: "No data found" (HTTP 472) = not an error,
                        # it's a holiday or no options for this ticker
                        if "No data" in str(error) or "472" in str(error):
                            pass  # Silently skip
                        else:
                            date_errors += 1
                    else:
                        if contract_keys:
                            date_has_data = True
                            all_contract_keys.extend(contract_keys)
                            all_eod_rows.extend(eod_rows)
                except Exception:
                    date_errors += 1

                if shutdown_requested:
                    break

        if shutdown_requested:
            break

        real_errors += date_errors

        if not date_has_data:
            # Entire date had no data = holiday or market closure
            no_data_dates += 1
            continue

        # Batch insert into DB
        if all_contract_keys:
            id_map = bulk_get_or_create_contracts(conn, all_contract_keys)
            final_rows = []
            for key, eod_tuple in zip(all_contract_keys, all_eod_rows):
                if key in id_map:
                    cid = id_map[key]
                    final_rows.append((cid,) + eod_tuple)

            if final_rows:
                upsert_eod_history(conn, final_rows)
                rows_inserted += len(final_rows)

        # Commit every date for resumability
        conn.commit()

    conn.close()

    elapsed = time.time() - t_start
    print(f"\n{'='*60}")
    print(f"Download complete in {elapsed/3600:.1f}h")
    print(f"  Rows inserted: {rows_inserted:,}")
    print(f"  Holidays/no-data: {no_data_dates}")
    print(f"  Real errors: {real_errors}")
    print(f"  API calls: {api_calls:,}")
    if elapsed > 0:
        print(f"  Rate: {api_calls/elapsed:.0f} calls/sec")


if __name__ == "__main__":
    test_mode = "--test" in sys.argv

    start_year = 2016
    end_year = 2026
    if "--year" in sys.argv:
        idx = sys.argv.index("--year")
        if idx + 1 < len(sys.argv):
            y = int(sys.argv[idx + 1])
            start_year = y
            end_year = y

    print("=" * 60)
    print(f"DAILY EOD DOWNLOAD: {start_year}-{end_year}")
    print("=" * 60, flush=True)

    download_daily(
        start_year=start_year,
        end_year=end_year,
        n_workers=16,
        test_mode=test_mode,
    )
