"""
Add tickers to the database for option data download.

Scans ThetaData for ALL option roots, filters by historical liquidity
(number of expirations as proxy), and adds them to the database.

Usage:
    python tools/add_tickers.py                     # Scan ThetaData, add liquid tickers (>=200 exps)
    python tools/add_tickers.py --min-exp 100       # Lower threshold = more tickers
    python tools/add_tickers.py --min-exp 50        # Even more tickers (less liquid)
    python tools/add_tickers.py --list TSLA,AMD     # Specific tickers only
    python tools/add_tickers.py --dry-run           # Show what would be added
    python tools/add_tickers.py --workers 32        # Faster scanning
"""

import sys
import sqlite3
import argparse
import requests
import time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from core.config import DB, THETADATA_URL, ensure_theta_terminal


def get_existing_tickers(conn):
    """Get set of tickers already in the database."""
    cur = conn.cursor()
    cur.execute("SELECT root FROM tickers")
    return {r[0] for r in cur.fetchall()}


def get_all_option_roots():
    """Fetch ALL option roots from ThetaData v3 API."""
    r = requests.get(
        f"{THETADATA_URL}/v3/option/list/symbols?format=json",
        timeout=30,
    )
    r.raise_for_status()
    data = r.json()
    symbols = [item["symbol"] for item in data["response"]]
    # Filter: pure alpha, 1-5 chars (no warrants, units, etc.)
    clean = sorted(set(s for s in symbols if s.isalpha() and 1 <= len(s) <= 5))
    return clean


def count_expirations(ticker):
    """Count historical expirations for a ticker (proxy for liquidity depth)."""
    try:
        r = requests.get(
            f"{THETADATA_URL}/v3/option/list/expirations",
            params={"symbol": ticker, "format": "json"},
            timeout=5,
        )
        if r.status_code == 200:
            data = r.json()
            return len(data.get("response", []))
    except Exception:
        pass
    return 0


def scan_liquid_tickers(roots, min_expirations=200, n_workers=16):
    """Scan all option roots and filter by minimum expirations."""
    print(f"Scanning {len(roots):,} option roots for liquidity "
          f"(>= {min_expirations} expirations)...")
    print(f"Workers: {n_workers}")

    results = {}
    t0 = time.time()

    with ThreadPoolExecutor(max_workers=n_workers) as pool:
        futures = {pool.submit(count_expirations, r): r for r in roots}
        done = 0
        for future in as_completed(futures):
            root = futures[future]
            try:
                n_exp = future.result()
                if n_exp >= min_expirations:
                    results[root] = n_exp
            except Exception:
                pass
            done += 1
            if done % 500 == 0:
                elapsed = time.time() - t0
                rate = done / elapsed
                eta = (len(roots) - done) / rate
                print(f"  [{done:,}/{len(roots):,}] "
                      f"liquid: {len(results):,} | "
                      f"{rate:.0f}/s | ETA {eta:.0f}s")

    elapsed = time.time() - t0
    print(f"Scan complete in {elapsed:.0f}s — "
          f"{len(results):,} liquid tickers found")
    return results


def add_tickers(conn, tickers, dry_run=False):
    """Add tickers to the database."""
    existing = get_existing_tickers(conn)
    new_tickers = sorted(set(tickers) - existing)

    if not new_tickers:
        print("No new tickers to add (all already in DB).")
        return 0

    print(f"\nNew tickers to add: {len(new_tickers)}")
    # Show all in groups of 20
    for i in range(0, len(new_tickers), 20):
        chunk = new_tickers[i:i+20]
        print(f"  {', '.join(chunk)}")

    if dry_run:
        print("\n[DRY RUN] No changes made.")
        return len(new_tickers)

    cur = conn.cursor()
    added = 0
    for ticker in new_tickers:
        try:
            cur.execute(
                "INSERT OR IGNORE INTO tickers (root, is_active) VALUES (?, 1)",
                (ticker,),
            )
            added += 1
        except Exception as e:
            print(f"  Error adding {ticker}: {e}")

    conn.commit()
    print(f"\nAdded {added} new tickers to database.")
    return added


def main():
    parser = argparse.ArgumentParser(
        description="Scan ThetaData for all liquid option roots and add to DB"
    )
    parser.add_argument("--min-exp", type=int, default=200,
                        help="Minimum historical expirations (default: 200, "
                             "~200 = traded consistently for years)")
    parser.add_argument("--list", type=str,
                        help="Comma-separated ticker list (skip scan)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would be added without writing")
    parser.add_argument("--workers", type=int, default=16,
                        help="Parallel workers for scanning (default: 16)")
    args = parser.parse_args()

    conn = sqlite3.connect(str(DB))
    existing = get_existing_tickers(conn)
    print(f"Current tickers in DB: {len(existing)}")

    if args.list:
        tickers = [t.strip().upper() for t in args.list.split(",")]
        print(f"User-specified: {len(tickers)} tickers")
        new_only = sorted(set(tickers) - existing)

    else:
        print("Connecting to ThetaData...")
        ensure_theta_terminal()

        # Step 1: Get all option roots
        all_roots = get_all_option_roots()
        print(f"ThetaData option roots: {len(all_roots):,}")

        # Step 2: Filter out already-in-DB
        to_scan = sorted(set(all_roots) - existing)
        print(f"Already in DB: {len(set(all_roots) & existing)}")
        print(f"New to scan: {len(to_scan):,}")

        if not to_scan:
            print("All option roots already in DB!")
            conn.close()
            return

        # Step 3: Scan for liquidity
        liquid = scan_liquid_tickers(
            to_scan,
            min_expirations=args.min_exp,
            n_workers=args.workers,
        )

        # Sort by expiration count (most liquid first)
        ranked = sorted(liquid.items(), key=lambda x: -x[1])
        print("\nTop 30 most liquid new tickers:")
        for root, n_exp in ranked[:30]:
            print(f"  {root:>5s}: {n_exp:,} expirations")

        new_only = [r for r, _ in ranked]

    print(f"\nTotal new: {len(new_only)}")
    print(f"DB after add: {len(existing) + len(new_only)}")

    if new_only:
        add_tickers(conn, new_only, dry_run=args.dry_run)

    conn.close()

    if not args.dry_run and new_only:
        print("\nNext step: download option data for new tickers:")
        print("  python core/download.py")
        print("  python core/download.py --year 2024  # Single year")


if __name__ == "__main__":
    main()
