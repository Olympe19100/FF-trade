"""
Liquidate ALL Positions — Close every IBKR option position.

Queries IBKR positions directly (not portfolio.json), then closes each
using execute_leg with IBKR OPRA pricing.

Usage:
    python tools/liquidate_all.py              # Dry run
    python tools/liquidate_all.py --live       # Execute
    python tools/liquidate_all.py --port 7497  # TWS instead of Gateway
"""

import sys
import asyncio
import argparse
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# Fix Python 3.14 asyncio
try:
    asyncio.get_event_loop()
except RuntimeError:
    asyncio.set_event_loop(asyncio.new_event_loop())

from ib_insync import IB

from core.config import DEFAULT_HOST, GW_PAPER, CLIENT_ID, get_logger
from core.execution import execute_leg

log = get_logger(__name__)


def main():
    parser = argparse.ArgumentParser(description="Liquidate all IBKR option positions")
    parser.add_argument("--port", type=int, default=GW_PAPER)
    parser.add_argument("--live", action="store_true", help="Execute (default: dry run)")
    parser.add_argument("--skip", default="", help="Comma-separated tickers to skip")
    args = parser.parse_args()

    ib = IB()
    log.info("Connecting to IBKR at %s:%d ...", DEFAULT_HOST, args.port)
    ib.connect(DEFAULT_HOST, args.port, clientId=CLIENT_ID + 5)
    log.info("Connected: %s", ib.isConnected())

    positions = ib.positions()
    option_positions = [p for p in positions if p.contract.secType == "OPT"]

    if not option_positions:
        log.info("No option positions to liquidate.")
        ib.disconnect()
        return

    log.info("=" * 60)
    log.info("LIQUIDATING ALL POSITIONS (%d options)", len(option_positions))
    log.info("=" * 60)

    for p in sorted(option_positions, key=lambda x: (x.contract.symbol, x.contract.strike)):
        c = p.contract
        side = "LONG" if p.position > 0 else "SHORT"
        avg_cost = getattr(p, "avgCost", 0) or 0
        log.info("  %6s %5s %s %s K=%7.1f x%-4d avgCost=$%.2f",
                 c.symbol, side, c.right, c.lastTradeDateOrContractMonth,
                 c.strike, abs(p.position), avg_cost)

    if not args.live:
        log.info("\n  DRY RUN — pass --live to execute")
        ib.disconnect()
        return

    # Skip orphans with no market data (micro-caps without OPRA quotes)
    SKIP_TICKERS = set()
    if args.skip:
        SKIP_TICKERS = set(t.strip().upper() for t in args.skip.split(","))
        log.info("  Skipping tickers: %s", ", ".join(sorted(SKIP_TICKERS)))

    log.info("\n  LIVE MODE — closing positions\n")

    filled = 0
    failed = 0
    skipped = 0

    for p in sorted(option_positions, key=lambda x: (x.contract.symbol, x.contract.strike)):
        c = p.contract
        qty = abs(int(p.position))
        action = "SELL" if p.position > 0 else "BUY"
        side = "LONG" if p.position > 0 else "SHORT"

        if c.symbol in SKIP_TICKERS:
            log.info("--- %s: SKIPPED (--skip) ---", c.symbol)
            skipped += 1
            continue

        log.info("--- %s %s %s %s K=%.0f x%d ---",
                 c.symbol, side, c.right, c.lastTradeDateOrContractMonth,
                 c.strike, qty)

        # Qualify the contract on IBKR
        try:
            ib.qualifyContracts(c)
            if c.conId == 0:
                log.error("  Cannot qualify contract")
                failed += 1
                continue
        except Exception as ex:
            log.error("  Cannot qualify: %s", ex)
            failed += 1
            continue

        # Use execute_leg (IBKR primary pricing)
        avg_cost = getattr(p, "avgCost", 0) or 0
        eodhd_mid = avg_cost / 100 if avg_cost > 0 else 0  # avgCost is per-share * 100

        try:
            ok, fill_px = execute_leg(ib, c, action, qty, eodhd_mid=eodhd_mid)
        except (ConnectionError, OSError) as ex:
            log.error("  Connection error: %s — reconnecting", ex)
            try:
                ib.disconnect()
                ib.sleep(3)
                ib.connect(DEFAULT_HOST, args.port, clientId=CLIENT_ID + 5)
                log.info("  Reconnected")
            except Exception:
                log.error("  Cannot reconnect — stopping")
                break
            failed += 1
            continue

        if ok:
            log.info("  => FILLED @ $%.2f", fill_px)
            filled += 1
        else:
            log.error("  => FAILED to close")
            failed += 1

        ib.sleep(2)

    log.info("=" * 60)
    log.info("LIQUIDATION COMPLETE: %d filled, %d failed out of %d",
             filled, failed, len(option_positions))
    log.info("=" * 60)
    ib.disconnect()


if __name__ == "__main__":
    main()
