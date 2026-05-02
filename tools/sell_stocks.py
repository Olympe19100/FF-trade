"""
Sell All Stocks — Liquidate every STK position on IBKR at market.

Usage:
    python tools/sell_stocks.py              # Dry run: show stock positions
    python tools/sell_stocks.py --live       # Execute MKT sell orders
    python tools/sell_stocks.py --port 7497  # TWS instead of Gateway
"""

import sys
import asyncio
import argparse
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

try:
    asyncio.get_event_loop()
except RuntimeError:
    asyncio.set_event_loop(asyncio.new_event_loop())

from ib_insync import IB

from core.config import DEFAULT_HOST, GW_PAPER, CLIENT_ID, get_logger
from core.trader import verify_paper, liquidate_stocks

log = get_logger("sell_stocks")


def main():
    parser = argparse.ArgumentParser(description="Sell all stock positions")
    parser.add_argument("--live", action="store_true", help="Execute MKT orders")
    parser.add_argument("--port", type=int, default=GW_PAPER, help="IBKR port (default: 4002)")
    parser.add_argument("--start-from", type=str, default=None,
                        help="Resume from this ticker alphabetically (skip earlier)")
    args = parser.parse_args()

    # ── Connect ──
    log.info("Connecting to IBKR on port %d...", args.port)
    ib = IB()
    ib.connect(DEFAULT_HOST, args.port, clientId=CLIENT_ID + 20, timeout=15)
    log.info("Connected: %s", ib.isConnected())

    acct = verify_paper(ib)
    if not acct:
        log.error("Not a paper account — aborting.")
        ib.disconnect()
        return

    # ── List stock positions ──
    items = ib.portfolio(acct)
    stocks = [i for i in items if i.contract.secType == "STK" and i.position != 0]

    if not stocks:
        log.info("No stock positions found.")
        ib.disconnect()
        return

    log.info("")
    log.info("%-6s  %8s  %10s  %10s  %10s", "Symbol", "Qty", "MktPrice", "MktValue", "UnrlzPnL")
    log.info("-" * 52)
    for s in stocks:
        log.info("%-6s  %8.0f  %10.2f  %10.2f  %10.2f",
                 s.contract.symbol, s.position,
                 s.marketPrice, s.marketValue, s.unrealizedPNL)

    log.info("")
    log.info("Total: %d stock positions", len(stocks))

    if not args.live:
        log.info("DRY RUN — add --live to sell all.")
        ib.disconnect()
        return

    # ── Filter by --start-from ──
    symbols_to_sell = None
    if args.start_from:
        start = args.start_from.upper()
        symbols_to_sell = [s.contract.symbol for s in stocks
                           if s.contract.symbol.upper() >= start]
        log.info("Resuming from %s: %d positions to process", start, len(symbols_to_sell))

    # ── Liquidate ──
    log.info("")
    count = len(symbols_to_sell) if symbols_to_sell else len(stocks)
    log.info("Selling %d positions at MKT...", count)
    results = liquidate_stocks(ib, acct, symbols=symbols_to_sell)

    log.info("")
    log.info("%-6s  %6s  %5s  %10s  %10s", "Symbol", "Action", "Qty", "FillPrice", "Status")
    log.info("-" * 45)
    for r in results:
        log.info("%-6s  %6s  %5.0f  %10.2f  %10s",
                 r["symbol"], r["action"], r["qty"], r["fill_price"], r["status"])

    filled = [r for r in results if r["filled"]]
    log.info("")
    log.info("Filled: %d/%d", len(filled), len(results))

    ib.disconnect()
    log.info("Done.")


if __name__ == "__main__":
    main()
