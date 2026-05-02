"""
Liquidate All Positions — Close every active spread on IBKR

Connects to IBKR Gateway, iterates through portfolio.json,
and closes each position via execute_spread_close (combo-first, legs-fallback).

Usage:
    python tools/liquidate_all.py              # Dry run: show what would be closed
    python tools/liquidate_all.py --live       # Execute closes on IBKR
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

from core.config import (
    DEFAULT_HOST, GW_PAPER, CLIENT_ID, COMMISSION_LEG, CONTRACT_MULT,
    get_logger,
)
from core.portfolio import load_portfolio, save_portfolio, record_trade
from core.trader import (
    connect_ibkr, verify_paper,
    close_position_ibkr,
)

log = get_logger("liquidate")


def main():
    parser = argparse.ArgumentParser(description="Liquidate all active positions")
    parser.add_argument("--live", action="store_true", help="Execute real orders (default: dry run)")
    parser.add_argument("--port", type=int, default=GW_PAPER, help="IBKR port (default: 4002)")
    args = parser.parse_args()

    # ── Load portfolio ──
    portfolio = load_portfolio()
    active = [p for p in portfolio["positions"] if "exit_date" not in p]

    if not active:
        log.info("No active positions to liquidate.")
        return

    log.info("=" * 60)
    log.info("LIQUIDATE ALL — %d active positions", len(active))
    log.info("=" * 60)

    total_deployed = 0.0
    for p in active:
        ps = p.get("put_strike", p["strike"])
        log.info("  %s  CallK=%.0f  PutK=%.0f  x%d  %s  cost=$%.2f  deployed=$%.0f",
                 p["ticker"], p["strike"], ps,
                 p["contracts"], p["combo"],
                 p["cost_per_share"], p["total_deployed"])
        total_deployed += p["total_deployed"]

    log.info("  Total deployed: $%s", f"{total_deployed:,.0f}")

    if not args.live:
        log.info("")
        log.info("DRY RUN — add --live to execute.")
        return

    # ── Connect to IBKR ──
    log.info("")
    log.info("Connecting to IBKR on port %d...", args.port)
    ib = IB()
    ib.connect(DEFAULT_HOST, args.port, clientId=CLIENT_ID + 10, timeout=15)
    log.info("Connected: %s", ib.isConnected())

    acct = verify_paper(ib)
    if not acct:
        log.error("Not a paper account — aborting.")
        ib.disconnect()
        return

    log.info("Paper account: %s", acct)
    log.info("")

    # ── Close each position ──
    results = []
    for pos in active:
        ticker = pos["ticker"]
        log.info("─" * 50)
        log.info("CLOSING %s  x%d ...", ticker, pos["contracts"])

        result = close_position_ibkr(ib, pos)

        if result.get("success"):
            # Update portfolio state
            from datetime import datetime
            pos["exit_date"] = datetime.now().strftime("%Y-%m-%d")
            pos["exit_price"] = result["exit_price"]
            pos["pnl"] = result["pnl"]
            pos["return_pct"] = result["return_pct"]
            pos["close_method"] = result.get("method", "unknown")

            record_trade(pos, result["exit_price"], result["pnl"], result["return_pct"])

            log.info("  OK  exit=$%.2f  P&L=$%s  (%+.1f%%)  method=%s",
                     result["exit_price"], f"{result['pnl']:+,.2f}",
                     result["return_pct"] * 100, result.get("method"))
        else:
            log.error("  FAILED: %s", result.get("error", "unknown"))

        results.append({"ticker": ticker, **result})

    save_portfolio(portfolio)

    # ── Summary ──
    log.info("")
    log.info("=" * 60)
    log.info("LIQUIDATION SUMMARY")
    log.info("=" * 60)

    ok = [r for r in results if r.get("success")]
    fail = [r for r in results if not r.get("success")]
    total_pnl = sum(r.get("pnl", 0) for r in ok)

    for r in ok:
        log.info("  %s  exit=$%.2f  P&L=$%s  %s",
                 r["ticker"], r["exit_price"], f"{r['pnl']:+,.2f}", r.get("method"))
    for r in fail:
        log.info("  %s  FAILED: %s", r["ticker"], r.get("error", "?"))

    log.info("")
    log.info("  Closed: %d/%d", len(ok), len(results))
    log.info("  Total P&L: $%s", f"{total_pnl:+,.2f}")

    # ── Cleanup ──
    ib.disconnect()
    log.info("Disconnected.")


if __name__ == "__main__":
    main()
