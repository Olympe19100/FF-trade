"""
Liquidate Orphan Positions — Close IBKR positions not in portfolio.json

Uses plain LMT orders (no Adaptive algo) with progressive walk to avoid
IBKR paper-account modify restrictions.  Falls back to MKT if walk exhausted.

Usage:
    python tools/liquidate_orphans.py              # Dry run
    python tools/liquidate_orphans.py --live       # Execute
    python tools/liquidate_orphans.py --port 7497  # TWS instead of Gateway
"""

import sys
import asyncio
import argparse
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# Fix Python 3.14 asyncio
try:
    asyncio.get_event_loop()
except RuntimeError:
    asyncio.set_event_loop(asyncio.new_event_loop())

from ib_insync import IB, LimitOrder, MarketOrder

from core.config import (
    DEFAULT_HOST, GW_PAPER, CLIENT_ID, CLOSE_PAUSE,
    SLICE_THRESHOLD, SLICE_SIZE,
    get_logger,
)

log = get_logger("liquidate_orphans")

WALK_STEPS    = 12       # Max price walk iterations
INITIAL_WAIT  = 15       # Seconds at initial price
WALK_WAIT     = 10       # Seconds per walk step
MKT_TIMEOUT   = 30       # Seconds for final MKT fallback


def _get_price(ib, contract) -> tuple[float, float, float]:
    """Get bid/ask/mid via ThetaData WS first, IBKR reqMktData fallback."""
    # ThetaData WS
    try:
        from core.theta_ws import theta_ws_get_leg_price
        bid, ask, mid = theta_ws_get_leg_price(contract)
        if mid > 0:
            return bid, ask, mid
    except Exception:
        pass

    # IBKR reqMktData
    import numpy as np
    ib.reqMktData(contract, "", False, False)
    ib.sleep(3)
    tk = ib.ticker(contract)
    if tk:
        b = float(tk.bid) if tk.bid and tk.bid > 0 and not np.isinf(tk.bid) else 0
        a = float(tk.ask) if tk.ask and tk.ask > 0 and not np.isinf(tk.ask) else 0
        if b > 0 and a > 0:
            return b, a, round((b + a) / 2, 2)
        if tk.last and tk.last > 0:
            return 0, 0, float(tk.last)
    try:
        ib.cancelMktData(contract)
    except Exception:
        pass
    return 0, 0, 0


def _wait_fill(ib, trade, timeout):
    """Wait for fill, return status string."""
    t0 = time.time()
    while time.time() - t0 < timeout:
        ib.sleep(0.5)
        status = trade.orderStatus.status
        if status == "Filled":
            return "Filled"
        if status in ("Cancelled", "ApiCancelled", "Inactive"):
            return status
    return "Timeout"


def _close_position(ib, contract, action, qty, portfolio_price):
    """Close a single position using plain LMT walk + MKT fallback.

    Returns (filled_qty, avg_price).
    """
    ib.qualifyContracts(contract)

    bid, ask, mid = _get_price(ib, contract)

    # Use portfolio price as fallback
    if mid <= 0 and portfolio_price > 0:
        log.info("    No live quote — using portfolio price $%.2f", portfolio_price)
        mid = portfolio_price
        bid = ask = mid

    if mid <= 0:
        log.warning("    No price at all — sending MKT order")
        order = MarketOrder(action, qty)
        order.tif = "DAY"
        trade = ib.placeOrder(contract, order)
        result = _wait_fill(ib, trade, MKT_TIMEOUT)
        if result == "Filled":
            return qty, trade.orderStatus.avgFillPrice
        ib.cancelOrder(order)
        ib.sleep(1)
        return 0, 0.0

    spread = round(ask - bid, 4) if ask > bid > 0 else 0.0
    has_real_spread = spread > 0.01

    # Initial price: mid if we have real spread, else portfolio price
    if action == "SELL":
        limit_price = round(mid, 2) if has_real_spread else round(mid * 0.95, 2)
    else:
        limit_price = round(mid, 2) if has_real_spread else round(mid * 1.05, 2)

    # Walk step: fraction of spread, or 2% of mid
    if has_real_spread:
        walk_step = max(0.01, round(spread / 4, 2))
    else:
        walk_step = max(0.01, round(mid * 0.02, 2))

    log.info("    %s x%d LMT $%.2f [bid=%.2f ask=%.2f spread=%.2f step=%.2f]",
             action, qty, limit_price, bid, ask, spread, walk_step)

    # Place plain LMT order (no Adaptive)
    order = LimitOrder(action, qty, limit_price)
    order.tif = "DAY"
    trade = ib.placeOrder(contract, order)

    result = _wait_fill(ib, trade, INITIAL_WAIT)
    if result == "Filled":
        px = trade.orderStatus.avgFillPrice
        log.info("    -> FILLED @ $%.2f (initial)", px)
        return qty, px
    if result in ("Cancelled", "ApiCancelled", "Inactive"):
        log.error("    -> REJECTED (%s)", result)
        return 0, 0.0

    # Cancel initial order before walking (avoids Error 103 on modify)
    ib.cancelOrder(order)
    ib.sleep(1)

    # Progressive walk: cancel + new order at each step (paper account safe)
    for step in range(WALK_STEPS):
        if action == "SELL":
            limit_price = round(limit_price - walk_step, 2)
            if bid > 0 and limit_price < bid:
                limit_price = round(bid, 2)
        else:
            limit_price = round(limit_price + walk_step, 2)
            if ask > 0 and limit_price > ask:
                limit_price = round(ask, 2)

        limit_price = max(0.01, limit_price)

        log.info("    Walk %d/%d: LMT $%.2f", step + 1, WALK_STEPS, limit_price)
        new_order = LimitOrder(action, qty, limit_price)
        new_order.tif = "DAY"
        trade = ib.placeOrder(contract, new_order)

        result = _wait_fill(ib, trade, WALK_WAIT)
        if result == "Filled":
            px = trade.orderStatus.avgFillPrice
            log.info("    -> FILLED @ $%.2f (walk %d)", px, step + 1)
            return qty, px
        if result in ("Cancelled", "ApiCancelled", "Inactive"):
            log.error("    -> REJECTED during walk (%s)", result)
            return 0, 0.0

        # Cancel before next step
        ib.cancelOrder(new_order)
        ib.sleep(1)

    # Walk exhausted — try MKT as last resort
    log.warning("    Walk exhausted — trying MKT fallback")

    mkt_order = MarketOrder(action, qty)
    mkt_order.tif = "DAY"
    trade = ib.placeOrder(contract, mkt_order)
    result = _wait_fill(ib, trade, MKT_TIMEOUT)
    if result == "Filled":
        px = trade.orderStatus.avgFillPrice
        log.info("    -> FILLED MKT @ $%.2f", px)
        return qty, px

    ib.cancelOrder(mkt_order)
    ib.sleep(1)
    log.error("    -> FAILED even with MKT")
    return 0, 0.0


def main():
    parser = argparse.ArgumentParser(description="Liquidate orphan IBKR positions")
    parser.add_argument("--live", action="store_true", help="Execute real orders (default: dry run)")
    parser.add_argument("--port", type=int, default=GW_PAPER, help="IBKR port (default: 4002)")
    args = parser.parse_args()

    # ── Connect ──
    ib = IB()
    log.info("Connecting to IBKR on port %d...", args.port)
    ib.connect(DEFAULT_HOST, args.port, clientId=CLIENT_ID + 11, timeout=15)
    log.info("Connected: %s", ib.isConnected())

    # ── Read live positions ──
    items = ib.portfolio()
    if not items:
        log.info("No positions on IBKR.")
        ib.disconnect()
        return

    log.info("=" * 60)
    log.info("ORPHAN POSITIONS — %d items on IBKR", len(items))
    log.info("=" * 60)

    total_unrealized = 0.0
    for item in items:
        c = item.contract
        direction = "LONG" if item.position > 0 else "SHORT"
        log.info("  %s %s %s%.0f exp=%s  qty=%d  mktPrice=$%.2f  mktVal=$%.0f  unrealPnl=$%.0f",
                 c.symbol, direction, c.right, c.strike,
                 c.lastTradeDateOrContractMonth,
                 int(item.position), item.marketPrice,
                 item.marketValue, item.unrealizedPNL)
        total_unrealized += item.unrealizedPNL

    log.info("  Total unrealized P&L: $%s", f"{total_unrealized:+,.2f}")

    if not args.live:
        log.info("")
        log.info("DRY RUN — add --live to execute closes.")
        ib.disconnect()
        return

    # ── Close each position ──
    log.info("")
    log.info("EXECUTING CLOSES...")
    log.info("")

    results = []
    for i, item in enumerate(items):
        c = item.contract
        qty = int(item.position)
        if qty == 0:
            continue

        action = "SELL" if qty > 0 else "BUY"
        abs_qty = abs(qty)
        ticker = c.symbol

        log.info("-" * 50)
        log.info("CLOSING %s %s K=%.0f exp=%s  %s x%d",
                 ticker, c.right, c.strike,
                 c.lastTradeDateOrContractMonth, action, abs_qty)

        # Slice large orders
        remaining = abs_qty
        total_fill_value = 0.0
        total_filled = 0

        while remaining > 0:
            chunk = min(SLICE_SIZE, remaining) if abs_qty > SLICE_THRESHOLD else remaining

            filled, fill_px = _close_position(
                ib, c, action, chunk, item.marketPrice
            )

            if filled:
                total_fill_value += fill_px * chunk
                total_filled += chunk
                remaining -= chunk
                log.info("  Slice filled %d @ $%.2f (%d remaining)", chunk, fill_px, remaining)

                if remaining > 0:
                    pause = CLOSE_PAUSE // 2  # shorter pause for same ticker
                    log.info("  Slice pause %ds...", pause)
                    ib.sleep(pause)
            else:
                log.error("  FAILED to fill %d contracts — stopping this position", chunk)
                break

        avg_fill = total_fill_value / total_filled if total_filled > 0 else 0
        entry_cost = item.averageCost / 100

        result = {
            "ticker": ticker,
            "right": c.right,
            "strike": float(c.strike),
            "exp": c.lastTradeDateOrContractMonth,
            "qty": qty,
            "filled": total_filled,
            "avg_fill": round(avg_fill, 2),
            "entry_cost": round(entry_cost, 2),
            "success": total_filled == abs_qty,
        }
        results.append(result)

        if i < len(items) - 1 and total_filled > 0:
            log.info("  Stagger pause %ds...", CLOSE_PAUSE)
            ib.sleep(CLOSE_PAUSE)

    # ── Summary ──
    log.info("")
    log.info("=" * 60)
    log.info("LIQUIDATION SUMMARY")
    log.info("=" * 60)

    ok = [r for r in results if r["success"]]
    fail = [r for r in results if not r["success"]]

    for r in ok:
        log.info("  %s %s%.0f exp=%s: CLOSED %d @ $%.2f",
                 r["ticker"], r["right"], r["strike"], r["exp"],
                 r["filled"], r["avg_fill"])
    for r in fail:
        log.info("  %s %s%.0f exp=%s: PARTIAL %d/%d filled",
                 r["ticker"], r["right"], r["strike"], r["exp"],
                 r["filled"], abs(r["qty"]))

    log.info("")
    log.info("  Closed: %d/%d", len(ok), len(results))
    if fail:
        log.warning("  %d positions NOT fully closed — manual intervention needed", len(fail))

    ib.disconnect()
    log.info("Disconnected.")


if __name__ == "__main__":
    main()
