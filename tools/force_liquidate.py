"""
Force Liquidate ALL IBKR Positions — Parallel LMT + Walk + ThetaData

Reads positions directly from ib.portfolio() and closes everything
using LMT orders with progressive walk (never MarketOrder).

Pricing waterfall (per position):
  1. ThetaData REST snapshot  (primary — batch per ticker)
  2. IBKR reqMktData           (fallback)
  3. Portfolio marketPrice     (last resort)

Execution strategy (based on Muravyev & Pearson 2020, Almgren & Chriss 2000):
  1. Cancel all pending orders
  2. Fetch ALL quotes upfront via ThetaData batch
  3. Phase 1: Place ALL long SELL orders simultaneously at mid
  4. Walk ALL orders in parallel (single loop, WALK_WAIT between steps)
  5. Phase 2: Place ALL short BUY orders simultaneously (margin freed)
  6. Walk ALL in parallel
  7. Leave unfilled orders RESTING at final walked price (TIF=DAY)
  8. Phase 3: Retry margin-rejected orders

Usage:
    python tools/force_liquidate.py              # Dry run
    python tools/force_liquidate.py --live       # Execute
    python tools/force_liquidate.py --port 7497  # TWS instead of Gateway
"""

import sys
import asyncio
import argparse
import time
import numpy as np
import requests
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# Fix Python 3.14 asyncio
try:
    asyncio.get_event_loop()
except RuntimeError:
    asyncio.set_event_loop(asyncio.new_event_loop())

from ib_insync import IB, LimitOrder, TagValue

from core.config import (
    DEFAULT_HOST, GW_PAPER, CLIENT_ID,
    THETADATA_URL,
    get_logger,
)
from core.trader import verify_paper

log = get_logger("force_liquidate")

# Walk parameters for liquidation (more aggressive than entry)
WALK_STEP = 0.05       # $/share per step
WALK_WAIT = 10          # Seconds per price level
WALK_MAX  = 15          # Max walk steps


# ═══════════════════════════════════════════════════════════════
#  Pricing: ThetaData REST batch → IBKR → Portfolio
# ═══════════════════════════════════════════════════════════════

def fetch_theta_quotes_batch(positions):
    """Batch-fetch quotes from ThetaData REST for all positions.

    Groups by (symbol, expiration) to minimize API calls.
    Returns dict: (symbol, exp, strike, right) → (bid, ask, mid).
    """
    quotes = {}
    right_map = {"C": "CALL", "P": "PUT"}

    # Group positions by (symbol, expiration)
    groups = defaultdict(list)
    for p in positions:
        c = p.contract
        exp = str(c.lastTradeDateOrContractMonth).replace("-", "")[:8]
        groups[(c.symbol, exp)].append((c.strike, c.right))

    for (symbol, exp), strikes in groups.items():
        try:
            url = (f"{THETADATA_URL}/v3/option/snapshot/quote"
                   f"?symbol={symbol}&expiration={exp}&format=json")
            resp = requests.get(url, timeout=10)
            if resp.status_code != 200:
                continue

            data = resp.json()
            records = data.get("response", []) if isinstance(data, dict) else data

            for item in records:
                contract = item.get("contract", {})
                item_strike = float(contract.get("strike", 0))
                item_right_full = contract.get("right", "")

                # Check if this matches any of our positions
                for strike, right in strikes:
                    theta_right = right_map.get(right, right)
                    if abs(item_strike - strike) < 0.01 and item_right_full == theta_right:
                        d = item.get("data", [])
                        if not d:
                            continue
                        row = d[0] if isinstance(d, list) and d else d
                        if isinstance(row, dict):
                            bid = float(row.get("bid", 0) or 0)
                            ask = float(row.get("ask", 0) or 0)
                        else:
                            bid = float(row[0]) if len(row) > 0 and row[0] else 0.0
                            ask = float(row[2]) if len(row) > 2 and row[2] else 0.0
                        mid = round((bid + ask) / 2, 2) if bid > 0 and ask > 0 else 0.0
                        quotes[(symbol, exp, strike, right)] = (bid, ask, mid)
        except Exception as ex:
            log.debug("  ThetaData REST batch failed for %s/%s: %s", symbol, exp, ex)

    return quotes


def get_ibkr_quote(ib, contract):
    """Get bid/ask/mid from IBKR market data snapshot."""
    ib.reqMktData(contract, "", False, False)
    ib.sleep(2)
    tk = ib.ticker(contract)
    bid = ask = mid = 0.0
    if tk:
        b = tk.bid
        a = tk.ask
        bid = float(b) if b and b > 0 and not np.isinf(b) else 0
        ask = float(a) if a and a > 0 and not np.isinf(a) else 0
        if bid > 0 and ask > 0:
            mid = round((bid + ask) / 2, 2)
    ib.cancelMktData(contract)
    return bid, ask, mid


def resolve_quote(ib, p, theta_quotes):
    """Resolve a quote for a single position using the pricing waterfall.

    Returns (bid, ask, mid, source).
    """
    c = p.contract
    exp = str(c.lastTradeDateOrContractMonth).replace("-", "")[:8]
    key = (c.symbol, exp, c.strike, c.right)

    # 1. ThetaData REST (already fetched in batch)
    if key in theta_quotes:
        bid, ask, mid = theta_quotes[key]
        if mid > 0:
            return bid, ask, mid, "ThetaREST"

    # 2. IBKR reqMktData
    bid, ask, mid = get_ibkr_quote(ib, c)
    if mid > 0:
        return bid, ask, mid, "IBKR"

    # 3. Portfolio marketPrice (last resort)
    mkt_price = abs(p.marketPrice) if hasattr(p, 'marketPrice') and p.marketPrice else 0.0
    if mkt_price > 0:
        return 0.0, 0.0, round(mkt_price, 2), "Portfolio"

    return 0.0, 0.0, 0.0, "None"


# ═══════════════════════════════════════════════════════════════
#  Parallel order placement + walk
# ═══════════════════════════════════════════════════════════════

def place_all_orders(ib, positions, theta_quotes):
    """Place LMT orders for all positions at mid price simultaneously.

    Returns list of active order dicts:
        [{contract, trade, order, action, qty, bid, ask, mid, limit_price,
          walk_limit, symbol, status}]
    """
    active = []

    for p in positions:
        c = p.contract
        qty = abs(int(p.position))
        action = "SELL" if p.position > 0 else "BUY"
        c.exchange = "SMART"

        bid, ask, mid, source = resolve_quote(ib, p, theta_quotes)

        # Determine starting limit price
        if mid > 0:
            limit_price = round(mid, 2)
        else:
            log.warning("  SKIP %s %s K=%.0f — no price from any source",
                        c.symbol, c.right, c.strike)
            continue

        # Compute walk limit
        if bid > 0 and ask > 0:
            if action == "SELL":
                walk_limit = round(bid * 0.95, 2)  # 5% below bid
            else:
                walk_limit = round(ask * 1.05, 2)   # 5% above ask
        else:
            if action == "SELL":
                walk_limit = round(limit_price * 0.85, 2)
            else:
                walk_limit = round(limit_price * 1.15, 2)

        log.info("  %s %s %s K=%.0f %s x%d  LMT $%.2f [%s] (walk→$%.2f)",
                 action, c.symbol, c.right, c.strike,
                 c.lastTradeDateOrContractMonth, qty,
                 limit_price, source, walk_limit)

        order = LimitOrder(action, qty, limit_price)
        order.algoStrategy = "Adaptive"
        order.algoParams = [TagValue("adaptivePriority", "Urgent")]
        order.outsideRth = False
        order.tif = "DAY"

        trade = ib.placeOrder(c, order)

        active.append({
            "contract": c,
            "trade": trade,
            "order": order,
            "action": action,
            "qty": qty,
            "bid": bid,
            "ask": ask,
            "mid": mid,
            "limit_price": limit_price,
            "walk_limit": walk_limit,
            "symbol": c.symbol,
            "status": "active",
            "steps": 0,
        })

    return active


def walk_all_orders(ib, active_orders):
    """Walk all active orders in parallel.

    Adjusts all order prices every WALK_WAIT seconds.
    Orders that fill, get rejected, or exhaust their walk are removed.

    Returns (filled_list, resting_list, failed_list).
    """
    filled = []
    resting = []
    failed = []

    # Initial wait at mid price for all orders
    log.info("  Waiting %ds at initial prices (%d orders)...", WALK_WAIT, len(active_orders))
    t0 = time.time()
    while time.time() - t0 < WALK_WAIT:
        ib.sleep(0.5)
        # Check if any orders completed early
        for o in active_orders:
            if o["status"] != "active":
                continue
            if o["trade"].orderStatus.status == "Filled":
                px = o["trade"].orderStatus.avgFillPrice
                o["status"] = "filled"
                filled.append(o)
                log.info("    FILLED %s %s x%d @ $%.2f",
                         o["action"], o["symbol"], o["qty"], px)
            elif o["trade"].orderStatus.status in ("Cancelled", "ApiCancelled", "Inactive"):
                o["status"] = "failed"
                failed.append(o)
                log.warning("    REJECTED %s %s x%d — %s",
                            o["action"], o["symbol"], o["qty"],
                            o["trade"].orderStatus.status)

    # Progressive walk loop
    for step in range(WALK_MAX):
        # Filter to still-active orders
        still_active = [o for o in active_orders if o["status"] == "active"]
        if not still_active:
            break

        log.info("  Walk step %d/%d — %d orders active",
                 step + 1, WALK_MAX, len(still_active))

        # Adjust all prices
        for o in still_active:
            if o["action"] == "SELL":
                new_price = round(max(o["limit_price"] - WALK_STEP, o["walk_limit"]), 2)
            else:
                new_price = round(min(o["limit_price"] + WALK_STEP, o["walk_limit"]), 2)

            if new_price != o["limit_price"]:
                o["limit_price"] = new_price
                o["order"].lmtPrice = new_price
                ib.placeOrder(o["contract"], o["order"])  # Modify in-place
            o["steps"] = step + 1

        # Wait and monitor
        t0 = time.time()
        while time.time() - t0 < WALK_WAIT:
            ib.sleep(0.5)
            for o in still_active:
                if o["status"] != "active":
                    continue
                if o["trade"].orderStatus.status == "Filled":
                    px = o["trade"].orderStatus.avgFillPrice
                    o["status"] = "filled"
                    filled.append(o)
                    log.info("    FILLED %s %s x%d @ $%.2f (walk %d)",
                             o["action"], o["symbol"], o["qty"], px, step + 1)
                elif o["trade"].orderStatus.status in ("Cancelled", "ApiCancelled", "Inactive"):
                    o["status"] = "failed"
                    failed.append(o)
                    log.warning("    REJECTED %s %s x%d — %s (walk %d)",
                                o["action"], o["symbol"], o["qty"],
                                o["trade"].orderStatus.status, step + 1)

    # Anything still active after walk exhaustion → leave resting
    for o in active_orders:
        if o["status"] == "active":
            o["status"] = "resting"
            resting.append(o)
            log.info("    RESTING %s %s x%d @ $%.2f (will work all day)",
                     o["action"], o["symbol"], o["qty"], o["limit_price"])

    return filled, resting, failed


# ═══════════════════════════════════════════════════════════════
#  Main
# ═══════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="Force liquidate ALL IBKR positions (parallel LMT + walk + ThetaData)"
    )
    parser.add_argument("--live", action="store_true", help="Execute (default: dry run)")
    parser.add_argument("--port", type=int, default=GW_PAPER, help="IBKR port")
    args = parser.parse_args()

    # Connect
    ib = IB()
    log.info("Connecting to IBKR on port %d...", args.port)
    ib.connect(DEFAULT_HOST, args.port, clientId=CLIENT_ID + 20, timeout=15)
    log.info("Connected: %s", ib.isConnected())

    acct = verify_paper(ib)
    if not acct:
        log.error("Not a paper account — aborting for safety.")
        ib.disconnect()
        return

    log.info("Paper account: %s", acct)

    # Get ALL positions from IBKR
    portfolio = ib.portfolio()
    log.info("IBKR portfolio: %d items", len(portfolio))

    if not portfolio:
        log.info("No positions found — nothing to liquidate.")
        ib.disconnect()
        return

    longs = [p for p in portfolio if p.position > 0]
    shorts = [p for p in portfolio if p.position < 0]

    # Sort: small abs(MV) first within each phase
    longs.sort(key=lambda p: abs(p.marketValue or 0))
    shorts.sort(key=lambda p: abs(p.marketValue or 0))

    log.info("  LONG positions:  %d (SELL first — frees margin)", len(longs))
    log.info("  SHORT positions: %d (BUY back after)", len(shorts))
    log.info("")

    # Display all positions
    total_mv = 0
    total_upnl = 0
    for p in sorted(portfolio, key=lambda x: abs(x.marketValue or 0)):
        c = p.contract
        action = "SELL" if p.position > 0 else "BUY"
        qty = abs(p.position)
        mv = p.marketValue or 0
        upnl = p.unrealizedPNL or 0
        total_mv += mv
        total_upnl += upnl
        log.info("  %s %s %s K=%.0f %s x%d  MV=$%.0f  uPnL=$%.0f",
                 action, c.symbol, c.right, c.strike,
                 c.lastTradeDateOrContractMonth, qty, mv, upnl)

    log.info("")
    log.info("  Total Market Value:   $%s", f"{total_mv:,.0f}")
    log.info("  Total Unrealized PnL: $%s", f"{total_upnl:+,.0f}")

    if not args.live:
        log.info("")
        log.info("DRY RUN — add --live to execute.")
        ib.disconnect()
        return

    # ── Batch fetch all ThetaData quotes upfront ──
    log.info("")
    log.info("Fetching ThetaData quotes for all %d positions...", len(portfolio))
    theta_quotes = fetch_theta_quotes_batch(portfolio)
    log.info("  Got quotes for %d/%d positions", len(theta_quotes), len(portfolio))

    # Cancel any pending orders
    log.info("")
    log.info("Cancelling all pending orders...")
    ib.reqGlobalCancel()
    ib.sleep(2)

    all_filled = []
    all_resting = []
    all_failed = []

    # ── Phase 1: SELL all LONG positions in parallel ──
    if longs:
        log.info("")
        log.info("=" * 60)
        log.info("PHASE 1: SELLING %d LONG positions IN PARALLEL", len(longs))
        log.info("=" * 60)

        active = place_all_orders(ib, longs, theta_quotes)
        filled, resting, failed = walk_all_orders(ib, active)
        all_filled.extend(filled)
        all_resting.extend(resting)
        all_failed.extend(failed)

        log.info("  Phase 1: %d filled, %d resting, %d failed",
                 len(filled), len(resting), len(failed))

    # ── Phase 2: BUY back all SHORT positions in parallel ──
    if shorts:
        log.info("")
        log.info("=" * 60)
        log.info("PHASE 2: BUYING BACK %d SHORT positions IN PARALLEL", len(shorts))
        log.info("=" * 60)

        active = place_all_orders(ib, shorts, theta_quotes)
        filled, resting, failed = walk_all_orders(ib, active)
        all_filled.extend(filled)
        all_resting.extend(resting)
        all_failed.extend(failed)

        log.info("  Phase 2: %d filled, %d resting, %d failed",
                 len(filled), len(resting), len(failed))

    # ── Phase 3: Retry margin-rejected orders ──
    margin_rejected = [o for o in all_failed
                       if "Inactive" in (o.get("trade").orderStatus.status if o.get("trade") else "")]
    if margin_rejected:
        log.info("")
        log.info("=" * 60)
        log.info("PHASE 3: RETRYING %d margin-rejected orders", len(margin_rejected))
        log.info("=" * 60)
        ib.sleep(3)

        # Re-read portfolio for current positions
        retry_positions = [p for p in ib.portfolio() if abs(p.position) > 0]
        retry_map = {}
        for p in retry_positions:
            c = p.contract
            key = (c.symbol, c.right, c.strike, c.lastTradeDateOrContractMonth)
            retry_map[key] = p

        # Refresh theta quotes
        if retry_positions:
            theta_quotes_retry = fetch_theta_quotes_batch(retry_positions)
        else:
            theta_quotes_retry = {}

        retry_list = []
        for o in margin_rejected:
            c = o["contract"]
            key = (c.symbol, c.right, c.strike, c.lastTradeDateOrContractMonth)
            if key in retry_map:
                retry_list.append(retry_map[key])
            else:
                log.info("  %s: already closed", c.symbol)
                all_failed.remove(o)

        if retry_list:
            active = place_all_orders(ib, retry_list, theta_quotes_retry)
            filled, resting, failed = walk_all_orders(ib, active)
            all_filled.extend(filled)
            all_resting.extend(resting)
            # Update failed count
            for f in filled + resting:
                # Remove from all_failed if it was retried successfully
                for orig in list(all_failed):
                    if orig["symbol"] == f["symbol"]:
                        all_failed.remove(orig)
                        break

    # ── Summary ──
    log.info("")
    log.info("=" * 60)
    log.info("LIQUIDATION SUMMARY")
    log.info("=" * 60)
    log.info("  Total legs:  %d", len(portfolio))
    log.info("  Filled:      %d", len(all_filled))
    log.info("  Resting:     %d (working until market close)", len(all_resting))
    log.info("  Failed:      %d", len(all_failed))

    if all_filled:
        log.info("")
        log.info("  Filled orders:")
        for o in all_filled:
            px = o["trade"].orderStatus.avgFillPrice
            log.info("    %s %s %s K=%.0f x%d @ $%.2f",
                     o["action"], o["symbol"], o["contract"].right,
                     o["contract"].strike, o["qty"], px)

    if all_resting:
        log.info("")
        log.info("  Resting orders (auto-cancel at close):")
        for o in all_resting:
            log.info("    %s %s %s K=%.0f x%d @ $%.2f",
                     o["action"], o["symbol"], o["contract"].right,
                     o["contract"].strike, o["qty"], o["limit_price"])

    if all_failed:
        log.info("")
        log.info("  Failed orders:")
        for o in all_failed:
            log.info("    %s %s %s K=%.0f x%d — %s",
                     o["action"], o["symbol"], o["contract"].right,
                     o["contract"].strike, o["qty"],
                     o["trade"].orderStatus.status)

    # Check remaining
    ib.sleep(2)
    remaining = ib.portfolio()
    remaining_pos = [p for p in remaining if abs(p.position) > 0]
    log.info("")
    log.info("  Remaining positions: %d", len(remaining_pos))

    if remaining_pos:
        for p in remaining_pos:
            c = p.contract
            log.info("    %s %s K=%.0f %s x%.0f  MV=$%.0f",
                     c.symbol, c.right, c.strike,
                     c.lastTradeDateOrContractMonth, p.position,
                     p.marketValue or 0)

    ib.disconnect()
    log.info("Disconnected.")


if __name__ == "__main__":
    main()
