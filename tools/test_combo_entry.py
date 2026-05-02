"""
Test Entry Script — 7 Double Calendar Spreads

Standalone integration test that prices (and optionally executes) 7 specific
double calendar positions on IBKR paper account using ThetaData WS for live
pricing and core/execution.py for order placement.

Usage:
    python tools/test_combo_entry.py              # Dry run: price all 7
    python tools/test_combo_entry.py --live       # Execute on IBKR + save
    python tools/test_combo_entry.py --port 7497  # TWS instead of Gateway
"""

import sys
import argparse
import asyncio
from pathlib import Path

# Ensure project root on path
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

# Fix Python 3.14 asyncio event loop before importing ib_insync
try:
    asyncio.get_event_loop()
except RuntimeError:
    asyncio.set_event_loop(asyncio.new_event_loop())

from core.config import (
    DEFAULT_HOST, TWS_PAPER, GW_PAPER, CLIENT_ID,
    ENFORCE_WINDOW, get_logger,
)
from core.trader import connect_ibkr, verify_paper, create_calendar_legs
from core.execution import execute_spread, get_combo_price, _build_combo
from core.portfolio import load_portfolio, save_portfolio, add_position

log = get_logger(__name__)

# ==============================================================
#  TARGET POSITIONS
# ==============================================================

POSITIONS = [
    {
        "ticker": "GE",
        "call_strike": 300,
        "put_strike": 280,
        "front_exp": "2026-05-15",
        "back_exp": "2026-06-18",
        "contracts": 3,
        "scanner_mid": 10.58,
        "ff": 8.8,
    },
    {
        "ticker": "INTC",
        "call_strike": 91,
        "put_strike": 81,
        "front_exp": "2026-05-15",
        "back_exp": "2026-06-18",
        "contracts": 10,
        "scanner_mid": 6.01,
        "ff": 3.6,
    },
    {
        "ticker": "RTX",
        "call_strike": 180,
        "put_strike": 170,
        "front_exp": "2026-05-15",
        "back_exp": "2026-06-18",
        "contracts": 10,
        "scanner_mid": 5.34,
        "ff": 9.8,
    },
    {
        "ticker": "NEM",
        "call_strike": 115,
        "put_strike": 106,
        "front_exp": "2026-05-15",
        "back_exp": "2026-07-17",
        "contracts": 10,
        "scanner_mid": 9.04,
        "ff": 6.6,
    },
    {
        "ticker": "C",
        "call_strike": 132,
        "put_strike": 125,
        "front_exp": "2026-05-15",
        "back_exp": "2026-06-18",
        "contracts": 10,
        "scanner_mid": 5.30,
        "ff": 8.9,
    },
    {
        "ticker": "PEP",
        "call_strike": 160,
        "put_strike": 153,
        "front_exp": "2026-05-15",
        "back_exp": "2026-07-17",
        "contracts": 10,
        "scanner_mid": 5.91,
        "ff": 7.3,
    },
    {
        "ticker": "TSLA",
        "call_strike": 390,
        "put_strike": 365,
        "front_exp": "2026-05-15",
        "back_exp": "2026-06-18",
        "contracts": 1,
        "scanner_mid": 18.20,
        "ff": 1.9,
    },
]


# ==============================================================
#  MAIN
# ==============================================================

def main():
    parser = argparse.ArgumentParser(
        description="Test entry — 7 double calendar spreads"
    )
    parser.add_argument("--live", action="store_true",
                        help="Execute orders on IBKR (default: dry run, price only)")
    parser.add_argument("--port", type=int, default=GW_PAPER,
                        help=f"IBKR port (Gateway={GW_PAPER}, TWS={TWS_PAPER})")
    parser.add_argument("--host", default=DEFAULT_HOST,
                        help="IBKR host IP")
    parser.add_argument("--no-window-check", action="store_true",
                        help="Skip optimal trading window check")
    args = parser.parse_args()

    mode = "LIVE" if args.live else "DRY RUN"
    log.info("=" * 60)
    log.info("TEST COMBO ENTRY — 7 Double Calendars (%s)", mode)
    log.info("=" * 60)

    # ── Connect to IBKR ──
    ib = None
    try:
        ib = connect_ibkr(args.host, args.port)
        acct = verify_paper(ib)
        log.info("  Account: %s", acct)
    except Exception as ex:
        log.error("Connection failed: %s", ex)
        return 1

    # ── Load portfolio, identify active tickers ──
    portfolio = load_portfolio()
    active_tickers = {
        p["ticker"] for p in portfolio["positions"] if "exit_date" not in p
    }
    if active_tickers:
        log.info("  Active tickers (will skip): %s", ", ".join(sorted(active_tickers)))

    # ── Optimal window check ──
    if args.live and not args.no_window_check:
        from core.trader import check_optimal_window
        is_optimal, time_msg = check_optimal_window()
        log.info("  %s", time_msg)
        if ENFORCE_WINDOW and not is_optimal:
            log.error("  Orders blocked by trading window. Use --no-window-check to override.")
            ib.disconnect()
            return 1

    # ── Qualify & Price all 7 ──
    log.info("")
    log.info("  %-6s  %5s  %5s  %8s  %8s  %3s  %7s  %7s  %7s  %+7s",
             "Ticker", "CallK", "PutK", "Front", "Back", "Ctr",
             "Bid", "Ask", "Mid", "vsScan")
    log.info("  %s", "-" * 85)

    qualified = []  # (pos_spec, legs, n_legs, actual_call_k, actual_put_k, bid, ask, mid)

    for spec in POSITIONS:
        ticker = spec["ticker"]

        # Skip already active
        if ticker in active_tickers:
            log.info("  %-6s  SKIP (already active)", ticker)
            continue

        # Create calendar legs on IBKR
        legs, n_legs, actual_call_k, actual_put_k = create_calendar_legs(
            ib, ticker, spec["call_strike"],
            spec["front_exp"], spec["back_exp"],
            double=True,
            put_strike=spec["put_strike"],
        )

        if not legs:
            log.info("  %-6s  FAILED to qualify on IBKR", ticker)
            continue

        # Build combo for pricing
        combo = _build_combo(legs)
        if combo is None:
            log.info("  %-6s  FAILED to build combo", ticker)
            continue

        # Price via ThetaData WS (primary) / IBKR (fallback)
        bid, ask, mid = get_combo_price(ib, combo, legs=legs)

        diff = mid - spec["scanner_mid"] if mid > 0 else 0
        mid_str = f"${mid:.2f}" if mid > 0 else "N/A"
        bid_str = f"${bid:.2f}" if bid > 0 else "N/A"
        ask_str = f"${ask:.2f}" if ask > 0 else "N/A"
        diff_str = f"${diff:+.2f}" if mid > 0 else "N/A"

        log.info("  %-6s  %5.0f  %5.0f  %8s  %8s  %3d  %7s  %7s  %7s  %7s",
                 ticker, actual_call_k or spec["call_strike"],
                 actual_put_k or spec["put_strike"],
                 spec["front_exp"], spec["back_exp"],
                 spec["contracts"],
                 bid_str, ask_str, mid_str, diff_str)

        qualified.append((spec, legs, n_legs, actual_call_k, actual_put_k, bid, ask, mid))

    log.info("")
    log.info("  Qualified: %d / %d", len(qualified), len(POSITIONS))

    # ── Execute if --live ──
    if not args.live:
        log.info("  DRY RUN complete. Use --live to execute.")
        _cleanup(ib)
        return 0

    log.info("")
    log.info("  --- EXECUTING %d positions ---", len(qualified))
    slippage_log = []

    for spec, legs, n_legs, actual_call_k, actual_put_k, bid, ask, mid in qualified:
        ticker = spec["ticker"]
        contracts = spec["contracts"]
        scanner_mid = spec["scanner_mid"]

        log.info("    %s: %dx double (%d legs, scanner=$%.2f)",
                 ticker, contracts, n_legs, scanner_mid)

        result, fill_cost, slippage, details = execute_spread(
            ib, ticker, legs, n_legs, contracts, scanner_mid, "double"
        )

        slippage_log.append({
            "ticker": ticker,
            "scanner_mid": scanner_mid,
            "fill_cost": fill_cost,
            "slippage": slippage,
            "method": details.get("method"),
            "result": result,
        })

        if result == "full":
            # Derive combo label from DTE
            from datetime import datetime as dt
            front_dt = dt.strptime(spec["front_exp"], "%Y-%m-%d")
            back_dt = dt.strptime(spec["back_exp"], "%Y-%m-%d")
            today = dt.now()
            front_dte = (front_dt - today).days
            back_dte = (back_dt - today).days
            combo_label = f"{front_dte}-{back_dte}"

            pos = add_position(
                portfolio, ticker, combo_label, actual_call_k or spec["call_strike"],
                spec["front_exp"], spec["back_exp"],
                contracts, fill_cost, "double", spec["ff"], n_legs,
                put_strike=actual_put_k or spec["put_strike"],
            )
            pos["execution_method"] = details.get("method")
            pos["slippage"] = round(slippage, 4)
            active_tickers.add(ticker)
            log.info("    -> FILLED @ $%.2f (slip=%+.2f, method=%s)",
                     fill_cost, slippage, details.get("method"))
        elif result == "partial":
            log.warning("    -> PARTIAL FILL — manual cleanup needed")
        else:
            log.warning("    -> NOT FILLED")

    # Save portfolio
    save_portfolio(portfolio)

    # ── Slippage report ──
    log.info("")
    log.info("  %s", "=" * 60)
    log.info("  SLIPPAGE REPORT")
    log.info("  %-6s  %-6s  %7s  %7s  %+7s  %8s",
             "Ticker", "Method", "Scanner", "Fill", "Slip", "Result")
    log.info("  %s", "-" * 55)
    for s in slippage_log:
        log.info("  %-6s  %-6s  $%6.2f  $%6.2f  $%+5.2f  %8s",
                 s["ticker"], s["method"] or "-",
                 s["scanner_mid"], s["fill_cost"],
                 s["slippage"], s["result"])

    filled = [s for s in slippage_log if s["result"] == "full"]
    if filled:
        import numpy as np
        avg_slip = np.mean([s["slippage"] for s in filled])
        log.info("  Avg slippage: $%+.3f/sh (%d fills)", avg_slip, len(filled))

    log.info("")
    log.info("  SUMMARY: %d/%d filled, %d failed",
             len(filled), len(qualified),
             len(qualified) - len(filled))

    _cleanup(ib)
    return 0


def _cleanup(ib):
    """Shutdown ThetaData WS and disconnect IBKR."""
    try:
        from core.theta_ws import theta_ws_shutdown
        theta_ws_shutdown()
    except Exception:
        pass
    if ib and ib.isConnected():
        ib.disconnect()
        log.info("  Disconnected from IBKR")


if __name__ == "__main__":
    sys.exit(main() or 0)
