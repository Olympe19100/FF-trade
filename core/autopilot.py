"""
Autopilot -- FF Double Calendar Spread Strategy Orchestrator

Automates the daily workflow: scan -> trade -> monitor -> report via email.
Uses ThetaData (real-time via local Theta Terminal). EODHD only for earnings calendar.

Usage:
    python core/autopilot.py --scan          # ThetaData scan
    python core/autopilot.py --trade         # Close J-1 + Enter new (IBKR required)
    python core/autopilot.py --paper         # Paper trade: scan -> trade -> monitor -> report (no IBKR)
    python core/autopilot.py --daemon        # Run as daemon with scheduled paper trading
    python core/autopilot.py --monitor       # Price positions via ThetaData (no IBKR)
    python core/autopilot.py --report        # Portfolio status + email recap
    python core/autopilot.py --full          # All steps sequentially (IBKR)
    python core/autopilot.py --dry-run       # Simulate without placing orders

Schedule via Windows Task Scheduler or --daemon:
    09:00 ET -> --scan
    10:15 ET -> --trade / --paper
    16:30 ET -> --monitor --report
"""

import json
import sys
import time
import argparse
import logging
import smtplib
import asyncio
import threading
from collections import deque
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from pathlib import Path
from datetime import datetime, timedelta
from logging.handlers import TimedRotatingFileHandler

# Force unbuffered output
sys.stdout.reconfigure(line_buffering=True)

# Fix Python 3.14 asyncio event loop before importing ib_insync
try:
    asyncio.get_event_loop()
except RuntimeError:
    asyncio.set_event_loop(asyncio.new_event_loop())

# ── Add project root to path for imports ──
ROOT = Path(r"C:\Users\ANTEC MSI\Desktop\pro\Option trading")
sys.path.insert(0, str(ROOT))

from core.config import (
    STATE, OUTPUT, CONFIG_FILE, PORTFOLIO_FILE, TRADES_FILE, LOG_FILE,
    CLOSE_DAYS, COMMISSION_LEG, CONTRACT_MULT, SLIPPAGE_BUFFER,
    load_json,
)
from core.portfolio import (
    load_portfolio, save_portfolio, load_latest_signals,
    compute_kelly, load_trade_history, size_portfolio,
    add_position, record_trade, cost_per_contract,
)


# ═══════════════════════════════════════════════════════════════
#  LOGGING
# ═══════════════════════════════════════════════════════════════

def setup_logging():
    """Configure file + console logging with daily rotation."""
    logger = logging.getLogger("autopilot")
    logger.setLevel(logging.DEBUG)

    # Daily rotating file handler
    fh = TimedRotatingFileHandler(
        str(LOG_FILE), when="midnight", backupCount=30, encoding="utf-8"
    )
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter(
        "[%(asctime)s] [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    ))

    # Console handler
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(logging.Formatter(
        "[%(asctime)s] [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S"
    ))

    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger


log = setup_logging()


# ═══════════════════════════════════════════════════════════════
#  CONFIG
# ═══════════════════════════════════════════════════════════════

def load_config():
    """Load configuration from state/config.json."""
    cfg = load_json(CONFIG_FILE, {})
    if not cfg:
        log.warning(f"Config not found or empty: {CONFIG_FILE}")
    return cfg


# ═══════════════════════════════════════════════════════════════
#  EMAIL
# ═══════════════════════════════════════════════════════════════

def send_email(subject, body, config):
    """Send email via SMTP (Gmail App Password)."""
    email_cfg = config.get("email", {})
    if not email_cfg.get("enabled", False):
        log.info("Email disabled in config, skipping")
        return False

    sender    = email_cfg.get("sender", "")
    password  = email_cfg.get("password", "")
    recipient = email_cfg.get("recipient", sender)
    smtp_srv  = email_cfg.get("smtp_server", "smtp.gmail.com")
    smtp_port = email_cfg.get("smtp_port", 465)

    if not sender or not password:
        log.warning("Email sender/password not configured")
        return False

    try:
        msg = MIMEMultipart()
        msg["From"]    = sender
        msg["To"]      = recipient
        msg["Subject"] = subject
        msg.attach(MIMEText(body, "plain"))

        with smtplib.SMTP_SSL(smtp_srv, smtp_port) as server:
            server.login(sender, password)
            server.send_message(msg)

        log.info(f"Email sent to {recipient}: {subject}")
        return True
    except Exception as ex:
        log.warning(f"Email failed: {ex}")
        return False


def send_alert(subject, body, config):
    """Send an alert email (errors, warnings)."""
    send_email(f"[FF ALERT] {subject}", body, config)


# ═══════════════════════════════════════════════════════════════
#  SCAN
# ═══════════════════════════════════════════════════════════════

def run_scan():
    """Run ThetaData scanner, return signals DataFrame."""
    log.info("=" * 60)
    log.info("AUTOPILOT - SCAN")
    log.info("=" * 60)

    try:
        from core.scanner import run_scanner
        signals = run_scanner()

        if signals is None or signals.empty:
            log.info("SCAN: No signals found")
            return signals

        n = len(signals)
        top5 = signals.head(5)
        tickers = ", ".join(top5["ticker"].tolist())
        log.info(f"SCAN: Found {n} signals (top 5: {tickers})")

        for _, r in top5.iterrows():
            log.debug(f"  {r['ticker']} {r['combo']} FF={r['ff']:.3f} "
                       f"dbl=${r.get('dbl_cost', 'N/A')}")

        return signals
    except Exception as ex:
        log.error(f"SCAN FAILED: {ex}", exc_info=True)
        return None


# ═══════════════════════════════════════════════════════════════
#  TRADE
# ═══════════════════════════════════════════════════════════════

def run_trade(dry_run=False, config=None):
    """Close expiring positions + Enter new signals on IBKR.

    Returns dict with close/enter results.
    """
    config = config or {}
    ibkr_cfg = config.get("ibkr", {})
    host      = ibkr_cfg.get("host", "127.0.0.1")
    port      = ibkr_cfg.get("port", 4002)
    client_id = ibkr_cfg.get("client_id", 60)

    log.info("=" * 60)
    log.info(f"AUTOPILOT - TRADE {'(DRY RUN)' if dry_run else ''}")
    log.info("=" * 60)

    result = {"closed": [], "entered": [], "errors": []}

    if dry_run:
        log.info("DRY RUN: simulating trade flow without IBKR")
        _dry_run_trade(result)
        return result

    ib = None
    try:
        from core.trader import (connect_ibkr, verify_paper,
                                 close_expiring_positions,
                                 enter_new_positions)

        ib = connect_ibkr(host, port, client_id)
        acct = verify_paper(ib)

        # Step 1: Close expiring positions (J-1)
        log.info("TRADE Step 1: Closing expiring positions")
        portfolio_before = load_portfolio()
        active_before = [p for p in portfolio_before["positions"]
                         if "exit_date" not in p]

        close_expiring_positions(ib, acct)

        portfolio_after = load_portfolio()
        active_after = [p for p in portfolio_after["positions"]
                        if "exit_date" not in p]
        newly_closed = [p for p in portfolio_after["positions"]
                        if "exit_date" in p and
                        p["exit_date"] == datetime.now().strftime("%Y-%m-%d")]

        for p in newly_closed:
            info = (f"CLOSED {p['ticker']} {p['combo']} "
                    f"{p['contracts']}cts P&L=${p.get('pnl', 0):+.2f}")
            log.info(info)
            result["closed"].append(p)

        # Step 2: Enter new positions
        log.info("TRADE Step 2: Entering new positions")
        strat_cfg = config.get("strategy", {})
        max_new = strat_cfg.get("max_positions", 20) - len(active_after)

        enter_result = enter_new_positions(ib, acct, max_new=max(0, max_new))

        if enter_result:
            n_entered = enter_result.get("entered", 0)
            log.info(f"TRADE: Entered {n_entered} new positions")

            # Reload to get newly entered positions
            portfolio_final = load_portfolio()
            for p in portfolio_final["positions"]:
                if ("exit_date" not in p and
                        p["entry_date"] == datetime.now().strftime("%Y-%m-%d")):
                    result["entered"].append(p)

        log.info(f"TRADE complete: {len(result['closed'])} closed, "
                 f"{len(result['entered'])} entered")

    except (ConnectionRefusedError, ConnectionError, OSError) as ex:
        msg = f"IBKR connection failed: {ex}"
        log.error(msg)
        result["errors"].append(msg)
        send_alert("IBKR Connection Failed", msg, config)
    except Exception as ex:
        msg = f"TRADE error: {ex}"
        log.error(msg, exc_info=True)
        result["errors"].append(msg)
        send_alert("Trade Error", msg, config)
    finally:
        if ib and ib.isConnected():
            ib.disconnect()
            log.info("Disconnected from IBKR")

    return result


# ═══════════════════════════════════════════════════════════════
#  PAPER TRADE — Simulate trading via ThetaData pricing (no IBKR)
# ═══════════════════════════════════════════════════════════════

def run_paper_trade(config=None):
    """Close expiring + Enter new positions using ThetaData prices.

    No IBKR connection needed. Uses Kelly sizing from trader.py,
    records trades in trades.json, updates portfolio.json.

    Returns trade_result dict compatible with run_report().
    """
    import pandas as pd

    config = config or {}
    strat_cfg = config.get("strategy", {})
    paper_account = strat_cfg.get("paper_account", 100_000)
    max_positions = strat_cfg.get("max_positions", 20)

    log.info("=" * 60)
    log.info("AUTOPILOT - PAPER TRADE (ThetaData, no IBKR)")
    log.info("=" * 60)

    result = {"closed": [], "entered": [], "errors": []}
    portfolio = load_portfolio()
    active = [p for p in portfolio["positions"] if "exit_date" not in p]
    today = datetime.now()
    today_str = today.strftime("%Y-%m-%d")

    # ── Step 1: Close expiring positions (DTE <= CLOSE_DAYS) ──
    log.info(f"PAPER Step 1: Closing expiring positions (DTE <= {CLOSE_DAYS})")

    for pos in active:
        front_exp = pd.Timestamp(pos["front_exp"])
        days_left = (front_exp - pd.Timestamp(today)).days
        if days_left > CLOSE_DAYS:
            continue

        ticker = pos["ticker"]
        log.info(f"  Closing {ticker} {pos['combo']} "
                 f"(front exp {pos['front_exp']}, {days_left}d left)")

        pricing = _price_position(pos)
        if pricing is None:
            msg = f"Cannot price {ticker} for close, skipping"
            log.warning(f"  {msg}")
            result["errors"].append(msg)
            continue

        exit_price = pricing["current_cost"]
        entry_cost = pos["cost_per_share"]
        contracts = pos["contracts"]
        n_legs = pos.get("n_legs", 4)
        commission = n_legs * COMMISSION_LEG * contracts

        pnl = (exit_price - entry_cost) * CONTRACT_MULT * contracts - commission
        ret_pct = (exit_price - entry_cost) / entry_cost if entry_cost != 0 else 0

        pos["exit_date"] = today_str
        pos["exit_price"] = round(exit_price, 4)
        pos["pnl"] = round(pnl, 2)
        pos["return_pct"] = round(ret_pct, 4)

        record_trade(pos, exit_price, pnl, ret_pct)
        result["closed"].append(pos)
        log.info(f"  CLOSED {ticker} exit=${exit_price:.2f} "
                 f"P&L=${pnl:+,.2f} ({ret_pct:+.1%})")

    # ── Step 2: Enter new positions ──
    log.info("PAPER Step 2: Entering new positions from latest signals")

    active_after = [p for p in portfolio["positions"] if "exit_date" not in p]
    active_tickers = {p["ticker"] for p in active_after}
    slots = max_positions - len(active_after)

    if slots <= 0:
        log.info(f"  Portfolio full: {len(active_after)}/{max_positions}")
    else:
        signals = load_latest_signals()
        if signals is None or signals.empty:
            log.info("  No signals available")
        else:
            # Compute account value: paper_account + realized P&L
            trades_data = load_json(TRADES_FILE, {"trades": []})
            realized_pnl = sum(t.get("pnl", 0) for t in trades_data.get("trades", []))
            total_deployed = sum(p.get("total_deployed", 0) for p in active_after)
            account_value = paper_account + realized_pnl - total_deployed

            # Kelly sizing
            returns = load_trade_history()
            kelly_f = compute_kelly(returns)

            log.info(f"  Paper account: ${paper_account:,.0f}")
            log.info(f"  Realized P&L:  ${realized_pnl:+,.0f}")
            log.info(f"  Deployed:      ${total_deployed:,.0f}")
            log.info(f"  Available:     ${account_value:,.0f}")
            log.info(f"  Kelly f:       {kelly_f:.4f} ({kelly_f:.1%})")
            log.info(f"  Slots:         {slots}")

            # Filter: skip active tickers, pick top signals up to slots
            candidates = []
            for _, sig in signals.iterrows():
                if len(candidates) >= slots:
                    break
                if sig["ticker"] in active_tickers:
                    continue
                has_dbl = pd.notna(sig.get("dbl_cost")) and sig["dbl_cost"] > 0
                cps = sig["dbl_cost"] if has_dbl else sig["call_cost"]
                if cps <= 0:
                    continue
                n_legs = 4 if has_dbl else 2
                candidates.append((sig, cps, n_legs, has_dbl))

            if not candidates:
                log.info("  No valid candidates after filtering")
            else:
                # Global Kelly sizing
                signals_info = [(c[0]["ticker"], c[1], c[2])
                                for c in candidates]
                sizing = size_portfolio(signals_info, kelly_f,
                                        paper_account + realized_pnl)

                for (sig, cps, n_legs, has_dbl), (ticker, contracts, deployed) in \
                        zip(candidates, sizing):
                    if contracts <= 0:
                        continue
                    if deployed > account_value:
                        log.warning(f"  {ticker}: skip (${deployed:,.0f} > "
                                    f"available ${account_value:,.0f})")
                        continue

                    spread_type = "double" if has_dbl else "single"
                    entry_cost = cps + SLIPPAGE_BUFFER

                    put_strike = sig.get("put_strike")
                    if pd.isna(put_strike):
                        put_strike = None

                    pos = add_position(
                        portfolio, ticker, sig["combo"],
                        sig["strike"], sig["front_exp"], sig["back_exp"],
                        contracts, entry_cost, spread_type,
                        sig["ff"], n_legs, put_strike=put_strike
                    )
                    pos["paper"] = True
                    result["entered"].append(pos)
                    active_tickers.add(ticker)
                    account_value -= deployed

                    log.info(f"  ENTERED {ticker} {sig['combo']} "
                             f"FF={sig['ff']:.3f} {contracts}x "
                             f"@ ${entry_cost:.2f}/sh ({spread_type})")

    save_portfolio(portfolio)
    log.info(f"PAPER complete: {len(result['closed'])} closed, "
             f"{len(result['entered'])} entered")
    return result


def _dry_run_trade(result):
    """Simulate trade flow by reading signals and portfolio."""
    try:
        import pandas as pd

        portfolio = load_portfolio()
        active = [p for p in portfolio["positions"] if "exit_date" not in p]
        today = datetime.now()

        # Check which would be closed
        for pos in active:
            front_exp = pd.Timestamp(pos["front_exp"])
            days_left = (front_exp - pd.Timestamp(today)).days
            if days_left <= 1:
                log.info(f"DRY RUN: Would close {pos['ticker']} {pos['combo']} "
                         f"(front exp {pos['front_exp']}, {days_left}d left)")
                result["closed"].append(pos)

        # Check which would be entered
        signals = load_latest_signals()
        if not signals.empty:
            active_tickers = {p["ticker"] for p in active}
            slots = 20 - len(active) + len(result["closed"])
            n = 0
            for _, sig in signals.iterrows():
                if n >= slots:
                    break
                if sig["ticker"] not in active_tickers:
                    has_dbl = pd.notna(sig.get("dbl_cost")) and sig["dbl_cost"] > 0
                    cost = sig["dbl_cost"] if has_dbl else sig["call_cost"]
                    log.info(f"DRY RUN: Would enter {sig['ticker']} {sig['combo']} "
                             f"FF={sig['ff']:.3f} ${cost:.2f}")
                    result["entered"].append({
                        "ticker": sig["ticker"],
                        "combo": sig["combo"],
                        "ff": sig["ff"],
                    })
                    n += 1
    except Exception as ex:
        log.warning(f"DRY RUN simulation error: {ex}")


# ═══════════════════════════════════════════════════════════════
#  MONITOR — Live P&L via ThetaData (no IBKR needed)
# ═══════════════════════════════════════════════════════════════

def _price_position(pos):
    """Price a single position using ThetaData.

    Matches the 4 legs (call/put x front/back) and computes current spread cost.
    Returns dict with pricing info, or None if pricing fails.
    """
    import pandas as pd
    from core.scanner import fetch_option_chain_thetadata

    ticker = pos["ticker"]
    strike = pos["strike"]
    put_strike = pos.get("put_strike", strike)
    front_exp = pos["front_exp"]
    back_exp = pos["back_exp"]
    is_double = pos.get("spread_type", "double") == "double"

    stock_px, chain = 0, pd.DataFrame()
    try:
        stock_px, chain = fetch_option_chain_thetadata(ticker)
    except Exception as ex:
        log.warning("MONITOR: ThetaData failed for %s: %s", ticker, ex)
    if stock_px <= 0 or chain.empty:
        log.warning(f"MONITOR: No data for {ticker}")
        return None

    calls = chain[chain["type"] == "call"]
    puts = chain[chain["type"] == "put"]

    def match_leg(df, target_strike, target_exp):
        """Find the option matching strike and expiration, return mid price."""
        matches = df[
            (df["exp_date"] == target_exp) &
            ((df["strike"] - target_strike).abs() < 0.01)
        ]
        if matches.empty:
            # Try closest strike within 2%
            exp_matches = df[df["exp_date"] == target_exp]
            if exp_matches.empty:
                return None
            exp_matches = exp_matches.copy()
            exp_matches["sdiff"] = (exp_matches["strike"] - target_strike).abs()
            best = exp_matches.loc[exp_matches["sdiff"].idxmin()]
            if best["sdiff"] / target_strike > 0.02:
                return None
            matches = exp_matches.loc[[exp_matches["sdiff"].idxmin()]]

        row = matches.iloc[0]
        bid = float(row["bid"]) if row["bid"] > 0 else 0
        ask = float(row["ask"]) if row["ask"] > 0 else 0
        if bid > 0 and ask > 0:
            return (bid + ask) / 2
        return None

    # Match call legs
    front_call_mid = match_leg(calls, strike, front_exp)
    back_call_mid = match_leg(calls, strike, back_exp)

    if front_call_mid is None or back_call_mid is None:
        log.warning(f"MONITOR: Cannot match call legs for {ticker} "
                    f"K={strike} {front_exp}/{back_exp}")
        return None

    call_spread = back_call_mid - front_call_mid
    current_cost = call_spread

    # Match put legs (double calendar)
    if is_double:
        front_put_mid = match_leg(puts, put_strike, front_exp)
        back_put_mid = match_leg(puts, put_strike, back_exp)

        if front_put_mid is not None and back_put_mid is not None:
            put_spread = back_put_mid - front_put_mid
            current_cost += put_spread
        else:
            log.warning(f"MONITOR: Cannot match put legs for {ticker} "
                        f"PutK={put_strike}, using call-only")

    entry_cost = pos["cost_per_share"]
    contracts = pos["contracts"]
    n_legs = pos.get("n_legs", 4)
    commission = n_legs * COMMISSION_LEG * contracts

    unrealized_pnl = (current_cost - entry_cost) * 100 * contracts - commission
    deployed = pos.get("total_deployed", entry_cost * 100 * contracts)
    return_pct = unrealized_pnl / deployed if deployed > 0 else 0

    # Front DTE
    today = datetime.now()
    try:
        front_dt = datetime.strptime(front_exp, "%Y-%m-%d")
        front_dte = (front_dt - today).days
    except ValueError:
        front_dte = -1

    return {
        "ticker": ticker,
        "combo": pos.get("combo", ""),
        "contracts": contracts,
        "strike": strike,
        "put_strike": put_strike,
        "entry_cost": round(entry_cost, 2),
        "current_cost": round(current_cost, 2),
        "unrealized_pnl": round(unrealized_pnl, 2),
        "return_pct": round(return_pct, 4),
        "front_dte": front_dte,
        "stock_px": round(stock_px, 2),
    }


def run_monitor(ib=None, acct=None):
    """Price all active positions and display live P&L.

    If ib (ib_insync.IB) is provided and connected, uses IBKR portfolio data
    for accurate P&L. Otherwise falls back to ThetaData REST pricing.

    Returns monitor_data dict (for build_report) or None if no positions.
    """
    portfolio = load_json(PORTFOLIO_FILE, {"positions": []})
    active = [p for p in portfolio["positions"] if "exit_date" not in p]

    if not active:
        log.info("MONITOR: No active positions")
        return None

    priced = []
    errors = []

    # Try IBKR direct if connection provided
    use_ibkr = False
    if ib is not None:
        try:
            if ib.isConnected():
                use_ibkr = True
        except Exception:
            pass

    if use_ibkr:
        log.info("=" * 60)
        log.info("AUTOPILOT - MONITOR (IBKR)")
        log.info("=" * 60)
        log.info(f"MONITOR: Pricing {len(active)} active positions via IBKR")

        from core.portfolio import ibkr_portfolio_to_positions
        try:
            items = ib.portfolio(acct)
            ibkr_items = [item for item in items
                          if getattr(item.contract, "secType", "") == "OPT"
                          and item.position != 0]
            priced, errors = ibkr_portfolio_to_positions(ibkr_items, active)
        except Exception as ex:
            log.warning(f"MONITOR: IBKR portfolio failed: {ex}, falling back to ThetaData")
            use_ibkr = False

    if not use_ibkr:
        log.info("=" * 60)
        log.info("AUTOPILOT - MONITOR (ThetaData)")
        log.info("=" * 60)
        log.info(f"MONITOR: Pricing {len(active)} active positions via ThetaData")

        for pos in active:
            try:
                result = _price_position(pos)
                if result:
                    priced.append(result)
                else:
                    errors.append(pos["ticker"])
            except Exception as ex:
                log.warning(f"MONITOR: Error pricing {pos['ticker']}: {ex}")
                errors.append(pos["ticker"])

    # Display table
    if priced:
        total_pnl = sum(p["unrealized_pnl"] for p in priced)

        print(f"\n{'='*75}")
        print(f"LIVE P&L (ThetaData) — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
        print(f"{'='*75}")
        print(f"  {'Ticker':>6s}  {'Combo':>5s}  {'Cts':>3s}  {'Entry':>7s}  "
              f"{'Current':>8s}  {'Unrlz P&L':>10s}  {'Ret%':>7s}  {'DTE':>4s}")
        print(f"  {'-'*68}")

        for p in sorted(priced, key=lambda x: -x["unrealized_pnl"]):
            dte_str = f"{p['front_dte']}d" if p["front_dte"] >= 0 else "exp"
            print(f"  {p['ticker']:>6s}  {p['combo']:>5s}  {p['contracts']:>3d}  "
                  f"${p['entry_cost']:>5.2f}  ${p['current_cost']:>6.2f}  "
                  f"${p['unrealized_pnl']:>+9.2f}  {p['return_pct']:>+6.1%}  "
                  f"{dte_str:>4s}")

        print(f"  {'-'*68}")
        print(f"  {'TOTAL':>6s}  {'':>5s}  {'':>3s}  {'':>7s}  {'':>8s}  "
              f"${total_pnl:>+9.2f}")
        print()
    else:
        print("  No positions could be priced")

    if errors:
        log.warning(f"MONITOR: Failed to price: {', '.join(errors)}")

    # Save snapshot
    today_str = datetime.now().strftime("%Y%m%d")
    monitor_data = {
        "date": datetime.now().strftime("%Y-%m-%d"),
        "positions": priced,
        "errors": errors,
        "total_unrealized_pnl": round(sum(p["unrealized_pnl"] for p in priced), 2)
                                if priced else 0,
    }

    snapshot_file = STATE / f"monitor_{today_str}.json"
    with open(snapshot_file, "w") as f:
        json.dump(monitor_data, f, indent=2)
    log.info(f"MONITOR: Snapshot saved to {snapshot_file}")

    return monitor_data


# ═══════════════════════════════════════════════════════════════
#  REPORT
# ═══════════════════════════════════════════════════════════════

def run_report(trade_result=None, config=None, monitor_data=None):
    """Generate daily report and send email."""
    config = config or {}

    log.info("=" * 60)
    log.info("AUTOPILOT - REPORT")
    log.info("=" * 60)

    # Auto-monitor if positions active and no monitor_data provided
    if monitor_data is None:
        portfolio = load_json(PORTFOLIO_FILE, {"positions": []})
        active = [p for p in portfolio["positions"] if "exit_date" not in p]
        if active:
            log.info("REPORT: Active positions detected, running monitor...")
            monitor_data = run_monitor()

    try:
        body = build_report(trade_result, monitor_data=monitor_data)
        today_str = datetime.now().strftime("%Y-%m-%d")
        subject = f"[FF Strategy] Daily Report - {today_str}"

        print(body)
        log.info(f"Report generated ({len(body)} chars)")

        sent = send_email(subject, body, config)
        if sent:
            log.info(f"Report sent via email")
        return body
    except Exception as ex:
        log.error(f"REPORT FAILED: {ex}", exc_info=True)
        send_alert("Report Failed", str(ex), config)
        return None


def build_report(trade_result=None, monitor_data=None):
    """Build the daily report text."""
    import pandas as pd

    today = datetime.now()
    today_str = today.strftime("%Y-%m-%d")
    lines = []

    lines.append(f"FF Double Calendar Strategy - Daily Report")
    lines.append(f"Date: {today_str}")
    lines.append("")

    # ── Portfolio Status ──
    portfolio = load_json(PORTFOLIO_FILE, {"positions": []})
    active = [p for p in portfolio["positions"] if "exit_date" not in p]
    closed = [p for p in portfolio["positions"] if "exit_date" in p]
    total_deployed = sum(p.get("total_deployed", 0) for p in active)

    lines.append("=== PORTFOLIO STATUS ===")
    lines.append(f"Positions: {len(active)}/20 | Deployed: ${total_deployed:,.0f}")
    lines.append("")

    # ── Today's Actions ──
    lines.append("=== TODAY'S ACTIONS ===")

    if trade_result:
        # Closed today
        closed_today = trade_result.get("closed", [])
        if closed_today:
            lines.append("CLOSED:")
            for p in closed_today:
                pnl = p.get("pnl", 0)
                ret = p.get("return_pct", 0)
                if isinstance(ret, (int, float)):
                    ret_str = f"{ret:+.1%}"
                else:
                    ret_str = "N/A"
                held = "?"
                try:
                    entry = datetime.strptime(p["entry_date"], "%Y-%m-%d")
                    held = (today - entry).days
                except (KeyError, ValueError):
                    pass
                lines.append(
                    f"  {p['ticker']} {p.get('combo', '')} | "
                    f"{p.get('contracts', 0)} cts | "
                    f"P&L: ${pnl:+,.0f} ({ret_str}) | Held {held} days"
                )
        else:
            lines.append("CLOSED: (none)")

        lines.append("")

        # Entered today
        entered_today = trade_result.get("entered", [])
        if entered_today:
            lines.append("ENTERED:")
            for p in entered_today:
                ff = p.get("ff", 0)
                cts = p.get("contracts", "?")
                cps = p.get("cost_per_share", 0)
                ks = f"K={p.get('strike', '?'):.0f}" if isinstance(p.get("strike"), (int, float)) else ""
                ps = f"/{p.get('put_strike', '?'):.0f}" if isinstance(p.get("put_strike"), (int, float)) else ""
                lines.append(
                    f"  {p['ticker']} {p.get('combo', '')} | "
                    f"FF={ff:.1f}% | {cts} cts @ ${cps:.2f}/sh | {ks}{ps}"
                )
        else:
            lines.append("ENTERED: (none)")

        # Errors
        errors = trade_result.get("errors", [])
        if errors:
            lines.append("")
            lines.append("ERRORS:")
            for e in errors:
                lines.append(f"  {e}")
    else:
        lines.append("(no trade data - report-only mode)")

    lines.append("")

    # ── Open Positions ──
    lines.append("=== OPEN POSITIONS ===")
    if active:
        for p in sorted(active, key=lambda x: -x.get("ff", 0)):
            ps = p.get("put_strike", p.get("strike", 0))
            lines.append(
                f"  {p['ticker']} {p.get('combo', '')} | "
                f"{p.get('contracts', 0)} cts | "
                f"Entry ${p.get('cost_per_share', 0):.2f} | "
                f"FF={p.get('ff', 0):.1f}% | "
                f"K={p.get('strike', 0):.0f}/{ps:.0f} | "
                f"Exp {p.get('front_exp', '?')} -> {p.get('back_exp', '?')}"
            )
    else:
        lines.append("  (no open positions)")
    lines.append("")

    # ── Performance ──
    trades_data = load_json(TRADES_FILE, {"trades": []})
    all_trades = trades_data.get("trades", [])

    # Today's closed trades from trade history
    today_trades = [t for t in all_trades
                    if t.get("exit_date") == today_str]
    today_pnl = sum(t.get("pnl", 0) for t in today_trades)

    # MTD
    month_start = today.strftime("%Y-%m-01")
    mtd_trades = [t for t in all_trades
                  if t.get("exit_date", "") >= month_start]
    mtd_pnl = sum(t.get("pnl", 0) for t in mtd_trades)

    # All time
    total_pnl = sum(t.get("pnl", 0) for t in all_trades)
    n_trades = len(all_trades)
    n_wins = sum(1 for t in all_trades if t.get("pnl", 0) > 0)
    wr = n_wins / n_trades * 100 if n_trades > 0 else 0
    avg_pnl = total_pnl / n_trades if n_trades > 0 else 0

    lines.append("=== PERFORMANCE ===")
    lines.append(f"  Today P&L:    ${today_pnl:+,.0f} ({len(today_trades)} trades)")
    lines.append(f"  MTD P&L:      ${mtd_pnl:+,.0f} ({len(mtd_trades)} trades)")
    lines.append(f"  Total trades: {n_trades} | WR: {wr:.0f}% | Avg: ${avg_pnl:+,.0f}")
    lines.append(f"  Total P&L:    ${total_pnl:+,.0f}")
    lines.append("")

    # ── Latest Signals ──
    latest_signals = _load_latest_signals_file()
    if latest_signals is not None and not latest_signals.empty:
        lines.append(f"=== LATEST SIGNALS ({len(latest_signals)} total) ===")
        for _, r in latest_signals.head(10).iterrows():
            dbl = f"${r['dbl_cost']:.2f}" if pd.notna(r.get("dbl_cost")) else "N/A"
            lines.append(
                f"  {r['ticker']} {r['combo']} | FF={r['ff']:.3f} | "
                f"CallK={r['strike']:.0f} | Dbl={dbl}"
            )
        lines.append("")

    # ── Live P&L (ThetaData Monitor) ──
    if monitor_data and monitor_data.get("positions"):
        lines.append("=== LIVE P&L (ThetaData) ===")
        lines.append(f"  {'Ticker':>6s}  {'Combo':>5s}  {'Cts':>3s}  {'Entry':>7s}  "
                     f"{'Current':>8s}  {'Unrlz P&L':>10s}  {'Ret%':>7s}  {'DTE':>4s}")
        lines.append(f"  {'-'*68}")

        for p in sorted(monitor_data["positions"],
                        key=lambda x: -x["unrealized_pnl"]):
            dte_str = f"{p['front_dte']}d" if p["front_dte"] >= 0 else "exp"
            lines.append(
                f"  {p['ticker']:>6s}  {p['combo']:>5s}  {p['contracts']:>3d}  "
                f"${p['entry_cost']:>5.2f}  ${p['current_cost']:>6.2f}  "
                f"${p['unrealized_pnl']:>+9.2f}  {p['return_pct']:>+6.1%}  "
                f"{dte_str:>4s}"
            )

        total = monitor_data.get("total_unrealized_pnl", 0)
        lines.append(f"  {'-'*68}")
        lines.append(f"  {'TOTAL':>6s}  {'':>5s}  {'':>3s}  {'':>7s}  {'':>8s}  "
                     f"${total:>+9.2f}")

        if monitor_data.get("errors"):
            lines.append(f"  (failed to price: {', '.join(monitor_data['errors'])})")
        lines.append("")

    lines.append("---")
    lines.append("Generated by FF Autopilot")
    return "\n".join(lines)


def _load_latest_signals_file():
    """Load most recent signals CSV."""
    import pandas as pd
    files = sorted(OUTPUT.glob("signals_*.csv"), reverse=True)
    if not files:
        return None
    try:
        df = pd.read_csv(str(files[0]))
        return df.sort_values("ff", ascending=False)
    except Exception:
        return None


# ═══════════════════════════════════════════════════════════════
#  TRADE (WEB) — Reuses existing IB connection from web app
# ═══════════════════════════════════════════════════════════════

def run_trade_web(ib, acct, config=None):
    """Close expiring + Enter new positions using existing IB connection.

    Called by the daemon when IBKR is already connected via the web app.
    Does NOT connect/disconnect — caller manages the connection.
    """
    from core.trader import close_expiring_positions, enter_new_positions

    config = config or {}
    result = {"closed": [], "entered": [], "errors": []}

    try:
        # Step 1: Close expiring positions
        log.info("TRADE_WEB Step 1: Closing expiring positions")
        portfolio_before = load_portfolio()
        close_expiring_positions(ib, acct)

        portfolio_after = load_portfolio()
        active_after = [p for p in portfolio_after["positions"]
                        if "exit_date" not in p]
        newly_closed = [p for p in portfolio_after["positions"]
                        if "exit_date" in p and
                        p["exit_date"] == datetime.now().strftime("%Y-%m-%d")]

        for p in newly_closed:
            info = (f"CLOSED {p['ticker']} {p['combo']} "
                    f"{p['contracts']}cts P&L=${p.get('pnl', 0):+.2f}")
            log.info(info)
            result["closed"].append(p)

        # Step 2: Enter new positions
        log.info("TRADE_WEB Step 2: Entering new positions")
        strat_cfg = config.get("strategy", {})
        max_new = strat_cfg.get("max_positions", 20) - len(active_after)

        enter_result = enter_new_positions(ib, acct, max_new=max(0, max_new))

        if enter_result:
            n_entered = enter_result.get("entered", 0)
            log.info(f"TRADE_WEB: Entered {n_entered} new positions")

            portfolio_final = load_portfolio()
            for p in portfolio_final["positions"]:
                if ("exit_date" not in p and
                        p["entry_date"] == datetime.now().strftime("%Y-%m-%d")):
                    result["entered"].append(p)

        log.info(f"TRADE_WEB complete: {len(result['closed'])} closed, "
                 f"{len(result['entered'])} entered")

    except Exception as ex:
        msg = f"TRADE_WEB error: {ex}"
        log.error(msg, exc_info=True)
        result["errors"].append(msg)

    return result


# ═══════════════════════════════════════════════════════════════
#  DAEMON — Scheduler loop (schedule 1.2.2)
# ═══════════════════════════════════════════════════════════════

def _is_weekday():
    """Return True if today is Mon-Fri."""
    return datetime.now().weekday() < 5


class DaemonScheduler:
    """Web-integrated autopilot daemon.

    Runs scan/trade/monitor jobs on a daily schedule.
    Trade job reuses the web app's IB connection (ib_state).
    """

    def __init__(self):
        self.running = False
        self._thread = None
        self._scheduler = None
        self._ib_state = None
        self._config = {}
        self.last_scan = None
        self.last_trade = None
        self.last_monitor = None
        self.logs = deque(maxlen=100)

    def start(self, config=None, ib_state_ref=None):
        """Start daemon in background thread."""
        if self.running:
            return
        self.running = True
        self._ib_state = ib_state_ref
        self._config = config or load_config()
        self._setup_scheduler()
        self._thread = threading.Thread(target=self._loop, daemon=True, name="autopilot-daemon")
        self._thread.start()
        self._log("DAEMON", "Daemon started — schedule: 09:00 Scan | 10:15 Trade | 16:30 Monitor")

    def stop(self):
        """Stop the daemon."""
        if not self.running:
            return
        self.running = False
        if self._scheduler:
            self._scheduler.clear()
        self._log("DAEMON", "Daemon stopped")

    def _setup_scheduler(self):
        import schedule
        self._scheduler = schedule.Scheduler()
        self._scheduler.every().day.at("09:00", "America/New_York").do(self._job_scan)
        self._scheduler.every().day.at("10:15", "America/New_York").do(self._job_trade)
        self._scheduler.every().day.at("16:30", "America/New_York").do(self._job_monitor)

    def _loop(self):
        while self.running:
            try:
                self._scheduler.run_pending()
            except Exception as ex:
                self._log("DAEMON", f"Scheduler error: {ex}", level="error")
            time.sleep(30)

    def _job_scan(self):
        if not _is_weekday():
            return
        self._log("SCAN", "Starting scheduled scan...")
        try:
            run_scan()
            self.last_scan = datetime.now().isoformat()
            self._log("SCAN", "Scan completed")
        except Exception as ex:
            self._log("SCAN", f"Scan failed: {ex}", level="error")
            send_alert("Daemon Scan Failed", str(ex), self._config)

    def _job_trade(self):
        if not _is_weekday():
            return
        ib_st = self._ib_state
        if not ib_st:
            self._log("TRADE", "No IBKR state available — skipping trade", level="warning")
            return

        # Auto-reconnect if disconnected but we have host/port
        if not ib_st.get("connected") or not ib_st.get("ib") or not ib_st["ib"].isConnected():
            host = ib_st.get("host")
            port = ib_st.get("port")
            if host and port:
                self._log("TRADE", f"IBKR disconnected — attempting auto-reconnect to {host}:{port}...")
                try:
                    from core.trader import connect_ibkr, verify_paper
                    from api.ibkr_worker import run_in_ib_thread
                    def do_reconnect():
                        ib = connect_ibkr(host, port)
                        acct = verify_paper(ib)
                        return ib, acct
                    
                    ib, acct = run_in_ib_thread(do_reconnect)
                    ib_st["ib"] = ib
                    ib_st["connected"] = True
                    ib_st["account"] = acct
                    self._log("TRADE", "Auto-reconnect successful")
                except Exception as ex:
                    self._log("TRADE", f"Auto-reconnect failed: {ex}", level="error")
                    send_alert("Daemon: IBKR Reconnect Failed", 
                               f"Trade failed because IBKR is offline and auto-reconnect failed: {ex}", 
                               self._config)
                    return
            else:
                self._log("TRADE", "IBKR not connected and no host/port available — skipping", level="warning")
                return

        self._log("TRADE", "Starting scheduled trade...")
        try:
            from api.ibkr_worker import run_in_ib_thread
            result = run_in_ib_thread(run_trade_web, ib_st["ib"], ib_st["account"], self._config)
            self.last_trade = datetime.now().isoformat()
            closed = len(result.get("closed", []))
            entered = len(result.get("entered", []))
            errors = result.get("errors", [])
            self._log("TRADE", f"Trade completed: {closed} closed, {entered} entered")
            if errors:
                for e in errors:
                    self._log("TRADE", f"Error: {e}", level="error")
        except Exception as ex:
            self._log("TRADE", f"Trade failed: {ex}", level="error")
            send_alert("Daemon Trade Failed", str(ex), self._config)

    def _job_monitor(self):
        if not _is_weekday():
            return
        self._log("MONITOR", "Starting scheduled monitor + report...")
        try:
            monitor_data = run_monitor()
            run_report(config=self._config, monitor_data=monitor_data)
            self.last_monitor = datetime.now().isoformat()
            self._log("MONITOR", "Monitor + report completed")
        except Exception as ex:
            self._log("MONITOR", f"Monitor/report failed: {ex}", level="error")
            send_alert("Daemon Monitor Failed", str(ex), self._config)

    def _log(self, job, msg, level="info"):
        entry = {"time": datetime.now().isoformat(), "job": job, "msg": msg, "level": level}
        self.logs.append(entry)
        getattr(log, level)(f"DAEMON: [{job}] {msg}")

    def status(self):
        next_jobs = []
        if self.running and self._scheduler:
            try:
                for j in self._scheduler.get_jobs():
                    next_run = j.next_run
                    if next_run:
                        next_jobs.append(next_run.strftime("%H:%M:%S %Z"))
            except Exception:
                pass

        return {
            "running": self.running,
            "last_scan": self.last_scan,
            "last_trade": self.last_trade,
            "last_monitor": self.last_monitor,
            "next_jobs": next_jobs,
            "logs": list(self.logs)[-20:],
        }


# Module-level singleton
daemon = DaemonScheduler()


def run_daemon(config=None):
    """Run autopilot as a daemon with scheduled jobs.

    Schedule (all times Eastern):
        09:00 -> run_scan()
        10:15 -> run_paper_trade()
        16:30 -> run_monitor() + run_report()

    Skips weekends. Loops every 30 seconds.
    """
    import schedule

    config = config or load_config()

    def job_scan():
        if not _is_weekday():
            log.debug("DAEMON: Weekend, skipping scan")
            return
        log.info("DAEMON: Triggering scheduled SCAN")
        try:
            run_scan()
        except Exception as ex:
            log.error(f"DAEMON: Scan failed: {ex}", exc_info=True)
            send_alert("Daemon Scan Failed", str(ex), config)

    def job_paper_trade():
        if not _is_weekday():
            log.debug("DAEMON: Weekend, skipping paper trade")
            return
        log.info("DAEMON: Triggering scheduled PAPER TRADE")
        try:
            run_paper_trade(config)
        except Exception as ex:
            log.error(f"DAEMON: Paper trade failed: {ex}", exc_info=True)
            send_alert("Daemon Paper Trade Failed", str(ex), config)

    def job_monitor_report():
        if not _is_weekday():
            log.debug("DAEMON: Weekend, skipping monitor/report")
            return
        log.info("DAEMON: Triggering scheduled MONITOR + REPORT")
        try:
            monitor_data = run_monitor()
            run_report(config=config, monitor_data=monitor_data)
        except Exception as ex:
            log.error(f"DAEMON: Monitor/report failed: {ex}", exc_info=True)
            send_alert("Daemon Monitor Failed", str(ex), config)

    # Schedule jobs (Eastern Time)
    schedule.every().day.at("09:00", "America/New_York").do(job_scan)
    schedule.every().day.at("10:15", "America/New_York").do(job_paper_trade)
    schedule.every().day.at("16:30", "America/New_York").do(job_monitor_report)

    log.info("=" * 60)
    log.info("DAEMON STARTED — Paper Trading Autopilot")
    log.info("=" * 60)
    log.info("  Schedule (ET):")
    log.info("    09:00 -> Scan")
    log.info("    10:15 -> Paper Trade")
    log.info("    16:30 -> Monitor + Report")
    log.info("  Checking every 30 seconds. Ctrl+C to stop.")

    try:
        while True:
            schedule.run_pending()
            time.sleep(30)
    except KeyboardInterrupt:
        log.info("DAEMON stopped by user")


# ═══════════════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="FF Double Calendar - Autopilot Orchestrator"
    )
    parser.add_argument("--scan", action="store_true",
                        help="Run ThetaData scan")
    parser.add_argument("--trade", action="store_true",
                        help="Close J-1 + Enter new (IBKR required)")
    parser.add_argument("--paper", action="store_true",
                        help="Paper trade: scan -> trade -> monitor -> report (no IBKR)")
    parser.add_argument("--daemon", action="store_true",
                        help="Run as daemon with scheduled paper trading")
    parser.add_argument("--monitor", action="store_true",
                        help="Price active positions via ThetaData (no IBKR needed)")
    parser.add_argument("--report", action="store_true",
                        help="Portfolio status + email recap")
    parser.add_argument("--full", action="store_true",
                        help="All steps: scan -> trade -> monitor -> report (IBKR)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Simulate without placing orders")
    args = parser.parse_args()

    config = load_config()
    trade_result = None
    monitor_data = None

    # Launch Theta Terminal if needed
    from core.config import ensure_theta_terminal
    ensure_theta_terminal()

    # Apply dry_run from config if not overridden on CLI
    dry_run = args.dry_run or config.get("strategy", {}).get("dry_run", False)

    log.info(f"Autopilot started - "
             f"scan={args.scan} trade={args.trade} paper={args.paper} "
             f"daemon={args.daemon} monitor={args.monitor} "
             f"report={args.report} full={args.full} dry_run={dry_run}")

    try:
        # --daemon: infinite loop scheduler
        if args.daemon:
            run_daemon(config)
            return

        # --paper: one-shot paper trading pipeline
        if args.paper:
            signals = run_scan()
            if signals is None:
                send_alert("Scan Failed",
                           "Scanner returned no data. Paper trade skipped.",
                           config)
            else:
                trade_result = run_paper_trade(config)
            monitor_data = run_monitor()
            run_report(trade_result=trade_result, config=config,
                       monitor_data=monitor_data)
            log.info("Autopilot finished")
            return

        if args.full or (not args.scan and not args.trade
                         and not args.report and not args.monitor):
            if not (args.scan or args.trade or args.report or args.monitor):
                # No flags = --full
                pass

            if args.full:
                # Full pipeline (IBKR)
                signals = run_scan()
                if signals is None:
                    send_alert("Scan Failed",
                               "Scanner returned no data. Trade skipped.",
                               config)
                else:
                    trade_result = run_trade(dry_run=dry_run, config=config)
                monitor_data = run_monitor()
                run_report(trade_result=trade_result, config=config,
                           monitor_data=monitor_data)
                return

        if args.scan:
            run_scan()

        if args.trade:
            trade_result = run_trade(dry_run=dry_run, config=config)

        if args.monitor:
            monitor_data = run_monitor()

        if args.report:
            run_report(trade_result=trade_result, config=config,
                       monitor_data=monitor_data)

        # If no flags at all, show help
        if not (args.scan or args.trade or args.report
                or args.monitor or args.full):
            parser.print_help()

    except KeyboardInterrupt:
        log.info("Interrupted by user")
    except Exception as ex:
        log.error(f"Autopilot fatal error: {ex}", exc_info=True)
        send_alert("Autopilot Crash", f"Fatal error:\n{ex}", config)
        sys.exit(1)

    log.info("Autopilot finished")


if __name__ == "__main__":
    main()
