"""Scanner routes — run scans, check status, scan+enter, auto-manage."""

import sys
import subprocess
import threading
import numpy as np
import pandas as pd
from datetime import datetime

from fastapi import APIRouter, HTTPException

from core.config import ROOT, MAX_POSITIONS, BA_PCT_MAX, FF_THRESHOLD_DEFAULT
from core.portfolio import (
    load_latest_signals, load_portfolio, save_portfolio,
    add_position, load_trade_history, compute_kelly, size_portfolio,
)
from core.trader import enter_new_positions, close_expiring_positions
from api.models import EnterRequest, AutoManageRequest
from api.ibkr_worker import ib_state, order_log, run_in_ib_thread, log_order

router = APIRouter(prefix="/api")

# ── Scanner state ──
_state = {
    "running": False,
    "start_time": None,
    "last_completed": None,
    "last_duration": None,
    "last_signals_count": None,
}


@router.post("/scan")
async def api_scan():
    """Run scanner as a background subprocess."""
    if _state["running"]:
        return {"status": "already_running"}

    _state["running"] = True
    _state["start_time"] = datetime.now().isoformat()

    log_order("scan", "Scanner started...", "running")

    def run_scanner_subprocess():
        try:
            scanner_path = str(ROOT / "core" / "scanner.py")
            result = subprocess.run(
                [sys.executable, scanner_path],
                capture_output=True, text=True,
                timeout=600,
                cwd=str(ROOT),
            )

            signals_count = 0
            for line in result.stdout.split("\n"):
                if "signals saved" in line.lower() or "signal" in line.lower():
                    for word in line.split():
                        try:
                            signals_count = int(word)
                            break
                        except ValueError:
                            continue

            duration = (datetime.now() - datetime.fromisoformat(
                _state["start_time"]
            )).total_seconds()

            _state["running"] = False
            _state["last_completed"] = datetime.now().isoformat()
            _state["last_duration"] = round(duration, 1)
            _state["last_signals_count"] = signals_count

            log_order("scan",
                f"Scanner completed: {signals_count} signals in {duration:.0f}s",
                "ok" if result.returncode == 0 else "error")

            if result.returncode != 0:
                log_order("scan", f"Scanner stderr: {result.stderr[:500]}", "error")

        except subprocess.TimeoutExpired:
            _state["running"] = False
            log_order("scan", "Scanner timed out after 600s", "error")
        except Exception as ex:
            _state["running"] = False
            log_order("scan", f"Scanner error: {ex}", "error")

    threading.Thread(target=run_scanner_subprocess, daemon=True).start()
    return {"status": "running"}


@router.get("/scanner_status")
async def api_scanner_status():
    """Get scanner state for UI polling."""
    return {
        "running": _state["running"],
        "start_time": _state["start_time"],
        "last_completed": _state["last_completed"],
        "last_duration": _state["last_duration"],
        "last_signals_count": _state["last_signals_count"],
    }


@router.post("/scan_and_enter")
async def api_scan_and_enter(req: EnterRequest):
    """Run scanner then automatically place orders."""
    if not ib_state["connected"] or not ib_state["ib"]:
        raise HTTPException(status_code=400, detail="Not connected to IBKR")

    if _state["running"]:
        raise HTTPException(status_code=400, detail="Scanner already running")

    _state["running"] = True
    _state["start_time"] = datetime.now().isoformat()

    log_order("scan_and_enter", "Scan & Execute: starting scanner...", "running")

    def run_scan_then_enter():
        try:
            scanner_path = str(ROOT / "core" / "scanner.py")
            result = subprocess.run(
                [sys.executable, scanner_path],
                capture_output=True, text=True,
                timeout=600,
                cwd=str(ROOT),
            )

            _state["running"] = False
            _state["last_completed"] = datetime.now().isoformat()

            if result.returncode != 0:
                log_order("scan_and_enter",
                    f"Scanner failed: {result.stderr[:300]}", "error")
                return

            log_order("scan_and_enter",
                "Scanner done, starting optimal execution...", "running")

            ib = ib_state["ib"]
            acct = ib_state["account"]
            results = run_in_ib_thread(
                enter_new_positions, ib, acct, req.max_new
            )

            entered = results.get("entered", 0) if results else 0
            log_order("scan_and_enter",
                f"Scan & Execute complete: {entered} positions filled")
        except Exception as ex:
            _state["running"] = False
            log_order("scan_and_enter", f"Error: {ex}", "error")

    threading.Thread(target=run_scan_then_enter, daemon=True).start()
    return {"status": "running"}


# ── Auto-Manage state ──
_manage_state = {
    "running": False,
    "last_result": None,
}


@router.post("/auto_manage")
async def api_auto_manage(req: AutoManageRequest | None = None):
    """One-click portfolio management: scan, prune expired, filter ba_pct, add new."""
    if _state["running"] or _manage_state["running"]:
        return {"status": "already_running"}

    if req is None:
        req = AutoManageRequest()

    _state["running"] = True
    _manage_state["running"] = True
    _state["start_time"] = datetime.now().isoformat()
    _manage_state["last_result"] = None

    log_order("auto_manage", "Auto-Manage started...", "running")

    def run_auto_manage():
        result = {"removed": [], "added": [], "filtered_ba": [], "errors": [], "n_signals": 0}
        try:
            # 1. Run scanner subprocess
            scanner_path = str(ROOT / "core" / "scanner.py")
            proc = subprocess.run(
                [sys.executable, scanner_path],
                capture_output=True, text=True,
                timeout=600,
                cwd=str(ROOT),
            )
            if proc.returncode != 0:
                result["errors"].append(f"Scanner failed: {proc.stderr[:300]}")
                log_order("auto_manage", f"Scanner failed: {proc.stderr[:200]}", "error")
                _state["running"] = False
                _manage_state["running"] = False
                _manage_state["last_result"] = result
                return

            # 2. Load signals, filter by ba_pct
            signals = load_latest_signals()
            result["n_signals"] = len(signals)

            if not signals.empty and "ba_pct" in signals.columns:
                rejected = signals[signals["ba_pct"] > BA_PCT_MAX]
                result["filtered_ba"] = rejected["ticker"].tolist()
                signals = signals[signals["ba_pct"] <= BA_PCT_MAX]

            # Double calendars only — reject singles (no put leg)
            if not signals.empty:
                signals = signals[signals["dbl_cost"].notna() & (signals["dbl_cost"] > 0)]

            # 3. Load portfolio, identify expired positions
            portfolio = load_portfolio()
            today_str = datetime.now().strftime("%Y-%m-%d")
            active = [p for p in portfolio["positions"] if "exit_date" not in p]

            expired = []
            for p in active:
                try:
                    if p.get("front_exp", "9999-99-99") <= today_str:
                        expired.append(p)
                except Exception:
                    pass

            # 4. Remove expired from portfolio (mark as closed)
            for p in expired:
                p["exit_date"] = today_str
                p["exit_reason"] = "expired"
                result["removed"].append(p["ticker"])

            if expired:
                save_portfolio(portfolio)
                log_order("auto_manage",
                    f"Removed {len(expired)} expired: {', '.join(result['removed'])}")

            # 5. Count available slots
            active_after = [p for p in portfolio["positions"] if "exit_date" not in p]
            active_tickers = {p["ticker"] for p in active_after}
            available_slots = max(0, MAX_POSITIONS - len(active_after))

            if available_slots == 0 or signals.empty:
                log_order("auto_manage",
                    f"No slots ({len(active_after)}/{MAX_POSITIONS}) or no signals")
                _state["running"] = False
                _manage_state["running"] = False
                _manage_state["last_result"] = result
                return

            # 6. Filter out already-held tickers + 2-pass entry (matches backtest)
            candidates = signals[~signals["ticker"].isin(active_tickers)]
            candidates = candidates[candidates["ff"] > 0]
            candidates = candidates.sort_values("ff", ascending=False).drop_duplicates(
                subset=["ticker"], keep="first"
            )
            # Pass 1: priority (FF >= threshold)
            priority = candidates[candidates["ff"] >= FF_THRESHOLD_DEFAULT].head(available_slots)
            remaining_slots = available_slots - len(priority)
            # Pass 2: fill remaining with best FF > 0
            if remaining_slots > 0:
                fill = candidates[~candidates.index.isin(priority.index)].head(remaining_slots)
                eligible = pd.concat([priority, fill])
            else:
                eligible = priority

            if eligible.empty:
                log_order("auto_manage", "No eligible signals (all held or filtered)")
                _state["running"] = False
                _manage_state["running"] = False
                _manage_state["last_result"] = result
                return

            # 7. Size positions via Kelly
            returns = load_trade_history()
            kelly_f = compute_kelly(returns)

            signals_info = []
            sig_rows = []
            for _, r in eligible.iterrows():
                has_dbl = pd.notna(r.get("dbl_cost")) and r["dbl_cost"] > 0
                cps = r["dbl_cost"] if has_dbl else r["call_cost"]
                n_legs = 4 if has_dbl else 2
                signals_info.append((r["ticker"], cps, n_legs))
                sig_rows.append(r)

            sizing = size_portfolio(signals_info, kelly_f, req.account_value)

            # 8. Add to portfolio
            for (ticker, contracts, deployed), row in zip(sizing, sig_rows):
                if contracts <= 0:
                    continue
                has_dbl = pd.notna(row.get("dbl_cost")) and row["dbl_cost"] > 0
                cps = row["dbl_cost"] if has_dbl else row["call_cost"]
                spread_type = "double" if has_dbl else "single"
                n_legs = 4 if has_dbl else 2
                put_strike = row.get("put_strike") if has_dbl else None

                add_position(
                    portfolio,
                    ticker=ticker,
                    combo=row["combo"],
                    strike=row["strike"],
                    front_exp=str(row["front_exp"]),
                    back_exp=str(row["back_exp"]),
                    contracts=contracts,
                    cost_per_share=float(cps),
                    spread_type=spread_type,
                    ff=float(row["ff"]),
                    n_legs=n_legs,
                    put_strike=float(put_strike) if put_strike is not None and pd.notna(put_strike) else None,
                )
                result["added"].append({"ticker": ticker, "contracts": contracts,
                                        "cost": round(float(cps), 2)})

            save_portfolio(portfolio)
            log_order("auto_manage",
                f"Added {len(result['added'])} positions to portfolio")

            # 9. If IBKR connected: close expiring + enter new
            if ib_state["connected"] and ib_state["ib"]:
                try:
                    ib = ib_state["ib"]
                    acct = ib_state["account"]
                    if expired:
                        run_in_ib_thread(close_expiring_positions, ib, acct)
                        log_order("auto_manage", "Closed expiring via IBKR")
                    ibkr_result = run_in_ib_thread(
                        enter_new_positions, ib, acct, len(result["added"])
                    )
                    entered = ibkr_result.get("entered", 0) if ibkr_result else 0
                    log_order("auto_manage", f"IBKR execution: {entered} filled")
                except Exception as ex:
                    result["errors"].append(f"IBKR: {ex}")
                    log_order("auto_manage", f"IBKR error: {ex}", "error")

            log_order("auto_manage",
                f"Done: removed={len(result['removed'])}, added={len(result['added'])}, "
                f"ba_filtered={len(result['filtered_ba'])}, signals={result['n_signals']}")

        except subprocess.TimeoutExpired:
            result["errors"].append("Scanner timed out after 600s")
            log_order("auto_manage", "Scanner timed out", "error")
        except Exception as ex:
            result["errors"].append(str(ex))
            log_order("auto_manage", f"Error: {ex}", "error")
        finally:
            _state["running"] = False
            _manage_state["running"] = False
            _state["last_completed"] = datetime.now().isoformat()
            _manage_state["last_result"] = result

    threading.Thread(target=run_auto_manage, daemon=True).start()
    return {"status": "running"}


@router.get("/auto_manage_result")
async def api_auto_manage_result():
    """Get the result of the last auto-manage run."""
    return {
        "running": _manage_state["running"],
        "result": _manage_state["last_result"],
    }
