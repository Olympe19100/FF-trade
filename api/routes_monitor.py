"""Monitor routes — live P&L, simulation, history."""

import json
import threading
import pandas as pd
from datetime import datetime

from fastapi import APIRouter

from core.config import STATE
from core.portfolio import load_portfolio

router = APIRouter(prefix="/api")

# ── Monitor state (encapsulated) ──
_state = {
    "refresh_running": False,
    "refresh_result": None,
    "refresh_error": None,
    "sim_running": False,
    "sim_result": None,
    "sim_error": None,
}


@router.get("/monitor")
async def api_monitor():
    """Return portfolio positions + latest cached price snapshot (fast, no API call)."""
    portfolio = load_portfolio()
    active = [p for p in portfolio["positions"] if "exit_date" not in p]

    today = datetime.now()
    for p in active:
        try:
            fe = pd.Timestamp(p["front_exp"])
            p["days_to_exp"] = (fe - pd.Timestamp(today)).days
        except Exception:
            p["days_to_exp"] = None

    cached_prices = {}
    cached_date = None
    monitor_files = sorted(STATE.glob("monitor_*.json"), reverse=True)
    if monitor_files:
        try:
            with open(monitor_files[0]) as f:
                snapshot = json.load(f)
            for pos in snapshot.get("positions", []):
                cached_prices[pos["ticker"]] = pos
            cached_date = snapshot.get("date")
        except Exception:
            pass

    return {
        "active": active,
        "cached_prices": cached_prices,
        "cached_date": cached_date,
        "n_active": len(active),
        "refresh_running": _state["refresh_running"],
    }


@router.post("/monitor/refresh")
async def api_monitor_refresh():
    """Start background ThetaData/EODHD pricing of all active positions."""
    if _state["refresh_running"]:
        return {"status": "already_running"}

    _state["refresh_running"] = True
    _state["refresh_result"] = None
    _state["refresh_error"] = None

    def do_refresh():
        try:
            from core.autopilot import run_monitor
            result = run_monitor()
            if result is None:
                _state["refresh_result"] = {
                    "positions": [], "errors": [],
                    "total_unrealized_pnl": 0,
                    "message": "No active positions",
                }
            else:
                _state["refresh_result"] = result
        except Exception as ex:
            _state["refresh_error"] = str(ex)
        finally:
            _state["refresh_running"] = False

    threading.Thread(target=do_refresh, daemon=True).start()
    return {"status": "running"}


@router.get("/monitor/refresh/status")
async def api_monitor_refresh_status():
    """Poll price refresh progress."""
    if _state["refresh_running"]:
        return {"status": "running"}
    if _state["refresh_error"]:
        return {"status": "error", "error": _state["refresh_error"]}
    if _state["refresh_result"]:
        return {"status": "done", "result": _state["refresh_result"]}
    return {"status": "idle"}


@router.get("/monitor/history")
async def api_monitor_history():
    """Return all monitor snapshots (live + sim) sorted by date."""
    snapshots = []
    for pattern in ["monitor_*.json", "sim_monitor_*.json"]:
        for f in STATE.glob(pattern):
            try:
                with open(f) as fh:
                    data = json.load(fh)
                data["file"] = f.name
                data["is_sim"] = f.name.startswith("sim_")
                snapshots.append(data)
            except Exception:
                continue
    snapshots.sort(key=lambda x: x.get("date", ""), reverse=True)
    return {"snapshots": snapshots, "count": len(snapshots)}


@router.post("/monitor/simulate")
async def api_monitor_simulate():
    """Run simulation: load historical signals, build portfolio, price via ThetaData/EODHD."""
    if _state["sim_running"]:
        return {"status": "already_running"}

    _state["sim_running"] = True
    _state["sim_result"] = None
    _state["sim_error"] = None

    def do_simulate():
        try:
            from tools.sim_monitor import (
                load_all_signals, build_simulated_portfolio, price_positions
            )

            signals = load_all_signals()
            if signals.empty:
                _state["sim_error"] = "No signal files found"
                return

            positions = build_simulated_portfolio(signals)
            if not positions:
                _state["sim_error"] = "No active positions (all expired)"
                return

            results, errors = price_positions(positions)

            total_pnl = sum(r["unrealized_pnl"] for r in results) if results else 0
            total_invested = sum(
                r["entry_cost"] * 100 * r["contracts"] for r in results
            ) if results else 0

            n_win = sum(1 for r in results if r["unrealized_pnl"] > 0)

            today_str = datetime.now().strftime("%Y%m%d")
            snapshot = {
                "date": datetime.now().strftime("%Y-%m-%d"),
                "simulation": True,
                "positions": results,
                "errors": errors,
                "total_unrealized_pnl": round(total_pnl, 2),
                "total_invested": round(total_invested, 2),
            }
            out_file = STATE / f"sim_monitor_{today_str}.json"
            with open(out_file, "w") as f:
                json.dump(snapshot, f, indent=2)

            _state["sim_result"] = {
                "positions": results,
                "errors": errors,
                "total_unrealized_pnl": round(total_pnl, 2),
                "total_invested": round(total_invested, 2),
                "n_positions": len(positions),
                "n_priced": len(results),
                "n_win": n_win,
                "n_loss": len(results) - n_win,
                "win_rate": round(n_win / len(results) * 100, 1) if results else 0,
                "snapshot_file": out_file.name,
            }
        except Exception as ex:
            _state["sim_error"] = str(ex)
        finally:
            _state["sim_running"] = False

    threading.Thread(target=do_simulate, daemon=True).start()
    return {"status": "running"}


@router.get("/monitor/simulate/status")
async def api_monitor_simulate_status():
    """Poll simulation progress. Also loads from disk if no in-memory result."""
    if _state["sim_running"]:
        return {"status": "running"}
    if _state["sim_error"]:
        return {"status": "error", "error": _state["sim_error"]}
    if _state["sim_result"]:
        return {"status": "done", "result": _state["sim_result"]}

    # No in-memory result — try loading latest sim snapshot from disk
    sim_files = sorted(STATE.glob("sim_monitor_*.json"), reverse=True)
    if sim_files:
        try:
            with open(sim_files[0]) as f:
                snapshot = json.load(f)
            positions = snapshot.get("positions", [])
            errors = snapshot.get("errors", [])
            total_pnl = snapshot.get("total_unrealized_pnl", 0)
            total_invested = snapshot.get("total_invested", 0)
            n_win = sum(1 for p in positions if (p.get("unrealized_pnl", 0)) > 0)

            return {"status": "done", "result": {
                "positions": positions,
                "errors": errors,
                "total_unrealized_pnl": total_pnl,
                "total_invested": total_invested,
                "n_positions": len(positions) + len(errors),
                "n_priced": len(positions),
                "n_win": n_win,
                "n_loss": len(positions) - n_win,
                "win_rate": round(n_win / len(positions) * 100, 1) if positions else 0,
                "snapshot_file": sim_files[0].name,
                "snapshot_date": snapshot.get("date"),
            }}
        except Exception:
            pass

    return {"status": "idle"}
