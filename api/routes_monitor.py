"""Monitor routes — live P&L, simulation, history, WebSocket live feed."""

import asyncio
import json
import logging
import threading
import time
import pandas as pd
from datetime import datetime

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from core.config import STATE
from core.portfolio import load_portfolio

router = APIRouter(prefix="/api")
log = logging.getLogger(__name__)

# ── Monitor state (encapsulated) ──
_state = {
    "refresh_running": False,
    "refresh_result": None,
    "refresh_error": None,
    "sim_running": False,
    "sim_result": None,
    "sim_error": None,
}

# ── WebSocket live pricing state ──
_ws_clients: set[WebSocket] = set()
_ws_lock = threading.Lock()
_pricing_thread: threading.Thread | None = None
_pricing_stop = threading.Event()
_pricing_force = threading.Event()
_ws_loop: asyncio.AbstractEventLoop | None = None
_last_snapshot: dict | None = None


def _fetch_ibkr_portfolio() -> list:
    """Fetch IBKR portfolio items (runs inside IB thread).

    Returns list of PortfolioItem objects (options with non-zero position).
    """
    from api.ibkr_worker import ib_state
    ib = ib_state.get("ib")
    acct = ib_state.get("account")
    if not ib or not ib.isConnected():
        return []
    items = ib.portfolio(acct)
    # Filter: options or combos, non-zero position
    return [item for item in items
            if getattr(item.contract, "secType", "") in ["OPT", "BAG"]
            and item.position != 0]


def _build_snapshot() -> dict:
    """Price all active positions via IBKR portfolio data.

    Uses ib.portfolio() to get real marketValue and unrealizedPNL per leg,
    then groups by ticker and matches with portfolio.json positions.
    Falls back to stale _last_snapshot if IBKR is disconnected.
    """
    from api.ibkr_worker import ib_state, run_in_ib_thread
    from core.portfolio import ibkr_portfolio_to_positions

    portfolio = load_portfolio()
    active = [p for p in portfolio["positions"] if "exit_date" not in p]

    if not active:
        return {
            "type": "snapshot",
            "positions": [],
            "errors": [],
            "total_unrealized_pnl": 0,
            "n_active": 0,
            "n_priced": 0,
            "date": datetime.now().strftime("%Y-%m-%d"),
            "time": datetime.now().strftime("%H:%M:%S"),
            "timestamp": time.time(),
        }

    # Check IBKR connection
    if not ib_state.get("connected"):
        log.debug("IBKR not connected, returning stale snapshot")
        if _last_snapshot:
            return _last_snapshot
        return {
            "type": "snapshot",
            "positions": [],
            "errors": [p["ticker"] for p in active],
            "total_unrealized_pnl": 0,
            "n_active": len(active),
            "n_priced": 0,
            "date": datetime.now().strftime("%Y-%m-%d"),
            "time": datetime.now().strftime("%H:%M:%S"),
            "timestamp": time.time(),
            "stale": True,
        }

    try:
        ibkr_items = run_in_ib_thread(_fetch_ibkr_portfolio)
    except Exception as exc:
        log.warning("IBKR portfolio fetch error: %s", exc)
        if _last_snapshot:
            return _last_snapshot
        return {
            "type": "snapshot",
            "positions": [],
            "errors": [p["ticker"] for p in active],
            "total_unrealized_pnl": 0,
            "n_active": len(active),
            "n_priced": 0,
            "date": datetime.now().strftime("%Y-%m-%d"),
            "time": datetime.now().strftime("%H:%M:%S"),
            "timestamp": time.time(),
            "stale": True,
        }

    positions, errors = ibkr_portfolio_to_positions(ibkr_items, active)

    total_pnl = sum(p["unrealized_pnl"] for p in positions)
    now = datetime.now()

    snapshot = {
        "type": "snapshot",
        "positions": positions,
        "errors": errors,
        "total_unrealized_pnl": round(total_pnl, 2),
        "n_active": len(active),
        "n_priced": len(positions),
        "date": now.strftime("%Y-%m-%d"),
        "time": now.strftime("%H:%M:%S"),
        "timestamp": time.time(),
    }

    # Save to disk (same format as run_monitor)
    try:
        today_str = now.strftime("%Y%m%d")
        out_file = STATE / f"monitor_{today_str}.json"
        with open(out_file, "w") as f:
            json.dump(snapshot, f, indent=2)
    except Exception as exc:
        log.warning("Snapshot save error: %s", exc)

    return snapshot


async def _broadcast(message: dict):
    """Send JSON message to all connected WS clients."""
    data = json.dumps(message)
    dead: list[WebSocket] = []
    with _ws_lock:
        clients = list(_ws_clients)
    for ws in clients:
        try:
            await ws.send_text(data)
        except Exception:
            dead.append(ws)
    if dead:
        with _ws_lock:
            for ws in dead:
                _ws_clients.discard(ws)


def _pricing_loop_worker():
    """Background thread: price positions every ~2s, broadcast to WS clients."""
    global _last_snapshot
    log.info("WS pricing loop started")
    while not _pricing_stop.is_set():
        try:
            snapshot = _build_snapshot()
            _last_snapshot = snapshot

            if _ws_loop and not _ws_loop.is_closed():
                asyncio.run_coroutine_threadsafe(_broadcast(snapshot), _ws_loop)

        except Exception as exc:
            log.warning("WS pricing cycle error: %s", exc)
            if _ws_loop and not _ws_loop.is_closed():
                err_msg = {"type": "error", "message": str(exc),
                           "timestamp": time.time()}
                asyncio.run_coroutine_threadsafe(_broadcast(err_msg), _ws_loop)

        # Wait 2s or until force-refresh is signalled
        _pricing_force.wait(timeout=2)
        _pricing_force.clear()

    log.info("Pricing loop stopped")


def _start_pricing_loop():
    """Start the background pricing thread if not already running."""
    global _pricing_thread
    with _ws_lock:
        if _pricing_thread and _pricing_thread.is_alive():
            return
        _pricing_stop.clear()
        _pricing_thread = threading.Thread(
            target=_pricing_loop_worker, daemon=True, name="ws-pricing"
        )
        _pricing_thread.start()


def _stop_pricing_loop():
    """Stop the background pricing thread if no clients remain."""
    global _pricing_thread
    with _ws_lock:
        if _ws_clients:
            return  # still have clients
    _pricing_stop.set()
    _pricing_force.set()  # unblock wait
    _pricing_thread = None


@router.websocket("/ws/monitor")
async def ws_monitor(ws: WebSocket):
    """WebSocket endpoint for live portfolio monitoring."""
    global _ws_loop
    await ws.accept()
    _ws_loop = asyncio.get_running_loop()

    with _ws_lock:
        _ws_clients.add(ws)
    log.info("WS client connected (%d total)", len(_ws_clients))

    # Send cached snapshot immediately if available
    if _last_snapshot:
        try:
            await ws.send_text(json.dumps(_last_snapshot))
        except Exception:
            pass

    # Start pricing loop on first client
    _start_pricing_loop()

    try:
        while True:
            data = await ws.receive_text()
            try:
                msg = json.loads(data)
                if msg.get("action") == "refresh":
                    _pricing_force.set()
            except (json.JSONDecodeError, KeyError):
                pass
    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        with _ws_lock:
            _ws_clients.discard(ws)
        log.info("WS client disconnected (%d remaining)", len(_ws_clients))
        _stop_pricing_loop()


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
    """Refresh pricing: IBKR direct if connected, ThetaData REST fallback."""
    if _state["refresh_running"]:
        return {"status": "already_running"}

    _state["refresh_running"] = True
    _state["refresh_result"] = None
    _state["refresh_error"] = None

    def do_refresh():
        try:
            from api.ibkr_worker import ib_state

            if ib_state.get("connected"):
                # IBKR connected: use _build_snapshot() directly
                result = _build_snapshot()
                if result is None:
                    _state["refresh_result"] = {
                        "positions": [], "errors": [],
                        "total_unrealized_pnl": 0,
                        "message": "No active positions",
                    }
                else:
                    _state["refresh_result"] = result
            else:
                # IBKR not connected: fallback to ThetaData REST
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
