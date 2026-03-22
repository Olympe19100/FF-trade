"""
Calendar Spread Trading — Web Application

FastAPI backend + HTML/CSS/JS frontend.
Single command: python app.py -> opens browser at http://localhost:8000

Reuses all logic from trader.py and scanner.py.
"""

import json
import sys
import subprocess
import queue
import threading
import webbrowser
import asyncio
import concurrent.futures
import numpy as np
import pandas as pd
from pathlib import Path
from datetime import datetime

# Fix Python 3.14 asyncio before importing ib_insync
try:
    asyncio.get_event_loop()
except RuntimeError:
    asyncio.set_event_loop(asyncio.new_event_loop())

from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel
from typing import Optional
import uvicorn

# Import trading functions
from core.trader import (
    connect_ibkr, verify_paper, get_account_info,
    load_latest_signals, load_portfolio, save_portfolio,
    load_trade_history, compute_kelly, size_portfolio,
    cost_per_contract, enter_new_positions, close_expiring_positions,
    MAX_POSITIONS, MAX_CONTRACTS, KELLY_FRAC, CONTRACT_MULT,
    SLIPPAGE_BUFFER, COMMISSION_LEG, DEFAULT_ALLOC, MIN_KELLY_TRADES,
)
from core.risk import compute_risk
from core.straddle import compute_straddle_analytics

# ── Paths ──
ROOT   = Path(r"C:\Users\ANTEC MSI\Desktop\pro\Option trading")
OUTPUT = ROOT / "output"
STATIC = ROOT / "static"
BACKTEST_TRADES = OUTPUT / "backtest_trades.csv"
RISK_FREE_RATE  = 0.045

# ── Global IBKR state ──
ib_state = {
    "ib": None,
    "connected": False,
    "account": None,
    "host": "127.0.0.1",
    "port": 4002,
}

# ── Order execution log (in-memory) ──
order_log = []

app = FastAPI(title="Calendar Spread Trading")
app.mount("/static", StaticFiles(directory=str(STATIC)), name="static")


# ═══════════════════════════════════════════════════════════
#  PERSISTENT IB WORKER THREAD
#
#  ib_insync uses asyncio internally. Its synchronous API calls
#  loop.run_until_complete(), which conflicts with uvicorn's loop.
#  Solution: a single persistent background thread with its own
#  event loop. All IB operations are dispatched to this thread
#  via a queue, ensuring the ib object always runs on its
#  original loop.
# ═══════════════════════════════════════════════════════════

_ib_queue = queue.Queue()


def _ib_worker():
    """Persistent IB thread — processes one request at a time."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    while True:
        future, func, args, kwargs = _ib_queue.get()
        try:
            result = func(*args, **kwargs)
            future.set_result(result)
        except Exception as ex:
            future.set_exception(ex)


# Start at module load
threading.Thread(target=_ib_worker, daemon=True).start()


def _run_in_ib_thread(func, *args, **kwargs):
    """Dispatch a function to the persistent IB thread and wait for result."""
    future = concurrent.futures.Future()
    _ib_queue.put((future, func, args, kwargs))
    return future.result(timeout=1800)  # 30 min for optimal execution


# ═══════════════════════════════════════════════════════════
#  MODELS
# ═══════════════════════════════════════════════════════════

class ConnectRequest(BaseModel):
    host: str = "127.0.0.1"
    port: int = 4002

class EnterRequest(BaseModel):
    max_new: Optional[int] = None
    tickers: Optional[list[str]] = None

class SizingRequest(BaseModel):
    account_value: Optional[float] = None


# ═══════════════════════════════════════════════════════════
#  ROUTES — PAGES
# ═══════════════════════════════════════════════════════════

@app.get("/")
async def index():
    return FileResponse(str(STATIC / "index.html"))


# ═══════════════════════════════════════════════════════════
#  ROUTES — READ APIs
# ═══════════════════════════════════════════════════════════

@app.get("/api/status")
async def api_status():
    """Global status: connection + portfolio + kelly summary."""
    portfolio = load_portfolio()
    active = [p for p in portfolio["positions"] if "exit_date" not in p]

    # Kelly
    returns = load_trade_history()
    kelly_f = compute_kelly(returns)

    # Account info (from IBKR if connected)
    account_value = 0
    buying_power = 0
    if ib_state["connected"] and ib_state["ib"]:
        try:
            info = _run_in_ib_thread(
                get_account_info, ib_state["ib"], ib_state["account"]
            )
            account_value = info.get("NetLiquidation", 0)
            buying_power = info.get("BuyingPower", 0)
        except Exception:
            pass

    total_deployed = sum(p.get("total_deployed", 0) for p in active)

    return {
        "connected": ib_state["connected"],
        "account": ib_state["account"],
        "host": ib_state["host"],
        "port": ib_state["port"],
        "account_value": account_value,
        "buying_power": buying_power,
        "n_active": len(active),
        "max_positions": MAX_POSITIONS,
        "total_deployed": total_deployed,
        "kelly_f": kelly_f,
        "kelly_target": kelly_f * account_value if account_value else 0,
        "kelly_trades": len(returns),
        "last_updated": portfolio.get("last_updated"),
    }


@app.get("/api/signals")
async def api_signals():
    """Latest scanner signals."""
    signals = load_latest_signals()
    if signals.empty:
        return {"signals": [], "file": None, "count": 0}

    file_name = None
    files = sorted(OUTPUT.glob("signals_*.csv"), reverse=True)
    if files:
        file_name = files[0].name

    records = signals.to_dict(orient="records")
    # Clean NaN for JSON
    for r in records:
        for k, v in r.items():
            if isinstance(v, float) and (np.isnan(v) or np.isinf(v)):
                r[k] = None

    return {"signals": records, "file": file_name, "count": len(records)}


@app.get("/api/sizing")
async def api_sizing(account_value: float = 1_023_443):
    """Kelly-optimized sizing for latest signals."""
    signals = load_latest_signals()
    if signals.empty:
        return {"sizing": [], "kelly_f": 0, "kelly_target": 0, "total_deployed": 0}

    portfolio = load_portfolio()
    active_tickers = {p["ticker"] for p in portfolio["positions"] if "exit_date" not in p}
    eligible = signals[~signals["ticker"].isin(active_tickers)].head(MAX_POSITIONS)

    returns = load_trade_history()
    kelly_f = compute_kelly(returns)
    kelly_target = kelly_f * account_value

    signals_info = []
    sig_details = []
    for _, r in eligible.iterrows():
        has_dbl = pd.notna(r.get("dbl_cost")) and r["dbl_cost"] > 0
        cps = r["dbl_cost"] if has_dbl else r["call_cost"]
        n_legs = 4 if has_dbl else 2
        signals_info.append((r["ticker"], cps, n_legs))
        sig_details.append(r.to_dict())

    sizing = size_portfolio(signals_info, kelly_f, account_value)
    total_deployed = sum(d for _, _, d in sizing)

    result = []
    for (ticker, contracts, deployed), detail in zip(sizing, sig_details):
        # Clean NaN
        for k, v in detail.items():
            if isinstance(v, float) and (np.isnan(v) or np.isinf(v)):
                detail[k] = None
        result.append({
            "ticker": ticker,
            "contracts": contracts,
            "deployed": round(deployed, 2),
            "ff": detail.get("ff", 0),
            **{k: detail[k] for k in ["combo", "strike", "stock_px", "front_exp",
               "back_exp", "front_iv", "back_iv", "call_cost", "put_cost",
               "dbl_cost", "volume"] if k in detail},
        })

    return {
        "sizing": result,
        "kelly_f": round(kelly_f, 6),
        "kelly_target": round(kelly_target, 2),
        "total_deployed": round(total_deployed, 2),
        "gap": round(total_deployed - kelly_target, 2),
        "gap_pct": round((total_deployed / kelly_target - 1) * 100, 2) if kelly_target > 0 else 0,
        "account_value": account_value,
        "n_positions": len(result),
        "alloc_per_pos": round(kelly_target / len(result), 2) if result else 0,
    }


@app.get("/api/portfolio")
async def api_portfolio():
    """Active and closed positions."""
    portfolio = load_portfolio()
    active = [p for p in portfolio["positions"] if "exit_date" not in p]
    closed = [p for p in portfolio["positions"] if "exit_date" in p]

    today = datetime.now()
    for p in active:
        try:
            fe = pd.Timestamp(p["front_exp"])
            p["days_to_exp"] = (fe - pd.Timestamp(today)).days
        except Exception:
            p["days_to_exp"] = None

    return {
        "active": active,
        "closed": closed,
        "n_active": len(active),
        "total_deployed": sum(p.get("total_deployed", 0) for p in active),
        "last_updated": portfolio.get("last_updated"),
    }


@app.get("/api/trades")
async def api_trades():
    """Closed trade history."""
    from core.trader import TRADES_FILE
    trades = []
    if TRADES_FILE.exists():
        with open(TRADES_FILE) as f:
            data = json.load(f)
        trades = data.get("trades", [])

    total_pnl = sum(t.get("pnl", 0) for t in trades)
    wins = sum(1 for t in trades if t.get("pnl", 0) > 0)
    wr = wins / len(trades) if trades else 0

    return {
        "trades": trades,
        "count": len(trades),
        "total_pnl": round(total_pnl, 2),
        "win_rate": round(wr, 4),
    }


@app.get("/api/kelly")
async def api_kelly(account_value: float = 1_023_443):
    """Kelly criterion stats."""
    returns = load_trade_history()
    kelly_f = compute_kelly(returns)

    arr = np.array(returns) if returns else np.array([0])
    mu = float(arr.mean())
    var = float(arr.var())
    std = float(arr.std())
    kelly_full = mu / var if var > 0 else 0

    kelly_target = kelly_f * account_value
    cash_reserve = account_value - kelly_target
    rf_income = cash_reserve * RISK_FREE_RATE

    # Backtest count vs live count
    bt_count = 0
    if BACKTEST_TRADES.exists():
        bt_count = len(pd.read_csv(str(BACKTEST_TRADES)).dropna(subset=["return_pct"]))

    from core.trader import TRADES_FILE
    live_count = 0
    if TRADES_FILE.exists():
        with open(TRADES_FILE) as f:
            live_count = len(json.load(f).get("trades", []))

    return {
        "kelly_f": round(kelly_f, 6),
        "kelly_full": round(kelly_full, 6),
        "mu": round(mu, 6),
        "var": round(var, 6),
        "std": round(std, 6),
        "n_trades": len(returns),
        "bt_trades": bt_count,
        "live_trades": live_count,
        "kelly_target": round(kelly_target, 2),
        "alloc_per_pos": round(kelly_target / MAX_POSITIONS, 2),
        "cash_reserve": round(cash_reserve, 2),
        "rf_rate": RISK_FREE_RATE,
        "rf_income": round(rf_income, 2),
        "account_value": account_value,
        "spread_cagr": 0.376,
        "combined_cagr": round(0.376 + RISK_FREE_RATE * (1 - kelly_f), 4),
    }


@app.get("/api/risk")
async def api_risk(account_value: float = 1_023_443):
    """Institutional risk analytics — MC simulation, VaR/CVaR, edge persistence."""
    result = compute_risk(account_value)
    return result


@app.get("/api/straddle")
async def api_straddle():
    """Earnings Vol Ramp — pre-earnings long straddle analytics."""
    result = compute_straddle_analytics()
    return result


@app.get("/api/orders")
async def api_orders():
    """Order execution log + live IBKR orders."""
    # Live IBKR orders
    ibkr_data = {"open": [], "filled": [], "positions": []}
    if ib_state["connected"] and ib_state["ib"]:
        try:
            ibkr_data = _run_in_ib_thread(_get_ibkr_orders)
        except Exception as ex:
            ibkr_data["error"] = str(ex)

    return {
        "log": order_log[-100:],
        "log_count": len(order_log),
        "open_orders": ibkr_data.get("open", []),
        "filled_orders": ibkr_data.get("filled", []),
        "positions": ibkr_data.get("positions", []),
    }


def _get_ibkr_orders():
    """Fetch ALL orders from IBKR — including from other API clients (runs in IB thread).

    Uses reqAllOpenOrders() to get pending orders from ALL clients,
    reqExecutions() for recent fills, and ib.portfolio() for live positions.
    """
    ib = ib_state["ib"]
    if not ib or not ib.isConnected():
        return {"open": [], "filled": [], "positions": []}

    # Request ALL open orders (from all API clients, not just ours)
    all_open = ib.reqAllOpenOrders()
    ib.sleep(1)

    open_orders = []
    for trade in all_open:
        o = trade.order
        s = trade.orderStatus
        c = trade.contract
        open_orders.append({
            "orderId": o.orderId,
            "permId": o.permId,
            "symbol": c.symbol,
            "secType": c.secType,
            "action": o.action,
            "qty": int(o.totalQuantity),
            "filled": int(s.filled) if s.filled else 0,
            "remaining": int(s.remaining) if s.remaining else int(o.totalQuantity),
            "orderType": o.orderType,
            "limitPrice": float(o.lmtPrice) if o.lmtPrice else None,
            "avgFillPrice": float(s.avgFillPrice) if s.avgFillPrice else None,
            "status": s.status if s.status else "Unknown",
            "tif": o.tif,
        })

    # Request recent executions (fills)
    from ib_insync import ExecutionFilter
    exec_filter = ExecutionFilter()
    exec_filter.acctCode = ib_state["account"] or ""
    fills = ib.reqExecutions(exec_filter)
    ib.sleep(1)

    filled_orders = []
    for fill in fills:
        c = fill.contract
        e = fill.execution
        filled_orders.append({
            "orderId": e.orderId,
            "permId": e.permId,
            "symbol": c.symbol,
            "secType": c.secType,
            "action": e.side,
            "qty": int(e.shares),
            "avgFillPrice": float(e.avgPrice),
            "execTime": e.time.isoformat() if hasattr(e.time, 'isoformat') else str(e.time),
            "exchange": e.exchange,
        })

    # Live portfolio positions
    positions = []
    for item in ib.portfolio(ib_state["account"]):
        c = item.contract
        positions.append({
            "symbol": c.symbol,
            "secType": c.secType,
            "conId": c.conId,
            "position": float(item.position),
            "marketPrice": float(item.marketPrice),
            "marketValue": float(item.marketValue),
            "avgCost": float(item.averageCost),
            "unrealizedPnl": float(item.unrealizedPNL),
            "realizedPnl": float(item.realizedPNL),
        })

    return {
        "open": open_orders,
        "filled": filled_orders,
        "positions": positions,
    }


# ═══════════════════════════════════════════════════════════
#  ROUTES — ACTION APIs
# ═══════════════════════════════════════════════════════════

@app.post("/api/connect")
async def api_connect(req: ConnectRequest):
    """Connect to IBKR."""
    if ib_state["connected"]:
        return {"status": "already_connected", "account": ib_state["account"]}

    def do_connect():
        ib = connect_ibkr(req.host, req.port, client_id=10)
        acct = verify_paper(ib)
        info = get_account_info(ib, acct)
        return ib, acct, info

    try:
        ib, acct, info = _run_in_ib_thread(do_connect)
        ib_state["ib"] = ib
        ib_state["connected"] = True
        ib_state["account"] = acct
        ib_state["host"] = req.host
        ib_state["port"] = req.port

        order_log.append({
            "time": datetime.now().isoformat(),
            "type": "connect",
            "message": f"Connected to {acct} @ {req.host}:{req.port}",
            "status": "ok",
        })

        return {
            "status": "connected",
            "account": acct,
            "account_value": info.get("NetLiquidation", 0),
            "buying_power": info.get("BuyingPower", 0),
            "currency": info.get("base_currency", "USD"),
        }
    except Exception as ex:
        order_log.append({
            "time": datetime.now().isoformat(),
            "type": "connect",
            "message": f"Connection failed: {ex}",
            "status": "error",
        })
        raise HTTPException(status_code=500, detail=str(ex))


@app.post("/api/disconnect")
async def api_disconnect():
    """Disconnect from IBKR."""
    if ib_state["ib"] and ib_state["connected"]:
        try:
            _run_in_ib_thread(lambda: ib_state["ib"].disconnect())
        except Exception:
            pass
    ib_state["ib"] = None
    ib_state["connected"] = False
    ib_state["account"] = None
    order_log.append({
        "time": datetime.now().isoformat(),
        "type": "disconnect",
        "message": "Disconnected from IBKR",
        "status": "ok",
    })
    return {"status": "disconnected"}


@app.post("/api/enter")
async def api_enter(req: EnterRequest):
    """Place new calendar spread orders using optimal execution."""
    if not ib_state["connected"] or not ib_state["ib"]:
        raise HTTPException(status_code=400, detail="Not connected to IBKR")

    from core.trader import check_optimal_window
    is_optimal, time_msg = check_optimal_window()

    order_log.append({
        "time": datetime.now().isoformat(),
        "type": "enter",
        "message": f"Starting optimal execution... {time_msg}",
        "status": "running",
    })

    try:
        ib = ib_state["ib"]
        acct = ib_state["account"]
        results = _run_in_ib_thread(enter_new_positions, ib, acct, req.max_new)

        # Log slippage details
        if results and results.get("slippage_log"):
            for s in results["slippage_log"]:
                if s["result"] == "full":
                    order_log.append({
                        "time": datetime.now().isoformat(),
                        "type": "fill",
                        "message": (f"{s['ticker']}: {s['method']} fill @ "
                                    f"${s['fill_cost']:.2f} "
                                    f"(EODHD ${s['eodhd_mid']:.2f}, "
                                    f"slip={s['slippage']:+.3f})"),
                        "status": "ok",
                    })

        entered = results.get("entered", 0) if results else 0
        partial = results.get("partial", 0) if results else 0
        not_filled = results.get("not_filled", 0) if results else 0

        order_log.append({
            "time": datetime.now().isoformat(),
            "type": "enter",
            "message": (f"Execution complete: {entered} filled, "
                        f"{partial} partial, {not_filled} failed"),
            "status": "ok",
        })
        return {"status": "ok", "results": results}
    except Exception as ex:
        order_log.append({
            "time": datetime.now().isoformat(),
            "type": "enter",
            "message": f"Error: {ex}",
            "status": "error",
        })
        raise HTTPException(status_code=500, detail=str(ex))


@app.post("/api/close")
async def api_close():
    """Close expiring positions."""
    if not ib_state["connected"] or not ib_state["ib"]:
        raise HTTPException(status_code=400, detail="Not connected to IBKR")

    try:
        _run_in_ib_thread(close_expiring_positions, ib_state["ib"], ib_state["account"])
        order_log.append({
            "time": datetime.now().isoformat(),
            "type": "close",
            "message": "Close expiring complete",
            "status": "ok",
        })
        return {"status": "ok"}
    except Exception as ex:
        raise HTTPException(status_code=500, detail=str(ex))


@app.post("/api/cancel_order")
async def api_cancel_order(order_id: int):
    """Cancel a specific pending order."""
    if not ib_state["connected"] or not ib_state["ib"]:
        raise HTTPException(status_code=400, detail="Not connected to IBKR")

    def do_cancel():
        ib = ib_state["ib"]
        for trade in ib.openTrades():
            if trade.order.orderId == order_id:
                ib.cancelOrder(trade.order)
                ib.sleep(2)
                return trade.orderStatus.status
        return "NotFound"

    try:
        status = _run_in_ib_thread(do_cancel)
        order_log.append({
            "time": datetime.now().isoformat(),
            "type": "cancel",
            "message": f"Cancelled order #{order_id} -> {status}",
            "status": "ok",
        })
        return {"status": status, "order_id": order_id}
    except Exception as ex:
        raise HTTPException(status_code=500, detail=str(ex))


@app.post("/api/cancel_all")
async def api_cancel_all():
    """Cancel all pending orders."""
    if not ib_state["connected"] or not ib_state["ib"]:
        raise HTTPException(status_code=400, detail="Not connected to IBKR")

    def do_cancel_all():
        ib = ib_state["ib"]
        ib.reqGlobalCancel()
        ib.sleep(3)
        return len(ib.openTrades())

    try:
        remaining = _run_in_ib_thread(do_cancel_all)
        order_log.append({
            "time": datetime.now().isoformat(),
            "type": "cancel",
            "message": f"Global cancel requested, {remaining} orders remaining",
            "status": "ok",
        })
        return {"status": "ok", "remaining": remaining}
    except Exception as ex:
        raise HTTPException(status_code=500, detail=str(ex))


# ═══════════════════════════════════════════════════════════
#  SCANNER SUBPROCESS
# ═══════════════════════════════════════════════════════════

scanner_state = {
    "running": False,
    "start_time": None,
    "last_completed": None,
    "last_duration": None,
    "last_signals_count": None,
}


@app.post("/api/scan")
async def api_scan():
    """Run scanner as a background subprocess."""
    if scanner_state["running"]:
        return {"status": "already_running"}

    scanner_state["running"] = True
    scanner_state["start_time"] = datetime.now().isoformat()

    order_log.append({
        "time": datetime.now().isoformat(),
        "type": "scan",
        "message": "Scanner started...",
        "status": "running",
    })

    def run_scanner_subprocess():
        try:
            scanner_path = str(ROOT / "core" / "scanner.py")
            result = subprocess.run(
                [sys.executable, scanner_path],
                capture_output=True, text=True,
                timeout=600,
                cwd=str(ROOT),
            )

            # Parse output for signal count
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
                scanner_state["start_time"]
            )).total_seconds()

            scanner_state["running"] = False
            scanner_state["last_completed"] = datetime.now().isoformat()
            scanner_state["last_duration"] = round(duration, 1)
            scanner_state["last_signals_count"] = signals_count

            order_log.append({
                "time": datetime.now().isoformat(),
                "type": "scan",
                "message": (f"Scanner completed: {signals_count} signals "
                            f"in {duration:.0f}s"),
                "status": "ok" if result.returncode == 0 else "error",
            })

            if result.returncode != 0:
                order_log.append({
                    "time": datetime.now().isoformat(),
                    "type": "scan",
                    "message": f"Scanner stderr: {result.stderr[:500]}",
                    "status": "error",
                })
        except subprocess.TimeoutExpired:
            scanner_state["running"] = False
            order_log.append({
                "time": datetime.now().isoformat(),
                "type": "scan",
                "message": "Scanner timed out after 600s",
                "status": "error",
            })
        except Exception as ex:
            scanner_state["running"] = False
            order_log.append({
                "time": datetime.now().isoformat(),
                "type": "scan",
                "message": f"Scanner error: {ex}",
                "status": "error",
            })

    threading.Thread(target=run_scanner_subprocess, daemon=True).start()
    return {"status": "running"}


@app.get("/api/scanner_status")
async def api_scanner_status():
    """Get scanner state for UI polling."""
    return {
        "running": scanner_state["running"],
        "start_time": scanner_state["start_time"],
        "last_completed": scanner_state["last_completed"],
        "last_duration": scanner_state["last_duration"],
        "last_signals_count": scanner_state["last_signals_count"],
    }


@app.post("/api/scan_and_enter")
async def api_scan_and_enter(req: EnterRequest):
    """Run scanner then automatically place orders."""
    if not ib_state["connected"] or not ib_state["ib"]:
        raise HTTPException(status_code=400, detail="Not connected to IBKR")

    if scanner_state["running"]:
        raise HTTPException(status_code=400, detail="Scanner already running")

    scanner_state["running"] = True
    scanner_state["start_time"] = datetime.now().isoformat()

    order_log.append({
        "time": datetime.now().isoformat(),
        "type": "scan_and_enter",
        "message": "Scan & Execute: starting scanner...",
        "status": "running",
    })

    def run_scan_then_enter():
        try:
            # Step 1: Run scanner
            scanner_path = str(ROOT / "core" / "scanner.py")
            result = subprocess.run(
                [sys.executable, scanner_path],
                capture_output=True, text=True,
                timeout=600,
                cwd=str(ROOT),
            )

            scanner_state["running"] = False
            scanner_state["last_completed"] = datetime.now().isoformat()

            if result.returncode != 0:
                order_log.append({
                    "time": datetime.now().isoformat(),
                    "type": "scan_and_enter",
                    "message": f"Scanner failed: {result.stderr[:300]}",
                    "status": "error",
                })
                return

            order_log.append({
                "time": datetime.now().isoformat(),
                "type": "scan_and_enter",
                "message": "Scanner done, starting optimal execution...",
                "status": "running",
            })

            # Step 2: Place orders
            ib = ib_state["ib"]
            acct = ib_state["account"]
            results = _run_in_ib_thread(
                enter_new_positions, ib, acct, req.max_new
            )

            entered = results.get("entered", 0) if results else 0
            order_log.append({
                "time": datetime.now().isoformat(),
                "type": "scan_and_enter",
                "message": f"Scan & Execute complete: {entered} positions filled",
                "status": "ok",
            })
        except Exception as ex:
            scanner_state["running"] = False
            order_log.append({
                "time": datetime.now().isoformat(),
                "type": "scan_and_enter",
                "message": f"Error: {ex}",
                "status": "error",
            })

    threading.Thread(target=run_scan_then_enter, daemon=True).start()
    return {"status": "running"}


# Serve output images (backtest charts)
app.mount("/output", StaticFiles(directory=str(OUTPUT)), name="output")


# ═══════════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════════

def open_browser():
    """Open browser after server starts."""
    import time
    time.sleep(1.5)
    webbrowser.open("http://localhost:8000")


if __name__ == "__main__":
    threading.Thread(target=open_browser, daemon=True).start()
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="warning")
