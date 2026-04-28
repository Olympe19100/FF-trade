"""Trading routes — connection, orders, portfolio management."""

import json
import numpy as np
import pandas as pd
from datetime import datetime

from fastapi import APIRouter, HTTPException

from core.config import (
    STATE, OUTPUT, TRADES_FILE, BACKTEST_TRADES_FILE,
    MAX_POSITIONS, BA_PCT_MAX, COMMISSION_LEG, CONTRACT_MULT,
    SLIPPAGE_PER_LEG, FF_THRESHOLD_DEFAULT,
)
from core.portfolio import (
    load_latest_signals, load_portfolio, save_portfolio, add_position,
    load_trade_history, compute_kelly, size_portfolio,
    record_trade, load_cached_monitor_prices, load_pnl_history,
)
from core.trader import (
    connect_ibkr, verify_paper, get_account_info,
    enter_new_positions, close_expiring_positions,
    check_optimal_window,
)
from core.pricing import RISK_FREE_RATE
from api.models import ConnectRequest, EnterRequest, AddPositionRequest, ClosePositionRequest
from api.ibkr_worker import ib_state, order_log, run_in_ib_thread, log_order

router = APIRouter(prefix="/api")


# ═══════════════════════════════════════════════════════════
#  READ APIs
# ═══════════════════════════════════════════════════════════

@router.get("/status")
async def api_status():
    """Global status: connection + portfolio + kelly summary."""
    portfolio = load_portfolio()
    active = [p for p in portfolio["positions"] if "exit_date" not in p]

    returns = load_trade_history()
    kelly_f = compute_kelly(returns)
    win_rate = sum(1 for r in returns if r > 0) / len(returns) if returns else None

    account_value = 0
    buying_power = 0
    if ib_state["connected"] and ib_state["ib"]:
        try:
            info = run_in_ib_thread(
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
        "win_rate": round(win_rate, 4) if win_rate is not None else None,
        "last_updated": portfolio.get("last_updated"),
    }


@router.get("/signals")
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
    for r in records:
        for k, v in r.items():
            if isinstance(v, float) and (np.isnan(v) or np.isinf(v)):
                r[k] = None

    return {"signals": records, "file": file_name, "count": len(records)}


@router.get("/sizing")
async def api_sizing(account_value: float = 1_023_443):
    """Kelly-optimized sizing for latest signals."""
    signals = load_latest_signals()
    if signals.empty:
        return {"sizing": [], "kelly_f": 0, "kelly_target": 0, "total_deployed": 0}

    # Filter illiquid spreads by bid-ask percentage
    if "ba_pct" in signals.columns:
        signals = signals[signals["ba_pct"] <= BA_PCT_MAX]

    # Double calendars only — reject singles (no put leg)
    signals = signals[signals["dbl_cost"].notna() & (signals["dbl_cost"] > 0)]

    # Keep only positive FF (backwardation)
    signals = signals[signals["ff"] > 0]

    portfolio = load_portfolio()
    active_tickers = {p["ticker"] for p in portfolio["positions"] if "exit_date" not in p}
    candidates = signals[~signals["ticker"].isin(active_tickers)]
    # Deduplicate: keep best FF per ticker
    candidates = candidates.sort_values("ff", ascending=False).drop_duplicates(subset=["ticker"], keep="first")

    # Two-pass entry (matches backtest): priority FF>=0.20, then fill with best FF>0
    slots = MAX_POSITIONS - len(active_tickers)
    priority = candidates[candidates["ff"] >= FF_THRESHOLD_DEFAULT].head(slots)
    remaining_slots = slots - len(priority)
    if remaining_slots > 0:
        fill = candidates[~candidates.index.isin(priority.index)].head(remaining_slots)
        eligible = pd.concat([priority, fill])
    else:
        eligible = priority

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
        for k, v in detail.items():
            if isinstance(v, float) and (np.isnan(v) or np.isinf(v)):
                detail[k] = None
        result.append({
            "ticker": ticker,
            "contracts": contracts,
            "deployed": round(deployed, 2),
            "ff": detail.get("ff", 0),
            **{k: detail[k] for k in ["combo", "strike", "put_strike", "stock_px",
               "front_exp", "back_exp", "front_iv", "back_iv", "call_cost",
               "put_cost", "dbl_cost", "ba_pct"] if k in detail},
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
        "active_tickers": sorted(active_tickers),
    }


@router.get("/portfolio")
async def api_portfolio():
    """Active and closed positions, enriched with cached monitor prices."""
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

    # Merge cached monitor prices into active positions
    cached_prices, cached_date = load_cached_monitor_prices()
    total_unrealized_pnl = 0.0
    n_priced = 0
    for p in active:
        c = cached_prices.get(p["ticker"])
        if c:
            p["current_cost"] = c.get("current_cost")
            p["stock_px"] = c.get("stock_px")
            p["unrealized_pnl"] = c.get("unrealized_pnl", 0)
            p["return_pct"] = c.get("return_pct")
            total_unrealized_pnl += c.get("unrealized_pnl", 0)
            n_priced += 1

    # Realized P&L from closed positions
    realized_pnl = sum(p.get("pnl", 0) for p in closed)
    n_wins = sum(1 for p in closed if p.get("pnl", 0) > 0)
    n_losses = sum(1 for p in closed if p.get("pnl", 0) <= 0) if closed else 0

    total_deployed = sum(p.get("total_deployed", 0) for p in active)

    # Monitor refresh state
    from api.routes_monitor import _state as monitor_state
    refresh_running = monitor_state["refresh_running"]

    # P&L history
    pnl_history = load_pnl_history()

    return {
        "active": active,
        "closed": closed,
        "n_active": len(active),
        "total_deployed": total_deployed,
        "last_updated": portfolio.get("last_updated"),
        "cached_date": cached_date,
        "refresh_running": refresh_running,
        "account_summary": {
            "total_unrealized_pnl": round(total_unrealized_pnl, 2),
            "realized_pnl": round(realized_pnl, 2),
            "n_priced": n_priced,
            "n_wins": n_wins,
            "n_losses": n_losses,
        },
        "pnl_history": pnl_history,
    }


@router.post("/portfolio/add")
async def api_portfolio_add(req: AddPositionRequest):
    """Add a single signal to the portfolio for tracking."""
    portfolio = load_portfolio()
    active = [p for p in portfolio["positions"] if "exit_date" not in p]

    # Check if ticker already in portfolio
    active_tickers = {p["ticker"] for p in active}
    if req.ticker in active_tickers:
        raise HTTPException(status_code=400, detail=f"{req.ticker} already in portfolio")

    if len(active) >= MAX_POSITIONS:
        raise HTTPException(status_code=400, detail=f"Portfolio full ({MAX_POSITIONS} positions)")

    pos = add_position(
        portfolio,
        ticker=req.ticker,
        combo=req.combo,
        strike=req.strike,
        front_exp=req.front_exp,
        back_exp=req.back_exp,
        contracts=req.contracts,
        cost_per_share=req.cost_per_share,
        spread_type=req.spread_type,
        ff=req.ff,
        n_legs=req.n_legs,
        put_strike=req.put_strike,
    )
    save_portfolio(portfolio)

    return {"status": "ok", "position": pos}


@router.post("/portfolio/close")
async def api_portfolio_close(req: ClosePositionRequest):
    """Close (paper-close) a position by ID, recording P&L."""
    portfolio = load_portfolio()

    # Find position by id
    pos = None
    pos_idx = None
    for i, p in enumerate(portfolio["positions"]):
        if p.get("id") == req.position_id and "exit_date" not in p:
            pos = p
            pos_idx = i
            break

    if pos is None:
        raise HTTPException(status_code=404, detail=f"Position {req.position_id} not found or already closed")

    # Determine exit price
    exit_price = req.exit_price
    if exit_price is None:
        # Try to get live price from ThetaData via autopilot
        try:
            from core.autopilot import _price_position
            pricing = _price_position(pos)
            if pricing and pricing.get("current_cost") is not None:
                exit_price = pricing["current_cost"]
        except Exception:
            pass

    if exit_price is None:
        # Fallback: try cached monitor prices
        cached_prices, _ = load_cached_monitor_prices()
        c = cached_prices.get(pos["ticker"])
        if c and c.get("current_cost") is not None:
            exit_price = c["current_cost"]

    if exit_price is None:
        raise HTTPException(status_code=400, detail="No exit price available. Refresh prices first or provide exit_price.")

    # Calculate P&L (matches backtest: entry+exit slippage, entry+exit commission)
    entry = pos.get("cost_per_share", 0)
    contracts = pos.get("contracts", 1)
    n_legs = pos.get("n_legs", 4)
    slippage_entry = SLIPPAGE_PER_LEG * n_legs
    slippage_exit = SLIPPAGE_PER_LEG * n_legs
    comm_entry = COMMISSION_LEG * n_legs * contracts
    comm_exit = COMMISSION_LEG * n_legs * contracts
    pnl_per_share = exit_price - entry - slippage_entry - slippage_exit
    pnl = pnl_per_share * CONTRACT_MULT * contracts - (comm_entry + comm_exit)
    deployed = pos.get("total_deployed", (entry + slippage_entry) * CONTRACT_MULT * contracts + comm_entry)
    return_pct = pnl / deployed if deployed > 0 else 0

    # Mark position as closed
    pos["exit_date"] = datetime.now().strftime("%Y-%m-%d")
    pos["exit_price"] = round(exit_price, 4)
    pos["pnl"] = round(pnl, 2)
    pos["return_pct"] = round(return_pct, 4)

    # Record trade and save
    record_trade(pos, exit_price, pnl, return_pct)
    save_portfolio(portfolio)

    return {
        "status": "ok",
        "position_id": req.position_id,
        "ticker": pos["ticker"],
        "exit_price": round(exit_price, 4),
        "pnl": round(pnl, 2),
        "return_pct": round(return_pct, 4),
    }


@router.get("/trades")
async def api_trades():
    """Closed trade history."""
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


@router.get("/orders")
async def api_orders():
    """Order execution log + live IBKR orders."""
    ibkr_data = {"open": [], "filled": [], "positions": []}
    if ib_state["connected"] and ib_state["ib"]:
        try:
            ibkr_data = run_in_ib_thread(_get_ibkr_orders)
        except Exception as ex:
            ibkr_data["error"] = str(ex)

    return {
        "log": list(order_log)[-100:],
        "log_count": len(order_log),
        "open_orders": ibkr_data.get("open", []),
        "filled_orders": ibkr_data.get("filled", []),
        "positions": ibkr_data.get("positions", []),
    }


def _get_ibkr_orders():
    """Fetch ALL orders from IBKR (runs in IB thread)."""
    ib = ib_state["ib"]
    if not ib or not ib.isConnected():
        return {"open": [], "filled": [], "positions": []}

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

    return {"open": open_orders, "filled": filled_orders, "positions": positions}


# ═══════════════════════════════════════════════════════════
#  ACTION APIs
# ═══════════════════════════════════════════════════════════

@router.post("/connect")
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
        ib, acct, info = run_in_ib_thread(do_connect)
        ib_state["ib"] = ib
        ib_state["connected"] = True
        ib_state["account"] = acct
        ib_state["host"] = req.host
        ib_state["port"] = req.port

        log_order("connect", f"Connected to {acct} @ {req.host}:{req.port}")

        return {
            "status": "connected",
            "account": acct,
            "account_value": info.get("NetLiquidation", 0),
            "buying_power": info.get("BuyingPower", 0),
            "currency": info.get("base_currency", "USD"),
        }
    except Exception as ex:
        log_order("connect", f"Connection failed: {ex}", "error")
        raise HTTPException(status_code=500, detail=str(ex))


@router.post("/disconnect")
async def api_disconnect():
    """Disconnect from IBKR."""
    if ib_state["ib"] and ib_state["connected"]:
        try:
            run_in_ib_thread(lambda: ib_state["ib"].disconnect())
        except Exception:
            pass
    ib_state["ib"] = None
    ib_state["connected"] = False
    ib_state["account"] = None
    log_order("disconnect", "Disconnected from IBKR")
    return {"status": "disconnected"}


@router.post("/enter")
async def api_enter(req: EnterRequest):
    """Place new calendar spread orders using optimal execution."""
    if not ib_state["connected"] or not ib_state["ib"]:
        raise HTTPException(status_code=400, detail="Not connected to IBKR")

    is_optimal, time_msg = check_optimal_window()

    log_order("enter", f"Starting optimal execution... {time_msg}", "running")

    try:
        ib = ib_state["ib"]
        acct = ib_state["account"]
        results = run_in_ib_thread(enter_new_positions, ib, acct, req.max_new)

        if results and results.get("slippage_log"):
            for s in results["slippage_log"]:
                if s["result"] == "full":
                    log_order("fill",
                        f"{s['ticker']}: {s['method']} fill @ "
                        f"${s['fill_cost']:.2f} "
                        f"(EODHD ${s['eodhd_mid']:.2f}, "
                        f"slip={s['slippage']:+.3f})")

        entered = results.get("entered", 0) if results else 0
        partial = results.get("partial", 0) if results else 0
        not_filled = results.get("not_filled", 0) if results else 0

        log_order("enter",
            f"Execution complete: {entered} filled, "
            f"{partial} partial, {not_filled} failed")
        return {"status": "ok", "results": results}
    except Exception as ex:
        log_order("enter", f"Error: {ex}", "error")
        raise HTTPException(status_code=500, detail=str(ex))


@router.post("/close")
async def api_close():
    """Close expiring positions."""
    if not ib_state["connected"] or not ib_state["ib"]:
        raise HTTPException(status_code=400, detail="Not connected to IBKR")

    try:
        run_in_ib_thread(close_expiring_positions, ib_state["ib"], ib_state["account"])
        log_order("close", "Close expiring complete")
        return {"status": "ok"}
    except Exception as ex:
        raise HTTPException(status_code=500, detail=str(ex))


@router.post("/cancel_order")
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
        status = run_in_ib_thread(do_cancel)
        log_order("cancel", f"Cancelled order #{order_id} -> {status}")
        return {"status": status, "order_id": order_id}
    except Exception as ex:
        raise HTTPException(status_code=500, detail=str(ex))


@router.post("/cancel_all")
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
        remaining = run_in_ib_thread(do_cancel_all)
        log_order("cancel", f"Global cancel requested, {remaining} orders remaining")
        return {"status": "ok", "remaining": remaining}
    except Exception as ex:
        raise HTTPException(status_code=500, detail=str(ex))
