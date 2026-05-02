"""
Daily Scanner — Forward Factor Double Calendar Spread Signals

Three modes:
  default: ThetaData real-time (Theta Terminal on localhost:25503)
  --ibkr : Live data from IBKR (requires paid market data subscription)

Workflow: Scan all 500 tickers for FF signals → place orders on IBKR

Usage:
    python scanner.py                 # ThetaData scan (recommended)
    python scanner.py --ticker AAPL   # Single ticker
    python scanner.py --ibkr          # IBKR scan (needs data subscription)
    python scanner.py --ibkr --test   # IBKR test with 5 tickers
"""

import requests
import numpy as np
import pandas as pd
import sqlite3
import time
import sys
import asyncio
import threading
from pathlib import Path
from datetime import datetime, timedelta
from typing import Any
from concurrent.futures import ThreadPoolExecutor, as_completed

from core.config import (
    OUTPUT, DB, API_KEY, BASE_URL, THETADATA_URL,
    FRONT_DTE_MIN, FRONT_DTE_MAX, BACK_DTE_MIN, BACK_DTE_MAX, MIN_DTE_GAP,
    STRIKE_PCT, TARGET_DELTA, FF_THRESHOLD_DEFAULT,
    MIN_COST, MIN_OI_LEG, MIN_MID, BA_PCT_MAX, TOP_N, MAX_WORKERS,
    get_http_session, get_logger,
)
from core.pricing import (
    RISK_FREE_RATE,
    implied_vol_vec, bs_delta_vec, compute_ff,
    put_call_parity_call_equiv,
)

log = get_logger(__name__)

OUT = OUTPUT  # alias for backward compat within this module

# Thread-safe progress counter
_progress_lock = threading.Lock()
_progress = {"done": 0, "signals": 0, "errors": 0}


def _get_session() -> requests.Session:
    return get_http_session(MAX_WORKERS)


def get_sp500_tickers() -> list[str]:
    """Get S&P 500 ticker list from our database."""
    conn = sqlite3.connect(str(DB))
    tickers = pd.read_sql_query(
        "SELECT DISTINCT root FROM contracts ORDER BY root", conn
    )["root"].tolist()
    conn.close()
    return tickers


def _sync_earnings_to_db(rows: list[dict]) -> int:
    """Persist EODHD earnings rows into the DB (upsert). Returns count of new rows."""
    if not rows:
        return 0
    conn = sqlite3.connect(str(DB))
    cur = conn.cursor()
    cur.execute(
        "CREATE TABLE IF NOT EXISTS earnings "
        "(root TEXT, report_date INTEGER, PRIMARY KEY (root, report_date))"
    )
    inserted = 0
    for r in rows:
        try:
            cur.execute(
                "INSERT OR IGNORE INTO earnings (root, report_date) VALUES (?, ?)",
                (r["root"], r["report_date"]),
            )
            inserted += cur.rowcount
        except Exception:
            pass
    conn.commit()
    conn.close()
    return inserted


def _fetch_earnings_ibkr(ib: Any, tickers: list[str]) -> list[dict]:
    """Fetch next earnings date from IBKR CalendarReport for tickers missing EODHD data.

    Returns list of {"root": ticker, "report_date": YYYYMMDD_int}.
    """
    import xml.etree.ElementTree as ET

    results = []
    for t in tickers:
        try:
            from ib_insync import Stock
            stock = Stock(t, "SMART", "USD")
            ib.qualifyContracts(stock)
            if stock.conId == 0:
                continue
            xml_str = ib.reqFundamentalData(stock, "CalendarReport")
            ib.sleep(0.5)  # rate limit
            if not xml_str:
                continue
            root_el = ET.fromstring(xml_str)
            # Search broadly for earnings-related date fields
            date_str = None
            for tag in ("nextReportDate", "NextEarningsDate", "reportDate"):
                el = root_el.find(f".//{tag}")
                if el is not None and el.text:
                    date_str = el.text.strip()
                    break
            # Also search for <Event type="Earnings"> with a date attribute
            if not date_str:
                for ev in root_el.iter("Event"):
                    if ev.get("type", "").lower() == "earnings":
                        date_str = ev.get("date") or ev.text
                        if date_str:
                            date_str = date_str.strip()
                            break
            if not date_str:
                continue
            # Parse date: try YYYY-MM-DD, MM/DD/YYYY, YYYYMMDD
            rd_int = None
            for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%Y%m%d"):
                try:
                    dt = datetime.strptime(date_str, fmt)
                    rd_int = int(dt.strftime("%Y%m%d"))
                    break
                except ValueError:
                    continue
            if rd_int:
                results.append({"root": t, "report_date": rd_int})
                log.debug("IBKR earnings %s: %d", t, rd_int)
        except Exception as ex:
            log.debug("IBKR earnings fetch failed for %s: %s", t, ex)
            continue
    return results


def _project_earnings_from_history(tickers: list[str]) -> list[dict]:
    """Project next earnings date from historical DB data using median gap.

    Requires at least 4 historical dates per ticker.
    Returns list of {"root": ticker, "report_date": YYYYMMDD_int}.
    Does NOT persist projections to DB.
    """
    results = []
    try:
        conn = sqlite3.connect(str(DB))
        for t in tickers:
            try:
                rows = pd.read_sql_query(
                    "SELECT report_date FROM earnings WHERE root = ? "
                    "ORDER BY report_date DESC LIMIT 8",
                    conn, params=(t,),
                )
                if len(rows) < 4:
                    continue
                dates = sorted(rows["report_date"].astype(int).tolist())
                # Compute gaps in days between consecutive dates
                gaps = []
                for i in range(1, len(dates)):
                    d0 = datetime.strptime(str(dates[i - 1]), "%Y%m%d")
                    d1 = datetime.strptime(str(dates[i]), "%Y%m%d")
                    gaps.append((d1 - d0).days)
                if not gaps:
                    continue
                median_gap = int(np.median(gaps))
                last_date = datetime.strptime(str(dates[-1]), "%Y%m%d")
                projected = last_date + timedelta(days=median_gap)
                if projected > datetime.now():
                    rd_int = int(projected.strftime("%Y%m%d"))
                    results.append({"root": t, "report_date": rd_int})
                    log.info("Projected %s: %d (median gap %dd from last %d)",
                             t, rd_int, median_gap, dates[-1])
            except Exception as ex:
                log.debug("Projection failed for %s: %s", t, ex)
                continue
        conn.close()
    except Exception as ex:
        log.warning("Projection DB error: %s", ex)
    return results


def get_earnings_dates(tickers: list[str], days_ahead: int = 120, ib: Any = None) -> dict[str, np.ndarray]:
    """Get upcoming earnings dates from DB + EODHD calendar.

    Fetches from EODHD, persists new dates into DB (auto-sync),
    then reads everything back from DB for the scan.
    """
    ticker_set = set(tickers)
    today_int = int(datetime.now().strftime("%Y%m%d"))
    future_int = int((datetime.now() + timedelta(days=days_ahead)).strftime("%Y%m%d"))

    # 1. Fetch from EODHD and persist into DB
    today_str = datetime.now().strftime("%Y-%m-%d")
    future_str = (datetime.now() + timedelta(days=days_ahead)).strftime("%Y-%m-%d")
    eodhd_rows = []
    try:
        sess = _get_session()
        url = (f"{BASE_URL}/calendar/earnings?"
               f"from={today_str}&to={future_str}&api_token={API_KEY}&fmt=json")
        resp = sess.get(url, timeout=30)
        if resp.status_code == 200:
            data = resp.json()
            for e in data.get("earnings", []):
                ticker = e.get("code", "").replace(".US", "")
                rd = e.get("report_date", "")
                if ticker and rd:
                    eodhd_rows.append({"root": ticker, "report_date": int(rd.replace("-", ""))})
            new_count = _sync_earnings_to_db(eodhd_rows)
            if new_count > 0:
                log.info("Earnings DB updated: %d new dates from EODHD", new_count)
        else:
            log.warning("EODHD calendar HTTP %d", resp.status_code)
    except Exception as ex:
        log.warning("EODHD calendar fetch failed: %s", ex)

    # 2. Read from DB (now includes fresh EODHD data)
    conn = sqlite3.connect(str(DB))
    df = pd.read_sql_query(
        f"SELECT root, report_date FROM earnings "
        f"WHERE report_date >= {today_int} AND report_date <= {future_int}",
        conn
    )
    conn.close()

    # 3. Build lookup: root -> sorted array of earnings dates (only scan universe)
    earn_by_root = {}
    if not df.empty:
        df["report_date"] = df["report_date"].astype(int)
        df = df[df["root"].isin(ticker_set)]
        for root, grp in df.groupby("root"):
            earn_by_root[root] = np.sort(grp["report_date"].values)

    # 4. Find tickers missing earnings data
    missing = [t for t in tickers if t not in earn_by_root]

    # 5. IBKR CalendarReport fallback
    n_ibkr = 0
    if missing and ib is not None:
        try:
            ibkr_rows = _fetch_earnings_ibkr(ib, missing)
            if ibkr_rows:
                _sync_earnings_to_db(ibkr_rows)
                for r in ibkr_rows:
                    root, rd = r["root"], r["report_date"]
                    if today_int <= rd <= future_int:
                        earn_by_root[root] = np.array([rd])
                        n_ibkr += 1
                missing = [t for t in tickers if t not in earn_by_root]
        except Exception as ex:
            log.warning("IBKR earnings cascade failed: %s", ex)

    # 6. Historical projection fallback
    n_projected = 0
    if missing:
        proj_rows = _project_earnings_from_history(missing)
        for r in proj_rows:
            root, rd = r["root"], r["report_date"]
            if today_int <= rd <= future_int:
                earn_by_root[root] = np.array([rd])
                n_projected += 1
        missing = [t for t in tickers if t not in earn_by_root]

    n_eodhd = len(earn_by_root) - n_ibkr - n_projected
    log.info("Earnings: %d EODHD, %d IBKR, %d projected, %d unknown",
             n_eodhd, n_ibkr, n_projected, len(missing))
    return earn_by_root


def has_earnings_between(root: str, start_int: int, end_int: int, earn_by_root: dict[str, np.ndarray]) -> bool:
    """Check if any earnings date falls in [start, end] range."""
    if root not in earn_by_root:
        return False
    edates = earn_by_root[root]
    idx = np.searchsorted(edates, start_int, side="left")
    return idx < len(edates) and edates[idx] <= end_int


# ═══════════════════════════════════════════════════════════════
# IBKR Option Chain Fetcher
# ═══════════════════════════════════════════════════════════════

def fetch_option_chain_ibkr(ib: Any, ticker: str, today: datetime) -> tuple[float, pd.DataFrame]:
    """Fetch option chain from IBKR (live or delayed 15min).

    Returns (stock_price, DataFrame) with columns: exp_date, type, strike, bid, ask, iv, volume, open_interest.
    Only fetches ATM options in relevant DTE windows for speed.
    """
    from ib_insync import Stock, Option

    # Enable delayed data if no live subscription
    ib.reqMarketDataType(4)  # 4 = delayed-frozen (free, 15 min delay)

    # 1. Stock price (streaming mode — snapshot not supported with delayed data)
    stock = Stock(ticker, "SMART", "USD")
    try:
        ib.qualifyContracts(stock)
    except Exception:
        return 0, pd.DataFrame()

    if stock.conId == 0:
        return 0, pd.DataFrame()

    ib.reqMktData(stock, "", False, False)
    ib.sleep(2)  # Wait for delayed data to stream in
    stk_tk = ib.ticker(stock)

    stock_px = 0
    if stk_tk:
        # Try market price first, then last, then close
        for attr in ['marketPrice', 'last', 'close']:
            val = getattr(stk_tk, attr, None)
            if callable(val):
                val = val()
            if val and val == val and val > 0 and val != float('inf'):
                stock_px = float(val)
                break
    ib.cancelMktData(stock)

    if stock_px <= 0:
        return 0, pd.DataFrame()

    # 2. Option chain params
    params = ib.reqSecDefOptParams(stock.symbol, "", stock.secType, stock.conId)
    ib.sleep(0.2)

    if not params:
        return stock_px, pd.DataFrame()

    # Use exchange with most strikes (usually SMART)
    chain = max(params, key=lambda p: len(p.strikes))

    # 3. Filter expirations to DTE 15-120
    today_dt = datetime(today.year, today.month, today.day)
    valid_exps = []
    for exp_str in chain.expirations:
        try:
            exp_dt = datetime.strptime(exp_str, "%Y%m%d")
            dte = (exp_dt - today_dt).days
            if 15 <= dte <= 120:
                valid_exps.append((exp_str, dte))
        except ValueError:
            continue

    if not valid_exps:
        return stock_px, pd.DataFrame()

    # 4. Filter strikes to near ATM (10% range for 35-delta OTM strikes)
    atm_strikes = sorted([s for s in chain.strikes
                          if abs(s - stock_px) / stock_px <= 0.10])

    if not atm_strikes:
        return stock_px, pd.DataFrame()

    # 5. Create option contracts (only relevant combos)
    options = []
    for exp_str, dte in valid_exps:
        for strike in atm_strikes:
            for right in ["C", "P"]:
                opt = Option(ticker, exp_str, strike, right, "SMART", "100", "USD")
                options.append((opt, exp_str, dte))

    # 6. Qualify in batches
    batch_size = 50
    qualified = []
    for i in range(0, len(options), batch_size):
        batch_opts = [o[0] for o in options[i:i+batch_size]]
        try:
            ib.qualifyContracts(*batch_opts)
        except Exception:
            pass
        for j, opt_tuple in enumerate(options[i:i+batch_size]):
            opt, exp_str, dte = opt_tuple
            if opt.conId > 0:
                qualified.append(opt_tuple)

    if not qualified:
        return stock_px, pd.DataFrame()

    # 7. Request market data via streaming (snapshot not supported with delayed data)
    snap_batch = 40  # IBKR limit ~100 simultaneous
    all_rows = []

    for i in range(0, len(qualified), snap_batch):
        batch = qualified[i:i+snap_batch]
        batch_opts = [q[0] for q in batch]

        # Request streaming data (no generic ticks, no snapshot)
        for opt in batch_opts:
            ib.reqMktData(opt, "", False, False)
        ib.sleep(3)  # Wait for delayed data to arrive

        # Collect data
        for opt, exp_str, dte in batch:
            tk = ib.ticker(opt)
            if tk is None:
                continue

            bid = float(tk.bid) if tk.bid and tk.bid > 0 and tk.bid != float('inf') else 0
            ask = float(tk.ask) if tk.ask and tk.ask > 0 and tk.ask != float('inf') else 0

            # Try modelGreeks first, then compute from mid price
            iv = 0.0
            if tk.modelGreeks and tk.modelGreeks.impliedVol:
                iv = float(tk.modelGreeks.impliedVol)
            elif bid > 0 and ask > 0:
                # Estimate IV from mid price using simple BS approximation
                mid = (bid + ask) / 2
                T = dte / 365.0
                if T > 0 and stock_px > 0:
                    strike = float(opt.strike)
                    # Brenner-Subrahmanyam ATM approximation: IV ≈ mid * sqrt(2*pi/T) / S
                    iv = mid * np.sqrt(2 * np.pi / T) / stock_px

            vol = int(tk.volume) if tk.volume and tk.volume > 0 else 0

            exp_fmt = f"{exp_str[:4]}-{exp_str[4:6]}-{exp_str[6:8]}"
            all_rows.append({
                "exp_date": exp_fmt,
                "type": "call" if opt.right == "C" else "put",
                "strike": float(opt.strike),
                "bid": bid,
                "ask": ask,
                "iv": iv,
                "volume": vol,
                "open_interest": 0,
            })

        # Cancel market data
        for opt in batch_opts:
            try:
                ib.cancelMktData(opt)
            except Exception:
                pass

    if all_rows:
        return stock_px, pd.DataFrame(all_rows)
    return stock_px, pd.DataFrame()


# ═══════════════════════════════════════════════════════════════
# ThetaData Option Chain Fetcher (primary — real-time via local Theta Terminal)
# ═══════════════════════════════════════════════════════════════

def _thetadata_stock_price(ticker: str) -> float:
    """Get latest stock close from ThetaData EOD (FREE plan)."""
    from datetime import datetime, timedelta
    today = datetime.now()
    # Query last 5 calendar days to handle weekends/holidays
    start = (today - timedelta(days=5)).strftime("%Y-%m-%d")
    end = today.strftime("%Y-%m-%d")
    url = (f"{THETADATA_URL}/v3/stock/history/eod"
           f"?symbol={ticker}&start_date={start}&end_date={end}&format=json")
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()
    data = resp.json()
    records = data.get("response", []) if isinstance(data, dict) else data
    if not records:
        return 0
    # Take the most recent record
    last = records[-1] if isinstance(records, list) else records
    return float(last.get("close", 0))


def fetch_option_chain_thetadata(ticker: str) -> tuple[float, pd.DataFrame]:
    """Fetch option chain from ThetaData REST API v3 (local Theta Terminal).

    Returns (stock_price, DataFrame) with columns: exp_date, type, strike, bid, ask, iv, volume, open_interest.
    Requires Theta Terminal running on localhost:25503.

    Uses STANDARD plan endpoints:
    - stock/history/eod (FREE) for stock price
    - option/snapshot/quote (STANDARD) for bid/ask on all expirations
    - IV computed from mid price via Newton-Raphson (greeks endpoint is PRO-only)
    """
    # 1. Stock price from EOD
    stock_px = _thetadata_stock_price(ticker)
    if stock_px <= 0:
        return 0, pd.DataFrame()

    # 2. Option quotes — all expirations in one call
    url = (f"{THETADATA_URL}/v3/option/snapshot/quote"
           f"?symbol={ticker}&expiration=*&format=json")
    resp = requests.get(url, timeout=120)
    resp.raise_for_status()
    data = resp.json()

    records = data.get("response", []) if isinstance(data, dict) else data
    if not records:
        return stock_px, pd.DataFrame()

    all_rows = []
    for item in records:
        contract = item.get("contract", {})
        data_arr = item.get("data", [])
        if not data_arr:
            continue
        quote = data_arr[0]  # Latest snapshot

        exp_date = contract.get("expiration", "")
        if not exp_date:
            continue
        # Support both YYYY-MM-DD and YYYYMMDD
        exp_date_str = str(exp_date)
        if "-" not in exp_date_str and len(exp_date_str) == 8:
            exp_date_str = f"{exp_date_str[:4]}-{exp_date_str[4:6]}-{exp_date_str[6:8]}"
        exp_date_str = exp_date_str[:10]

        try:
            strike = float(contract.get("strike", 0))
        except (TypeError, ValueError):
            continue

        right = str(contract.get("right", "")).upper()
        if right in ("CALL", "C"):
            opt_type = "call"
        elif right in ("PUT", "P"):
            opt_type = "put"
        else:
            continue

        try:
            bid = float(quote.get("bid") or 0)
            ask = float(quote.get("ask") or 0)
            # If one side is missing, use a tiny value or the other side to allow mid-pricing
            # but we will filter for real liquidity later
        except (TypeError, ValueError):
            continue

        all_rows.append({
            "exp_date": exp_date_str,
            "type": opt_type,
            "strike": strike,
            "bid": max(0.0, bid),
            "ask": max(0.0, ask),
            "iv": 0.0,
            "volume": int(quote.get("volume") or 0),
            "open_interest": int(quote.get("open_interest") or 0),
        })

    if not all_rows:
        return stock_px, pd.DataFrame()

    df = pd.DataFrame(all_rows)

    # 3. Compute IV from mid prices via Newton-Raphson
    df["mid"] = (df["bid"] + df["ask"]) / 2
    today_ts = pd.Timestamp(datetime.now())
    df["exp_dt"] = pd.to_datetime(df["exp_date"], errors="coerce")
    df["dte_days"] = (df["exp_dt"] - today_ts).dt.days
    T = df["dte_days"].values / 365.0

    # For puts, use put-call parity to get call-equivalent price
    is_call = (df["type"] == "call").values
    mid_prices = df["mid"].values.copy()
    strikes = df["strike"].values
    put_mask = ~is_call & (T > 0)
    mid_prices[put_mask] = put_call_parity_call_equiv(
        mid_prices[put_mask], stock_px, strikes[put_mask], T[put_mask],
    )
    mid_prices = np.maximum(mid_prices, 0.001)

    iv = implied_vol_vec(
        mid_prices,
        np.full(len(df), stock_px),
        strikes,
        T,
    )
    df["iv"] = iv
    df.drop(columns=["mid", "exp_dt", "dte_days"], inplace=True)

    return stock_px, df


# ═══════════════════════════════════════════════════════════════
# EODHD Option Chain Fetcher (fallback)
# ═══════════════════════════════════════════════════════════════

def fetch_option_chain_eodhd(ticker: str) -> tuple[float, pd.DataFrame]:
    """Fetch full option chain from EODHD /api/options/ endpoint.

    Returns (stock_price, DataFrame).
    Uses persistent HTTP session for connection pooling.
    """
    sess = _get_session()
    url = f"{BASE_URL}/options/{ticker}.US?api_token={API_KEY}&fmt=json"

    try:
        resp = sess.get(url, timeout=30)
        if resp.status_code == 200:
            data = resp.json()
            stock_px = float(data.get("lastTradePrice", 0))

            all_rows = []
            for exp_data in data.get("data", []):
                exp_date = exp_data.get("expirationDate", "")
                options = exp_data.get("options", {})

                for opt_type, opt_list in [("call", "CALL"), ("put", "PUT")]:
                    for opt in options.get(opt_list, []):
                        try:
                            strike = float(opt.get("strike") or 0)
                            bid = float(opt.get("bid") or 0)
                            ask = float(opt.get("ask") or 0)
                            iv = float(opt.get("impliedVolatility") or 0) / 100
                            vol = int(opt.get("volume") or 0)
                            oi = int(opt.get("openInterest") or 0)
                        except (TypeError, ValueError):
                            continue
                        all_rows.append({
                            "exp_date": exp_date,
                            "type": opt_type,
                            "strike": strike,
                            "bid": bid,
                            "ask": ask,
                            "iv": iv,
                            "volume": vol,
                            "open_interest": oi,
                        })

            if all_rows:
                return stock_px, pd.DataFrame(all_rows)
            return stock_px, pd.DataFrame()

        elif resp.status_code == 429:
            time.sleep(10)
            # Retry once (no recursion to avoid stack overflow)
            resp = sess.get(url, timeout=30)
            if resp.status_code == 200:
                data = resp.json()
                stock_px = float(data.get("lastTradePrice", 0))
                all_rows = []
                for exp_data in data.get("data", []):
                    exp_date = exp_data.get("expirationDate", "")
                    options = exp_data.get("options", {})
                    for opt_type, opt_list in [("call", "CALL"), ("put", "PUT")]:
                        for opt in options.get(opt_list, []):
                            try:
                                strike = float(opt.get("strike") or 0)
                                bid = float(opt.get("bid") or 0)
                                ask = float(opt.get("ask") or 0)
                                iv = float(opt.get("impliedVolatility") or 0) / 100
                                vol = int(opt.get("volume") or 0)
                                oi = int(opt.get("openInterest") or 0)
                            except (TypeError, ValueError):
                                continue
                            all_rows.append({
                                "exp_date": exp_date, "type": opt_type,
                                "strike": strike, "bid": bid, "ask": ask,
                                "iv": iv, "volume": vol, "open_interest": oi,
                            })
                if all_rows:
                    return stock_px, pd.DataFrame(all_rows)
                return stock_px, pd.DataFrame()

    except Exception as ex:
        with _progress_lock:
            _progress["errors"] += 1

    return 0, pd.DataFrame()


# ═══════════════════════════════════════════════════════════════
# FF Computation + Ticker Scan
# ═══════════════════════════════════════════════════════════════

def scan_ticker_from_chain(ticker: str, stock_px: float, chain: pd.DataFrame, earn_by_root: dict[str, np.ndarray], today: datetime, verbose: bool = False) -> list[dict]:
    """Scan one ticker for calendar spread opportunities from a pre-fetched chain.

    Works with both IBKR and EODHD data (same DataFrame format).

    Liquidity filters (per-leg, academic standard — Cao & Wei 2010):
      - Open Interest >= MIN_OI_LEG (200) on each call leg
      - Midpoint >= MIN_MID ($0.25) — OptionMetrics standard
    """
    results = []

    if stock_px <= 0 or chain.empty:
        return results

    # Compute DTE and Mid
    chain = chain.copy()
    chain["exp_dt"] = pd.to_datetime(chain["exp_date"], errors="coerce")
    chain["dte"] = (chain["exp_dt"] - pd.Timestamp(today).normalize()).dt.days

    # Relaxed filter: at least one side must be > 0 and DTE 15-120
    chain = chain[
        ((chain["bid"] > 0) | (chain["ask"] > 0)) &
        (chain["dte"] >= 15) & (chain["dte"] <= 120)
    ].copy()
    chain["mid"] = (chain["bid"] + chain["ask"]) / 2
    # If one side is 0, mid is half the other side (conservative)
    mask_zero_bid = (chain["bid"] == 0) & (chain["ask"] > 0)
    chain.loc[mask_zero_bid, "mid"] = chain.loc[mask_zero_bid, "ask"] # Pay the full ask if no bid
    mask_zero_ask = (chain["ask"] == 0) & (chain["bid"] > 0)
    chain.loc[mask_zero_ask, "mid"] = chain.loc[mask_zero_ask, "bid"] # Receive only bid if no ask

    if chain.empty:
        return results

    # Check if OI data is available (EODHD yes, IBKR may not)
    has_oi = "open_interest" in chain.columns and chain["open_interest"].max() > 0

    calls = chain[chain["type"] == "call"].copy()
    puts = chain[chain["type"] == "put"].copy()

    if verbose:
        log.info("%s: $%.2f, %d calls, %d puts", ticker, stock_px, len(calls), len(puts))

    # ── Dynamic DTE pair discovery ──
    # Extract unique expirations with their DTE
    exp_dte = chain.drop_duplicates("exp_date")[["exp_date", "dte"]].copy()
    exp_dte = exp_dte.sort_values("dte")

    front_exps = exp_dte[
        (exp_dte["dte"] >= FRONT_DTE_MIN) & (exp_dte["dte"] <= FRONT_DTE_MAX)
    ]
    back_exps = exp_dte[
        (exp_dte["dte"] >= BACK_DTE_MIN) & (exp_dte["dte"] <= BACK_DTE_MAX)
    ]

    # Enumerate all valid (front, back) pairs
    pairs = []
    for _, fr in front_exps.iterrows():
        for _, bk in back_exps.iterrows():
            if bk["exp_date"] == fr["exp_date"]:
                continue
            gap = bk["dte"] - fr["dte"]
            if gap >= MIN_DTE_GAP:
                pairs.append((fr["exp_date"], int(fr["dte"]),
                              bk["exp_date"], int(bk["dte"])))

    if verbose:
        log.info("%s: %d valid DTE pairs from %d front x %d back expirations",
                 ticker, len(pairs), len(front_exps), len(back_exps))

    for front_exp, front_dte_target, back_exp, back_dte_target in pairs:
        # 1. Find common call strikes between both expiries
        f_calls = calls[calls["exp_date"] == front_exp].copy()
        b_calls = calls[calls["exp_date"] == back_exp].copy()
        
        common_call_strikes = set(f_calls["strike"]).intersection(set(b_calls["strike"]))
        
        if not common_call_strikes:
            continue
            
        # 2. Filter front calls to common strikes and compute deltas
        front = f_calls[
            f_calls["strike"].isin(common_call_strikes) &
            ((f_calls["strike"] - stock_px).abs() / stock_px <= STRIKE_PCT)
        ].copy()
        
        if front.empty:
            continue

        # Compute call delta for all candidates
        f_T = front["dte"].values / 365.0
        f_iv = front["iv"].values
        f_delta = bs_delta_vec(np.full(len(front), stock_px), front["strike"].values, f_T, f_iv)
        front["call_delta"] = f_delta
        front = front.dropna(subset=["call_delta"])
        
        if front.empty:
            continue

        # 3. Select strike closest to TARGET_DELTA (35-delta)
        front["delta_diff"] = (front["call_delta"] - TARGET_DELTA).abs()
        front_best = front.loc[front["delta_diff"].idxmin()]

        strike = front_best["strike"]
        front_iv = front_best["iv"]
        front_mid = front_best["mid"]
        front_dte = int(front_best["dte"])
        front_exp_str = str(front_best["exp_date"])
        front_oi = int(front_best.get("open_interest", 0)) if has_oi else -1
        call_delta_val = float(front_best["call_delta"])

        # Liquidity: front call OI >= MIN_OI_LEG and mid >= MIN_MID
        if has_oi and front_oi < MIN_OI_LEG:
            continue
        if front_mid < MIN_MID:
            continue

        # 4. Back call: Use the same common strike
        back_best = b_calls[b_calls["strike"] == strike].iloc[0]
        
        back_iv = back_best["iv"]
        back_mid = back_best["mid"]
        back_dte = int(back_best["dte"])
        back_exp_str = str(back_best["exp_date"])
        back_oi = int(back_best.get("open_interest", 0)) if has_oi else -1

        # Liquidity: back call OI >= MIN_OI_LEG and mid >= MIN_MID
        if has_oi and back_oi < MIN_OI_LEG:
            continue
        if back_mid < MIN_MID:
            continue

        # Spread cost (call calendar)
        spread_cost = back_mid - front_mid
        if spread_cost < MIN_COST:
            continue

        # Forward Factor (PDF/Campasano: (front_iv - fwd_iv) / fwd_iv)
        ff = compute_ff(front_iv, back_iv, front_dte, back_dte)
        if np.isnan(ff) or ff <= 0:
            continue

        # Earnings filter: no earnings between today and back expiry (ex-earn IVs)
        today_int = int(today.strftime("%Y%m%d"))
        back_exp_int = int(back_exp_str.replace("-", "")[:8])
        if has_earnings_between(ticker, today_int, back_exp_int, earn_by_root):
            continue

        # ── Put leg: find best common strike available in BOTH expirations ──
        put_cost = np.nan
        combined_cost = np.nan
        put_strike_val = np.nan
        put_delta_val = np.nan

        # 1. Get all puts for both expiries
        f_puts = puts[puts["exp_date"] == front_exp].copy()
        b_puts = puts[puts["exp_date"] == back_exp].copy()

        if not f_puts.empty and not b_puts.empty:
            # 2. Find common strikes
            common_strikes = set(f_puts["strike"]).intersection(set(b_puts["strike"]))
            
            if common_strikes:
                # 3. Filter front puts to common strikes and compute deltas
                f_cands = f_puts[f_puts["strike"].isin(common_strikes)].copy()
                
                pf_T = f_cands["dte"].values / 365.0
                pf_S = np.full(len(f_cands), stock_px)
                pf_K = f_cands["strike"].values
                
                # Approximate IV/Delta for candidate strikes
                f_cands["call_equiv"] = put_call_parity_call_equiv(
                    f_cands["mid"].values, stock_px, pf_K, pf_T,
                )
                f_cands["iv_est"] = implied_vol_vec(f_cands["call_equiv"].values, pf_S, pf_K, pf_T)
                f_cands["put_delta_abs"] = 1.0 - bs_delta_vec(pf_S, pf_K, pf_T, f_cands["iv_est"].values)
                
                f_cands = f_cands.dropna(subset=["put_delta_abs"])
                
                if not f_cands.empty:
                    # 4. Select common strike closest to Target Delta
                    f_cands["delta_diff"] = (f_cands["put_delta_abs"] - TARGET_DELTA).abs()
                    pf_best = f_cands.loc[f_cands["delta_diff"].idxmin()]
                    
                    put_strike_val = float(pf_best["strike"])
                    put_delta_val = float(pf_best["put_delta_abs"])
                    
                    # 5. Get back leg at the SAME common strike
                    pb_best = b_puts[b_puts["strike"] == put_strike_val].iloc[0]
                    
                    # Liquidity checks
                    pf_oi = int(pf_best.get("open_interest", 0)) if has_oi else -1
                    pb_oi = int(pb_best.get("open_interest", 0)) if has_oi else -1
                    
                    pf_ok = (not has_oi) or pf_oi >= MIN_OI_LEG
                    pb_ok = (not has_oi) or pb_oi >= MIN_OI_LEG
                    
                    if pf_ok and pb_ok:
                        pf_mid = pf_best["mid"]
                        pb_mid = (pb_best["bid"] + pb_best["ask"]) / 2
                        put_cost = pb_mid - pf_mid
                        if put_cost > 0:
                            combined_cost = spread_cost + put_cost

        # ── Bid-ask spread cost (liquidity metric) ──
        call_ba = (front_best["ask"] - front_best["bid"]) + (back_best["ask"] - back_best["bid"])
        call_ba_pct = call_ba / (2 * spread_cost) if spread_cost > 0 else 999.0

        if not np.isnan(put_cost) and put_cost > 0 and not np.isnan(combined_cost):
            put_ba = (pf_best["ask"] - pf_best["bid"]) + (pb_best["ask"] - pb_best["bid"])
            dbl_ba = call_ba + put_ba
            ba_pct = dbl_ba / (2 * combined_cost) if combined_cost > 0 else 999.0
        else:
            ba_pct = call_ba_pct

        # Bid-ask filter: reject illiquid spreads (same as backtest)
        if ba_pct > BA_PCT_MAX:
            continue

        # Double Calendar Filter: Only keep if both legs exist and were priced
        if np.isnan(put_cost) or put_cost <= 0 or np.isnan(combined_cost):
            continue

        combo_label = f"{front_dte}-{back_dte}"
        results.append({
            "ticker": ticker,
            "combo": combo_label,
            "strike": strike,
            "put_strike": put_strike_val if not np.isnan(put_strike_val) else None,
            "stock_px": stock_px,
            "front_exp": front_exp_str,
            "front_dte": front_dte,
            "front_iv": round(front_iv * 100, 1),
            "back_exp": back_exp_str,
            "back_dte": back_dte,
            "back_iv": round(back_iv * 100, 1),
            "ff": round(ff, 4),
            "call_cost": round(spread_cost, 2),
            "put_cost": round(put_cost, 2) if not np.isnan(put_cost) else None,
            "dbl_cost": round(combined_cost, 2) if not np.isnan(combined_cost) else None,
            "call_delta": round(call_delta_val, 3),
            "put_delta": round(put_delta_val, 3) if not np.isnan(put_delta_val) else None,
            "front_oi": front_oi,
            "back_oi": back_oi,
            "ba_pct": round(ba_pct, 4),
        })

        if verbose:
            ps_str = f", PutK={put_strike_val:.0f}" if not np.isnan(put_strike_val) else ""
            log.info("%s: SIGNAL! FF=%.3f (%.1f%%), CallK=%.0f%s, "
                     "dc=%.2f, cost=$%.2f, %s(DTE=%d) -> %s(DTE=%d)",
                     combo_label, ff, ff*100, strike, ps_str,
                     call_delta_val, spread_cost, front_exp_str, front_dte,
                     back_exp_str, back_dte)

    # Keep only the best FF signal per ticker
    if len(results) > 1:
        results.sort(key=lambda r: r["ff"], reverse=True)
        results = [results[0]]

    return results


# ThetaData only (no EODHD fallback)
def scan_ticker(ticker: str, earn_by_root: dict[str, np.ndarray], today: datetime, verbose: bool = False) -> list[dict]:
    """Scan one ticker via ThetaData."""
    try:
        stock_px, chain = fetch_option_chain_thetadata(ticker)
        if stock_px > 0 and not chain.empty:
            return scan_ticker_from_chain(ticker, stock_px, chain, earn_by_root, today, verbose)
        if verbose:
            log.warning("ThetaData: no data for %s (px=%.2f, chain=%d rows)",
                        ticker, stock_px, len(chain))
        return []
    except Exception as ex:
        log.warning("ThetaData failed for %s: %s", ticker, ex)
        return []


def _scan_one(args: tuple) -> tuple[str, list[dict]]:
    """Worker function for parallel scanning (ThetaData)."""
    ticker, earn_by_root, today, verbose = args
    signals = scan_ticker(ticker, earn_by_root, today, verbose=verbose)
    with _progress_lock:
        _progress["done"] += 1
        _progress["signals"] += len(signals)
        done = _progress["done"]
    if done % 50 == 0 or done == 1:
        log.info("[%d] %s... (%d signals so far)", done, ticker, _progress['signals'])
    return ticker, signals


# ═══════════════════════════════════════════════════════════════
# Main Scanner — IBKR mode (sequential, live data)
# ═══════════════════════════════════════════════════════════════

def run_scanner_ibkr(ib: Any, tickers: list[str] | None = None, test_mode: bool = False) -> pd.DataFrame:
    """Scanner using IBKR live data. Sequential but accurate prices.

    ~3s/ticker = ~25 min for 500 tickers (real-time data).
    """
    today = datetime.now()
    t0 = time.time()
    log.info("=" * 70)
    log.info("FORWARD FACTOR SCANNER [IBKR LIVE] — %s", today.strftime('%Y-%m-%d %H:%M'))
    log.info("=" * 70)

    if tickers is None:
        tickers = get_sp500_tickers()
    if test_mode:
        tickers = tickers[:5]

    log.info("Tickers: %d", len(tickers))
    log.info("Mode: IBKR real-time")
    log.info("DTE: front [%d-%d], back [%d-%d], gap >= %d (dynamic discovery)",
             FRONT_DTE_MIN, FRONT_DTE_MAX, BACK_DTE_MIN, BACK_DTE_MAX, MIN_DTE_GAP)
    log.info("FF threshold (PDF): %.3f", FF_THRESHOLD_DEFAULT)
    log.info("Min spread cost: $%.2f", MIN_COST)
    log.info("Min OI per leg: %d", MIN_OI_LEG)
    log.info("Min midpoint: $%.2f", MIN_MID)

    # Load earnings
    earn_by_root = get_earnings_dates(tickers, ib=ib)

    all_signals = []
    n_errors = 0
    n_no_data = 0
    verbose = len(tickers) <= 5

    for i, ticker in enumerate(tickers):
        if (i + 1) % 25 == 0 or i == 0:
            elapsed = time.time() - t0
            eta = (elapsed / max(i, 1)) * (len(tickers) - i) if i > 0 else 0
            log.info("[%d/%d] %s... (%d signals, %.0fs elapsed, ETA %.0fs)",
                     i+1, len(tickers), ticker, len(all_signals), elapsed, eta)

        try:
            stock_px, chain = fetch_option_chain_ibkr(ib, ticker, today)
            if stock_px <= 0 or chain.empty:
                n_no_data += 1
                continue

            signals = scan_ticker_from_chain(
                ticker, stock_px, chain, earn_by_root, today, verbose=verbose
            )
            all_signals.extend(signals)

        except Exception as ex:
            n_errors += 1
            if verbose:
                log.error("  %s: %s", ticker, ex)

    elapsed = time.time() - t0
    log.info("Scanned %d tickers in %.1fs (%.2fs/ticker)",
             len(tickers), elapsed, elapsed/len(tickers))
    log.info("Found %d raw signals, %d errors, %d no data",
             len(all_signals), n_errors, n_no_data)

    if not all_signals:
        log.info("No signals found!")
        return pd.DataFrame()

    df = pd.DataFrame(all_signals)
    df = df.sort_values("ff", ascending=False)

    # Top N
    top = df.head(TOP_N)

    _print_results(top, df, today)
    return df


# ═══════════════════════════════════════════════════════════════
# Main Scanner — ThetaData mode (parallel, real-time)
# ═══════════════════════════════════════════════════════════════

def run_scanner(tickers: list[str] | None = None, test_mode: bool = False, ib: Any | None = None) -> pd.DataFrame:
    """Main scanner entry point.

    If ib is provided, uses IBKR live data.
    Otherwise uses ThetaData parallel scan.
    """
    if ib is not None:
        return run_scanner_ibkr(ib, tickers, test_mode)

    today = datetime.now()
    t0 = time.time()
    log.info("=" * 70)
    log.info("FORWARD FACTOR SCANNER [ThetaData] — %s", today.strftime('%Y-%m-%d %H:%M'))
    log.info("=" * 70)

    if tickers is None:
        tickers = get_sp500_tickers()
    if test_mode:
        tickers = tickers[:5]

    n_workers = min(MAX_WORKERS, len(tickers))
    log.info("Tickers: %d", len(tickers))
    log.info("Workers: %d parallel", n_workers)
    log.info("DTE: front [%d-%d], back [%d-%d], gap >= %d (dynamic discovery)",
             FRONT_DTE_MIN, FRONT_DTE_MAX, BACK_DTE_MIN, BACK_DTE_MAX, MIN_DTE_GAP)
    log.info("FF threshold (PDF): %.3f", FF_THRESHOLD_DEFAULT)
    log.info("Min spread cost: $%.2f", MIN_COST)
    log.info("Min OI per leg: %d", MIN_OI_LEG)
    log.info("Min midpoint: $%.2f", MIN_MID)

    # Load earnings (ib=None in ThetaData mode — cascade skips IBKR, goes to projection)
    earn_by_root = get_earnings_dates(tickers)

    # Reset progress counter
    _progress["done"] = 0
    _progress["signals"] = 0
    _progress["errors"] = 0

    verbose = len(tickers) <= 5
    args_list = [(t, earn_by_root, today, verbose) for t in tickers]

    all_signals = []
    with ThreadPoolExecutor(max_workers=n_workers) as executor:
        futures = {executor.submit(_scan_one, a): a[0] for a in args_list}
        for future in as_completed(futures):
            try:
                ticker, signals = future.result()
                all_signals.extend(signals)
            except Exception:
                _progress["errors"] += 1

    elapsed = time.time() - t0
    log.info("Scanned %d tickers in %.1fs (%.2fs/ticker)",
             len(tickers), elapsed, elapsed/len(tickers))
    log.info("Found %d raw signals, %d errors",
             len(all_signals), _progress['errors'])

    if not all_signals:
        log.info("No signals found!")
        return pd.DataFrame()

    df = pd.DataFrame(all_signals)
    df = df.sort_values("ff", ascending=False)

    top = df.head(TOP_N)
    _print_results(top, df, today)
    return df


def _print_results(top: pd.DataFrame, df: pd.DataFrame, today: datetime) -> None:
    """Log top signals and save to files."""
    log.info("=" * 120)
    log.info("TOP %d SIGNALS (ranked by FF, 35-delta strikes)", TOP_N)
    log.info("=" * 120)
    log.info("%-6s  %-5s  %-6s  %-6s  %-7s  %-5s  %-5s  %-6s  %-8s  %-6s  %-6s  %-10s  %-10s",
             "Ticker", "Combo", "CallK", "PutK", "Stock", "CDlt", "PDlt",
             "FF", "DblCost", "F.OI", "B.OI", "FrontExp", "BackExp")

    for _, r in top.iterrows():
        dbl = f"${r['dbl_cost']:.2f}" if pd.notna(r.get("dbl_cost")) and r["dbl_cost"] else "N/A"
        f_oi = f"{r.get('front_oi', 0):,}" if r.get('front_oi', 0) > 0 else "N/A"
        b_oi = f"{r.get('back_oi', 0):,}" if r.get('back_oi', 0) > 0 else "N/A"
        ps = f"${r['put_strike']:>5.0f}" if pd.notna(r.get("put_strike")) else "  N/A"
        cd = f"{r['call_delta']:.2f}" if pd.notna(r.get("call_delta")) else "N/A"
        pd_val = f"{r['put_delta']:.2f}" if pd.notna(r.get("put_delta")) else "N/A"
        ff_display = f"{r['ff']:.2f}" if pd.notna(r.get("ff")) else "N/A"
        log.info("%6s  %5s  $%5.0f  %6s  $%6.2f  %5s  %5s  %6s  %8s  %6s  %6s  %10s  %10s",
                 r['ticker'], r['combo'], r['strike'], ps,
                 r['stock_px'], cd, pd_val, ff_display,
                 dbl, f_oi, b_oi, r['front_exp'], r['back_exp'])

    # Save to file
    out_file = OUT / f"signals_{today.strftime('%Y%m%d')}.csv"
    df.to_csv(str(out_file), index=False)
    log.info("All %d signals saved to %s", len(df), out_file)

    top_file = OUT / f"top{TOP_N}_{today.strftime('%Y%m%d')}.csv"
    top.to_csv(str(top_file), index=False)
    log.info("Top %d saved to %s", TOP_N, top_file)


if __name__ == "__main__":
    from core.config import ensure_theta_terminal
    ensure_theta_terminal()

    test_mode = "--test" in sys.argv
    use_ibkr = "--ibkr" in sys.argv

    # Single ticker mode
    ticker_arg = None
    if "--ticker" in sys.argv:
        idx = sys.argv.index("--ticker")
        if idx + 1 < len(sys.argv):
            ticker_arg = [sys.argv[idx + 1]]

    if use_ibkr:
        # Connect to IBKR
        try:
            asyncio.get_event_loop()
        except RuntimeError:
            asyncio.set_event_loop(asyncio.new_event_loop())

        from ib_insync import IB
        ib = IB()
        port = 4002
        if "--port" in sys.argv:
            idx = sys.argv.index("--port")
            if idx + 1 < len(sys.argv):
                port = int(sys.argv[idx + 1])

        log.info("Connecting to IBKR on port %d...", port)
        ib.connect("127.0.0.1", port, clientId=60, timeout=10)
        log.info("Connected: %s", ib.isConnected())

        try:
            run_scanner_ibkr(ib, tickers=ticker_arg, test_mode=test_mode)
        finally:
            ib.disconnect()
            log.info("Disconnected from IBKR")
    else:
        run_scanner(tickers=ticker_arg, test_mode=test_mode)
