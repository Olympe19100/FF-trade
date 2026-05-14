"""
Daily Scanner — Forward Factor Calendar Spread Signals (Call Only)

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
from datetime import datetime, timedelta
from typing import Any
from concurrent.futures import ThreadPoolExecutor, as_completed

from core.config import (
    OUTPUT, DB, API_KEY, BASE_URL, THETADATA_URL,
    FRONT_DTE_MIN, FRONT_DTE_MAX, BACK_DTE_MIN, BACK_DTE_MAX, MIN_DTE_GAP,
    STRIKE_PCT, TARGET_DELTA, FF_THRESHOLD_DEFAULT,
    MIN_COST, MIN_OI_LEG, MIN_MID, BA_PCT_MAX, TOP_N, MAX_WORKERS,
    MAX_UNDERLYING_BA,
    get_http_session, get_logger,
)
from core.pricing import (
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

    # 5. IBKR CalendarReport fallback (cap at 200 to avoid API spam)
    n_ibkr = 0
    MAX_IBKR_EARNINGS = 200
    if missing and ib is not None:
        try:
            ibkr_batch = missing[:MAX_IBKR_EARNINGS]
            if len(missing) > MAX_IBKR_EARNINGS:
                log.info("Earnings IBKR: capped at %d/%d (skipping %d)",
                         MAX_IBKR_EARNINGS, len(missing), len(missing) - MAX_IBKR_EARNINGS)
            ibkr_rows = _fetch_earnings_ibkr(ib, ibkr_batch)
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


def _get_underlying_spread(ticker: str) -> float | None:
    """Get underlying stock bid-ask spread via ThetaData REST (Huh & Lin 2013).

    Returns spread in dollars, or None if unavailable.
    """
    try:
        url = (f"{THETADATA_URL}/v3/stock/snapshot/quote"
               f"?symbol={ticker}&format=json")
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        records = data.get("response", []) if isinstance(data, dict) else data
        if not records:
            return None
        quote = records[0] if isinstance(records, list) else records
        # Handle nested data arrays (ThetaData v3 format)
        if isinstance(quote, dict) and "data" in quote:
            data_arr = quote["data"]
            if data_arr:
                quote = data_arr[0]
        bid = float(quote.get("bid", 0) or 0)
        ask = float(quote.get("ask", 0) or 0)
        if bid > 0 and ask > bid:
            return round(ask - bid, 4)
        return None
    except Exception as ex:
        log.debug("Underlying spread fetch failed for %s: %s", ticker, ex)
        return None


# ═══════════════════════════════════════════════════════════════
# IBKR Option Chain Fetcher
# ═══════════════════════════════════════════════════════════════

def _is_monthly_expiration(exp_dt: datetime) -> bool:
    """True if date is a 3rd Friday (standard monthly option expiration)."""
    return exp_dt.weekday() == 4 and 15 <= exp_dt.day <= 21


def fetch_option_chain_ibkr(ib: Any, ticker: str, today: datetime) -> tuple[float, pd.DataFrame, float | None]:
    """Fetch option chain from IBKR (optimized for speed).

    Optimizations:
    1. Monthly expirations only (3rd Fridays — most liquid, best FF signals)
    2. reqSecDefOptParams for valid strikes, qualify only monthly+ATM combos
    3. Batch size 80 for market data requests
    4. 1.5s wait for live OPRA data
    5. Skip stocks < $10 (illiquid options)

    Returns (stock_price, DataFrame, underlying_ba).
    """
    from ib_insync import Stock, Option

    ib.reqMarketDataType(4)  # best available (live > delayed > frozen)

    # 1. Stock price
    stock = Stock(ticker, "SMART", "USD")
    try:
        ib.qualifyContracts(stock)
    except Exception:
        return 0, pd.DataFrame(), None

    if stock.conId == 0:
        return 0, pd.DataFrame(), None

    ib.reqMktData(stock, "", False, False)
    ib.sleep(1.5)
    stk_tk = ib.ticker(stock)

    stock_px = 0
    underlying_ba = None
    if stk_tk:
        for attr in ['marketPrice', 'last', 'close']:
            val = getattr(stk_tk, attr, None)
            if callable(val):
                val = val()
            if val and val == val and val > 0 and val != float('inf'):
                stock_px = float(val)
                break
        stk_bid = float(stk_tk.bid) if stk_tk.bid and stk_tk.bid > 0 and stk_tk.bid != float('inf') else 0
        stk_ask = float(stk_tk.ask) if stk_tk.ask and stk_tk.ask > 0 and stk_tk.ask != float('inf') else 0
        if stk_bid > 0 and stk_ask > stk_bid:
            underlying_ba = round(stk_ask - stk_bid, 4)
    ib.cancelMktData(stock)

    if stock_px <= 0 or stock_px < 10:
        return 0, pd.DataFrame(), underlying_ba

    # 2. Get available strikes/expirations
    params = ib.reqSecDefOptParams(stock.symbol, "", stock.secType, stock.conId)
    ib.sleep(0.2)

    if not params:
        return stock_px, pd.DataFrame(), underlying_ba

    chain = max(params, key=lambda p: len(p.strikes))

    # 3. Filter expirations in DTE range (weeklies + monthlies)
    #    Front weeklies (15-30 DTE) capture near-term IV crucial for FF.
    #    Strike increment filter handles contract volume.
    today_dt = datetime(today.year, today.month, today.day)
    valid_exps = []
    for exp_str in chain.expirations:
        try:
            exp_dt = datetime.strptime(exp_str, "%Y%m%d")
        except ValueError:
            continue
        dte = (exp_dt - today_dt).days
        if FRONT_DTE_MIN <= dte <= BACK_DTE_MAX:
            valid_exps.append((exp_str, dte))

    if not valid_exps:
        return stock_px, pd.DataFrame(), underlying_ba

    # 4. ATM strikes — filter to standard monthly increments only
    #    CBOE monthly increment rules:
    #    Stock >= $500: $25 increments (far monthlies use $25/$50)
    #    Stock >= $200: $10 increments (monthlies at this level)
    #    Stock >= $100: $5 increments
    #    Stock $25-$100: $2.50
    #    Stock < $25: $1.00
    all_strikes = sorted([s for s in chain.strikes
                          if abs(s - stock_px) / stock_px <= STRIKE_PCT])
    if stock_px >= 500:
        atm_strikes = [s for s in all_strikes if s % 25 == 0]
    elif stock_px >= 200:
        atm_strikes = [s for s in all_strikes if s % 10 == 0]
    elif stock_px >= 100:
        atm_strikes = [s for s in all_strikes if s % 5 == 0]
    else:
        # Stocks < $100: weeklies use $1, monthlies use $2.50
        # Keep all strikes — invalid ones just fail to qualify harmlessly
        atm_strikes = list(all_strikes)
    # Fallback: if filtering removed everything, use all
    if not atm_strikes:
        atm_strikes = all_strikes

    if not atm_strikes:
        return stock_px, pd.DataFrame(), underlying_ba

    # 5. Create + qualify (monthly × ATM = much fewer combos)
    options = []
    for exp_str, dte in valid_exps:
        for strike in atm_strikes:
            for right in ["C", "P"]:
                opt = Option(ticker, exp_str, strike, right, "SMART", "100", "USD")
                options.append((opt, exp_str, dte))

    qualified = []
    batch_size = 100
    for i in range(0, len(options), batch_size):
        batch_opts = [o[0] for o in options[i:i+batch_size]]
        try:
            ib.qualifyContracts(*batch_opts)
        except Exception:
            pass
        for opt_tuple in options[i:i+batch_size]:
            if opt_tuple[0].conId > 0:
                qualified.append(opt_tuple)

    if not qualified:
        return stock_px, pd.DataFrame(), underlying_ba

    # 6. Request market data in batches
    #    IBKR paper accounts: 100 concurrent data lines max.
    #    Back-month options are less frequently quoted — need longer wait.
    snap_batch = 45       # well under 100-line limit
    base_wait  = 3.0      # seconds per batch
    all_rows = {}         # keyed by (exp_str, strike, right) for retry dedup

    def _read_ticker(c, exp_str, dte):
        tk = ib.ticker(c)
        if tk is None:
            return None
        bid = float(tk.bid) if tk.bid and tk.bid > 0 and tk.bid != float('inf') else 0
        ask = float(tk.ask) if tk.ask and tk.ask > 0 and tk.ask != float('inf') else 0
        iv = 0.0
        if tk.modelGreeks and tk.modelGreeks.impliedVol:
            iv = float(tk.modelGreeks.impliedVol)
        elif bid > 0 and ask > 0:
            mid = (bid + ask) / 2
            T = dte / 365.0
            if T > 0 and stock_px > 0:
                strike_val = float(c.strike)
                if c.right == "P":
                    mid_ce = put_call_parity_call_equiv(
                        np.array([mid]), stock_px, np.array([strike_val]), np.array([T])
                    )[0]
                else:
                    mid_ce = mid
                iv_arr = implied_vol_vec(
                    np.array([max(mid_ce, 0.001)]),
                    np.array([stock_px]),
                    np.array([strike_val]),
                    np.array([T]),
                )
                iv = float(iv_arr[0]) if not np.isnan(iv_arr[0]) else 0.0
        vol = int(tk.volume) if tk.volume and tk.volume > 0 else 0
        exp_fmt = f"{exp_str[:4]}-{exp_str[4:6]}-{exp_str[6:8]}"
        return {
            "exp_date": exp_fmt,
            "type": "call" if c.right == "C" else "put",
            "strike": float(c.strike),
            "bid": bid, "ask": ask, "iv": iv,
            "volume": vol, "open_interest": 0,
        }

    # Pass 1: batch request
    empty_contracts = []   # contracts that got 0 data — retry candidates
    for i in range(0, len(qualified), snap_batch):
        batch = qualified[i:i+snap_batch]
        batch_contracts = [q[0] for q in batch]

        for c in batch_contracts:
            ib.reqMktData(c, "", False, False)
        ib.sleep(base_wait)

        for c, exp_str, dte in batch:
            row = _read_ticker(c, exp_str, dte)
            if row is None:
                continue
            key = (exp_str, c.strike, c.right)
            all_rows[key] = row
            if row["bid"] == 0 and row["ask"] == 0:
                empty_contracts.append((c, exp_str, dte))

        for c in batch_contracts:
            try:
                ib.cancelMktData(c)
            except Exception:
                pass
        ib.sleep(0.3)

    # Pass 2: retry contracts that got 0 data (back months typically)
    #   Smaller batch + longer wait gives IBKR time to respond
    if empty_contracts:
        retry_batch = 20
        retry_wait  = 5.0
        for i in range(0, len(empty_contracts), retry_batch):
            batch = empty_contracts[i:i+retry_batch]
            batch_contracts = [q[0] for q in batch]
            for c in batch_contracts:
                ib.reqMktData(c, "", False, False)
            ib.sleep(retry_wait)
            for c, exp_str, dte in batch:
                row = _read_ticker(c, exp_str, dte)
                if row and (row["bid"] > 0 or row["ask"] > 0):
                    key = (exp_str, c.strike, c.right)
                    all_rows[key] = row  # overwrite the empty row
            for c in batch_contracts:
                try:
                    ib.cancelMktData(c)
                except Exception:
                    pass
            ib.sleep(0.3)

    if all_rows:
        return stock_px, pd.DataFrame(list(all_rows.values())), underlying_ba
    return stock_px, pd.DataFrame(), underlying_ba


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

    # 2b. Fetch OI from separate endpoint (quote endpoint doesn't return OI)
    try:
        oi_url = (f"{THETADATA_URL}/v3/option/snapshot/open_interest"
                  f"?symbol={ticker}&expiration=*&format=json")
        oi_resp = requests.get(oi_url, timeout=120)
        oi_resp.raise_for_status()
        oi_data = oi_resp.json()
        oi_records = oi_data.get("response", []) if isinstance(oi_data, dict) else oi_data

        if oi_records:
            oi_rows = []
            for item in oi_records:
                contract = item.get("contract", {})
                data_arr = item.get("data", [])
                if not data_arr:
                    continue
                oi_val = data_arr[0].get("open_interest", 0)
                if oi_val is None:
                    oi_val = 0

                exp = str(contract.get("expiration", ""))
                if "-" not in exp and len(exp) == 8:
                    exp = f"{exp[:4]}-{exp[4:6]}-{exp[6:8]}"
                exp = exp[:10]

                right = str(contract.get("right", "")).upper()
                if right in ("CALL", "C"):
                    oi_type = "call"
                elif right in ("PUT", "P"):
                    oi_type = "put"
                else:
                    continue

                try:
                    strike = float(contract.get("strike", 0))
                except (TypeError, ValueError):
                    continue

                oi_rows.append({
                    "exp_date": exp,
                    "type": oi_type,
                    "strike": strike,
                    "oi_fetched": int(oi_val),
                })

            if oi_rows:
                oi_df = pd.DataFrame(oi_rows)
                df = df.merge(oi_df, on=["exp_date", "type", "strike"], how="left")
                df["open_interest"] = df["oi_fetched"].fillna(0).astype(int)
                df.drop(columns=["oi_fetched"], inplace=True)
    except Exception as ex:
        log.warning("ThetaData OI fetch failed for %s: %s (OI will be 0)", ticker, ex)

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

    except Exception:
        with _progress_lock:
            _progress["errors"] += 1

    return 0, pd.DataFrame()


# ═══════════════════════════════════════════════════════════════
# FF Computation + Ticker Scan
# ═══════════════════════════════════════════════════════════════

def scan_ticker_from_chain(ticker: str, stock_px: float, chain: pd.DataFrame, earn_by_root: dict[str, np.ndarray], today: datetime, verbose: bool = False, underlying_ba: float | None = None) -> list[dict]:
    """Scan one ticker for calendar spread opportunities from a pre-fetched chain.

    Works with both IBKR and EODHD data (same DataFrame format).

    Liquidity filters (per-leg, academic standard — Cao & Wei 2010):
      - Open Interest >= MIN_OI_LEG (200) on each call leg
      - Midpoint >= MIN_MID ($0.25) — OptionMetrics standard
    """
    results = []

    if stock_px <= 0 or chain.empty:
        return results

    # ── Underlying spread filter (Huh & Lin 2013) — percentage-based ──
    if underlying_ba is None:
        underlying_ba = _get_underlying_spread(ticker)
    if underlying_ba is not None and stock_px > 0:
        ba_pct = underlying_ba / stock_px
        if ba_pct > MAX_UNDERLYING_BA:
            if verbose:
                log.info("%s: SKIP — underlying spread %.3f%% > %.3f%%",
                         ticker, ba_pct * 100, MAX_UNDERLYING_BA * 100)
            return results

    # Compute DTE and Mid
    chain = chain.copy()
    chain["exp_dt"] = pd.to_datetime(chain["exp_date"], errors="coerce")
    chain["dte"] = (chain["exp_dt"] - pd.Timestamp(today).normalize()).dt.days

    # Diagnostic: show expirations BEFORE filtering
    if verbose:
        exp_summary = chain.groupby("exp_date").agg(
            rows=("bid", "size"),
            has_data=("bid", lambda x: ((x > 0) | (chain.loc[x.index, "ask"] > 0)).sum())
        )
        for exp, row in exp_summary.iterrows():
            dte_val = chain[chain["exp_date"] == exp]["dte"].iloc[0]
            log.info("%s:   exp %s (DTE %d): %d rows, %d with bid/ask",
                     ticker, exp, dte_val, row["rows"], row["has_data"])

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

        # ── Bid-ask spread cost (liquidity metric) ──
        call_ba = (front_best["ask"] - front_best["bid"]) + (back_best["ask"] - back_best["bid"])
        ba_pct = call_ba / (2 * spread_cost) if spread_cost > 0 else 999.0

        # Bid-ask filter: reject illiquid spreads (same as backtest)
        if ba_pct > BA_PCT_MAX:
            continue

        combo_label = f"{front_dte}-{back_dte}"
        results.append({
            "ticker": ticker,
            "combo": combo_label,
            "strike": strike,
            "stock_px": stock_px,
            "front_exp": front_exp_str,
            "front_dte": front_dte,
            "front_iv": round(front_iv * 100, 1),
            "back_exp": back_exp_str,
            "back_dte": back_dte,
            "back_iv": round(back_iv * 100, 1),
            "ff": round(ff, 4),
            "call_cost": round(spread_cost, 2),
            "call_delta": round(call_delta_val, 3),
            "front_oi": front_oi,
            "back_oi": back_oi,
            "ba_pct": round(ba_pct, 4),
        })

        if verbose:
            log.info("%s: SIGNAL! FF=%.3f (%.1f%%), K=%.0f, "
                     "dc=%.2f, cost=$%.2f, %s(DTE=%d) -> %s(DTE=%d)",
                     combo_label, ff, ff*100, strike,
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
# IBKR Scanner Universe — query IBKR directly for optionable US stocks
# ═══════════════════════════════════════════════════════════════

def get_ibkr_universe(ib: Any, max_per_scan: int = 50) -> list[str]:
    """Build scan universe by querying IBKR's built-in market scanners.

    Runs multiple scans to capture different FF candidate profiles:
    - HIGH_OPT_IMP_VOLAT: high current IV (potential front-month richness)
    - TOP_OPT_IMP_VOL_GAIN: IV increasing (term structure steepening)
    - HOT_BY_OPT_VOLUME: liquid option markets (tradeable spreads)
    - MOST_ACTIVE_USD: most active US stocks (liquid underlyings)

    Returns deduplicated sorted list of tickers.
    """
    from ib_insync import ScannerSubscription

    scan_codes = [
        "HIGH_OPT_IMP_VOLAT",
        "TOP_OPT_IMP_VOL_GAIN",
        "HOT_BY_OPT_VOLUME",
        "MOST_ACTIVE_USD",
    ]

    all_tickers = set()
    for code in scan_codes:
        try:
            sub = ScannerSubscription(
                instrument="STK",
                locationCode="STK.US.MAJOR",
                scanCode=code,
                numberOfRows=max_per_scan,
            )
            results = ib.reqScannerData(sub)
            for item in results:
                sym = item.contractDetails.contract.symbol
                if sym:
                    all_tickers.add(sym)
            log.info("IBKR scanner %s: %d results", code, len(results))
        except Exception as ex:
            log.warning("IBKR scanner %s failed: %s", code, ex)

    tickers = sorted(all_tickers)
    log.info("IBKR universe: %d unique tickers from %d scans", len(tickers), len(scan_codes))

    # Fallback to DB if IBKR scanners return empty (e.g. market closed, paper account)
    if not tickers:
        log.warning("IBKR scanners returned 0 tickers (market closed?) — falling back to DB")
        tickers = get_sp500_tickers()

    return tickers


# ═══════════════════════════════════════════════════════════════
# Main Scanner — IBKR mode (sequential, live data)
# ═══════════════════════════════════════════════════════════════

def run_scanner_ibkr(ib: Any, tickers: list[str] | None = None, test_mode: bool = False) -> pd.DataFrame:
    """Scanner using IBKR live data. Sequential but accurate prices.

    If no tickers provided, queries IBKR scanners for optionable US stocks.
    """
    today = datetime.now()
    t0 = time.time()
    log.info("=" * 70)
    log.info("FORWARD FACTOR SCANNER [IBKR LIVE] — %s", today.strftime('%Y-%m-%d %H:%M'))
    log.info("=" * 70)

    if tickers is None:
        tickers = get_ibkr_universe(ib)
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
            stock_px, chain, stk_ba = fetch_option_chain_ibkr(ib, ticker, today)
            if stock_px <= 0 or chain.empty:
                n_no_data += 1
                continue

            signals = scan_ticker_from_chain(
                ticker, stock_px, chain, earn_by_root, today, verbose=verbose,
                underlying_ba=stk_ba
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
# IBKR Parallel Scanner — multi-connection mass scan
# ═══════════════════════════════════════════════════════════════

def _worker_scan(worker_id: int, tickers: list[str], port: int,
                 client_id_base: int, result_queue, earn_by_root: dict) -> None:
    """Worker process: connect to IBKR and scan a chunk of tickers.

    Each worker gets its own IBKR connection with a unique clientId.
    Results are put into a multiprocessing Queue.
    """
    import asyncio
    try:
        asyncio.get_event_loop()
    except RuntimeError:
        asyncio.set_event_loop(asyncio.new_event_loop())

    from ib_insync import IB
    cid = client_id_base + worker_id
    ib = IB()

    try:
        ib.connect("127.0.0.1", port, clientId=cid, timeout=15)
    except Exception as ex:
        log.error("Worker %d: cannot connect (clientId=%d): %s", worker_id, cid, ex)
        result_queue.put((worker_id, []))
        return

    today = datetime.now()
    signals = []
    n_ok = 0
    n_err = 0

    for i, ticker in enumerate(tickers):
        if (i + 1) % 10 == 0 or i == 0:
            log.info("W%d [%d/%d] %s (%d sigs)", worker_id, i+1, len(tickers), ticker, len(signals))
        try:
            stock_px, chain, stk_ba = fetch_option_chain_ibkr(ib, ticker, today)
            if stock_px <= 0 or chain.empty:
                continue
            sigs = scan_ticker_from_chain(
                ticker, stock_px, chain, earn_by_root, today,
                underlying_ba=stk_ba,
            )
            signals.extend(sigs)
            n_ok += 1
        except (ConnectionError, OSError) as ex:
            log.warning("W%d: connection lost at %s: %s — reconnecting", worker_id, ticker, ex)
            try:
                ib.disconnect()
                time.sleep(2)
                ib.connect("127.0.0.1", port, clientId=cid, timeout=15)
            except Exception:
                log.error("W%d: reconnect failed — stopping", worker_id)
                break
            n_err += 1
        except Exception:
            n_err += 1

    try:
        ib.disconnect()
    except Exception:
        pass

    log.info("W%d done: %d/%d ok, %d sigs, %d err", worker_id, n_ok, len(tickers), len(signals), n_err)
    result_queue.put((worker_id, signals))


def run_scanner_ibkr_parallel(port: int = 4002, n_workers: int = 4,
                               tickers: list[str] | None = None) -> pd.DataFrame:
    """Scan NASDAQ/US universe in parallel using multiple IBKR connections.

    Each worker gets its own IBKR connection (different clientId) and scans
    a chunk of the universe simultaneously.  ~4x faster than sequential.

    Args:
        port: IBKR Gateway port (4002=paper, 4001=live)
        n_workers: Number of parallel IBKR connections (default 4)
        tickers: Override ticker list (default: DB universe + IBKR scanners)
    """
    from multiprocessing import Process, Queue
    import asyncio
    try:
        asyncio.get_event_loop()
    except RuntimeError:
        asyncio.set_event_loop(asyncio.new_event_loop())

    from ib_insync import IB

    today = datetime.now()
    t0 = time.time()
    log.info("=" * 70)
    log.info("FORWARD FACTOR SCANNER [IBKR PARALLEL x%d] — %s", n_workers, today.strftime('%Y-%m-%d %H:%M'))
    log.info("=" * 70)

    # 1. Build universe — connect briefly to get IBKR scanner tickers, merge with DB
    ib = IB()
    ib.connect("127.0.0.1", port, clientId=60, timeout=10)

    if tickers is None:
        # Use IBKR scanner universe (targeted, liquid tickers)
        # --db flag merges with full DB for exhaustive scan
        tickers = sorted(get_ibkr_universe(ib))
        log.info("Universe: %d tickers from IBKR scanners", len(tickers))

    # 2. Load earnings (shared across all workers)
    earn_by_root = get_earnings_dates(tickers, ib=ib)
    ib.disconnect()

    log.info("Tickers: %d", len(tickers))
    log.info("Workers: %d parallel connections", n_workers)
    log.info("FF threshold: %.3f", FF_THRESHOLD_DEFAULT)
    log.info("Estimated time: ~%.0f min", len(tickers) * 5 / n_workers / 60)

    # 3. Split tickers round-robin (distributes expensive tickers evenly)
    chunks = [[] for _ in range(n_workers)]
    for i, t in enumerate(tickers):
        chunks[i % n_workers].append(t)
    for w, chunk in enumerate(chunks):
        log.info("  Worker %d: %d tickers (%s ... %s)", w, len(chunk), chunk[0], chunk[-1])

    # 4. Launch workers as separate processes
    #    clientId base = 61, so workers get 61, 62, 63, 64, ...
    result_queue = Queue()
    processes = []
    for w in range(n_workers):
        p = Process(
            target=_worker_scan,
            args=(w, chunks[w], port, 61, result_queue, earn_by_root),
        )
        p.start()
        processes.append(p)
        time.sleep(1)  # stagger connections to avoid IBKR throttle

    # 5. Collect results
    all_signals = []
    for _ in range(n_workers):
        try:
            worker_id, signals = result_queue.get(timeout=1800)  # 30 min max
            all_signals.extend(signals)
            log.info("Worker %d returned %d signals", worker_id, len(signals))
        except Exception as ex:
            log.error("Worker result timeout: %s", ex)

    for p in processes:
        p.join(timeout=60)

    elapsed = time.time() - t0
    log.info("=" * 70)
    log.info("PARALLEL SCAN COMPLETE: %d tickers in %.1fs (%.1fs/ticker, %.1f min)",
             len(tickers), elapsed, elapsed / max(len(tickers), 1), elapsed / 60)
    log.info("Found %d raw signals", len(all_signals))
    log.info("=" * 70)

    if not all_signals:
        log.info("No signals found!")
        return pd.DataFrame()

    df = pd.DataFrame(all_signals)
    df = df.sort_values("ff", ascending=False)

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
    log.info("%-6s  %-5s  %-6s  %-7s  %-5s  %-6s  %-8s  %-6s  %-6s  %-10s  %-10s",
             "Ticker", "Combo", "Strike", "Stock", "Delta",
             "FF", "Cost", "F.OI", "B.OI", "FrontExp", "BackExp")

    for _, r in top.iterrows():
        f_oi = f"{r.get('front_oi', 0):,}" if r.get('front_oi', 0) > 0 else "N/A"
        b_oi = f"{r.get('back_oi', 0):,}" if r.get('back_oi', 0) > 0 else "N/A"
        cd = f"{r['call_delta']:.2f}" if pd.notna(r.get("call_delta")) else "N/A"
        ff_display = f"{r['ff']:.2f}" if pd.notna(r.get("ff")) else "N/A"
        log.info("%6s  %5s  $%5.0f  $%6.2f  %5s  %6s  $%7.2f  %6s  %6s  %10s  %10s",
                 r['ticker'], r['combo'], r['strike'],
                 r['stock_px'], cd, ff_display,
                 r['call_cost'], f_oi, b_oi, r['front_exp'], r['back_exp'])

    # Save to file
    out_file = OUT / f"signals_{today.strftime('%Y%m%d')}.csv"
    df.to_csv(str(out_file), index=False)
    log.info("All %d signals saved to %s", len(df), out_file)

    top_file = OUT / f"top{TOP_N}_{today.strftime('%Y%m%d')}.csv"
    top.to_csv(str(top_file), index=False)
    log.info("Top %d saved to %s", TOP_N, top_file)


if __name__ == "__main__":
    test_mode = "--test" in sys.argv
    use_ibkr = "--ibkr" in sys.argv
    use_db = "--db" in sys.argv        # Force DB tickers instead of IBKR scanner
    use_parallel = "--parallel" in sys.argv  # Multi-connection parallel scan

    # --workers N (default 4)
    n_workers = 4
    if "--workers" in sys.argv:
        idx = sys.argv.index("--workers")
        if idx + 1 < len(sys.argv):
            n_workers = int(sys.argv[idx + 1])

    # --port N (default 4002)
    port = 4002
    if "--port" in sys.argv:
        idx = sys.argv.index("--port")
        if idx + 1 < len(sys.argv):
            port = int(sys.argv[idx + 1])

    # Single ticker mode
    ticker_arg = None
    if "--ticker" in sys.argv:
        idx = sys.argv.index("--ticker")
        if idx + 1 < len(sys.argv):
            ticker_arg = [sys.argv[idx + 1]]

    # --tickers AAPL,GOOG,MSFT (comma-separated list)
    if "--tickers" in sys.argv:
        idx = sys.argv.index("--tickers")
        if idx + 1 < len(sys.argv):
            ticker_arg = [t.strip() for t in sys.argv[idx + 1].split(",")]

    if use_parallel:
        # Parallel multi-connection IBKR scanner (mass NASDAQ scan)
        run_scanner_ibkr_parallel(port=port, n_workers=n_workers, tickers=ticker_arg)

    elif use_ibkr:
        # Sequential single-connection IBKR scanner
        try:
            asyncio.get_event_loop()
        except RuntimeError:
            asyncio.set_event_loop(asyncio.new_event_loop())

        from ib_insync import IB
        ib = IB()

        log.info("Connecting to IBKR on port %d...", port)
        ib.connect("127.0.0.1", port, clientId=60, timeout=10)
        log.info("Connected: %s", ib.isConnected())

        # --db: use DB tickers; otherwise IBKR scanner discovers universe
        tickers = ticker_arg
        if tickers is None and use_db:
            tickers = get_sp500_tickers()

        try:
            run_scanner_ibkr(ib, tickers=tickers, test_mode=test_mode)
        finally:
            ib.disconnect()
            log.info("Disconnected from IBKR")
    else:
        from core.config import ensure_theta_terminal
        ensure_theta_terminal()
        run_scanner(tickers=ticker_arg, test_mode=test_mode)
