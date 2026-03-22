"""
Daily Scanner — Forward Factor Double Calendar Spread Signals

Two modes:
  --ibkr : Live data from IBKR (requires paid market data subscription)
  default: EODHD API (delayed, parallel — recommended for scanning)

Workflow: EODHD scans all 500 tickers for FF signals → place MKT orders on IBKR

Usage:
    python scanner.py                 # EODHD scan (recommended)
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
from concurrent.futures import ThreadPoolExecutor, as_completed

ROOT = Path(r"C:\Users\ANTEC MSI\Desktop\pro\Option trading")
DB   = ROOT / "sp500_options.db"
OUT  = ROOT / "output"
OUT.mkdir(exist_ok=True)

API_KEY = "688202596a2968.33250849"
BASE_URL = "https://eodhd.com/api"

# ── Strategy parameters (same as backtest) ──
DTE_COMBOS   = [(30, 60), (30, 90), (60, 90)]
DTE_TOL      = 5       # Aligned with research/spreads.py
STRIKE_PCT   = 0.03   # max 3% from ATM
# FF thresholds in OLD formula (fwd_var/front_var - 1), from PDF filtered model
FF_THRESHOLD_OLD = {"30-60": 0.230, "30-90": 0.230, "60-90": 0.200}
FF_THRESHOLD_DEFAULT = 0.230
MIN_COST     = 1.00   # $1.00/share minimum spread cost
MIN_OI_LEG   = 100    # Min open interest per leg (Cao&Wei 2010, practitioner minimum)
MIN_MID      = 0.25   # Min midpoint price per leg (OptionMetrics standard)
TOP_N        = 20     # Top N signals to output
MAX_WORKERS  = 10     # Concurrent API calls (EODHD mode)

# Thread-safe progress counter
_progress_lock = threading.Lock()
_progress = {"done": 0, "signals": 0, "errors": 0}

# Reusable HTTP session (connection pooling)
_session = None

def _get_session():
    global _session
    if _session is None:
        _session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=MAX_WORKERS,
            pool_maxsize=MAX_WORKERS,
            max_retries=requests.adapters.Retry(total=2, backoff_factor=0.5,
                                                 status_forcelist=[429, 500, 502, 503]),
        )
        _session.mount("https://", adapter)
    return _session


def get_sp500_tickers():
    """Get S&P 500 ticker list from our database."""
    conn = sqlite3.connect(str(DB))
    tickers = pd.read_sql_query(
        "SELECT DISTINCT root FROM contracts ORDER BY root", conn
    )["root"].tolist()
    conn.close()
    return tickers


def get_earnings_dates(tickers, days_ahead=120):
    """Get upcoming earnings dates from DB + EODHD calendar.

    Only keeps earnings for tickers in the scan universe.
    """
    ticker_set = set(tickers)
    conn = sqlite3.connect(str(DB))
    today_int = int(datetime.now().strftime("%Y%m%d"))
    future_int = int((datetime.now() + timedelta(days=days_ahead)).strftime("%Y%m%d"))

    df = pd.read_sql_query(
        f"SELECT root, report_date FROM earnings "
        f"WHERE report_date >= {today_int} AND report_date <= {future_int}",
        conn
    )
    conn.close()

    # Also try EODHD calendar for more up-to-date data
    today_str = datetime.now().strftime("%Y-%m-%d")
    future_str = (datetime.now() + timedelta(days=days_ahead)).strftime("%Y-%m-%d")
    try:
        sess = _get_session()
        url = (f"{BASE_URL}/calendar/earnings?"
               f"from={today_str}&to={future_str}&api_token={API_KEY}&fmt=json")
        resp = sess.get(url, timeout=30)
        if resp.status_code == 200:
            data = resp.json()
            rows = []
            for e in data.get("earnings", []):
                ticker = e.get("code", "").replace(".US", "")
                rd = e.get("report_date", "")
                if ticker in ticker_set and rd:
                    rows.append({"root": ticker, "report_date": int(rd.replace("-", ""))})
            if rows:
                df = pd.concat([df, pd.DataFrame(rows)], ignore_index=True)
    except Exception as ex:
        print(f"  Warning: EODHD calendar fetch failed: {ex}")

    # Build lookup: root -> sorted array of earnings dates (only scan universe)
    earn_by_root = {}
    if not df.empty:
        df["report_date"] = df["report_date"].astype(int)
        df = df[df["root"].isin(ticker_set)]
        for root, grp in df.groupby("root"):
            earn_by_root[root] = np.sort(grp["report_date"].values)

    print(f"  Earnings dates loaded: {len(earn_by_root)} tickers")
    return earn_by_root


def has_earnings_between(root, front_exp_int, back_exp_int, earn_by_root):
    """Check if earnings falls between front and back expiry."""
    if root not in earn_by_root:
        return False
    edates = earn_by_root[root]
    idx = np.searchsorted(edates, front_exp_int, side="left")
    return idx < len(edates) and edates[idx] <= back_exp_int


# ═══════════════════════════════════════════════════════════════
# IBKR Option Chain Fetcher
# ═══════════════════════════════════════════════════════════════

def fetch_option_chain_ibkr(ib, ticker, today):
    """Fetch option chain from IBKR (live or delayed 15min).

    Returns (stock_price, DataFrame) with same format as EODHD version.
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

    # 4. Filter strikes to near ATM (5% range, scan_ticker filters to 3%)
    atm_strikes = sorted([s for s in chain.strikes
                          if abs(s - stock_px) / stock_px <= 0.05])

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
# EODHD Option Chain Fetcher (fallback)
# ═══════════════════════════════════════════════════════════════

def fetch_option_chain_eodhd(ticker):
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

def compute_ff(iv_front, iv_back, dte_front, dte_back):
    """
    Compute Forward Factor (OLD formula, aligned with PDF):
    FF = fwd_var / front_var - 1
    where fwd_var = (sigma_b^2 * T_b - sigma_f^2 * T_f) / (T_b - T_f)
    """
    T_f = dte_front / 365.0
    T_b = dte_back / 365.0
    dT = T_b - T_f
    if dT <= 0 or iv_front <= 0 or iv_back <= 0:
        return np.nan

    fwd_var = (iv_back**2 * T_b - iv_front**2 * T_f) / dT
    front_var = iv_front**2
    if front_var <= 0:
        return np.nan

    ff = fwd_var / front_var - 1.0
    return ff


def scan_ticker_from_chain(ticker, stock_px, chain, earn_by_root, today, verbose=False):
    """Scan one ticker for calendar spread opportunities from a pre-fetched chain.

    Works with both IBKR and EODHD data (same DataFrame format).

    Liquidity filters (per-leg, academic standard — Cao & Wei 2010):
      - Open Interest >= MIN_OI_LEG (200) on each call leg
      - Midpoint >= MIN_MID ($0.25) — OptionMetrics standard
    """
    results = []

    if stock_px <= 0 or chain.empty:
        return results

    # Compute DTE
    chain = chain.copy()
    chain["exp_dt"] = pd.to_datetime(chain["exp_date"], errors="coerce")
    chain["dte"] = (chain["exp_dt"] - pd.Timestamp(today)).dt.days

    # Filter: valid bid/ask/iv, DTE 15-120
    chain = chain[
        (chain["bid"] > 0) & (chain["ask"] > 0) &
        (chain["iv"] > 0) &
        (chain["dte"] >= 15) & (chain["dte"] <= 120)
    ].copy()
    chain["mid"] = (chain["bid"] + chain["ask"]) / 2

    if chain.empty:
        return results

    # Check if OI data is available (EODHD yes, IBKR may not)
    has_oi = "open_interest" in chain.columns and chain["open_interest"].max() > 0

    calls = chain[chain["type"] == "call"].copy()
    puts = chain[chain["type"] == "put"].copy()

    if verbose:
        print(f"    {ticker}: ${stock_px:.2f}, {len(calls)} calls, {len(puts)} puts")

    for short_dte, long_dte in DTE_COMBOS:
        # Find front: closest expiry to short_dte within tolerance, ATM
        front = calls[
            (calls["dte"] >= short_dte - DTE_TOL) &
            (calls["dte"] <= short_dte + DTE_TOL) &
            ((calls["strike"] - stock_px).abs() / stock_px <= STRIKE_PCT)
        ].copy()
        if front.empty:
            continue

        # Best front = closest to ATM within closest DTE to target
        front["dte_diff"] = (front["dte"] - short_dte).abs()
        best_dte = front["dte_diff"].min()
        front_exp_mask = front["dte_diff"] == best_dte
        front_atm_candidates = front[front_exp_mask].copy()
        front_atm_candidates["strike_pct"] = (
            (front_atm_candidates["strike"] - stock_px).abs() / stock_px
        )
        front_atm = front_atm_candidates.loc[
            front_atm_candidates["strike_pct"].idxmin()
        ]

        strike = front_atm["strike"]
        front_iv = front_atm["iv"]
        front_mid = front_atm["mid"]
        front_dte = int(front_atm["dte"])
        front_exp = str(front_atm["exp_date"])
        front_oi = int(front_atm.get("open_interest", 0)) if has_oi else -1

        # Liquidity: front call OI >= MIN_OI_LEG and mid >= MIN_MID
        if has_oi and front_oi < MIN_OI_LEG:
            continue
        if front_mid < MIN_MID:
            continue

        # Find back: different expiry, closest DTE to long_dte, same strike
        back = calls[
            (calls["dte"] >= long_dte - DTE_TOL) &
            (calls["dte"] <= long_dte + DTE_TOL) &
            (calls["exp_date"] != front_exp)
        ].copy()
        if back.empty:
            continue

        # Match at same strike
        back_same = back[(back["strike"] - strike).abs() < 0.01]
        if back_same.empty:
            back["sdiff"] = (back["strike"] - strike).abs()
            back_best = back.loc[back["sdiff"].idxmin()]
            if back_best["sdiff"] / strike > 0.02:
                continue
        else:
            back_same = back_same.copy()
            back_same["dte_diff"] = (back_same["dte"] - long_dte).abs()
            back_best = back_same.loc[back_same["dte_diff"].idxmin()]

        back_iv = back_best["iv"]
        back_mid = back_best["mid"]
        back_dte = int(back_best["dte"])
        back_exp = str(back_best["exp_date"])
        back_oi = int(back_best.get("open_interest", 0)) if has_oi else -1

        # Liquidity: back call OI >= MIN_OI_LEG and mid >= MIN_MID
        if has_oi and back_oi < MIN_OI_LEG:
            continue
        if back_mid < MIN_MID:
            continue

        # Spread cost
        spread_cost = back_mid - front_mid
        if spread_cost < MIN_COST:
            continue

        # Forward Factor (OLD formula)
        ff = compute_ff(front_iv, back_iv, front_dte, back_dte)
        combo_key = f"{short_dte}-{long_dte}"
        ff_thresh = FF_THRESHOLD_OLD.get(combo_key, FF_THRESHOLD_DEFAULT)
        if np.isnan(ff) or ff < ff_thresh:
            continue

        # Earnings filter
        front_exp_int = int(front_exp.replace("-", "")[:8])
        back_exp_int = int(back_exp.replace("-", "")[:8])
        if has_earnings_between(ticker, front_exp_int, back_exp_int, earn_by_root):
            continue

        # Put leg (double calendar)
        put_cost = np.nan
        combined_cost = np.nan
        put_front_cands = puts[
            (puts["exp_date"] == front_exp) &
            ((puts["strike"] - strike).abs() / strike <= STRIKE_PCT)
        ]
        put_back_cands = puts[
            (puts["exp_date"] == back_exp) &
            ((puts["strike"] - strike).abs() / strike <= STRIKE_PCT)
        ]
        if not put_front_cands.empty and not put_back_cands.empty:
            pf = put_front_cands.loc[
                (put_front_cands["strike"] - strike).abs().idxmin()
            ]
            pb = put_back_cands.loc[
                (put_back_cands["strike"] - strike).abs().idxmin()
            ]
            # Check put legs OI (if available)
            pf_oi = int(pf.get("open_interest", 0)) if has_oi else -1
            pb_oi = int(pb.get("open_interest", 0)) if has_oi else -1
            pf_ok = (not has_oi) or pf_oi >= MIN_OI_LEG
            pb_ok = (not has_oi) or pb_oi >= MIN_OI_LEG
            if pf_ok and pb_ok:
                pf_mid = (pf["bid"] + pf["ask"]) / 2
                pb_mid = (pb["bid"] + pb["ask"]) / 2
                put_cost = pb_mid - pf_mid
                if put_cost > 0:
                    combined_cost = spread_cost + put_cost

        # Convert FF to GUI for display: GUI = 1/sqrt(1+old) - 1
        ff_gui = 1.0 / np.sqrt(1.0 + ff) - 1.0 if ff > -1 else 0.0

        results.append({
            "ticker": ticker,
            "combo": f"{short_dte}-{long_dte}",
            "strike": strike,
            "stock_px": stock_px,
            "front_exp": front_exp,
            "front_dte": front_dte,
            "front_iv": round(front_iv * 100, 1),
            "back_exp": back_exp,
            "back_dte": back_dte,
            "back_iv": round(back_iv * 100, 1),
            "ff_old": round(ff, 4),
            "ff": round(ff_gui * 100, 1),
            "call_cost": round(spread_cost, 2),
            "put_cost": round(put_cost, 2) if not np.isnan(put_cost) else None,
            "dbl_cost": round(combined_cost, 2) if not np.isnan(combined_cost) else None,
            "front_oi": front_oi,
            "back_oi": back_oi,
        })

        if verbose:
            print(f"    {short_dte}-{long_dte}: SIGNAL! FF_old={ff:.3f} "
                  f"(GUI={ff_gui*100:.1f}%), K={strike:.0f}, "
                  f"cost=${spread_cost:.2f}, "
                  f"{front_exp}(DTE={front_dte}) -> {back_exp}(DTE={back_dte})")

    return results


# Legacy wrapper for EODHD mode
def scan_ticker(ticker, earn_by_root, today, verbose=False):
    stock_px, chain = fetch_option_chain_eodhd(ticker)
    return scan_ticker_from_chain(ticker, stock_px, chain, earn_by_root, today, verbose)


def _scan_one(args):
    """Worker function for parallel EODHD scanning."""
    ticker, earn_by_root, today, verbose = args
    signals = scan_ticker(ticker, earn_by_root, today, verbose=verbose)
    with _progress_lock:
        _progress["done"] += 1
        _progress["signals"] += len(signals)
        done = _progress["done"]
    if done % 50 == 0 or done == 1:
        print(f"  [{done}] {ticker}... ({_progress['signals']} signals so far)")
    return ticker, signals


# ═══════════════════════════════════════════════════════════════
# Main Scanner — IBKR mode (sequential, live data)
# ═══════════════════════════════════════════════════════════════

def run_scanner_ibkr(ib, tickers=None, test_mode=False):
    """Scanner using IBKR live data. Sequential but accurate prices.

    ~3s/ticker = ~25 min for 500 tickers (vs 5 min EODHD but real-time).
    """
    today = datetime.now()
    t0 = time.time()
    print("=" * 70)
    print(f"FORWARD FACTOR SCANNER [IBKR LIVE] — {today.strftime('%Y-%m-%d %H:%M')}")
    print("=" * 70)

    if tickers is None:
        tickers = get_sp500_tickers()
    if test_mode:
        tickers = tickers[:5]

    print(f"  Tickers: {len(tickers)}")
    print(f"  Mode: IBKR real-time")
    print(f"  DTE combos: {DTE_COMBOS}")
    print(f"  FF thresholds (old): {FF_THRESHOLD_OLD}")
    print(f"  Min spread cost: ${MIN_COST}")
    print(f"  Min OI per leg: {MIN_OI_LEG}")
    print(f"  Min midpoint: ${MIN_MID}")

    # Load earnings
    earn_by_root = get_earnings_dates(tickers)

    all_signals = []
    n_errors = 0
    n_no_data = 0
    verbose = len(tickers) <= 5

    for i, ticker in enumerate(tickers):
        if (i + 1) % 25 == 0 or i == 0:
            elapsed = time.time() - t0
            eta = (elapsed / max(i, 1)) * (len(tickers) - i) if i > 0 else 0
            print(f"  [{i+1}/{len(tickers)}] {ticker}... "
                  f"({len(all_signals)} signals, {elapsed:.0f}s elapsed, "
                  f"ETA {eta:.0f}s)")

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
                print(f"    ERROR: {ex}")

    elapsed = time.time() - t0
    print(f"\n  Scanned {len(tickers)} tickers in {elapsed:.1f}s "
          f"({elapsed/len(tickers):.2f}s/ticker)")
    print(f"  Found {len(all_signals)} raw signals, "
          f"{n_errors} errors, {n_no_data} no data")

    if not all_signals:
        print("  No signals found!")
        return pd.DataFrame()

    df = pd.DataFrame(all_signals)
    df = df.sort_values("ff_old", ascending=False)

    # Top N
    top = df.head(TOP_N)

    _print_results(top, df, today)
    return df


# ═══════════════════════════════════════════════════════════════
# Main Scanner — EODHD mode (parallel, delayed data)
# ═══════════════════════════════════════════════════════════════

def run_scanner(tickers=None, test_mode=False, ib=None):
    """Main scanner entry point.

    If ib is provided, uses IBKR live data.
    Otherwise falls back to EODHD parallel scan.
    """
    if ib is not None:
        return run_scanner_ibkr(ib, tickers, test_mode)

    today = datetime.now()
    t0 = time.time()
    print("=" * 70)
    print(f"FORWARD FACTOR SCANNER [EODHD] — {today.strftime('%Y-%m-%d %H:%M')}")
    print("=" * 70)

    if tickers is None:
        tickers = get_sp500_tickers()
    if test_mode:
        tickers = tickers[:5]

    n_workers = min(MAX_WORKERS, len(tickers))
    print(f"  Tickers: {len(tickers)}")
    print(f"  Workers: {n_workers} parallel")
    print(f"  DTE combos: {DTE_COMBOS}")
    print(f"  FF thresholds (old): {FF_THRESHOLD_OLD}")
    print(f"  Min spread cost: ${MIN_COST}")
    print(f"  Min OI per leg: {MIN_OI_LEG}")
    print(f"  Min midpoint: ${MIN_MID}")

    # Load earnings
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
    print(f"\n  Scanned {len(tickers)} tickers in {elapsed:.1f}s "
          f"({elapsed/len(tickers):.2f}s/ticker)")
    print(f"  Found {len(all_signals)} raw signals, "
          f"{_progress['errors']} errors")

    if not all_signals:
        print("  No signals found!")
        return pd.DataFrame()

    df = pd.DataFrame(all_signals)
    df = df.sort_values("ff_old", ascending=False)

    top = df.head(TOP_N)
    _print_results(top, df, today)
    return df


def _print_results(top, df, today):
    """Print top signals and save to files."""
    print(f"\n{'='*70}")
    print(f"TOP {TOP_N} SIGNALS (ranked by FF)")
    print("=" * 70)
    print(f"{'Ticker':>6s}  {'Combo':>5s}  {'Strike':>7s}  {'Stock':>7s}  "
          f"{'FF%':>5s}  {'FrontIV':>7s}  {'BackIV':>6s}  "
          f"{'DblCost':>8s}  {'F.OI':>6s}  {'B.OI':>6s}  "
          f"{'FrontExp':>10s}  {'BackExp':>10s}")
    print("-" * 105)

    for _, r in top.iterrows():
        dbl = f"${r['dbl_cost']:.2f}" if pd.notna(r.get("dbl_cost")) and r["dbl_cost"] else "N/A"
        f_oi = f"{r.get('front_oi', 0):,}" if r.get('front_oi', 0) > 0 else "N/A"
        b_oi = f"{r.get('back_oi', 0):,}" if r.get('back_oi', 0) > 0 else "N/A"
        print(f"{r['ticker']:>6s}  {r['combo']:>5s}  ${r['strike']:>6.0f}  "
              f"${r['stock_px']:>6.2f}  {r['ff']:>4.1f}%  "
              f"{r['front_iv']:>6.1f}%  {r['back_iv']:>5.1f}%  "
              f"{dbl:>8s}  {f_oi:>6s}  {b_oi:>6s}  "
              f"{r['front_exp']:>10s}  {r['back_exp']:>10s}")

    # Save to file
    out_file = OUT / f"signals_{today.strftime('%Y%m%d')}.csv"
    df.to_csv(str(out_file), index=False)
    print(f"\nAll {len(df)} signals saved to {out_file}")

    top_file = OUT / f"top{TOP_N}_{today.strftime('%Y%m%d')}.csv"
    top.to_csv(str(top_file), index=False)
    print(f"Top {TOP_N} saved to {top_file}")


if __name__ == "__main__":
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

        print(f"Connecting to IBKR on port {port}...")
        ib.connect("127.0.0.1", port, clientId=60, timeout=10)
        print(f"Connected: {ib.isConnected()}")

        try:
            run_scanner_ibkr(ib, tickers=ticker_arg, test_mode=test_mode)
        finally:
            ib.disconnect()
            print("Disconnected from IBKR")
    else:
        run_scanner(tickers=ticker_arg, test_mode=test_mode)
