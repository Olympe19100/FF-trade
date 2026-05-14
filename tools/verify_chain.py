"""
Verify IBKR option chain fetching — works with markets closed (frozen data).

Run: python tools/verify_chain.py [--port 7497] [--tickers AAPL,MSFT,TSLA]

Tests:
  1. IBKR connection (TWS or Gateway)
  2. Stock qualification + price (frozen/delayed)
  3. reqSecDefOptParams → strikes & expirations
  4. ATM strike selection + contract qualification
  5. Market data snapshot (bid/ask/greeks)
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import argparse
from datetime import datetime

# ib_insync import with asyncio fix
import asyncio
try:
    asyncio.get_event_loop()
except RuntimeError:
    asyncio.set_event_loop(asyncio.new_event_loop())

from ib_insync import IB, Stock, Option


def run_verification(host="127.0.0.1", port=7497, tickers=None):
    if tickers is None:
        tickers = ["AAPL", "MSFT", "TSLA"]

    ib = IB()
    results = {}

    # ── Step 1: Connection ──
    print("=" * 60)
    print("STEP 1: Connecting to IBKR")
    print("=" * 60)
    try:
        ib.connect(host, port, clientId=99, timeout=10, readonly=True)
        print(f"  [OK] Connected to {host}:{port}")
        print(f"  [OK] Server version: {ib.client.serverVersion()}")
        accounts = ib.managedAccounts()
        print(f"  [OK] Account: {accounts[0] if accounts else 'N/A'}")
    except Exception as ex:
        print(f"  [FAIL] Cannot connect: {ex}")
        print()
        print("Make sure TWS or IB Gateway is running.")
        print("  - TWS Paper: port 7497")
        print("  - TWS Live:  port 7496")
        print("  - GW Paper:  port 4002")
        print("  - GW Live:   port 4001")
        return

    # Use frozen data (works when markets are closed)
    ib.reqMarketDataType(4)
    print("  [OK] Market data type set to 4 (frozen/delayed)")
    print()

    today = datetime.now()

    for ticker in tickers:
        print("=" * 60)
        print(f"STEP 2-5: Testing {ticker}")
        print("=" * 60)
        result = {"ticker": ticker}

        # ── Step 2: Qualify stock + get price ──
        print(f"\n  [2] Qualifying stock {ticker}...")
        stock = Stock(ticker, "SMART", "USD")
        try:
            ib.qualifyContracts(stock)
        except Exception as ex:
            print(f"      [FAIL] Cannot qualify: {ex}")
            results[ticker] = result
            continue

        if stock.conId == 0:
            print("      [FAIL] conId=0, stock not found")
            results[ticker] = result
            continue

        print(f"      [OK] conId={stock.conId}")

        ib.reqMktData(stock, "", False, False)
        ib.sleep(2)
        stk_tk = ib.ticker(stock)

        stock_px = 0
        if stk_tk:
            for attr in ['marketPrice', 'last', 'close']:
                val = getattr(stk_tk, attr, None)
                if callable(val):
                    val = val()
                if val and val == val and val > 0 and val != float('inf'):
                    stock_px = float(val)
                    break
            bid = float(stk_tk.bid) if stk_tk.bid and stk_tk.bid > 0 and stk_tk.bid != float('inf') else 0
            ask = float(stk_tk.ask) if stk_tk.ask and stk_tk.ask > 0 and stk_tk.ask != float('inf') else 0
            print(f"      [OK] Price=${stock_px:.2f}  bid=${bid:.2f}  ask=${ask:.2f}")
            result["price"] = stock_px
        else:
            print("      [WARN] No ticker data (markets closed, frozen data may be unavailable)")

        ib.cancelMktData(stock)

        # ── Step 3: reqSecDefOptParams ──
        print("\n  [3] Fetching option chain parameters...")
        params = ib.reqSecDefOptParams(stock.symbol, "", stock.secType, stock.conId)
        ib.sleep(0.5)

        if not params:
            print("      [FAIL] No option params returned")
            results[ticker] = result
            continue

        chain = max(params, key=lambda p: len(p.strikes))
        n_strikes = len(chain.strikes)
        n_exps = len(chain.expirations)
        print(f"      [OK] {len(params)} chain(s) returned")
        print(f"      [OK] Best chain: exchange={chain.exchange}, tradingClass={chain.tradingClass}")
        print(f"      [OK] {n_strikes} strikes, {n_exps} expirations")
        result["n_strikes"] = n_strikes
        result["n_expirations"] = n_exps

        # Show expiration range
        sorted_exps = sorted(chain.expirations)
        if sorted_exps:
            first_exp = sorted_exps[0]
            last_exp = sorted_exps[-1]
            print(f"      [OK] Expirations: {first_exp} ... {last_exp}")

        # Show strike range
        sorted_strikes = sorted(chain.strikes)
        if sorted_strikes:
            print(f"      [OK] Strikes: ${sorted_strikes[0]:.0f} ... ${sorted_strikes[-1]:.0f}")

        # ── Step 4: ATM strike selection + qualification ──
        print("\n  [4] Selecting ATM strikes...")

        if stock_px <= 0:
            print("      [SKIP] No stock price available, using middle strike")
            stock_px = sorted_strikes[len(sorted_strikes) // 2]

        # ATM strike = closest to spot
        atm_strike = min(chain.strikes, key=lambda s: abs(s - stock_px))
        print(f"      [OK] ATM strike: ${atm_strike:.2f} (spot=${stock_px:.2f})")

        # Nearby strikes (±5%)
        nearby = sorted([s for s in chain.strikes if stock_px * 0.95 < s < stock_px * 1.05])
        print(f"      [OK] Strikes within +/-5%: {len(nearby)}  -> {[f'${s:.0f}' for s in nearby[:8]]}{'...' if len(nearby) > 8 else ''}")

        # Filter expirations 15-120 DTE
        today_dt = datetime(today.year, today.month, today.day)
        valid_exps = []
        for exp_str in chain.expirations:
            try:
                exp_dt = datetime.strptime(exp_str, "%Y%m%d")
            except ValueError:
                continue
            dte = (exp_dt - today_dt).days
            if 15 <= dte <= 120:
                valid_exps.append((exp_str, dte))

        valid_exps.sort(key=lambda x: x[1])
        print(f"      [OK] Expirations 15-120 DTE: {len(valid_exps)}")
        for exp_str, dte in valid_exps[:5]:
            is_monthly = _is_monthly(exp_str)
            tag = " (monthly)" if is_monthly else " (weekly)"
            print(f"          {exp_str}  DTE={dte}{tag}")
        if len(valid_exps) > 5:
            print(f"          ... and {len(valid_exps) - 5} more")

        # Qualify a small set of ATM contracts
        # Try multiple strikes near ATM — some strikes don't exist for weeklies
        if valid_exps:
            test_exp = valid_exps[0][0]  # nearest valid expiration
            # Build candidate strikes: closest first, then nearby integers
            candidates = sorted(nearby, key=lambda s: abs(s - stock_px))[:6]
            if atm_strike not in candidates:
                candidates.insert(0, atm_strike)

            qualified = []
            winning_strike = None
            for try_strike in candidates:
                test_contracts = [
                    Option(ticker, test_exp, try_strike, "C", "SMART", "100", "USD"),
                    Option(ticker, test_exp, try_strike, "P", "SMART", "100", "USD"),
                ]
                try:
                    ib.qualifyContracts(*test_contracts)
                    qualified = [c for c in test_contracts if c.conId > 0]
                    if qualified:
                        winning_strike = try_strike
                        break
                except Exception:
                    continue

            if qualified:
                print(f"      [OK] Qualified {len(qualified)}/{2} ATM contracts for {test_exp} @ ${winning_strike:.2f}")
                for c in qualified:
                    print(f"          {c.right} ${c.strike:.0f} exp={c.lastTradeDateOrContractMonth} conId={c.conId}")
                result["qualified"] = len(qualified)
            else:
                print(f"      [WARN] No strikes qualified for {test_exp} (tried {len(candidates)} candidates)")
                print("             This is normal for far weeklies — monthlies will work")
                result["qualified"] = 0

            # ── Step 5: Market data snapshot ──
            print("\n  [5] Requesting market data (frozen)...")
            for c in qualified:
                ib.reqMktData(c, "", False, False)
            ib.sleep(3)

            for c in qualified:
                tk = ib.ticker(c)
                if tk is None:
                    print(f"      [{c.right}] No ticker data")
                    continue
                bid = float(tk.bid) if tk.bid and tk.bid > 0 and tk.bid != float('inf') else 0
                ask = float(tk.ask) if tk.ask and tk.ask > 0 and tk.ask != float('inf') else 0
                last = float(tk.last) if tk.last and tk.last > 0 and tk.last != float('inf') else 0
                close = float(tk.close) if tk.close and tk.close > 0 and tk.close != float('inf') else 0
                vol = int(tk.volume) if tk.volume and tk.volume > 0 else 0

                # Greeks
                greeks_src = None
                iv = delta = gamma = theta = vega = 0
                for g_attr in ['modelGreeks', 'bidGreeks', 'askGreeks', 'lastGreeks']:
                    g = getattr(tk, g_attr, None)
                    if g and g.impliedVol and g.impliedVol > 0:
                        iv = g.impliedVol
                        delta = g.delta or 0
                        gamma = g.gamma or 0
                        theta = g.theta or 0
                        vega = g.vega or 0
                        greeks_src = g_attr
                        break

                status = "[OK]" if (bid > 0 or ask > 0 or close > 0) else "[WARN: no prices]"
                print(f"      {status} {c.right} ${c.strike:.0f}:")
                print(f"          bid=${bid:.2f}  ask=${ask:.2f}  last=${last:.2f}  close=${close:.2f}  vol={vol}")
                if greeks_src:
                    print(f"          IV={iv:.4f}  delta={delta:.4f}  gamma={gamma:.6f}  theta={theta:.4f}  vega={vega:.4f}  (from {greeks_src})")
                else:
                    print("          [WARN] No greeks available (normal when markets closed)")

            for c in qualified:
                try:
                    ib.cancelMktData(c)
                except Exception:
                    pass
        else:
            print("      [WARN] No valid expirations in 15-120 DTE range")

        results[ticker] = result
        print()

    # ── Summary ──
    print("=" * 60)
    print("SUMMARY")
    print("=" * 60)
    all_ok = True
    for ticker, r in results.items():
        price = r.get("price", 0)
        n_s = r.get("n_strikes", 0)
        n_e = r.get("n_expirations", 0)
        q = r.get("qualified", 0)
        status = "OK" if n_s > 0 and n_e > 0 else "ISSUE"
        if status != "OK":
            all_ok = False
        print(f"  {ticker:6s}  price=${price:>8.2f}  strikes={n_s:>4d}  exps={n_e:>3d}  qualified={q}  [{status}]")

    print()
    if all_ok:
        print("All checks passed. Option chain fetching is ready for tomorrow.")
    else:
        print("Some checks failed — review the output above.")

    ib.disconnect()
    print("\nDisconnected from IBKR.")


def _is_monthly(exp_str: str) -> bool:
    """True if date is a 3rd Friday (standard monthly option expiration)."""
    try:
        exp_dt = datetime.strptime(exp_str, "%Y%m%d")
        return exp_dt.weekday() == 4 and 15 <= exp_dt.day <= 21
    except ValueError:
        return False


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Verify IBKR option chain fetching")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=7497, help="7497=TWS paper, 7496=TWS live, 4002=GW paper")
    parser.add_argument("--tickers", default="AAPL,MSFT,TSLA", help="Comma-separated tickers to test")
    args = parser.parse_args()

    tickers = [t.strip().upper() for t in args.tickers.split(",")]
    run_verification(args.host, args.port, tickers)
