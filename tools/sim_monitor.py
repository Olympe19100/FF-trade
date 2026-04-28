"""
Simulation: What if we had entered the top signals from the last 30-50 days?

Loads historical signal files, builds a simulated portfolio of up to 20
positions (still-active only), and prices them via ThetaData/EODHD today.

Usage:
    python tools/sim_monitor.py
"""

import sys
import json
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from core.config import OUTPUT, STATE
sys.stdout.reconfigure(line_buffering=True)
MAX_POS = 20
ACCOUNT_VALUE = 1_023_443
TODAY = datetime.now()
TODAY_STR = TODAY.strftime("%Y-%m-%d")

# ── Step 1: Load all signal files and build simulated portfolio ──

def load_all_signals():
    """Load and merge all signal files, sorted by date then FF."""
    files = sorted(OUTPUT.glob("signals_*.csv"))
    all_dfs = []
    for f in files:
        date_str = f.stem.replace("signals_", "")
        try:
            df = pd.read_csv(str(f))
            df["signal_date"] = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
            all_dfs.append(df)
        except Exception as ex:
            print(f"  Warning: cannot read {f.name}: {ex}")
    if not all_dfs:
        return pd.DataFrame()
    return pd.concat(all_dfs, ignore_index=True)


def build_simulated_portfolio(signals, account_value=None):
    """Simulate entries: walk through signal dates chronologically,
    pick best FF signals (no duplicate tickers), max 20 positions.
    Only keep positions whose front_exp is still in the future.
    Contracts sized via Half Kelly (same as live trading)."""
    from core.trader import (
        compute_kelly, load_trade_history, size_portfolio,
    )

    if account_value is None:
        account_value = ACCOUNT_VALUE

    positions = []
    used_tickers = set()

    # Process signals date by date (chronological)
    for signal_date, grp in signals.groupby("signal_date", sort=True):
        grp = grp.sort_values("ff", ascending=False)

        for _, sig in grp.iterrows():
            if len(positions) >= MAX_POS:
                break
            ticker = sig["ticker"]
            if ticker in used_tickers:
                continue

            front_exp = sig["front_exp"]
            # Skip if front already expired (would have been closed)
            try:
                front_dt = datetime.strptime(front_exp, "%Y-%m-%d")
                if front_dt <= TODAY:
                    continue
            except ValueError:
                continue

            # Use dbl_cost if available, else call_cost
            has_dbl = pd.notna(sig.get("dbl_cost")) and sig["dbl_cost"] > 0
            cost = sig["dbl_cost"] if has_dbl else sig["call_cost"]
            spread_type = "double" if has_dbl else "single"

            # Put strike (newer schema has it, older doesn't)
            put_strike = sig.get("put_strike")
            if pd.isna(put_strike) if isinstance(put_strike, float) else put_strike is None:
                put_strike = sig["strike"]

            n_legs = 4 if spread_type == "double" else 2
            pos = {
                "ticker": ticker,
                "combo": sig["combo"],
                "strike": float(sig["strike"]),
                "put_strike": float(put_strike),
                "spread_type": spread_type,
                "front_exp": front_exp,
                "back_exp": sig["back_exp"],
                "entry_date": signal_date,
                "contracts": 1,  # placeholder, sized below
                "cost_per_share": float(cost),
                "n_legs": n_legs,
                "ff": float(sig["ff"]),
                "stock_px_entry": float(sig["stock_px"]),
            }
            positions.append(pos)
            used_tickers.add(ticker)

    if not positions:
        return positions

    # ── Half Kelly sizing ──
    returns = load_trade_history()
    kelly_f = compute_kelly(returns)

    signals_info = [
        (p["ticker"], p["cost_per_share"], p["n_legs"])
        for p in positions
    ]
    sizing = size_portfolio(signals_info, kelly_f, account_value)

    # Apply sized contracts back to positions
    sizing_map = {ticker: cts for ticker, cts, _ in sizing}
    for pos in positions:
        pos["contracts"] = sizing_map.get(pos["ticker"], 1)

    print(f"  Half Kelly f={kelly_f:.4f} ({kelly_f*100:.2f}%), "
          f"target=${kelly_f * account_value:,.0f}, "
          f"account=${account_value:,.0f}")

    return positions


# ── Step 2: Price each position via EODHD ──

def price_positions(positions):
    """Price positions via ThetaData/EODHD using the autopilot monitor function."""
    from core.autopilot import _price_position

    results = []
    errors = []

    for i, pos in enumerate(positions):
        ticker = pos["ticker"]
        print(f"  [{i+1}/{len(positions)}] Pricing {ticker} "
              f"{pos['combo']} K={pos['strike']:.0f} ...", end="", flush=True)
        try:
            result = _price_position(pos)
            if result:
                result["entry_date"] = pos["entry_date"]
                result["ff"] = pos["ff"]
                result["stock_px_entry"] = pos["stock_px_entry"]
                result["stock_px_now"] = result["stock_px"]
                results.append(result)
                pnl = result["unrealized_pnl"]
                print(f" ${pnl:+.2f} ({result['return_pct']:+.1%})")
            else:
                errors.append(ticker)
                print(" FAILED (no data)")
        except Exception as ex:
            errors.append(ticker)
            print(f" ERROR: {ex}")

    return results, errors


# ── Step 3: Display results ──

def display_results(results, errors, positions):
    """Pretty-print the simulation results."""
    print(f"\n{'='*90}")
    print(f"SIMULATION: MONITOR — Positions prises sur les signaux historiques")
    print(f"Date: {TODAY_STR}")
    print(f"{'='*90}")

    print(f"\nPortefeuille simule: {len(positions)} positions construites, "
          f"{len(results)} pricees, {len(errors)} erreurs")

    if not results:
        print("  Aucune position pricee.")
        return

    total_pnl = sum(r["unrealized_pnl"] for r in results)
    total_invested = sum(r["entry_cost"] * 100 * r["contracts"] for r in results)
    total_current = sum(r["current_cost"] * 100 * r["contracts"] for r in results)

    print(f"\n  {'Ticker':>6s}  {'Combo':>5s}  {'Cts':>3s}  {'Entry$':>7s}  "
          f"{'Now$':>7s}  {'Unrlz P&L':>10s}  {'Ret%':>7s}  {'DTE':>4s}  "
          f"{'FF':>6s}  {'EntryDt':>10s}  {'Stk.E':>7s}  {'Stk.N':>7s}")
    print(f"  {'-'*100}")

    for r in sorted(results, key=lambda x: -x["unrealized_pnl"]):
        dte_str = f"{r['front_dte']}d" if r["front_dte"] >= 0 else "exp"
        ff_str = f"{r['ff']:.1f}%" if r["ff"] < 1 else f"{r['ff']:.0f}%"
        print(f"  {r['ticker']:>6s}  {r['combo']:>5s}  {r['contracts']:>3d}  "
              f"${r['entry_cost']:>5.2f}  ${r['current_cost']:>5.2f}  "
              f"${r['unrealized_pnl']:>+9.2f}  {r['return_pct']:>+6.1%}  "
              f"{dte_str:>4s}  {ff_str:>6s}  {r['entry_date']:>10s}  "
              f"${r['stock_px_entry']:>6.2f}  ${r['stock_px_now']:>6.2f}")

    print(f"  {'-'*100}")
    print(f"  {'TOTAL':>6s}  {'':>5s}  {'':>3s}  {'':>7s}  {'':>7s}  "
          f"${total_pnl:>+9.2f}  {total_pnl/total_invested:>+6.1%}")
    print()
    print(f"  Capital deploye:  ${total_invested:>+10,.2f}")
    print(f"  Valeur actuelle:  ${total_current:>+10,.2f}")
    print(f"  P&L non-realise:  ${total_pnl:>+10,.2f}")

    n_win = sum(1 for r in results if r["unrealized_pnl"] > 0)
    n_loss = sum(1 for r in results if r["unrealized_pnl"] <= 0)
    print(f"  Gagnantes: {n_win} | Perdantes: {n_loss} | "
          f"WR: {n_win/len(results)*100:.0f}%")

    # Kelly info
    try:
        from core.trader import compute_kelly, load_trade_history
        returns = load_trade_history()
        kelly_f = compute_kelly(returns)
        kelly_target = kelly_f * ACCOUNT_VALUE
        print(f"  Half Kelly f={kelly_f:.4f} ({kelly_f*100:.2f}%), "
              f"target=${kelly_target:,.0f}, utilization={total_invested/kelly_target*100:.1f}%")
    except Exception:
        pass

    if errors:
        print(f"\n  Erreurs de pricing: {', '.join(errors)}")

    # Save snapshot
    snapshot = {
        "date": TODAY_STR,
        "simulation": True,
        "positions": results,
        "errors": errors,
        "total_unrealized_pnl": round(total_pnl, 2),
        "total_invested": round(total_invested, 2),
    }
    out_file = STATE / f"sim_monitor_{TODAY.strftime('%Y%m%d')}.json"
    with open(out_file, "w") as f:
        json.dump(snapshot, f, indent=2)
    print(f"\n  Snapshot sauvegarde: {out_file}")


def main():
    print(f"{'='*90}")
    print(f"SIMULATION MONITOR — Quelles positions auraient ete prises ?")
    print(f"{'='*90}")

    # Load signals
    print("\n[1/3] Chargement des signaux historiques...")
    signals = load_all_signals()
    if signals.empty:
        print("  Aucun fichier signal trouve !")
        return

    dates = sorted(signals["signal_date"].unique())
    print(f"  {len(signals)} signaux charges sur {len(dates)} jours: {', '.join(dates)}")

    # Build portfolio
    print(f"\n[2/3] Construction du portefeuille simule (max {MAX_POS} positions)...")
    positions = build_simulated_portfolio(signals)
    print(f"  {len(positions)} positions actives (front_exp > {TODAY_STR})")

    for p in positions:
        ps = f"/{p['put_strike']:.0f}" if p["put_strike"] != p["strike"] else ""
        print(f"    {p['ticker']:>6s} {p['combo']:>5s} K={p['strike']:.0f}{ps} "
              f"${p['cost_per_share']:.2f} FF={p['ff']:.1f}% "
              f"({p['entry_date']}) {p['front_exp']}->{p['back_exp']}")

    if not positions:
        print("  Aucune position active (toutes expiries passees)")
        return

    # Price via ThetaData/EODHD
    print(f"\n[3/3] Pricing via ThetaData ({len(positions)} positions)...")
    results, errors = price_positions(positions)

    # Display
    display_results(results, errors, positions)


if __name__ == "__main__":
    main()
