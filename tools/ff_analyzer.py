"""
FF Analyzer CLI — Detailed Forward Factor analysis per ticker

Fetches option chains via ThetaData (EODHD fallback) and displays
step-by-step FF computation for each DTE combo.

Usage:
    python tools/ff_analyzer.py AAPL           # Single ticker
    python tools/ff_analyzer.py AAPL MSFT OXY  # Multiple tickers
    python tools/ff_analyzer.py --all          # All S&P 500 tickers with signals
"""

import sys
import numpy as np
import pandas as pd
from pathlib import Path
from datetime import datetime

# Allow imports from project root
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from core.scanner import (
    fetch_option_chain_thetadata, fetch_option_chain_eodhd,
    compute_ff, bs_delta_vec,
    implied_vol_vec, get_earnings_dates, has_earnings_between,
    get_sp500_tickers,
    DTE_COMBOS, DTE_TOL, STRIKE_PCT, TARGET_DELTA, MIN_OI_LEG, MIN_MID,
    FF_THRESHOLD_DEFAULT, MIN_COST,
)


def analyze_ticker(ticker, earn_by_root, today):
    """Full FF analysis for one ticker across all DTE combos."""
    # ThetaData first, EODHD fallback
    stock_px, chain = 0, pd.DataFrame()
    try:
        stock_px, chain = fetch_option_chain_thetadata(ticker)
    except Exception:
        pass
    if stock_px <= 0 or chain.empty:
        stock_px, chain = fetch_option_chain_eodhd(ticker)

    if stock_px <= 0 or chain.empty:
        print(f"\n  {ticker}: No data (stock_px={stock_px}, chain empty={chain.empty})")
        return

    # Prepare chain
    chain = chain.copy()
    chain["exp_dt"] = pd.to_datetime(chain["exp_date"], errors="coerce")
    chain["dte"] = (chain["exp_dt"] - pd.Timestamp(today)).dt.days
    chain = chain[
        (chain["bid"] > 0) & (chain["ask"] > 0) &
        (chain["iv"] > 0) &
        (chain["dte"] >= 15) & (chain["dte"] <= 120)
    ].copy()
    chain["mid"] = (chain["bid"] + chain["ask"]) / 2

    if chain.empty:
        print(f"\n  {ticker}: No valid options after filtering")
        return

    has_oi = "open_interest" in chain.columns and chain["open_interest"].max() > 0
    calls = chain[chain["type"] == "call"].copy()
    puts = chain[chain["type"] == "put"].copy()

    for short_dte, long_dte in DTE_COMBOS:
        _analyze_and_print_combo(
            ticker, stock_px, calls, puts,
            short_dte, long_dte, has_oi,
            earn_by_root, today,
        )


def _analyze_and_print_combo(ticker, stock_px, calls, puts,
                              short_dte, long_dte, has_oi, earn_by_root, today):
    """Analyze one DTE combo and print full detail, even if it fails checks."""
    combo = f"{short_dte}-{long_dte}"
    warnings = []   # non-fatal issues
    skip = None     # fatal: stops further analysis

    # ── Front call selection ──
    front = calls[
        (calls["dte"] >= short_dte - DTE_TOL) &
        (calls["dte"] <= short_dte + DTE_TOL) &
        ((calls["strike"] - stock_px).abs() / stock_px <= STRIKE_PCT)
    ].copy()
    if front.empty:
        _print_skip(ticker, combo, stock_px,
                    f"No front calls DTE={short_dte}+/-{DTE_TOL} within {STRIKE_PCT*100:.0f}% of ATM")
        return

    front["dte_diff"] = (front["dte"] - short_dte).abs()
    best_dte = front["dte_diff"].min()
    front_exp_cands = front[front["dte_diff"] == best_dte].copy()

    # Call delta
    f_T = front_exp_cands["dte"].values / 365.0
    f_iv = front_exp_cands["iv"].values
    f_delta = bs_delta_vec(
        np.full(len(front_exp_cands), stock_px),
        front_exp_cands["strike"].values,
        f_T, f_iv,
    )
    front_exp_cands["call_delta"] = f_delta
    front_exp_cands = front_exp_cands.dropna(subset=["call_delta"])
    if front_exp_cands.empty:
        _print_skip(ticker, combo, stock_px, "No valid call deltas for front expiry")
        return

    # Closest to TARGET_DELTA
    front_exp_cands["delta_diff"] = (front_exp_cands["call_delta"] - TARGET_DELTA).abs()
    front_best = front_exp_cands.loc[front_exp_cands["delta_diff"].idxmin()]

    call_strike = front_best["strike"]
    front_iv = front_best["iv"]
    front_mid = front_best["mid"]
    front_dte = int(front_best["dte"])
    front_exp = str(front_best["exp_date"])
    front_oi = int(front_best.get("open_interest", 0)) if has_oi else -1
    call_delta_val = float(front_best["call_delta"])

    # Liquidity (warn but continue)
    if has_oi and front_oi < MIN_OI_LEG:
        warnings.append(f"Front call OI={front_oi} < {MIN_OI_LEG}")
    if front_mid < MIN_MID:
        warnings.append(f"Front call mid=${front_mid:.2f} < ${MIN_MID}")

    # ── Back call selection ──
    back = calls[
        (calls["dte"] >= long_dte - DTE_TOL) &
        (calls["dte"] <= long_dte + DTE_TOL) &
        (calls["exp_date"] != front_exp)
    ].copy()
    if back.empty:
        _print_skip(ticker, combo, stock_px,
                    f"No back calls DTE={long_dte}+/-{DTE_TOL}")
        return

    back_same = back[(back["strike"] - call_strike).abs() < 0.01]
    if back_same.empty:
        back["sdiff"] = (back["strike"] - call_strike).abs()
        back_best = back.loc[back["sdiff"].idxmin()]
        if back_best["sdiff"] / call_strike > 0.02:
            _print_skip(ticker, combo, stock_px,
                        f"No back call at strike K={call_strike:.0f} (nearest {back_best['sdiff']:.1f} away)")
            return
    else:
        back_same = back_same.copy()
        back_same["dte_diff"] = (back_same["dte"] - long_dte).abs()
        back_best = back_same.loc[back_same["dte_diff"].idxmin()]

    back_iv = back_best["iv"]
    back_mid = back_best["mid"]
    back_dte = int(back_best["dte"])
    back_exp = str(back_best["exp_date"])
    back_oi = int(back_best.get("open_interest", 0)) if has_oi else -1

    if has_oi and back_oi < MIN_OI_LEG:
        warnings.append(f"Back call OI={back_oi} < {MIN_OI_LEG}")
    if back_mid < MIN_MID:
        warnings.append(f"Back call mid=${back_mid:.2f} < ${MIN_MID}")

    # Call spread cost
    call_spread = back_mid - front_mid
    if call_spread < MIN_COST:
        warnings.append(f"Call spread ${call_spread:.2f} < min ${MIN_COST}")

    # ── FF computation (step by step) ──
    T1 = front_dte / 365.0
    T2 = back_dte / 365.0
    s1 = front_iv
    s2 = back_iv
    tv1 = s1**2 * T1
    tv2 = s2**2 * T2
    dT = T2 - T1

    fwd_var = None
    fwd_vol = None
    ff = None

    if dT > 0:
        fwd_var = (tv2 - tv1) / dT
        if fwd_var > 0:
            fwd_vol = np.sqrt(fwd_var)
            ff = (s1 - fwd_vol) / fwd_vol
            if ff <= 0:
                warnings.append(f"FF={ff:.4f} <= 0 (contango)")
        else:
            warnings.append(f"Negative fwd_var={fwd_var:.6f}")
    else:
        warnings.append(f"dT={dT:.6f} <= 0")

    # ── Earnings check ──
    today_int = int(today.strftime("%Y%m%d"))
    back_exp_int = int(back_exp.replace("-", "")[:8])
    has_earn = has_earnings_between(ticker, today_int, back_exp_int, earn_by_root)
    if has_earn:
        warnings.append("Earnings between today and back expiry")

    # ── Put leg ──
    put_strike_val = np.nan
    put_delta_val = np.nan
    put_front_mid = np.nan
    put_back_mid = np.nan
    put_spread = np.nan
    put_front_oi = -1
    put_back_oi = -1

    put_front_cands = puts[
        (puts["exp_date"] == front_exp) &
        ((puts["strike"] - stock_px).abs() / stock_px <= STRIKE_PCT)
    ].copy()

    if not put_front_cands.empty:
        pf_T = put_front_cands["dte"].values / 365.0
        pf_S = np.full(len(put_front_cands), stock_px)
        pf_K = put_front_cands["strike"].values
        r_rate = 0.04
        call_equiv = (put_front_cands["mid"].values + pf_S
                      - pf_K * np.exp(-r_rate * pf_T))
        call_equiv = np.maximum(call_equiv, 0.001)

        pf_iv = implied_vol_vec(call_equiv, pf_S, pf_K, pf_T)
        pf_call_delta = bs_delta_vec(pf_S, pf_K, pf_T, pf_iv)
        put_delta_abs = 1.0 - pf_call_delta

        put_front_cands["put_delta_abs"] = put_delta_abs
        put_front_cands = put_front_cands.dropna(subset=["put_delta_abs"])

        if not put_front_cands.empty:
            put_front_cands["delta_diff"] = (
                put_front_cands["put_delta_abs"] - TARGET_DELTA
            ).abs()
            pf_best = put_front_cands.loc[put_front_cands["delta_diff"].idxmin()]
            put_strike_val = float(pf_best["strike"])
            put_delta_val = float(pf_best["put_delta_abs"])
            put_front_mid = float(pf_best["mid"])
            put_front_oi = int(pf_best.get("open_interest", 0)) if has_oi else -1

            # Back put
            put_back_cands = puts[
                (puts["exp_date"] == back_exp) &
                ((puts["strike"] - put_strike_val).abs() / put_strike_val <= 0.02)
            ]
            if not put_back_cands.empty:
                pb_best = put_back_cands.loc[
                    (put_back_cands["strike"] - put_strike_val).abs().idxmin()
                ]
                put_back_mid = float((pb_best["bid"] + pb_best["ask"]) / 2)
                put_back_oi = int(pb_best.get("open_interest", 0)) if has_oi else -1
                ps = put_back_mid - put_front_mid
                if ps > 0:
                    put_spread = ps
                else:
                    warnings.append(f"Put spread negative: ${ps:.2f}")
            else:
                warnings.append("No back put at matching strike")
        else:
            warnings.append("No valid put deltas")
    else:
        warnings.append("No front puts within strike range")

    combined_cost = np.nan
    if not np.isnan(put_spread):
        combined_cost = call_spread + put_spread

    # ── Determine status ──
    is_signal = (
        ff is not None and ff >= FF_THRESHOLD_DEFAULT
        and not has_earn
        and call_spread >= MIN_COST
        and (not has_oi or (front_oi >= MIN_OI_LEG and back_oi >= MIN_OI_LEG))
    )
    tag = "SIGNAL" if is_signal else "---"

    # ── Print ──
    print()
    print("=" * 56)
    print(f"  {ticker} -- {combo} combo | Stock: ${stock_px:.2f}  [{tag}]")
    print("=" * 56)

    print(f"  Front: {front_exp} (DTE={front_dte}) | "
          f"Call K=${call_strike:.0f} | "
          f"Put K={'${:.0f}'.format(put_strike_val) if not np.isnan(put_strike_val) else 'N/A'}")
    print(f"  Back:  {back_exp} (DTE={back_dte})")

    # Inputs
    print()
    print("  Inputs:")
    print(f"    T1 = {front_dte}/365 = {T1:.6f} yr"
          f"     T2 = {back_dte}/365 = {T2:.6f} yr")
    print(f"    s1 = {s1:.4f} (IV={s1*100:.2f}%)"
          f"       s2 = {s2:.4f} (IV={s2*100:.2f}%)")

    # Forward Variance
    print()
    print("  Forward Variance:")
    print(f"    tv1 = s1^2 * T1 = {tv1:.6f}"
          f"    tv2 = s2^2 * T2 = {tv2:.6f}")
    if fwd_var is not None:
        print(f"    fwd_var = (tv2-tv1)/(T2-T1)   = {fwd_var:.6f}")
        if fwd_vol is not None:
            print(f"    fwd_vol = sqrt(fwd_var)       = {fwd_vol:.4f} ({fwd_vol*100:.2f}%)")
        else:
            print(f"    fwd_vol = N/A (negative fwd_var)")
    else:
        print(f"    fwd_var = N/A (dT <= 0)")

    # Forward Factor
    print()
    print("  Forward Factor:")
    if ff is not None and fwd_vol is not None:
        print(f"    FF = (s1 - fwd_vol) / fwd_vol")
        print(f"    FF = ({s1:.4f} - {fwd_vol:.4f}) / {fwd_vol:.4f}")
        passes = ff >= FF_THRESHOLD_DEFAULT
        threshold_mark = ">=" if passes else "<"
        print(f"    FF = {ff:.4f} = {ff*100:.2f}%"
              f"          [{threshold_mark} {FF_THRESHOLD_DEFAULT*100:.0f}% threshold]")
    else:
        print(f"    FF = N/A")

    # Strikes
    print()
    print("  Strikes (35-delta):")
    print(f"    Call: K=${call_strike:.0f}, delta={call_delta_val:.2f}"
          f"       Put: K={'${:.0f}'.format(put_strike_val) if not np.isnan(put_strike_val) else 'N/A'}"
          f", |delta|={'%.2f' % put_delta_val if not np.isnan(put_delta_val) else 'N/A'}")
    print(f"    Call spread: ${call_spread:.2f}/sh"
          f"          Put spread: ${'%.2f' % put_spread if not np.isnan(put_spread) else 'N/A'}/sh")
    dbl = f"${combined_cost:.2f}" if not np.isnan(combined_cost) else "N/A"
    print(f"    Double cal cost: {dbl}/sh")

    # Earnings
    earn_str = "YES -- filtered out" if has_earn else "none"
    print()
    print(f"  Earnings: {earn_str} "
          f"(between today and {back_exp})")

    # Liquidity
    def _oi(v):
        return f"{v:,}" if v >= 0 else "N/A"

    print(f"  Liquidity: Call front OI={_oi(front_oi)} | "
          f"Call back OI={_oi(back_oi)}")
    if not np.isnan(put_strike_val):
        print(f"             Put front OI={_oi(put_front_oi)} | "
              f"Put back OI={_oi(put_back_oi)}")

    # Warnings
    if warnings:
        print()
        print(f"  Issues ({len(warnings)}):")
        for w in warnings:
            print(f"    - {w}")

    print("-" * 56)


def _print_skip(ticker, combo, stock_px, reason):
    """Print a short skip message for combos that can't even be evaluated."""
    print()
    print(f"  {ticker} -- {combo} | ${stock_px:.2f} | SKIP: {reason}")


def main():
    args = [a for a in sys.argv[1:] if not a.startswith("-")]
    flags = [a for a in sys.argv[1:] if a.startswith("-")]

    if "--all" in flags:
        tickers = get_sp500_tickers()
        print(f"Scanning all {len(tickers)} S&P 500 tickers...")
    elif args:
        tickers = [t.upper() for t in args]
    else:
        print("Usage: python tools/ff_analyzer.py AAPL [MSFT] [--all]")
        sys.exit(1)

    today = datetime.now()
    print(f"FF Analyzer -- {today.strftime('%Y-%m-%d %H:%M')}")
    print(f"Tickers: {', '.join(tickers[:10])}{'...' if len(tickers) > 10 else ''}")
    print(f"DTE combos: {DTE_COMBOS}, Tolerance: +/-{DTE_TOL}")
    print(f"Target delta: {TARGET_DELTA}, FF threshold: {FF_THRESHOLD_DEFAULT*100:.0f}%")

    # Load earnings
    earn_by_root = get_earnings_dates(tickers)

    for ticker in tickers:
        analyze_ticker(ticker, earn_by_root, today)

    print(f"\nDone -- {len(tickers)} ticker(s) analyzed.")


if __name__ == "__main__":
    main()
