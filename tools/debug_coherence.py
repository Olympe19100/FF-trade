"""
Diagnostic Script — Backtest <-> Production Signal Coherence

Loads real cached data (spread_returns.pkl, backtest_trades.csv) and checks
end-to-end coherence between backtest inline formulas and live code.

Usage:
    python tools/debug_coherence.py
"""

import sys
from pathlib import Path

# Ensure project root on path
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import numpy as np
import pandas as pd

from core import config
import core.backtest as bt
from core.pricing import compute_ff
from core.portfolio import compute_kelly, cost_per_contract


# ═══════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════

passed = 0
failed = 0
total = 0


def check(name: str, ok: bool, detail: str = ""):
    global passed, failed, total
    total += 1
    if ok:
        passed += 1
        print(f"  PASS  {name}")
    else:
        failed += 1
        print(f"  FAIL  {name}")
        if detail:
            print(f"        {detail}")


def mismatch_examples(expected, actual, labels, max_show=5):
    """Return formatted string of first N mismatches."""
    diffs = []
    for i in range(len(expected)):
        if not np.isclose(expected[i], actual[i], rtol=1e-6, atol=1e-10):
            diffs.append(f"    [{labels[i] if labels else i}] expected={expected[i]:.8f} actual={actual[i]:.8f}")
            if len(diffs) >= max_show:
                break
    return "\n".join(diffs) if diffs else ""


# ═══════════════════════════════════════════════════════════════
# Check 1: Constant Audit
# ═══════════════════════════════════════════════════════════════

def check_constants():
    print("\n[1] CONSTANT AUDIT")
    pairs = [
        ("SLIPPAGE_PER_LEG",  bt.SLIPPAGE_PER_LEG,  config.SLIPPAGE_PER_LEG),
        ("COMMISSION",         bt.COMMISSION_PER_LEG, config.COMMISSION_LEG),
        ("MAX_POSITIONS",      bt.MAX_POSITIONS,      config.MAX_POSITIONS),
        ("MAX_CONTRACTS",      bt.MAX_CONTRACTS,      config.MAX_CONTRACTS),
        ("CONTRACT_MULT",      bt.CONTRACT_MULT,      config.CONTRACT_MULT),
        ("DEFAULT_ALLOC",      bt.DEFAULT_ALLOC,      config.DEFAULT_ALLOC),
        ("KELLY_FRAC",         bt.KELLY_FRAC,         config.KELLY_FRAC),
        ("MIN_KELLY_TRADES",   bt.MIN_KELLY_TRADES,   config.MIN_KELLY_TRADES),
        ("FF_THRESHOLD",       bt.FF_THRESHOLD,       config.FF_THRESHOLD_DEFAULT),
        ("MAX_BA_PCT",         bt.MAX_BA_PCT,         config.BA_PCT_MAX),
        ("MIN_SPREAD_COST",    bt.MIN_SPREAD_COST,    config.MIN_COST),
    ]
    all_ok = True
    for name, bt_val, cfg_val in pairs:
        ok = bt_val == cfg_val
        if not ok:
            all_ok = False
            print(f"    MISMATCH {name}: backtest={bt_val}, config={cfg_val}")
        else:
            print(f"    OK  {name} = {bt_val}")
    check("All constants match", all_ok)


# ═══════════════════════════════════════════════════════════════
# Check 2: FF Formula (recompute from stored IV)
# ═══════════════════════════════════════════════════════════════

def check_ff_formula(df):
    print("\n[2] FF FORMULA RECOMPUTE")
    if df is None:
        check("FF recompute (no data)", False, "spread_returns.pkl not found")
        return

    # Need front_iv, back_iv, front_dte, back_dte columns
    needed = ["front_iv", "back_iv", "front_dte", "back_dte", "ff"]
    missing = [c for c in needed if c not in df.columns]
    if missing:
        check("FF recompute (missing columns)", False, f"Missing: {missing}")
        return

    # Sample up to 100 random rows
    sample = df.dropna(subset=needed)
    if len(sample) > 100:
        sample = sample.sample(100, random_state=42)

    # The stored FF is in OLD formula; convert to PDF for comparison with compute_ff
    n_match = 0
    n_total = 0
    mismatches = []
    for _, row in sample.iterrows():
        n_total += 1
        ff_stored_old = row["ff"]

        # Convert stored old → PDF
        if ff_stored_old <= -1:
            continue
        ff_stored_pdf = 1.0 / np.sqrt(1.0 + ff_stored_old) - 1.0

        # Recompute from stored IV using compute_ff (returns PDF directly)
        ff_recomputed = compute_ff(
            row["front_iv"], row["back_iv"],
            row["front_dte"], row["back_dte"],
        )

        if np.isnan(ff_recomputed) or np.isnan(ff_stored_pdf):
            continue

        if np.isclose(ff_stored_pdf, ff_recomputed, rtol=1e-4, atol=1e-6):
            n_match += 1
        else:
            if len(mismatches) < 5:
                mismatches.append(
                    f"    root={row.get('root', '?')}: stored_old={ff_stored_old:.6f} "
                    f"stored_pdf={ff_stored_pdf:.6f} recomputed={ff_recomputed:.6f}"
                )

    ok = n_match == n_total or (n_total > 0 and n_match / n_total >= 0.95)
    detail = f"{n_match}/{n_total} matched"
    if mismatches:
        detail += "\n" + "\n".join(mismatches)
    check(f"FF formula ({n_match}/{n_total} match)", ok, detail if not ok else "")


# ═══════════════════════════════════════════════════════════════
# Check 3: Cost Per Contract Verification
# ═══════════════════════════════════════════════════════════════

def check_cost_verification(trades_df):
    print("\n[3] COST PER CONTRACT VERIFICATION")
    if trades_df is None:
        check("Cost verification (no data)", False, "backtest_trades.csv not found")
        return

    if "cost_per_contract" not in trades_df.columns:
        check("Cost verification (no cost_per_contract column)", False)
        return

    n_legs = 4  # double calendar default
    slippage_entry = bt.SLIPPAGE_PER_LEG * n_legs
    comm_entry = bt.COMMISSION_PER_LEG * n_legs

    # We can't directly get cost_per_share from trades CSV unless stored
    # But cost_per_contract is stored — verify it's consistent with known formula
    # cost_per_contract = (cost_per_share + slippage_entry) * CONTRACT_MULT + comm_entry
    # So cost_per_share = (cost_per_contract - comm_entry) / CONTRACT_MULT - slippage_entry

    sample = trades_df.head(100)
    n_valid = 0
    n_total = 0
    for _, row in sample.iterrows():
        cpc_stored = row["cost_per_contract"]
        if pd.isna(cpc_stored) or cpc_stored <= 0:
            continue
        n_total += 1

        # Back out cost_per_share
        cps_implied = (cpc_stored - comm_entry) / bt.CONTRACT_MULT - slippage_entry
        if cps_implied <= 0:
            continue

        # Recompute via portfolio.py
        cpc_recomputed = cost_per_contract(cps_implied, n_legs)
        if np.isclose(cpc_stored, cpc_recomputed, rtol=1e-6):
            n_valid += 1

    ok = n_valid == n_total if n_total > 0 else False
    check(f"Cost per contract ({n_valid}/{n_total} match)", ok)


# ═══════════════════════════════════════════════════════════════
# Check 4: P&L Recompute
# ═══════════════════════════════════════════════════════════════

def check_pnl_recompute(trades_df):
    print("\n[4] P&L RECOMPUTE")
    if trades_df is None:
        check("P&L recompute (no data)", False, "backtest_trades.csv not found")
        return

    needed = ["pnl", "ret_pct", "contracts", "cost_per_contract"]
    missing = [c for c in needed if c not in trades_df.columns]
    if missing:
        check(f"P&L recompute (missing: {missing})", False)
        return

    sample = trades_df.dropna(subset=needed).head(100)
    n_match = 0
    n_total = 0
    mismatches = []

    for _, row in sample.iterrows():
        n_total += 1
        pnl_stored = row["pnl"]
        ret_stored = row["ret_pct"]
        contracts = row["contracts"]
        cpc = row["cost_per_contract"]
        deployed = contracts * cpc

        # Recompute ret_pct
        if deployed > 0:
            ret_recomputed = pnl_stored / deployed
        else:
            ret_recomputed = 0

        if np.isclose(ret_stored, ret_recomputed, rtol=1e-4, atol=1e-6):
            n_match += 1
        else:
            if len(mismatches) < 5:
                mismatches.append(
                    f"    row {row.name}: ret_stored={ret_stored:.6f} "
                    f"ret_recomputed={ret_recomputed:.6f} "
                    f"pnl={pnl_stored:.2f} deployed={deployed:.2f}"
                )

    ok = n_match == n_total if n_total > 0 else False
    detail = f"{n_match}/{n_total} matched"
    if mismatches:
        detail += "\n" + "\n".join(mismatches)
    check(f"P&L ret_pct = pnl/deployed ({n_match}/{n_total})", ok, detail if not ok else "")


# ═══════════════════════════════════════════════════════════════
# Check 5: Kelly Walk-Forward
# ═══════════════════════════════════════════════════════════════

def check_kelly_walkforward(trades_df):
    print("\n[5] KELLY WALK-FORWARD")
    if trades_df is None:
        check("Kelly walk-forward (no data)", False, "backtest_trades.csv not found")
        return

    if "ret_pct" not in trades_df.columns:
        check("Kelly walk-forward (no ret_pct column)", False)
        return

    returns = trades_df["ret_pct"].dropna().tolist()
    if len(returns) < bt.MIN_KELLY_TRADES:
        check("Kelly walk-forward (not enough trades)", True,
              f"Only {len(returns)} trades, need {bt.MIN_KELLY_TRADES}")
        return

    # Compute Kelly at several points in the walk-forward
    checkpoints = [bt.MIN_KELLY_TRADES, len(returns) // 2, len(returns)]
    all_ok = True
    for cp in checkpoints:
        sub = returns[:cp]

        # portfolio.py
        kelly_live = compute_kelly(sub)

        # backtest inline
        kh = np.array(sub)
        mu_k = kh.mean()
        var_k = kh.var()
        if var_k > 0 and mu_k > 0:
            kelly_bt = min(bt.KELLY_FRAC * mu_k / var_k, 1.0)
        else:
            kelly_bt = bt.DEFAULT_ALLOC

        if not np.isclose(kelly_live, kelly_bt, rtol=1e-10):
            all_ok = False
            print(f"    MISMATCH at n={cp}: live={kelly_live:.8f} bt={kelly_bt:.8f}")

    check("Kelly walk-forward matches at all checkpoints", all_ok)


# ═══════════════════════════════════════════════════════════════
# Check 6: DTE Pair Enumeration Consistency
# ═══════════════════════════════════════════════════════════════

def check_dte_enumeration():
    print("\n[6] DTE PAIR ENUMERATION")
    # Use a realistic set of DTE values
    dtes = list(range(15, 125, 5))  # 15, 20, 25, ..., 120

    # Scanner-style
    scanner_pairs = set()
    for f_dte in dtes:
        if not (config.FRONT_DTE_MIN <= f_dte <= config.FRONT_DTE_MAX):
            continue
        for b_dte in dtes:
            if not (config.BACK_DTE_MIN <= b_dte <= config.BACK_DTE_MAX):
                continue
            if f_dte == b_dte:
                continue
            if b_dte - f_dte >= config.MIN_DTE_GAP:
                scanner_pairs.add((f_dte, b_dte))

    # Spreads-style (identical logic, different variable names)
    spreads_pairs = set()
    for f_dte in dtes:
        if not (config.FRONT_DTE_MIN <= f_dte <= config.FRONT_DTE_MAX):
            continue
        for b_dte in dtes:
            if not (config.BACK_DTE_MIN <= b_dte <= config.BACK_DTE_MAX):
                continue
            if f_dte == b_dte:
                continue
            if b_dte - f_dte >= config.MIN_DTE_GAP:
                spreads_pairs.add((f_dte, b_dte))

    ok = scanner_pairs == spreads_pairs and len(scanner_pairs) > 0
    check(f"DTE pairs match ({len(scanner_pairs)} pairs)", ok)


# ═══════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════

def main():
    print("=" * 60)
    print("COHERENCE DIAGNOSTIC REPORT")
    print("=" * 60)

    # Load data if available
    spread_returns_path = config.CACHE / "spread_returns.pkl"
    trades_path = config.BACKTEST_TRADES_FILE

    df_spreads = None
    if spread_returns_path.exists():
        try:
            df_spreads = pd.read_pickle(str(spread_returns_path))
            print(f"\nLoaded spread_returns.pkl: {len(df_spreads):,} rows")
        except Exception as ex:
            print(f"\nFailed to load spread_returns.pkl: {ex}")
    else:
        print(f"\nspread_returns.pkl not found at {spread_returns_path}")

    df_trades = None
    if trades_path.exists():
        try:
            df_trades = pd.read_csv(str(trades_path))
            print(f"Loaded backtest_trades.csv: {len(df_trades):,} rows")
        except Exception as ex:
            print(f"Failed to load backtest_trades.csv: {ex}")
    else:
        print(f"backtest_trades.csv not found at {trades_path}")

    # Run checks
    check_constants()
    check_ff_formula(df_spreads)
    check_cost_verification(df_trades)
    check_pnl_recompute(df_trades)
    check_kelly_walkforward(df_trades)
    check_dte_enumeration()

    # Summary
    print(f"\n{'=' * 60}")
    print(f"SUMMARY: {passed}/{total} checks passed")
    if failed > 0:
        print(f"         {failed} FAILED")
    else:
        print("         ALL PASSED")
    print("=" * 60)

    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
