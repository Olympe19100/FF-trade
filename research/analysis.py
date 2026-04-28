"""
Forward Factor Research — Steps 3-6
Aligned with Forward Factors Research PDF methodology.

  Step 3: Return distribution analysis (full universe, no FF filter)
  Step 4: Returns vs Forward Factor (scatter + deciles)
  Step 5: The Model — All Trades (FF >= crossover threshold per combo)
  Step 6: The Model — Filtered (~20 Trades/Month) + Generalized Kelly + Equity Curves

Key methodological alignment with PDF:
  - FF thresholds are per-combo, in OLD formula (fwd_var/front_var - 1)
  - "All Trades" model: FF >= empirical crossover per combo
  - "Filtered" model: FF threshold calibrated to yield ~20 trades/month
    (pure threshold, NOT top-N selection)
  - Generalized Kelly: numerical argmax E[log(1 + f*R)]
  - Equity curves: per-trade compounding with CAGR/Sharpe/Start/End stats

Usage:
    python analysis.py
"""

import sys
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from pathlib import Path
from datetime import datetime as _dt
from scipy.optimize import minimize_scalar

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from core.config import CACHE, OUTPUT
OUT = OUTPUT

# ── FF crossover thresholds (OLD formula: fwd_var/front_var - 1) ──
# These are the empirical crossover points from the PDF
FF_CROSSOVER_OLD = {"30-60": 0.141, "30-90": 0.032, "60-90": 0.415}

# ── Filtered model: FF thresholds calibrated for ~20 trades/month (OLD formula) ──
# From PDF: these yield approximately 20 trades/month per combo
FF_FILTERED_OLD = {"30-60": 0.230, "30-90": 0.230, "60-90": 0.200}

TRADES_PER_MONTH_TARGET = 20
INITIAL_CAPITAL = 100_000


def load_returns():
    """Load spread returns. FF stays in OLD formula (fwd_var/front_var - 1)."""
    df = pd.read_pickle(str(CACHE / "spread_returns.pkl"))
    # FF is already in old formula from spreads.py
    # Keep it in old formula to match PDF methodology
    df = df[np.isfinite(df["ff"])].copy()
    print(f"Loaded {len(df):,} spreads with returns (FF in old formula)")
    return df


def old_to_gui(ff_old):
    """Convert FF from old formula to GUI formula."""
    if ff_old <= -1:
        return np.nan
    return 1.0 / np.sqrt(1.0 + ff_old) - 1.0


def gui_to_old(ff_gui):
    """Convert FF from GUI formula to old formula."""
    ratio = 1.0 + ff_gui
    if ratio <= 0:
        return np.nan
    return 1.0 / (ratio ** 2) - 1.0


# ══════════════════════════════════════════════════════════════
# GENERALIZED KELLY CRITERION
# ══════════════════════════════════════════════════════════════

def generalized_kelly(returns, f_max=1.0, n_points=500):
    """
    Compute Generalized Kelly Criterion via numerical optimization.
    f* = argmax_f E[log(1 + f*R)]

    Returns (f_star, growth_rate, f_array, growth_array) for plotting.
    """
    returns = np.asarray(returns, dtype=float)
    returns = returns[np.isfinite(returns)]

    if len(returns) < 10:
        return 0.0, 0.0, np.array([]), np.array([])

    f_array = np.linspace(0, f_max, n_points)
    growth_array = np.zeros(n_points)

    for i, f in enumerate(f_array):
        log_growth = np.log1p(f * returns)
        # Handle -inf from log(0 or negative)
        log_growth = np.where(np.isfinite(log_growth), log_growth, -100)
        growth_array[i] = np.mean(log_growth)

    # Find f* at the peak
    best_idx = np.argmax(growth_array)
    f_star = f_array[best_idx]
    max_growth = growth_array[best_idx]

    # Refine with scipy
    def neg_growth(f):
        lg = np.log1p(f * returns)
        lg = np.where(np.isfinite(lg), lg, -100)
        return -np.mean(lg)

    try:
        result = minimize_scalar(neg_growth, bounds=(0, f_max), method='bounded')
        if result.success and 0 < result.x < f_max:
            f_star = result.x
            max_growth = -result.fun
    except Exception:
        pass

    return f_star, max_growth, f_array, growth_array


def compute_equity_curve(returns, f, initial=INITIAL_CAPITAL):
    """Compute per-trade compounding equity curve."""
    returns = np.asarray(returns, dtype=float)
    equity = np.zeros(len(returns) + 1)
    equity[0] = initial

    for i, r in enumerate(returns):
        growth = 1.0 + f * r
        if growth > 0:
            equity[i + 1] = equity[i] * growth
        else:
            equity[i + 1] = 0  # bankrupt
            break
    else:
        return equity

    # Fill remaining with 0 if bankrupt
    equity[np.argmax(equity == 0):] = 0
    return equity


# ══════════════════════════════════════════════════════════════
# STEP 3: Return Distribution (full universe, no FF filter)
# ══════════════════════════════════════════════════════════════

def step3_distribution(df, ret_col="ret", label="Call Calendar"):
    print(f"\n{'='*60}")
    print(f"STEP 3: RETURN DISTRIBUTION ({label})")
    print("=" * 60)

    tag = "" if ret_col == "ret" else "_dbl"

    # Compute period
    obs_min = int(df["obs_date"].min())
    obs_max = int(df["obs_date"].max())
    d0 = _dt.strptime(str(obs_min), "%Y%m%d")
    d1 = _dt.strptime(str(obs_max), "%Y%m%d")
    n_years = max((d1 - d0).days / 365.25, 0.1)

    for combo in sorted(df["combo"].unique()):
        sub = df[df["combo"] == combo]
        r = sub[ret_col]
        wins = (r > 0).mean()
        per_year = len(sub) / n_years
        per_month = per_year / 12

        print(f"\n{combo} for Atm {label} Long Return")
        print("-" * 60)
        print(f"count: {len(sub):.6f}")
        print(f"mean : {r.mean():.6f}")
        print(f"std  : {r.std():.6f}")
        print(f"min  : {r.min():.6f}")
        print(f"25%  : {r.quantile(0.25):.6f}")
        print(f"50%  : {r.median():.6f}")
        print(f"75%  : {r.quantile(0.75):.6f}")
        print(f"max  : {r.max():.6f}")
        print(f"win rate:: {wins:.6f}")
        print(f"\nAverages ({n_years:.2f} years total):")
        print(f"per year : {per_year:.2f}")
        print(f"per month: {per_month:.2f}")

        # Tail quantiles
        print(f"\nTail Quantiles:")
        for pct in [0.05, 0.01, 0.001, 0.0001]:
            q = r.quantile(pct)
            cnt = (r <= q).sum()
            print(f" {pct*100:.2f}%  -> quantile = {q:.4f}, "
                  f"count = {cnt}/{len(sub)}")

        fig, ax = plt.subplots(figsize=(8, 5))
        ax.hist(r.clip(-2, 6), bins=80, edgecolor="white", alpha=0.8)
        ax.set_xlabel("Return")
        ax.set_ylabel("Count")
        ax.set_title(f"{combo} for Atm {label} Long Return")
        ax.grid(True, alpha=0.3)
        fig.tight_layout()
        fig.savefig(str(OUT / f"dist{tag}_{combo}.png"), dpi=150)
        plt.close(fig)
        print(f"  -> Saved dist{tag}_{combo}.png")


# ══════════════════════════════════════════════════════════════
# STEP 4: Returns vs Forward Factor
# ══════════════════════════════════════════════════════════════

def step4_ff_analysis(df, ret_col="ret", label="Call Calendar"):
    print(f"\n{'='*60}")
    print(f"STEP 4: RETURNS vs FORWARD FACTOR ({label})")
    print("=" * 60)

    tag = "" if ret_col == "ret" else "_dbl"

    for combo in sorted(df["combo"].unique()):
        sub = df[df["combo"] == combo].copy()

        r_corr = sub["ff"].corr(sub[ret_col])
        fig, ax = plt.subplots(figsize=(8, 6))
        ax.scatter(sub["ff"], sub[ret_col], alpha=0.15, s=5, color="steelblue")
        m, b = np.polyfit(sub["ff"], sub[ret_col], 1)
        ff_range = np.linspace(sub["ff"].min(), sub["ff"].max(), 100)
        ax.plot(ff_range, m * ff_range + b, "b-", linewidth=2,
                label=f"Linear fit (r={r_corr:.2f})")
        ax.legend()
        ax.set_xlabel("Forward Factor")
        ax.set_ylabel(f"Atm {label} Long Return")
        ax.set_title(f"{combo} {label} Return vs Forward Factor")
        ax.grid(True, alpha=0.3)
        fig.tight_layout()
        fig.savefig(str(OUT / f"ff_scatter{tag}_{combo}.png"), dpi=150)
        plt.close(fig)

        print(f"\n{combo}: correlation = {r_corr:.4f}")

        # Decile analysis
        sub["ff_decile"] = pd.qcut(sub["ff"], 10, labels=False, duplicates="drop")
        deciles = sub.groupby("ff_decile").agg(
            count=(ret_col, "size"),
            mean_ret=(ret_col, "mean"),
            median_ret=(ret_col, "median"),
            win_rate=(ret_col, lambda x: (x > 0).mean()),
            mean_ff=("ff", "mean"),
        )
        print(f"  {'Dec':>4} {'N':>6} {'MeanFF':>8} {'MeanRet':>9} "
              f"{'MedRet':>9} {'WinRate':>8}")
        for dec, row in deciles.iterrows():
            print(f"  {dec:>4} {row['count']:>6.0f} {row['mean_ff']:>8.3f} "
                  f"{row['mean_ret']:>9.4f} {row['median_ret']:>9.4f} "
                  f"{row['win_rate']:>8.3f}")

        fig, ax = plt.subplots(figsize=(8, 5))
        ax.bar(deciles.index, deciles["mean_ret"], color="steelblue",
               edgecolor="white")
        ax.axhline(0, color="gray", linestyle="--")
        ax.set_xlabel("Forward Factor Decile")
        ax.set_ylabel("Mean Return")
        ax.set_title(f"{combo} {label} Return vs Forward Factor Deciles")
        ax.grid(True, alpha=0.3, axis="y")
        fig.tight_layout()
        fig.savefig(str(OUT / f"ff_decile{tag}_{combo}.png"), dpi=150)
        plt.close(fig)


# ══════════════════════════════════════════════════════════════
# STEP 5: The Model — All Trades (FF >= crossover, OLD formula)
# ══════════════════════════════════════════════════════════════

def step5_model(df, ret_col="ret", label="Call Calendar"):
    print(f"\n{'='*60}")
    print(f"STEP 5: THE MODEL — ALL TRADES ({label})")
    print("=" * 60)

    tag = "" if ret_col == "ret" else "_dbl"

    obs_min = int(df["obs_date"].min())
    obs_max = int(df["obs_date"].max())
    d0 = _dt.strptime(str(obs_min), "%Y%m%d")
    d1 = _dt.strptime(str(obs_max), "%Y%m%d")
    n_years = max((d1 - d0).days / 365.25, 0.1)

    for combo in sorted(df["combo"].unique()):
        sub = df[df["combo"] == combo].copy()
        threshold = FF_CROSSOVER_OLD.get(combo, 0.0)

        model = sub[sub["ff"] >= threshold].copy()
        baseline = sub.copy()

        per_year = len(model) / n_years
        per_month = per_year / 12

        print(f"\n{combo} for Atm {label} Long Return")
        print("-" * 60)
        print(f"FF   : {threshold:.6f}")
        print(f"count: {len(model):.6f}")
        print(f"mean : {model[ret_col].mean():.6f}")
        print(f"std  : {model[ret_col].std():.6f}")
        print(f"min  : {model[ret_col].min():.6f}")
        print(f"25%  : {model[ret_col].quantile(0.25):.6f}")
        print(f"50%  : {model[ret_col].median():.6f}")
        print(f"75%  : {model[ret_col].quantile(0.75):.6f}")
        print(f"max  : {model[ret_col].max():.6f}")
        print(f"win rate:: {model[ret_col].apply(lambda x: x > 0).mean():.6f}")
        print(f"\nAverages ({n_years:.2f} years total):")
        print(f"per year : {per_year:.2f}")
        print(f"per month: {per_month:.2f}")

        # Tail quantiles
        print(f"\nTail Quantiles:")
        r = model[ret_col]
        for pct in [0.05, 0.01, 0.001, 0.0001]:
            q = r.quantile(pct)
            cnt = (r <= q).sum()
            print(f" {pct*100:.2f}%  -> quantile = {q:.4f}, "
                  f"count = {cnt}/{len(model)}")

        if len(model) == 0:
            continue

        # Histogram
        fig, ax = plt.subplots(figsize=(8, 5))
        ax.hist(model[ret_col].clip(-2, 6), bins=80, edgecolor="white", alpha=0.8)
        ax.set_xlabel("Return")
        ax.set_ylabel("Count")
        ax.set_title(f"{combo} for Atm {label} Long Return")
        ax.grid(True, alpha=0.3)
        fig.tight_layout()
        fig.savefig(str(OUT / f"dist_model{tag}_{combo}.png"), dpi=150)
        plt.close(fig)

        # Returns over time (quarterly)
        model["dt"] = pd.to_datetime(model["obs_date"].astype(str), format="%Y%m%d")
        model["quarter"] = model["dt"].dt.to_period("Q")
        quarterly = model.groupby("quarter")[ret_col].mean()
        q_labels = [str(q) for q in quarterly.index]
        trades_q = model.groupby("quarter")[ret_col].count()

        print(f"\nTrades per quarter: mean={trades_q.mean():.2f}, "
              f"std={trades_q.std():.2f} (quarters={len(trades_q)})")

        fig, ax = plt.subplots(figsize=(12, 5))
        ax.plot(range(len(quarterly)), quarterly.values, "o-",
                color="steelblue", markersize=4)
        ax.axhline(0, color="red", linestyle="--", alpha=0.5)
        step = max(1, len(quarterly) // 10)
        ax.set_xticks(range(0, len(quarterly), step))
        ax.set_xticklabels([q_labels[i] for i in range(0, len(quarterly), step)],
                           rotation=45, ha="right")
        ax.set_xlabel("Quarter")
        ax.set_ylabel(f"Mean Atm {label} Long Return")
        ax.set_title(f"Mean Atm {label} Long Return Over Time (Quarterly) "
                     f"-- {combo}")
        ax.grid(True, alpha=0.3)
        fig.tight_layout()
        fig.savefig(str(OUT / f"returns_quarterly_model{tag}_{combo}.png"), dpi=150)
        plt.close(fig)


# ══════════════════════════════════════════════════════════════
# STEP 6: Filtered Model + Generalized Kelly + Equity Curves
# ══════════════════════════════════════════════════════════════

def calibrate_ff_threshold(df, combo, ret_col, target_per_month=20):
    """
    Find the FF threshold (OLD formula) that yields ~target trades/month.
    Uses binary search on the FF distribution.
    """
    sub = df[df["combo"] == combo].copy()
    if len(sub) == 0:
        return 0.0

    obs_min = int(sub["obs_date"].min())
    obs_max = int(sub["obs_date"].max())
    d0 = _dt.strptime(str(obs_min), "%Y%m%d")
    d1 = _dt.strptime(str(obs_max), "%Y%m%d")
    n_months = max((d1 - d0).days / 30.44, 1)

    # Binary search for threshold
    lo, hi = sub["ff"].min(), sub["ff"].max()
    for _ in range(50):
        mid = (lo + hi) / 2
        count = (sub["ff"] >= mid).sum()
        per_month = count / n_months
        if per_month > target_per_month:
            lo = mid
        else:
            hi = mid

    # Use the threshold that gives closest to target
    threshold = (lo + hi) / 2
    count = (sub["ff"] >= threshold).sum()
    per_month = count / n_months
    return threshold


def step6_filtered(df, ret_col="ret", label="Call Calendar"):
    print(f"\n{'='*60}")
    print(f"STEP 6: FILTERED MODEL (~{TRADES_PER_MONTH_TARGET}/month) — {label}")
    print("=" * 60)

    tag = "" if ret_col == "ret" else "_dbl"

    obs_min = int(df["obs_date"].min())
    obs_max = int(df["obs_date"].max())
    d0 = _dt.strptime(str(obs_min), "%Y%m%d")
    d1 = _dt.strptime(str(obs_max), "%Y%m%d")
    n_years = max((d1 - d0).days / 365.25, 0.1)
    n_quarters = int(n_years * 4)

    for combo in sorted(df["combo"].unique()):
        sub = df[df["combo"] == combo].copy()

        # ── Calibrate FF threshold for ~20/month ──
        # Try PDF threshold first, then auto-calibrate
        ff_pdf = FF_FILTERED_OLD.get(combo, 0.2)
        count_pdf = (sub["ff"] >= ff_pdf).sum()
        per_month_pdf = count_pdf / (n_years * 12)

        # Auto-calibrate to match our data
        ff_cal = calibrate_ff_threshold(
            df, combo, ret_col, TRADES_PER_MONTH_TARGET)

        print(f"\n{combo}: FF threshold calibration")
        print(f"  PDF threshold (old): {ff_pdf:.3f} -> "
              f"{count_pdf:,} trades ({per_month_pdf:.1f}/month)")
        print(f"  Auto-calibrated:     {ff_cal:.3f} -> ", end="")

        # Use auto-calibrated threshold
        threshold = ff_cal
        model = sub[sub["ff"] >= threshold].copy()
        per_year = len(model) / n_years
        per_month = per_year / 12
        print(f"{len(model):,} trades ({per_month:.1f}/month)")

        if len(model) == 0:
            continue

        # ── Return Statistics (matching PDF format) ──
        r = model[ret_col]
        wins = (r > 0).mean()

        print(f"\n{combo} for Atm {label} Long Return")
        print("-" * 60)
        print(f"FF   : {threshold:.6f}")
        print(f"count: {len(model):.6f}")
        print(f"mean : {r.mean():.6f}")
        print(f"std  : {r.std():.6f}")
        print(f"min  : {r.min():.6f}")
        print(f"25%  : {r.quantile(0.25):.6f}")
        print(f"50%  : {r.median():.6f}")
        print(f"75%  : {r.quantile(0.75):.6f}")
        print(f"max  : {r.max():.6f}")
        print(f"win rate:: {wins:.6f}")
        print(f"\nAverages ({n_years:.2f} years total):")
        print(f"per year : {per_year:.2f}")
        print(f"per month: {per_month:.2f}")

        # Tail quantiles
        print(f"\nTail Quantiles:")
        for pct in [0.05, 0.01, 0.001, 0.0001]:
            q = r.quantile(pct)
            cnt = (r <= q).sum()
            print(f" {pct*100:.2f}%  -> quantile = {q:.4f}, "
                  f"count = {cnt}/{len(model)}")

        # ── Histogram ──
        fig, ax = plt.subplots(figsize=(8, 5))
        ax.hist(r.clip(-2, 6), bins=60, edgecolor="white", alpha=0.8)
        ax.set_xlabel("Return")
        ax.set_ylabel("Count")
        ax.set_title(f"{combo} for Atm {label} Long Return")
        ax.grid(True, alpha=0.3)
        fig.tight_layout()
        fig.savefig(str(OUT / f"dist_filtered{tag}_{combo}.png"), dpi=150)
        plt.close(fig)

        # ── Returns over time (quarterly) ──
        model["dt"] = pd.to_datetime(
            model["obs_date"].astype(str), format="%Y%m%d")
        model = model.sort_values("dt")
        model["quarter"] = model["dt"].dt.to_period("Q")
        quarterly_mean = model.groupby("quarter")[ret_col].mean()
        quarterly_count = model.groupby("quarter")[ret_col].count()
        q_labels = [str(q) for q in quarterly_mean.index]

        print(f"\nTrades per quarter: mean={quarterly_count.mean():.2f}, "
              f"std={quarterly_count.std():.2f} "
              f"(quarters={len(quarterly_count)})")

        fig, ax = plt.subplots(figsize=(12, 5))
        ax.plot(range(len(quarterly_mean)), quarterly_mean.values, "o-",
                color="steelblue", markersize=4)
        ax.axhline(0, color="red", linestyle="--", alpha=0.5)
        step = max(1, len(quarterly_mean) // 10)
        ax.set_xticks(range(0, len(quarterly_mean), step))
        ax.set_xticklabels([q_labels[i] for i in range(0, len(q_labels), step)],
                           rotation=45, ha="right")
        ax.set_xlabel("Quarter")
        ax.set_ylabel(f"Mean Atm {label} Long Return")
        ax.set_title(f"Mean Atm {label} Long Return Over Time (Quarterly) "
                     f"-- {combo}")
        ax.grid(True, alpha=0.3)
        fig.tight_layout()
        fig.savefig(str(OUT / f"returns_quarterly_filtered{tag}_{combo}.png"),
                    dpi=150)
        plt.close(fig)

        # ── Generalized Kelly Criterion ──
        rets = model[ret_col].values
        f_star, max_growth, f_arr, g_arr = generalized_kelly(rets)

        print(f"\nKelly Criterion:")
        print(f"  f* = {f_star:.3f} (Generalized Kelly)")
        print(f"  Expected log growth rate: {max_growth:.6f}")

        # Simple Kelly for comparison
        mu = r.mean()
        var = r.var()
        if var > 0:
            simple_kelly = mu / var
            print(f"  f* = {simple_kelly:.3f} (Simple mu/sigma^2)")

        # Kelly Criterion Curve plot (matching PDF)
        if len(f_arr) > 0:
            fig, ax = plt.subplots(figsize=(8, 6))
            ax.plot(f_arr, g_arr, "b-", linewidth=2, label="Kelly Curve")
            ax.axvline(f_star, color="lightblue", linestyle="--",
                       label=f"f* = {f_star:.3f}")
            ax.plot(f_star, max_growth, "ro", markersize=8, zorder=5)
            # Mark zero growth
            ax.axhline(0, color="gray", linestyle="-", alpha=0.3)
            ax.set_xlabel("Bet Fraction")
            ax.set_ylabel("Expected Logarithmic Growth Rate")
            ax.set_title(f"Generalized Kelly Criterion Curve -- {combo} "
                         f"-- Atm {label} Long Return")
            ax.legend()
            ax.grid(True, alpha=0.3)
            fig.tight_layout()
            fig.savefig(str(OUT / f"kelly_curve{tag}_{combo}.png"), dpi=150)
            plt.close(fig)

        # ── Equity Curves (Full, Half, Quarter Kelly) ──
        n_days = (model["dt"].max() - model["dt"].min()).days

        for frac, frac_name in [
            (1.0, "Full Kelly"),
            (0.5, "Half Kelly"),
            (0.25, "Quarter Kelly"),
        ]:
            bet = frac * f_star
            equity = compute_equity_curve(rets, bet, INITIAL_CAPITAL)

            if equity[-1] <= 0:
                print(f"  {frac_name} (bet={bet:.2%}): BANKRUPT")
                continue

            final_val = equity[-1]
            cagr = (final_val / INITIAL_CAPITAL) ** (1 / n_years) - 1

            # Sharpe: compute on per-trade log returns
            trade_rets = np.diff(equity[1:]) / equity[1:-1]
            trade_rets = trade_rets[np.isfinite(trade_rets)]
            if len(trade_rets) > 1 and trade_rets.std() > 0:
                # Annualize: trades_per_year
                trades_py = len(rets) / n_years
                sharpe = (trade_rets.mean() / trade_rets.std()
                          * np.sqrt(trades_py))
            else:
                sharpe = 0

            print(f"  {frac_name} (bet={bet:.2%}): "
                  f"CAGR={cagr:.2%}, Sharpe={sharpe:.2f}, "
                  f"${INITIAL_CAPITAL:,.0f} -> ${final_val:,.2f}")

            # Plot equity curve with stats box (matching PDF)
            fig, ax = plt.subplots(figsize=(10, 6))
            # Convert trade indices to dates
            dates = model["dt"].values
            ax.plot(dates, equity[1:], "b-", linewidth=1)
            ax.set_xlabel("Date")
            ax.set_ylabel("Equity")
            ax.set_title(f"Equity Curve -- {combo} -- bet "
                         f"({frac_name})={bet*100:.0f}%")
            ax.grid(True, alpha=0.3)

            # Stats box (matching PDF format)
            stats_text = (f"CAGR: {cagr:.2%}\n"
                          f"Sharpe: {sharpe:.2f}\n"
                          f"Start: {INITIAL_CAPITAL:,.2f}\n"
                          f"End: {final_val:,.2f}\n"
                          f"Days: {n_days}\n"
                          f"Bet: {bet*100:.2f}%")
            ax.text(0.02, 0.98, stats_text, transform=ax.transAxes,
                    fontsize=10, verticalalignment='top',
                    bbox=dict(boxstyle='round', facecolor='lightgray',
                              alpha=0.8), family='monospace')

            fig.tight_layout()
            fname = frac_name.lower().replace(" ", "_")
            fig.savefig(str(OUT / f"equity_{fname}{tag}_{combo}.png"),
                        dpi=150)
            plt.close(fig)


# ══════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    df = load_returns()

    # ── Single calendar (call only) ──
    print("\n" + "#" * 60)
    print("# LONG CALL CALENDAR")
    print("#" * 60)
    step3_distribution(df, ret_col="ret", label="Call Calendar")
    step4_ff_analysis(df, ret_col="ret", label="Call Calendar")
    step5_model(df, ret_col="ret", label="Call Calendar")
    step6_filtered(df, ret_col="ret", label="Call Calendar")

    # ── Double calendar (call + put) ──
    if "double_ret" in df.columns:
        dbl = df.dropna(subset=["double_ret"]).copy()
        if len(dbl) > 0:
            print("\n" + "#" * 60)
            print("# LONG DOUBLE CALENDAR")
            print("#" * 60)
            step3_distribution(dbl, ret_col="double_ret",
                               label="Double Calendar")
            step4_ff_analysis(dbl, ret_col="double_ret",
                              label="Double Calendar")
            step5_model(dbl, ret_col="double_ret",
                        label="Double Calendar")
            step6_filtered(dbl, ret_col="double_ret",
                           label="Double Calendar")

    print("\n" + "=" * 60)
    print("ANALYSIS COMPLETE")
    print("=" * 60)
