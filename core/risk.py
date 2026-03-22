"""
Risk & Analytics Module — Institutional-Grade Risk Metrics

Monte Carlo simulation, VaR/CVaR, edge persistence, distribution analysis.
All computed server-side from backtest + live trade returns.

Charts generated with matplotlib (dark theme matching the web app).
"""

import hashlib
import pickle
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from pathlib import Path
from scipy import stats as sp_stats

from core.trader import load_trade_history

ROOT   = Path(r"C:\Users\ANTEC MSI\Desktop\pro\Option trading")
OUTPUT = ROOT / "output"
CACHE  = ROOT / "cache"
CACHE.mkdir(exist_ok=True)

TRADES_PER_YEAR = 35  # ~347 trades over 10 years

# ── Dark theme colors (match app CSS) ──
DARK_BG     = "#0d1117"
DARK_CARD   = "#1c2333"
DARK_BORDER = "#30363d"
TEXT_PRI    = "#e6edf3"
TEXT_SEC    = "#8b949e"
ACCENT      = "#58a6ff"
GREEN       = "#3fb950"
RED         = "#f85149"
YELLOW      = "#d29922"
PURPLE      = "#bc8cff"


def _dark_style():
    """Apply dark theme to matplotlib figure."""
    plt.rcParams.update({
        "figure.facecolor": DARK_BG,
        "axes.facecolor": DARK_CARD,
        "axes.edgecolor": DARK_BORDER,
        "axes.labelcolor": TEXT_SEC,
        "xtick.color": TEXT_SEC,
        "ytick.color": TEXT_SEC,
        "text.color": TEXT_PRI,
        "grid.color": DARK_BORDER,
        "grid.alpha": 0.5,
        "font.size": 11,
        "legend.facecolor": DARK_CARD,
        "legend.edgecolor": DARK_BORDER,
    })


# ═══════════════════════════════════════════════════════════
#  DATA
# ═══════════════════════════════════════════════════════════

def load_returns():
    """Load all trade returns (backtest + live) as numpy array."""
    returns = load_trade_history()
    if not returns:
        return np.array([])
    return np.array(returns, dtype=np.float64)


# ═══════════════════════════════════════════════════════════
#  MONTE CARLO
# ═══════════════════════════════════════════════════════════

def monte_carlo(returns, n_sims=10000, n_trades=100, initial=100_000,
                kelly_f=0.041, n_positions=20):
    """Bootstrap Monte Carlo simulation with Kelly-sized positions.

    Each trade return is a raw spread return (can be -100% to +3000%).
    The portfolio impact per trade = (kelly_f / n_positions) * trade_return,
    because only kelly_f fraction of capital is in spreads, split across
    n_positions positions.

    For each sim: sample n_trades with replacement, compound at Kelly scale.
    """
    rng = np.random.default_rng(42)
    n = len(returns)

    # Per-trade portfolio allocation fraction
    pos_alloc = kelly_f / n_positions

    # Generate all random indices at once: (n_sims, n_trades)
    idx = rng.integers(0, n, size=(n_sims, n_trades))
    sampled = returns[idx]  # (n_sims, n_trades)

    # Compound equity paths — Kelly-scaled
    # Each trade moves portfolio by pos_alloc * trade_return
    growth = 1.0 + pos_alloc * sampled  # (n_sims, n_trades)
    cum = np.cumprod(growth, axis=1)  # (n_sims, n_trades)

    # Prepend initial
    ones = np.ones((n_sims, 1))
    paths = np.hstack([ones, cum]) * initial  # (n_sims, n_trades+1)

    # Terminal wealth
    terminal = paths[:, -1]

    # Percentile paths (for fan chart)
    pctls = {}
    for p in [5, 25, 50, 75, 95]:
        pctls[p] = np.percentile(paths, p, axis=0).tolist()

    # Drawdown per simulation
    running_max = np.maximum.accumulate(paths, axis=1)
    drawdowns = (paths - running_max) / running_max
    max_dd_per_sim = drawdowns.min(axis=1)

    # Annualize: n_trades / TRADES_PER_YEAR years
    years = n_trades / TRADES_PER_YEAR
    cagr_per_sim = (terminal / initial) ** (1 / years) - 1

    return {
        "terminal_median": float(np.median(terminal)),
        "terminal_mean": float(np.mean(terminal)),
        "terminal_p5": float(np.percentile(terminal, 5)),
        "terminal_p25": float(np.percentile(terminal, 25)),
        "terminal_p75": float(np.percentile(terminal, 75)),
        "terminal_p95": float(np.percentile(terminal, 95)),
        "prob_profit": float((terminal > initial).mean()),
        "prob_double": float((terminal > 2 * initial).mean()),
        "prob_loss_20": float((terminal < initial * 0.8).mean()),
        "median_cagr": float(np.median(cagr_per_sim)),
        "mean_cagr": float(np.mean(cagr_per_sim)),
        "p5_cagr": float(np.percentile(cagr_per_sim, 5)),
        "p95_cagr": float(np.percentile(cagr_per_sim, 95)),
        "max_dd_median": float(np.median(max_dd_per_sim)),
        "max_dd_p5": float(np.percentile(max_dd_per_sim, 5)),
        "max_dd_p95": float(np.percentile(max_dd_per_sim, 95)),
        "percentile_paths": pctls,
        "n_sims": n_sims,
        "n_trades": n_trades,
        "initial": initial,
        # For terminal histogram chart — subsample for efficiency
        "_terminal_wealth": terminal,  # kept for chart gen, stripped before JSON
    }


# ═══════════════════════════════════════════════════════════
#  RISK METRICS (VaR / CVaR)
# ═══════════════════════════════════════════════════════════

def risk_metrics(returns, kelly_f=0.041, n_positions=20, account_value=100_000):
    """Compute VaR, CVaR at trade level and portfolio level."""
    var_95 = float(np.percentile(returns, 5))
    var_99 = float(np.percentile(returns, 1))
    cvar_95 = float(returns[returns <= np.percentile(returns, 5)].mean())
    cvar_99 = float(returns[returns <= np.percentile(returns, 1)].mean())

    # Position-level dollar VaR
    pos_alloc = kelly_f * account_value / n_positions

    # Portfolio-level VaR (fraction of total account)
    port_frac = kelly_f / n_positions  # ~0.2% of portfolio per position

    return {
        "var_95": round(var_95, 4),
        "var_99": round(var_99, 4),
        "cvar_95": round(cvar_95, 4),
        "cvar_99": round(cvar_99, 4),
        "var_95_dollar": round(var_95 * pos_alloc, 2),
        "var_99_dollar": round(var_99 * pos_alloc, 2),
        "cvar_95_dollar": round(cvar_95 * pos_alloc, 2),
        "cvar_99_dollar": round(cvar_99 * pos_alloc, 2),
        "var_95_portfolio": round(var_95 * port_frac, 6),
        "var_99_portfolio": round(var_99 * port_frac, 6),
        "worst_trade": round(float(returns.min()), 4),
        "best_trade": round(float(returns.max()), 4),
        "pos_alloc": round(pos_alloc, 2),
    }


# ═══════════════════════════════════════════════════════════
#  EDGE PERSISTENCE
# ═══════════════════════════════════════════════════════════

def edge_persistence(returns, window=50, n_perms=10000):
    """Rolling metrics + permutation test to detect edge decay."""
    n = len(returns)
    obs_mean = float(returns.mean())

    # Rolling metrics (expanding for Kelly, rolling window for Sharpe/WR)
    rolling_kelly = []
    rolling_sharpe = []
    rolling_winrate = []
    rolling_mean = []

    for i in range(window, n + 1):
        chunk = returns[:i]
        mu = chunk.mean()
        var = chunk.var()
        if var > 0 and mu > 0:
            rolling_kelly.append(round(float(0.5 * mu / var), 6))
        else:
            rolling_kelly.append(0.0)

    for i in range(window, n + 1):
        chunk = returns[i - window:i]
        mu = chunk.mean()
        std = chunk.std()
        rolling_sharpe.append(round(float(mu / std * np.sqrt(TRADES_PER_YEAR)) if std > 0 else 0, 4))
        rolling_winrate.append(round(float((chunk > 0).mean()), 4))
        rolling_mean.append(round(float(mu), 6))

    # CUSUM: cumulative sum of (r_i - mean) for regime detection
    cusum = np.cumsum(returns - obs_mean).tolist()

    # Permutation test: is the mean return significantly > 0?
    rng = np.random.default_rng(42)
    perm_means = np.empty(n_perms)
    for i in range(n_perms):
        signs = rng.choice([-1, 1], size=n)
        perm_means[i] = (returns * signs).mean()

    p_value = float((perm_means >= obs_mean).mean())

    return {
        "rolling_kelly": rolling_kelly,
        "rolling_sharpe": rolling_sharpe,
        "rolling_winrate": rolling_winrate,
        "rolling_mean": rolling_mean,
        "cusum": [round(c, 4) for c in cusum],
        "window": window,
        "n_trades": n,
        "permutation_pvalue": round(p_value, 4),
        "observed_mean": round(obs_mean, 6),
        "current_kelly": rolling_kelly[-1] if rolling_kelly else 0,
        "current_sharpe": rolling_sharpe[-1] if rolling_sharpe else 0,
        "current_winrate": rolling_winrate[-1] if rolling_winrate else 0,
    }


# ═══════════════════════════════════════════════════════════
#  DISTRIBUTION ANALYSIS
# ═══════════════════════════════════════════════════════════

def distribution_stats(returns):
    """Return distribution analysis."""
    skew = float(sp_stats.skew(returns))
    kurt = float(sp_stats.kurtosis(returns))  # excess kurtosis
    jb_stat, jb_pval = sp_stats.jarque_bera(returns)

    pctls = {}
    for p in [1, 5, 10, 25, 50, 75, 90, 95, 99]:
        pctls[str(p)] = round(float(np.percentile(returns, p)), 4)

    # Histogram data
    counts, bin_edges = np.histogram(returns, bins=40)
    bin_centers = ((bin_edges[:-1] + bin_edges[1:]) / 2).tolist()

    # QQ data
    osm, osr = sp_stats.probplot(returns, dist="norm")[:2]

    return {
        "mean": round(float(returns.mean()), 6),
        "std": round(float(returns.std()), 6),
        "skewness": round(skew, 4),
        "kurtosis": round(kurt, 4),
        "jarque_bera": round(float(jb_stat), 2),
        "jb_pvalue": round(float(jb_pval), 4),
        "is_normal": float(jb_pval) > 0.05,
        "percentiles": pctls,
        "win_rate": round(float((returns > 0).mean()), 4),
        "n_trades": len(returns),
        "hist_bins": [round(b, 4) for b in bin_centers],
        "hist_counts": counts.tolist(),
        "qq_theoretical": osm[0].tolist(),
        "qq_observed": osm[1].tolist(),
    }


# ═══════════════════════════════════════════════════════════
#  CHART GENERATION
# ═══════════════════════════════════════════════════════════

def generate_charts(mc_result, edge_data, dist_data, returns, output_dir):
    """Generate 4 dark-themed charts as PNG files."""
    _dark_style()
    output_dir = Path(output_dir)

    # ── 1. MC Fan Chart ──
    fig, ax = plt.subplots(figsize=(12, 6))
    pctls = mc_result["percentile_paths"]
    x = list(range(len(pctls[50])))

    ax.fill_between(x, [v/1000 for v in pctls[5]], [v/1000 for v in pctls[95]],
                    alpha=0.15, color=ACCENT, label="5-95%")
    ax.fill_between(x, [v/1000 for v in pctls[25]], [v/1000 for v in pctls[75]],
                    alpha=0.25, color=ACCENT, label="25-75%")
    ax.plot(x, [v/1000 for v in pctls[50]], color=ACCENT, linewidth=2, label="Median")
    ax.axhline(mc_result["initial"]/1000, color=TEXT_SEC, linestyle="--", alpha=0.5, label="Initial")

    ax.set_xlabel("Trade #")
    ax.set_ylabel("Portfolio ($K)")
    ax.set_title(f"Monte Carlo Simulation ({mc_result['n_sims']:,} paths, {mc_result['n_trades']} trades)",
                 color=TEXT_PRI, fontsize=14, fontweight="bold")
    ax.legend(loc="upper left", fontsize=10)
    ax.grid(True)
    fig.tight_layout()
    fig.savefig(str(output_dir / "risk_mc_fan.png"), dpi=150, facecolor=DARK_BG)
    plt.close(fig)

    # ── 2. Terminal Wealth Histogram ──
    fig, ax = plt.subplots(figsize=(12, 5))
    terminal = mc_result.get("_terminal_wealth", None)
    init = mc_result["initial"]

    if terminal is not None:
        ax.hist(terminal / 1000, bins=60, color=ACCENT, alpha=0.7, edgecolor=DARK_BORDER)
    ax.axvline(init / 1000, color=YELLOW, linestyle="--", linewidth=2,
               label=f"Initial ${init/1000:.0f}K")
    ax.axvline(mc_result["terminal_median"] / 1000, color=GREEN, linestyle="-", linewidth=2,
               label=f"Median ${mc_result['terminal_median']/1000:.0f}K")
    ax.axvline(mc_result["terminal_p5"] / 1000, color=RED, linestyle="--", linewidth=2,
               label=f"5th pctl ${mc_result['terminal_p5']/1000:.0f}K")

    ax.set_xlabel("Terminal Wealth ($K)")
    ax.set_ylabel("Frequency")
    ax.set_title(f"Terminal Wealth Distribution ({mc_result['n_sims']:,} simulations)",
                 color=TEXT_PRI, fontsize=14, fontweight="bold")
    ax.legend(loc="upper right", fontsize=10)
    ax.grid(True)
    fig.tight_layout()
    fig.savefig(str(output_dir / "risk_mc_terminal.png"), dpi=150, facecolor=DARK_BG)
    plt.close(fig)

    # ── 3. Edge Persistence (3 subplots) ──
    fig, axes = plt.subplots(3, 1, figsize=(12, 9), sharex=True)
    w = edge_data["window"]
    x_roll = list(range(w, edge_data["n_trades"] + 1))

    # Kelly
    axes[0].plot(x_roll, edge_data["rolling_kelly"], color=ACCENT, linewidth=1.5)
    axes[0].axhline(edge_data["current_kelly"], color=GREEN, linestyle="--", alpha=0.5)
    axes[0].set_ylabel("Half Kelly f")
    axes[0].set_title("Edge Persistence — Rolling Metrics", color=TEXT_PRI, fontsize=14, fontweight="bold")
    axes[0].grid(True)

    # Sharpe
    axes[1].plot(x_roll, edge_data["rolling_sharpe"], color=GREEN, linewidth=1.5)
    axes[1].axhline(0, color=RED, linestyle="--", alpha=0.3)
    axes[1].set_ylabel(f"Sharpe ({w}-trade)")
    axes[1].grid(True)

    # Win rate
    axes[2].plot(x_roll, edge_data["rolling_winrate"], color=YELLOW, linewidth=1.5)
    axes[2].axhline(0.5, color=RED, linestyle="--", alpha=0.3, label="50%")
    axes[2].set_ylabel(f"Win Rate ({w}-trade)")
    axes[2].set_xlabel("Trade #")
    axes[2].grid(True)

    fig.tight_layout()
    fig.savefig(str(output_dir / "risk_edge.png"), dpi=150, facecolor=DARK_BG)
    plt.close(fig)

    # ── 4. Distribution: Histogram + QQ Plot ──
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))

    # Histogram
    ax1.bar(dist_data["hist_bins"], dist_data["hist_counts"],
            width=(dist_data["hist_bins"][1] - dist_data["hist_bins"][0]) * 0.9,
            color=ACCENT, alpha=0.7, edgecolor=DARK_BORDER)
    ax1.axvline(0, color=RED, linestyle="--", alpha=0.5, label="Break-even")
    ax1.axvline(dist_data["mean"], color=GREEN, linestyle="-", linewidth=2,
                label=f"Mean {dist_data['mean']*100:.2f}%")
    ax1.set_xlabel("Return per trade")
    ax1.set_ylabel("Count")
    ax1.set_title("Return Distribution", color=TEXT_PRI, fontsize=13, fontweight="bold")
    ax1.legend(fontsize=9)
    ax1.grid(True)

    # QQ plot
    ax2.scatter(dist_data["qq_theoretical"], dist_data["qq_observed"],
                s=8, color=ACCENT, alpha=0.7)
    qq_min = min(min(dist_data["qq_theoretical"]), min(dist_data["qq_observed"]))
    qq_max = max(max(dist_data["qq_theoretical"]), max(dist_data["qq_observed"]))
    ax2.plot([qq_min, qq_max], [qq_min, qq_max], color=RED, linestyle="--", alpha=0.7, label="Normal")
    ax2.set_xlabel("Theoretical Quantiles")
    ax2.set_ylabel("Observed Quantiles")
    ax2.set_title("QQ Plot (vs Normal)", color=TEXT_PRI, fontsize=13, fontweight="bold")
    ax2.legend(fontsize=9)
    ax2.grid(True)

    fig.tight_layout()
    fig.savefig(str(output_dir / "risk_distribution.png"), dpi=150, facecolor=DARK_BG)
    plt.close(fig)

    return [
        "/output/risk_mc_fan.png",
        "/output/risk_mc_terminal.png",
        "/output/risk_edge.png",
        "/output/risk_distribution.png",
    ]


# ═══════════════════════════════════════════════════════════
#  MAIN ENTRY POINT
# ═══════════════════════════════════════════════════════════

def compute_risk(account_value=100_000):
    """Compute all risk analytics. Returns JSON-serializable dict.

    Uses pickle cache keyed on trade count + account_value.
    """
    returns = load_returns()
    if len(returns) < 10:
        return {"error": "Not enough trades for risk analysis", "n_trades": len(returns)}

    # Cache key: hash of returns + account_value
    cache_key = hashlib.md5(
        returns.tobytes() + str(account_value).encode()
    ).hexdigest()[:12]
    cache_file = CACHE / f"risk_{cache_key}.pkl"

    if cache_file.exists():
        with open(cache_file, "rb") as f:
            return pickle.load(f)

    # Kelly target per position (for dollar VaR)
    mu = returns.mean()
    var = returns.var()
    kelly_f = min(0.5 * mu / var, 1.0) if var > 0 and mu > 0 else 0.04
    kelly_target = kelly_f * account_value
    kelly_per_pos = kelly_target / 20  # MAX_POSITIONS

    # Compute all analytics
    mc = monte_carlo(returns, n_sims=10000, n_trades=100, initial=account_value,
                     kelly_f=kelly_f, n_positions=20)
    risk = risk_metrics(returns, kelly_f=kelly_f, n_positions=20,
                        account_value=account_value)
    edge = edge_persistence(returns, window=50, n_perms=10000)
    dist = distribution_stats(returns)

    # Generate charts (needs _terminal_wealth array)
    charts = generate_charts(mc, edge, dist, returns, OUTPUT)

    # Strip numpy array before JSON serialization
    mc.pop("_terminal_wealth", None)

    result = {
        "monte_carlo": mc,
        "risk_metrics": risk,
        "edge": edge,
        "distribution": dist,
        "charts": charts,
        "account_value": account_value,
        "kelly_f": round(kelly_f, 6),
        "kelly_target": round(kelly_target, 2),
    }

    # Cache result
    with open(cache_file, "wb") as f:
        pickle.dump(result, f)

    return result
