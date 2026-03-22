"""
Portfolio Backtest — Forward Factor Double Calendar Spreads

Single combined portfolio:
  - $100K starting capital
  - Max 20 concurrent positions (top FF across all combos)
  - Half Kelly (f/2) position sizing
  - Slippage + commissions
  - FF >= 20% entry signal (GUI formula)
  - Close as spread 1 day before front expiry (ex-earn filtered)

Usage:
    python backtest.py
"""

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from pathlib import Path

ROOT  = Path(r"C:\Users\ANTEC MSI\Desktop\pro\Option trading")
CACHE = ROOT / "cache"
OUT   = ROOT / "output"
OUT.mkdir(exist_ok=True)

# ── Parameters ──
INITIAL_CAPITAL  = 100_000
MAX_POSITIONS    = 20
KELLY_FRAC       = 0.5          # Half Kelly (f/2)
# FF thresholds in OLD formula (fwd_var/front_var - 1), from PDF filtered model
FF_THRESHOLD_OLD = {"30-60": 0.230, "30-90": 0.230, "60-90": 0.200}
FF_THRESHOLD_DEFAULT = 0.230    # Fallback for unknown combos
MIN_SPREAD_COST  = 1.00         # $1.00/share minimum
# Per-leg costs (ORATS methodology: ~66% of quoted spread for 2-leg, ~53% for 4-leg)
# Muravyev & Pearson (2020): timed execution ~1-2c/leg on liquid names
# Conservative: $0.05/leg covers ~50-75% of typical ATM bid-ask on S&P 500 names
SLIPPAGE_PER_LEG = 0.03         # $0.03/leg/share (Muravyev & Pearson 2020: effective spread ~40% of quoted)
COMMISSION_PER_LEG = 0.65       # $0.65/leg/contract (IBKR US options)
CONTRACT_MULT    = 100          # 1 option contract = 100 shares
MAX_CONTRACTS    = 10           # Max contracts per position
DEFAULT_ALLOC    = 0.04         # 4% per name default (before Kelly kicks in)
MIN_KELLY_TRADES = 50           # Need 50 trades before using Kelly


def load_data():
    """Load spread returns. FF stays in OLD formula (aligned with PDF)."""
    df = pd.read_pickle(str(CACHE / "spread_returns.pkl"))

    # Keep FF in old formula (fwd_var/front_var - 1) to match PDF methodology
    df = df[np.isfinite(df["ff"])].copy()

    # Parse dates
    df["entry_dt"] = pd.to_datetime(df["obs_date"].astype(str), format="%Y%m%d")
    # Exit = 1 day before front expiry (already computed in returns.py as exit_date)
    df["exit_dt"] = pd.to_datetime(df["exit_date"].astype(str), format="%Y%m%d")

    print(f"Loaded {len(df):,} trades ({df['entry_dt'].min().date()} to "
          f"{df['entry_dt'].max().date()})")
    return df


def run_portfolio(df, mode="double"):
    """
    Run single combined portfolio across all combos.
    mode: "double" (call+put) or "single" (call only)
    """
    if mode == "double":
        sub = df.dropna(subset=["double_ret", "combined_cost"]).copy()
        sub = sub[sub["combined_cost"] > 0]
        ret_col = "double_ret"
        cost_col = "combined_cost"
        n_legs = 4                # call front + call back + put front + put back
        label = "Double Calendar"
    else:
        sub = df.copy()
        ret_col = "ret"
        cost_col = "spread_cost"
        n_legs = 2                # call front + call back
        label = "Single Calendar (Call)"

    # Per-trade slippage & commission scaled by number of legs
    slippage_entry = SLIPPAGE_PER_LEG * n_legs    # $0.10 (2-leg) or $0.20 (4-leg)
    slippage_exit  = SLIPPAGE_PER_LEG * n_legs
    comm_entry     = COMMISSION_PER_LEG * n_legs   # $1.30 (2-leg) or $2.60 (4-leg)
    comm_exit      = COMMISSION_PER_LEG * n_legs

    # Filters
    sub = sub[sub[cost_col] >= MIN_SPREAD_COST].copy()
    # Apply per-combo FF threshold (OLD formula, from PDF filtered model)
    ff_mask = pd.Series(False, index=sub.index)
    for combo_name, thresh in FF_THRESHOLD_OLD.items():
        ff_mask |= (sub["combo"] == combo_name) & (sub["ff"] >= thresh)
    # Fallback for unknown combos
    known = set(FF_THRESHOLD_OLD.keys())
    ff_mask |= (~sub["combo"].isin(known)) & (sub["ff"] >= FF_THRESHOLD_DEFAULT)
    sub = sub[ff_mask].copy()

    if len(sub) == 0:
        print(f"  No trades after filters for {label}")
        return None

    # Stats per combo
    for combo in sorted(sub["combo"].unique()):
        c = sub[sub["combo"] == combo]
        print(f"  {combo}: {len(c):,} trades, "
              f"mean ret={c[ret_col].mean():.3f}, wr={(c[ret_col]>0).mean():.1%}")
    print(f"  TOTAL: {len(sub):,} trades")

    # Build date-indexed lookup (all combos together)
    trades_by_entry = {d: g for d, g in sub.groupby("entry_dt")}

    # ── Day-by-day simulation ──
    cash = INITIAL_CAPITAL
    positions = []
    daily_log = []
    trade_log = []
    kelly_history = []

    # Continuous business day calendar
    all_event_dates = sorted(
        set(sub["entry_dt"].unique()) | set(sub["exit_dt"].unique()))
    d0 = all_event_dates[0]
    d1 = all_event_dates[-1]
    all_dates = pd.bdate_range(d0, d1).tolist()

    for d in all_dates:
        # ─── 1. CLOSE expired positions ───
        still_open = []
        for pos in positions:
            if pos["exit_dt"] <= d:
                n_cts = pos["contracts"]
                # True P&L = (mid_exit - mid_entry - slip_entry - slip_exit) * shares
                #            - (comm_entry + comm_exit) * contracts
                pnl_per_share = (pos["exit_val_per_share"] - pos["cost_per_share"]
                                 - slippage_entry - slippage_exit)
                pnl = (pnl_per_share * n_cts * CONTRACT_MULT
                       - n_cts * (comm_entry + comm_exit))

                deployed = pos["deployed"]
                cash += deployed + pnl

                ret_pct = pnl / deployed if deployed > 0 else 0
                trade_log.append({
                    "entry_date": pos["entry_dt"],
                    "exit_date": pos["exit_dt"],
                    "ticker": pos["ticker"],
                    "combo": pos["combo"],
                    "ff": pos["ff"],
                    "contracts": n_cts,
                    "cost_per_contract": deployed / n_cts,
                    "pnl": pnl,
                    "ret_pct": ret_pct,
                })
                kelly_history.append(ret_pct)
            else:
                still_open.append(pos)
        positions = still_open

        # ─── 2. ENTER new positions (top FF, fill up to 20) ───
        slots = MAX_POSITIONS - len(positions)

        if slots > 0 and d in trades_by_entry:
            candidates = trades_by_entry[d]
            # Rank by FF descending — top 20 across all combos
            candidates = candidates.nlargest(slots, "ff")

            account_value = cash + sum(p["deployed"] for p in positions)

            # Walk-forward Kelly
            if len(kelly_history) >= MIN_KELLY_TRADES:
                kh = np.array(kelly_history)
                mu_k = kh.mean()
                var_k = kh.var()
                if var_k > 0 and mu_k > 0:
                    kelly_f = min(KELLY_FRAC * mu_k / var_k, 1.0)
                else:
                    kelly_f = DEFAULT_ALLOC
            else:
                kelly_f = DEFAULT_ALLOC

            # Two-pass sizing: guarantee all positions, then add Kelly extras
            slots = MAX_POSITIONS - len(positions)
            n_candidates = min(len(candidates), slots)
            kelly_budget = kelly_f * account_value

            # Pre-compute cost per contract for each candidate
            cand_list = []
            for _, trade in candidates.head(n_candidates).iterrows():
                cpc = ((trade[cost_col] + slippage_entry) * CONTRACT_MULT
                       + comm_entry)
                if cpc > 0:
                    cand_list.append((trade, cpc))

            # Pass 1: reserve 1 contract per position (minimum)
            min_total = sum(cpc for _, cpc in cand_list)
            # Pass 2: distribute remaining Kelly budget as extra contracts
            extra_budget = max(0, kelly_budget - min_total)

            for trade, cost_per_contract in cand_list:
                if len(positions) >= MAX_POSITIONS:
                    break

                # 1 base contract + extras from Kelly budget
                extra_cts = int(extra_budget / len(cand_list) / cost_per_contract)
                contracts = 1 + max(0, min(extra_cts, MAX_CONTRACTS - 1))

                total_deployed = contracts * cost_per_contract

                if total_deployed > cash:
                    contracts = max(1, int(cash / cost_per_contract))
                    if contracts < 1:
                        continue
                    total_deployed = contracts * cost_per_contract

                cash -= total_deployed

                # Exit value per share from stored return
                exit_val_per_share = trade[cost_col] * (1 + trade[ret_col])

                positions.append({
                    "ticker": trade["root"],
                    "combo": trade["combo"],
                    "entry_dt": d,
                    "exit_dt": trade["exit_dt"],
                    "contracts": contracts,
                    "cost_per_share": trade[cost_col],
                    "exit_val_per_share": exit_val_per_share,
                    "deployed": total_deployed,
                    "ff": trade["ff"],
                })

        # ─── 3. RECORD daily state ───
        invested = sum(p["deployed"] for p in positions)
        account_value = cash + invested
        daily_log.append({
            "date": d,
            "account": account_value,
            "cash": cash,
            "invested": invested,
            "n_positions": len(positions),
        })

    daily = pd.DataFrame(daily_log)
    trades = pd.DataFrame(trade_log) if trade_log else pd.DataFrame()

    return {
        "daily": daily,
        "trades": trades,
        "label": label,
        "mode": mode,
    }


def print_stats(result):
    """Print performance statistics."""
    daily = result["daily"]
    trades = result["trades"]
    label = result["label"]

    print(f"\n{'='*60}")
    print(f"PORTFOLIO: {label}")
    print(f"${INITIAL_CAPITAL:,} | {MAX_POSITIONS} max positions | "
          f"Half Kelly | slippage+commission")
    print("=" * 60)

    if trades.empty:
        print("  No trades executed")
        return

    # Period
    d0, d1 = daily["date"].min(), daily["date"].max()
    n_years = max((d1 - d0).days / 365.25, 0.1)

    # Account
    final = daily["account"].iloc[-1]
    cagr = (final / INITIAL_CAPITAL) ** (1 / n_years) - 1
    print(f"  Period: {d0.date()} -> {d1.date()} ({n_years:.1f}y)")
    print(f"  Account: ${INITIAL_CAPITAL:,.0f} -> ${final:,.0f} "
          f"(CAGR={cagr:.1%})")

    # Sharpe
    daily_ret = daily["account"].pct_change().dropna()
    n_periods_per_year = len(daily_ret) / n_years
    sharpe = 0
    if len(daily_ret) > 20 and daily_ret.std() > 0:
        sharpe = daily_ret.mean() / daily_ret.std() * np.sqrt(n_periods_per_year)
        print(f"  Sharpe (daily, ann): {sharpe:.2f}")

    # Max Drawdown
    peak = daily["account"].cummax()
    dd = (daily["account"] - peak) / peak
    max_dd = dd.min()
    print(f"  Max Drawdown: {max_dd:.1%}")

    # Trade stats
    n_trades = len(trades)
    wins = (trades["pnl"] > 0).sum()
    win_rate = wins / n_trades if n_trades > 0 else 0
    print(f"\n  Trades: {n_trades:,} ({n_trades/n_years:.0f}/year)")
    print(f"  Win rate: {win_rate:.1%}")
    print(f"  Avg P&L/trade: ${trades['pnl'].mean():,.0f}")
    print(f"  Median P&L/trade: ${trades['pnl'].median():,.0f}")
    print(f"  Avg contracts: {trades['contracts'].mean():.1f}")
    print(f"  Total P&L: ${trades['pnl'].sum():,.0f}")

    # P&L distribution
    print(f"\n  P&L distribution:")
    for pct in [0.05, 0.25, 0.50, 0.75, 0.95]:
        print(f"    {pct*100:.0f}%: ${trades['pnl'].quantile(pct):,.0f}")

    # Positions
    print(f"\n  Positions: mean={daily['n_positions'].mean():.1f}, "
          f"max={daily['n_positions'].max()}")

    # Combo breakdown
    if "combo" in trades.columns:
        print(f"\n  By combo:")
        for combo, grp in trades.groupby("combo"):
            wr = (grp["pnl"] > 0).mean()
            print(f"    {combo}: {len(grp):4d} trades, "
                  f"P&L=${grp['pnl'].sum():>10,.0f}, wr={wr:.0%}")

    # Monthly returns
    daily_cp = daily.copy()
    daily_cp["month"] = daily_cp["date"].dt.to_period("M")
    monthly = daily_cp.groupby("month")["account"].last()
    monthly_ret = monthly.pct_change().dropna()
    if len(monthly_ret) > 0:
        print(f"\n  Monthly returns:")
        print(f"    mean={monthly_ret.mean():.2%}, "
              f"std={monthly_ret.std():.2%}")
        print(f"    worst={monthly_ret.min():.2%}, "
              f"best={monthly_ret.max():.2%}")
        n_pos = (monthly_ret > 0).sum()
        print(f"    positive months: {n_pos}/{len(monthly_ret)} "
              f"({n_pos/len(monthly_ret):.0%})")

    # Year-by-year
    trades_cp = trades.copy()
    trades_cp["exit_year"] = pd.to_datetime(trades_cp["exit_date"]).dt.year
    print(f"\n  Year-by-year:")
    for year, grp in trades_cp.groupby("exit_year"):
        wr = (grp["pnl"] > 0).mean()
        print(f"    {year}: {len(grp):4d} trades, "
              f"P&L=${grp['pnl'].sum():>10,.0f}, "
              f"avg=${grp['pnl'].mean():>7,.0f}, "
              f"wr={wr:.0%}")


def plot_results(result):
    """Generate backtest charts."""
    daily = result["daily"]
    trades = result["trades"]
    label = result["label"]
    tag = result["mode"]

    if trades.empty:
        return

    # 1. Equity Curve + Drawdown
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 8), height_ratios=[3, 1],
                                    sharex=True)
    ax1.plot(daily["date"], daily["account"] / 1000, color="C0", linewidth=1.5)
    ax1.axhline(INITIAL_CAPITAL / 1000, color="gray", linestyle="--", alpha=0.5)
    ax1.set_ylabel("Account ($K)")
    ax1.set_title(f"Portfolio: {label}\n"
                  f"${INITIAL_CAPITAL/1000:.0f}K start | {MAX_POSITIONS} max pos | "
                  f"Half Kelly | slippage + commissions")
    ax1.grid(True, alpha=0.3)

    peak = daily["account"].cummax()
    dd = (daily["account"] - peak) / peak * 100
    ax2.fill_between(daily["date"], dd, 0, color="red", alpha=0.3)
    ax2.set_ylabel("Drawdown (%)")
    ax2.set_xlabel("Date")
    ax2.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(str(OUT / f"bt_portfolio_{tag}.png"), dpi=150)
    plt.close(fig)

    # 2. Position count
    fig, ax = plt.subplots(figsize=(14, 4))
    ax.plot(daily["date"], daily["n_positions"], color="C1", alpha=0.8)
    ax.axhline(MAX_POSITIONS, color="red", linestyle="--", alpha=0.5,
               label=f"Max = {MAX_POSITIONS}")
    ax.set_ylabel("Active Positions")
    ax.set_title(f"Portfolio: {label} — Position Count")
    ax.legend()
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(str(OUT / f"bt_positions_{tag}.png"), dpi=150)
    plt.close(fig)

    # 3. Monthly P&L bars
    trades_cp = trades.copy()
    trades_cp["exit_month"] = pd.to_datetime(trades_cp["exit_date"]).dt.to_period("M")
    monthly_pnl = trades_cp.groupby("exit_month")["pnl"].sum()
    fig, ax = plt.subplots(figsize=(14, 5))
    colors = ["C2" if v > 0 else "C3" for v in monthly_pnl.values]
    ax.bar(range(len(monthly_pnl)), monthly_pnl.values / 1000,
           color=colors, edgecolor="white", alpha=0.8)
    step = max(1, len(monthly_pnl) // 12)
    ax.set_xticks(range(0, len(monthly_pnl), step))
    labels = [str(m) for m in monthly_pnl.index]
    ax.set_xticklabels([labels[i] for i in range(0, len(labels), step)],
                       rotation=45, ha="right")
    ax.axhline(0, color="gray", linestyle="--", alpha=0.5)
    ax.set_ylabel("Monthly P&L ($K)")
    ax.set_title(f"Portfolio: {label} — Monthly P&L")
    ax.grid(True, alpha=0.3, axis="y")
    fig.tight_layout()
    fig.savefig(str(OUT / f"bt_monthly_{tag}.png"), dpi=150)
    plt.close(fig)

    print(f"  Charts saved: bt_portfolio_{tag}.png, "
          f"bt_positions_{tag}.png, bt_monthly_{tag}.png")


# ══════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    df = load_data()

    print(f"\nParameters:")
    print(f"  Capital:     ${INITIAL_CAPITAL:,}")
    print(f"  Max pos:     {MAX_POSITIONS}")
    print(f"  Kelly:       {KELLY_FRAC} (Half Kelly)")
    print(f"  FF thresh:   {FF_THRESHOLD_OLD} (OLD formula, per combo)")
    print(f"  Min cost:    ${MIN_SPREAD_COST}/share")
    print(f"  Slippage:    ${SLIPPAGE_PER_LEG}/leg/share "
          f"(2-leg=${SLIPPAGE_PER_LEG*2}, 4-leg=${SLIPPAGE_PER_LEG*4})")
    print(f"  Commission:  ${COMMISSION_PER_LEG}/leg/contract "
          f"(2-leg=${COMMISSION_PER_LEG*2}, 4-leg=${COMMISSION_PER_LEG*4})")
    print(f"  Max cts:     {MAX_CONTRACTS}/position")

    # ── Double Calendar Portfolio (all combos combined) ──
    print("\n" + "#" * 60)
    print("# DOUBLE CALENDAR PORTFOLIO (all combos, top 20 by FF)")
    print("#" * 60)
    result_dbl = run_portfolio(df, mode="double")
    if result_dbl:
        print_stats(result_dbl)
        plot_results(result_dbl)

    # ── Single Calendar Portfolio for comparison ──
    print("\n" + "#" * 60)
    print("# SINGLE CALENDAR PORTFOLIO (all combos, top 20 by FF)")
    print("#" * 60)
    result_sgl = run_portfolio(df, mode="single")
    if result_sgl:
        print_stats(result_sgl)
        plot_results(result_sgl)

    print("\n" + "=" * 60)
    print("BACKTEST COMPLETE")
    print("=" * 60)
