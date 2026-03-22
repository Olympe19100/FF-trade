"""
PDF-Style Backtest: reproduce the PDF's exact methodology.
- No costs (no slippage, no commission)
- FF threshold filter (>= 0.20 or 0.23 per combo) — ALL trades above threshold
- Generalized Kelly (sequential compounding)
- Daily-grouped returns for realistic equity curve
"""
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from scipy.optimize import minimize_scalar
from datetime import datetime
from pathlib import Path

OUT = Path(r"C:\Users\ANTEC MSI\Desktop\pro\Option trading\output")
CACHE = Path(r"C:\Users\ANTEC MSI\Desktop\pro\Option trading\cache")

df = pd.read_pickle(str(CACHE / "spread_returns.pkl"))
df = df[np.isfinite(df["ff"])].copy()
df["entry_dt"] = pd.to_datetime(df["obs_date"].astype(str), format="%Y%m%d")

# PDF original thresholds
FF_PDF = {"30-60": 0.230, "30-90": 0.230, "60-90": 0.200}
RET_CLIP = (-1.5, 5.5)


def generalized_kelly(rets):
    def neg_g(f):
        return -np.mean(np.log(np.maximum(1 + f * rets, 1e-12)))
    res = minimize_scalar(neg_g, bounds=(0.001, 2.0), method="bounded")
    return res.x


pdf_ref = {
    "30-60": {"cagr": 20.46, "sharpe": 1.92, "kelly": 0.161},
    "30-90": {"cagr": 21.93, "sharpe": 2.06, "kelly": 0.201},
    "60-90": {"cagr": 27.79, "sharpe": 1.97, "kelly": 0.184},
}

print("=" * 70)
print("PDF EXACT: FF threshold + ALL trades above + Kelly sequentiel")
print("=" * 70)

fig, axes = plt.subplots(1, 3, figsize=(20, 6))
fig.suptitle("Single Calendar — PDF Methodology (FF Threshold, No Costs)",
             fontsize=13)

for ci, combo in enumerate(["30-60", "30-90", "60-90"]):
    ff_t = FF_PDF[combo]
    sub = df[(df["combo"] == combo) & (df["ff"] >= ff_t)].copy()
    sub["ret_c"] = sub["ret"].clip(*RET_CLIP)
    sub = sub.sort_values("entry_dt")

    rets = sub["ret_c"].values
    n = len(rets)
    d0c, d1c = sub["entry_dt"].min(), sub["entry_dt"].max()
    days = (d1c - d0c).days
    ny = days / 365.25
    nm = ny * 12
    p = pdf_ref[combo]

    kelly_f = generalized_kelly(rets)
    half_kelly = kelly_f / 2

    # Equity: daily-grouped for realism
    daily = sub.groupby("entry_dt")["ret_c"].mean().sort_index()

    # Our Kelly
    eq_ours = [100000.0]
    for r in daily.values:
        eq_ours.append(eq_ours[-1] * (1 + half_kelly * r))
    eq_ours = np.array(eq_ours)

    # PDF Kelly
    pdf_hk = p["kelly"] / 2
    eq_pdf = [100000.0]
    for r in daily.values:
        eq_pdf.append(eq_pdf[-1] * (1 + pdf_hk * r))
    eq_pdf = np.array(eq_pdf)

    cagr_o = (eq_ours[-1] / eq_ours[0]) ** (365.25 / days) - 1
    cagr_p = (eq_pdf[-1] / eq_pdf[0]) ** (365.25 / days) - 1

    daily_ret_o = half_kelly * daily.values
    daily_ret_p = pdf_hk * daily.values
    sharpe_o = (np.mean(daily_ret_o) / np.std(daily_ret_o) * np.sqrt(252)
                if np.std(daily_ret_o) > 0 else 0)
    sharpe_p = (np.mean(daily_ret_p) / np.std(daily_ret_p) * np.sqrt(252)
                if np.std(daily_ret_p) > 0 else 0)

    peak_o = np.maximum.accumulate(eq_ours)
    maxdd_o = ((eq_ours - peak_o) / peak_o).min()
    peak_p = np.maximum.accumulate(eq_pdf)
    maxdd_p = ((eq_pdf - peak_p) / peak_p).min()

    # FF quintiles to check monotonicity
    sub_q = sub.copy()
    sub_q["ff_q"] = pd.qcut(sub_q["ff"], 5, labels=False, duplicates="drop")

    print(f"\n--- {combo} (FF>={ff_t}, ALL trades above threshold) ---")
    print(f"  n={n}, {n/nm:.1f}/mois, "
          f"mean={rets.mean():+.4f}, std={rets.std():.4f}, wr={(rets>0).mean():.1%}")
    print(f"  Kelly f*={kelly_f:.4f} ({kelly_f*100:.1f}%)  "
          f"[PDF: {p['kelly']*100:.1f}%]")
    print(f"  Our Half Kelly: CAGR={cagr_o*100:.2f}%, "
          f"Sharpe={sharpe_o:.2f}, MaxDD={maxdd_o*100:.1f}%")
    print(f"  PDF Half Kelly: CAGR={cagr_p*100:.2f}%, "
          f"Sharpe={sharpe_p:.2f}, MaxDD={maxdd_p*100:.1f}%")
    print(f"  [PDF ref:       CAGR={p['cagr']:.2f}%, "
          f"Sharpe={p['sharpe']:.2f}]")
    end_o = eq_ours[-1]
    end_p = eq_pdf[-1]
    print(f"  $100K -> ours: ${end_o:,.0f} | pdf_k: ${end_p:,.0f}")

    # FF quintiles
    print(f"  FF -> Return monotonicity:")
    for q in sorted(sub_q["ff_q"].unique()):
        grp = sub_q[sub_q["ff_q"] == q]
        print(f"    Q{q}: FF=[{grp['ff'].min():.3f}-{grp['ff'].max():.3f}], "
              f"mean_ret={grp['ret_c'].mean():+.4f}, "
              f"wr={(grp['ret_c']>0).mean():.1%}, n={len(grp)}")

    # Plot
    ax = axes[ci]
    eq_d_plot = np.concatenate([[daily.index[0]], daily.index.values])
    ax.plot(pd.to_datetime(eq_d_plot), eq_ours, linewidth=1.2,
            color="steelblue", label=f"Our Kelly ({half_kelly*100:.1f}%)")
    ax.plot(pd.to_datetime(eq_d_plot), eq_pdf, linewidth=1.2,
            color="darkorange", ls="--",
            label=f"PDF Kelly ({pdf_hk*100:.1f}%)")
    ax.set_title(f"{combo} Half Kelly", fontsize=12)
    ax.legend(fontsize=8)
    ax.grid(True, alpha=0.3)

    stats = (f"Our:  CAGR={cagr_o*100:.1f}% Sh={sharpe_o:.2f}\n"
             f"PDF:  CAGR={cagr_p*100:.1f}% Sh={sharpe_p:.2f}\n"
             f"Ref:  CAGR={p['cagr']:.1f}% Sh={p['sharpe']:.2f}\n"
             f"n={n} ({n/nm:.0f}/mo)")
    ax.text(0.02, 0.98, stats, transform=ax.transAxes, fontsize=9,
            verticalalignment="top", fontfamily="monospace",
            bbox=dict(boxstyle="round", facecolor="lightyellow", alpha=0.8))

    ymax = max(eq_ours.max(), eq_pdf.max())
    if ymax > 1e6:
        ax.yaxis.set_major_formatter(
            plt.FuncFormatter(lambda x, _: f"${x/1e6:.1f}M"))
    else:
        ax.yaxis.set_major_formatter(
            plt.FuncFormatter(lambda x, _: f"${x/1e3:.0f}K"))

plt.tight_layout(rect=[0, 0, 1, 0.94])
fig.savefig(str(OUT / "pdf_exact_methodology.png"), dpi=150,
            bbox_inches="tight")
print(f"\nChart: output/pdf_exact_methodology.png")
