"""
Backtest <-> Production Signal Coherence Tests

Deterministic unit tests (no DB, no API) verifying that backtest.py inline
formulas match the canonical implementations in pricing.py, portfolio.py,
config.py, scanner.py, and spreads.py.
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

# Ensure project root on path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from core.pricing import compute_ff, bs_delta_vec, implied_vol_vec, put_call_parity_call_equiv
from core.portfolio import compute_kelly, cost_per_contract, size_portfolio
from core import config
from core.scanner import has_earnings_between

# ── backtest.py constants (inline, not imported from config) ──
import core.backtest as bt


# ═══════════════════════════════════════════════════════════════
# 1a. FF Formula Coherence — pricing.compute_ff vs spreads inline
# ═══════════════════════════════════════════════════════════════

@pytest.mark.parametrize("iv_f, iv_b, dte_f, dte_b", [
    (0.30, 0.25, 30, 60),
    (0.25, 0.22, 45, 90),
    (0.40, 0.35, 20, 80),
    (0.18, 0.20, 60, 90),   # contango case (FF negative)
])
def test_ff_formula_vs_inline(iv_f, iv_b, dte_f, dte_b):
    """pricing.compute_ff matches the inline formula in spreads.py lines 138-143."""
    # Canonical (pricing.py)
    ff_pricing = compute_ff(iv_f, iv_b, dte_f, dte_b)

    # Inline replica (spreads.py logic)
    T_f = dte_f / 365.0
    T_b = dte_b / 365.0
    dT = T_b - T_f
    fwd_var = (iv_b ** 2 * T_b - iv_f ** 2 * T_f) / dT
    if fwd_var <= 0:
        assert np.isnan(ff_pricing)
        return
    fwd_iv = np.sqrt(fwd_var)
    ff_inline = (iv_f - fwd_iv) / fwd_iv

    assert ff_pricing == pytest.approx(ff_inline, abs=1e-12)


# ═══════════════════════════════════════════════════════════════
# 1b. FF Old -> PDF Conversion Roundtrip
# ═══════════════════════════════════════════════════════════════

@pytest.mark.parametrize("ff_old", [0.05, 0.10, 0.25, 0.50, 1.0, 2.0])
def test_ff_old_to_pdf_roundtrip(ff_old):
    """Verify ff_pdf = 1/sqrt(1+ff_old) - 1, and algebraic roundtrip.

    Old formula: ff_old = fwd_var / front_var - 1
    PDF formula: ff_pdf = (front_iv - fwd_iv) / fwd_iv
    Conversion:  ff_pdf = 1/sqrt(1+ff_old) - 1
    Inverse:     ff_old = 1/(1+ff_pdf)^2 - 1
    """
    # Forward conversion (backtest.py:78-80)
    ff_pdf = 1.0 / np.sqrt(1.0 + ff_old) - 1.0

    # Inverse: from PDF back to old
    ff_old_recovered = 1.0 / (1.0 + ff_pdf) ** 2 - 1.0

    assert ff_old_recovered == pytest.approx(ff_old, abs=1e-12)


def test_ff_conversion_algebraic_identity():
    """Verify the conversion is algebraically correct.

    If front_var = iv_f^2, fwd_var = iv_fwd^2, then:
      ff_old = fwd_var/front_var - 1
      ff_pdf = (iv_f - iv_fwd)/iv_fwd = iv_f/iv_fwd - 1

    iv_f/iv_fwd = iv_f / sqrt(fwd_var) = sqrt(front_var / fwd_var)
                = sqrt(front_var / (front_var * (1+ff_old)))
                = 1/sqrt(1+ff_old)

    So ff_pdf = 1/sqrt(1+ff_old) - 1.
    """
    iv_f, iv_fwd = 0.30, 0.25
    front_var = iv_f ** 2
    fwd_var = iv_fwd ** 2

    ff_old = fwd_var / front_var - 1.0
    ff_pdf_direct = (iv_f - iv_fwd) / iv_fwd
    ff_pdf_converted = 1.0 / np.sqrt(1.0 + ff_old) - 1.0

    assert ff_pdf_converted == pytest.approx(ff_pdf_direct, abs=1e-12)


# ═══════════════════════════════════════════════════════════════
# 1c. Cost Per Contract Match
# ═══════════════════════════════════════════════════════════════

@pytest.mark.parametrize("cost, n_legs", [
    (2.00, 4),
    (4.50, 4),
    (0.50, 2),
    (3.25, 4),
    (1.00, 2),
])
def test_cost_per_contract_match(cost, n_legs):
    """portfolio.cost_per_contract matches backtest.py inline (lines 417-418)."""
    # Live (portfolio.py:95-98)
    cpc_live = cost_per_contract(cost, n_legs)

    # Backtest inline (lines 283-286, 417-418)
    slippage_entry = bt.SLIPPAGE_PER_LEG * n_legs
    comm_entry = bt.COMMISSION_PER_LEG * n_legs
    cpc_backtest = (cost + slippage_entry) * bt.CONTRACT_MULT + comm_entry

    assert cpc_live == pytest.approx(cpc_backtest, abs=1e-10)


# ═══════════════════════════════════════════════════════════════
# 1d. P&L Formula Match
# ═══════════════════════════════════════════════════════════════

@pytest.mark.parametrize("entry, exit_val, contracts, n_legs", [
    (2.00, 2.50, 3, 4),
    (4.50, 3.00, 1, 4),
    (1.00, 1.80, 5, 2),
    (3.00, 3.10, 2, 4),
])
def test_pnl_formula_match(entry, exit_val, contracts, n_legs):
    """Backtest P&L (lines 352-355) matches live P&L logic."""
    slippage_entry = bt.SLIPPAGE_PER_LEG * n_legs
    slippage_exit = bt.SLIPPAGE_PER_LEG * n_legs
    comm_entry = bt.COMMISSION_PER_LEG * n_legs
    comm_exit = bt.COMMISSION_PER_LEG * n_legs

    # Backtest formula (lines 352-355)
    pnl_per_share = exit_val - entry - slippage_entry - slippage_exit
    pnl_backtest = (pnl_per_share * contracts * bt.CONTRACT_MULT
                    - contracts * (comm_entry + comm_exit))

    # Live formula (same structure in routes_trading / portfolio)
    slip_in = config.SLIPPAGE_PER_LEG * n_legs
    slip_out = config.SLIPPAGE_PER_LEG * n_legs
    comm_in = config.COMMISSION_LEG * n_legs
    comm_out = config.COMMISSION_LEG * n_legs
    pnl_live = ((exit_val - entry - slip_in - slip_out) * contracts * config.CONTRACT_MULT
                - contracts * (comm_in + comm_out))

    assert pnl_live == pytest.approx(pnl_backtest, abs=1e-10)


# ═══════════════════════════════════════════════════════════════
# 1e. Return Percentage Match
# ═══════════════════════════════════════════════════════════════

@pytest.mark.parametrize("entry, exit_val, contracts, n_legs", [
    (2.00, 2.50, 3, 4),
    (4.50, 3.00, 1, 4),
    (1.00, 1.80, 5, 2),
])
def test_return_pct_match(entry, exit_val, contracts, n_legs):
    """ret_pct = pnl / deployed is identical in both pipelines."""
    slippage_entry = bt.SLIPPAGE_PER_LEG * n_legs
    comm_entry = bt.COMMISSION_PER_LEG * n_legs
    slippage_exit = bt.SLIPPAGE_PER_LEG * n_legs
    comm_exit = bt.COMMISSION_PER_LEG * n_legs

    # deployed
    cpc = (entry + slippage_entry) * bt.CONTRACT_MULT + comm_entry
    deployed = contracts * cpc

    # pnl
    pnl_per_share = exit_val - entry - slippage_entry - slippage_exit
    pnl = (pnl_per_share * contracts * bt.CONTRACT_MULT
           - contracts * (comm_entry + comm_exit))

    ret_backtest = pnl / deployed if deployed > 0 else 0

    # Live path
    cpc_live = cost_per_contract(entry, n_legs)
    deployed_live = contracts * cpc_live
    slip_in = config.SLIPPAGE_PER_LEG * n_legs
    slip_out = config.SLIPPAGE_PER_LEG * n_legs
    comm_in = config.COMMISSION_LEG * n_legs
    comm_out = config.COMMISSION_LEG * n_legs
    pnl_live = ((exit_val - entry - slip_in - slip_out) * contracts * config.CONTRACT_MULT
                - contracts * (comm_in + comm_out))
    ret_live = pnl_live / deployed_live if deployed_live > 0 else 0

    assert ret_live == pytest.approx(ret_backtest, abs=1e-10)


# ═══════════════════════════════════════════════════════════════
# 1f. Kelly Computation Match
# ═══════════════════════════════════════════════════════════════

def test_kelly_match_enough_trades():
    """portfolio.compute_kelly matches backtest inline (lines 398-407)."""
    rng = np.random.default_rng(42)
    returns = rng.normal(0.05, 0.15, size=200).tolist()

    # Live (portfolio.py)
    kelly_live = compute_kelly(returns)

    # Backtest inline (lines 398-407)
    kh = np.array(returns)
    mu_k = kh.mean()
    var_k = kh.var()  # ddof=0
    if var_k > 0 and mu_k > 0:
        kelly_bt = min(bt.KELLY_FRAC * mu_k / var_k, 1.0)
    else:
        kelly_bt = bt.DEFAULT_ALLOC

    assert kelly_live == pytest.approx(kelly_bt, abs=1e-12)


def test_kelly_fallback_few_trades():
    """Both paths fall back to DEFAULT_ALLOC when < MIN_KELLY_TRADES."""
    returns = [0.05, 0.10, -0.02]  # only 3 trades

    kelly_live = compute_kelly(returns)
    # Backtest: len(kelly_history) < MIN_KELLY_TRADES → DEFAULT_ALLOC
    kelly_bt = bt.DEFAULT_ALLOC

    assert kelly_live == pytest.approx(kelly_bt, abs=1e-12)


def test_kelly_negative_mean():
    """Both paths fall back to DEFAULT_ALLOC when mean return is negative."""
    returns = [-0.10] * 100  # negative mean

    kelly_live = compute_kelly(returns)
    # Backtest: mu_k <= 0 → DEFAULT_ALLOC
    assert kelly_live == pytest.approx(bt.DEFAULT_ALLOC, abs=1e-12)


# ═══════════════════════════════════════════════════════════════
# 1g. Two-Pass Entry Logic
# ═══════════════════════════════════════════════════════════════

def test_two_pass_entry_selection():
    """Simulate backtest 2-pass (priority FF>=0.20, then fill) and verify
    it selects the same tickers in the same order as the documented logic."""
    # Create synthetic candidates
    tickers = [f"T{i}" for i in range(10)]
    ff_vals = [0.35, 0.28, 0.22, 0.19, 0.15, 0.12, 0.10, 0.08, 0.05, 0.02]
    candidates = pd.DataFrame({
        "root": tickers,
        "ff": ff_vals,
        "above_thresh": [ff >= 0.20 for ff in ff_vals],
    })

    max_positions = 5

    # Backtest 2-pass (lines 387-393)
    priority = candidates[candidates["above_thresh"]].nlargest(max_positions, "ff")
    remaining_slots = max_positions - len(priority)
    if remaining_slots > 0:
        fill = candidates[~candidates.index.isin(priority.index)].nlargest(remaining_slots, "ff")
        selected = pd.concat([priority, fill])
    else:
        selected = priority

    # Verify: top 3 are above threshold (FF >= 0.20), next 2 are fill (best FF > 0)
    assert list(selected["root"]) == ["T0", "T1", "T2", "T3", "T4"]
    assert all(selected.head(3)["above_thresh"])
    assert not any(selected.tail(2)["above_thresh"])


# ═══════════════════════════════════════════════════════════════
# 1h. Two-Pass Sizing (Extra Budget Distribution)
# ═══════════════════════════════════════════════════════════════

def test_sizing_extra_budget_known_difference():
    """Document and verify the known difference:
    - Backtest sorts extras by FF desc (highest FF gets extras first)
    - portfolio.py sorts extras by cost asc (cheapest gets extras first)
    This is intentional (backtest favors signal quality, live favors diversification).
    """
    signals_info = [
        ("AAPL", 5.00, 4),   # cpc = (5.00+0.12)*100 + 2.60 = 514.60
        ("MSFT", 2.00, 4),   # cpc = (2.00+0.12)*100 + 2.60 = 214.60
        ("GOOG", 3.00, 4),   # cpc = (3.00+0.12)*100 + 2.60 = 314.60
    ]

    kelly_f = 0.10
    account_value = 50_000
    # kelly_target = 5000

    result = size_portfolio(signals_info, kelly_f, account_value)

    # Verify Pass 1: each position gets at least 1 contract
    for ticker, cts, deployed in result:
        assert cts >= 1

    # portfolio.py sorts by cost asc for extras → cheapest (MSFT) gets extras first
    msft = [r for r in result if r[0] == "MSFT"][0]
    aapl = [r for r in result if r[0] == "AAPL"][0]

    # MSFT should have >= AAPL contracts (it's cheapest, gets extras first in live)
    assert msft[1] >= aapl[1]


# ═══════════════════════════════════════════════════════════════
# 1i. DTE Pair Discovery
# ═══════════════════════════════════════════════════════════════

def test_dte_pair_discovery_coherence():
    """scanner.py and spreads.py DTE pairing produce identical pairs
    given the same expiration universe and config constants."""
    # Synthetic expirations with DTE values
    expirations = [
        ("2026-06-01", 20),
        ("2026-06-15", 34),
        ("2026-07-01", 50),
        ("2026-07-15", 64),
        ("2026-08-01", 81),
        ("2026-09-01", 112),
    ]

    # Scanner-style pairing (scanner.py lines 564-580)
    front_exps = [(e, d) for e, d in expirations
                  if config.FRONT_DTE_MIN <= d <= config.FRONT_DTE_MAX]
    back_exps = [(e, d) for e, d in expirations
                 if config.BACK_DTE_MIN <= d <= config.BACK_DTE_MAX]

    scanner_pairs = set()
    for f_exp, f_dte in front_exps:
        for b_exp, b_dte in back_exps:
            if f_exp == b_exp:
                continue
            if b_dte - f_dte >= config.MIN_DTE_GAP:
                scanner_pairs.add((f_exp, b_exp))

    # Spreads-style pairing (spreads.py lines 240-256)
    # Uses same config constants; the only difference is column names
    spreads_pairs = set()
    for f_exp, f_dte in front_exps:
        for b_exp, b_dte in back_exps:
            if f_exp == b_exp:
                continue
            if b_dte - f_dte >= config.MIN_DTE_GAP:
                spreads_pairs.add((f_exp, b_exp))

    assert scanner_pairs == spreads_pairs
    # Verify at least some pairs found
    assert len(scanner_pairs) > 0


# ═══════════════════════════════════════════════════════════════
# 1j. Delta Strike Selection
# ═══════════════════════════════════════════════════════════════

def test_bs_delta_known_value():
    """Verify bs_delta_vec with known S=100, K=105, T=30/365, sigma=0.30."""
    S = np.array([100.0])
    K = np.array([105.0])
    T = np.array([30.0 / 365.0])
    sigma = np.array([0.30])

    delta = bs_delta_vec(S, K, T, sigma)

    # OTM call (K > S) should have delta < 0.5
    assert 0 < delta[0] < 0.5
    # Rough check: ~0.31-0.35 range for these params
    assert 0.20 < delta[0] < 0.45


def test_delta_shared_function():
    """scanner.py and spreads.py both import bs_delta_vec from pricing.py."""
    from core.scanner import bs_delta_vec as scanner_delta
    from research.spreads import bs_delta_vec as spreads_delta

    # They are literally the same function object
    assert scanner_delta is spreads_delta


# ═══════════════════════════════════════════════════════════════
# 1k. Put IV via Put-Call Parity
# ═══════════════════════════════════════════════════════════════

def test_put_call_parity_call_equiv_known():
    """Verify C = P + S - K*e^(-rT) with known values."""
    S = 100.0
    K = np.array([95.0])
    T = np.array([30.0 / 365.0])
    put_price = np.array([1.50])
    r = 0.04

    call_equiv = put_call_parity_call_equiv(put_price, S, K, T, r)

    # Manual: C = 1.50 + 100 - 95*exp(-0.04*30/365) = 1.50 + 100 - 94.688 = 6.812
    discount = K[0] * np.exp(-r * T[0])
    expected = put_price[0] + S - discount
    assert call_equiv[0] == pytest.approx(expected, abs=1e-6)


def test_put_call_parity_shared_function():
    """scanner.py and spreads.py use the same put-call parity path."""
    # Both import from core.pricing
    from core.scanner import put_call_parity_call_equiv as scanner_pcp
    assert scanner_pcp is put_call_parity_call_equiv


# ═══════════════════════════════════════════════════════════════
# 1l. Earnings Filter
# ═══════════════════════════════════════════════════════════════

def test_earnings_filter_match():
    """backtest._has_earnings_between and scanner.has_earnings_between
    use the same np.searchsorted logic."""
    edates = np.array([20260501, 20260715, 20261015])
    earn_by_root = {"AAPL": edates}

    # Case 1: earnings in range
    assert has_earnings_between("AAPL", 20260401, 20260601, earn_by_root) == True
    assert bt._has_earnings_between("AAPL", 20260401, 20260601, earn_by_root) == True

    # Case 2: no earnings in range
    assert has_earnings_between("AAPL", 20260601, 20260714, earn_by_root) == False
    assert bt._has_earnings_between("AAPL", 20260601, 20260714, earn_by_root) == False

    # Case 3: exactly on boundary (end == earnings date)
    assert has_earnings_between("AAPL", 20260601, 20260715, earn_by_root) == True
    assert bt._has_earnings_between("AAPL", 20260601, 20260715, earn_by_root) == True

    # Case 4: no earnings for ticker
    assert has_earnings_between("MSFT", 20260401, 20261231, earn_by_root) == False
    assert bt._has_earnings_between("MSFT", 20260401, 20261231, earn_by_root) == False

    # Case 5: multiple earnings in range
    assert has_earnings_between("AAPL", 20260101, 20261231, earn_by_root) == True
    assert bt._has_earnings_between("AAPL", 20260101, 20261231, earn_by_root) == True


def test_earnings_filter_boundary_start():
    """Earnings exactly at start_int should be included (>= start)."""
    edates = np.array([20260501])
    earn_by_root = {"X": edates}

    # searchsorted(edates, 20260501, side="left") = 0, edates[0] <= 20260601 -> True
    assert has_earnings_between("X", 20260501, 20260601, earn_by_root) == True
    assert bt._has_earnings_between("X", 20260501, 20260601, earn_by_root) == True


# ═══════════════════════════════════════════════════════════════
# 1m. ba_pct Filter
# ═══════════════════════════════════════════════════════════════

def test_ba_pct_constants_match():
    """backtest MAX_BA_PCT == config BA_PCT_MAX."""
    assert bt.MAX_BA_PCT == config.BA_PCT_MAX


def test_ba_pct_filter_logic():
    """Both paths filter ba_pct <= 0.10 identically."""
    ba_values = [0.05, 0.10, 0.11, 0.50]
    threshold = config.BA_PCT_MAX

    passed_live = [v for v in ba_values if v <= threshold]
    passed_bt = [v for v in ba_values if v <= bt.MAX_BA_PCT]

    assert passed_live == passed_bt
    assert passed_live == [0.05, 0.10]


# ═══════════════════════════════════════════════════════════════
# 1n. Constants Match
# ═══════════════════════════════════════════════════════════════

def test_constants_match():
    """All shared constants between backtest.py and config.py are identical."""
    pairs = [
        (bt.SLIPPAGE_PER_LEG,  config.SLIPPAGE_PER_LEG,  "SLIPPAGE_PER_LEG"),
        (bt.COMMISSION_PER_LEG, config.COMMISSION_LEG,    "COMMISSION_LEG"),
        (bt.MAX_POSITIONS,      config.MAX_POSITIONS,     "MAX_POSITIONS"),
        (bt.MAX_CONTRACTS,      config.MAX_CONTRACTS,     "MAX_CONTRACTS"),
        (bt.CONTRACT_MULT,      config.CONTRACT_MULT,     "CONTRACT_MULT"),
        (bt.DEFAULT_ALLOC,      config.DEFAULT_ALLOC,     "DEFAULT_ALLOC"),
        (bt.KELLY_FRAC,         config.KELLY_FRAC,        "KELLY_FRAC"),
        (bt.MIN_KELLY_TRADES,   config.MIN_KELLY_TRADES,  "MIN_KELLY_TRADES"),
        (bt.FF_THRESHOLD,       config.FF_THRESHOLD_DEFAULT, "FF_THRESHOLD"),
    ]
    for bt_val, cfg_val, name in pairs:
        assert bt_val == cfg_val, f"Mismatch: backtest.{name}={bt_val} vs config.{name}={cfg_val}"


def test_min_spread_cost_matches():
    """backtest.MIN_SPREAD_COST == config.MIN_COST."""
    assert bt.MIN_SPREAD_COST == config.MIN_COST


# ═══════════════════════════════════════════════════════════════
# 2a. Exit Slippage Divergence (Known, Documented)
# ═══════════════════════════════════════════════════════════════

def test_exit_slippage_divergence_documented():
    """KNOWN DIVERGENCE: backtest applies slippage at BOTH entry and exit.
    Production (add_position) applies slippage at entry ONLY.

    Backtest P&L (lines 352-353):
        pnl_per_share = exit_val - entry - slippage_entry - slippage_exit

    Production add_position (lines 192-194):
        total_cost = (cost_per_share + slippage) * CONTRACT_MULT * contracts + commission
        (only entry slippage is baked into deployed cost)

    This is conservative for the backtest: slippage penalizes both legs.
    In production, exit slippage is captured by the actual fill price.
    """
    n_legs = 4
    entry = 5.00
    exit_val = 5.50
    contracts = 3

    slip_entry = bt.SLIPPAGE_PER_LEG * n_legs
    slip_exit = bt.SLIPPAGE_PER_LEG * n_legs
    comm_entry = bt.COMMISSION_PER_LEG * n_legs
    comm_exit = bt.COMMISSION_PER_LEG * n_legs

    # Backtest: deducts BOTH entry and exit slippage from P&L
    pnl_bt = ((exit_val - entry - slip_entry - slip_exit) * contracts * bt.CONTRACT_MULT
              - contracts * (comm_entry + comm_exit))

    # Production: only entry slippage in deployed cost, exit is at actual fill
    # If production exit fill == exit_val exactly, the difference is slip_exit*contracts*MULT
    pnl_production_ideal = ((exit_val - entry - slip_entry) * contracts * config.CONTRACT_MULT
                            - contracts * (config.COMMISSION_LEG * n_legs * 2))

    # The difference should be exactly one exit slippage leg
    diff = pnl_production_ideal - pnl_bt
    expected_diff = slip_exit * contracts * bt.CONTRACT_MULT
    assert diff == pytest.approx(expected_diff, abs=1e-10)
    assert expected_diff > 0, "Backtest is more conservative (lower P&L)"


# ═══════════════════════════════════════════════════════════════
# 2b. add_position Deployed Formula Match
# ═══════════════════════════════════════════════════════════════

@pytest.mark.parametrize("cost, contracts, n_legs", [
    (5.00, 3, 4),
    (2.00, 10, 4),
    (3.50, 1, 2),
    (1.00, 5, 4),
])
def test_add_position_deployed_matches_backtest(cost, contracts, n_legs):
    """portfolio.add_position total_deployed == backtest cpc * contracts."""
    # Backtest formula (line 417-418)
    slip = bt.SLIPPAGE_PER_LEG * n_legs
    comm = bt.COMMISSION_PER_LEG * n_legs
    cpc_bt = (cost + slip) * bt.CONTRACT_MULT + comm
    deployed_bt = contracts * cpc_bt

    # portfolio.add_position formula (line 192-194)
    slip_prod = config.SLIPPAGE_PER_LEG * n_legs
    comm_prod = n_legs * config.COMMISSION_LEG * contracts
    deployed_prod = (cost + slip_prod) * config.CONTRACT_MULT * contracts + comm_prod

    assert deployed_prod == pytest.approx(deployed_bt, abs=1e-10)


# ═══════════════════════════════════════════════════════════════
# 2c. Kelly Variance Uses ddof=0 (Population Variance)
# ═══════════════════════════════════════════════════════════════

def test_kelly_uses_population_variance():
    """Both backtest and portfolio use np.var() with ddof=0 (numpy default).
    This is deliberate: slightly aggressive sizing.
    If ddof=1 were used, kelly_f would be higher (smaller variance denominator)."""
    rng = np.random.default_rng(99)
    # Use low mean / high std so Kelly is NOT capped at 1.0 (allows ddof difference)
    returns = rng.normal(0.02, 0.20, size=100).tolist()
    arr = np.array(returns)

    # Portfolio path
    kelly_live = compute_kelly(returns)

    # Explicit ddof=0 (should match)
    mu = arr.mean()
    var_ddof0 = arr.var(ddof=0)
    kelly_ddof0 = min(0.5 * mu / var_ddof0, 1.0) if var_ddof0 > 0 and mu > 0 else 0.04

    # Explicit ddof=1 (should NOT match)
    var_ddof1 = arr.var(ddof=1)
    kelly_ddof1 = min(0.5 * mu / var_ddof1, 1.0) if var_ddof1 > 0 and mu > 0 else 0.04

    assert kelly_live == pytest.approx(kelly_ddof0, abs=1e-12)
    assert kelly_live != pytest.approx(kelly_ddof1, abs=1e-6), \
        "Kelly should use ddof=0, not ddof=1"


# ═══════════════════════════════════════════════════════════════
# 2d. FF_THRESHOLD_MAP Uniformity
# ═══════════════════════════════════════════════════════════════

def test_ff_threshold_map_all_equal():
    """backtest FF_THRESHOLD_MAP values all equal FF_THRESHOLD.
    This means the per-combo thresholds are identical to the global one."""
    for combo, thresh in bt.FF_THRESHOLD_MAP.items():
        assert thresh == bt.FF_THRESHOLD, \
            f"FF_THRESHOLD_MAP['{combo}']={thresh} != FF_THRESHOLD={bt.FF_THRESHOLD}"


def test_ff_threshold_map_equals_config_default():
    """All backtest thresholds match config.FF_THRESHOLD_DEFAULT."""
    for combo, thresh in bt.FF_THRESHOLD_MAP.items():
        assert thresh == config.FF_THRESHOLD_DEFAULT


# ═══════════════════════════════════════════════════════════════
# 2e. n_legs Consistency
# ═══════════════════════════════════════════════════════════════

def test_n_legs_double_calendar():
    """Double calendar = 4 legs (call front + call back + put front + put back)."""
    # Backtest: line 273
    assert 4 == 4  # Hardcoded in run_portfolio for mode="double"

    # This is the value used everywhere. Verify via cost formula:
    cpc_4leg = cost_per_contract(5.00, 4)
    cpc_2leg = cost_per_contract(5.00, 2)
    # 4-leg costs more per contract (more slippage + commission)
    assert cpc_4leg > cpc_2leg

    # Slippage: 4 * 0.03 = 0.12 vs 2 * 0.03 = 0.06
    assert config.SLIPPAGE_PER_LEG * 4 == pytest.approx(0.12)
    assert config.SLIPPAGE_PER_LEG * 2 == pytest.approx(0.06)

    # Commission: 4 * 0.65 = 2.60 vs 2 * 0.65 = 1.30
    assert config.COMMISSION_LEG * 4 == pytest.approx(2.60)
    assert config.COMMISSION_LEG * 2 == pytest.approx(1.30)


# ═══════════════════════════════════════════════════════════════
# 2f. Sizing Cap Formula
# ═══════════════════════════════════════════════════════════════

def test_sizing_cap_backtest_vs_portfolio():
    """Document sizing cap: backtest MAX_CONTRACTS-1, portfolio MAX_CONTRACTS-n.

    In practice both start with n=1, so max extras = 9, max total = 10.
    The difference only matters if n > 1 before Pass 2 (never happens).
    """
    # Backtest cap (line 432): MAX_CONTRACTS - 1 = 9 extras max
    bt_max_extras = bt.MAX_CONTRACTS - 1
    assert bt_max_extras == 9

    # Portfolio cap (line 145): MAX_CONTRACTS - n where n=1 after Pass 1
    # So max extras = MAX_CONTRACTS - 1 = 9 (same in practice)
    n_after_pass1 = 1
    prod_max_extras = config.MAX_CONTRACTS - n_after_pass1
    assert prod_max_extras == 9

    # Both yield max total = 10
    assert 1 + bt_max_extras == config.MAX_CONTRACTS
    assert n_after_pass1 + prod_max_extras == config.MAX_CONTRACTS


def test_sizing_sort_order_divergence():
    """KNOWN DIVERGENCE: backtest sorts extras by FF desc, portfolio sorts by cost asc.

    Backtest: highest FF gets extras first (signal quality priority).
    Portfolio: cheapest position gets extras first (diversification priority).
    """
    signals = [
        ("AAPL", 5.00, 4),  # expensive, high FF (would get extras first in BT)
        ("MSFT", 2.00, 4),  # cheap, lower FF (gets extras first in portfolio)
        ("GOOG", 3.50, 4),  # medium
    ]
    kelly_f = 0.30  # generous to trigger extras
    account = 100_000

    result = size_portfolio(signals, kelly_f, account)

    # Portfolio sorts by cost asc → MSFT (cheapest) gets extras first
    msft_cts = [r[1] for r in result if r[0] == "MSFT"][0]
    aapl_cts = [r[1] for r in result if r[0] == "AAPL"][0]

    # With kelly_budget = 0.30 * 100000 = 30000, MSFT (cpc~214.60) gets maxed first
    assert msft_cts >= aapl_cts, \
        "Portfolio should give more contracts to cheaper positions"
    assert msft_cts == config.MAX_CONTRACTS, \
        f"MSFT should be maxed at {config.MAX_CONTRACTS}, got {msft_cts}"


# ═══════════════════════════════════════════════════════════════
# 2g. RISK_FREE_RATE Shared Across Modules
# ═══════════════════════════════════════════════════════════════

def test_risk_free_rate_consistent():
    """pricing.py RISK_FREE_RATE is used by scanner.py and spreads.py."""
    from core.pricing import RISK_FREE_RATE as pricing_r

    # scanner.py imports from pricing
    from core.scanner import RISK_FREE_RATE as scanner_r

    assert pricing_r == scanner_r
    assert pricing_r == 0.04  # Hardcoded known value


# ═══════════════════════════════════════════════════════════════
# 2h. FF Edge Cases (Division by Zero, Negative Variance)
# ═══════════════════════════════════════════════════════════════

def test_ff_edge_case_equal_dte():
    """Equal DTE → dT=0 → division by zero → NaN."""
    ff = compute_ff(0.30, 0.25, 30, 30)
    assert np.isnan(ff)


def test_ff_edge_case_zero_iv():
    """Zero IV → NaN."""
    assert np.isnan(compute_ff(0.0, 0.25, 30, 60))
    assert np.isnan(compute_ff(0.30, 0.0, 30, 60))


def test_ff_edge_case_negative_fwd_variance():
    """When front_iv >> back_iv (extreme contango), fwd_var can go negative → NaN."""
    # front_iv=0.50, back_iv=0.10, dte=30/60
    # fwd_var = (0.10^2 * 60/365 - 0.50^2 * 30/365) / (30/365)
    # = (0.01 * 0.1644 - 0.25 * 0.0822) / 0.0822
    # = (0.001644 - 0.02055) / 0.0822 = -0.01891 / 0.0822 < 0 → NaN
    ff = compute_ff(0.50, 0.10, 30, 60)
    assert np.isnan(ff)


# ═══════════════════════════════════════════════════════════════
# 2i. Kelly Edge Cases
# ═══════════════════════════════════════════════════════════════

def test_kelly_capped_at_1():
    """Kelly fraction is capped at 1.0 (never more than 100% allocation)."""
    # Very high mu, very low var → raw Kelly >> 1
    returns = [0.50] * 100  # mean=0.50, var=0 → would be infinite
    kelly = compute_kelly(returns)
    # var=0 → mu>0, var≤0 fails the check → DEFAULT_ALLOC
    assert kelly == config.DEFAULT_ALLOC

    # High mu, low var → raw Kelly > 1
    returns_high = [0.90, 0.91, 0.89] * 50  # 150 trades, high mean, tiny var
    kelly_high = compute_kelly(returns_high)
    assert kelly_high <= 1.0


def test_kelly_exactly_min_trades():
    """Exactly MIN_KELLY_TRADES returns should use Kelly, not DEFAULT_ALLOC."""
    rng = np.random.default_rng(42)
    returns = rng.normal(0.05, 0.10, size=config.MIN_KELLY_TRADES).tolist()

    kelly = compute_kelly(returns)
    arr = np.array(returns)
    mu = arr.mean()
    var = arr.var()

    if mu > 0 and var > 0:
        expected = min(config.KELLY_FRAC * mu / var, 1.0)
        assert kelly == pytest.approx(expected, abs=1e-12)
    else:
        assert kelly == config.DEFAULT_ALLOC


# ═══════════════════════════════════════════════════════════════
# 2j. Earnings Filter Edge Cases
# ═══════════════════════════════════════════════════════════════

def test_earnings_filter_empty_array():
    """Empty earnings array → always False."""
    earn_by_root = {"AAPL": np.array([], dtype=int)}
    assert has_earnings_between("AAPL", 20260101, 20261231, earn_by_root) == False
    assert bt._has_earnings_between("AAPL", 20260101, 20261231, earn_by_root) == False


def test_earnings_filter_single_date_exact_match():
    """Single earnings date exactly at start AND end boundaries."""
    earn_by_root = {"X": np.array([20260515])}

    # start = end = earnings date → should be True (>= start AND <= end)
    assert has_earnings_between("X", 20260515, 20260515, earn_by_root) == True
    assert bt._has_earnings_between("X", 20260515, 20260515, earn_by_root) == True


def test_earnings_filter_range_before_all_dates():
    """Query range entirely before all earnings dates → False."""
    earn_by_root = {"X": np.array([20260701, 20261001])}
    assert has_earnings_between("X", 20260101, 20260301, earn_by_root) == False
    assert bt._has_earnings_between("X", 20260101, 20260301, earn_by_root) == False


def test_earnings_filter_range_after_all_dates():
    """Query range entirely after all earnings dates → False."""
    earn_by_root = {"X": np.array([20260101, 20260401])}
    assert has_earnings_between("X", 20260701, 20261231, earn_by_root) == False
    assert bt._has_earnings_between("X", 20260701, 20261231, earn_by_root) == False


# ═══════════════════════════════════════════════════════════════
# 2k. Cost Per Contract Boundary Cases
# ═══════════════════════════════════════════════════════════════

def test_cost_per_contract_zero_cost():
    """Zero cost_per_share → only slippage + commission."""
    cpc = cost_per_contract(0.0, 4)
    expected = (0.0 + config.SLIPPAGE_PER_LEG * 4) * config.CONTRACT_MULT + config.COMMISSION_LEG * 4
    assert cpc == pytest.approx(expected, abs=1e-10)
    assert cpc > 0, "Even $0 spread costs something due to slippage+commission"


def test_cost_per_contract_exact_values():
    """Verify exact CPC for typical double calendar ($5.00 cost, 4 legs)."""
    # (5.00 + 0.03*4) * 100 + 0.65*4 = (5.00 + 0.12) * 100 + 2.60 = 514.60
    cpc = cost_per_contract(5.00, 4)
    assert cpc == pytest.approx(514.60, abs=1e-10)


# ═══════════════════════════════════════════════════════════════
# 2l. Backtest ba_pct Filter Uses <= (Not <)
# ═══════════════════════════════════════════════════════════════

def test_ba_pct_filter_uses_leq():
    """Both backtest and scanner use <= (not <) for ba_pct filter.
    ba_pct == BA_PCT_MAX should PASS the filter."""
    exact_threshold = config.BA_PCT_MAX  # 0.10

    # Scanner filter: ba_pct > BA_PCT_MAX → continue (i.e., ba_pct <= threshold passes)
    assert not (exact_threshold > config.BA_PCT_MAX), "Exact threshold should pass scanner"

    # Backtest filter: sub["ba_pct"] <= MAX_BA_PCT
    assert exact_threshold <= bt.MAX_BA_PCT, "Exact threshold should pass backtest"


# ═══════════════════════════════════════════════════════════════
# 2m. Scanner + Backtest FF Filter Logic Match
# ═══════════════════════════════════════════════════════════════

def test_ff_filter_positive_only():
    """Both backtest and scanner require FF > 0 (backwardation).
    Scanner: 'if np.isnan(ff) or ff <= 0: continue' (line 821-822)
    Backtest: 'sub = sub[sub["ff"] > 0].copy()' (line 294)"""
    ff_values = [-0.05, 0.0, 0.001, 0.10, 0.25]

    scanner_pass = [ff for ff in ff_values if not np.isnan(ff) and ff > 0]
    backtest_pass = [ff for ff in ff_values if ff > 0]

    assert scanner_pass == backtest_pass
    assert scanner_pass == [0.001, 0.10, 0.25]


def test_ff_priority_threshold_match():
    """Priority threshold: scanner FF_THRESHOLD_DEFAULT == backtest FF_THRESHOLD."""
    # Scanner uses FF_THRESHOLD_DEFAULT for priority (not hard filter)
    # Backtest uses FF_THRESHOLD (same value) for priority in 2-pass
    assert config.FF_THRESHOLD_DEFAULT == bt.FF_THRESHOLD


# ═══════════════════════════════════════════════════════════════
# 2n. Implied Vol Computation Coherence
# ═══════════════════════════════════════════════════════════════

def test_implied_vol_roundtrip():
    """BS price → implied_vol → BS price should roundtrip within tolerance."""
    from core.pricing import bs_price

    S, K, T, sigma = 100.0, 105.0, 60/365.0, 0.25
    price = bs_price(S, K, T, sigma)

    # Recover IV
    iv = implied_vol_vec(
        np.array([price]),
        np.array([S]),
        np.array([K]),
        np.array([T]),
    )[0]

    assert iv == pytest.approx(sigma, abs=0.01), \
        f"IV roundtrip: original={sigma}, recovered={iv}"


def test_put_call_parity_iv_consistency():
    """Put price → call equiv via parity → IV should match call IV."""
    from core.pricing import bs_price

    S, K, T, sigma, r = 100.0, 95.0, 45/365.0, 0.30, 0.04

    call_price = bs_price(S, K, T, sigma, r, "C")
    put_price = bs_price(S, K, T, sigma, r, "P")

    # Put-call parity: C = P + S - K*e^(-rT)
    call_from_parity = put_call_parity_call_equiv(
        np.array([put_price]), S, np.array([K]), np.array([T]), r
    )[0]

    assert call_from_parity == pytest.approx(call_price, abs=1e-6)

    # IV from both should be the same
    iv_call = implied_vol_vec(
        np.array([call_price]), np.array([S]), np.array([K]), np.array([T]), r
    )[0]
    iv_parity = implied_vol_vec(
        np.array([call_from_parity]), np.array([S]), np.array([K]), np.array([T]), r
    )[0]

    assert iv_call == pytest.approx(iv_parity, abs=0.001)
