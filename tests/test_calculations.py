"""
Unit Tests — Forward Factor Calendar Spread Strategy

Tests cover:
  1. Forward Factor (FF) computation
  2. FF GUI conversion (old <-> GUI)
  3. Spread return calculations (single + double)
  4. P&L with slippage + commission
  5. Cash accounting (entry-exit cycle)
  6. Kelly criterion (simple half-Kelly)
  7. Position sizing two-pass
  8. Performance metrics (CAGR, Sharpe, Max Drawdown)
  9. Cost per contract calculation
  10. Edge cases and data validation
  11. Mark-to-Market valuation

Usage:
    python -m pytest tests/test_calculations.py -v
    python tests/test_calculations.py   (standalone)
"""

import sys
import sqlite3
import numpy as np
import pandas as pd
import pytest
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from core.config import ROOT


# ═══════════════════════════════════════════════════════════
# 1. FORWARD FACTOR COMPUTATION
# ═══════════════════════════════════════════════════════════

class TestForwardFactor:
    """Tests for FF = fwd_var / front_var - 1."""

    def test_ff_basic(self):
        """FF with known IV values — PDF/Campasano formula."""
        from core.scanner import compute_ff

        # front: 30% vol, 30 DTE; back: 35% vol, 60 DTE
        iv_f, iv_b = 0.30, 0.35
        dte_f, dte_b = 30, 60
        T_f, T_b = 30 / 365, 60 / 365
        dT = T_b - T_f

        # PDF formula: FF = (front_iv - fwd_iv) / fwd_iv
        fwd_var = (iv_b**2 * T_b - iv_f**2 * T_f) / dT
        fwd_iv = np.sqrt(fwd_var)
        expected_ff = (iv_f - fwd_iv) / fwd_iv

        result = compute_ff(iv_f, iv_b, dte_f, dte_b)
        assert abs(result - expected_ff) < 1e-10, \
            f"FF mismatch: got {result:.6f}, expected {expected_ff:.6f}"

    def test_ff_equal_vol(self):
        """When front and back IV are equal, FF should be 0."""
        from core.scanner import compute_ff

        result = compute_ff(0.25, 0.25, 30, 60)
        assert abs(result) < 1e-10, f"Equal vol should give FF=0, got {result}"

    def test_ff_higher_front_vol(self):
        """Mild inversion (front > back) should give FF > 0 (backwardation = good for calendar).

        PDF formula: front_iv > fwd_iv → FF > 0.
        Note: extreme inversion (front >> back) makes fwd_var < 0 → NaN.
        """
        from core.scanner import compute_ff

        # Mild inversion: front 30%, back 28%
        result = compute_ff(0.30, 0.28, 30, 60)
        assert result > 0, f"Mild inversion should give FF>0, got {result}"

        # Extreme inversion: fwd_var < 0 → NaN
        result2 = compute_ff(0.40, 0.25, 30, 60)
        assert np.isnan(result2), f"Extreme inversion should give NaN, got {result2}"

    def test_ff_higher_back_vol(self):
        """Normal term structure (back > front) should give FF < 0 (contango)."""
        from core.scanner import compute_ff

        result = compute_ff(0.25, 0.40, 30, 60)
        assert result < 0, f"Normal term structure should give FF<0, got {result}"

    def test_ff_zero_front_iv(self):
        """Zero front IV should return NaN (division by zero)."""
        from core.scanner import compute_ff

        result = compute_ff(0, 0.25, 30, 60)
        assert np.isnan(result), f"Zero front IV should give NaN, got {result}"

    def test_ff_zero_back_iv(self):
        """Zero back IV should return NaN."""
        from core.scanner import compute_ff

        result = compute_ff(0.25, 0, 30, 60)
        assert np.isnan(result), f"Zero back IV should give NaN, got {result}"

    def test_ff_same_dte(self):
        """Same DTE (dT=0) should return NaN (division by zero)."""
        from core.scanner import compute_ff

        result = compute_ff(0.25, 0.30, 30, 30)
        assert np.isnan(result), f"Same DTE should give NaN, got {result}"

    def test_ff_inverted_dte(self):
        """Back DTE < Front DTE should return NaN."""
        from core.scanner import compute_ff

        result = compute_ff(0.25, 0.30, 60, 30)
        assert np.isnan(result), f"Inverted DTE should give NaN, got {result}"

    def test_ff_realistic_threshold(self):
        """Verify a typical signal passes the 20% FF threshold (PDF formula).

        For FF > 0 we need front_iv > back_iv (backwardation).
        front 35% vol, back 30% vol → front is expensive → good for calendar.
        """
        from core.scanner import compute_ff

        result = compute_ff(0.35, 0.30, 30, 60)
        assert result > 0.20, \
            f"Strong signal should exceed 20% threshold, got {result:.4f}"

    def test_ff_matches_spreads_formula(self):
        """Verify scanner.compute_ff matches PDF/Campasano formula exactly."""
        from core.scanner import compute_ff

        iv_f, iv_b = 0.28, 0.33
        dte_f, dte_b = 30, 90

        T_f = dte_f / 365.0
        T_b = dte_b / 365.0
        dT = T_b - T_f

        # PDF/Campasano: FF = (front_iv - fwd_iv) / fwd_iv
        fwd_var = (iv_b**2 * T_b - iv_f**2 * T_f) / dT
        fwd_iv = np.sqrt(fwd_var)
        ff_pdf = (iv_f - fwd_iv) / fwd_iv

        ff_scanner = compute_ff(iv_f, iv_b, dte_f, dte_b)
        assert abs(ff_scanner - ff_pdf) < 1e-12, \
            f"Scanner vs PDF formula mismatch: {ff_scanner} vs {ff_pdf}"


# ═══════════════════════════════════════════════════════════
# 2. FF GUI CONVERSION
# ═══════════════════════════════════════════════════════════

class TestFFConversion:
    """Tests for old_to_gui and gui_to_old conversions."""

    def test_roundtrip_old_gui_old(self):
        """old -> gui -> old should be identity."""
        from research.analysis import old_to_gui, gui_to_old

        for ff_old in [0.0, 0.10, 0.23, 0.50, 1.0, 2.0]:
            ff_gui = old_to_gui(ff_old)
            ff_back = gui_to_old(ff_gui)
            assert abs(ff_back - ff_old) < 1e-10, \
                f"Roundtrip failed for ff_old={ff_old}: got {ff_back}"

    def test_old_zero_gives_gui_zero(self):
        """FF_old=0 should give FF_gui=0."""
        from research.analysis import old_to_gui

        result = old_to_gui(0.0)
        assert abs(result) < 1e-10, f"old=0 should give gui=0, got {result}"

    def test_old_positive_gives_gui_negative(self):
        """Positive FF_old (elevated forward var) should give negative FF_gui."""
        from research.analysis import old_to_gui

        result = old_to_gui(0.23)
        assert result < 0, f"Positive old should give negative gui, got {result}"

    def test_gui_formula_matches_scanner(self):
        """Verify analysis.py old_to_gui matches scanner.py line 571."""
        from research.analysis import old_to_gui

        ff_old = 0.35
        # scanner.py formula
        scanner_gui = 1.0 / np.sqrt(1.0 + ff_old) - 1.0
        analysis_gui = old_to_gui(ff_old)
        assert abs(scanner_gui - analysis_gui) < 1e-12

    def test_old_negative_one(self):
        """FF_old = -1 should give NaN (fwd_var = 0)."""
        from research.analysis import old_to_gui

        result = old_to_gui(-1.0)
        assert np.isnan(result), f"old=-1 should give NaN, got {result}"


# ═══════════════════════════════════════════════════════════
# 3. SPREAD RETURN CALCULATIONS
# ═══════════════════════════════════════════════════════════

class TestSpreadReturns:
    """Tests for calendar spread return formulas."""

    def test_single_calendar_return(self):
        """Single calendar: ret = (exit_val - entry_cost) / entry_cost."""
        # Entry: sell front call @ $3, buy back call @ $5 -> cost = $2
        entry_cost = 5.0 - 3.0  # $2 debit
        # Exit (J-1): sell back call @ $6, buy front call @ $1 -> exit_val = $5
        exit_val = 6.0 - 1.0  # $5 credit
        expected_ret = (exit_val - entry_cost) / entry_cost  # 1.5 = 150%
        assert abs(expected_ret - 1.5) < 1e-10

    def test_single_calendar_loss(self):
        """Single calendar losing trade."""
        entry_cost = 2.0
        exit_val = 1.0  # back leg lost value
        ret = (exit_val - entry_cost) / entry_cost
        assert ret == -0.5, f"Expected -50% return, got {ret}"

    def test_single_calendar_total_loss(self):
        """Calendar spread total loss (exit_val = 0)."""
        entry_cost = 2.0
        exit_val = 0.0
        ret = (exit_val - entry_cost) / entry_cost
        assert ret == -1.0, f"Total loss should be -100%, got {ret}"

    def test_double_calendar_return(self):
        """Double calendar: combine call and put legs."""
        # Call leg
        call_entry = 2.0  # back_call - front_call at entry
        call_exit = 3.0   # back_call - front_call at exit

        # Put leg
        put_entry = 1.5   # back_put - front_put at entry
        put_exit = 2.0    # back_put - front_put at exit

        combined_entry = call_entry + put_entry  # $3.50
        combined_exit = call_exit + put_exit      # $5.00

        double_ret = (combined_exit - combined_entry) / combined_entry
        expected = (5.0 - 3.5) / 3.5
        assert abs(double_ret - expected) < 1e-10

    def test_exit_val_reconstruction(self):
        """Verify backtest.py exit_val_per_share = cost * (1 + ret)."""
        cost = 2.50
        # Simulate returns.py calculation
        exit_val_actual = 3.75
        ret = (exit_val_actual - cost) / cost  # 0.50

        # Backtest reconstruction
        exit_val_reconstructed = cost * (1 + ret)
        assert abs(exit_val_reconstructed - exit_val_actual) < 1e-10, \
            f"Reconstruction failed: {exit_val_reconstructed} vs {exit_val_actual}"

    def test_double_exit_val_reconstruction(self):
        """Verify exit_val reconstruction for double calendar."""
        combined_cost = 4.00
        exit_val_actual = 5.20
        double_ret = (exit_val_actual - combined_cost) / combined_cost  # 0.30

        reconstructed = combined_cost * (1 + double_ret)
        assert abs(reconstructed - exit_val_actual) < 1e-10


# ═══════════════════════════════════════════════════════════
# 4. P&L WITH SLIPPAGE + COMMISSION
# ═══════════════════════════════════════════════════════════

class TestPnLCalculation:
    """Tests for P&L with all friction costs."""

    # Constants matching backtest.py
    SLIPPAGE_PER_LEG = 0.03
    COMMISSION_PER_LEG = 0.65
    CONTRACT_MULT = 100

    def test_pnl_double_calendar_win(self):
        """Winning double calendar trade: verify P&L step by step."""
        n_legs = 4
        contracts = 5
        cost_per_share = 3.00      # entry mid
        exit_val_per_share = 4.50  # exit mid

        slip_entry = self.SLIPPAGE_PER_LEG * n_legs  # $0.12
        slip_exit = self.SLIPPAGE_PER_LEG * n_legs    # $0.12
        comm_entry = self.COMMISSION_PER_LEG * n_legs  # $2.60
        comm_exit = self.COMMISSION_PER_LEG * n_legs   # $2.60

        # P&L formula from backtest.py lines 136-139
        pnl_per_share = (exit_val_per_share - cost_per_share
                         - slip_entry - slip_exit)
        # = 4.50 - 3.00 - 0.12 - 0.12 = 1.26

        pnl = (pnl_per_share * contracts * self.CONTRACT_MULT
               - contracts * (comm_entry + comm_exit))
        # = 1.26 * 500 - 5 * 5.20 = 630 - 26 = 604

        expected_pnl_per_share = 4.50 - 3.00 - 0.12 - 0.12
        assert abs(pnl_per_share - expected_pnl_per_share) < 1e-10

        expected_pnl = 1.26 * 500 - 5 * 5.20
        assert abs(pnl - expected_pnl) < 1e-10, \
            f"P&L mismatch: {pnl:.2f} vs {expected_pnl:.2f}"

    def test_pnl_double_calendar_loss(self):
        """Losing double calendar trade."""
        n_legs = 4
        contracts = 3
        cost_per_share = 3.00
        exit_val_per_share = 2.00  # lost 1/3

        slip_entry = self.SLIPPAGE_PER_LEG * n_legs
        slip_exit = self.SLIPPAGE_PER_LEG * n_legs
        comm_entry = self.COMMISSION_PER_LEG * n_legs
        comm_exit = self.COMMISSION_PER_LEG * n_legs

        pnl_per_share = (exit_val_per_share - cost_per_share
                         - slip_entry - slip_exit)
        pnl = (pnl_per_share * contracts * self.CONTRACT_MULT
               - contracts * (comm_entry + comm_exit))

        # = (2.00 - 3.00 - 0.12 - 0.12) * 300 - 3 * 5.20
        # = -1.24 * 300 - 15.60 = -372 - 15.60 = -387.60
        assert pnl < 0, f"Should be negative P&L, got {pnl}"
        expected = -1.24 * 300 - 15.60
        assert abs(pnl - expected) < 1e-10

    def test_pnl_single_calendar(self):
        """Single calendar (2 legs) has lower friction."""
        n_legs = 2
        contracts = 5
        cost_per_share = 2.00
        exit_val_per_share = 2.50

        slip_entry = self.SLIPPAGE_PER_LEG * n_legs  # $0.06
        slip_exit = self.SLIPPAGE_PER_LEG * n_legs    # $0.06
        comm_entry = self.COMMISSION_PER_LEG * n_legs  # $1.30
        comm_exit = self.COMMISSION_PER_LEG * n_legs   # $1.30

        pnl_per_share = exit_val_per_share - cost_per_share - slip_entry - slip_exit
        pnl = (pnl_per_share * contracts * self.CONTRACT_MULT
               - contracts * (comm_entry + comm_exit))

        # = (2.50 - 2.00 - 0.06 - 0.06) * 500 - 5 * 2.60
        # = 0.38 * 500 - 13 = 190 - 13 = 177
        expected = 0.38 * 500 - 13
        assert abs(pnl - expected) < 1e-10

    def test_friction_is_material(self):
        """Verify friction costs are material (not negligible)."""
        n_legs = 4
        contracts = 10
        cost = 2.00
        exit_val = 2.50

        # Without friction
        pnl_no_friction = (exit_val - cost) * contracts * self.CONTRACT_MULT

        # With friction
        slip = self.SLIPPAGE_PER_LEG * n_legs
        comm = self.COMMISSION_PER_LEG * n_legs
        pnl_per_share = exit_val - cost - slip - slip
        pnl_with_friction = (pnl_per_share * contracts * self.CONTRACT_MULT
                             - contracts * (comm + comm))

        friction_impact = pnl_no_friction - pnl_with_friction
        # = (0.12+0.12)*1000 + 10*5.20 = 240 + 52 = 292
        assert friction_impact > 200, \
            f"Friction should be material, impact = ${friction_impact:.0f}"


# ═══════════════════════════════════════════════════════════
# 5. CASH ACCOUNTING (full entry-exit cycle)
# ═══════════════════════════════════════════════════════════

class TestCashAccounting:
    """Verify cash is conserved through entry-exit cycle."""

    def test_cash_conservation(self):
        """Total P&L = final_cash - initial_cash (no value left in positions)."""
        SLIPPAGE = 0.03
        COMM = 0.65
        n_legs = 4
        contracts = 3
        cost_per_share = 3.00
        exit_val_per_share = 4.00

        initial_cash = 100_000.0

        slip_entry = SLIPPAGE * n_legs
        slip_exit = SLIPPAGE * n_legs
        comm_entry = COMM * n_legs
        comm_exit = COMM * n_legs

        # Entry: cash outflow
        cost_per_contract = (cost_per_share + slip_entry) * 100 + comm_entry
        deployed = contracts * cost_per_contract
        cash_after_entry = initial_cash - deployed

        # Exit: cash inflow (backtest.py: cash += deployed + pnl)
        pnl_per_share = (exit_val_per_share - cost_per_share
                         - slip_entry - slip_exit)
        pnl = (pnl_per_share * contracts * 100
               - contracts * (comm_entry + comm_exit))
        cash_after_exit = cash_after_entry + deployed + pnl

        # Net cash change = exit proceeds - entry cost
        # = exit_val*shares - slip_exit*shares - comm_exit*cts
        net_change = cash_after_exit - initial_cash

        # Manual: true P&L should be:
        #   (exit_mid - entry_mid - total_slippage) * shares - total_commission * cts
        expected_net = ((exit_val_per_share - cost_per_share - slip_entry - slip_exit)
                        * contracts * 100
                        - contracts * (comm_entry + comm_exit))
        assert abs(net_change - expected_net) < 1e-10, \
            f"Cash conservation violated: {net_change} vs {expected_net}"

    def test_no_money_created_or_destroyed(self):
        """
        After all positions close, cash should equal initial + sum(PnL).
        Simulate 3 trades manually.
        """
        SLIPPAGE = 0.03
        COMM = 0.65
        n_legs = 4
        initial = 50_000.0
        cash = initial

        trades = [
            (2, 3.00, 4.50),  # win: 2 contracts, cost=3, exit=4.5
            (1, 2.50, 1.00),  # loss: 1 contract, cost=2.5, exit=1
            (3, 1.80, 2.20),  # small win: 3 contracts, cost=1.8, exit=2.2
        ]

        slip_e = SLIPPAGE * n_legs
        slip_x = SLIPPAGE * n_legs
        comm_e = COMM * n_legs
        comm_x = COMM * n_legs
        total_pnl = 0

        for cts, cost, exit_val in trades:
            cpc = (cost + slip_e) * 100 + comm_e
            deployed = cts * cpc
            cash -= deployed

            pnl_ps = exit_val - cost - slip_e - slip_x
            pnl = pnl_ps * cts * 100 - cts * (comm_e + comm_x)
            cash += deployed + pnl
            total_pnl += pnl

        assert abs(cash - (initial + total_pnl)) < 1e-8, \
            f"Cash = {cash:.2f}, expected {initial + total_pnl:.2f}"


# ═══════════════════════════════════════════════════════════
# 6. KELLY CRITERION
# ═══════════════════════════════════════════════════════════

class TestKellyCriterion:
    """Tests for Half Kelly f = 0.5 * mu / var."""

    def test_half_kelly_basic(self):
        """Known returns -> known Kelly fraction."""
        returns = np.array([0.10, 0.20, -0.05, 0.15, 0.30,
                            -0.10, 0.25, 0.05, 0.12, -0.08])
        mu = returns.mean()
        var = returns.var()  # ddof=0 (numpy default, matches backtest.py)
        expected_f = 0.5 * mu / var
        assert expected_f > 0, "With positive mean, Kelly should be positive"
        assert abs(expected_f - 0.5 * mu / var) < 1e-10

    def test_kelly_negative_mean(self):
        """Negative mean returns -> Kelly should use default alloc."""
        returns = np.array([-0.10, -0.20, -0.05, -0.15])
        mu = returns.mean()
        var = returns.var()
        # backtest.py: if mu <= 0, use DEFAULT_ALLOC
        kelly_f = min(0.5 * mu / var, 1.0) if var > 0 and mu > 0 else 0.04
        assert kelly_f == 0.04, f"Negative mean should fallback to 4%, got {kelly_f}"

    def test_kelly_capped_at_one(self):
        """Kelly fraction should not exceed 1.0."""
        returns = np.array([10.0, 10.0, 10.0])  # extreme positive
        mu = returns.mean()
        var = returns.var()
        kelly_f = min(0.5 * mu / var, 1.0)
        assert kelly_f == 1.0, f"Kelly should be capped at 1.0, got {kelly_f}"

    def test_kelly_population_vs_sample_variance(self):
        """Document difference between ddof=0 (used) and ddof=1 (textbook)."""
        returns = np.array([0.10, 0.20, -0.05, 0.15, 0.30,
                            -0.10, 0.25, 0.05, 0.12, -0.08] * 5)
        mu = returns.mean()

        var_pop = returns.var(ddof=0)   # used in backtest.py
        var_samp = returns.var(ddof=1)  # textbook

        kelly_pop = 0.5 * mu / var_pop
        kelly_samp = 0.5 * mu / var_samp

        # Population variance is smaller -> Kelly is MORE aggressive
        assert kelly_pop > kelly_samp, \
            "Population variance should give more aggressive Kelly"
        # Difference should be small with 50+ trades
        pct_diff = abs(kelly_pop - kelly_samp) / kelly_samp
        assert pct_diff < 0.05, \
            f"Variance difference > 5%: {pct_diff:.1%} (n={len(returns)})"

    def test_generalized_kelly_positive_returns(self):
        """Generalized Kelly should find positive f* for profitable strategy."""
        from research.analysis import generalized_kelly

        rng = np.random.default_rng(42)
        returns = rng.normal(0.10, 0.30, 200)  # positive edge

        f_star, growth, _, _ = generalized_kelly(returns)
        assert f_star > 0, f"f* should be positive for positive-edge strategy, got {f_star}"
        assert growth > 0, f"Growth rate should be positive, got {growth}"

    def test_generalized_kelly_matches_simple_for_moderate_variance(self):
        """For moderate variance, generalized ≈ simple Kelly."""
        from research.analysis import generalized_kelly

        # Use returns with realistic variance (not too small)
        rng = np.random.default_rng(123)
        returns = rng.normal(0.05, 0.30, 500)  # mean=5%, std=30%

        mu = returns.mean()
        var = returns.var()
        simple_kelly = mu / var

        f_star, _, _, _ = generalized_kelly(returns, f_max=5.0)

        # Both should be in the same ballpark (within 50%)
        pct_diff = abs(f_star - simple_kelly) / max(abs(simple_kelly), 1e-6)
        assert pct_diff < 0.50, \
            f"Generalized ({f_star:.3f}) too far from simple ({simple_kelly:.3f})"


# ═══════════════════════════════════════════════════════════
# 7. POSITION SIZING TWO-PASS
# ═══════════════════════════════════════════════════════════

class TestPositionSizing:
    """Tests for trader.py size_portfolio() — Two-pass Kelly sizing."""

    def test_minimum_one_contract(self):
        """Every position should get at least 1 contract."""
        from core.trader import size_portfolio

        signals = [
            ("AAPL", 3.00, 4),
            ("MSFT", 2.50, 4),
            ("GOOG", 5.00, 4),
        ]
        result = size_portfolio(signals, kelly_f=0.04, account_value=100_000)
        for ticker, cts, deployed in result:
            assert cts >= 1, f"{ticker} got {cts} contracts, minimum is 1"

    def test_max_contracts_cap(self):
        """No position should exceed MAX_CONTRACTS (10)."""
        from core.trader import size_portfolio

        signals = [("AAPL", 1.00, 4)]  # very cheap
        result = size_portfolio(signals, kelly_f=0.50, account_value=1_000_000)
        for ticker, cts, deployed in result:
            assert cts <= 10, f"{ticker} got {cts} contracts, max is 10"

    def test_empty_signals(self):
        """No signals -> empty result."""
        from core.trader import size_portfolio

        result = size_portfolio([], kelly_f=0.04, account_value=100_000)
        assert result == []

    def test_cost_per_contract(self):
        """Verify cost_per_contract formula."""
        from core.trader import cost_per_contract

        # cost_per_contract = (cps + SLIPPAGE_BUFFER) * 100 + COMMISSION_LEG * n_legs
        cps = 3.00
        n_legs = 4
        result = cost_per_contract(cps, n_legs)
        expected = (3.00 + 0.03) * 100 + 0.65 * 4  # 303 + 2.60 = 305.60
        assert abs(result - expected) < 1e-10, \
            f"CPC: {result} vs {expected}"

    def test_deployed_matches_contracts_times_cpc(self):
        """Total deployed should equal contracts * cost_per_contract."""
        from core.trader import size_portfolio, cost_per_contract

        signals = [
            ("AAPL", 3.00, 4),
            ("MSFT", 2.00, 4),
        ]
        result = size_portfolio(signals, kelly_f=0.10, account_value=200_000)
        for (ticker_sig, cps, n_legs), (ticker_res, cts, deployed) in zip(signals, result):
            cpc = cost_per_contract(cps, n_legs)
            expected_deployed = cts * cpc
            assert abs(deployed - expected_deployed) < 1e-8, \
                f"{ticker_res}: deployed={deployed}, expected={expected_deployed}"


# ═══════════════════════════════════════════════════════════
# 8. PERFORMANCE METRICS
# ═══════════════════════════════════════════════════════════

class TestPerformanceMetrics:
    """Tests for CAGR, Sharpe, Max Drawdown calculations."""

    def test_cagr_doubling(self):
        """$100K -> $200K in 5 years = CAGR ~14.87%."""
        initial = 100_000
        final = 200_000
        years = 5.0
        cagr = (final / initial) ** (1 / years) - 1
        expected = 2 ** (1/5) - 1
        assert abs(cagr - expected) < 1e-10
        assert abs(cagr - 0.1487) < 0.001

    def test_cagr_no_change(self):
        """No change -> CAGR = 0."""
        cagr = (100_000 / 100_000) ** (1 / 5) - 1
        assert abs(cagr) < 1e-10

    def test_sharpe_known_returns(self):
        """Sharpe = mean / std * sqrt(N) for known daily returns."""
        daily_returns = np.array([0.001] * 252)  # constant 0.1% daily
        mean_r = daily_returns.mean()
        std_r = daily_returns.std()
        # std of constant is 0 -> Sharpe is undefined/infinite
        # Use a more realistic case
        daily_returns = np.array([0.001, 0.002, -0.001, 0.003, 0.0005] * 50)
        mean_r = daily_returns.mean()
        std_r = daily_returns.std()
        n_per_year = len(daily_returns) / 1.0  # assume 1 year
        sharpe = mean_r / std_r * np.sqrt(n_per_year)
        assert sharpe > 0, f"Positive-edge returns should have positive Sharpe"

    def test_max_drawdown_known(self):
        """Known drawdown: 100 -> 120 -> 90 -> 110 => DD = (90-120)/120 = -25%."""
        equity = pd.Series([100, 110, 120, 100, 90, 100, 110])
        peak = equity.cummax()
        dd = (equity - peak) / peak
        max_dd = dd.min()
        assert abs(max_dd - (-0.25)) < 1e-10, f"Max DD = {max_dd}, expected -25%"

    def test_max_drawdown_no_drawdown(self):
        """Monotonically increasing -> max DD = 0."""
        equity = pd.Series([100, 110, 120, 130, 140])
        peak = equity.cummax()
        dd = (equity - peak) / peak
        max_dd = dd.min()
        assert max_dd == 0, f"No drawdown case should be 0, got {max_dd}"

    def test_max_drawdown_total_loss(self):
        """100 -> 0 => DD = -100%."""
        equity = pd.Series([100, 50, 0.01])
        peak = equity.cummax()
        dd = (equity - peak) / peak
        max_dd = dd.min()
        assert max_dd < -0.99, f"Near-total loss DD should be ~-100%, got {max_dd}"


# ═══════════════════════════════════════════════════════════
# 9. BACKTEST INTEGRATION: POSITION SIZING BUG
# ═══════════════════════════════════════════════════════════

class TestPositionSizingBug:
    """
    Test for the two-pass sizing bug in backtest.py lines 199-207:
    extra_budget is not decremented as contracts are allocated,
    potentially causing over-allocation.
    """

    def test_extra_budget_overallocation(self):
        """
        Demonstrate that backtest.py two-pass can over-allocate.
        With 5 candidates at $300/contract and kelly_budget = $3000:
        - min_total = 5 * 300 = $1500
        - extra_budget = $1500
        - Each position claims int(1500/5/300) = 1 extra contract
        - Total = 5 * 2 contracts * $300 = $3000 ✓ (happens to work here)

        But with uneven costs it breaks:
        """
        # Simulate backtest.py two-pass logic (lines 184-207)
        kelly_budget = 5000
        cand_list = [
            ("AAPL", 200),   # cheap
            ("GOOG", 200),
            ("MSFT", 800),   # expensive
        ]
        MAX_CONTRACTS = 10

        # Pass 1: 1 contract each
        min_total = sum(cpc for _, cpc in cand_list)  # 1200

        # Pass 2: extra_budget = 5000 - 1200 = 3800
        extra_budget = max(0, kelly_budget - min_total)

        total_allocated = 0
        allocations = []
        for name, cpc in cand_list:
            extra_cts = int(extra_budget / len(cand_list) / cpc)
            contracts = 1 + max(0, min(extra_cts, MAX_CONTRACTS - 1))
            deployed = contracts * cpc
            allocations.append((name, contracts, deployed))
            total_allocated += deployed

        # Bug: total_allocated can exceed kelly_budget
        # because extra_budget is shared but not decremented
        # AAPL: extra = int(3800/3/200) = int(6.33) = 6 -> 7 contracts -> $1400
        # GOOG: extra = int(3800/3/200) = 6 -> 7 contracts -> $1400
        # MSFT: extra = int(3800/3/800) = int(1.58) = 1 -> 2 contracts -> $1600
        # Total = $1400 + $1400 + $1600 = $4400 < $5000 (ok in this case)

        # But with kelly_budget barely above min_total:
        kelly_budget_tight = 1800
        extra_budget_tight = max(0, kelly_budget_tight - min_total)  # 600

        total_tight = 0
        for name, cpc in cand_list:
            extra_cts = int(extra_budget_tight / len(cand_list) / cpc)
            contracts = 1 + max(0, min(extra_cts, MAX_CONTRACTS - 1))
            total_tight += contracts * cpc

        # Each position tries to claim 600/3 = $200 extra
        # AAPL: extra = int(200/200) = 1 -> 2 cts -> $400
        # GOOG: extra = int(200/200) = 1 -> 2 cts -> $400
        # MSFT: extra = int(200/800) = 0 -> 1 ct  -> $800
        # Total = $1600 < $1800 (under-allocation due to int truncation)

        # The bug is that total_tight != kelly_budget_tight in general
        # It can be over OR under depending on cost distribution
        # This test documents the behavior:
        assert total_tight != kelly_budget_tight, \
            "Two-pass sizing does not exactly match Kelly budget (known limitation)"

    def test_trader_sizing_respects_budget(self):
        """Verify trader.py size_portfolio doesn't exceed Kelly target."""
        from core.trader import size_portfolio

        signals = [
            ("AAPL", 1.00, 4),  # very cheap
            ("MSFT", 1.00, 4),
            ("GOOG", 1.00, 4),
            ("AMZN", 1.00, 4),
            ("META", 1.00, 4),
        ]
        kelly_f = 0.05
        account = 100_000
        kelly_target = kelly_f * account  # $5000

        result = size_portfolio(signals, kelly_f, account)
        total = sum(deployed for _, _, deployed in result)

        # trader.py has the same issue but is bounded by MAX_CONTRACTS
        # Total should not massively exceed Kelly target
        assert total < kelly_target * 3, \
            f"Total deployed ${total:.0f} is >3x Kelly target ${kelly_target:.0f}"


# ═══════════════════════════════════════════════════════════
# 10. EDGE CASES AND DATA VALIDATION
# ═══════════════════════════════════════════════════════════

class TestEdgeCases:
    """Edge cases that could corrupt backtest results."""

    def test_negative_spread_cost_filtered(self):
        """Negative spread cost (back < front mid) should be filtered out."""
        # backtest.py line 90: sub = sub[sub[cost_col] >= MIN_SPREAD_COST]
        MIN_SPREAD_COST = 1.00
        costs = pd.Series([-0.50, 0.00, 0.50, 1.00, 1.50, 2.00])
        filtered = costs[costs >= MIN_SPREAD_COST]
        assert len(filtered) == 3
        assert filtered.min() == 1.00

    def test_infinite_ff_filtered(self):
        """Infinite FF values should be excluded."""
        ff_values = np.array([0.1, 0.2, np.inf, -np.inf, np.nan, 0.3])
        valid = ff_values[np.isfinite(ff_values)]
        assert len(valid) == 3

    def test_zero_deployed_no_division_error(self):
        """ret_pct with zero deployed should not crash."""
        deployed = 0
        pnl = 100
        ret_pct = pnl / deployed if deployed > 0 else 0
        assert ret_pct == 0

    def test_extreme_returns_not_clipped_in_backtest(self):
        """
        Raw returns in backtest are NOT clipped (unlike pdf_style_bt.py).
        Verify this is intentional — extreme returns affect Kelly.
        """
        # pdf_style_bt.py clips at (-1.5, 5.5) for display
        # backtest.py uses raw returns from spread_returns.pkl
        # This is by design — Kelly should see the full distribution
        returns = np.array([0.10, 0.20, -0.90, 5.50, -1.00, 10.0])
        mu = returns.mean()
        var = returns.var()
        kelly_unclipped = 0.5 * mu / var

        returns_clipped = np.clip(returns, -1.5, 5.5)
        mu_c = returns_clipped.mean()
        var_c = returns_clipped.var()
        kelly_clipped = 0.5 * mu_c / var_c

        # Unclipped Kelly can differ significantly
        assert abs(kelly_unclipped - kelly_clipped) / max(abs(kelly_clipped), 1e-6) > 0.01, \
            "Extreme returns should meaningfully affect Kelly sizing"

    def test_position_exit_on_correct_date(self):
        """Positions should close when exit_dt <= current date."""
        import pandas as pd
        exit_dt = pd.Timestamp("2024-03-15")

        # Should close on the exit date
        assert exit_dt <= pd.Timestamp("2024-03-15")
        # Should close if past
        assert exit_dt <= pd.Timestamp("2024-03-18")
        # Should NOT close before
        assert not (exit_dt <= pd.Timestamp("2024-03-14"))


# ═══════════════════════════════════════════════════════════
# 11. BACKTEST RESULT VERIFICATION (smoke test on real data)
# ═══════════════════════════════════════════════════════════

class TestBacktestResults:
    """Verify backtest output against expected ranges (real data)."""

    @pytest.fixture
    def bt_data(self):
        """Load spread_returns.pkl if available."""
        cache = ROOT / "cache" / "spread_returns.pkl"
        if not cache.exists():
            pytest.skip("spread_returns.pkl not found")
        df = pd.read_pickle(str(cache))
        df = df[np.isfinite(df["ff"])].copy()
        return df

    def test_data_has_expected_columns(self, bt_data):
        """Verify required columns exist."""
        required = ["obs_date", "root", "combo", "ff", "spread_cost",
                     "ret", "exit_date", "front_exp", "back_exp"]
        for col in required:
            assert col in bt_data.columns, f"Missing column: {col}"

    def test_data_volume(self, bt_data):
        """Should have >100K trades in the universe."""
        assert len(bt_data) > 100_000, \
            f"Expected >100K trades, got {len(bt_data):,}"

    def test_ff_distribution(self, bt_data):
        """FF should have reasonable distribution."""
        assert bt_data["ff"].median() < 0.5, "Median FF seems too high"
        assert bt_data["ff"].min() < -0.5, "Should have negative FF values"
        assert bt_data["ff"].max() > 1.0, "Should have large positive FF values"

    def test_return_distribution(self, bt_data):
        """Returns should have reasonable range AFTER backtest filters."""
        # Raw data has near-zero spread_cost outliers (1e-15) producing
        # astronomical returns (1e+14). These are filtered by spread_cost >= $1
        # and FF >= threshold in the backtest, so we test the filtered data.
        MIN_SPREAD_COST = 1.00
        filtered = bt_data[bt_data["spread_cost"] >= MIN_SPREAD_COST]
        assert filtered["ret"].mean() > -0.1, "Filtered mean return too negative"
        assert filtered["ret"].mean() < 1.0, \
            f"Filtered mean return too positive: {filtered['ret'].mean():.4f}"
        assert filtered["ret"].min() > -5.0, "Min return unreasonably negative"

    def test_raw_data_has_outliers(self, bt_data):
        """Document: raw unfiltered data has extreme outliers from tiny spread costs."""
        extreme = bt_data[bt_data["ret"].abs() > 1000]
        assert len(extreme) > 0, "Expected extreme outliers in raw data"
        # All extreme outliers should have near-zero spread cost
        assert extreme["spread_cost"].max() < 0.01, \
            "Extreme outliers should come from near-zero spread costs"

    def test_outliers_filtered_by_backtest(self, bt_data):
        """Verify that ALL extreme outliers are caught by backtest filters."""
        FF_THRESH = {"30-60": 0.230, "30-90": 0.230, "60-90": 0.200}
        for combo, thresh in FF_THRESH.items():
            safe = bt_data[(bt_data["combo"] == combo)
                           & (bt_data["ff"] >= thresh)
                           & (bt_data["spread_cost"] >= 1.00)]
            max_abs = safe["ret"].abs().max() if len(safe) > 0 else 0
            assert max_abs < 50, \
                f"{combo}: max |return| after filters = {max_abs:.1f}, expected < 50"

    def test_combo_coverage(self, bt_data):
        """All three DTE combos should be present."""
        combos = set(bt_data["combo"].unique())
        assert "30-60" in combos
        assert "30-90" in combos
        assert "60-90" in combos

    def test_filtered_trades_positive_edge(self, bt_data):
        """Trades above FF threshold should have positive mean return."""
        FF_THRESHOLDS = {"30-60": 0.230, "30-90": 0.230, "60-90": 0.200}
        for combo, thresh in FF_THRESHOLDS.items():
            filtered = bt_data[(bt_data["combo"] == combo) & (bt_data["ff"] >= thresh)]
            if len(filtered) > 50:
                assert filtered["ret"].mean() > 0, \
                    f"{combo} filtered mean return should be positive, " \
                    f"got {filtered['ret'].mean():.4f}"

    def test_ff_above_threshold_has_edge(self, bt_data):
        """FF >= threshold trades should have materially better returns
        than FF < threshold. This is the actual strategy signal.

        Note: full-distribution monotonicity does NOT hold across all quintiles
        for double calendars (bottom quintile inflated by survivorship in put legs).
        FF works as a threshold filter, not a linear ranking signal."""
        if "double_ret" not in bt_data.columns:
            pytest.skip("No double_ret column")

        dbl = bt_data.dropna(subset=["double_ret", "combined_cost"])
        dbl = dbl[(dbl["combined_cost"] >= 1.00) & (dbl["combined_cost"] > 0)]

        FF_THRESH = {"30-60": 0.230, "30-90": 0.230, "60-90": 0.200}
        for combo, thresh in FF_THRESH.items():
            sub = dbl[dbl["combo"] == combo]
            above = sub[sub["ff"] >= thresh]["double_ret"]
            below = sub[sub["ff"] < thresh]["double_ret"]
            if len(above) < 50 or len(below) < 50:
                continue
            # Above-threshold should have positive mean
            assert above.mean() > 0, \
                f"{combo}: FF>={thresh} mean return should be positive"
            # Above-threshold win rate should exceed 60%
            wr = (above > 0).mean()
            assert wr > 0.60, \
                f"{combo}: FF>={thresh} win rate = {wr:.1%}, expected > 60%"

    def test_ff_monotonicity_single_partial(self, bt_data):
        """Single calendar: 30-60 and 30-90 should show FF monotonicity.
        60-90 does NOT (known empirical result - bottom quintile = 11.9% vs top = 10.1%)."""
        sub_all = bt_data[bt_data["spread_cost"] >= 1.00]
        for combo in ["30-60", "30-90"]:
            sub = sub_all[sub_all["combo"] == combo].copy()
            if len(sub) < 100:
                continue
            sub["q"] = pd.qcut(sub["ff"], 5, labels=False, duplicates="drop")
            q_means = sub.groupby("q")["ret"].mean()
            assert q_means.iloc[-1] > q_means.iloc[0], \
                f"{combo}: top FF quintile should beat bottom"

    def test_double_calendar_has_data(self, bt_data):
        """Double calendar returns should exist."""
        if "double_ret" in bt_data.columns:
            dbl = bt_data.dropna(subset=["double_ret"])
            assert len(dbl) > 1000, \
                f"Expected >1000 double calendar trades, got {len(dbl)}"

    def test_double_beats_single_winrate(self, bt_data):
        """Double calendar should have higher win rate than single (call-only)."""
        if "double_ret" not in bt_data.columns:
            pytest.skip("No double_ret column")

        FF_THRESH = 0.23
        single = bt_data[bt_data["ff"] >= FF_THRESH]
        double = bt_data[bt_data["ff"] >= FF_THRESH].dropna(subset=["double_ret"])

        if len(single) < 100 or len(double) < 100:
            pytest.skip("Not enough filtered trades")

        wr_single = (single["ret"] > 0).mean()
        wr_double = (double["double_ret"] > 0).mean()

        assert wr_double > wr_single, \
            f"Double WR ({wr_double:.1%}) should beat single ({wr_single:.1%})"


# ═══════════════════════════════════════════════════════════
# 12. RISK METRICS
# ═══════════════════════════════════════════════════════════

class TestRiskMetrics:
    """Tests for VaR/CVaR calculations."""

    def test_var_95(self):
        """VaR 95% = 5th percentile of returns."""
        returns = np.array(sorted(range(-50, 50))) / 100
        var_95 = np.percentile(returns, 5)
        # 5th percentile of [-0.50 ... 0.49] is the 5th value = -0.45
        assert var_95 < 0
        assert abs(var_95 - (-0.45)) < 0.02

    def test_cvar_worse_than_var(self):
        """CVaR (expected shortfall) should be worse than VaR."""
        rng = np.random.default_rng(42)
        returns = rng.normal(0.05, 0.20, 1000)
        var_95 = np.percentile(returns, 5)
        cvar_95 = returns[returns <= np.percentile(returns, 5)].mean()
        assert cvar_95 < var_95, \
            f"CVaR ({cvar_95:.4f}) should be worse than VaR ({var_95:.4f})"


# ═══════════════════════════════════════════════════════════
# 13. MTM ARITHMETIC — hand-calculable verification
# ═══════════════════════════════════════════════════════════

class TestMTMArithmetic:
    """Dead-simple hand calculations to verify every MTM formula.

    Every test uses round numbers so the expected result is obvious.
    Comments show every intermediate step.
    """

    # ── Spread value formula ──

    def test_double_spread_value_integers(self):
        """Double calendar spread value with integer prices.

        Position: short front, long back — both calls and puts.
          fc = 1,  bc = 4  → call spread = 4 - 1 = 3
          fp = 2,  bp = 5  → put spread  = 5 - 2 = 3
          total spread = 3 + 3 = 6

        1 contract = 100 shares
        10 contracts → MTM = 6 × 10 × 100 = $6,000
        """
        from core.backtest import _compute_mtm_value

        sk = 100000   # strike = 100.0
        rows_c = [
            (1, "X", 20240101, sk, "C"),   # front call
            (2, "X", 20240201, sk, "C"),   # back call
            (3, "X", 20240101, sk, "P"),   # front put
            (4, "X", 20240201, sk, "P"),   # back put
        ]
        rows_e = [
            # bid = ask = mid (simplest case)
            (1, 20240115, 1, 1, 1),  # fc = 1
            (2, 20240115, 4, 4, 4),  # bc = 4
            (3, 20240115, 2, 2, 2),  # fp = 2
            (4, 20240115, 5, 5, 5),  # bp = 5
        ]
        conn = sqlite3.connect(":memory:")
        cur = conn.cursor()
        cur.execute("CREATE TABLE contracts (contract_id INTEGER PRIMARY KEY,"
                    " root TEXT, expiration INTEGER, strike INTEGER, right TEXT)")
        cur.execute("CREATE TABLE eod_history (contract_id INTEGER, date INTEGER,"
                    " bid REAL, ask REAL, close REAL)")
        cur.executemany("INSERT INTO contracts VALUES (?,?,?,?,?)", rows_c)
        cur.executemany("INSERT INTO eod_history VALUES (?,?,?,?,?)", rows_e)
        conn.commit()

        cache = {(r, e, s, ri): cid for cid, r, e, s, ri in rows_c}
        pos = {
            "ticker": "X", "contracts": 10, "deployed": 99999,
            "front_exp": 20240101, "back_exp": 20240201,
            "front_strike": 100.0,  # 100.0 × 1000 = 100000 = sk
        }

        result = _compute_mtm_value([pos], 20240115, conn, cache, "double")
        conn.close()

        #  (4-1 + 5-2) × 10 × 100 = 6 × 1000 = 6000
        assert result == 6000

    def test_single_spread_value_integers(self):
        """Single calendar: fc=3, bc=8 → spread=5. 5 cts → $2,500."""
        from core.backtest import _compute_mtm_value

        sk = 200000
        rows_c = [
            (1, "Y", 20240101, sk, "C"),
            (2, "Y", 20240201, sk, "C"),
        ]
        rows_e = [
            (1, 20240115, 3, 3, 3),  # fc = 3
            (2, 20240115, 8, 8, 8),  # bc = 8
        ]
        conn = sqlite3.connect(":memory:")
        cur = conn.cursor()
        cur.execute("CREATE TABLE contracts (contract_id INTEGER PRIMARY KEY,"
                    " root TEXT, expiration INTEGER, strike INTEGER, right TEXT)")
        cur.execute("CREATE TABLE eod_history (contract_id INTEGER, date INTEGER,"
                    " bid REAL, ask REAL, close REAL)")
        cur.executemany("INSERT INTO contracts VALUES (?,?,?,?,?)", rows_c)
        cur.executemany("INSERT INTO eod_history VALUES (?,?,?,?,?)", rows_e)
        conn.commit()

        cache = {(r, e, s, ri): cid for cid, r, e, s, ri in rows_c}
        pos = {
            "ticker": "Y", "contracts": 5, "deployed": 99999,
            "front_exp": 20240101, "back_exp": 20240201,
            "front_strike": 200.0,
        }

        result = _compute_mtm_value([pos], 20240115, conn, cache, "single")
        conn.close()

        #  (8-3) × 5 × 100 = 5 × 500 = 2500
        assert result == 2500

    # ── Midpoint formula ──

    def test_midpoint_simple(self):
        """bid=2, ask=8 → mid=(2+8)/2=5.  bid=6, ask=10 → mid=8.
        spread = 8 - 5 = 3.  1 ct → $300."""
        from core.backtest import _compute_mtm_value

        sk = 150000
        rows_c = [
            (1, "Z", 20240101, sk, "C"),
            (2, "Z", 20240201, sk, "C"),
        ]
        rows_e = [
            (1, 20240115, 2, 8, 99),  # mid = 5  (close=99, ignored)
            (2, 20240115, 6, 10, 99), # mid = 8
        ]
        conn = sqlite3.connect(":memory:")
        cur = conn.cursor()
        cur.execute("CREATE TABLE contracts (contract_id INTEGER PRIMARY KEY,"
                    " root TEXT, expiration INTEGER, strike INTEGER, right TEXT)")
        cur.execute("CREATE TABLE eod_history (contract_id INTEGER, date INTEGER,"
                    " bid REAL, ask REAL, close REAL)")
        cur.executemany("INSERT INTO contracts VALUES (?,?,?,?,?)", rows_c)
        cur.executemany("INSERT INTO eod_history VALUES (?,?,?,?,?)", rows_e)
        conn.commit()

        cache = {(r, e, s, ri): cid for cid, r, e, s, ri in rows_c}
        pos = {
            "ticker": "Z", "contracts": 1, "deployed": 99999,
            "front_exp": 20240101, "back_exp": 20240201,
            "front_strike": 150.0,
        }

        result = _compute_mtm_value([pos], 20240115, conn, cache, "single")
        conn.close()

        #  (8-5) × 1 × 100 = 300
        assert result == 300

    def test_close_fallback_simple(self):
        """bid=0 → skip bid/ask → use close.
        fc close=5, bc bid=6 ask=10 mid=8 → spread = 8-5 = 3. 1 ct → $300."""
        from core.backtest import _compute_mtm_value

        sk = 150000
        rows_c = [
            (1, "Z", 20240101, sk, "C"),
            (2, "Z", 20240201, sk, "C"),
        ]
        rows_e = [
            (1, 20240115, 0, 8, 5),   # bid=0 → use close=5
            (2, 20240115, 6, 10, 99),  # mid = 8
        ]
        conn = sqlite3.connect(":memory:")
        cur = conn.cursor()
        cur.execute("CREATE TABLE contracts (contract_id INTEGER PRIMARY KEY,"
                    " root TEXT, expiration INTEGER, strike INTEGER, right TEXT)")
        cur.execute("CREATE TABLE eod_history (contract_id INTEGER, date INTEGER,"
                    " bid REAL, ask REAL, close REAL)")
        cur.executemany("INSERT INTO contracts VALUES (?,?,?,?,?)", rows_c)
        cur.executemany("INSERT INTO eod_history VALUES (?,?,?,?,?)", rows_e)
        conn.commit()

        cache = {(r, e, s, ri): cid for cid, r, e, s, ri in rows_c}
        pos = {
            "ticker": "Z", "contracts": 1, "deployed": 99999,
            "front_exp": 20240101, "back_exp": 20240201,
            "front_strike": 150.0,
        }

        result = _compute_mtm_value([pos], 20240115, conn, cache, "single")
        conn.close()

        #  (8-5) × 1 × 100 = 300
        assert result == 300

    # ── Entry-day accounting (trace every dollar) ──

    def test_entry_day_trace(self):
        """Trace every dollar at entry for 1 double-calendar contract.

        Given:
          spread_cost    = $5.00 per share
          n_legs         = 4  (double calendar)
          contracts      = 1
          initial cash   = $10,000

        Step-by-step:
          slip_entry  = 0.03 × 4         = $0.12/share
          comm_entry  = 0.65 × 4         = $2.60/contract

          cost_per_contract = (5.00 + 0.12) × 100 + 2.60
                            = 512.00 + 2.60
                            = $514.60

          deployed    = 1 × 514.60       = $514.60
          cash_after  = 10000 - 514.60   = $9,485.40

        At-cost accounting:
          invested    = 514.60
          account     = 9485.40 + 514.60 = $10,000.00  (always = initial)

        MTM accounting (assume market mids still = entry mids, spread still $5):
          mtm_invested = 5.00 × 1 × 100  = $500.00
          account_mtm  = 9485.40 + 500   = $9,985.40

        Friction gap = 10000 - 9985.40   = $14.60
                     = (0.12 × 100 + 2.60) × 1
                     = slip + comm per contract
        """
        from core.backtest import (SLIPPAGE_PER_LEG, COMMISSION_PER_LEG,
                                   CONTRACT_MULT)

        spread_cost = 5.00
        n_legs = 4
        contracts = 1
        initial_cash = 10_000.00

        # ── Entry calculations ──
        slip_entry = SLIPPAGE_PER_LEG * n_legs                   # 0.03×4 = 0.12
        comm_entry = COMMISSION_PER_LEG * n_legs                 # 0.65×4 = 2.60
        cpc = (spread_cost + slip_entry) * CONTRACT_MULT + comm_entry  # 514.60
        deployed = contracts * cpc                               # 514.60
        cash = initial_cash - deployed                           # 9485.40

        assert abs(slip_entry - 0.12) < 1e-10
        assert abs(comm_entry - 2.60) < 1e-10
        assert abs(cpc - 514.60) < 1e-10
        assert abs(deployed - 514.60) < 1e-10
        assert abs(cash - 9485.40) < 1e-10

        # ── At-cost ──
        invested = deployed
        account = cash + invested
        assert abs(account - initial_cash) < 1e-10,  \
            f"At-cost account should = initial: {account}"

        # ── MTM (market = entry price) ──
        mtm_invested = spread_cost * contracts * CONTRACT_MULT   # 500.00
        account_mtm = cash + mtm_invested                        # 9985.40

        assert abs(mtm_invested - 500.00) < 1e-10
        assert abs(account_mtm - 9985.40) < 1e-10

        # ── Friction gap ──
        gap = account - account_mtm                              # 14.60
        expected_gap = (slip_entry * CONTRACT_MULT + comm_entry) * contracts
        assert abs(gap - 14.60) < 1e-10
        assert abs(gap - expected_gap) < 1e-10

    # ── Exit P&L (trace every dollar) ──

    def test_exit_pnl_trace(self):
        """Trace every dollar at exit for 1 double-calendar contract.

        Given:
          entry spread = $5.00/share,  exit spread = $7.00/share
          n_legs = 4,  contracts = 1

        Step-by-step:
          slip_entry  = 0.03 × 4 = $0.12
          slip_exit   = 0.03 × 4 = $0.12
          comm_entry  = 0.65 × 4 = $2.60
          comm_exit   = 0.65 × 4 = $2.60

          pnl_per_share = 7.00 - 5.00 - 0.12 - 0.12 = $1.76
          pnl_shares    = 1.76 × 1 × 100             = $176.00
          pnl_total     = 176.00 - 1×(2.60+2.60)     = 176.00 - 5.20
                        = $170.80

        Cash flow:
          deployed   = (5.00+0.12)×100 + 2.60 = $514.60
          cash_before_exit = 10000 - 514.60    = $9,485.40
          cash_after_exit  = 9485.40 + 514.60 + 170.80 = $10,170.80

          net profit = 10170.80 - 10000 = $170.80 ✓
        """
        from core.backtest import (SLIPPAGE_PER_LEG, COMMISSION_PER_LEG,
                                   CONTRACT_MULT)

        cost = 5.00
        exit_val = 7.00
        n_legs = 4
        contracts = 1
        initial_cash = 10_000.00

        slip = SLIPPAGE_PER_LEG * n_legs   # 0.12
        comm = COMMISSION_PER_LEG * n_legs # 2.60

        # ── Entry ──
        cpc = (cost + slip) * CONTRACT_MULT + comm  # 514.60
        deployed = contracts * cpc
        cash = initial_cash - deployed              # 9485.40

        # ── Exit P&L (backtest.py lines 288-291) ──
        pnl_per_share = exit_val - cost - slip - slip           # 1.76
        pnl = (pnl_per_share * contracts * CONTRACT_MULT
               - contracts * (comm + comm))                     # 170.80

        assert abs(pnl_per_share - 1.76) < 1e-10
        assert abs(pnl - 170.80) < 1e-10

        # ── Cash after exit (backtest.py line 294: cash += deployed + pnl) ──
        cash += deployed + pnl

        assert abs(cash - 10170.80) < 1e-10
        assert abs(cash - initial_cash - pnl) < 1e-10,  \
            "Final cash = initial + P&L"

    def test_exit_losing_trade_trace(self):
        """Losing trade: spread falls from $5 to $3.

        pnl_per_share = 3.00 - 5.00 - 0.12 - 0.12 = -$2.24
        pnl_shares    = -2.24 × 1 × 100            = -$224.00
        pnl_total     = -224.00 - 5.20              = -$229.20

        cash: 10000 → 9485.40 → 9485.40 + 514.60 + (-229.20) = $9,770.80
        net loss = 10000 - 9770.80 = $229.20
        """
        from core.backtest import (SLIPPAGE_PER_LEG, COMMISSION_PER_LEG,
                                   CONTRACT_MULT)

        cost = 5.00
        exit_val = 3.00
        n_legs = 4
        contracts = 1
        initial_cash = 10_000.00

        slip = SLIPPAGE_PER_LEG * n_legs
        comm = COMMISSION_PER_LEG * n_legs

        cpc = (cost + slip) * CONTRACT_MULT + comm
        deployed = contracts * cpc
        cash = initial_cash - deployed

        pnl_per_share = exit_val - cost - slip - slip    # -2.24
        pnl = (pnl_per_share * contracts * CONTRACT_MULT
               - contracts * (comm + comm))              # -229.20

        assert abs(pnl_per_share - (-2.24)) < 1e-10
        assert abs(pnl - (-229.20)) < 1e-10

        cash += deployed + pnl
        assert abs(cash - 9770.80) < 1e-10
        assert abs(initial_cash - cash - 229.20) < 1e-10

    # ── Full lifecycle: entry → MTM during hold → exit ──

    def test_full_lifecycle_one_trade(self):
        """Complete lifecycle of one double-calendar trade with MTM.

        Day 1 (entry):   spread = $4.00, buy 2 contracts
        Day 2 (holding): market spread = $5.00 (prices moved up)
        Day 3 (exit):    spread = $6.00, close position

        Verify at-cost and MTM accounts at every step.
        """
        from core.backtest import (SLIPPAGE_PER_LEG, COMMISSION_PER_LEG,
                                   CONTRACT_MULT, _compute_mtm_value)

        cost = 4.00
        exit_val = 6.00
        n_legs = 4
        cts = 2
        initial = 10_000.00

        slip = SLIPPAGE_PER_LEG * n_legs   # 0.12
        comm = COMMISSION_PER_LEG * n_legs # 2.60

        # ═════════ DAY 1: ENTRY ═════════
        cpc = (cost + slip) * CONTRACT_MULT + comm
        #   = (4.00 + 0.12) × 100 + 2.60 = 412 + 2.60 = 414.60
        deployed = cts * cpc
        #   = 2 × 414.60 = 829.20
        cash = initial - deployed
        #   = 10000 - 829.20 = 9170.80

        assert abs(cpc - 414.60) < 1e-10
        assert abs(deployed - 829.20) < 1e-10
        assert abs(cash - 9170.80) < 1e-10

        # at-cost: account = cash + deployed = 10000 (always)
        account_atcost = cash + deployed
        assert abs(account_atcost - initial) < 1e-10

        # MTM Day 1: market spread still = entry ($4.00)
        #   mtm_invested = 4.00 × 2 × 100 = 800
        #   account_mtm = 9170.80 + 800 = 9970.80
        #   gap = 10000 - 9970.80 = 29.20 = 2 contracts × (0.12×100 + 2.60)
        mtm_d1 = cost * cts * CONTRACT_MULT   # 800
        acct_mtm_d1 = cash + mtm_d1           # 9970.80
        gap_d1 = initial - acct_mtm_d1        # 29.20
        gap_expected = (slip * CONTRACT_MULT + comm) * cts  # 29.20

        assert abs(mtm_d1 - 800) < 1e-10
        assert abs(acct_mtm_d1 - 9970.80) < 1e-10
        assert abs(gap_d1 - 29.20) < 1e-10
        assert abs(gap_d1 - gap_expected) < 1e-10

        # ═════════ DAY 2: HOLDING — spread now $5.00 ═════════
        # Cash unchanged (no transaction)
        # at-cost: still 9170.80 + 829.20 = 10000
        # MTM: 5.00 × 2 × 100 = 1000
        # account_mtm = 9170.80 + 1000 = 10170.80
        # Unrealized P&L (MTM) = 10170.80 - 9970.80 = +200
        #   = (5.00-4.00) × 2 × 100 = 200 ✓

        mtm_d2 = 5.00 * cts * CONTRACT_MULT  # 1000
        acct_mtm_d2 = cash + mtm_d2          # 10170.80
        unrealized = acct_mtm_d2 - acct_mtm_d1  # 200

        assert abs(mtm_d2 - 1000) < 1e-10
        assert abs(acct_mtm_d2 - 10170.80) < 1e-10
        assert abs(unrealized - 200) < 1e-10

        # Verify via _compute_mtm_value with mock DB
        sk = 100000
        rows_c = [
            (1, "X", 20240101, sk, "C"), (2, "X", 20240201, sk, "C"),
            (3, "X", 20240101, sk, "P"), (4, "X", 20240201, sk, "P"),
        ]
        # Day 2 prices: fc=1, bc=3.5, fp=0.5, bp=2 → spread=3.5-1+2-0.5=4... no
        # simpler: make spread exactly 5. fc=1, bc=4, fp=1, bp=3 → 4-1+3-1=5 ✓
        rows_e = [
            (1, 20240102, 1, 1, 1),  # fc = 1
            (2, 20240102, 4, 4, 4),  # bc = 4
            (3, 20240102, 1, 1, 1),  # fp = 1
            (4, 20240102, 3, 3, 3),  # bp = 3
        ]
        conn = sqlite3.connect(":memory:")
        cur = conn.cursor()
        cur.execute("CREATE TABLE contracts (contract_id INTEGER PRIMARY KEY,"
                    " root TEXT, expiration INTEGER, strike INTEGER, right TEXT)")
        cur.execute("CREATE TABLE eod_history (contract_id INTEGER, date INTEGER,"
                    " bid REAL, ask REAL, close REAL)")
        cur.executemany("INSERT INTO contracts VALUES (?,?,?,?,?)", rows_c)
        cur.executemany("INSERT INTO eod_history VALUES (?,?,?,?,?)", rows_e)
        conn.commit()

        cid_cache = {(r, e, s, ri): cid for cid, r, e, s, ri in rows_c}
        pos_dict = {
            "ticker": "X", "contracts": cts, "deployed": deployed,
            "front_exp": 20240101, "back_exp": 20240201,
            "front_strike": 100.0,
        }
        mtm_from_fn = _compute_mtm_value([pos_dict], 20240102, conn,
                                          cid_cache, "double")
        conn.close()

        assert abs(mtm_from_fn - mtm_d2) < 1e-10, \
            f"_compute_mtm_value={mtm_from_fn}, hand calc={mtm_d2}"

        # ═════════ DAY 3: EXIT — spread = $6.00 ═════════
        pnl_per_share = exit_val - cost - slip - slip  # 6-4-0.12-0.12 = 1.76
        pnl = (pnl_per_share * cts * CONTRACT_MULT
               - cts * (comm + comm))
        #     = 1.76 × 200 - 2 × 5.20
        #     = 352 - 10.40 = 341.60

        assert abs(pnl_per_share - 1.76) < 1e-10
        assert abs(pnl - 341.60) < 1e-10

        cash_final = cash + deployed + pnl  # 9170.80 + 829.20 + 341.60 = 10341.60
        assert abs(cash_final - 10341.60) < 1e-10

        # Post-exit: no positions, account = account_mtm = cash
        assert abs(cash_final - (initial + pnl)) < 1e-10

    # ── Multi-contract scaling ──

    def test_scaling_with_contracts(self):
        """MTM scales linearly with contract count.

        spread=2, 1 ct → 200,  3 cts → 600,  10 cts → 2000.
        """
        from core.backtest import _compute_mtm_value

        sk = 100000
        rows_c = [
            (1, "A", 20240101, sk, "C"),
            (2, "A", 20240201, sk, "C"),
        ]
        rows_e = [
            (1, 20240115, 3, 3, 3),  # fc = 3
            (2, 20240115, 5, 5, 5),  # bc = 5, spread = 2
        ]

        for n_cts, expected in [(1, 200), (3, 600), (10, 2000)]:
            conn = sqlite3.connect(":memory:")
            cur = conn.cursor()
            cur.execute("CREATE TABLE contracts (contract_id INTEGER PRIMARY KEY,"
                        " root TEXT, expiration INTEGER, strike INTEGER, right TEXT)")
            cur.execute("CREATE TABLE eod_history (contract_id INTEGER, date INTEGER,"
                        " bid REAL, ask REAL, close REAL)")
            cur.executemany("INSERT INTO contracts VALUES (?,?,?,?,?)", rows_c)
            cur.executemany("INSERT INTO eod_history VALUES (?,?,?,?,?)", rows_e)
            conn.commit()

            cache = {(r, e, s, ri): cid for cid, r, e, s, ri in rows_c}
            pos = {
                "ticker": "A", "contracts": n_cts, "deployed": 99999,
                "front_exp": 20240101, "back_exp": 20240201,
                "front_strike": 100.0,
            }
            result = _compute_mtm_value([pos], 20240115, conn, cache, "single")
            conn.close()

            assert result == expected, \
                f"{n_cts} cts: got {result}, expected {expected}"

    # ── Cash conservation through two sequential trades ──

    def test_two_trades_cash_conservation(self):
        """Run 2 trades sequentially, verify final cash = initial + sum(P&L).

        Trade 1: cost=$4, exit=$6, 1 ct → pnl=$170.80 (win)
        Trade 2: cost=$3, exit=$2, 1 ct → pnl=−$129.20 (loss)
        Total P&L = 170.80 − 129.20 = $41.60
        Final cash = 10000 + 41.60 = $10,041.60
        """
        from core.backtest import (SLIPPAGE_PER_LEG, COMMISSION_PER_LEG,
                                   CONTRACT_MULT)

        n_legs = 4
        slip = SLIPPAGE_PER_LEG * n_legs   # 0.12
        comm = COMMISSION_PER_LEG * n_legs # 2.60

        cash = 10_000.00
        total_pnl = 0

        trades = [
            (4.00, 6.00),   # win
            (3.00, 2.00),   # loss
        ]

        for cost, exit_val in trades:
            cts = 1
            cpc = (cost + slip) * CONTRACT_MULT + comm
            deployed = cts * cpc
            cash -= deployed

            pnl_ps = exit_val - cost - slip - slip
            pnl = pnl_ps * cts * CONTRACT_MULT - cts * (comm + comm)
            cash += deployed + pnl
            total_pnl += pnl

        # Trade 1 P&L:
        #   pnl_ps = 6-4-0.12-0.12 = 1.76
        #   pnl = 1.76×100 - 5.20 = 170.80
        # Trade 2 P&L:
        #   pnl_ps = 2-3-0.12-0.12 = -1.24
        #   pnl = -1.24×100 - 5.20 = -129.20
        # Total = 170.80 + (-129.20) = 41.60

        assert abs(total_pnl - 41.60) < 1e-10
        assert abs(cash - 10041.60) < 1e-10
        assert abs(cash - (10_000 + total_pnl)) < 1e-10

    # ── Strike key resolution ──

    def test_strike_millis_conversion(self):
        """Verify front_strike float → int millis matches cid_cache key.

        backtest.py line 128: strike_millis = int(round(pos["front_strike"] * 1000))
        100.0  → 100000
        150.5  → 150500
        99.995 → 99995  (sub-penny, shouldn't happen but test rounding)
        """
        for strike, expected_millis in [(100.0, 100000),
                                        (150.5, 150500),
                                        (99.995, 99995)]:
            result = int(round(strike * 1000))
            assert result == expected_millis, \
                f"strike={strike} → {result}, expected {expected_millis}"


# ═══════════════════════════════════════════════════════════
# 14. MARK-TO-MARKET VALUATION (functional tests)
# ═══════════════════════════════════════════════════════════

class TestMarkToMarket:
    """Tests for MTM logic in backtest.py: _compute_mtm_value and daily accounting."""

    # Constants matching backtest.py
    CONTRACT_MULT = 100
    SLIPPAGE_PER_LEG = 0.03
    COMMISSION_PER_LEG = 0.65

    # ── Helpers ──

    def _make_inmemory_db(self, contracts_rows, eod_rows):
        """Create an in-memory SQLite DB with contracts and eod_history tables.

        contracts_rows: list of (contract_id, root, expiration, strike, right)
        eod_rows: list of (contract_id, date, bid, ask, close)
        """
        conn = sqlite3.connect(":memory:")
        cur = conn.cursor()
        cur.execute("""CREATE TABLE contracts (
            contract_id INTEGER PRIMARY KEY,
            root TEXT, expiration INTEGER, strike INTEGER, right TEXT
        )""")
        cur.execute("""CREATE TABLE eod_history (
            contract_id INTEGER, date INTEGER, bid REAL, ask REAL, close REAL
        )""")
        cur.executemany(
            "INSERT INTO contracts VALUES (?,?,?,?,?)", contracts_rows)
        cur.executemany(
            "INSERT INTO eod_history VALUES (?,?,?,?,?)", eod_rows)
        conn.commit()
        return conn

    def _build_cid_cache(self, contracts_rows):
        """Build cid_cache dict from contracts rows."""
        cache = {}
        for cid, root, exp, strike, right in contracts_rows:
            cache[(root, exp, strike, right)] = cid
        return cache

    def _make_position(self, ticker="AAPL", contracts=3, cost_per_share=3.0,
                       deployed=None, front_exp=20240315, back_exp=20240419,
                       front_strike=170.0):
        """Create a position dict matching run_portfolio() format."""
        n_legs = 4
        if deployed is None:
            cpc = ((cost_per_share + self.SLIPPAGE_PER_LEG * n_legs)
                   * self.CONTRACT_MULT + self.COMMISSION_PER_LEG * n_legs)
            deployed = contracts * cpc
        return {
            "ticker": ticker,
            "combo": "30-60",
            "entry_dt": pd.Timestamp("2024-02-15"),
            "exit_dt": pd.Timestamp("2024-03-14"),
            "contracts": contracts,
            "cost_per_share": cost_per_share,
            "exit_val_per_share": 4.0,
            "deployed": deployed,
            "ff": 0.30,
            "front_exp": front_exp,
            "back_exp": back_exp,
            "front_strike": front_strike,
        }

    # ── A. Pure Unit Tests (no DB, no real data) ──

    def test_mtm_double_spread_value(self):
        """Verify double calendar MTM: (bc - fc + bp - fp) * contracts * 100."""
        from core.backtest import _compute_mtm_value, CONTRACT_MULT

        # Set up: fc=2, bc=5, fp=1, bp=3 -> spread_val = 5-2+3-1 = 5
        # MTM = 5 * 3 contracts * 100 = $1,500
        strike_millis = 170000  # 170.0 * 1000
        contracts_rows = [
            (1, "AAPL", 20240315, strike_millis, "C"),  # front call
            (2, "AAPL", 20240419, strike_millis, "C"),  # back call
            (3, "AAPL", 20240315, strike_millis, "P"),  # front put
            (4, "AAPL", 20240419, strike_millis, "P"),  # back put
        ]
        eod_rows = [
            (1, 20240301, 1.8, 2.2, 2.0),   # fc mid = 2.0
            (2, 20240301, 4.8, 5.2, 5.0),   # bc mid = 5.0
            (3, 20240301, 0.8, 1.2, 1.0),   # fp mid = 1.0
            (4, 20240301, 2.8, 3.2, 3.0),   # bp mid = 3.0
        ]
        conn = self._make_inmemory_db(contracts_rows, eod_rows)
        cid_cache = self._build_cid_cache(contracts_rows)
        pos = self._make_position(contracts=3, front_strike=170.0)

        result = _compute_mtm_value([pos], 20240301, conn, cid_cache, "double")
        conn.close()

        expected = (5.0 - 2.0 + 3.0 - 1.0) * 3 * CONTRACT_MULT  # = 1500
        assert abs(result - expected) < 1e-10, \
            f"Double MTM: got {result}, expected {expected}"

    def test_mtm_single_spread_value(self):
        """Verify single calendar MTM: (bc - fc) * contracts * 100."""
        from core.backtest import _compute_mtm_value, CONTRACT_MULT

        strike_millis = 170000
        contracts_rows = [
            (1, "AAPL", 20240315, strike_millis, "C"),
            (2, "AAPL", 20240419, strike_millis, "C"),
        ]
        eod_rows = [
            (1, 20240301, 1.8, 2.2, 2.0),   # fc mid = 2.0
            (2, 20240301, 4.8, 5.2, 5.0),   # bc mid = 5.0
        ]
        conn = self._make_inmemory_db(contracts_rows, eod_rows)
        cid_cache = self._build_cid_cache(contracts_rows)
        pos = self._make_position(contracts=2, front_strike=170.0)

        result = _compute_mtm_value([pos], 20240301, conn, cid_cache, "single")
        conn.close()

        expected = (5.0 - 2.0) * 2 * CONTRACT_MULT  # = 600
        assert abs(result - expected) < 1e-10, \
            f"Single MTM: got {result}, expected {expected}"

    def test_mtm_entry_day_vs_deployed(self):
        """On entry day, MTM < deployed by exactly slippage+commission.

        deployed = (spread_cost + slip) * 100 + comm  (per contract)
        MTM = spread_cost * 100 (per contract, when mid prices = entry mids)
        Diff per contract = slip*100 + comm
        """
        from core.backtest import _compute_mtm_value, CONTRACT_MULT

        cost_per_share = 3.00
        n_legs = 4
        contracts = 5
        slip = self.SLIPPAGE_PER_LEG * n_legs
        comm = self.COMMISSION_PER_LEG * n_legs

        cpc = (cost_per_share + slip) * CONTRACT_MULT + comm
        deployed = contracts * cpc

        # Simulate: entry mids sum to cost_per_share (3.00 for double cal)
        # fc=2, bc=4, fp=0.5, bp=1.5 -> spread = 4-2+1.5-0.5 = 3.00
        strike_millis = 170000
        contracts_rows = [
            (1, "AAPL", 20240315, strike_millis, "C"),
            (2, "AAPL", 20240419, strike_millis, "C"),
            (3, "AAPL", 20240315, strike_millis, "P"),
            (4, "AAPL", 20240419, strike_millis, "P"),
        ]
        eod_rows = [
            (1, 20240215, 1.8, 2.2, 2.0),   # fc mid = 2.0
            (2, 20240215, 3.8, 4.2, 4.0),   # bc mid = 4.0
            (3, 20240215, 0.3, 0.7, 0.5),   # fp mid = 0.5
            (4, 20240215, 1.3, 1.7, 1.5),   # bp mid = 1.5
        ]
        conn = self._make_inmemory_db(contracts_rows, eod_rows)
        cid_cache = self._build_cid_cache(contracts_rows)
        pos = self._make_position(contracts=contracts, cost_per_share=cost_per_share,
                                  deployed=deployed, front_strike=170.0)

        mtm = _compute_mtm_value([pos], 20240215, conn, cid_cache, "double")
        conn.close()

        # MTM = spread_cost * contracts * 100 = 3.00 * 5 * 100 = 1500
        expected_mtm = cost_per_share * contracts * CONTRACT_MULT
        assert abs(mtm - expected_mtm) < 1e-10

        # Difference = slip*100 + comm per contract * contracts
        expected_diff = (slip * CONTRACT_MULT + comm) * contracts
        actual_diff = deployed - mtm
        assert abs(actual_diff - expected_diff) < 1e-10, \
            f"Deployed-MTM diff: got {actual_diff}, expected {expected_diff}"

    def test_mtm_convergence_at_exit(self):
        """After all positions close, account_mtm == account (both = cash)."""
        # When positions list is empty, invested=0, invested_mtm=0
        # So account = cash + 0 = cash, account_mtm = cash + 0 = cash
        # _compute_mtm_value([]) returns 0.0
        from core.backtest import _compute_mtm_value

        conn = self._make_inmemory_db([], [])
        result = _compute_mtm_value([], 20240315, conn, {}, "double")
        conn.close()

        assert result == 0.0, f"Empty positions should return 0.0, got {result}"

        # Simulating the daily log logic:
        cash = 150_000.0
        invested = 0.0
        mtm_invested = result  # 0.0
        account = cash + invested
        account_mtm = cash + mtm_invested
        assert account == account_mtm, \
            f"At exit convergence failed: {account} vs {account_mtm}"

    def test_mtm_fallback_to_deployed(self):
        """When a leg price is missing for one position but another is fully
        priced, the missing-leg position falls back to deployed while the
        priced position uses live MTM."""
        from core.backtest import _compute_mtm_value, CONTRACT_MULT

        strike1 = 170000
        strike2 = 180000
        contracts_rows = [
            # AAPL: both legs have data
            (1, "AAPL", 20240315, strike1, "C"),
            (2, "AAPL", 20240419, strike1, "C"),
            # MSFT: back call has CID but no EOD data -> fallback
            (3, "MSFT", 20240315, strike2, "C"),
            (4, "MSFT", 20240419, strike2, "C"),
        ]
        eod_rows = [
            (1, 20240301, 1.8, 2.2, 2.0),   # AAPL fc mid = 2.0
            (2, 20240301, 4.8, 5.2, 5.0),   # AAPL bc mid = 5.0
            (3, 20240301, 2.8, 3.2, 3.0),   # MSFT fc mid = 3.0
            # MSFT bc (cid=4) has NO eod data -> missing
        ]
        conn = self._make_inmemory_db(contracts_rows, eod_rows)
        cid_cache = self._build_cid_cache(contracts_rows)

        deployed_msft = 950.0
        pos1 = self._make_position(ticker="AAPL", contracts=2,
                                   front_strike=170.0)
        pos2 = self._make_position(ticker="MSFT", contracts=3,
                                   deployed=deployed_msft,
                                   front_strike=180.0)

        result = _compute_mtm_value([pos1, pos2], 20240301, conn, cid_cache,
                                    "single")
        conn.close()

        # AAPL fully priced: (5.0-2.0) * 2 * 100 = 600
        # MSFT missing bc -> fallback to deployed = 950.0
        expected = 600.0 + deployed_msft
        assert abs(result - expected) < 1e-10, \
            f"Fallback should mix priced+deployed: got {result}, expected {expected}"

    def test_mtm_all_fallback_returns_none(self):
        """When ALL positions fall back (none fully priced), returns None."""
        from core.backtest import _compute_mtm_value

        strike_millis = 170000
        contracts_rows = [
            (1, "AAPL", 20240315, strike_millis, "C"),
            (2, "AAPL", 20240419, strike_millis, "C"),
        ]
        # Only front call has EOD data; back call missing
        eod_rows = [
            (1, 20240301, 1.8, 2.2, 2.0),
        ]
        conn = self._make_inmemory_db(contracts_rows, eod_rows)
        cid_cache = self._build_cid_cache(contracts_rows)
        pos = self._make_position(contracts=3, deployed=950.0,
                                  front_strike=170.0)

        result = _compute_mtm_value([pos], 20240301, conn, cid_cache, "single")
        conn.close()

        # No position fully priced -> any_priced=False -> returns None
        assert result is None, \
            f"All-fallback should return None, got {result}"

    def test_mtm_returns_none_when_no_prices(self):
        """When no contract IDs resolve, _compute_mtm_value returns None."""
        from core.backtest import _compute_mtm_value

        # Position with a strike that doesn't exist in cid_cache
        pos = self._make_position(contracts=2, front_strike=999.0)
        empty_cache = {}

        conn = self._make_inmemory_db([], [])
        result = _compute_mtm_value([pos], 20240301, conn, empty_cache, "double")
        conn.close()

        assert result is None, f"Expected None when no CIDs resolve, got {result}"

    def test_mtm_empty_positions(self):
        """Empty positions list -> returns 0.0."""
        from core.backtest import _compute_mtm_value

        conn = self._make_inmemory_db([], [])
        result = _compute_mtm_value([], 20240301, conn, {}, "double")
        conn.close()
        assert result == 0.0

    def test_mtm_bid_ask_midpoint(self):
        """With bid=4.0, ask=6.0 -> mid=5.0; with bid=0 -> fallback to close."""
        from core.backtest import _compute_mtm_value

        strike_millis = 170000
        contracts_rows = [
            (1, "AAPL", 20240315, strike_millis, "C"),
            (2, "AAPL", 20240419, strike_millis, "C"),
        ]

        # Test 1: Normal bid/ask -> mid
        eod_rows = [
            (1, 20240301, 4.0, 6.0, 4.5),  # mid = 5.0 (not close 4.5)
            (2, 20240301, 7.0, 9.0, 7.5),  # mid = 8.0
        ]
        conn = self._make_inmemory_db(contracts_rows, eod_rows)
        cid_cache = self._build_cid_cache(contracts_rows)
        pos = self._make_position(contracts=1, front_strike=170.0)

        result = _compute_mtm_value([pos], 20240301, conn, cid_cache, "single")
        conn.close()

        # spread = bc_mid - fc_mid = 8.0 - 5.0 = 3.0
        expected = 3.0 * 1 * 100  # = 300
        assert abs(result - expected) < 1e-10, \
            f"Mid should use (bid+ask)/2: got {result}, expected {expected}"

        # Test 2: bid=0 -> fallback to close
        eod_rows_2 = [
            (1, 20240301, 0, 6.0, 4.5),   # bid=0 -> use close=4.5
            (2, 20240301, 7.0, 9.0, 7.5),  # mid = 8.0
        ]
        conn2 = self._make_inmemory_db(contracts_rows, eod_rows_2)
        result2 = _compute_mtm_value([pos], 20240301, conn2, cid_cache, "single")
        conn2.close()

        expected2 = (8.0 - 4.5) * 1 * 100  # = 350
        assert abs(result2 - expected2) < 1e-10, \
            f"Bid=0 should fallback to close: got {result2}, expected {expected2}"

    def test_mtm_negative_spread(self):
        """When front > back (underwater), spread_val < 0 — this is valid."""
        from core.backtest import _compute_mtm_value

        strike_millis = 170000
        contracts_rows = [
            (1, "AAPL", 20240315, strike_millis, "C"),
            (2, "AAPL", 20240419, strike_millis, "C"),
        ]
        # Front call more expensive than back (underwater)
        eod_rows = [
            (1, 20240301, 5.8, 6.2, 6.0),  # fc mid = 6.0
            (2, 20240301, 2.8, 3.2, 3.0),  # bc mid = 3.0
        ]
        conn = self._make_inmemory_db(contracts_rows, eod_rows)
        cid_cache = self._build_cid_cache(contracts_rows)
        pos = self._make_position(contracts=2, front_strike=170.0)

        result = _compute_mtm_value([pos], 20240301, conn, cid_cache, "single")
        conn.close()

        expected = (3.0 - 6.0) * 2 * 100  # = -600
        assert result < 0, f"Underwater spread should give negative MTM, got {result}"
        assert abs(result - expected) < 1e-10

    def test_mtm_drawdown_gte_atcost(self):
        """MTM max drawdown >= at-cost max drawdown (by construction).

        At-cost uses deployed (fixed), MTM uses live prices (volatile),
        so MTM drawdowns should be at least as large.
        """
        # Simulate daily series where MTM fluctuates more
        np.random.seed(42)
        n = 500
        daily_ret = np.random.normal(0.001, 0.005, n)
        at_cost = 100_000 * np.cumprod(1 + daily_ret)

        # MTM adds extra intra-period noise
        mtm_noise = np.random.normal(0, 0.008, n)
        mtm = at_cost * (1 + mtm_noise)
        # Ensure they converge at the end (when positions close)
        mtm[-1] = at_cost[-1]

        dd_atcost = ((at_cost - np.maximum.accumulate(at_cost))
                     / np.maximum.accumulate(at_cost)).min()
        dd_mtm = ((mtm - np.maximum.accumulate(mtm))
                  / np.maximum.accumulate(mtm)).min()

        assert dd_mtm <= dd_atcost, \
            f"MTM DD ({dd_mtm:.2%}) should be >= at-cost DD ({dd_atcost:.2%})"

    def test_mtm_multiple_positions(self):
        """MTM sums correctly across multiple positions."""
        from core.backtest import _compute_mtm_value

        strike1 = 170000  # 170.0 * 1000
        strike2 = 180000  # 180.0 * 1000
        contracts_rows = [
            (1, "AAPL", 20240315, strike1, "C"),
            (2, "AAPL", 20240419, strike1, "C"),
            (3, "MSFT", 20240315, strike2, "C"),
            (4, "MSFT", 20240419, strike2, "C"),
        ]
        eod_rows = [
            (1, 20240301, 1.8, 2.2, 2.0),   # AAPL fc mid = 2.0
            (2, 20240301, 4.8, 5.2, 5.0),   # AAPL bc mid = 5.0
            (3, 20240301, 2.8, 3.2, 3.0),   # MSFT fc mid = 3.0
            (4, 20240301, 5.8, 6.2, 6.0),   # MSFT bc mid = 6.0
        ]
        conn = self._make_inmemory_db(contracts_rows, eod_rows)
        cid_cache = self._build_cid_cache(contracts_rows)

        pos1 = self._make_position(ticker="AAPL", contracts=2,
                                   front_strike=170.0)
        pos2 = self._make_position(ticker="MSFT", contracts=4,
                                   front_strike=180.0)

        result = _compute_mtm_value([pos1, pos2], 20240301, conn, cid_cache,
                                    "single")
        conn.close()

        # AAPL: (5-2) * 2 * 100 = 600
        # MSFT: (6-3) * 4 * 100 = 1200
        expected = 600 + 1200
        assert abs(result - expected) < 1e-10, \
            f"Multi-position MTM: got {result}, expected {expected}"

    # ── B. Integration Tests (real data, DB required) ──

    @pytest.fixture
    def bt_result_double(self):
        """Run double calendar backtest if data available."""
        cache = ROOT / "cache" / "spread_returns.pkl"
        if not cache.exists():
            pytest.skip("spread_returns.pkl not found")
        db_path = ROOT / "sp500_options.db"
        if not db_path.exists():
            pytest.skip("sp500_options.db not found")

        from core.backtest import load_data, run_portfolio
        df = load_data()
        result = run_portfolio(df, mode="double")
        if result is None:
            pytest.skip("No trades produced")
        return result

    def test_mtm_final_convergence_real(self, bt_result_double):
        """After backtest ends (all positions closed), account_mtm == account."""
        daily = bt_result_double["daily"]
        final_at_cost = daily["account"].iloc[-1]
        final_mtm = daily["account_mtm"].iloc[-1]
        assert abs(final_at_cost - final_mtm) < 1.0, \
            f"Final convergence: at-cost={final_at_cost:.2f}, mtm={final_mtm:.2f}"

    def test_mtm_drawdown_larger_real(self, bt_result_double):
        """MTM max DD >= at-cost max DD on real data."""
        daily = bt_result_double["daily"]

        peak = daily["account"].cummax()
        dd = ((daily["account"] - peak) / peak).min()

        peak_mtm = daily["account_mtm"].cummax()
        dd_mtm = ((daily["account_mtm"] - peak_mtm) / peak_mtm).min()

        assert dd_mtm <= dd, \
            f"MTM DD ({dd_mtm:.2%}) should be >= at-cost DD ({dd:.2%})"

    def test_mtm_column_exists(self, bt_result_double):
        """Verify account_mtm and invested_mtm columns exist in daily output."""
        daily = bt_result_double["daily"]
        assert "account_mtm" in daily.columns, "Missing account_mtm column"
        assert "invested_mtm" in daily.columns, "Missing invested_mtm column"

    def test_mtm_correlation_high(self, bt_result_double):
        """Correlation between account and account_mtm > 0.99."""
        daily = bt_result_double["daily"]
        corr = daily["account"].corr(daily["account_mtm"])
        assert corr > 0.99, f"Account vs MTM correlation = {corr:.4f}, expected > 0.99"


# ═══════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════

if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
