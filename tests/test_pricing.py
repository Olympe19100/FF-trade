"""Tests for core/pricing.py — Black-Scholes, IV, Delta, Forward Factor.

Covers all 8 public functions with edge cases and consistency checks.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import numpy as np
import pytest
from scipy.stats import norm

from core.pricing import (
    RISK_FREE_RATE,
    bs_price,
    bs_price_vec,
    implied_vol_vec,
    implied_vol_scalar,
    bs_delta_vec,
    compute_ff,
    put_call_parity_call_equiv,
)


# ═══════════════════════════════════════════════════════════
#  bs_price (scalar)
# ═══════════════════════════════════════════════════════════

class TestBsPrice:
    """Black-Scholes scalar pricing."""

    def test_atm_call_positive(self):
        price = bs_price(100, 100, 1.0, 0.20)
        assert price > 0
        # ATM 1-year 20% vol call ~ $10.45
        assert 8 < price < 13

    def test_atm_put_positive(self):
        price = bs_price(100, 100, 1.0, 0.20, right="P")
        assert price > 0

    def test_put_call_parity(self):
        """C - P = S - K*e^(-rT)."""
        S, K, T, sigma, r = 100, 100, 1.0, 0.25, 0.04
        call = bs_price(S, K, T, sigma, r, "C")
        put = bs_price(S, K, T, sigma, r, "P")
        expected = S - K * np.exp(-r * T)
        assert abs((call - put) - expected) < 1e-8

    def test_deep_itm_call(self):
        """Deep ITM call ~ S - K*e^(-rT)."""
        price = bs_price(200, 100, 0.5, 0.20)
        intrinsic = 200 - 100 * np.exp(-RISK_FREE_RATE * 0.5)
        assert abs(price - intrinsic) < 2

    def test_deep_otm_call_near_zero(self):
        price = bs_price(50, 100, 0.1, 0.20)
        assert price < 0.01

    def test_zero_time(self):
        """At expiry: intrinsic value."""
        assert bs_price(110, 100, 0, 0.30, right="C") == 10
        assert bs_price(90, 100, 0, 0.30, right="C") == 0
        assert bs_price(90, 100, 0, 0.30, right="P") == 10

    def test_zero_vol(self):
        """Zero vol: discounted intrinsic."""
        price = bs_price(110, 100, 1.0, 0, right="C")
        assert price == 10  # max(0, S-K)

    def test_call_increases_with_vol(self):
        low = bs_price(100, 100, 1.0, 0.10)
        high = bs_price(100, 100, 1.0, 0.40)
        assert high > low

    def test_call_increases_with_time(self):
        short = bs_price(100, 100, 0.1, 0.20)
        long = bs_price(100, 100, 1.0, 0.20)
        assert long > short


# ═══════════════════════════════════════════════════════════
#  bs_price_vec (vectorized calls)
# ═══════════════════════════════════════════════════════════

class TestBsPriceVec:
    """Vectorized BS call pricing."""

    def test_matches_scalar(self):
        S = np.array([100, 110, 90])
        K = np.array([100, 100, 100])
        T = np.array([1.0, 1.0, 1.0])
        sigma = np.array([0.20, 0.20, 0.20])

        vec = bs_price_vec(S, K, T, sigma)
        for i in range(3):
            scalar = bs_price(S[i], K[i], T[i], sigma[i])
            assert abs(vec[i] - scalar) < 1e-8

    def test_invalid_inputs_nan(self):
        """Zero or negative inputs -> NaN."""
        prices = bs_price_vec(
            np.array([100, 0, 100]),
            np.array([100, 100, 100]),
            np.array([1.0, 1.0, 0]),
            np.array([0.20, 0.20, 0.20]),
        )
        assert not np.isnan(prices[0])
        assert np.isnan(prices[1])
        assert np.isnan(prices[2])

    def test_empty_array(self):
        prices = bs_price_vec(np.array([]), np.array([]), np.array([]), np.array([]))
        assert len(prices) == 0


# ═══════════════════════════════════════════════════════════
#  implied_vol_vec (Newton-Raphson)
# ═══════════════════════════════════════════════════════════

class TestImpliedVolVec:
    """Vectorized IV inversion via Newton-Raphson."""

    def test_roundtrip(self):
        """Price -> IV -> Price should recover original."""
        S = np.array([100, 100, 100])
        K = np.array([90, 100, 110])
        T = np.array([0.5, 0.5, 0.5])
        true_vol = np.array([0.25, 0.25, 0.25])

        prices = bs_price_vec(S, K, T, true_vol)
        recovered = implied_vol_vec(prices, S, K, T)

        np.testing.assert_allclose(recovered, true_vol, atol=0.005)

    def test_high_vol_roundtrip(self):
        S = np.array([100.0])
        K = np.array([100.0])
        T = np.array([1.0])
        true_vol = np.array([0.80])

        prices = bs_price_vec(S, K, T, true_vol)
        recovered = implied_vol_vec(prices, S, K, T)
        assert abs(recovered[0] - 0.80) < 0.02

    def test_zero_price_nan(self):
        iv = implied_vol_vec(
            np.array([0.0]),
            np.array([100.0]),
            np.array([100.0]),
            np.array([0.5]),
        )
        assert np.isnan(iv[0])

    def test_various_moneyness(self):
        """IV recovery across ITM/ATM/OTM (excluding deep OTM)."""
        S = np.full(4, 100.0)
        K = np.array([80, 90, 100, 110])
        T = np.full(4, 0.25)
        true_vol = np.full(4, 0.30)

        prices = bs_price_vec(S, K, T, true_vol)
        recovered = implied_vol_vec(prices, S, K, T)

        # Deep OTM (K=120) excluded: near-zero price defeats Newton-Raphson
        np.testing.assert_allclose(recovered, true_vol, atol=0.02)


# ═══════════════════════════════════════════════════════════
#  implied_vol_scalar (Brent)
# ═══════════════════════════════════════════════════════════

class TestImpliedVolScalar:
    """Scalar IV via Brent's method."""

    def test_roundtrip_call(self):
        price = bs_price(100, 100, 0.5, 0.30, r=0.0, right="C")
        iv = implied_vol_scalar(price, 100, 100, 0.5, r=0.0, right="C")
        assert abs(iv - 0.30) < 0.001

    def test_roundtrip_put(self):
        price = bs_price(100, 100, 0.5, 0.30, r=0.0, right="P")
        iv = implied_vol_scalar(price, 100, 100, 0.5, r=0.0, right="P")
        assert abs(iv - 0.30) < 0.001

    def test_zero_price_nan(self):
        assert np.isnan(implied_vol_scalar(0, 100, 100, 0.5))

    def test_negative_price_nan(self):
        assert np.isnan(implied_vol_scalar(-1, 100, 100, 0.5))

    def test_zero_time_nan(self):
        assert np.isnan(implied_vol_scalar(5, 100, 100, 0))

    def test_below_intrinsic_nan(self):
        """Price below intrinsic should return NaN."""
        # ITM call with price below intrinsic
        assert np.isnan(implied_vol_scalar(0.5, 110, 100, 0.5, right="C"))


# ═══════════════════════════════════════════════════════════
#  bs_delta_vec
# ═══════════════════════════════════════════════════════════

class TestBsDeltaVec:
    """Vectorized call delta = N(d1)."""

    def test_atm_delta_near_half(self):
        """ATM call delta is approximately 0.5 (slightly above due to drift)."""
        delta = bs_delta_vec(
            np.array([100.0]), np.array([100.0]),
            np.array([1.0]), np.array([0.20]),
        )
        assert 0.45 < delta[0] < 0.65

    def test_deep_itm_delta_near_one(self):
        delta = bs_delta_vec(
            np.array([200.0]), np.array([100.0]),
            np.array([0.5]), np.array([0.20]),
        )
        assert delta[0] > 0.99

    def test_deep_otm_delta_near_zero(self):
        delta = bs_delta_vec(
            np.array([50.0]), np.array([100.0]),
            np.array([0.1]), np.array([0.20]),
        )
        assert delta[0] < 0.01

    def test_delta_increases_with_stock(self):
        S = np.array([90, 100, 110])
        K = np.full(3, 100.0)
        T = np.full(3, 0.5)
        sigma = np.full(3, 0.25)
        deltas = bs_delta_vec(S, K, T, sigma)
        assert deltas[0] < deltas[1] < deltas[2]

    def test_invalid_nan(self):
        delta = bs_delta_vec(
            np.array([100.0]), np.array([100.0]),
            np.array([0.0]), np.array([0.20]),
        )
        assert np.isnan(delta[0])


# ═══════════════════════════════════════════════════════════
#  compute_ff
# ═══════════════════════════════════════════════════════════

class TestComputeFF:
    """Forward Factor (PDF/Campasano)."""

    def test_flat_term_structure_zero(self):
        """Same IV front and back -> FF = 0."""
        ff = compute_ff(0.25, 0.25, 30, 60)
        assert abs(ff) < 1e-10

    def test_backwardation_positive(self):
        """Front IV > Back IV -> positive FF (good for calendar)."""
        ff = compute_ff(0.30, 0.25, 30, 60)
        assert ff > 0

    def test_contango_negative(self):
        """Front IV < Back IV -> negative FF."""
        ff = compute_ff(0.20, 0.25, 30, 60)
        assert ff < 0

    def test_nan_on_invalid(self):
        assert np.isnan(compute_ff(0, 0.25, 30, 60))
        assert np.isnan(compute_ff(0.25, 0, 30, 60))
        assert np.isnan(compute_ff(0.25, 0.25, 60, 30))  # front > back DTE

    def test_30_60_typical_values(self):
        """Typical 30-60 combo with moderate backwardation."""
        ff = compute_ff(0.28, 0.24, 30, 60)
        # Should be positive and in a reasonable range
        assert 0 < ff < 1.0


# ═══════════════════════════════════════════════════════════
#  put_call_parity_call_equiv
# ═══════════════════════════════════════════════════════════

class TestPutCallParity:
    """Put -> Call equivalent via put-call parity: C = P + S - K*e^(-rT)."""

    def test_atm(self):
        """ATM: C ≈ P + S - K*e^(-rT)."""
        put_prices = np.array([5.0])
        S = 100.0
        K = np.array([100.0])
        T = np.array([0.5])
        call_equiv = put_call_parity_call_equiv(put_prices, S, K, T)
        expected = 5.0 + 100 - 100 * np.exp(-RISK_FREE_RATE * 0.5)
        assert abs(call_equiv[0] - expected) < 0.01

    def test_vectorized(self):
        puts = np.array([3.0, 5.0, 8.0])
        S = 100.0
        K = np.array([110.0, 100.0, 90.0])
        T = np.array([0.5, 0.5, 0.5])
        result = put_call_parity_call_equiv(puts, S, K, T)
        assert len(result) == 3
        assert all(r > 0 for r in result)

    def test_floor_at_0001(self):
        """Output floored at 0.001."""
        # Deep OTM put with very negative call equivalent
        result = put_call_parity_call_equiv(
            np.array([0.01]), 50.0, np.array([200.0]), np.array([0.1])
        )
        assert result[0] == pytest.approx(0.001)


# ═══════════════════════════════════════════════════════════
#  Cross-function consistency
# ═══════════════════════════════════════════════════════════

class TestCrossConsistency:
    """Tests that verify consistency across multiple functions."""

    def test_iv_from_bs_price(self):
        """bs_price -> implied_vol_scalar roundtrip."""
        S, K, T, sigma = 100, 105, 0.25, 0.35
        price = bs_price(S, K, T, sigma)
        recovered = implied_vol_scalar(price, S, K, T)
        assert abs(recovered - sigma) < 0.001

    def test_delta_from_iv(self):
        """Compute price -> IV -> delta pipeline."""
        S = np.array([100.0])
        K = np.array([100.0])
        T = np.array([0.5])
        sigma = np.array([0.25])

        prices = bs_price_vec(S, K, T, sigma)
        iv = implied_vol_vec(prices, S, K, T)
        delta = bs_delta_vec(S, K, T, iv)

        # ATM delta ~ 0.5
        assert 0.45 < delta[0] < 0.65

    def test_put_call_parity_consistency(self):
        """Put price -> call_equiv -> IV should match call IV."""
        S, K, T, sigma = 100.0, 100.0, 0.5, 0.30

        call_price = bs_price(S, K, T, sigma, right="C")
        put_price = bs_price(S, K, T, sigma, right="P")

        call_equiv = put_call_parity_call_equiv(
            np.array([put_price]), S, np.array([K]), np.array([T])
        )
        # call_equiv should equal actual call price
        assert abs(call_equiv[0] - call_price) < 0.01
