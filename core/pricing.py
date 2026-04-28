"""
Centralized Options Pricing — Black-Scholes, IV, Delta, Forward Factor

Single source of truth for all BS math used across the codebase.
Replaces duplicate implementations in scanner.py, straddle.py, spreads.py.
"""

import numpy as np
from scipy.stats import norm
from scipy.optimize import brentq


RISK_FREE_RATE: float = 0.04


# ═══════════════════════════════════════════════════════════════
#  Black-Scholes Pricing
# ═══════════════════════════════════════════════════════════════

def bs_price(
    S: float, K: float, T: float, sigma: float,
    r: float = RISK_FREE_RATE, right: str = "C",
) -> float:
    """Black-Scholes price for a European option (scalar).

    Args:
        S: Underlying price.
        K: Strike price.
        T: Time to expiry in years.
        sigma: Implied volatility.
        r: Risk-free rate.
        right: 'C' for call, 'P' for put.

    Returns:
        Option price.
    """
    if T <= 0 or sigma <= 0 or S <= 0:
        return max(0.0, (S - K) if right == "C" else (K - S))
    sqrt_T = np.sqrt(T)
    d1 = (np.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * sqrt_T)
    d2 = d1 - sigma * sqrt_T
    if right == "C":
        return float(S * norm.cdf(d1) - K * np.exp(-r * T) * norm.cdf(d2))
    else:
        return float(K * np.exp(-r * T) * norm.cdf(-d2) - S * norm.cdf(-d1))


def bs_price_vec(
    S: np.ndarray, K: np.ndarray, T: np.ndarray, sigma: np.ndarray,
    r: float = RISK_FREE_RATE,
) -> np.ndarray:
    """Vectorized Black-Scholes call price.

    All inputs are arrays of the same length. Returns call prices.
    """
    S = np.asarray(S, dtype=float)
    K = np.asarray(K, dtype=float)
    T = np.asarray(T, dtype=float)
    sigma = np.asarray(sigma, dtype=float)

    prices = np.full_like(S, np.nan)
    ok = (T > 0) & (S > 0) & (K > 0) & (sigma > 0)
    if not ok.any():
        return prices

    s, k, t, sig = S[ok], K[ok], T[ok], sigma[ok]
    sqrt_t = np.sqrt(t)
    d1 = (np.log(s / k) + (r + 0.5 * sig ** 2) * t) / (sig * sqrt_t)
    d2 = d1 - sig * sqrt_t
    prices[ok] = s * norm.cdf(d1) - k * np.exp(-r * t) * norm.cdf(d2)
    return prices


# ═══════════════════════════════════════════════════════════════
#  Implied Volatility
# ═══════════════════════════════════════════════════════════════

def implied_vol_vec(
    prices: np.ndarray, S: np.ndarray, K: np.ndarray, T: np.ndarray,
    r: float = RISK_FREE_RATE, n_iter: int = 8,
) -> np.ndarray:
    """Vectorized IV via Newton-Raphson with Brenner-Subrahmanyam start.

    Operates on call prices (use put-call parity for puts beforehand).
    """
    prices = np.asarray(prices, dtype=float)
    S = np.asarray(S, dtype=float)
    K = np.asarray(K, dtype=float)
    T = np.asarray(T, dtype=float)

    iv = np.full_like(prices, np.nan)
    ok = (T > 0) & (S > 0) & (K > 0) & (prices > 0)
    if not ok.any():
        return iv

    p, s, k, t = prices[ok], S[ok], K[ok], T[ok]

    # Brenner-Subrahmanyam ATM approx as starting guess
    sigma = p * np.sqrt(2 * np.pi / t) / s
    sigma = np.clip(sigma, 0.02, 3.0)
    sqrt_t = np.sqrt(t)

    for _ in range(n_iter):
        d1 = (np.log(s / k) + (r + 0.5 * sigma ** 2) * t) / (sigma * sqrt_t)
        d2 = d1 - sigma * sqrt_t
        bs = s * norm.cdf(d1) - k * np.exp(-r * t) * norm.cdf(d2)
        vega = s * sqrt_t * norm.pdf(d1)
        sigma = sigma - (bs - p) / (vega + 1e-10)
        sigma = np.clip(sigma, 0.02, 3.0)

    iv[ok] = sigma
    return iv


def implied_vol_scalar(
    price: float, S: float, K: float, T: float,
    r: float = RISK_FREE_RATE, right: str = "C",
) -> float:
    """Scalar IV via Brent's method (robust, for straddle use).

    Args:
        price: Observed option price.
        S, K, T, r: BS parameters.
        right: 'C' or 'P'.

    Returns:
        Implied volatility, or NaN if inversion fails.
    """
    if T <= 1e-6 or price <= 0 or S <= 0 or K <= 0:
        return np.nan
    intrinsic = max(0, (S - K) if right == "C" else (K - S)) * np.exp(-r * T)
    if price < intrinsic:
        return np.nan
    try:
        iv = brentq(
            lambda sigma: bs_price(S, K, T, sigma, r, right) - price,
            0.01, 10.0, xtol=1e-6, maxiter=100,
        )
        return float(iv)
    except (ValueError, RuntimeError):
        return np.nan


# ═══════════════════════════════════════════════════════════════
#  Delta
# ═══════════════════════════════════════════════════════════════

def bs_delta_vec(
    S: np.ndarray, K: np.ndarray, T: np.ndarray, sigma: np.ndarray,
    r: float = RISK_FREE_RATE,
) -> np.ndarray:
    """Vectorized Black-Scholes call delta: N(d1)."""
    S = np.asarray(S, dtype=float)
    K = np.asarray(K, dtype=float)
    T = np.asarray(T, dtype=float)
    sigma = np.asarray(sigma, dtype=float)

    delta = np.full_like(S, np.nan)
    ok = (T > 0) & (S > 0) & (K > 0) & (sigma > 0)
    if not ok.any():
        return delta

    s, k, t, sig = S[ok], K[ok], T[ok], sigma[ok]
    d1 = (np.log(s / k) + (r + 0.5 * sig ** 2) * t) / (sig * np.sqrt(t))
    delta[ok] = norm.cdf(d1)
    return delta


# ═══════════════════════════════════════════════════════════════
#  Forward Factor
# ═══════════════════════════════════════════════════════════════

def compute_ff(
    iv_front: float, iv_back: float,
    dte_front: float, dte_back: float,
) -> float:
    """Compute Forward Factor (PDF/Campasano formula).

    FF = (Front IV - Forward IV) / Forward IV
    where Forward IV = sqrt((sigma_b^2 * T_b - sigma_f^2 * T_f) / (T_b - T_f))

    Positive FF = front IV > forward IV (backwardation) = good for calendar.
    """
    T_f = dte_front / 365.0
    T_b = dte_back / 365.0
    dT = T_b - T_f
    if dT <= 0 or iv_front <= 0 or iv_back <= 0:
        return np.nan

    fwd_var = (iv_back ** 2 * T_b - iv_front ** 2 * T_f) / dT
    if fwd_var <= 0:
        return np.nan

    fwd_iv = np.sqrt(fwd_var)
    return float((iv_front - fwd_iv) / fwd_iv)


# ═══════════════════════════════════════════════════════════════
#  Helpers
# ═══════════════════════════════════════════════════════════════

def put_call_parity_call_equiv(
    put_price: np.ndarray, S: float, K: np.ndarray, T: np.ndarray,
    r: float = RISK_FREE_RATE,
) -> np.ndarray:
    """Convert put prices to call-equivalent via put-call parity.

    C = P + S - K * e^(-rT)
    """
    put_price = np.asarray(put_price, dtype=float)
    K = np.asarray(K, dtype=float)
    T = np.asarray(T, dtype=float)
    return np.maximum(put_price + S - K * np.exp(-r * T), 0.001)
