"""Shared pytest fixtures for the test suite."""

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

# Ensure project root is on sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


@pytest.fixture
def sample_returns():
    """Sample trade returns for Kelly / risk calculations."""
    rng = np.random.default_rng(42)
    # Realistic spread returns: mostly small positive, some negative
    return (rng.normal(0.05, 0.15, size=200)).tolist()


@pytest.fixture
def sample_signals_df():
    """Minimal signals DataFrame matching scanner output."""
    return pd.DataFrame({
        "ticker": ["AAPL", "MSFT", "GOOG"],
        "combo": ["30-60", "30-90", "60-90"],
        "strike": [180.0, 420.0, 170.0],
        "put_strike": [170.0, 400.0, 160.0],
        "stock_px": [185.0, 425.0, 175.0],
        "front_exp": ["2026-06-01", "2026-06-01", "2026-07-01"],
        "back_exp": ["2026-07-01", "2026-08-01", "2026-09-01"],
        "front_iv": [25.0, 22.0, 28.0],
        "back_iv": [24.0, 21.0, 27.0],
        "ff": [0.35, 0.28, 0.22],
        "call_cost": [3.50, 5.20, 4.10],
        "put_cost": [2.80, 4.50, 3.60],
        "dbl_cost": [6.30, 9.70, 7.70],
        "call_delta": [0.35, 0.34, 0.36],
        "put_delta": [0.35, 0.33, 0.37],
        "front_oi": [500, 800, 300],
        "back_oi": [400, 600, 250],
        "volume": [1200, 2500, 800],
    })


@pytest.fixture
def tmp_state(tmp_path):
    """Create temporary state/output/cache directories."""
    (tmp_path / "state").mkdir()
    (tmp_path / "output").mkdir()
    (tmp_path / "cache").mkdir()
    return tmp_path
