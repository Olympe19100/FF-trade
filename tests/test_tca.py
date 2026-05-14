"""
Unit Tests — Transaction Cost Analysis (core/tca.py)

Tests for TCA metrics computation, execution record persistence,
and aggregate summary statistics.

Usage:
    python -m pytest tests/test_tca.py -v
"""

import sys
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# Mock ib_insync before importing anything that might chain-import it
sys.modules.setdefault("ib_insync", MagicMock())
sys.modules.setdefault("websocket", MagicMock())

from core.tca import (
    compute_tca_metrics,
    ExecutionRecord,
    record_execution,
    load_execution_log,
    tca_summary,
)


# ============================================================
#  1. compute_tca_metrics
# ============================================================

class TestComputeTcaMetrics:

    def test_entry_implementation_shortfall_positive(self):
        """Entry IS: fill > scanner_mid → positive (paid more)."""
        m = compute_tca_metrics(5.10, 5.00, 4.90, 5.10, 5.00, "entry")
        assert m["implementation_shortfall"] == pytest.approx(0.10, abs=1e-4)

    def test_entry_implementation_shortfall_negative(self):
        """Entry IS: fill < scanner_mid → negative (savings)."""
        m = compute_tca_metrics(4.90, 5.00, 4.80, 5.20, 5.00, "entry")
        assert m["implementation_shortfall"] == pytest.approx(-0.10, abs=1e-4)

    def test_close_implementation_shortfall(self):
        """Close IS: scanner_mid - fill (opposite of entry)."""
        m = compute_tca_metrics(5.10, 5.00, 4.90, 5.10, 5.00, "close")
        assert m["implementation_shortfall"] == pytest.approx(-0.10, abs=1e-4)

    def test_spread_capture_at_mid(self):
        """Fill exactly at mid → spread_capture = 1.0."""
        m = compute_tca_metrics(5.00, 5.00, 4.90, 5.10, 5.00, "entry")
        assert m["spread_capture"] == pytest.approx(1.0)

    def test_spread_capture_at_edge(self):
        """Fill at ask → spread_capture = 0.0."""
        m = compute_tca_metrics(5.10, 5.00, 4.90, 5.10, 5.00, "entry")
        assert m["spread_capture"] == pytest.approx(0.0)

    def test_spread_capture_clamped_low(self):
        """Fill far outside spread → clamp to -1.0."""
        m = compute_tca_metrics(5.50, 5.00, 4.90, 5.10, 5.00, "entry")
        assert m["spread_capture"] >= -1.0

    def test_spread_capture_clamped_high(self):
        """Fill better than mid (inside spread) → clamp to 2.0 max."""
        m = compute_tca_metrics(5.00, 5.00, 4.99, 5.01, 5.00, "entry")
        assert m["spread_capture"] <= 2.0

    def test_arrival_slippage(self):
        """arrival_slippage = fill - theta_mid."""
        m = compute_tca_metrics(5.05, 5.00, 4.90, 5.10, 5.00, "entry")
        assert m["arrival_slippage"] == pytest.approx(0.05, abs=1e-4)

    def test_realized_spread(self):
        """realized_spread = 2×|fill-mid| / BA."""
        m = compute_tca_metrics(5.05, 5.00, 4.90, 5.10, 5.00, "entry")
        # BA = 0.20, |5.05-5.00| = 0.05, realized = 2*0.05/0.20 = 0.50
        assert m["realized_spread"] == pytest.approx(0.50, abs=1e-2)

    def test_zero_spread_defaults(self):
        """No spread data → spread_capture=0, realized_spread=0."""
        m = compute_tca_metrics(5.00, 5.00, 0, 0, 0, "entry")
        assert m["spread_capture"] == 0.0
        assert m["realized_spread"] == 0.0


# ============================================================
#  2. ExecutionRecord
# ============================================================

class TestExecutionRecord:

    def test_default_values(self):
        r = ExecutionRecord(ticker="AAPL", direction="entry")
        assert r.ticker == "AAPL"
        assert r.direction == "entry"
        assert r.walk_steps == 0
        assert r.tca == {}
        assert r.priority == "Normal"

    def test_full_initialization(self):
        r = ExecutionRecord(
            ticker="MSFT",
            direction="close",
            scanner_mid=5.0,
            theta_bid=4.9,
            theta_ask=5.1,
            theta_mid=5.0,
            method="combo",
            combo_attempted=True,
            combo_result="filled",
            walk_steps=2,
            initial_limit=5.0,
            final_limit=5.1,
            exec_seconds=35.5,
            fill_price=5.05,
            contracts=10,
            slippage=0.05,
            min_leg_oi=500,
            option_ba=0.10,
        )
        assert r.method == "combo"
        assert r.exec_seconds == 35.5
        assert r.min_leg_oi == 500


# ============================================================
#  3. record_execution + load_execution_log
# ============================================================

class TestRecordAndLoad:

    def test_record_and_load(self, tmp_path):
        log_file = tmp_path / "execution_log.json"
        with patch("core.tca.EXECUTION_LOG_FILE", log_file):
            r = ExecutionRecord(
                ticker="AAPL",
                direction="entry",
                scanner_mid=5.0,
                theta_bid=4.9,
                theta_ask=5.1,
                theta_mid=5.0,
                fill_price=5.05,
                contracts=5,
            )
            record_execution(r)

            entries = load_execution_log()
            assert len(entries) == 1
            assert entries[0]["ticker"] == "AAPL"
            assert entries[0]["direction"] == "entry"
            assert "implementation_shortfall" in entries[0]["tca"]

    def test_multiple_records_append(self, tmp_path):
        log_file = tmp_path / "execution_log.json"
        with patch("core.tca.EXECUTION_LOG_FILE", log_file):
            for t in ["AAPL", "MSFT", "GOOG"]:
                record_execution(ExecutionRecord(
                    ticker=t, direction="entry",
                    scanner_mid=5.0, theta_mid=5.0,
                    fill_price=5.0,
                ))
            entries = load_execution_log()
            assert len(entries) == 3

    def test_load_empty_file(self, tmp_path):
        log_file = tmp_path / "execution_log.json"
        with patch("core.tca.EXECUTION_LOG_FILE", log_file):
            entries = load_execution_log()
            assert entries == []

    def test_load_corrupt_file(self, tmp_path):
        log_file = tmp_path / "execution_log.json"
        log_file.write_text("not json at all")
        with patch("core.tca.EXECUTION_LOG_FILE", log_file):
            entries = load_execution_log()
            assert entries == []


# ============================================================
#  4. tca_summary
# ============================================================

class TestTcaSummary:

    def test_empty_log(self, tmp_path):
        log_file = tmp_path / "execution_log.json"
        with patch("core.tca.EXECUTION_LOG_FILE", log_file):
            s = tca_summary()
            assert s["count"] == 0
            assert s["mean_is"] == 0.0

    def test_aggregate_stats(self, tmp_path):
        log_file = tmp_path / "execution_log.json"
        with patch("core.tca.EXECUTION_LOG_FILE", log_file):
            for px in [5.10, 5.05, 4.95]:
                record_execution(ExecutionRecord(
                    ticker="AAPL", direction="entry",
                    scanner_mid=5.0,
                    theta_bid=4.9, theta_ask=5.1, theta_mid=5.0,
                    fill_price=px,
                    method="combo",
                    combo_attempted=True,
                    combo_result="filled",
                    walk_steps=1,
                    exec_seconds=30.0,
                ))

            s = tca_summary()
            assert s["count"] == 3
            assert s["combo_fill_rate"] == pytest.approx(1.0)
            assert s["avg_walk_steps"] == pytest.approx(1.0)
            assert s["avg_exec_seconds"] == pytest.approx(30.0)

    def test_filter_by_ticker(self, tmp_path):
        log_file = tmp_path / "execution_log.json"
        with patch("core.tca.EXECUTION_LOG_FILE", log_file):
            for t in ["AAPL", "MSFT", "AAPL"]:
                record_execution(ExecutionRecord(
                    ticker=t, direction="entry",
                    scanner_mid=5.0, theta_mid=5.0,
                    fill_price=5.0,
                ))
            s = tca_summary(ticker="AAPL")
            assert s["count"] == 2

    def test_last_n(self, tmp_path):
        log_file = tmp_path / "execution_log.json"
        with patch("core.tca.EXECUTION_LOG_FILE", log_file):
            for i in range(10):
                record_execution(ExecutionRecord(
                    ticker="AAPL", direction="entry",
                    scanner_mid=5.0, theta_mid=5.0,
                    fill_price=5.0 + i * 0.01,
                ))
            s = tca_summary(last_n=3)
            assert s["count"] == 3

    def test_combo_fill_rate_partial(self, tmp_path):
        log_file = tmp_path / "execution_log.json"
        with patch("core.tca.EXECUTION_LOG_FILE", log_file):
            # 2 combo attempts: 1 filled, 1 not
            record_execution(ExecutionRecord(
                ticker="AAPL", direction="entry",
                scanner_mid=5.0, theta_mid=5.0, fill_price=5.0,
                combo_attempted=True, combo_result="filled", method="combo",
            ))
            record_execution(ExecutionRecord(
                ticker="AAPL", direction="entry",
                scanner_mid=5.0, theta_mid=5.0, fill_price=5.0,
                combo_attempted=True, combo_result="no_fill", method="legs",
            ))
            s = tca_summary()
            assert s["combo_fill_rate"] == pytest.approx(0.5)
