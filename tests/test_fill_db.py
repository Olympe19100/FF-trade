"""
Unit Tests — Fill Rate Database + Adaptive Routing (core/fill_db.py)

Tests for fill statistics CRUD, should_skip_combo logic,
and routing recommendation.

Usage:
    python -m pytest tests/test_fill_db.py -v
"""

import sys
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# Mock ib_insync before importing anything that might chain-import it
sys.modules.setdefault("ib_insync", MagicMock())
sys.modules.setdefault("websocket", MagicMock())

from core.fill_db import (
    load_fill_stats,
    save_fill_stats,
    update_fill_stats,
    should_skip_combo,
    get_ticker_routing_info,
)
from core.config import MIN_ATTEMPTS_FOR_ROUTING, COMBO_SKIP_THRESHOLD


# ============================================================
#  1. load_fill_stats / save_fill_stats
# ============================================================

class TestFillStatsCRUD:

    def test_load_empty(self, tmp_path):
        f = tmp_path / "fill_stats.json"
        with patch("core.fill_db.FILL_STATS_FILE", f):
            assert load_fill_stats() == {}

    def test_save_and_load(self, tmp_path):
        f = tmp_path / "fill_stats.json"
        with patch("core.fill_db.FILL_STATS_FILE", f):
            data = {"AAPL": {"combo_attempts": 5, "combo_fills": 3}}
            save_fill_stats(data)
            loaded = load_fill_stats()
            assert loaded["AAPL"]["combo_attempts"] == 5

    def test_load_corrupt_file(self, tmp_path):
        f = tmp_path / "fill_stats.json"
        f.write_text("not valid json")
        with patch("core.fill_db.FILL_STATS_FILE", f):
            assert load_fill_stats() == {}


# ============================================================
#  2. update_fill_stats
# ============================================================

class TestUpdateFillStats:

    def test_new_ticker_created(self, tmp_path):
        f = tmp_path / "fill_stats.json"
        with patch("core.fill_db.FILL_STATS_FILE", f):
            update_fill_stats("AAPL", "combo", True, True, True, 30.0)
            stats = load_fill_stats()
            assert "AAPL" in stats
            assert stats["AAPL"]["combo_attempts"] == 1
            assert stats["AAPL"]["combo_fills"] == 1
            assert stats["AAPL"]["avg_combo_seconds"] == 30.0

    def test_combo_attempt_no_fill(self, tmp_path):
        f = tmp_path / "fill_stats.json"
        with patch("core.fill_db.FILL_STATS_FILE", f):
            update_fill_stats("AAPL", "legs", True, True, False, 45.0)
            stats = load_fill_stats()
            assert stats["AAPL"]["combo_attempts"] == 1
            assert stats["AAPL"]["combo_fills"] == 0
            assert stats["AAPL"]["legs_fills"] == 1

    def test_legs_only_no_combo(self, tmp_path):
        f = tmp_path / "fill_stats.json"
        with patch("core.fill_db.FILL_STATS_FILE", f):
            update_fill_stats("AAPL", "legs", True, False, False, 20.0)
            stats = load_fill_stats()
            assert stats["AAPL"]["combo_attempts"] == 0
            assert stats["AAPL"]["legs_attempts"] == 1
            assert stats["AAPL"]["legs_fills"] == 1

    def test_multiple_updates_accumulate(self, tmp_path):
        f = tmp_path / "fill_stats.json"
        with patch("core.fill_db.FILL_STATS_FILE", f):
            update_fill_stats("AAPL", "combo", True, True, True, 30.0)
            update_fill_stats("AAPL", "combo", True, True, True, 40.0)
            update_fill_stats("AAPL", "legs", True, True, False, 50.0)
            stats = load_fill_stats()
            assert stats["AAPL"]["combo_attempts"] == 3
            assert stats["AAPL"]["combo_fills"] == 2
            assert stats["AAPL"]["legs_fills"] == 1
            assert stats["AAPL"]["avg_combo_seconds"] == pytest.approx(35.0)


# ============================================================
#  3. should_skip_combo
# ============================================================

class TestShouldSkipCombo:

    def test_no_data_returns_false(self, tmp_path):
        f = tmp_path / "fill_stats.json"
        with patch("core.fill_db.FILL_STATS_FILE", f):
            assert should_skip_combo("AAPL") is False

    def test_insufficient_attempts_returns_false(self, tmp_path):
        f = tmp_path / "fill_stats.json"
        with patch("core.fill_db.FILL_STATS_FILE", f):
            # Only 2 attempts, need MIN_ATTEMPTS_FOR_ROUTING (3)
            save_fill_stats({"AAPL": {
                "combo_attempts": 2, "combo_fills": 0,
                "legs_attempts": 0, "legs_fills": 0,
                "avg_combo_seconds": 0, "avg_legs_seconds": 0,
            }})
            assert should_skip_combo("AAPL") is False

    def test_low_fill_rate_skips(self, tmp_path):
        f = tmp_path / "fill_stats.json"
        with patch("core.fill_db.FILL_STATS_FILE", f):
            # 5 attempts, 1 fill = 20% < 25% threshold
            save_fill_stats({"AAPL": {
                "combo_attempts": 5, "combo_fills": 1,
                "legs_attempts": 4, "legs_fills": 4,
                "avg_combo_seconds": 30, "avg_legs_seconds": 40,
            }})
            assert should_skip_combo("AAPL") is True

    def test_high_fill_rate_no_skip(self, tmp_path):
        f = tmp_path / "fill_stats.json"
        with patch("core.fill_db.FILL_STATS_FILE", f):
            # 4 attempts, 3 fills = 75% > 25% threshold
            save_fill_stats({"AAPL": {
                "combo_attempts": 4, "combo_fills": 3,
                "legs_attempts": 1, "legs_fills": 1,
                "avg_combo_seconds": 30, "avg_legs_seconds": 40,
            }})
            assert should_skip_combo("AAPL") is False

    def test_exact_threshold_skips(self, tmp_path):
        f = tmp_path / "fill_stats.json"
        with patch("core.fill_db.FILL_STATS_FILE", f):
            # 4 attempts, 1 fill = 25% → NOT < 25%, should NOT skip
            save_fill_stats({"AAPL": {
                "combo_attempts": 4, "combo_fills": 1,
                "legs_attempts": 3, "legs_fills": 3,
                "avg_combo_seconds": 30, "avg_legs_seconds": 40,
            }})
            assert should_skip_combo("AAPL") is False

    def test_zero_fills_skips(self, tmp_path):
        f = tmp_path / "fill_stats.json"
        with patch("core.fill_db.FILL_STATS_FILE", f):
            save_fill_stats({"AAPL": {
                "combo_attempts": 5, "combo_fills": 0,
                "legs_attempts": 5, "legs_fills": 5,
                "avg_combo_seconds": 0, "avg_legs_seconds": 40,
            }})
            assert should_skip_combo("AAPL") is True


# ============================================================
#  4. get_ticker_routing_info
# ============================================================

class TestGetTickerRoutingInfo:

    def test_no_data_returns_insufficient(self, tmp_path):
        f = tmp_path / "fill_stats.json"
        with patch("core.fill_db.FILL_STATS_FILE", f):
            info = get_ticker_routing_info("AAPL")
            assert info["recommendation"] == "insufficient_data"
            assert info["skip_combo"] is False

    def test_legs_only_recommendation(self, tmp_path):
        f = tmp_path / "fill_stats.json"
        with patch("core.fill_db.FILL_STATS_FILE", f):
            save_fill_stats({"AAPL": {
                "combo_attempts": 5, "combo_fills": 0,
                "legs_attempts": 5, "legs_fills": 5,
                "avg_combo_seconds": 0, "avg_legs_seconds": 40,
            }})
            info = get_ticker_routing_info("AAPL")
            assert info["recommendation"] == "legs_only"
            assert info["skip_combo"] is True

    def test_combo_preferred_recommendation(self, tmp_path):
        f = tmp_path / "fill_stats.json"
        with patch("core.fill_db.FILL_STATS_FILE", f):
            save_fill_stats({"AAPL": {
                "combo_attempts": 4, "combo_fills": 4,
                "legs_attempts": 0, "legs_fills": 0,
                "avg_combo_seconds": 25, "avg_legs_seconds": 0,
            }})
            info = get_ticker_routing_info("AAPL")
            assert info["recommendation"] == "combo_preferred"
            assert info["combo_fill_rate"] == 1.0

    def test_routing_info_fields(self, tmp_path):
        f = tmp_path / "fill_stats.json"
        with patch("core.fill_db.FILL_STATS_FILE", f):
            save_fill_stats({"AAPL": {
                "combo_attempts": 10, "combo_fills": 4,
                "legs_attempts": 6, "legs_fills": 6,
                "avg_combo_seconds": 30, "avg_legs_seconds": 45,
            }})
            info = get_ticker_routing_info("AAPL")
            assert info["combo_fill_rate"] == pytest.approx(0.4)
            assert info["legs_fill_rate"] == pytest.approx(1.0)
            assert info["avg_combo_seconds"] == 30
            assert info["avg_legs_seconds"] == 45


# ============================================================
#  5. Config constants
# ============================================================

class TestConfigConstants:

    def test_min_attempts_for_routing(self):
        assert MIN_ATTEMPTS_FOR_ROUTING == 3

    def test_combo_skip_threshold(self):
        assert COMBO_SKIP_THRESHOLD == 0.25
