"""Tests for FastAPI routes — GET endpoints returning 200 + valid JSON.

Uses FastAPI TestClient. No IBKR connection needed — tests verify the
routes are wired correctly and return expected response shapes.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import json
import pytest
from fastapi.testclient import TestClient

from app import app


@pytest.fixture(scope="module")
def client():
    """FastAPI TestClient (no server needed)."""
    return TestClient(app)


# ═══════════════════════════════════════════════════════════
#  Trading routes (GET only)
# ═══════════════════════════════════════════════════════════

class TestStatusRoute:
    def test_returns_200(self, client):
        r = client.get("/api/status")
        assert r.status_code == 200

    def test_response_shape(self, client):
        data = client.get("/api/status").json()
        assert "connected" in data
        assert "n_active" in data
        assert "kelly_f" in data
        assert "max_positions" in data
        assert isinstance(data["connected"], bool)

    def test_disconnected_by_default(self, client):
        data = client.get("/api/status").json()
        assert data["connected"] is False
        assert data["account_value"] == 0


class TestSignalsRoute:
    def test_returns_200(self, client):
        r = client.get("/api/signals")
        assert r.status_code == 200

    def test_response_shape(self, client):
        data = client.get("/api/signals").json()
        assert "signals" in data
        assert "count" in data
        assert isinstance(data["signals"], list)
        assert isinstance(data["count"], int)


class TestSizingRoute:
    def test_returns_200(self, client):
        r = client.get("/api/sizing")
        assert r.status_code == 200

    def test_response_shape(self, client):
        data = client.get("/api/sizing").json()
        assert "sizing" in data
        assert "kelly_f" in data
        assert "kelly_target" in data
        assert isinstance(data["sizing"], list)

    def test_custom_account_value(self, client):
        data = client.get("/api/sizing?account_value=500000").json()
        assert data["account_value"] == 500000


class TestPortfolioRoute:
    def test_returns_200(self, client):
        r = client.get("/api/portfolio")
        assert r.status_code == 200

    def test_response_shape(self, client):
        data = client.get("/api/portfolio").json()
        assert "active" in data
        assert "closed" in data
        assert "n_active" in data
        assert isinstance(data["active"], list)
        assert isinstance(data["closed"], list)


class TestTradesRoute:
    def test_returns_200(self, client):
        r = client.get("/api/trades")
        assert r.status_code == 200

    def test_response_shape(self, client):
        data = client.get("/api/trades").json()
        assert "trades" in data
        assert "count" in data
        assert "win_rate" in data
        assert isinstance(data["trades"], list)


class TestOrdersRoute:
    def test_returns_200(self, client):
        r = client.get("/api/orders")
        assert r.status_code == 200

    def test_response_shape(self, client):
        data = client.get("/api/orders").json()
        assert "log" in data
        assert "open_orders" in data
        assert isinstance(data["log"], list)


# ═══════════════════════════════════════════════════════════
#  Analytics routes
# ═══════════════════════════════════════════════════════════

class TestTrackRecordRoute:
    def test_returns_200(self, client):
        r = client.get("/api/track-record")
        assert r.status_code == 200

    def test_response_shape(self, client):
        data = client.get("/api/track-record").json()
        assert "initial" in data
        assert "backtest" in data
        assert "live" in data
        assert "chart" in data
        assert isinstance(data["initial"], (int, float))


class TestStraddleRoute:
    def test_returns_200(self, client):
        r = client.get("/api/straddle")
        assert r.status_code == 200

    def test_response_is_dict(self, client):
        data = client.get("/api/straddle").json()
        assert isinstance(data, dict)


# ═══════════════════════════════════════════════════════════
#  Monitor routes
# ═══════════════════════════════════════════════════════════

class TestMonitorRoute:
    def test_returns_200(self, client):
        r = client.get("/api/monitor")
        assert r.status_code == 200

    def test_response_shape(self, client):
        data = client.get("/api/monitor").json()
        assert "active" in data
        assert "n_active" in data
        assert "refresh_running" in data
        assert isinstance(data["active"], list)


class TestMonitorRefreshStatus:
    def test_returns_200(self, client):
        r = client.get("/api/monitor/refresh/status")
        assert r.status_code == 200

    def test_idle_by_default(self, client):
        data = client.get("/api/monitor/refresh/status").json()
        assert data["status"] in ("idle", "done", "running")


class TestMonitorHistory:
    def test_returns_200(self, client):
        r = client.get("/api/monitor/history")
        assert r.status_code == 200

    def test_response_shape(self, client):
        data = client.get("/api/monitor/history").json()
        assert "snapshots" in data
        assert "count" in data
        assert isinstance(data["snapshots"], list)


class TestMonitorSimulateStatus:
    def test_returns_200(self, client):
        r = client.get("/api/monitor/simulate/status")
        assert r.status_code == 200

    def test_response_has_status(self, client):
        data = client.get("/api/monitor/simulate/status").json()
        assert "status" in data


# ═══════════════════════════════════════════════════════════
#  Scanner routes
# ═══════════════════════════════════════════════════════════

class TestScannerStatusRoute:
    def test_returns_200(self, client):
        r = client.get("/api/scanner_status")
        assert r.status_code == 200

    def test_response_shape(self, client):
        data = client.get("/api/scanner_status").json()
        assert "running" in data
        assert isinstance(data["running"], bool)


# ═══════════════════════════════════════════════════════════
#  Static / index
# ═══════════════════════════════════════════════════════════

class TestIndexRoute:
    def test_returns_200(self, client):
        r = client.get("/")
        assert r.status_code == 200

    def test_returns_html(self, client):
        r = client.get("/")
        assert "text/html" in r.headers.get("content-type", "")
