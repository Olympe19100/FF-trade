"""
Autonomous Flow Unit Tests — scan → earnings → signal → sizing → paper trade → scheduling

All external dependencies (IBKR, ThetaData, EODHD, SMTP) are mocked so tests run offline.
"""

import json
import sys
import threading
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch, PropertyMock

import numpy as np
import pandas as pd
import pytest

# Ensure project root is importable
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from core.config import (
    MAX_POSITIONS, MAX_CONTRACTS, DEFAULT_ALLOC,
    MIN_KELLY_TRADES, KELLY_FRAC, CONTRACT_MULT,
    COMMISSION_LEG, SLIPPAGE_PER_LEG, SLIPPAGE_BUFFER,
    CLOSE_DAYS, OPTIMAL_START_ET, OPTIMAL_END_ET,
)


# ═══════════════════════════════════════════════════════════════
#  HELPERS
# ═══════════════════════════════════════════════════════════════

def _make_chain(stock_px, expirations, strikes, base_iv=0.30, oi=500):
    """Build a synthetic option chain DataFrame matching scanner format.

    Args:
        stock_px: underlying price (used for IV scaling, not stored)
        expirations: list of (exp_date_str, dte_days) tuples
        strikes: list of floats
        base_iv: implied volatility for all rows
        oi: open interest per row
    """
    rows = []
    for exp_date, _dte in expirations:
        for strike in strikes:
            for opt_type in ("call", "put"):
                # Realistic bid/ask around a base mid price
                intrinsic = max(0, stock_px - strike) if opt_type == "call" else max(0, strike - stock_px)
                time_val = base_iv * stock_px * 0.1
                mid = intrinsic + time_val
                mid = max(mid, 0.50)
                rows.append({
                    "exp_date": exp_date,
                    "type": opt_type,
                    "strike": float(strike),
                    "bid": round(mid * 0.95, 2),
                    "ask": round(mid * 1.05, 2),
                    "iv": base_iv,
                    "volume": 1000,
                    "open_interest": oi,
                })
    return pd.DataFrame(rows)


def _make_position(ticker, combo="30-60", days_to_front_exp=30, **kw):
    """Build a realistic position dict matching add_position() output."""
    today = datetime.now()
    front_exp = (today + timedelta(days=days_to_front_exp)).strftime("%Y-%m-%d")
    back_exp = (today + timedelta(days=days_to_front_exp + 30)).strftime("%Y-%m-%d")
    pos = {
        "id": f"{ticker}_{combo}_{today.strftime('%Y%m%d_%H%M%S')}",
        "ticker": ticker,
        "combo": combo,
        "strike": 180.0,
        "put_strike": 170.0,
        "spread_type": "double",
        "front_exp": front_exp,
        "back_exp": back_exp,
        "entry_date": today.strftime("%Y-%m-%d"),
        "contracts": 2,
        "cost_per_share": 6.30,
        "total_deployed": 1324.0,
        "n_legs": 4,
        "ff": 0.35,
    }
    pos.update(kw)
    return pos


class MockDatetime(datetime):
    """Datetime subclass that allows controlling now(tz)."""
    _fixed_now = None

    @classmethod
    def now(cls, tz=None):
        if cls._fixed_now is not None:
            if tz is not None:
                return cls._fixed_now.astimezone(tz)
            return cls._fixed_now
        return super().now(tz)


# ═══════════════════════════════════════════════════════════════
#  CLASS 1: Earnings Cascade
# ═══════════════════════════════════════════════════════════════

class TestEarningsCascade:
    """Tests scanner.get_earnings_dates() cascade: EODHD → IBKR → projection."""

    def _mock_eodhd_response(self, tickers, status=200):
        """Build a mock EODHD earnings JSON response."""
        earnings = []
        base_date = datetime.now() + timedelta(days=30)
        for t in tickers:
            earnings.append({
                "code": f"{t}.US",
                "report_date": base_date.strftime("%Y-%m-%d"),
            })
        return {"earnings": earnings}, status

    @patch("core.scanner.sqlite3")
    @patch("core.scanner._get_session")
    def test_eodhd_covers_tickers(self, mock_session_fn, mock_sqlite):
        """EODHD returns data → all tickers appear in result."""
        from core.scanner import get_earnings_dates

        tickers = ["AAPL", "MSFT", "GOOG"]
        data, _ = self._mock_eodhd_response(tickers)

        # Mock HTTP
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = data
        mock_sess = MagicMock()
        mock_sess.get.return_value = mock_resp
        mock_session_fn.return_value = mock_sess

        # Mock DB — return the rows we "synced"
        base_date = datetime.now() + timedelta(days=30)
        rd_int = int(base_date.strftime("%Y%m%d"))
        db_df = pd.DataFrame({
            "root": tickers,
            "report_date": [rd_int] * 3,
        })
        mock_conn = MagicMock()
        mock_sqlite.connect.return_value = mock_conn

        with patch("core.scanner.pd.read_sql_query", return_value=db_df):
            result = get_earnings_dates(tickers)

        assert isinstance(result, dict)
        for t in tickers:
            assert t in result

    @patch("core.scanner._project_earnings_from_history", return_value=[])
    @patch("core.scanner._fetch_earnings_ibkr")
    @patch("core.scanner.sqlite3")
    @patch("core.scanner._get_session")
    def test_eodhd_fails_ibkr_fallback(self, mock_session_fn, mock_sqlite,
                                        mock_ibkr, mock_proj):
        """EODHD HTTP error → _fetch_earnings_ibkr called with missing tickers."""
        from core.scanner import get_earnings_dates

        tickers = ["AAPL", "TSLA"]

        # EODHD fails
        mock_resp = MagicMock()
        mock_resp.status_code = 500
        mock_sess = MagicMock()
        mock_sess.get.return_value = mock_resp
        mock_session_fn.return_value = mock_sess

        # DB returns empty
        mock_conn = MagicMock()
        mock_sqlite.connect.return_value = mock_conn
        with patch("core.scanner.pd.read_sql_query", return_value=pd.DataFrame()):
            # IBKR returns data for AAPL
            rd_int = int((datetime.now() + timedelta(days=30)).strftime("%Y%m%d"))
            mock_ibkr.return_value = [{"root": "AAPL", "report_date": rd_int}]

            ib_mock = MagicMock()
            result = get_earnings_dates(tickers, ib=ib_mock)

        mock_ibkr.assert_called_once()
        called_tickers = mock_ibkr.call_args[0][1]
        assert "AAPL" in called_tickers

    @patch("core.scanner._project_earnings_from_history")
    @patch("core.scanner._fetch_earnings_ibkr")
    @patch("core.scanner.sqlite3")
    @patch("core.scanner._get_session")
    def test_all_fail_projection_fallback(self, mock_session_fn, mock_sqlite,
                                           mock_ibkr, mock_proj):
        """EODHD + IBKR fail → _project_earnings_from_history called."""
        from core.scanner import get_earnings_dates

        tickers = ["XYZ"]

        # EODHD exception
        mock_sess = MagicMock()
        mock_sess.get.side_effect = Exception("network down")
        mock_session_fn.return_value = mock_sess

        # DB empty
        mock_conn = MagicMock()
        mock_sqlite.connect.return_value = mock_conn
        with patch("core.scanner.pd.read_sql_query", return_value=pd.DataFrame()):
            mock_ibkr.return_value = []
            mock_proj.return_value = []

            ib_mock = MagicMock()
            result = get_earnings_dates(tickers, ib=ib_mock)

        mock_proj.assert_called_once()

    @patch("core.scanner._project_earnings_from_history", return_value=[])
    @patch("core.scanner.sqlite3")
    @patch("core.scanner._get_session")
    def test_all_sources_fail_gracefully(self, mock_session_fn, mock_sqlite,
                                          mock_proj):
        """Everything fails → empty dict returned, no crash."""
        from core.scanner import get_earnings_dates

        # EODHD exception
        mock_sess = MagicMock()
        mock_sess.get.side_effect = Exception("network down")
        mock_session_fn.return_value = mock_sess

        # DB empty
        mock_conn = MagicMock()
        mock_sqlite.connect.return_value = mock_conn
        with patch("core.scanner.pd.read_sql_query", return_value=pd.DataFrame()):
            result = get_earnings_dates(["AAPL", "MSFT"])

        assert isinstance(result, dict)
        # No crash — may or may not have entries depending on fallback

    def test_has_earnings_between(self):
        """True inside range, False outside, False for unknown ticker."""
        from core.scanner import has_earnings_between

        earn_by_root = {
            "AAPL": np.array([20260515, 20260815]),
            "MSFT": np.array([20260710]),
        }

        # Inside range
        assert has_earnings_between("AAPL", 20260501, 20260601, earn_by_root)
        # Outside range
        assert not has_earnings_between("AAPL", 20260601, 20260701, earn_by_root)
        # Unknown ticker
        assert not has_earnings_between("ZZZZ", 20260101, 20261231, earn_by_root)
        # Second date inside
        assert has_earnings_between("AAPL", 20260801, 20260901, earn_by_root)


# ═══════════════════════════════════════════════════════════════
#  CLASS 2: Scanner Flow
# ═══════════════════════════════════════════════════════════════

class TestScannerFlow:
    """Tests scanner.scan_ticker_from_chain() with synthetic chain data."""

    def _high_ff_chain(self, stock_px=200.0):
        """Build a chain where front IV > forward IV to guarantee FF > 0.

        Uses front_iv=0.40 (30d), back_iv=0.38 (60d) which produces:
          fwd_var = (0.38^2 * 60/365 - 0.40^2 * 30/365) / (30/365) ≈ 0.129
          fwd_iv ≈ 0.359, FF ≈ (0.40 - 0.359)/0.359 ≈ 0.115
        Back mid > front mid to ensure spread_cost >= $1.00.
        Tight bid-ask (1% spread) to pass ba_pct <= 0.10 filter.
        """
        today = datetime.now()
        front_exp = (today + timedelta(days=30)).strftime("%Y-%m-%d")
        back_exp = (today + timedelta(days=60)).strftime("%Y-%m-%d")

        strikes = [190.0, 195.0, 200.0, 205.0, 210.0]
        rows = []
        for exp, dte, iv in [(front_exp, 30, 0.40), (back_exp, 60, 0.38)]:
            for s in strikes:
                for t in ("call", "put"):
                    # Use BS-like mid: longer DTE → higher premium
                    from math import sqrt
                    T = dte / 365.0
                    base_mid = iv * stock_px * sqrt(T) * 0.4
                    mid = max(base_mid, 1.50)
                    rows.append({
                        "exp_date": exp,
                        "type": t,
                        "strike": float(s),
                        "bid": round(mid * 0.995, 2),
                        "ask": round(mid * 1.005, 2),
                        "iv": iv,
                        "volume": 1000,
                        "open_interest": 500,
                    })
        return pd.DataFrame(rows), front_exp, back_exp

    def test_valid_signal_generated(self):
        """High front IV chain → at least 1 signal with required fields."""
        from core.scanner import scan_ticker_from_chain

        chain, _, _ = self._high_ff_chain()
        today = datetime.now()
        signals = scan_ticker_from_chain("TEST", 200.0, chain, {}, today)

        assert len(signals) >= 1
        sig = signals[0]
        assert sig["ticker"] == "TEST"
        assert sig["ff"] > 0
        assert "strike" in sig
        assert "front_exp" in sig
        assert "back_exp" in sig

    def test_earnings_blocks_signal(self):
        """Valid chain + earnings in expiry range → 0 signals."""
        from core.scanner import scan_ticker_from_chain

        chain, front_exp, back_exp = self._high_ff_chain()
        today = datetime.now()
        # Earnings between today and back expiry
        earn_date = int((today + timedelta(days=45)).strftime("%Y%m%d"))
        earn_by_root = {"TEST": np.array([earn_date])}

        signals = scan_ticker_from_chain("TEST", 200.0, chain, earn_by_root, today)
        assert len(signals) == 0

    def test_low_oi_rejected(self):
        """OI < MIN_OI_LEG → 0 signals."""
        from core.scanner import scan_ticker_from_chain

        chain, _, _ = self._high_ff_chain()
        # Set all OI to 10 (below MIN_OI_LEG=100)
        chain["open_interest"] = 10
        today = datetime.now()

        signals = scan_ticker_from_chain("TEST", 200.0, chain, {}, today)
        assert len(signals) == 0

    def test_empty_chain_returns_empty(self):
        """Empty DataFrame → empty list."""
        from core.scanner import scan_ticker_from_chain

        signals = scan_ticker_from_chain("TEST", 200.0, pd.DataFrame(), {}, datetime.now())
        assert signals == []

    def test_best_ff_per_ticker(self):
        """Multiple DTE pairs → only 1 signal (highest FF) kept."""
        from core.scanner import scan_ticker_from_chain

        today = datetime.now()
        # 3 expirations → multiple (front, back) pairs
        exps = [
            ((today + timedelta(days=25)).strftime("%Y-%m-%d"), 25),
            ((today + timedelta(days=50)).strftime("%Y-%m-%d"), 50),
            ((today + timedelta(days=80)).strftime("%Y-%m-%d"), 80),
        ]
        strikes = [195.0, 200.0, 205.0]
        chain = _make_chain(200.0, exps, strikes, base_iv=0.40, oi=500)
        # Give the front-most expiration highest IV to create varied FF
        chain.loc[chain["exp_date"] == exps[0][0], "iv"] = 0.50

        signals = scan_ticker_from_chain("TEST", 200.0, chain, {}, today)
        # Should be at most 1 (best FF per ticker)
        assert len(signals) <= 1


# ═══════════════════════════════════════════════════════════════
#  CLASS 3: Portfolio Lifecycle
# ═══════════════════════════════════════════════════════════════

class TestPortfolioLifecycle:
    """Tests portfolio.py with real file I/O on tmp_path."""

    def test_add_load_roundtrip(self, tmp_path):
        """add_position + save + load → same position data."""
        pf_file = tmp_path / "portfolio.json"
        trades_file = tmp_path / "trades.json"

        with patch("core.portfolio.PORTFOLIO_FILE", pf_file), \
             patch("core.portfolio.TRADES_FILE", trades_file):
            from core.portfolio import load_portfolio, save_portfolio, add_position

            portfolio = load_portfolio()
            assert portfolio["positions"] == []

            pos = add_position(
                portfolio, "AAPL", "30-60", 180.0,
                "2026-06-01", "2026-07-01", 2, 6.30,
                "double", 0.35, 4, put_strike=170.0,
            )

            save_portfolio(portfolio)
            reloaded = load_portfolio()

            assert len(reloaded["positions"]) == 1
            rpos = reloaded["positions"][0]
            assert rpos["ticker"] == "AAPL"
            assert rpos["strike"] == 180.0
            assert rpos["put_strike"] == 170.0
            assert rpos["contracts"] == 2

    def test_close_records_trade(self, tmp_path):
        """record_trade → correct entry in trades.json."""
        trades_file = tmp_path / "trades.json"

        with patch("core.portfolio.TRADES_FILE", trades_file):
            from core.portfolio import record_trade

            pos = _make_position("MSFT", contracts=3)
            record_trade(pos, exit_price=7.00, pnl=120.0, return_pct=0.10)

            with open(trades_file) as f:
                data = json.load(f)

            assert len(data["trades"]) == 1
            trade = data["trades"][0]
            assert trade["ticker"] == "MSFT"
            assert trade["pnl"] == 120.0
            assert trade["return_pct"] == 0.10

    def test_multiple_positions(self, tmp_path):
        """3 added, 1 closed → 2 active."""
        pf_file = tmp_path / "portfolio.json"

        with patch("core.portfolio.PORTFOLIO_FILE", pf_file), \
             patch("core.portfolio.TRADES_FILE", tmp_path / "trades.json"):
            from core.portfolio import load_portfolio, save_portfolio, add_position

            portfolio = load_portfolio()
            add_position(portfolio, "AAPL", "30-60", 180.0,
                         "2026-06-01", "2026-07-01", 1, 5.0, "double", 0.30, 4)
            add_position(portfolio, "MSFT", "30-90", 420.0,
                         "2026-06-01", "2026-08-01", 1, 8.0, "double", 0.25, 4)
            add_position(portfolio, "GOOG", "60-90", 170.0,
                         "2026-07-01", "2026-09-01", 1, 6.0, "single", 0.20, 2)

            # Close MSFT
            portfolio["positions"][1]["exit_date"] = "2026-05-15"
            save_portfolio(portfolio)

            reloaded = load_portfolio()
            active = [p for p in reloaded["positions"] if "exit_date" not in p]
            assert len(active) == 2

    def test_cost_per_contract(self):
        """cost_per_contract matches formula: (cps + 0.03*n_legs)*100 + 0.65*n_legs."""
        from core.portfolio import cost_per_contract

        cps = 6.30
        n_legs = 4
        expected = (cps + SLIPPAGE_PER_LEG * n_legs) * CONTRACT_MULT + COMMISSION_LEG * n_legs
        assert cost_per_contract(cps, n_legs) == pytest.approx(expected)

        # Single calendar: 2 legs
        cps2 = 3.50
        expected2 = (cps2 + SLIPPAGE_PER_LEG * 2) * CONTRACT_MULT + COMMISSION_LEG * 2
        assert cost_per_contract(cps2, 2) == pytest.approx(expected2)

    def test_total_deployed(self, tmp_path):
        """add_position total_deployed matches: (cps + slip*n_legs)*MULT*contracts + comm."""
        pf_file = tmp_path / "portfolio.json"

        with patch("core.portfolio.PORTFOLIO_FILE", pf_file):
            from core.portfolio import load_portfolio, add_position

            portfolio = load_portfolio()
            pos = add_position(
                portfolio, "AAPL", "30-60", 180.0,
                "2026-06-01", "2026-07-01", 3, 6.30,
                "double", 0.35, 4, put_strike=170.0,
            )

            cps = 6.30
            n_legs = 4
            contracts = 3
            slip = SLIPPAGE_PER_LEG * n_legs
            commission = n_legs * COMMISSION_LEG * contracts
            expected = (cps + slip) * CONTRACT_MULT * contracts + commission
            assert pos["total_deployed"] == pytest.approx(expected, rel=1e-2)


# ═══════════════════════════════════════════════════════════════
#  CLASS 4: Sizing Constraints
# ═══════════════════════════════════════════════════════════════

class TestSizingConstraints:
    """Tests compute_kelly() and size_portfolio(). Pure functions, no mocking."""

    def test_kelly_insufficient_history(self):
        """< MIN_KELLY_TRADES trades → DEFAULT_ALLOC (0.04)."""
        from core.portfolio import compute_kelly

        returns = [0.05] * (MIN_KELLY_TRADES - 1)
        assert compute_kelly(returns) == DEFAULT_ALLOC

    def test_kelly_valid_history(self, sample_returns):
        """200 returns → f = 0.5 * mu / var."""
        from core.portfolio import compute_kelly

        arr = np.array(sample_returns)
        mu = arr.mean()
        var = arr.var()

        result = compute_kelly(sample_returns)
        if mu > 0 and var > 0:
            expected = min(KELLY_FRAC * mu / var, 1.0)
            assert result == pytest.approx(expected, rel=1e-6)
        else:
            assert result == DEFAULT_ALLOC

    def test_sizing_empty(self):
        """Empty signals → empty sizing."""
        from core.portfolio import size_portfolio

        assert size_portfolio([], 0.04, 100_000) == []

    def test_sizing_budget_exhaustion(self):
        """Small budget → 1 contract each, limited by budget."""
        from core.portfolio import size_portfolio

        # 3 signals, each costs ~$700/contract
        signals = [
            ("AAPL", 6.30, 4),
            ("MSFT", 6.50, 4),
            ("GOOG", 6.00, 4),
        ]
        # Kelly fraction * account_value must be small so no extras
        result = size_portfolio(signals, 0.001, 50_000)

        # Each gets at least 1 contract (guaranteed)
        for ticker, contracts, deployed in result:
            assert contracts >= 1

    def test_sizing_max_contracts_cap(self):
        """Huge budget → capped at MAX_CONTRACTS per position."""
        from core.portfolio import size_portfolio

        signals = [("AAPL", 2.00, 4)]  # Very cheap
        result = size_portfolio(signals, 1.0, 10_000_000)

        for ticker, contracts, deployed in result:
            assert contracts <= MAX_CONTRACTS


# ═══════════════════════════════════════════════════════════════
#  CLASS 5: Trading Window Guard
# ═══════════════════════════════════════════════════════════════

class TestTradingWindowGuard:
    """Tests trader.check_optimal_window() with mocked datetime."""

    def _check_at_time(self, hour, minute):
        """Run check_optimal_window at a specific ET time."""
        from zoneinfo import ZoneInfo
        et = ZoneInfo("America/New_York")
        fixed = datetime(2026, 4, 29, hour, minute, 0, tzinfo=et)
        MockDatetime._fixed_now = fixed

        with patch("core.trader.datetime", MockDatetime):
            from core.trader import check_optimal_window
            return check_optimal_window()

    def test_within_window(self):
        """11:30 ET → True."""
        ok, msg = self._check_at_time(11, 30)
        assert ok is True
        assert "optimal window" in msg.lower() or "Within" in msg

    def test_before_open(self):
        """09:15 ET → False."""
        ok, msg = self._check_at_time(9, 15)
        assert ok is False

    def test_after_close(self):
        """15:30 ET → False."""
        ok, msg = self._check_at_time(15, 30)
        assert ok is False

    def test_boundaries(self):
        """10:00→True, 15:00→True, 09:59→False, 15:01→False."""
        ok_start, _ = self._check_at_time(10, 0)
        assert ok_start is True

        ok_end, _ = self._check_at_time(15, 0)
        assert ok_end is True

        ok_before, _ = self._check_at_time(9, 59)
        assert ok_before is False

        ok_after, _ = self._check_at_time(15, 1)
        assert ok_after is False


# ═══════════════════════════════════════════════════════════════
#  CLASS 6: Position Close Logic
# ═══════════════════════════════════════════════════════════════

class TestPositionCloseLogic:
    """Tests expiry detection and close P&L in autopilot.run_paper_trade()."""

    def _run_paper_with_positions(self, positions, pricing_map, signals_df=None):
        """Run run_paper_trade with mocked portfolio and pricing.

        Args:
            positions: list of position dicts
            pricing_map: dict ticker -> pricing dict (or None)
            signals_df: signals DataFrame (empty by default)
        """
        portfolio = {"positions": positions, "last_updated": None}

        if signals_df is None:
            signals_df = pd.DataFrame()

        with patch("core.autopilot.load_portfolio", return_value=portfolio), \
             patch("core.autopilot.save_portfolio"), \
             patch("core.autopilot.load_latest_signals", return_value=signals_df), \
             patch("core.autopilot.load_json", return_value={"trades": []}), \
             patch("core.autopilot.load_trade_history", return_value=[0.05] * 100), \
             patch("core.autopilot.compute_kelly", return_value=0.04), \
             patch("core.autopilot.record_trade"), \
             patch("core.autopilot._price_position") as mock_price:

            mock_price.side_effect = lambda pos: pricing_map.get(pos["ticker"])
            from core.autopilot import run_paper_trade
            return run_paper_trade()

    def test_expiring_detected(self):
        """Front exp tomorrow → position closed."""
        pos = _make_position("AAPL", days_to_front_exp=1)
        pricing = {"AAPL": {"current_cost": 7.00}}

        result = self._run_paper_with_positions([pos], pricing)
        assert len(result["closed"]) == 1
        assert result["closed"][0]["ticker"] == "AAPL"

    def test_non_expiring_kept(self):
        """Front exp +30d → not closed."""
        pos = _make_position("AAPL", days_to_front_exp=30)
        pricing = {"AAPL": {"current_cost": 7.00}}

        result = self._run_paper_with_positions([pos], pricing)
        assert len(result["closed"]) == 0

    def test_pricing_failure_skipped(self):
        """_price_position returns None → error logged, not closed."""
        pos = _make_position("AAPL", days_to_front_exp=1)
        pricing = {"AAPL": None}  # Pricing fails

        result = self._run_paper_with_positions([pos], pricing)
        assert len(result["closed"]) == 0
        assert len(result["errors"]) >= 1

    def test_close_pnl_correct(self):
        """P&L = (exit - entry) * 100 * contracts - commission."""
        entry_cost = 6.30
        exit_price = 7.50
        contracts = 2
        n_legs = 4
        pos = _make_position("AAPL", days_to_front_exp=0,
                             cost_per_share=entry_cost, contracts=contracts,
                             n_legs=n_legs)
        pricing = {"AAPL": {"current_cost": exit_price}}

        result = self._run_paper_with_positions([pos], pricing)
        assert len(result["closed"]) == 1

        closed = result["closed"][0]
        commission = n_legs * COMMISSION_LEG * contracts
        expected_pnl = (exit_price - entry_cost) * CONTRACT_MULT * contracts - commission
        assert closed["pnl"] == pytest.approx(expected_pnl, rel=1e-2)


# ═══════════════════════════════════════════════════════════════
#  CLASS 7: Daemon Scheduler
# ═══════════════════════════════════════════════════════════════

class TestDaemonScheduler:
    """Tests autopilot.DaemonScheduler and _is_weekday()."""

    def test_weekday_mon_fri(self):
        """weekday() 0-4 → True."""
        from core.autopilot import _is_weekday

        # Monday
        with patch("core.autopilot.datetime") as mock_dt:
            mock_dt.now.return_value = datetime(2026, 4, 27)  # Monday
            assert _is_weekday() is True

        # Friday
        with patch("core.autopilot.datetime") as mock_dt:
            mock_dt.now.return_value = datetime(2026, 5, 1)   # Friday
            assert _is_weekday() is True

    def test_weekend(self):
        """weekday() 5-6 → False."""
        from core.autopilot import _is_weekday

        # Saturday
        with patch("core.autopilot.datetime") as mock_dt:
            mock_dt.now.return_value = datetime(2026, 4, 25)  # Saturday
            assert _is_weekday() is False

        # Sunday
        with patch("core.autopilot.datetime") as mock_dt:
            mock_dt.now.return_value = datetime(2026, 4, 26)  # Sunday
            assert _is_weekday() is False

    def test_daemon_start_stop(self):
        """running=True after start, False after stop."""
        from core.autopilot import DaemonScheduler

        d = DaemonScheduler()
        assert d.running is False

        with patch.object(d, "_setup_scheduler"):
            d.start()
            assert d.running is True

            d.stop()
            assert d.running is False

    @patch("core.autopilot.run_scan")
    def test_scan_skips_weekend(self, mock_scan):
        """Weekend → _job_scan doesn't call run_scan."""
        from core.autopilot import DaemonScheduler

        d = DaemonScheduler()
        with patch("core.autopilot._is_weekday", return_value=False):
            d._job_scan()

        mock_scan.assert_not_called()

    @patch("core.autopilot.send_alert")
    def test_trade_no_ibkr_alerts(self, mock_alert):
        """No IBKR connection → send_alert called."""
        from core.autopilot import DaemonScheduler

        d = DaemonScheduler()
        d._config = {"email": {"enabled": False}}
        d._ib_state = None  # No IBKR

        with patch("core.autopilot._is_weekday", return_value=True):
            d._job_trade()

        mock_alert.assert_called_once()
        assert "IBKR" in mock_alert.call_args[0][0]


# ═══════════════════════════════════════════════════════════════
#  CLASS 8: Paper Trade Flow (end-to-end)
# ═══════════════════════════════════════════════════════════════

class TestPaperTradeFlow:
    """Tests autopilot.run_paper_trade() end-to-end scenarios."""

    def _make_signals_df(self, tickers):
        """Build a signals DataFrame for the given tickers."""
        rows = []
        for i, t in enumerate(tickers):
            rows.append({
                "ticker": t,
                "combo": "30-60",
                "strike": 180.0 + i * 10,
                "put_strike": 170.0 + i * 10,
                "stock_px": 185.0 + i * 10,
                "front_exp": "2026-06-01",
                "back_exp": "2026-07-01",
                "front_iv": 25.0,
                "back_iv": 24.0,
                "ff": 0.35 - i * 0.05,
                "call_cost": 3.50,
                "put_cost": 2.80,
                "dbl_cost": 6.30,
                "call_delta": 0.35,
                "put_delta": 0.35,
                "front_oi": 500,
                "back_oi": 400,
            })
        return pd.DataFrame(rows)

    @patch("core.autopilot.record_trade")
    @patch("core.autopilot.save_portfolio")
    @patch("core.autopilot.load_portfolio")
    def test_enters_new_positions(self, mock_load, mock_save, mock_record):
        """Empty portfolio + signals → positions entered."""
        portfolio = {"positions": [], "last_updated": None}
        mock_load.return_value = portfolio

        signals = self._make_signals_df(["AAPL", "MSFT"])

        with patch("core.autopilot.load_latest_signals", return_value=signals), \
             patch("core.autopilot.load_json", return_value={"trades": []}), \
             patch("core.autopilot.load_trade_history", return_value=[0.05] * 100), \
             patch("core.autopilot.compute_kelly", return_value=0.04), \
             patch("core.autopilot.size_portfolio") as mock_size, \
             patch("core.autopilot._price_position"):

            mock_size.return_value = [
                ("AAPL", 2, 1400.0),
                ("MSFT", 2, 1400.0),
            ]

            from core.autopilot import run_paper_trade
            result = run_paper_trade()

        assert len(result["entered"]) == 2
        entered_tickers = {p["ticker"] for p in result["entered"]}
        assert "AAPL" in entered_tickers
        assert "MSFT" in entered_tickers

    @patch("core.autopilot.record_trade")
    @patch("core.autopilot.save_portfolio")
    @patch("core.autopilot.load_portfolio")
    def test_skips_active_tickers(self, mock_load, mock_save, mock_record):
        """Active AAPL + AAPL signal → AAPL skipped."""
        active_pos = _make_position("AAPL", days_to_front_exp=30)
        portfolio = {"positions": [active_pos], "last_updated": None}
        mock_load.return_value = portfolio

        signals = self._make_signals_df(["AAPL", "MSFT"])

        with patch("core.autopilot.load_latest_signals", return_value=signals), \
             patch("core.autopilot.load_json", return_value={"trades": []}), \
             patch("core.autopilot.load_trade_history", return_value=[0.05] * 100), \
             patch("core.autopilot.compute_kelly", return_value=0.04), \
             patch("core.autopilot.size_portfolio") as mock_size, \
             patch("core.autopilot._price_position"):

            mock_size.return_value = [("MSFT", 2, 1400.0)]

            from core.autopilot import run_paper_trade
            result = run_paper_trade()

        entered_tickers = {p["ticker"] for p in result["entered"]}
        assert "AAPL" not in entered_tickers

    @patch("core.autopilot.record_trade")
    @patch("core.autopilot.save_portfolio")
    @patch("core.autopilot.load_portfolio")
    def test_portfolio_full(self, mock_load, mock_save, mock_record):
        """20 active positions → 0 entered."""
        positions = [_make_position(f"T{i:03d}", days_to_front_exp=30)
                     for i in range(MAX_POSITIONS)]
        portfolio = {"positions": positions, "last_updated": None}
        mock_load.return_value = portfolio

        signals = self._make_signals_df(["AAPL"])

        with patch("core.autopilot.load_latest_signals", return_value=signals), \
             patch("core.autopilot.load_json", return_value={"trades": []}), \
             patch("core.autopilot._price_position"):

            from core.autopilot import run_paper_trade
            result = run_paper_trade()

        assert len(result["entered"]) == 0

    @patch("core.autopilot.record_trade")
    @patch("core.autopilot.save_portfolio")
    @patch("core.autopilot.load_portfolio")
    def test_close_and_enter(self, mock_load, mock_save, mock_record):
        """1 expiring + signals → 1 closed + N entered."""
        expiring = _make_position("OLD", days_to_front_exp=0, contracts=1)
        active = _make_position("HOLD", days_to_front_exp=30)
        portfolio = {"positions": [expiring, active], "last_updated": None}
        mock_load.return_value = portfolio

        signals = self._make_signals_df(["AAPL", "MSFT"])

        with patch("core.autopilot.load_latest_signals", return_value=signals), \
             patch("core.autopilot.load_json", return_value={"trades": []}), \
             patch("core.autopilot.load_trade_history", return_value=[0.05] * 100), \
             patch("core.autopilot.compute_kelly", return_value=0.04), \
             patch("core.autopilot.size_portfolio") as mock_size, \
             patch("core.autopilot._price_position") as mock_price:

            mock_price.return_value = {"current_cost": 7.00}
            mock_size.return_value = [
                ("AAPL", 1, 700.0),
                ("MSFT", 1, 700.0),
            ]

            from core.autopilot import run_paper_trade
            result = run_paper_trade()

        assert len(result["closed"]) == 1
        assert result["closed"][0]["ticker"] == "OLD"
        assert len(result["entered"]) >= 1

    @patch("core.autopilot.record_trade")
    @patch("core.autopilot.save_portfolio")
    @patch("core.autopilot.load_portfolio")
    def test_no_signals(self, mock_load, mock_save, mock_record):
        """Empty signals DataFrame → 0 entered."""
        portfolio = {"positions": [], "last_updated": None}
        mock_load.return_value = portfolio

        with patch("core.autopilot.load_latest_signals", return_value=pd.DataFrame()), \
             patch("core.autopilot.load_json", return_value={"trades": []}), \
             patch("core.autopilot._price_position"):

            from core.autopilot import run_paper_trade
            result = run_paper_trade()

        assert len(result["entered"]) == 0
