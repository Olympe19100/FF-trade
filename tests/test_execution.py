"""
Unit Tests -- Execution Engine (core/execution.py)

Comprehensive mock-based tests for every IBKR interaction:
  1. _wait_for_fill: Filled, Cancelled, Inactive, timeout
  2. _build_combo: valid legs, missing conId, empty legs
  3. get_combo_price: ThetaData WS -> IBKR fallback -> both fail
  4. get_leg_price: ThetaData WS -> IBKR fallback -> both fail
  5. execute_combo: fill, rejection, timeout, EODHD fallback
  6. execute_leg: fill BUY/SELL, rejection, timeout, EODHD fallback, MKT fallback
  7. execute_spread: combo fill, legs fallback, partial, failed
  8. execute_spread_close: reverse actions, combo close, legs fallback

Usage:
    python -m pytest tests/test_execution.py -v
"""

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch, PropertyMock, call
import pytest

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# ============================================================
#  Mock ib_insync BEFORE importing core.execution
#  (ib_insync triggers asyncio event loop at import time)
# ============================================================

class _MockOption:
    def __init__(self, symbol="AAPL", strike=180.0, right="C",
                 exp="20260601", conId=12345):
        self.symbol = symbol
        self.strike = strike
        self.right = right
        self.lastTradeDateOrContractMonth = exp
        self.conId = conId

class _MockBag:
    def __init__(self):
        self.symbol = ""
        self.exchange = ""
        self.currency = ""
        self.comboLegs = []

class _MockComboLeg:
    def __init__(self):
        self.conId = 0
        self.ratio = 0
        self.action = ""
        self.exchange = ""

class _MockTagValue:
    def __init__(self, tag="", value=""):
        self.tag = tag
        self.value = value

class _MockLimitOrder:
    def __init__(self, action="", totalQuantity=0, lmtPrice=0.0):
        self.orderType = "LMT"
        self.action = action
        self.totalQuantity = totalQuantity
        self.lmtPrice = lmtPrice
        self.algoStrategy = ""
        self.algoParams = []
        self.outsideRth = True
        self.tif = ""

class _MockMarketOrder:
    def __init__(self, action="", totalQuantity=0):
        self.orderType = "MKT"
        self.action = action
        self.totalQuantity = totalQuantity
        self.outsideRth = True
        self.tif = ""

_ib_insync_mock = MagicMock()
_ib_insync_mock.IB = MagicMock
_ib_insync_mock.Option = _MockOption
_ib_insync_mock.Bag = _MockBag
_ib_insync_mock.ComboLeg = _MockComboLeg
_ib_insync_mock.LimitOrder = _MockLimitOrder
_ib_insync_mock.MarketOrder = _MockMarketOrder
_ib_insync_mock.TagValue = _MockTagValue

sys.modules.setdefault("ib_insync", _ib_insync_mock)
sys.modules.setdefault("websocket", MagicMock())

from core.execution import (
    _wait_for_fill,
    _build_combo,
    get_combo_price,
    get_leg_price,
    execute_combo,
    execute_leg,
    execute_spread,
    execute_spread_close,
)


# ============================================================
#  Mock helpers
# ============================================================

def make_option(symbol="AAPL", strike=180.0, right="C",
                exp="20260601", conId=12345):
    return _MockOption(symbol=symbol, strike=strike, right=right,
                       exp=exp, conId=conId)

def make_trade(status="Filled", avg_fill=5.00):
    trade = MagicMock()
    trade.orderStatus = MagicMock()
    trade.orderStatus.status = status
    trade.orderStatus.avgFillPrice = avg_fill
    return trade

def make_ticker(bid=4.90, ask=5.10):
    tk = MagicMock()
    tk.bid = bid
    tk.ask = ask
    return tk

def make_ib(ticker=None, trade=None):
    ib = MagicMock()
    ib.sleep = MagicMock()
    ib.ticker.return_value = ticker
    if trade is not None:
        ib.placeOrder.return_value = trade
    else:
        ib.placeOrder.return_value = make_trade("Filled", 5.00)
    return ib

def make_legs(symbol="AAPL", call_strike=180.0, put_strike=170.0,
              front_exp="20260515", back_exp="20260618"):
    """BUY back call, SELL front call, BUY back put, SELL front put."""
    return [
        (make_option(symbol, call_strike, "C", back_exp, conId=1001), "BUY"),
        (make_option(symbol, call_strike, "C", front_exp, conId=1002), "SELL"),
        (make_option(symbol, put_strike, "P", back_exp, conId=1003), "BUY"),
        (make_option(symbol, put_strike, "P", front_exp, conId=1004), "SELL"),
    ]


# ============================================================
#  1. _wait_for_fill
# ============================================================

class TestWaitForFill:

    def test_filled_immediately(self):
        ib = make_ib()
        trade = make_trade("Filled")
        assert _wait_for_fill(ib, trade, 10) == "Filled"

    def test_filled_after_delay(self):
        ib = make_ib()
        trade = make_trade("PreSubmitted")
        statuses = iter(["PreSubmitted", "Submitted", "Submitted", "Filled"])
        def next_status():
            try:
                return next(statuses)
            except StopIteration:
                return "Filled"
        type(trade.orderStatus).status = PropertyMock(side_effect=next_status)
        assert _wait_for_fill(ib, trade, 10) == "Filled"

    def test_cancelled(self):
        ib = make_ib()
        assert _wait_for_fill(ib, make_trade("Cancelled"), 10) == "Cancelled"

    def test_api_cancelled(self):
        ib = make_ib()
        assert _wait_for_fill(ib, make_trade("ApiCancelled"), 10) == "ApiCancelled"

    def test_inactive_rejected(self):
        ib = make_ib()
        assert _wait_for_fill(ib, make_trade("Inactive"), 10) == "Inactive"

    def test_timeout_returns_none(self):
        ib = make_ib()
        trade = make_trade("Submitted")
        assert _wait_for_fill(ib, trade, 3) is None
        assert ib.sleep.call_count == 3


# ============================================================
#  2. _build_combo
# ============================================================

class TestBuildCombo:

    def test_valid_4_leg_combo(self):
        combo = _build_combo(make_legs())
        assert combo is not None
        assert combo.symbol == "AAPL"
        assert combo.exchange == "SMART"
        assert combo.currency == "USD"
        assert len(combo.comboLegs) == 4

    def test_combo_leg_attributes(self):
        cl = _build_combo(make_legs()).comboLegs[0]
        assert cl.conId == 1001
        assert cl.ratio == 1
        assert cl.action == "BUY"
        assert cl.exchange == "SMART"

    def test_combo_actions_correct(self):
        actions = [cl.action for cl in _build_combo(make_legs()).comboLegs]
        assert actions == ["BUY", "SELL", "BUY", "SELL"]

    def test_empty_legs_returns_none(self):
        assert _build_combo([]) is None

    def test_missing_conid_returns_none(self):
        legs = make_legs()
        legs[2] = (make_option("AAPL", 170, "P", "20260618", conId=0), "BUY")
        assert _build_combo(legs) is None

    def test_missing_conid_first_leg(self):
        legs = make_legs()
        legs[0] = (make_option("AAPL", 180, "C", "20260618", conId=0), "BUY")
        assert _build_combo(legs) is None

    def test_2_leg_single_calendar(self):
        legs = [
            (make_option("AAPL", 180, "C", "20260618", conId=2001), "BUY"),
            (make_option("AAPL", 180, "C", "20260515", conId=2002), "SELL"),
        ]
        combo = _build_combo(legs)
        assert combo is not None
        assert len(combo.comboLegs) == 2


# ============================================================
#  3. get_combo_price
# ============================================================

class TestGetComboPrice:

    def test_thetadata_ws_primary(self):
        """ThetaData WS returns valid price -> use it, IBKR NOT called."""
        ib = make_ib()
        combo = MagicMock()
        legs = make_legs()

        with patch("core.theta_ws.theta_ws_get_combo_price",
                    return_value=(4.50, 5.50, 5.00)):
            bid, ask, mid = get_combo_price(ib, combo, legs=legs)

        assert bid == 4.50
        assert ask == 5.50
        assert mid == 5.00
        ib.reqMktData.assert_not_called()

    def test_theta_exception_falls_to_ibkr(self):
        """ThetaData raises exception -> IBKR fallback with valid data."""
        tk = make_ticker(bid=4.80, ask=5.20)
        ib = make_ib(ticker=tk)
        combo = MagicMock()
        legs = make_legs()

        with patch("core.theta_ws.theta_ws_get_combo_price",
                    side_effect=Exception("WS down")):
            bid, ask, mid = get_combo_price(ib, combo, legs=legs)

        assert bid == 4.80
        assert ask == 5.20
        assert mid == 5.00
        ib.reqMktData.assert_called_once()

    def test_ibkr_fallback_when_no_legs(self):
        """No legs passed -> skip ThetaData, go to IBKR reqMktData."""
        tk = make_ticker(bid=4.80, ask=5.20)
        ib = make_ib(ticker=tk)
        combo = MagicMock()

        bid, ask, mid = get_combo_price(ib, combo, legs=None)

        assert bid == 4.80
        assert ask == 5.20
        assert mid == 5.00
        ib.reqMktData.assert_called_once()
        ib.cancelMktData.assert_called_once()

    def test_ibkr_fallback_when_theta_returns_zero(self):
        """ThetaData WS returns mid=0 -> fallback to IBKR."""
        tk = make_ticker(bid=3.00, ask=3.50)
        ib = make_ib(ticker=tk)
        combo = MagicMock()
        legs = make_legs()

        with patch("core.theta_ws.theta_ws_get_combo_price",
                    return_value=(0.0, 0.0, 0.0)):
            bid, ask, mid = get_combo_price(ib, combo, legs=legs)

        assert mid == 3.25
        ib.reqMktData.assert_called_once()

    def test_both_fail_returns_zeros(self):
        """No ThetaData, no IBKR data -> (0, 0, 0)."""
        ib = make_ib(ticker=None)
        combo = MagicMock()
        bid, ask, mid = get_combo_price(ib, combo, legs=None)
        assert (bid, ask, mid) == (0.0, 0.0, 0.0)

    def test_cancel_mkt_data_always_called(self):
        """cancelMktData is called even when ticker returns None."""
        ib = make_ib(ticker=None)
        combo = MagicMock()
        get_combo_price(ib, combo, legs=None)
        ib.cancelMktData.assert_called_once_with(combo)

    def test_ibkr_handles_inf_values(self):
        tk = make_ticker(bid=float("inf"), ask=float("inf"))
        ib = make_ib(ticker=tk)
        bid, ask, mid = get_combo_price(ib, MagicMock(), legs=None)
        assert (bid, ask, mid) == (0.0, 0.0, 0.0)

    def test_ibkr_handles_negative_bid(self):
        tk = make_ticker(bid=-1.0, ask=5.0)
        ib = make_ib(ticker=tk)
        bid, ask, mid = get_combo_price(ib, MagicMock(), legs=None)
        assert bid == 0.0
        assert mid == 0.0

    def test_ibkr_bid_only_no_ask(self):
        """Bid valid but ask=0 -> mid must be 0 (needs both for mid)."""
        tk = make_ticker(bid=5.0, ask=0.0)
        ib = make_ib(ticker=tk)
        bid, ask, mid = get_combo_price(ib, MagicMock(), legs=None)
        # bid=5.0 passes filter (5.0 and 5.0 > 0), ask=0.0 fails (falsy)
        assert bid == 5.0
        assert ask == 0.0
        assert mid == 0.0  # both must be > 0 for mid computation


# ============================================================
#  4. get_leg_price
# ============================================================

class TestGetLegPrice:

    def test_thetadata_ws_primary(self):
        ib = make_ib()
        opt = make_option()

        with patch("core.theta_ws.theta_ws_get_leg_price",
                    return_value=(2.50, 2.80, 2.65)):
            bid, ask, mid = get_leg_price(ib, opt)

        assert (bid, ask, mid) == (2.50, 2.80, 2.65)
        ib.reqMktData.assert_not_called()

    def test_theta_exception_falls_to_ibkr(self):
        """ThetaData raises -> IBKR fallback with valid data."""
        tk = make_ticker(bid=2.40, ask=2.70)
        ib = make_ib(ticker=tk)
        opt = make_option()

        with patch("core.theta_ws.theta_ws_get_leg_price",
                    side_effect=Exception("WS down")):
            bid, ask, mid = get_leg_price(ib, opt)

        assert bid == 2.40
        assert ask == 2.70
        assert mid == 2.55
        ib.reqMktData.assert_called_once()

    def test_ibkr_fallback(self):
        """ThetaData returns zero -> IBKR reqMktData."""
        tk = make_ticker(bid=2.40, ask=2.70)
        ib = make_ib(ticker=tk)
        opt = make_option()

        with patch("core.theta_ws.theta_ws_get_leg_price",
                    return_value=(0.0, 0.0, 0.0)):
            bid, ask, mid = get_leg_price(ib, opt)

        assert (bid, ask, mid) == (2.40, 2.70, 2.55)
        ib.reqMktData.assert_called_once()

    def test_both_fail(self):
        ib = make_ib(ticker=None)
        opt = make_option()

        with patch("core.theta_ws.theta_ws_get_leg_price",
                    side_effect=Exception("WS not connected")):
            bid, ask, mid = get_leg_price(ib, opt)

        assert (bid, ask, mid) == (0.0, 0.0, 0.0)

    def test_cancel_mkt_data_always_called(self):
        """cancelMktData even when ticker is None (resource cleanup)."""
        ib = make_ib(ticker=None)
        opt = make_option()

        with patch("core.theta_ws.theta_ws_get_leg_price",
                    side_effect=Exception("down")):
            get_leg_price(ib, opt)

        ib.cancelMktData.assert_called_once_with(opt)

    def test_ibkr_inf_handling(self):
        tk = make_ticker(bid=float("inf"), ask=2.50)
        ib = make_ib(ticker=tk)
        opt = make_option()

        with patch("core.theta_ws.theta_ws_get_leg_price",
                    return_value=(0.0, 0.0, 0.0)):
            bid, ask, mid = get_leg_price(ib, opt)

        assert bid == 0.0
        assert mid == 0.0

    def test_ibkr_bid_only_no_ask(self):
        """Bid valid, ask=0 -> mid must be 0."""
        tk = make_ticker(bid=3.00, ask=0.0)
        ib = make_ib(ticker=tk)
        opt = make_option()

        with patch("core.theta_ws.theta_ws_get_leg_price",
                    return_value=(0.0, 0.0, 0.0)):
            bid, ask, mid = get_leg_price(ib, opt)

        assert mid == 0.0


# ============================================================
#  5. execute_combo
# ============================================================

class TestExecuteCombo:

    def test_buy_fills_at_mid(self):
        """BUY combo -> LMT @ mid, Adaptive algo optimizes."""
        trade = make_trade("Filled", avg_fill=5.05)
        ib = make_ib(trade=trade)
        combo = MagicMock()

        with patch("core.execution.get_combo_price",
                    return_value=(4.80, 5.30, 5.05)):
            filled, fill_px, slip = execute_combo(
                ib, combo, "BUY", 10, eodhd_mid=5.00, legs=None
            )

        assert filled is True
        assert fill_px == 5.05
        assert slip == pytest.approx(0.05)

        order = ib.placeOrder.call_args[0][1]
        assert order.lmtPrice == 5.05
        assert order.action == "BUY"
        assert order.totalQuantity == 10
        # Verify combo contract passed as 1st arg
        assert ib.placeOrder.call_args[0][0] is combo

    def test_sell_fills_at_mid(self):
        """SELL combo -> LMT @ mid."""
        trade = make_trade("Filled", avg_fill=5.05)
        ib = make_ib(trade=trade)
        combo = MagicMock()

        with patch("core.execution.get_combo_price",
                    return_value=(4.80, 5.30, 5.05)):
            filled, fill_px, slip = execute_combo(
                ib, combo, "SELL", 10, eodhd_mid=5.00, legs=None
            )

        assert filled is True
        assert fill_px == 5.05
        # SELL slippage: eodhd_mid - fill_px = 5.00 - 5.05 = -0.05
        assert slip == pytest.approx(-0.05)

        order = ib.placeOrder.call_args[0][1]
        assert order.lmtPrice == 5.05
        assert order.action == "SELL"

    def test_rejection_inactive(self):
        trade = make_trade("Inactive")
        ib = make_ib(trade=trade)
        combo = MagicMock()

        with patch("core.execution.get_combo_price",
                    return_value=(4.80, 5.30, 5.05)), \
             patch("core.execution._wait_for_fill", return_value="Inactive"):
            filled, fill_px, slip = execute_combo(
                ib, combo, "BUY", 10, eodhd_mid=5.00
            )

        assert filled is False
        assert fill_px == 0.0
        assert slip == 0.0

    def test_rejection_cancelled(self):
        """Cancelled status also returns (False, 0, 0)."""
        trade = make_trade("Cancelled")
        ib = make_ib(trade=trade)
        combo = MagicMock()

        with patch("core.execution.get_combo_price",
                    return_value=(4.80, 5.30, 5.05)), \
             patch("core.execution._wait_for_fill", return_value="Cancelled"):
            filled, fill_px, slip = execute_combo(
                ib, combo, "BUY", 10, eodhd_mid=5.00
            )

        assert filled is False

    def test_timeout_cancels_order(self):
        trade = make_trade("Submitted")
        ib = make_ib(trade=trade)
        combo = MagicMock()

        with patch("core.execution.get_combo_price",
                    return_value=(4.80, 5.30, 5.05)), \
             patch("core.execution._wait_for_fill", return_value=None):
            filled, fill_px, slip = execute_combo(
                ib, combo, "BUY", 10, eodhd_mid=5.00
            )

        assert filled is False
        ib.cancelOrder.assert_called_once()

    def test_eodhd_fallback_when_no_live_data(self):
        """No live data + EODHD -> bid=ask=mid=eodhd_mid."""
        trade = make_trade("Filled", avg_fill=5.50)
        ib = make_ib(trade=trade)
        combo = MagicMock()

        with patch("core.execution.get_combo_price",
                    return_value=(0.0, 0.0, 0.0)), \
             patch("core.execution._wait_for_fill", return_value="Filled"):
            filled, fill_px, slip = execute_combo(
                ib, combo, "BUY", 10, eodhd_mid=5.50
            )

        assert filled is True
        order = ib.placeOrder.call_args[0][1]
        assert order.lmtPrice == 5.50

    def test_negative_eodhd_mid_not_used(self):
        """eodhd_mid=-1.0 should NOT be used as fallback (condition: > 0)."""
        ib = make_ib()
        combo = MagicMock()

        with patch("core.execution.get_combo_price",
                    return_value=(0.0, 0.0, 0.0)):
            filled, fill_px, slip = execute_combo(
                ib, combo, "BUY", 10, eodhd_mid=-1.0
            )

        assert filled is False
        ib.placeOrder.assert_not_called()

    def test_no_price_at_all_skips(self):
        """No live data AND eodhd_mid=0 -> skip."""
        ib = make_ib()
        combo = MagicMock()

        with patch("core.execution.get_combo_price",
                    return_value=(0.0, 0.0, 0.0)):
            filled, fill_px, slip = execute_combo(
                ib, combo, "BUY", 10, eodhd_mid=0
            )

        assert filled is False
        ib.placeOrder.assert_not_called()

    def test_adaptive_algo_params(self):
        """Combo uses Adaptive Normal, outsideRth=False, tif=DAY."""
        trade = make_trade("Filled", avg_fill=5.00)
        ib = make_ib(trade=trade)
        combo = MagicMock()

        with patch("core.execution.get_combo_price",
                    return_value=(4.80, 5.30, 5.05)), \
             patch("core.execution._wait_for_fill", return_value="Filled"):
            execute_combo(ib, combo, "BUY", 10, eodhd_mid=5.00)

        order = ib.placeOrder.call_args[0][1]
        assert order.algoStrategy == "Adaptive"
        assert order.outsideRth is False
        assert order.tif == "DAY"
        assert any(
            tv.tag == "adaptivePriority" and tv.value == "Normal"
            for tv in order.algoParams
        )


# ============================================================
#  6. execute_leg
# ============================================================

class TestExecuteLeg:

    def test_buy_at_mid(self):
        """BUY leg -> LMT @ mid."""
        trade = make_trade("Filled", avg_fill=2.65)
        ib = make_ib(trade=trade)
        opt = make_option()

        with patch("core.execution.get_leg_price",
                    return_value=(2.50, 2.80, 2.65)), \
             patch("core.execution._wait_for_fill", return_value="Filled"):
            filled, fill_px = execute_leg(ib, opt, "BUY", 10)

        assert filled is True
        assert fill_px == 2.65
        order = ib.placeOrder.call_args[0][1]
        assert order.lmtPrice == 2.65
        assert order.totalQuantity == 10
        # Verify leg contract passed as 1st arg
        assert ib.placeOrder.call_args[0][0] is opt

    def test_sell_at_mid(self):
        """SELL leg -> LMT @ mid."""
        trade = make_trade("Filled", avg_fill=2.65)
        ib = make_ib(trade=trade)
        opt = make_option()

        with patch("core.execution.get_leg_price",
                    return_value=(2.50, 2.80, 2.65)), \
             patch("core.execution._wait_for_fill", return_value="Filled"):
            filled, fill_px = execute_leg(ib, opt, "SELL", 10)

        assert filled is True
        order = ib.placeOrder.call_args[0][1]
        assert order.lmtPrice == 2.65
        assert order.action == "SELL"

    def test_rejection(self):
        trade = make_trade("Inactive")
        ib = make_ib(trade=trade)
        opt = make_option()

        with patch("core.execution.get_leg_price",
                    return_value=(2.50, 2.80, 2.65)), \
             patch("core.execution._wait_for_fill", return_value="Inactive"):
            filled, fill_px = execute_leg(ib, opt, "BUY", 10)

        assert filled is False
        assert fill_px == 0.0

    def test_timeout_cancels(self):
        trade = make_trade("Submitted")
        ib = make_ib(trade=trade)
        opt = make_option()

        with patch("core.execution.get_leg_price",
                    return_value=(2.50, 2.80, 2.65)), \
             patch("core.execution._wait_for_fill", return_value=None):
            filled, fill_px = execute_leg(ib, opt, "BUY", 10)

        assert filled is False
        ib.cancelOrder.assert_called_once()

    def test_eodhd_fallback_sets_bid_ask_mid(self):
        """EODHD fallback: bid=ask=mid to avoid the $0.00 LMT bug."""
        trade = make_trade("Filled", avg_fill=3.00)
        ib = make_ib(trade=trade)
        opt = make_option()

        with patch("core.execution.get_leg_price",
                    return_value=(0.0, 0.0, 0.0)), \
             patch("core.execution._wait_for_fill", return_value="Filled"):
            filled, fill_px = execute_leg(ib, opt, "BUY", 10, eodhd_mid=3.00)

        assert filled is True
        order = ib.placeOrder.call_args[0][1]
        assert order.lmtPrice == 3.00
        assert order.lmtPrice > 0, "LMT must never be $0.00"

    def test_eodhd_fallback_sell_price_correct(self):
        """EODHD fallback SELL: LMT @ mid = eodhd_mid."""
        trade = make_trade("Filled", avg_fill=3.00)
        ib = make_ib(trade=trade)
        opt = make_option()

        with patch("core.execution.get_leg_price",
                    return_value=(0.0, 0.0, 0.0)), \
             patch("core.execution._wait_for_fill", return_value="Filled"):
            filled, fill_px = execute_leg(ib, opt, "SELL", 10, eodhd_mid=3.00)

        assert filled is True
        order = ib.placeOrder.call_args[0][1]
        assert order.lmtPrice == 3.00

    def test_mkt_fallback_when_no_price(self):
        """No live data AND no EODHD -> MKT order."""
        trade = make_trade("Filled", avg_fill=2.50)
        ib = make_ib(trade=trade)
        opt = make_option()

        with patch("core.execution.get_leg_price",
                    return_value=(0.0, 0.0, 0.0)), \
             patch("core.execution._wait_for_fill", return_value="Filled"):
            filled, fill_px = execute_leg(ib, opt, "BUY", 10, eodhd_mid=0)

        assert filled is True
        assert fill_px == 2.50
        order = ib.placeOrder.call_args[0][1]
        assert order.orderType == "MKT"
        assert order.outsideRth is False
        assert order.tif == "DAY"
        assert order.totalQuantity == 10

    def test_mkt_fallback_timeout(self):
        """MKT order times out -> cancel + (False, 0)."""
        trade = make_trade("Submitted")
        ib = make_ib(trade=trade)
        opt = make_option()

        with patch("core.execution.get_leg_price",
                    return_value=(0.0, 0.0, 0.0)), \
             patch("core.execution._wait_for_fill", return_value=None):
            filled, fill_px = execute_leg(ib, opt, "BUY", 10, eodhd_mid=0)

        assert filled is False
        ib.cancelOrder.assert_called_once()

    def test_mkt_rejection(self):
        """MKT order gets Cancelled -> (False, 0)."""
        trade = make_trade("Cancelled")
        ib = make_ib(trade=trade)
        opt = make_option()

        with patch("core.execution.get_leg_price",
                    return_value=(0.0, 0.0, 0.0)), \
             patch("core.execution._wait_for_fill", return_value="Cancelled"):
            filled, fill_px = execute_leg(ib, opt, "BUY", 10, eodhd_mid=0)

        assert filled is False
        assert fill_px == 0.0
        # MKT rejection does NOT call cancelOrder (only timeout does)
        ib.cancelOrder.assert_not_called()

    def test_adaptive_urgent_params(self):
        """Leg orders use Adaptive Urgent (not Normal like combo)."""
        trade = make_trade("Filled", avg_fill=2.80)
        ib = make_ib(trade=trade)
        opt = make_option()

        with patch("core.execution.get_leg_price",
                    return_value=(2.50, 2.80, 2.65)), \
             patch("core.execution._wait_for_fill", return_value="Filled"):
            execute_leg(ib, opt, "BUY", 10)

        order = ib.placeOrder.call_args[0][1]
        assert order.algoStrategy == "Adaptive"
        assert order.outsideRth is False
        assert order.tif == "DAY"
        assert any(
            tv.tag == "adaptivePriority" and tv.value == "Urgent"
            for tv in order.algoParams
        )


# ============================================================
#  7. execute_spread
# ============================================================

class TestExecuteSpread:

    def test_combo_fill_returns_full(self):
        legs = make_legs()
        ib = make_ib()

        with patch("core.execution._build_combo") as mock_bc, \
             patch("core.execution.execute_combo",
                    return_value=(True, 5.20, 0.20)):
            mock_bc.return_value = MagicMock()
            result, cost, slip, details = execute_spread(
                ib, "AAPL", legs, 4, 10, 5.00, "double"
            )

        assert result == "full"
        assert cost == 5.20
        assert slip == 0.20
        assert details["method"] == "combo"
        assert details["combo_attempted"] is True
        assert details["combo_result"] == "filled"

    def test_combo_fail_falls_to_legs(self):
        legs = make_legs()
        ib = make_ib()

        with patch("core.execution._build_combo") as mock_bc, \
             patch("core.execution.execute_combo",
                    return_value=(False, 0.0, 0.0)), \
             patch("core.execution.execute_leg",
                    return_value=(True, 1.50)):
            mock_bc.return_value = MagicMock()
            result, cost, slip, details = execute_spread(
                ib, "AAPL", legs, 4, 10, 5.00, "double"
            )

        assert result == "full"
        assert details["method"] == "legs"
        assert details["combo_result"] == "no_fill"
        assert len(details["leg_fills"]) == 4

    def test_leg_fill_cost_accounting(self):
        """BUY legs add to cost, SELL legs subtract."""
        legs = make_legs()
        ib = make_ib()

        fill_prices = iter([3.00, 1.50, 2.50, 1.00])

        def mock_leg(ib_, contract, action, contracts, eodhd_mid=0):
            return True, next(fill_prices)

        with patch("core.execution._build_combo", return_value=None), \
             patch("core.execution.execute_leg", side_effect=mock_leg):
            result, cost, slip, details = execute_spread(
                ib, "AAPL", legs, 4, 10, 5.00, "double"
            )

        assert result == "full"
        # net = +3.00 - 1.50 + 2.50 - 1.00 = 3.00
        assert cost == pytest.approx(3.00)
        # slip = 3.00 - 5.00 = -2.00
        assert slip == pytest.approx(-2.00)

    def test_est_leg_mid_calculation(self):
        """Per-leg EODHD mid = abs(eodhd_cps) / (n_legs // 2)."""
        legs = make_legs()
        ib = make_ib()

        captured_eodhd_mids = []

        def mock_leg(ib_, contract, action, contracts, eodhd_mid=0):
            captured_eodhd_mids.append(eodhd_mid)
            return True, 1.25

        with patch("core.execution._build_combo", return_value=None), \
             patch("core.execution.execute_leg", side_effect=mock_leg):
            execute_spread(ib, "AAPL", legs, 4, 10, 6.00, "double")

        # est_leg_mid = abs(6.00) / max(4 // 2, 1) = 6.00 / 2 = 3.00
        assert all(m == pytest.approx(3.00) for m in captured_eodhd_mids)

    def test_est_leg_mid_zero_when_eodhd_negative(self):
        """eodhd_cps <= 0 -> est_leg_mid = 0."""
        legs = make_legs()
        ib = make_ib()

        captured_eodhd_mids = []

        def mock_leg(ib_, contract, action, contracts, eodhd_mid=0):
            captured_eodhd_mids.append(eodhd_mid)
            return True, 1.25

        with patch("core.execution._build_combo", return_value=None), \
             patch("core.execution.execute_leg", side_effect=mock_leg):
            execute_spread(ib, "AAPL", legs, 4, 10, -1.0, "double")

        assert all(m == 0 for m in captured_eodhd_mids)

    def test_partial_fill_stops_on_failure(self):
        """2nd leg fails -> stops (no naked short), returns partial."""
        legs = make_legs()
        ib = make_ib()
        call_count = [0]

        def mock_leg(ib_, contract, action, contracts, eodhd_mid=0):
            call_count[0] += 1
            if call_count[0] == 1:
                return True, 3.00
            return False, 0.0

        with patch("core.execution._build_combo", return_value=None), \
             patch("core.execution.execute_leg", side_effect=mock_leg):
            result, cost, slip, details = execute_spread(
                ib, "AAPL", legs, 4, 10, 5.00, "double"
            )

        assert result == "partial"
        assert len(details["leg_fills"]) == 1
        assert call_count[0] == 2

    def test_all_fail_returns_failed(self):
        legs = make_legs()
        ib = make_ib()

        with patch("core.execution._build_combo", return_value=None), \
             patch("core.execution.execute_leg",
                    return_value=(False, 0.0)):
            result, cost, slip, details = execute_spread(
                ib, "AAPL", legs, 4, 10, 5.00, "double"
            )

        assert result == "failed"
        assert cost == 0.0
        assert len(details["leg_fills"]) == 0

    def test_cannot_build_combo_goes_to_legs(self):
        legs = make_legs()
        ib = make_ib()

        with patch("core.execution._build_combo", return_value=None), \
             patch("core.execution.execute_leg",
                    return_value=(True, 1.50)):
            result, cost, slip, details = execute_spread(
                ib, "AAPL", legs, 4, 10, 5.00, "double"
            )

        assert result == "full"
        assert details["method"] == "legs"
        assert details["combo_attempted"] is False


# ============================================================
#  8. execute_spread_close
# ============================================================

class TestExecuteSpreadClose:

    def test_close_reverses_contract_order_and_actions(self):
        """Close: reversed leg order + BUY<->SELL swap."""
        legs = make_legs()
        ib = make_ib()

        with patch("core.execution._build_combo") as mock_bc, \
             patch("core.execution.execute_combo",
                    return_value=(True, 4.80, 0.0)):
            mock_bc.return_value = MagicMock()
            success, exit_px, method, filled = execute_spread_close(
                ib, "AAPL", legs, 4, 10
            )

        assert success is True

        # Verify contracts AND actions
        close_legs = mock_bc.call_args[0][0]
        # Original: [(back_call,BUY), (front_call,SELL), (back_put,BUY), (front_put,SELL)]
        # reversed: [(front_put,SELL), (back_put,BUY), (front_call,SELL), (back_call,BUY)]
        # flipped:  [(front_put,BUY), (back_put,SELL), (front_call,BUY), (back_call,SELL)]
        close_contracts = [(c.right, c.strike, c.lastTradeDateOrContractMonth, a)
                          for c, a in close_legs]
        assert close_contracts == [
            ("P", 170.0, "20260515", "BUY"),   # front_put -> BUY (was SELL)
            ("P", 170.0, "20260618", "SELL"),   # back_put -> SELL (was BUY)
            ("C", 180.0, "20260515", "BUY"),    # front_call -> BUY (was SELL)
            ("C", 180.0, "20260618", "SELL"),    # back_call -> SELL (was BUY)
        ]

    def test_close_combo_uses_sell_action(self):
        """Close combo should call execute_combo with action='SELL', eodhd_mid=0."""
        legs = make_legs()
        ib = make_ib()

        with patch("core.execution._build_combo") as mock_bc, \
             patch("core.execution.execute_combo",
                    return_value=(True, 5.00, 0.0)) as mock_ec:
            mock_bc.return_value = MagicMock()
            execute_spread_close(ib, "AAPL", legs, 4, 10)

        args, kwargs = mock_ec.call_args
        assert args[2] == "SELL"           # action
        assert args[3] == 10              # contracts
        assert kwargs["eodhd_mid"] == 0   # no reference price for close

    def test_close_combo_fills(self):
        legs = make_legs()
        ib = make_ib()

        with patch("core.execution._build_combo") as mock_bc, \
             patch("core.execution.execute_combo",
                    return_value=(True, 5.00, 0.0)):
            mock_bc.return_value = MagicMock()
            success, exit_px, method, filled = execute_spread_close(
                ib, "AAPL", legs, 4, 10
            )

        assert success is True
        assert method == "combo"
        assert exit_px == 5.00

    def test_close_falls_to_legs(self):
        legs = make_legs()
        ib = make_ib()

        with patch("core.execution._build_combo", return_value=None), \
             patch("core.execution.execute_leg",
                    return_value=(True, 1.50)):
            success, exit_px, method, filled = execute_spread_close(
                ib, "AAPL", legs, 4, 10
            )

        assert success is True
        assert method == "legs"
        assert len(filled) == 4

    def test_close_legs_no_eodhd_mid(self):
        """Close legs are called WITHOUT eodhd_mid (defaults to 0)."""
        legs = make_legs()
        ib = make_ib()

        captured_calls = []

        def mock_leg(ib_, contract, action, contracts, eodhd_mid=0):
            captured_calls.append(eodhd_mid)
            return True, 1.50

        with patch("core.execution._build_combo", return_value=None), \
             patch("core.execution.execute_leg", side_effect=mock_leg):
            execute_spread_close(ib, "AAPL", legs, 4, 10)

        # All legs should have eodhd_mid=0 (no reference price for close)
        assert all(m == 0 for m in captured_calls)

    def test_close_leg_cost_accounting(self):
        """SELL adds to exit_price, BUY subtracts."""
        legs = make_legs()
        ib = make_ib()

        # Close legs (reversed+flipped): BUY, SELL, BUY, SELL
        prices = iter([0.80, 2.00, 1.20, 2.80])

        def mock_leg(ib_, contract, action, contracts, eodhd_mid=0):
            return True, next(prices)

        with patch("core.execution._build_combo", return_value=None), \
             patch("core.execution.execute_leg", side_effect=mock_leg):
            success, exit_px, method, filled = execute_spread_close(
                ib, "AAPL", legs, 4, 10
            )

        assert success is True
        # actions: BUY(-0.80), SELL(+2.00), BUY(-1.20), SELL(+2.80) = 2.80
        assert exit_px == pytest.approx(2.80)

    def test_close_partial_failure(self):
        legs = make_legs()
        ib = make_ib()
        call_count = [0]

        def mock_leg(ib_, contract, action, contracts, eodhd_mid=0):
            call_count[0] += 1
            if call_count[0] <= 2:
                return True, 1.50
            return False, 0.0

        with patch("core.execution._build_combo", return_value=None), \
             patch("core.execution.execute_leg", side_effect=mock_leg):
            success, exit_px, method, filled = execute_spread_close(
                ib, "AAPL", legs, 4, 10
            )

        assert success is False
        assert len(filled) == 2


# ============================================================
#  9. Integration / Regression scenarios
# ============================================================

class TestIntegrationScenarios:

    def test_full_entry_close_cycle(self):
        """Entry combo + close combo -> round-trip P&L."""
        legs = make_legs()
        ib = make_ib()

        with patch("core.execution._build_combo") as mock_bc, \
             patch("core.execution.execute_combo") as mock_ec:
            mock_bc.return_value = MagicMock()

            mock_ec.return_value = (True, 5.00, 0.10)
            result, entry_cost, slip, details = execute_spread(
                ib, "AAPL", legs, 4, 10, 4.90, "double"
            )
            assert result == "full"

            mock_ec.return_value = (True, 5.50, 0.0)
            success, exit_px, method, filled = execute_spread_close(
                ib, "AAPL", legs, 4, 10
            )
            assert success is True

            assert exit_px - entry_cost == pytest.approx(0.50)

    def test_entry_combo_fail_legs_fill_close_combo(self):
        legs = make_legs()
        ib = make_ib()

        with patch("core.execution._build_combo") as mock_bc, \
             patch("core.execution.execute_combo") as mock_ec, \
             patch("core.execution.execute_leg",
                    return_value=(True, 1.50)):
            mock_bc.return_value = MagicMock()

            mock_ec.return_value = (False, 0.0, 0.0)
            result, cost, slip, details = execute_spread(
                ib, "AAPL", legs, 4, 10, 5.00, "double"
            )
            assert result == "full"
            assert details["method"] == "legs"

            mock_ec.return_value = (True, 4.50, 0.0)
            success, exit_px, method, _ = execute_spread_close(
                ib, "AAPL", legs, 4, 10
            )
            assert success is True
            assert method == "combo"

    def test_eodhd_fallback_prevents_zero_limit(self):
        """Regression: the $0.00 LMT bug must never happen."""
        trade = make_trade("Filled", avg_fill=5.00)
        ib = make_ib(trade=trade)
        opt = make_option()

        with patch("core.execution.get_leg_price",
                    return_value=(0.0, 0.0, 0.0)), \
             patch("core.execution._wait_for_fill", return_value="Filled"):
            filled, fill_px = execute_leg(ib, opt, "BUY", 10, eodhd_mid=5.00)

        order = ib.placeOrder.call_args[0][1]
        assert order.lmtPrice == 5.00
        assert order.lmtPrice > 0, "LMT must never be $0.00"

    def test_pricing_waterfall_combo_theta_ibkr_eodhd(self):
        """Full waterfall: ThetaData=0 -> IBKR=0 -> EODHD mid used."""
        trade = make_trade("Filled", avg_fill=6.00)
        ib = make_ib(ticker=None, trade=trade)
        combo = MagicMock()
        legs = make_legs()

        with patch("core.theta_ws.theta_ws_get_combo_price",
                    return_value=(0.0, 0.0, 0.0)), \
             patch("core.execution._wait_for_fill", return_value="Filled"):
            filled, fill_px, slip = execute_combo(
                ib, combo, "BUY", 10, eodhd_mid=6.00, legs=legs
            )

        assert filled is True
        order = ib.placeOrder.call_args[0][1]
        # EODHD fallback: bid=ask=mid=6.00
        assert order.lmtPrice == 6.00
