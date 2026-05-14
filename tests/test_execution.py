"""
Unit Tests -- Execution Engine (core/execution.py)

Comprehensive mock-based tests for every IBKR interaction:
  1. _wait_for_fill: Filled, Cancelled, Inactive, timeout
  2. _build_combo: valid legs, missing conId, empty legs
  3. get_combo_price: ThetaData WS -> IBKR fallback -> both fail
  4. get_leg_price: ThetaData WS -> IBKR fallback -> both fail
  5. execute_combo: fill, rejection, timeout, EODHD fallback
  6. execute_leg: fill BUY/SELL, rejection, timeout, EODHD fallback, MKT fallback
  7. execute_spread: BAG-only combo fill, failed (no legs fallback)
  8. execute_spread_close: reverse actions, BAG-only combo close

Usage:
    python -m pytest tests/test_execution.py -v
"""

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch, PropertyMock
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
    _snipe_best_quote,
    get_combo_price,
    get_leg_price,
    execute_combo,
    execute_leg,
    _execute_spread_once,
    execute_spread,
    execute_spread_close,
    _compute_walk_step,
    _compute_walk_wait,
    _compute_initial_price,
    _compute_slice_pause,
    _compute_slice_size,
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

    def test_ibkr_synthesis_primary(self):
        """IBKR individual leg quotes synthesize combo price (primary source)."""
        tickers = {
            1001: make_ticker(bid=10.0, ask=10.50),   # back call BUY
            1002: make_ticker(bid=5.0, ask=5.50),      # front call SELL
            1003: make_ticker(bid=3.0, ask=3.50),      # back put BUY
            1004: make_ticker(bid=1.0, ask=1.50),      # front put SELL
        }
        ib = make_ib()
        ib.ticker.side_effect = lambda opt: tickers.get(getattr(opt, 'conId', 0), None)
        combo = MagicMock()
        legs = make_legs()

        bid, ask, mid = get_combo_price(ib, combo, legs=legs)

        # combo_bid = 10.0 - 5.50 + 3.0 - 1.50 = 6.0
        # combo_ask = 10.50 - 5.0 + 3.50 - 1.0 = 8.0
        assert bid == 6.0
        assert ask == 8.0
        assert mid == 7.0
        # reqMktData called for each of the 4 legs
        assert ib.reqMktData.call_count == 4

    def test_ibkr_synthesis_fails_theta_fallback(self):
        """IBKR leg quotes return nan -> ThetaData WS fallback."""
        ib = make_ib(ticker=make_ticker(bid=float("nan"), ask=float("nan")))
        combo = MagicMock()
        legs = make_legs()

        with patch("core.theta_ws.theta_ws_get_combo_price",
                    return_value=(4.50, 5.50, 5.00)):
            bid, ask, mid = get_combo_price(ib, combo, legs=legs)

        assert bid == 4.50
        assert ask == 5.50
        assert mid == 5.00

    def test_ibkr_fallback_when_no_legs(self):
        """No legs passed -> skip synthesis, go to IBKR BAG reqMktData."""
        tk = make_ticker(bid=4.80, ask=5.20)
        ib = make_ib(ticker=tk)
        combo = MagicMock()

        bid, ask, mid = get_combo_price(ib, combo, legs=None)

        assert bid == 4.80
        assert ask == 5.20
        assert mid == 5.00
        ib.reqMktData.assert_called_once()
        ib.cancelMktData.assert_called_once()

    def test_all_sources_fail_returns_zeros(self):
        """No IBKR, no ThetaData -> (0, 0, 0)."""
        ib = make_ib(ticker=None)
        combo = MagicMock()

        with patch("core.theta_ws.theta_ws_get_combo_price",
                    side_effect=Exception("WS down")):
            bid, ask, mid = get_combo_price(ib, combo, legs=make_legs())

        assert (bid, ask, mid) == (0.0, 0.0, 0.0)

    def test_ibkr_bag_handles_inf_values(self):
        tk = make_ticker(bid=float("inf"), ask=float("inf"))
        ib = make_ib(ticker=tk)
        bid, ask, mid = get_combo_price(ib, MagicMock(), legs=None)
        assert (bid, ask, mid) == (0.0, 0.0, 0.0)

    def test_ibkr_bag_handles_negative_bid(self):
        tk = make_ticker(bid=-1.0, ask=5.0)
        ib = make_ib(ticker=tk)
        bid, ask, mid = get_combo_price(ib, MagicMock(), legs=None)
        assert bid == 0.0
        assert mid == 0.0


# ============================================================
#  4. get_leg_price
# ============================================================

class TestGetLegPrice:

    def test_ibkr_primary(self):
        """IBKR returns valid data -> use it (primary source)."""
        tk = make_ticker(bid=2.50, ask=2.80)
        ib = make_ib(ticker=tk)
        opt = make_option()

        bid, ask, mid = get_leg_price(ib, opt)

        assert (bid, ask, mid) == (2.50, 2.80, 2.65)
        ib.reqMktData.assert_called_once()

    def test_ibkr_fails_theta_fallback(self):
        """IBKR returns nan -> ThetaData WS fallback."""
        ib = make_ib(ticker=make_ticker(bid=float("nan"), ask=float("nan")))
        opt = make_option()

        with patch("core.theta_ws.theta_ws_get_leg_price",
                    return_value=(2.40, 2.70, 2.55)):
            bid, ask, mid = get_leg_price(ib, opt)

        assert bid == 2.40
        assert ask == 2.70
        assert mid == 2.55

    def test_ibkr_no_ticker_theta_fallback(self):
        """IBKR ticker is None -> ThetaData WS fallback."""
        ib = make_ib(ticker=None)
        opt = make_option()

        with patch("core.theta_ws.theta_ws_get_leg_price",
                    return_value=(2.40, 2.70, 2.55)):
            bid, ask, mid = get_leg_price(ib, opt)

        assert (bid, ask, mid) == (2.40, 2.70, 2.55)

    def test_both_fail(self):
        ib = make_ib(ticker=None)
        opt = make_option()

        with patch("core.theta_ws.theta_ws_get_leg_price",
                    side_effect=Exception("WS not connected")):
            bid, ask, mid = get_leg_price(ib, opt)

        assert (bid, ask, mid) == (0.0, 0.0, 0.0)

    def test_cancel_mkt_data_always_called(self):
        """cancelMktData called for IBKR cleanup even when data is bad."""
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
            filled, fill_px, slip, _details = execute_combo(
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
            filled, fill_px, slip, _details = execute_combo(
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
            filled, fill_px, slip, _details = execute_combo(
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
            filled, fill_px, slip, _details = execute_combo(
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
            filled, fill_px, slip, _details = execute_combo(
                ib, combo, "BUY", 10, eodhd_mid=5.00
            )

        assert filled is False
        # cancel+replace: called for initial + each walk step
        assert ib.cancelOrder.call_count >= 1

    def test_eodhd_fallback_when_no_live_data(self):
        """No live data + EODHD -> bid=ask=mid=eodhd_mid."""
        trade = make_trade("Filled", avg_fill=5.50)
        ib = make_ib(trade=trade)
        combo = MagicMock()

        with patch("core.execution.get_combo_price",
                    return_value=(0.0, 0.0, 0.0)), \
             patch("core.execution._wait_for_fill", return_value="Filled"):
            filled, fill_px, slip, _details = execute_combo(
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
            filled, fill_px, slip, _details = execute_combo(
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
            filled, fill_px, slip, _details = execute_combo(
                ib, combo, "BUY", 10, eodhd_mid=0
            )

        assert filled is False
        ib.placeOrder.assert_not_called()

    def test_combo_order_params(self):
        """Combo uses plain LMT (no Adaptive — IBKR Error 329), outsideRth=False, tif=DAY."""
        trade = make_trade("Filled", avg_fill=5.00)
        ib = make_ib(trade=trade)
        combo = MagicMock()

        with patch("core.execution.get_combo_price",
                    return_value=(4.80, 5.30, 5.05)), \
             patch("core.execution._wait_for_fill", return_value="Filled"):
            execute_combo(ib, combo, "BUY", 10, eodhd_mid=5.00)

        order = ib.placeOrder.call_args[0][1]
        # No Adaptive algo on combos (IBKR doesn't support modify on BAG+Adaptive)
        assert not getattr(order, "algoStrategy", None)
        assert order.outsideRth is False
        assert order.tif == "DAY"


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
        # cancel+replace: called for initial + each walk step
        assert ib.cancelOrder.call_count >= 1

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
    """Test _execute_spread_once (single-batch, no slicing)."""

    def test_combo_fill_returns_full(self):
        legs = make_legs()
        ib = make_ib()

        with patch("core.execution._build_combo") as mock_bc, \
             patch("core.execution.execute_combo",
                    return_value=(True, 5.20, 0.20, {})):
            mock_bc.return_value = MagicMock()
            result, cost, slip, details = _execute_spread_once(
                ib, "AAPL", legs, 4, 5, 5.00, "double"
            )

        assert result == "full"
        assert cost == 5.20
        assert slip == 0.20
        assert details["method"] == "combo"
        assert details["combo_attempted"] is True
        assert details["combo_result"] == "filled"

    def test_combo_fail_returns_failed(self):
        """BAG-only: combo failure returns 'failed' (no legs fallback)."""
        legs = make_legs()
        ib = make_ib()

        with patch("core.execution._build_combo") as mock_bc, \
             patch("core.execution.execute_combo",
                    return_value=(False, 0.0, 0.0, {})):
            mock_bc.return_value = MagicMock()
            result, cost, slip, details = _execute_spread_once(
                ib, "AAPL", legs, 4, 5, 5.00, "double"
            )

        assert result == "failed"
        assert details["combo_result"] == "no_fill"
        assert len(details["leg_fills"]) == 0

    def test_cannot_build_combo_returns_failed(self):
        """BAG-only: cannot build combo returns 'failed' (no legs fallback)."""
        legs = make_legs()
        ib = make_ib()

        with patch("core.execution._build_combo", return_value=None):
            result, cost, slip, details = _execute_spread_once(
                ib, "AAPL", legs, 4, 5, 5.00, "double"
            )

        assert result == "failed"
        assert cost == 0.0
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
                    return_value=(True, 4.80, 0.0, {})):
            mock_bc.return_value = MagicMock()
            success, exit_px, method, filled = execute_spread_close(
                ib, "AAPL", legs, 4, 5
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
                    return_value=(True, 5.00, 0.0, {})) as mock_ec:
            mock_bc.return_value = MagicMock()
            execute_spread_close(ib, "AAPL", legs, 4, 5)

        args, kwargs = mock_ec.call_args
        assert args[2] == "SELL"           # action
        assert args[3] == 5               # contracts
        assert kwargs["eodhd_mid"] == 0   # no reference price for close

    def test_close_combo_fills(self):
        legs = make_legs()
        ib = make_ib()

        with patch("core.execution._build_combo") as mock_bc, \
             patch("core.execution.execute_combo",
                    return_value=(True, 5.00, 0.0, {})):
            mock_bc.return_value = MagicMock()
            success, exit_px, method, filled = execute_spread_close(
                ib, "AAPL", legs, 4, 5
            )

        assert success is True
        assert method == "combo"
        assert exit_px == 5.00

    def test_close_combo_fail_returns_failed(self):
        """BAG-only close: combo failure returns (False, 0, 'failed', [])."""
        legs = make_legs()
        ib = make_ib()

        with patch("core.execution._build_combo") as mock_bc, \
             patch("core.execution.execute_combo",
                    return_value=(False, 0.0, 0.0, {})):
            mock_bc.return_value = MagicMock()
            success, exit_px, method, filled = execute_spread_close(
                ib, "AAPL", legs, 4, 5
            )

        assert success is False
        assert method == "failed"
        assert len(filled) == 0

    def test_close_no_combo_returns_failed(self):
        """BAG-only close: cannot build combo returns failed."""
        legs = make_legs()
        ib = make_ib()

        with patch("core.execution._build_combo", return_value=None):
            success, exit_px, method, filled = execute_spread_close(
                ib, "AAPL", legs, 4, 5
            )

        assert success is False
        assert method == "failed"
        assert len(filled) == 0


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

            mock_ec.return_value = (True, 5.00, 0.10, {})
            result, entry_cost, slip, details = execute_spread(
                ib, "AAPL", legs, 4, 5, 4.90, "double"
            )
            assert result == "full"

            mock_ec.return_value = (True, 5.50, 0.0, {})
            success, exit_px, method, filled = execute_spread_close(
                ib, "AAPL", legs, 4, 5
            )
            assert success is True

            assert exit_px - entry_cost == pytest.approx(0.50)

    def test_entry_combo_fail_goes_to_pending(self):
        """BAG-only: combo failure returns 'failed' — signal goes to pending."""
        legs = make_legs()
        ib = make_ib()

        with patch("core.execution._build_combo") as mock_bc, \
             patch("core.execution.execute_combo") as mock_ec:
            mock_bc.return_value = MagicMock()

            mock_ec.return_value = (False, 0.0, 0.0, {})
            result, cost, slip, details = execute_spread(
                ib, "AAPL", legs, 4, 5, 5.00, "double"
            )
            assert result == "failed"
            assert cost == 0.0

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
            filled, fill_px, slip, _details = execute_combo(
                ib, combo, "BUY", 10, eodhd_mid=6.00, legs=legs
            )

        assert filled is True
        order = ib.placeOrder.call_args[0][1]
        # EODHD fallback: bid=ask=mid=6.00
        assert order.lmtPrice == 6.00


# ============================================================
#  10. Liquidity Guard — OI cap + filters
# ============================================================

from core.portfolio import size_portfolio, cost_per_contract
from core.config import MIN_OI_LEG, MAX_BA_PCT, MAX_PCT_OF_OI


class TestOICap:
    """OI-based contract cap in size_portfolio()."""

    def test_oi_cap_reduces_contracts(self):
        """100 OI with 5% cap -> max 5 contracts, even if budget says more."""
        # 6-tuple: (ticker, cps, n_legs, ff, ba_pct, min_leg_oi)
        signals_info = [
            ("AAPL", 2.00, 2, 0.30, 0.10, 100),
        ]
        result = size_portfolio(signals_info, 0.25, 500_000)
        ticker, n, deployed = result[0]
        assert ticker == "AAPL"
        assert n <= 5, f"Expected <= 5 contracts (5% of 100 OI), got {n}"
        assert n >= 1

    def test_oi_zero_no_cap(self):
        """OI=0 means data missing — no OI cap applied (only budget/MAX_CONTRACTS limits)."""
        # Same budget but OI=0 vs OI=50 — OI=0 should yield >= OI=50
        signals_oi0 = [("AAPL", 2.00, 2, 0.30, 0.10, 0)]
        signals_oi50 = [("AAPL", 2.00, 2, 0.30, 0.10, 50)]  # cap=2 (5% of 50)
        result_0 = size_portfolio(signals_oi0, 0.25, 500_000)
        result_50 = size_portfolio(signals_oi50, 0.25, 500_000)
        n_0 = result_0[0][1]
        n_50 = result_50[0][1]
        assert n_0 >= n_50, f"OI=0 should not cap: got {n_0} vs OI=50 capped at {n_50}"
        assert n_50 <= 2, f"OI=50 should be capped at 2, got {n_50}"

    def test_oi_cap_minimum_one_contract(self):
        """Even with very low OI (e.g. 5), cap is at least 1 contract."""
        signals_info = [
            ("TSLA", 5.00, 2, 0.50, 0.05, 5),
        ]
        result = size_portfolio(signals_info, 0.25, 100_000)
        ticker, n, deployed = result[0]
        assert n >= 1

    def test_oi_cap_large_oi_no_effect(self):
        """Large OI (10,000) -> cap=1000, budget likely smaller, no effect."""
        signals_info = [
            ("MSFT", 3.00, 2, 0.25, 0.05, 10_000),
        ]
        result = size_portfolio(signals_info, 0.04, 100_000)
        ticker, n, deployed = result[0]
        assert n > 0

    def test_oi_cap_multiple_signals(self):
        """Multiple signals: OI cap applied independently per signal."""
        signals_info = [
            ("AAPL", 2.00, 2, 0.30, 0.10, 50),   # cap=2 (5% of 50)
            ("MSFT", 2.00, 2, 0.30, 0.10, 500),   # cap=25 (5% of 500)
        ]
        result = size_portfolio(signals_info, 0.25, 1_000_000)
        aapl_n = result[0][1]
        msft_n = result[1][1]
        assert aapl_n <= 2, f"AAPL should be capped at 2, got {aapl_n}"
        assert msft_n <= 25, f"MSFT should be capped at 25, got {msft_n}"

    def test_backward_compat_3_tuple(self):
        """Old 3-tuple format still works — no cap applied."""
        signals_info = [
            ("AAPL", 2.00, 2),
        ]
        result = size_portfolio(signals_info, 0.04, 100_000)
        ticker, n, deployed = result[0]
        assert n > 0


class TestLiquidityWeightedSizing:
    """Liquidity-weighted dynamic sizing: liquid names get more contracts."""

    def test_liquid_gets_more_than_illiquid(self):
        """Same FF, same cost — liquid name (OI=5000) gets more than illiquid (OI=200).
        Use expensive options so MAX_CONTRACTS isn't the binding constraint."""
        signals_info = [
            ("AAPL", 50.00, 4, 0.20, 0.05, 5000),   # liquid, expensive
            ("ARDX", 50.00, 4, 0.20, 0.05, 200),     # illiquid, expensive
        ]
        result = size_portfolio(signals_info, 0.10, 500_000)
        aapl_n = result[0][1]
        ardx_n = result[1][1]
        assert aapl_n > ardx_n, \
            f"AAPL (OI=5000) should get more than ARDX (OI=200): {aapl_n} vs {ardx_n}"

    def test_very_illiquid_gets_minimum(self):
        """OI=100 name with full-size peers should get 1-5 contracts, not hundreds."""
        signals_info = [
            ("AAPL", 3.00, 4, 0.15, 0.05, 10000),  # very liquid
            ("TINY", 0.50, 4, 0.15, 0.05, 100),     # illiquid, cheap
        ]
        result = size_portfolio(signals_info, 0.10, 1_000_000)
        tiny_n = result[1][1]
        # OI=100 → cap at 5 (5% of 100), plus weight is low
        assert tiny_n <= 5, f"TINY (OI=100) should be <= 5, got {tiny_n}"

    def test_all_liquid_equal_gets_equal(self):
        """Same FF, same cost, same OI → roughly equal allocation."""
        signals_info = [
            ("A", 5.00, 4, 0.20, 0.05, 5000),
            ("B", 5.00, 4, 0.20, 0.05, 5000),
        ]
        result = size_portfolio(signals_info, 0.10, 200_000)
        a_n = result[0][1]
        b_n = result[1][1]
        assert a_n == b_n, f"Equal signals should get equal contracts: {a_n} vs {b_n}"

    def test_high_ff_liquid_beats_low_ff_illiquid(self):
        """High FF + liquid should dominate low FF + illiquid."""
        signals_info = [
            ("AAPL", 5.00, 4, 0.40, 0.03, 8000),   # high FF, liquid
            ("TINY", 5.00, 4, 0.10, 0.08, 150),     # low FF, illiquid
        ]
        result = size_portfolio(signals_info, 0.10, 500_000)
        aapl_n = result[0][1]
        tiny_n = result[1][1]
        assert aapl_n > tiny_n * 3, \
            f"AAPL (FF=0.4,OI=8k) should be >> TINY (FF=0.1,OI=150): {aapl_n} vs {tiny_n}"

    def test_wide_ba_gets_less(self):
        """Same OI, same FF — wide bid-ask should get less allocation.
        Use expensive options so MAX_CONTRACTS isn't the binding constraint."""
        signals_info = [
            ("TIGHT", 50.00, 4, 0.20, 0.02, 2000),  # tight spread, expensive
            ("WIDE",  50.00, 4, 0.20, 0.35, 2000),   # wide spread, expensive
        ]
        result = size_portfolio(signals_info, 0.10, 500_000)
        tight_n = result[0][1]
        wide_n = result[1][1]
        assert tight_n > wide_n, \
            f"Tight BA should get more than wide BA: {tight_n} vs {wide_n}"

    def test_catastrophe_prevented(self):
        """The ARDX scenario: cheap illiquid option should NOT get 1800 contracts."""
        signals_info = [
            ("ARDX", 0.05, 4, 0.50, 0.15, 120),  # very cheap, low OI
        ]
        result = size_portfolio(signals_info, 0.10, 1_000_000)
        n = result[0][1]
        # OI cap: max(1, int(120 * 0.05)) = 6
        assert n <= 6, f"ARDX should be capped at 6, got {n}"


class TestLiquidityFilters:
    """MIN_OI_LEG and MAX_BA_PCT filter constants are properly defined."""

    def test_min_oi_leg_constant(self):
        assert MIN_OI_LEG == 100

    def test_max_ba_pct_constant(self):
        assert MAX_BA_PCT == 0.40

    def test_max_pct_of_oi_constant(self):
        assert MAX_PCT_OF_OI == 0.05

    def test_oi_cap_formula(self):
        """Verify the OI cap formula: max(1, int(oi * 0.05))."""
        assert max(1, int(100 * MAX_PCT_OF_OI)) == 5
        assert max(1, int(50 * MAX_PCT_OF_OI)) == 2
        assert max(1, int(5 * MAX_PCT_OF_OI)) == 1   # int(0.25) = 0 -> max(1, 0) = 1
        assert max(1, int(1000 * MAX_PCT_OF_OI)) == 50


# ============================================================
#  11. LMT Progressive Walk
# ============================================================

from core.config import (
    LMT_WALK_MAX, SLICE_THRESHOLD, SLICE_SIZE, SLICE_PAUSE,
    SLICE_PAUSE_ILLIQUID,
)


class TestComboWalk:
    """Combo LMT @ mid with progressive walk toward ask/bid."""

    def test_walk_fills_on_second_step(self):
        """Timeout at mid, then fills on first walk step."""
        ib = make_ib()
        combo = MagicMock()

        # _wait_for_fill: None (timeout) on first call, "Filled" on second
        call_count = [0]
        def mock_wait(ib_, trade, seconds):
            call_count[0] += 1
            if call_count[0] == 1:
                return None  # initial attempt times out
            return "Filled"  # walk step fills

        trade = make_trade("Filled", avg_fill=5.10)
        ib.placeOrder.return_value = trade

        with patch("core.execution.get_combo_price",
                    return_value=(4.80, 5.30, 5.05)), \
             patch("core.execution._wait_for_fill", side_effect=mock_wait):
            filled, fill_px, slip, _details = execute_combo(
                ib, combo, "BUY", 3, eodhd_mid=5.00
            )

        assert filled is True
        assert fill_px == 5.10
        # placeOrder called: 1 initial + 1 walk step = 2
        assert ib.placeOrder.call_count == 2

    def test_walk_exhausted_cancels(self):
        """All walk steps timeout → cancel orders (cancel+replace pattern)."""
        ib = make_ib()
        combo = MagicMock()

        trade = make_trade("Submitted")
        ib.placeOrder.return_value = trade

        with patch("core.execution.get_combo_price",
                    return_value=(4.80, 5.30, 5.05)), \
             patch("core.execution._wait_for_fill", return_value=None):
            filled, fill_px, slip, _details = execute_combo(
                ib, combo, "BUY", 3, eodhd_mid=5.00
            )

        assert filled is False
        # cancel+replace: initial cancel + one per walk step
        assert ib.cancelOrder.call_count >= 1

    def test_sell_walks_down(self):
        """SELL combo walks price DOWN from mid toward bid."""
        ib = make_ib()
        combo = MagicMock()

        # Track all limit prices set
        limit_prices = []

        def track_place(contract, order):
            if hasattr(order, 'lmtPrice'):
                limit_prices.append(order.lmtPrice)
            return make_trade("Submitted")

        ib.placeOrder.side_effect = track_place

        with patch("core.execution.get_combo_price",
                    return_value=(4.80, 5.30, 5.05)), \
             patch("core.execution._wait_for_fill", return_value=None):
            execute_combo(ib, combo, "SELL", 3, eodhd_mid=5.00)

        # Initial price is mid=5.05, walk steps go down toward bid=4.80
        assert limit_prices[0] == 5.05  # initial
        for i in range(1, len(limit_prices)):
            assert limit_prices[i] < limit_prices[i - 1] or \
                   limit_prices[i] == limit_prices[i - 1], \
                f"SELL walk should go down: {limit_prices}"


class TestLegWalk:
    """Leg LMT @ mid with progressive walk."""

    def test_walk_fills_on_third_step(self):
        """Timeout at mid and first 2 walk steps, fills on 3rd."""
        ib = make_ib()
        opt = make_option()

        call_count = [0]
        def mock_wait(ib_, trade, seconds):
            call_count[0] += 1
            if call_count[0] <= 3:  # initial + 2 walk steps timeout
                return None
            return "Filled"  # 3rd walk step fills

        trade = make_trade("Filled", avg_fill=2.71)
        ib.placeOrder.return_value = trade

        with patch("core.execution.get_leg_price",
                    return_value=(2.50, 2.80, 2.65)), \
             patch("core.execution._wait_for_fill", side_effect=mock_wait):
            filled, fill_px = execute_leg(ib, opt, "BUY", 3)

        assert filled is True
        assert fill_px == 2.71
        # placeOrder: 1 initial + 3 walk steps = 4
        assert ib.placeOrder.call_count == 4

    def test_walk_exhausted_cancels(self):
        """All LMT_WALK_MAX steps timeout → cancel orders (cancel+replace)."""
        ib = make_ib()
        opt = make_option()

        trade = make_trade("Submitted")
        ib.placeOrder.return_value = trade

        with patch("core.execution.get_leg_price",
                    return_value=(2.50, 2.80, 2.65)), \
             patch("core.execution._wait_for_fill", return_value=None):
            filled, fill_px = execute_leg(ib, opt, "BUY", 3)

        assert filled is False
        # cancel+replace: initial cancel + one per walk step
        assert ib.cancelOrder.call_count >= 1
        # Initial + LMT_WALK_MAX walk steps (each a new order)
        assert ib.placeOrder.call_count == 1 + LMT_WALK_MAX

    def test_rejection_during_walk_stops(self):
        """Rejection mid-walk stops immediately."""
        ib = make_ib()
        opt = make_option()

        call_count = [0]
        def mock_wait(ib_, trade, seconds):
            call_count[0] += 1
            if call_count[0] == 1:
                return None  # initial timeout
            return "Inactive"  # rejected on first walk

        trade = make_trade("Inactive")
        ib.placeOrder.return_value = trade

        with patch("core.execution.get_leg_price",
                    return_value=(2.50, 2.80, 2.65)), \
             patch("core.execution._wait_for_fill", side_effect=mock_wait):
            filled, fill_px = execute_leg(ib, opt, "BUY", 3)

        assert filled is False
        # cancel+replace: initial order cancelled before walk starts
        assert ib.cancelOrder.call_count >= 1


# ============================================================
#  12. Order Slicing
# ============================================================

@patch("core.execution.ENABLE_JITTER", False)
class TestOrderSlicing:
    """Orders > SLICE_THRESHOLD get split into chunks."""

    def test_small_order_no_slicing(self):
        """contracts <= SLICE_THRESHOLD → no slicing, direct execution."""
        legs = make_legs()
        ib = make_ib()

        with patch("core.execution._build_combo") as mock_bc, \
             patch("core.execution.execute_combo",
                    return_value=(True, 5.00, 0.10, {})):
            mock_bc.return_value = MagicMock()
            result, cost, slip, details = execute_spread(
                ib, "AAPL", legs, 4, SLICE_THRESHOLD, 5.00, "double"
            )

        assert result == "full"
        assert "slices" not in details  # no slicing metadata

    def test_large_order_gets_sliced(self):
        """contracts > SLICE_THRESHOLD → sliced into chunks."""
        legs = make_legs()
        ib = make_ib()
        total_contracts = SLICE_THRESHOLD + 3  # e.g., 8

        with patch("core.execution._build_combo") as mock_bc, \
             patch("core.execution.execute_combo",
                    return_value=(True, 5.00, 0.10, {})):
            mock_bc.return_value = MagicMock()
            result, cost, slip, details = execute_spread(
                ib, "AAPL", legs, 4, total_contracts, 5.00, "double"
            )

        assert result == "full"
        assert "slices" in details
        # Should have 2 slices: SLICE_SIZE + remainder
        assert len(details["slices"]) == 2
        assert details["slices"][0]["contracts"] == SLICE_SIZE
        assert details["slices"][1]["contracts"] == total_contracts - SLICE_SIZE
        # Average cost should be 5.00 (all same price)
        assert cost == pytest.approx(5.00)

    def test_slice_failure_stops_and_returns_partial(self):
        """Second slice fails → return partial with first slice's data."""
        legs = make_legs()
        ib = make_ib()
        total_contracts = SLICE_SIZE * 2 + 1  # 11

        call_count = [0]
        def mock_combo(ib_, combo, action, contracts, eodhd_mid, legs=None, priority="Normal"):
            call_count[0] += 1
            if call_count[0] == 1:
                return True, 5.00, 0.10, {}  # first slice OK
            return False, 0.0, 0.0, {}  # second slice fails

        with patch("core.execution._build_combo") as mock_bc, \
             patch("core.execution.execute_combo", side_effect=mock_combo):
            mock_bc.return_value = MagicMock()
            result, cost, slip, details = execute_spread(
                ib, "AAPL", legs, 4, total_contracts, 5.00, "double"
            )

        # First slice filled, second failed → partial
        assert result == "partial"
        assert cost == pytest.approx(5.00)
        assert len(details["slices"]) == 2

    def test_slicing_constants(self):
        """Verify slicing constants from config."""
        assert SLICE_THRESHOLD == 5
        assert SLICE_SIZE == 5
        assert SLICE_PAUSE == 90

    def test_slice_pauses_between_chunks(self):
        """Verify adaptive ib.sleep() called between slices.

        With min_leg_oi=0 (default), adaptive pause = SLICE_PAUSE_ILLIQUID (120s).
        """
        legs = make_legs()
        ib = make_ib()
        total_contracts = SLICE_SIZE * 2  # exactly 2 slices

        with patch("core.execution._build_combo") as mock_bc, \
             patch("core.execution.execute_combo",
                    return_value=(True, 5.00, 0.10, {})):
            mock_bc.return_value = MagicMock()
            result, cost, slip, details = execute_spread(
                ib, "AAPL", legs, 4, total_contracts, 5.00, "double"
            )

        assert result == "full"
        # With OI=0 (default), adaptive pause = SLICE_PAUSE_ILLIQUID
        sleep_calls = [c[0][0] for c in ib.sleep.call_args_list]
        assert SLICE_PAUSE_ILLIQUID in sleep_calls, \
            f"Expected sleep({SLICE_PAUSE_ILLIQUID}) between slices, got: {sleep_calls}"


# ============================================================
#  13. Adaptive Walk Step (Cont & Kukanov 2017)
# ============================================================

@patch("core.execution.ENABLE_JITTER", False)
class TestComputeWalkStep:

    def test_normal_spread(self):
        """20% of $0.50 BA = $0.10."""
        step = _compute_walk_step(4.50, 5.00, fallback=0.05)
        assert step == pytest.approx(0.10)

    def test_tight_spread(self):
        """20% of $0.02 = $0.004, clamps to MIN $0.01."""
        step = _compute_walk_step(5.00, 5.02, fallback=0.05)
        assert step == 0.01

    def test_zero_bid(self):
        """bid=0 -> invalid BA, returns fallback."""
        step = _compute_walk_step(0, 5.00, fallback=0.05)
        assert step == 0.05

    def test_bid_equals_ask(self):
        """bid == ask -> no spread, returns fallback."""
        step = _compute_walk_step(5.00, 5.00, fallback=0.05)
        assert step == 0.05

    def test_bid_above_ask(self):
        """bid > ask (crossed market) -> fallback."""
        step = _compute_walk_step(5.10, 5.00, fallback=0.05)
        assert step == 0.05

    def test_large_spread(self):
        """20% of $2.00 BA = $0.40."""
        step = _compute_walk_step(10.00, 12.00, fallback=0.05)
        assert step == pytest.approx(0.40)


# ============================================================
#  14. Exponential Backoff (CFM 2018)
# ============================================================

@patch("core.execution.ENABLE_JITTER", False)
class TestComputeWalkWait:

    def test_step_0(self):
        """First walk step: 30 × 1.5^0 = 30s."""
        assert _compute_walk_wait(30, 0) == 30

    def test_step_1(self):
        """Second walk step: 30 × 1.25^1 = 37.5 → 37s."""
        assert _compute_walk_wait(30, 1) == 37

    def test_step_2(self):
        """Third walk step: 30 × 1.25^2 = 46.875 → 46s."""
        assert _compute_walk_wait(30, 2) == 46

    def test_step_3_capped(self):
        """Fourth walk step: 30 × 1.25^3 = 58.59 → 58s (< cap 60)."""
        assert _compute_walk_wait(30, 3) == 58

    def test_step_4_capped(self):
        """Fifth walk step: 30 × 1.25^4 = 73.24 → capped at 30 × 2.0 = 60."""
        assert _compute_walk_wait(30, 4) == 60

    def test_step_large_capped(self):
        """Very large step index → capped at initial × 2.0."""
        assert _compute_walk_wait(30, 10) == 60

    def test_leg_wait_step_0(self):
        """Leg walk: 15 × 1.25^0 = 15s."""
        assert _compute_walk_wait(15, 0) == 15

    def test_leg_wait_step_1(self):
        """Leg walk: 15 × 1.25^1 = 18.75 → 18s (int truncation)."""
        assert _compute_walk_wait(15, 1) == 18

    def test_leg_wait_step_2(self):
        """Leg walk: 15 × 1.25^2 = 23.4375 → 23s."""
        assert _compute_walk_wait(15, 2) == 23


# ============================================================
#  15. Adaptive Initial Price (Cont & Kukanov 2017)
# ============================================================

class TestComputeInitialPrice:

    def test_tight_spread_buy(self):
        """Tight spread (<$0.10): BUY → bid + tick."""
        px = _compute_initial_price("BUY", 5.00, 5.05, 5.025)
        assert px == 5.01

    def test_tight_spread_sell(self):
        """Tight spread (<$0.10): SELL → ask - tick."""
        px = _compute_initial_price("SELL", 5.00, 5.05, 5.025)
        assert px == 5.04

    def test_wide_spread_buy(self):
        """Wide spread (≥$0.10): BUY → mid."""
        px = _compute_initial_price("BUY", 4.80, 5.00, 4.90)
        assert px == 4.90

    def test_wide_spread_sell(self):
        """Wide spread (≥$0.10): SELL → mid."""
        px = _compute_initial_price("SELL", 4.80, 5.00, 4.90)
        assert px == 4.90

    def test_exact_threshold(self):
        """Spread exactly $0.10 → wide → mid."""
        px = _compute_initial_price("BUY", 5.00, 5.10, 5.05)
        assert px == 5.05

    def test_no_spread_info(self):
        """bid=0 → spread=0, falls through to mid."""
        px = _compute_initial_price("BUY", 0, 0, 5.00)
        assert px == 5.00

    def test_bid_ask_equal(self):
        """bid == ask → spread=0, falls through to mid."""
        px = _compute_initial_price("BUY", 5.00, 5.00, 5.00)
        assert px == 5.00


# ============================================================
#  16. Adaptive Slice Pause (Bouchaud et al. 2009)
# ============================================================

from core.config import (
    SLICE_PAUSE_LIQUID, SLICE_PAUSE_NORMAL, WALK_STEP_PCT, WALK_STEP_MIN, TIGHT_SPREAD_THRESHOLD,
    WALK_BACKOFF_FACTOR, WALK_BACKOFF_CAP,
    CLOSE_PAUSE, SWEET_SPOT_START_ET, SWEET_SPOT_END_ET,
    MAX_UNDERLYING_BA,
)


@patch("core.execution.ENABLE_JITTER", False)
class TestComputeSlicePause:

    def test_liquid(self):
        """OI >= 5000 → 60s."""
        assert _compute_slice_pause(5000) == SLICE_PAUSE_LIQUID
        assert _compute_slice_pause(10000) == SLICE_PAUSE_LIQUID

    def test_normal(self):
        """500 <= OI < 5000 → 90s."""
        assert _compute_slice_pause(500) == SLICE_PAUSE_NORMAL
        assert _compute_slice_pause(2000) == SLICE_PAUSE_NORMAL
        assert _compute_slice_pause(4999) == SLICE_PAUSE_NORMAL

    def test_illiquid(self):
        """OI < 500 → 120s."""
        assert _compute_slice_pause(499) == SLICE_PAUSE_ILLIQUID
        assert _compute_slice_pause(100) == SLICE_PAUSE_ILLIQUID
        assert _compute_slice_pause(0) == SLICE_PAUSE_ILLIQUID


# ============================================================
#  17. New Config Constants
# ============================================================

class TestNewConfigConstants:
    """Verify all new config constants exist with correct values."""

    def test_walk_step_pct(self):
        assert WALK_STEP_PCT == 0.20

    def test_walk_step_min(self):
        assert WALK_STEP_MIN == 0.01

    def test_tight_spread_threshold(self):
        assert TIGHT_SPREAD_THRESHOLD == 0.10

    def test_walk_backoff_factor(self):
        assert WALK_BACKOFF_FACTOR == 1.25

    def test_walk_backoff_cap(self):
        assert WALK_BACKOFF_CAP == 2.0

    def test_slice_pause_liquid(self):
        assert SLICE_PAUSE_LIQUID == 60

    def test_slice_pause_normal(self):
        assert SLICE_PAUSE_NORMAL == 90

    def test_slice_pause_illiquid(self):
        assert SLICE_PAUSE_ILLIQUID == 120

    def test_close_pause(self):
        assert CLOSE_PAUSE == 60

    def test_sweet_spot_window(self):
        assert SWEET_SPOT_START_ET == "10:30"
        assert SWEET_SPOT_END_ET == "11:30"

    def test_max_underlying_ba(self):
        assert MAX_UNDERLYING_BA == 0.001


# ============================================================
#  18. estimate_slippage (Almgren et al. 2005)
# ============================================================

from core.portfolio import estimate_slippage


class TestEstimateSlippage:

    def test_normal_case(self):
        """Standard case: positive inputs produce reasonable slippage."""
        slip = estimate_slippage(mid=5.00, contracts=10, daily_volume=1000,
                                 underlying_vol=0.30)
        assert 0.005 <= slip <= 0.50  # within clamped bounds

    def test_zero_volume_fallback(self):
        """volume=0 → fallback to SLIPPAGE_PER_LEG."""
        from core.config import SLIPPAGE_PER_LEG
        slip = estimate_slippage(mid=5.00, contracts=10, daily_volume=0,
                                 underlying_vol=0.30)
        assert slip == SLIPPAGE_PER_LEG

    def test_zero_mid_fallback(self):
        """mid=0 → fallback."""
        from core.config import SLIPPAGE_PER_LEG
        slip = estimate_slippage(mid=0, contracts=10, daily_volume=1000,
                                 underlying_vol=0.30)
        assert slip == SLIPPAGE_PER_LEG

    def test_clamp_minimum(self):
        """Very low participation → clamp at $0.005."""
        slip = estimate_slippage(mid=0.10, contracts=1, daily_volume=100000,
                                 underlying_vol=0.01)
        assert slip >= 0.005

    def test_clamp_maximum(self):
        """Very high participation → clamp at 10% of mid."""
        slip = estimate_slippage(mid=5.00, contracts=500, daily_volume=100,
                                 underlying_vol=0.80)
        assert slip <= 0.50  # 10% of 5.00

    def test_cost_per_contract_with_override(self):
        """slippage_override replaces default per-leg slippage."""
        cpc_default = cost_per_contract(5.00, 4)
        cpc_override = cost_per_contract(5.00, 4, slippage_override=0.50)
        assert cpc_override != cpc_default
        # With 0.50 override: (5.00 + 0.50) * 100 + 0.65*4 = 552.60
        assert cpc_override == pytest.approx(552.60)

    def test_cost_per_contract_backward_compat(self):
        """Without override: same as original formula."""
        from core.config import SLIPPAGE_PER_LEG, CONTRACT_MULT, COMMISSION_LEG
        cpc = cost_per_contract(5.00, 4)
        expected = (5.00 + SLIPPAGE_PER_LEG * 4) * CONTRACT_MULT + COMMISSION_LEG * 4
        assert cpc == pytest.approx(expected)


# ============================================================
#  19. Adaptive Slice Pause Integration
# ============================================================

@patch("core.execution.ENABLE_JITTER", False)
class TestAdaptiveSlicePauseIntegration:
    """execute_spread uses _compute_slice_pause(min_leg_oi) for inter-slice pause."""

    def test_slice_uses_adaptive_pause_liquid(self):
        """OI=10000 → slice pause = 60s."""
        legs = make_legs()
        ib = make_ib()
        total_contracts = SLICE_SIZE * 2

        with patch("core.execution._build_combo") as mock_bc, \
             patch("core.execution.execute_combo",
                    return_value=(True, 5.00, 0.10, {})):
            mock_bc.return_value = MagicMock()
            result, cost, slip, details = execute_spread(
                ib, "AAPL", legs, 4, total_contracts, 5.00, "double",
                min_leg_oi=10000
            )

        assert result == "full"
        sleep_calls = [c[0][0] for c in ib.sleep.call_args_list]
        assert SLICE_PAUSE_LIQUID in sleep_calls

    def test_slice_uses_adaptive_pause_illiquid(self):
        """OI=100 → slice pause = 120s."""
        legs = make_legs()
        ib = make_ib()
        total_contracts = SLICE_SIZE * 2

        with patch("core.execution._build_combo") as mock_bc, \
             patch("core.execution.execute_combo",
                    return_value=(True, 5.00, 0.10, {})):
            mock_bc.return_value = MagicMock()
            result, cost, slip, details = execute_spread(
                ib, "AAPL", legs, 4, total_contracts, 5.00, "double",
                min_leg_oi=100
            )

        assert result == "full"
        sleep_calls = [c[0][0] for c in ib.sleep.call_args_list]
        assert SLICE_PAUSE_ILLIQUID in sleep_calls


# ============================================================
#  20. Priority Parameter (Muravyev & Pearson 2020)
# ============================================================

class TestPriorityParam:
    """execute_combo accepts priority param and records it in exec_details."""

    def test_default_priority_normal(self):
        """Default priority is 'Normal' (recorded in details, not in algo)."""
        trade = make_trade("Filled", avg_fill=5.05)
        ib = make_ib(trade=trade)
        combo = MagicMock()

        with patch("core.execution.get_combo_price",
                    return_value=(4.80, 5.30, 5.05)):
            filled, fill_px, slip, details = execute_combo(
                ib, combo, "BUY", 10, eodhd_mid=5.00, legs=None
            )

        # Combo uses plain LMT (no Adaptive) — priority tracked in details
        assert details["priority"] == "Normal"

    def test_urgent_priority(self):
        """priority='Urgent' recorded in exec_details."""
        trade = make_trade("Filled", avg_fill=5.05)
        ib = make_ib(trade=trade)
        combo = MagicMock()

        with patch("core.execution.get_combo_price",
                    return_value=(4.80, 5.30, 5.05)):
            filled, fill_px, slip, details = execute_combo(
                ib, combo, "BUY", 10, eodhd_mid=5.00, legs=None,
                priority="Urgent"
            )

        assert details["priority"] == "Urgent"


# ============================================================
#  21. Timing Metadata (TCA support)
# ============================================================

class TestTimingMetadata:
    """execute_combo returns exec_details with timing and walk info."""

    def test_exec_details_on_fill(self):
        """Successful fill includes theta_bid/ask/mid, walk_steps, limits."""
        trade = make_trade("Filled", avg_fill=5.05)
        ib = make_ib(trade=trade)
        combo = MagicMock()

        with patch("core.execution.get_combo_price",
                    return_value=(4.80, 5.30, 5.05)):
            filled, fill_px, slip, details = execute_combo(
                ib, combo, "BUY", 10, eodhd_mid=5.00, legs=None
            )

        assert filled is True
        assert details["theta_bid"] == 4.80
        assert details["theta_ask"] == 5.30
        assert details["theta_mid"] == 5.05
        assert details["walk_steps"] == 0  # filled on initial attempt
        assert details["initial_limit"] == details["final_limit"]
        assert details["exec_seconds"] >= 0
        assert "priority" in details

    def test_exec_details_after_walk(self):
        """Walk steps counted in exec_details."""
        ib = make_ib()
        combo = MagicMock()

        call_count = [0]
        def mock_wait(ib_, trade, seconds):
            call_count[0] += 1
            if call_count[0] == 1:
                return None  # initial timeout
            return "Filled"  # first walk fills

        trade = make_trade("Filled", avg_fill=5.15)
        ib.placeOrder.return_value = trade

        with patch("core.execution.get_combo_price",
                    return_value=(4.80, 5.30, 5.05)), \
             patch("core.execution._wait_for_fill", side_effect=mock_wait):
            filled, fill_px, slip, details = execute_combo(
                ib, combo, "BUY", 3, eodhd_mid=5.00
            )

        assert filled is True
        assert details["walk_steps"] >= 1

    def test_exec_details_on_failure(self):
        """Even failed executions return exec_details."""
        ib = make_ib()
        combo = MagicMock()

        with patch("core.execution.get_combo_price",
                    return_value=(0.0, 0.0, 0.0)):
            filled, fill_px, slip, details = execute_combo(
                ib, combo, "BUY", 10, eodhd_mid=0
            )

        assert filled is False
        assert details == {}  # no price → empty details

    def test_spread_once_captures_timing(self):
        """_execute_spread_once captures exec_seconds in details."""
        legs = make_legs()
        ib = make_ib()

        with patch("core.execution._build_combo") as mock_bc, \
             patch("core.execution.execute_combo",
                    return_value=(True, 5.20, 0.20, {
                        "theta_bid": 4.80, "theta_ask": 5.30,
                        "theta_mid": 5.05, "walk_steps": 1,
                        "initial_limit": 5.05, "final_limit": 5.10,
                    })):
            mock_bc.return_value = MagicMock()
            result, cost, slip, details = _execute_spread_once(
                ib, "AAPL", legs, 4, 5, 5.00, "double"
            )

        assert result == "full"
        assert details["theta_bid"] == 4.80
        assert details["theta_ask"] == 5.30
        assert details["exec_seconds"] >= 0


# ============================================================
#  22. Adaptive Routing Integration
# ============================================================

class TestBagOnlyExecution:
    """BAG-only: _execute_spread_once always uses combo, no legs fallback."""

    def test_combo_always_attempted(self):
        """BAG-only: combo is always attempted (no adaptive routing skip)."""
        legs = make_legs()
        ib = make_ib()

        with patch("core.execution._build_combo") as mock_bc, \
             patch("core.execution.execute_combo",
                    return_value=(True, 5.20, 0.20, {})):
            mock_bc.return_value = MagicMock()
            result, cost, slip, details = _execute_spread_once(
                ib, "AAPL", legs, 4, 5, 5.00, "double"
            )

        assert result == "full"
        assert details["combo_attempted"] is True

    def test_priority_propagated_to_combo(self):
        """priority param passes through to execute_combo."""
        legs = make_legs()
        ib = make_ib()

        with patch("core.execution._build_combo") as mock_bc, \
             patch("core.execution.execute_combo",
                    return_value=(True, 5.20, 0.20, {"priority": "Urgent"})) as mock_ec:
            mock_bc.return_value = MagicMock()
            result, cost, slip, details = _execute_spread_once(
                ib, "AAPL", legs, 4, 5, 5.00, "double",
                priority="Urgent"
            )

        # Verify priority was passed to execute_combo
        args, kwargs = mock_ec.call_args
        assert kwargs.get("priority") == "Urgent"

    def test_fill_stats_import_failure_graceful(self):
        """If fill_db import fails, combo still attempted."""
        legs = make_legs()
        ib = make_ib()

        with patch("core.execution._build_combo") as mock_bc, \
             patch("core.execution.execute_combo",
                    return_value=(True, 5.20, 0.20, {})):
            mock_bc.return_value = MagicMock()
            # Don't patch fill_db — let it fail naturally
            result, cost, slip, details = _execute_spread_once(
                ib, "AAPL", legs, 4, 5, 5.00, "double"
            )

        assert result == "full"
        assert details["combo_attempted"] is True

    def test_combo_fail_no_leg_fallback(self):
        """BAG-only: combo failure → 'failed', execute_leg never called."""
        legs = make_legs()
        ib = make_ib()

        with patch("core.execution._build_combo") as mock_bc, \
             patch("core.execution.execute_combo",
                    return_value=(False, 0.0, 0.0, {})), \
             patch("core.execution.execute_leg") as mock_leg:
            mock_bc.return_value = MagicMock()
            result, cost, slip, details = _execute_spread_once(
                ib, "AAPL", legs, 4, 5, 5.00, "double"
            )

        assert result == "failed"
        mock_leg.assert_not_called()


# ============================================================
#  23. New Config Constants for Round 2
# ============================================================

from core.config import (
    SWEET_SPOT_GATE, SWEET_SPOT_AGGRESSION_BOOST,
    ENABLE_NETTING, USE_ALMGREN_SIZING,
    COMBO_SKIP_THRESHOLD, MIN_ATTEMPTS_FOR_ROUTING,
    JITTER_PCT, ENABLE_JITTER,
    BA_WIDEN_ABORT, ENABLE_BA_CIRCUIT_BREAKER,
    MAX_PARTICIPATION_RATE, ENABLE_PARTICIPATION_CAP,
)
from core.execution import _jitter


class TestRound2ConfigConstants:

    def test_sweet_spot_gate_default(self):
        assert SWEET_SPOT_GATE is False

    def test_sweet_spot_aggression_boost_default(self):
        assert SWEET_SPOT_AGGRESSION_BOOST is True

    def test_enable_netting_default(self):
        assert ENABLE_NETTING is True

    def test_use_almgren_sizing_default(self):
        assert USE_ALMGREN_SIZING is True

    def test_combo_skip_threshold(self):
        assert COMBO_SKIP_THRESHOLD == 0.25

    def test_min_attempts_for_routing(self):
        assert MIN_ATTEMPTS_FOR_ROUTING == 3


# ============================================================
#  24. Anti-Gaming Randomization (Wyart et al. 2008, BIS 2020)
# ============================================================

class TestJitter:
    """_jitter applies ±JITTER_PCT random variance to values."""

    def test_jitter_enabled_varies_output(self):
        """With ENABLE_JITTER=True, output varies from input."""
        results = set()
        for _ in range(50):
            results.add(round(_jitter(100.0), 2))
        # Should produce multiple distinct values
        assert len(results) > 1

    def test_jitter_stays_within_bounds(self):
        """Output is within [value*(1-JITTER_PCT), value*(1+JITTER_PCT)]."""
        for _ in range(100):
            val = _jitter(100.0)
            assert 100.0 * (1 - JITTER_PCT) <= val <= 100.0 * (1 + JITTER_PCT)

    def test_jitter_disabled_returns_exact(self):
        """With ENABLE_JITTER=False, returns exact value."""
        with patch("core.execution.ENABLE_JITTER", False):
            assert _jitter(100.0) == 100.0
            assert _jitter(42.5) == 42.5

    def test_jitter_zero_returns_zero(self):
        """Jitter of zero is still zero."""
        assert _jitter(0.0) == 0.0

    def test_walk_step_jittered(self):
        """_compute_walk_step with jitter produces varied results."""
        results = set()
        for _ in range(50):
            step = _compute_walk_step(4.50, 5.00, fallback=0.05)
            results.add(round(step, 4))
        assert len(results) > 1

    def test_walk_wait_jittered(self):
        """_compute_walk_wait with jitter produces varied results."""
        results = set()
        for _ in range(50):
            wait = _compute_walk_wait(30, 0)
            results.add(wait)
        assert len(results) > 1

    def test_slice_pause_jittered(self):
        """_compute_slice_pause with jitter produces varied results."""
        results = set()
        for _ in range(50):
            pause = _compute_slice_pause(5000)
            results.add(pause)
        assert len(results) > 1


# ============================================================
#  25. Round 3 Config Constants
# ============================================================

class TestRound3ConfigConstants:

    def test_jitter_pct(self):
        assert JITTER_PCT == 0.20

    def test_enable_jitter_default(self):
        assert ENABLE_JITTER is True

    def test_ba_widen_abort(self):
        assert BA_WIDEN_ABORT == 2.0

    def test_enable_ba_circuit_breaker_default(self):
        assert ENABLE_BA_CIRCUIT_BREAKER is True

    def test_max_participation_rate(self):
        assert MAX_PARTICIPATION_RATE == 0.05

    def test_enable_participation_cap_default(self):
        assert ENABLE_PARTICIPATION_CAP is True


# ============================================================
#  26. New Config Constants (Execution Fixes)
# ============================================================

from core.config import IBKR_QUOTE_SETTLE, INTER_SIGNAL_PAUSE, MAX_COMBO_WALK_STEPS


class TestExecutionFixesConfig:
    """New config constants from execution engine fixes."""

    def test_ibkr_quote_settle(self):
        assert IBKR_QUOTE_SETTLE == 5

    def test_inter_signal_pause(self):
        assert INTER_SIGNAL_PAUSE == 5

    def test_max_combo_walk_steps(self):
        assert MAX_COMBO_WALK_STEPS == 8


# ============================================================
#  27. Adaptive Slice Size
# ============================================================

class TestComputeSliceSize:
    """_compute_slice_size adapts to OI."""

    def test_oi_zero_returns_default(self):
        """OI=0 (unknown) → use default SLICE_SIZE."""
        assert _compute_slice_size(10, 0) == SLICE_SIZE

    def test_oi_negative_returns_default(self):
        """OI<0 (invalid) → use default SLICE_SIZE."""
        assert _compute_slice_size(10, -1) == SLICE_SIZE

    def test_oi_150_caps_at_3(self):
        """OI=150 → 2% = 3 contracts per slice."""
        assert _compute_slice_size(10, 150) == 3

    def test_oi_50_caps_at_1(self):
        """OI=50 → 2% = 1 contract per slice."""
        assert _compute_slice_size(10, 50) == 1

    def test_oi_10000_caps_at_slice_size(self):
        """OI=10000 → 2% = 200, capped at SLICE_SIZE=5."""
        assert _compute_slice_size(10, 10000) == SLICE_SIZE

    def test_oi_500_caps_at_slice_size(self):
        """OI=500 → 2% = 10, capped at SLICE_SIZE=5."""
        assert _compute_slice_size(10, 500) == SLICE_SIZE

    def test_oi_200_returns_4(self):
        """OI=200 → 2% = 4 contracts per slice."""
        assert _compute_slice_size(10, 200) == 4


# ============================================================
#  28. MAX_COMBO_WALK_STEPS cap
# ============================================================

@patch("core.execution.ENABLE_JITTER", False)
class TestMaxComboWalkSteps:
    """Walk steps capped at MAX_COMBO_WALK_STEPS."""

    def test_walk_steps_capped(self):
        """Very wide spread → many potential steps, but capped at MAX_COMBO_WALK_STEPS."""
        ib = make_ib()
        combo = MagicMock()

        # Track how many walk orders are placed
        walk_orders = []
        def track_place(contract, order):
            if hasattr(order, 'lmtPrice'):
                walk_orders.append(order.lmtPrice)
            return make_trade("Submitted")

        ib.placeOrder.side_effect = track_place

        # Very wide spread: bid=1.00, ask=10.00 → many walk steps possible
        with patch("core.execution.get_combo_price",
                    return_value=(1.00, 10.00, 5.50)), \
             patch("core.execution._wait_for_fill", return_value=None):
            filled, fill_px, slip, details = execute_combo(
                ib, combo, "BUY", 3, eodhd_mid=5.00
            )

        assert filled is False
        # 1 initial + at most MAX_COMBO_WALK_STEPS walk steps
        assert ib.placeOrder.call_count <= 1 + MAX_COMBO_WALK_STEPS


# ============================================================
#  Round 2 Config Constants
# ============================================================

class TestRound2NewConfigConstants:
    """Verify new Round 2 config constants exist with expected defaults."""

    def test_quote_snipe_samples(self):
        from core.config import QUOTE_SNIPE_SAMPLES
        assert QUOTE_SNIPE_SAMPLES == 3

    def test_quote_snipe_interval(self):
        from core.config import QUOTE_SNIPE_INTERVAL
        assert QUOTE_SNIPE_INTERVAL == 5

    def test_fallback_exchange(self):
        from core.config import FALLBACK_EXCHANGE
        assert FALLBACK_EXCHANGE == "BOX"

    def test_enable_exchange_fallback(self):
        from core.config import ENABLE_EXCHANGE_FALLBACK
        assert ENABLE_EXCHANGE_FALLBACK is True

    def test_retry_time_et(self):
        from core.config import RETRY_TIME_ET
        assert RETRY_TIME_ET == "13:30"

    def test_enable_intraday_retry(self):
        from core.config import ENABLE_INTRADAY_RETRY
        assert ENABLE_INTRADAY_RETRY is True


# ============================================================
#  Quote Sniping
# ============================================================

@patch("core.execution.ENABLE_JITTER", False)
class TestSnipeBestQuote:
    """Tests for _snipe_best_quote."""

    def test_returns_tightest_spread(self):
        """Should return the sample with the smallest bid-ask spread."""
        ib = make_ib()
        combo = MagicMock()

        quotes = [
            (1.00, 1.40, 1.20),  # spread=0.40
            (1.05, 1.25, 1.15),  # spread=0.20 ← tightest
            (1.00, 1.30, 1.15),  # spread=0.30
        ]
        with patch("core.execution.get_combo_price", side_effect=quotes):
            bid, ask, mid = _snipe_best_quote(ib, combo, None, 3, 5)

        assert bid == 1.05
        assert ask == 1.25
        assert mid == 1.15

    def test_early_exit_on_tight_spread(self):
        """Should stop sampling when spread <= TIGHT_SPREAD_THRESHOLD."""
        ib = make_ib()
        combo = MagicMock()

        quotes = [
            (1.00, 1.08, 1.04),  # spread=0.08 < 0.10 threshold
            (1.05, 1.25, 1.15),  # should not be called
        ]
        with patch("core.execution.get_combo_price", side_effect=quotes) as mock_gcp:
            bid, ask, mid = _snipe_best_quote(ib, combo, None, 3, 5)

        assert mock_gcp.call_count == 1  # early exit after first
        assert bid == 1.00
        assert ask == 1.08

    def test_all_fail_returns_zeros(self):
        """Should return (0, 0, 0) if all samples fail."""
        ib = make_ib()
        combo = MagicMock()

        with patch("core.execution.get_combo_price", return_value=(0, 0, 0)):
            bid, ask, mid = _snipe_best_quote(ib, combo, None, 3, 5)

        assert (bid, ask, mid) == (0.0, 0.0, 0.0)

    def test_single_sample_mode(self):
        """With samples=1, should call get_combo_price once."""
        ib = make_ib()
        combo = MagicMock()

        with patch("core.execution.get_combo_price",
                    return_value=(2.00, 2.20, 2.10)) as mock_gcp:
            bid, ask, mid = _snipe_best_quote(ib, combo, None, 1, 5)

        assert mock_gcp.call_count == 1
        assert mid == 2.10


# ============================================================
#  BOX Exchange Fallback
# ============================================================

@patch("core.execution.ENABLE_JITTER", False)
class TestBOXFallback:
    """Tests for BOX exchange fallback in _execute_spread_once."""

    def test_build_combo_exchange_param(self):
        """_build_combo should set exchange on combo and legs."""
        legs = make_legs()[:2]  # 2 legs
        combo = _build_combo(legs, exchange="BOX")
        assert combo is not None
        assert combo.exchange == "BOX"
        for cl in combo.comboLegs:
            assert cl.exchange == "BOX"

    def test_build_combo_default_smart(self):
        """_build_combo default should use SMART."""
        legs = make_legs()[:2]
        combo = _build_combo(legs)
        assert combo.exchange == "SMART"

    @patch("core.execution.QUOTE_SNIPE_SAMPLES", 1)
    @patch("core.execution.ENABLE_EXCHANGE_FALLBACK", True)
    @patch("core.execution.FALLBACK_EXCHANGE", "BOX")
    def test_fallback_fills_on_box(self):
        """SMART fails → BOX fallback fills."""
        ib = make_ib()
        legs = make_legs()[:2]

        # First execute_combo call (SMART) fails, second (BOX) fills
        with patch("core.execution.get_combo_price",
                    return_value=(1.00, 1.20, 1.10)), \
             patch("core.execution.execute_combo") as mock_exec, \
             patch("core.execution._update_fill_stats_safe"):

            # SMART attempt returns no fill
            mock_exec.side_effect = [
                (False, 0.0, 0.0, {"walk_steps": 3, "theta_bid": 1.0,
                                   "theta_ask": 1.2, "theta_mid": 1.1,
                                   "initial_limit": 1.1, "final_limit": 1.2}),
                # BOX attempt returns fill
                (True, 1.15, 0.05, {"walk_steps": 1, "theta_bid": 1.0,
                                    "theta_ask": 1.2, "theta_mid": 1.1,
                                    "initial_limit": 1.1, "final_limit": 1.15}),
            ]
            result, fill_px, slip, details = _execute_spread_once(
                ib, "AAPL", legs, 2, 1, 1.10, "single"
            )

        assert result == "full"
        assert details["method"] == "combo_BOX"
        assert details["fallback_attempted"] is True

    @patch("core.execution.QUOTE_SNIPE_SAMPLES", 1)
    @patch("core.execution.ENABLE_EXCHANGE_FALLBACK", False)
    def test_fallback_disabled_skips_box(self):
        """When ENABLE_EXCHANGE_FALLBACK=False, should not try BOX."""
        ib = make_ib()
        legs = make_legs()[:2]

        with patch("core.execution.get_combo_price",
                    return_value=(1.00, 1.20, 1.10)), \
             patch("core.execution.execute_combo",
                    return_value=(False, 0.0, 0.0, {"walk_steps": 3})) as mock_exec, \
             patch("core.execution._update_fill_stats_safe"):

            result, fill_px, slip, details = _execute_spread_once(
                ib, "AAPL", legs, 2, 1, 1.10, "single"
            )

        assert result == "failed"
        # execute_combo called only once (SMART), not twice (no BOX)
        assert mock_exec.call_count == 1

    @patch("core.execution.QUOTE_SNIPE_SAMPLES", 1)
    @patch("core.execution.ENABLE_EXCHANGE_FALLBACK", True)
    def test_close_fallback_fills_on_box(self):
        """Close: SMART fails → BOX fallback fills."""
        ib = make_ib()
        legs = make_legs()[:2]

        call_count = [0]

        def mock_exec_combo(ib_, combo, action, contracts, eodhd_mid, legs=None, **kw):
            call_count[0] += 1
            if call_count[0] == 1:
                return False, 0.0, 0.0, {}  # SMART fails
            return True, 1.05, 0.0, {}       # BOX fills

        with patch("core.execution.execute_combo", side_effect=mock_exec_combo):
            success, fill_px, method, filled_legs = execute_spread_close(
                ib, "AAPL", legs, 2, 1
            )

        assert success is True
        assert "BOX" in method
