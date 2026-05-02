"""
ThetaData WebSocket Client — Real-time NBBO Option Quotes

Singleton WebSocket client running in a background daemon thread.
Provides real-time bid/ask quotes for option contracts via ThetaData's
WebSocket API (ws://127.0.0.1:25520/v1/events).

Used as the PRIMARY price source for execution pricing in trader.py,
with IBKR reqMktData as fallback.

Usage:
    from core.theta_ws import theta_ws_get_leg_price, theta_ws_get_spread_prices

    bid, ask, mid = theta_ws_get_leg_price(option_contract)
    prices = theta_ws_get_spread_prices(legs)
"""

import json
import threading
import time
from dataclasses import dataclass, field
from typing import Optional

from core.config import (
    THETADATA_WS_URL,
    THETADATA_WS_QUOTE_TIMEOUT,
    THETADATA_WS_SPREAD_TIMEOUT,
    THETADATA_WS_RECONNECT_MAX,
    COMMISSION_LEG,
    get_logger,
)

try:
    import websocket
except ImportError:
    websocket = None

log = get_logger(__name__)

# ── Contract key: unique identifier for an option leg ──

@dataclass(frozen=True)
class ContractKey:
    root: str
    expiration: int   # YYYYMMDD
    strike: int       # price * 1000 (ThetaData format)
    right: str        # "C" or "P"


@dataclass
class QuoteData:
    bid: float = 0.0
    ask: float = 0.0
    mid: float = 0.0
    bid_size: int = 0
    ask_size: int = 0
    timestamp: float = field(default_factory=time.time)


# Max age before a quote is considered stale
QUOTE_MAX_AGE = 60.0  # seconds


def _ibkr_to_contract_key(option) -> ContractKey:
    """Convert an ib_insync Option contract to a ContractKey."""
    root = option.symbol
    expiration = int(str(option.lastTradeDateOrContractMonth).replace("-", "")[:8])
    strike = int(float(option.strike) * 1000)
    right = option.right[0].upper()  # "C" or "P"
    return ContractKey(root=root, expiration=expiration, strike=strike, right=right)


def _contract_key_to_theta(key: ContractKey) -> dict:
    """Convert a ContractKey to a ThetaData contract dict."""
    return {
        "root": key.root,
        "expiration": key.expiration,
        "strike": key.strike,
        "right": key.right,
    }


class ThetaWSClient:
    """Singleton WebSocket client for ThetaData real-time quotes."""

    _instance: Optional["ThetaWSClient"] = None
    _instance_lock = threading.Lock()

    def __init__(self):
        self._ws: Optional[websocket.WebSocketApp] = None
        self._thread: Optional[threading.Thread] = None
        self._connected = threading.Event()
        self._quotes: dict[ContractKey, QuoteData] = {}
        self._quotes_lock = threading.Lock()
        self._subscribed: set[ContractKey] = set()
        self._sub_lock = threading.Lock()
        self._shutdown = False
        self._reconnect_count = 0
        self._msg_id = 0

    @classmethod
    def get_instance(cls) -> Optional["ThetaWSClient"]:
        """Get or create the singleton client. Returns None if websocket-client not installed or connection fails."""
        if websocket is None:
            log.warning("websocket-client not installed — ThetaData WS unavailable")
            return None

        with cls._instance_lock:
            if cls._instance is not None:
                if cls._instance._shutdown:
                    cls._instance = None
                elif cls._instance._connected.is_set():
                    return cls._instance
                else:
                    # Previously created but not connected — try again
                    cls._instance = None

            client = cls()
            if client._start():
                cls._instance = client
                return client
            return None

    def _start(self) -> bool:
        """Start the WebSocket connection in a background thread."""
        self._shutdown = False
        self._reconnect_count = 0

        try:
            self._ws = websocket.WebSocketApp(
                THETADATA_WS_URL,
                on_open=self._on_open,
                on_message=self._on_message,
                on_error=self._on_error,
                on_close=self._on_close,
            )
        except Exception as ex:
            log.warning("Failed to create WebSocket: %s", ex)
            return False

        self._thread = threading.Thread(
            target=self._run_forever,
            name="ThetaWS",
            daemon=True,
        )
        self._thread.start()

        # Wait for initial connection
        if self._connected.wait(timeout=5.0):
            log.info("ThetaData WebSocket connected")
            return True

        log.warning("ThetaData WebSocket connection timeout")
        self._shutdown = True
        return False

    def _run_forever(self):
        """Run WebSocket with reconnection logic."""
        while not self._shutdown:
            try:
                self._ws.run_forever(
                    ping_interval=30,
                    ping_timeout=10,
                )
            except Exception as ex:
                log.warning("WebSocket run_forever error: %s", ex)

            if self._shutdown:
                break

            # Reconnect with exponential backoff
            self._reconnect_count += 1
            if self._reconnect_count > THETADATA_WS_RECONNECT_MAX:
                log.error("ThetaData WS: max reconnect attempts (%d) exceeded",
                          THETADATA_WS_RECONNECT_MAX)
                self._shutdown = True
                break

            delay = min(2 ** (self._reconnect_count - 1), 30)
            log.info("ThetaData WS: reconnecting in %ds (attempt %d/%d)",
                     delay, self._reconnect_count, THETADATA_WS_RECONNECT_MAX)
            time.sleep(delay)

            self._connected.clear()
            try:
                self._ws = websocket.WebSocketApp(
                    THETADATA_WS_URL,
                    on_open=self._on_open,
                    on_message=self._on_message,
                    on_error=self._on_error,
                    on_close=self._on_close,
                )
            except Exception:
                continue

    def _on_open(self, ws):
        """Called when WebSocket connection is established."""
        log.info("ThetaData WS: connected to %s", THETADATA_WS_URL)
        self._connected.set()
        self._reconnect_count = 0

        # Re-subscribe to any active subscriptions
        with self._sub_lock:
            if self._subscribed:
                log.info("ThetaData WS: re-subscribing to %d contracts", len(self._subscribed))
                for key in self._subscribed:
                    self._send_subscribe(key, add=True)

    def _on_message(self, ws, message):
        """Parse incoming quote messages and update the cache."""
        try:
            data = json.loads(message)
        except json.JSONDecodeError:
            return

        header = data.get("header", {})
        msg_type = header.get("type", "")

        if msg_type != "QUOTE":
            return

        contract = data.get("contract", {})
        quote = data.get("quote", {})

        if not contract or not quote:
            return

        try:
            key = ContractKey(
                root=contract["root"],
                expiration=int(contract["expiration"]),
                strike=int(contract["strike"]),
                right=contract["right"],
            )
        except (KeyError, ValueError):
            return

        bid = float(quote.get("bid", 0) or 0)
        ask = float(quote.get("ask", 0) or 0)
        mid = round((bid + ask) / 2, 4) if bid > 0 and ask > 0 else 0.0

        qd = QuoteData(
            bid=bid,
            ask=ask,
            mid=mid,
            bid_size=int(quote.get("bid_size", 0) or 0),
            ask_size=int(quote.get("ask_size", 0) or 0),
            timestamp=time.time(),
        )

        with self._quotes_lock:
            self._quotes[key] = qd

    def _on_error(self, ws, error):
        log.warning("ThetaData WS error: %s", error)

    def _on_close(self, ws, close_status_code, close_msg):
        log.info("ThetaData WS: connection closed (code=%s, msg=%s)",
                 close_status_code, close_msg)
        self._connected.clear()

    def _send_subscribe(self, key: ContractKey, add: bool = True):
        """Send a subscribe/unsubscribe message for a single contract."""
        if not self._connected.is_set():
            return
        self._msg_id += 1
        msg = {
            "msg_type": "STREAM",
            "sec_type": "OPTION",
            "req_type": "QUOTE",
            "add": add,
            "id": self._msg_id,
            "contract": _contract_key_to_theta(key),
        }
        try:
            self._ws.send(json.dumps(msg))
        except Exception as ex:
            log.warning("ThetaData WS: send failed: %s", ex)

    def subscribe(self, contracts: list):
        """Subscribe to quotes for a list of ib_insync Option contracts."""
        keys = [_ibkr_to_contract_key(c) for c in contracts]
        with self._sub_lock:
            for key in keys:
                if key not in self._subscribed:
                    self._subscribed.add(key)
                    self._send_subscribe(key, add=True)

    def unsubscribe(self, contracts: list):
        """Unsubscribe from quotes for a list of ib_insync Option contracts."""
        keys = [_ibkr_to_contract_key(c) for c in contracts]
        with self._sub_lock:
            for key in keys:
                if key in self._subscribed:
                    self._subscribed.discard(key)
                    self._send_subscribe(key, add=False)
        # Remove from cache
        with self._quotes_lock:
            for key in keys:
                self._quotes.pop(key, None)

    def wait_for_quotes(self, contracts: list, timeout: float = THETADATA_WS_QUOTE_TIMEOUT) -> bool:
        """Block until we have at least one quote for every contract, or timeout.

        Returns True if all quotes received, False on timeout.
        """
        keys = [_ibkr_to_contract_key(c) for c in contracts]
        deadline = time.time() + timeout
        while time.time() < deadline:
            with self._quotes_lock:
                all_present = all(
                    k in self._quotes and self._quotes[k].mid > 0
                    for k in keys
                )
            if all_present:
                return True
            time.sleep(0.1)
        return False

    def get_quote(self, contract) -> tuple[float, float, float]:
        """Get cached quote for an ib_insync Option contract.

        Returns (bid, ask, mid). Returns (0, 0, 0) if no quote or stale.
        """
        key = _ibkr_to_contract_key(contract)
        with self._quotes_lock:
            qd = self._quotes.get(key)
        if qd is None:
            return 0.0, 0.0, 0.0
        if time.time() - qd.timestamp > QUOTE_MAX_AGE:
            return 0.0, 0.0, 0.0
        return qd.bid, qd.ask, qd.mid

    # ── Key-based subscribe/unsubscribe (used by execution) ──

    def subscribe_keys(self, keys: list[ContractKey]):
        """Subscribe to quotes for a list of ContractKeys directly."""
        with self._sub_lock:
            for key in keys:
                if key not in self._subscribed:
                    self._subscribed.add(key)
                    self._send_subscribe(key, add=True)

    def unsubscribe_keys(self, keys: list[ContractKey]):
        """Unsubscribe from a list of ContractKeys directly."""
        with self._sub_lock:
            for key in keys:
                if key in self._subscribed:
                    self._subscribed.discard(key)
                    self._send_subscribe(key, add=False)
        with self._quotes_lock:
            for key in keys:
                self._quotes.pop(key, None)

    def get_quote_by_key(self, key: ContractKey) -> Optional[QuoteData]:
        """Get cached QuoteData for a ContractKey. Returns None if missing or stale."""
        with self._quotes_lock:
            qd = self._quotes.get(key)
        if qd is None:
            return None
        if time.time() - qd.timestamp > QUOTE_MAX_AGE:
            return None
        return qd

    def shutdown(self):
        """Close the WebSocket connection and join the thread."""
        self._shutdown = True
        if self._ws:
            try:
                self._ws.close()
            except Exception:
                pass
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=5)
        with ThetaWSClient._instance_lock:
            if ThetaWSClient._instance is self:
                ThetaWSClient._instance = None
        log.info("ThetaData WS: shutdown complete")


# ═══════════════════════════════════════════════════════════════
#  Module-level helpers for trader.py
# ═══════════════════════════════════════════════════════════════

def theta_ws_get_leg_price(option_contract) -> tuple[float, float, float]:
    """Get bid/ask/mid for a single option via ThetaData WebSocket.

    Subscribes, waits for quote, unsubscribes, and returns (bid, ask, mid).
    Returns (0, 0, 0) on failure.
    """
    client = ThetaWSClient.get_instance()
    if client is None:
        return 0.0, 0.0, 0.0

    client.subscribe([option_contract])
    client.wait_for_quotes([option_contract], timeout=THETADATA_WS_QUOTE_TIMEOUT)
    bid, ask, mid = client.get_quote(option_contract)
    client.unsubscribe([option_contract])
    return bid, ask, mid


def theta_ws_get_spread_prices(legs: list[tuple]) -> dict:
    """Get quotes for all legs in a spread via ThetaData WebSocket.

    Args:
        legs: list of (Option, action) tuples from create_calendar_legs()

    Returns:
        dict mapping each Option contract to (bid, ask, mid).
        Empty dict on failure.
    """
    client = ThetaWSClient.get_instance()
    if client is None:
        return {}

    contracts = [leg[0] for leg in legs]
    client.subscribe(contracts)
    client.wait_for_quotes(contracts, timeout=THETADATA_WS_SPREAD_TIMEOUT)

    prices = {}
    for contract in contracts:
        prices[contract] = client.get_quote(contract)

    client.unsubscribe(contracts)
    return prices


def theta_ws_get_combo_price(legs: list[tuple]) -> tuple[float, float, float]:
    """Compute synthetic combo bid/ask/mid from individual ThetaData leg quotes.

    Two-tier approach for resilience:
      1. Batch subscribe all legs, wait SPREAD_TIMEOUT (15s)
      2. Retry any missing legs individually, wait QUOTE_TIMEOUT (10s) each

    For a calendar spread (BUY back, SELL front):
      combo_ask = sum(BUY leg asks) - sum(SELL leg bids)  (what we pay)
      combo_bid = sum(BUY leg bids) - sum(SELL leg asks)  (what we'd receive)
      combo_mid = (combo_bid + combo_ask) / 2

    Returns (combo_bid, combo_ask, combo_mid) or (0, 0, 0) on failure.
    """
    client = ThetaWSClient.get_instance()
    if client is None:
        return 0.0, 0.0, 0.0

    contracts = [leg[0] for leg in legs]

    # Step 1: Batch subscribe + wait
    client.subscribe(contracts)
    client.wait_for_quotes(contracts, timeout=THETADATA_WS_SPREAD_TIMEOUT)

    # Step 2: Check which legs we have, retry missing ones individually
    missing = [c for c in contracts if client.get_quote(c)[2] <= 0]
    if missing:
        log.info("  ThetaWS combo: %d/%d legs received, retrying %d individually",
                 len(contracts) - len(missing), len(contracts), len(missing))
        for c in missing:
            client.wait_for_quotes([c], timeout=THETADATA_WS_QUOTE_TIMEOUT)

    # Step 3: Compute synthetic combo price from whatever we have
    combo_bid = combo_ask = 0.0
    for contract, action in legs:
        bid, ask, mid = client.get_quote(contract)
        if mid <= 0:
            log.warning("  ThetaWS: no quote for %s %s K=%.0f %s after retry",
                        contract.symbol, contract.right,
                        contract.strike, action)
            client.unsubscribe(contracts)
            return 0.0, 0.0, 0.0  # Still fail if any leg truly unavailable

        if action == "BUY":
            combo_ask += ask
            combo_bid += bid
        else:  # SELL
            combo_ask -= bid
            combo_bid -= ask

    client.unsubscribe(contracts)
    combo_mid = round((combo_bid + combo_ask) / 2, 4)
    combo_bid = round(combo_bid, 4)
    combo_ask = round(combo_ask, 4)

    return combo_bid, combo_ask, combo_mid


def theta_ws_shutdown():
    """Shut down the ThetaData WebSocket client if running."""
    with ThetaWSClient._instance_lock:
        client = ThetaWSClient._instance
    if client is not None:
        client.shutdown()
