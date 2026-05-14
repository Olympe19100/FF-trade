"""
IBKR Worker Thread — Thread-safe IB connection management.

ib_insync uses asyncio internally. Its synchronous API calls
loop.run_until_complete(), which conflicts with uvicorn's loop.
Solution: a single persistent background thread with its own
event loop. All IB operations are dispatched to this thread
via a queue, ensuring the ib object always runs on its
original loop.
"""

import asyncio
import queue
import random
import threading
import concurrent.futures
from collections import deque
from datetime import datetime


# ── Global IBKR state ──
ib_state: dict = {
    "ib": None,
    "connected": False,
    "account": None,
    "host": "127.0.0.1",
    "port": 4002,
    "connect_time": None,
}


def safe_disconnect() -> None:
    """Cleanly disconnect the current IB session (if any).

    Resets ib_state so a fresh connection can be made without
    clientId conflicts.
    """
    ib = ib_state.get("ib")
    if ib is not None:
        try:
            if ib.isConnected():
                ib.disconnect()
        except Exception:
            pass
        # Give IBKR time to release the clientId
        import time as _time
        _time.sleep(1)
    ib_state["ib"] = None
    ib_state["connected"] = False
    ib_state["account"] = None
    ib_state["connect_time"] = None


_last_client_id = 0

def next_client_id() -> int:
    """Return a sequential clientId to avoid conflicts.

    Uses a random base + incrementing counter so we never reuse
    a clientId from a stale session that IBKR hasn't released yet.
    """
    global _last_client_id
    if _last_client_id == 0:
        _last_client_id = random.randint(100, 500)
    _last_client_id += 1
    if _last_client_id > 999:
        _last_client_id = 100
    return _last_client_id

# Order execution log (bounded deque, last 500 entries)
order_log: deque = deque(maxlen=500)

_ib_queue: queue.Queue = queue.Queue()


def _ib_worker() -> None:
    """Persistent IB thread — processes one request at a time."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    while True:
        future, func, args, kwargs = _ib_queue.get()
        try:
            result = func(*args, **kwargs)
            future.set_result(result)
        except Exception as ex:
            future.set_exception(ex)


# Start at module load
threading.Thread(target=_ib_worker, daemon=True).start()


def run_in_ib_thread(func, *args, **kwargs):
    """Dispatch a function to the persistent IB thread and wait for result."""
    future = concurrent.futures.Future()
    _ib_queue.put((future, func, args, kwargs))
    return future.result(timeout=1800)  # 30 min for optimal execution


def log_order(type_: str, message: str, status: str = "ok") -> None:
    """Append an entry to the order log."""
    order_log.append({
        "time": datetime.now().isoformat(),
        "type": type_,
        "message": message,
        "status": status,
    })


# ── Auto-reconnect engine ──
import time as _time
from core.config import get_logger

_reconnect_log = get_logger("api.ibkr.reconnect")

MAX_RECONNECT_ATTEMPTS = 5
RECONNECT_BACKOFF = [2, 4, 8, 16, 30]  # seconds between each attempt

_suppress_reconnect: bool = False
_reconnect_lock = threading.Lock()
_reconnect_thread: threading.Thread | None = None


def suppress_reconnect(val: bool) -> None:
    """Enable/disable reconnect suppression (used during intentional disconnects)."""
    global _suppress_reconnect
    _suppress_reconnect = val


def trigger_reconnect() -> None:
    """Trigger an auto-reconnect if not suppressed and not already in progress."""
    global _reconnect_thread
    if _suppress_reconnect:
        _reconnect_log.info("Reconnect suppressed (intentional disconnect)")
        return
    with _reconnect_lock:
        if _reconnect_thread is not None and _reconnect_thread.is_alive():
            _reconnect_log.info("Reconnect already in progress, skipping")
            return
        _reconnect_thread = threading.Thread(target=_do_reconnect, daemon=True)
        _reconnect_thread.start()


def _do_reconnect() -> None:
    """Attempt to reconnect to IBKR with exponential backoff."""
    # Import here to avoid circular import at module load
    from core.trader import connect_ibkr, verify_paper

    host = ib_state["host"]
    port = ib_state["port"]

    for attempt in range(MAX_RECONNECT_ATTEMPTS):
        if _suppress_reconnect:
            _reconnect_log.info("Reconnect aborted (suppress flag set)")
            return

        wait = RECONNECT_BACKOFF[attempt]
        _reconnect_log.warning(
            "Auto-reconnect attempt %d/%d in %ds to %s:%d",
            attempt + 1, MAX_RECONNECT_ATTEMPTS, wait, host, port,
        )
        _time.sleep(wait)

        # Maybe something else reconnected us already
        if ib_state["connected"]:
            _reconnect_log.info("Already reconnected, aborting")
            return

        if _suppress_reconnect:
            _reconnect_log.info("Reconnect aborted (suppress flag set)")
            return

        try:
            cid = next_client_id()

            def do_connect():
                ib = connect_ibkr(host, port, client_id=cid)
                acct = verify_paper(ib)
                return ib, acct

            ib, acct = run_in_ib_thread(do_connect)

            ib_state["ib"] = ib
            ib_state["connected"] = True
            ib_state["account"] = acct
            ib_state["connect_time"] = datetime.now().isoformat()

            # Attach disconnect handler for the new connection
            from api.routes_trading import _attach_disconnect_handler
            _attach_disconnect_handler(ib)

            _reconnect_log.warning(
                "Auto-reconnect successful: %s @ %s:%d", acct, host, port,
            )
            log_order("reconnect", f"Auto-reconnect successful: {acct}")
            return

        except Exception as ex:
            _reconnect_log.warning(
                "Auto-reconnect attempt %d/%d failed: %s",
                attempt + 1, MAX_RECONNECT_ATTEMPTS, ex,
            )

    _reconnect_log.error(
        "Auto-reconnect failed after %d attempts", MAX_RECONNECT_ATTEMPTS,
    )
    log_order("reconnect", f"Auto-reconnect failed after {MAX_RECONNECT_ATTEMPTS} attempts", "error")
