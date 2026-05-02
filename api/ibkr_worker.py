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
