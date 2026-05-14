"""
Fill Rate Database — Adaptive Routing from Fill History

Almgren & Lorenz 2007: learn from past execution outcomes to route
future orders (combo vs legs) per ticker.

Usage:
    from core.fill_db import should_skip_combo, update_fill_stats
"""

import json

from core.config import (
    FILL_STATS_FILE, MIN_ATTEMPTS_FOR_ROUTING, COMBO_SKIP_THRESHOLD,
    get_logger,
)

log = get_logger(__name__)


def load_fill_stats() -> dict:
    """Load per-ticker fill statistics from state/fill_stats.json.

    Returns dict keyed by ticker, each value:
        {combo_attempts, combo_fills, legs_attempts, legs_fills,
         avg_combo_seconds, avg_legs_seconds}
    """
    try:
        if FILL_STATS_FILE.exists():
            with open(FILL_STATS_FILE) as f:
                data = json.load(f)
            if isinstance(data, dict):
                return data
    except Exception as ex:
        log.warning("Failed to read fill stats: %s", ex)
    return {}


def save_fill_stats(stats: dict) -> None:
    """Save fill statistics to state/fill_stats.json."""
    try:
        with open(FILL_STATS_FILE, "w") as f:
            json.dump(stats, f, indent=2)
    except Exception as ex:
        log.warning("Failed to write fill stats: %s", ex)


def update_fill_stats(
    ticker: str,
    method: str,
    filled: bool,
    combo_attempted: bool,
    combo_filled: bool,
    exec_seconds: float,
) -> None:
    """Update fill statistics for a ticker after an execution attempt.

    Args:
        ticker: symbol
        method: "combo" or "legs" (which method ultimately filled/failed)
        filled: whether the execution succeeded
        combo_attempted: whether combo was tried
        combo_filled: whether combo specifically filled
        exec_seconds: total execution time
    """
    stats = load_fill_stats()

    if ticker not in stats:
        stats[ticker] = {
            "combo_attempts": 0,
            "combo_fills": 0,
            "legs_attempts": 0,
            "legs_fills": 0,
            "avg_combo_seconds": 0.0,
            "avg_legs_seconds": 0.0,
        }

    s = stats[ticker]

    if combo_attempted:
        s["combo_attempts"] += 1
        if combo_filled:
            s["combo_fills"] += 1
            # Rolling average for combo seconds
            n = s["combo_fills"]
            s["avg_combo_seconds"] = round(
                ((n - 1) * s["avg_combo_seconds"] + exec_seconds) / n, 2
            )

    if method == "legs":
        s["legs_attempts"] += 1
        if filled:
            s["legs_fills"] += 1
            n = s["legs_fills"]
            s["avg_legs_seconds"] = round(
                ((n - 1) * s["avg_legs_seconds"] + exec_seconds) / n, 2
            )

    save_fill_stats(stats)


def should_skip_combo(ticker: str) -> bool:
    """Return True if combo fill rate < COMBO_SKIP_THRESHOLD with sufficient history.

    Requires >= MIN_ATTEMPTS_FOR_ROUTING combo attempts before skipping.
    """
    stats = load_fill_stats()
    s = stats.get(ticker)
    if not s:
        return False

    attempts = s.get("combo_attempts", 0)
    if attempts < MIN_ATTEMPTS_FOR_ROUTING:
        return False

    fills = s.get("combo_fills", 0)
    fill_rate = fills / attempts

    if fill_rate < COMBO_SKIP_THRESHOLD:
        log.info("Skipping combo for %s (fill rate %.0f%% = %d/%d)",
                 ticker, fill_rate * 100, fills, attempts)
        return True

    return False


def get_ticker_routing_info(ticker: str) -> dict:
    """Return routing recommendation for a ticker.

    Returns dict with:
        skip_combo: bool
        combo_fill_rate: float
        combo_attempts: int
        combo_fills: int
        legs_fill_rate: float
        recommendation: str
    """
    stats = load_fill_stats()
    s = stats.get(ticker, {})

    combo_attempts = s.get("combo_attempts", 0)
    combo_fills = s.get("combo_fills", 0)
    legs_attempts = s.get("legs_attempts", 0)
    legs_fills = s.get("legs_fills", 0)

    combo_fill_rate = combo_fills / combo_attempts if combo_attempts > 0 else 0.0
    legs_fill_rate = legs_fills / legs_attempts if legs_attempts > 0 else 0.0

    skip = should_skip_combo(ticker)

    if combo_attempts < MIN_ATTEMPTS_FOR_ROUTING:
        recommendation = "insufficient_data"
    elif skip:
        recommendation = "legs_only"
    elif combo_fill_rate >= 0.75:
        recommendation = "combo_preferred"
    else:
        recommendation = "combo_first"

    return {
        "skip_combo": skip,
        "combo_fill_rate": round(combo_fill_rate, 4),
        "combo_attempts": combo_attempts,
        "combo_fills": combo_fills,
        "legs_fill_rate": round(legs_fill_rate, 4),
        "legs_attempts": legs_attempts,
        "legs_fills": legs_fills,
        "avg_combo_seconds": s.get("avg_combo_seconds", 0.0),
        "avg_legs_seconds": s.get("avg_legs_seconds", 0.0),
        "recommendation": recommendation,
    }
