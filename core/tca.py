"""
Transaction Cost Analysis — Execution Quality Measurement

Kissell & Glantz 2003 / Perold 1988 implementation shortfall framework.
Records every execution event with benchmarks and computes TCA metrics.

Usage:
    from core.tca import compute_tca_metrics, ExecutionRecord, record_execution
"""

import json
from dataclasses import dataclass, field, asdict
from typing import Optional

from core.config import EXECUTION_LOG_FILE, get_logger

log = get_logger(__name__)


# ═══════════════════════════════════════════════════════════════
#  TCA METRICS (Kissell & Glantz 2003, Perold 1988)
# ═══════════════════════════════════════════════════════════════

def compute_tca_metrics(
    fill_price: float,
    scanner_mid: float,
    theta_bid: float,
    theta_ask: float,
    theta_mid: float,
    direction: str,
) -> dict:
    """Compute TCA metrics for a single execution.

    Args:
        fill_price: actual fill price per share
        scanner_mid: EODHD scanner cost estimate (pre-trade benchmark)
        theta_bid: ThetaData bid at order time
        theta_ask: ThetaData ask at order time
        theta_mid: ThetaData mid at order time
        direction: "entry" or "close"

    Returns dict with:
        implementation_shortfall: fill vs scanner_mid (Perold 1988)
        spread_capture: 1 - |fill-mid|/(BA/2), clamped [-1, 2]
        arrival_slippage: fill vs theta_mid
        realized_spread: 2×|fill-mid| / BA
    """
    # Implementation shortfall (Perold 1988)
    if direction == "entry":
        implementation_shortfall = fill_price - scanner_mid
    else:
        implementation_shortfall = scanner_mid - fill_price

    # Spread capture: how much of the half-spread we saved
    ba = theta_ask - theta_bid if theta_ask > theta_bid > 0 else 0.0
    half_ba = ba / 2.0 if ba > 0 else 0.0
    if half_ba > 0 and theta_mid > 0:
        distance_from_mid = abs(fill_price - theta_mid)
        spread_capture = 1.0 - distance_from_mid / half_ba
        spread_capture = max(-1.0, min(2.0, spread_capture))
    else:
        spread_capture = 0.0

    # Arrival slippage: fill vs theta_mid
    arrival_slippage = fill_price - theta_mid if theta_mid > 0 else 0.0

    # Realized spread: 2×|fill-mid| / BA
    if ba > 0:
        realized_spread = 2.0 * abs(fill_price - theta_mid) / ba
    else:
        realized_spread = 0.0

    return {
        "implementation_shortfall": round(implementation_shortfall, 6),
        "spread_capture": round(spread_capture, 4),
        "arrival_slippage": round(arrival_slippage, 6),
        "realized_spread": round(realized_spread, 4),
    }


# ═══════════════════════════════════════════════════════════════
#  EXECUTION RECORD
# ═══════════════════════════════════════════════════════════════

@dataclass
class ExecutionRecord:
    """Full execution event for TCA analysis."""

    # Identity
    ticker: str
    direction: str  # "entry" or "close"
    timestamp: str = ""

    # Benchmarks
    scanner_mid: float = 0.0
    theta_bid: float = 0.0
    theta_ask: float = 0.0
    theta_mid: float = 0.0

    # Execution details
    method: str = ""  # "combo" or "legs"
    combo_attempted: bool = False
    combo_result: Optional[str] = None
    walk_steps: int = 0
    initial_limit: float = 0.0
    final_limit: float = 0.0
    exec_seconds: float = 0.0
    priority: str = "Normal"

    # Fill result
    fill_price: float = 0.0
    contracts: int = 0
    slippage: float = 0.0

    # TCA metrics (computed)
    tca: dict = field(default_factory=dict)

    # Market microstructure
    min_leg_oi: int = 0
    option_ba: float = 0.0
    spread_type: str = ""
    n_legs: int = 0


# ═══════════════════════════════════════════════════════════════
#  PERSISTENCE
# ═══════════════════════════════════════════════════════════════

def record_execution(record: ExecutionRecord) -> None:
    """Append an ExecutionRecord to state/execution_log.json."""
    # Compute TCA metrics
    record.tca = compute_tca_metrics(
        fill_price=record.fill_price,
        scanner_mid=record.scanner_mid,
        theta_bid=record.theta_bid,
        theta_ask=record.theta_ask,
        theta_mid=record.theta_mid,
        direction=record.direction,
    )

    # Set timestamp if not set
    if not record.timestamp:
        from datetime import datetime
        record.timestamp = datetime.now().isoformat()

    # Load existing log
    entries = load_execution_log()

    # Append
    entries.append(asdict(record))

    # Save
    try:
        with open(EXECUTION_LOG_FILE, "w") as f:
            json.dump(entries, f, indent=2)
    except Exception as ex:
        log.warning("Failed to write execution log: %s", ex)

    # Log summary
    is_val = record.tca.get("implementation_shortfall", 0)
    sc_val = record.tca.get("spread_capture", 0)
    log.info("TCA: recorded %s %s %s (IS=%+.4f, capture=%.0f%%)",
             record.direction, record.ticker, record.method,
             is_val, sc_val * 100)


def load_execution_log() -> list[dict]:
    """Read execution log from state/execution_log.json."""
    try:
        if EXECUTION_LOG_FILE.exists():
            with open(EXECUTION_LOG_FILE) as f:
                data = json.load(f)
            if isinstance(data, list):
                return data
    except Exception as ex:
        log.warning("Failed to read execution log: %s", ex)
    return []


def tca_summary(ticker: str | None = None, last_n: int | None = None) -> dict:
    """Aggregate TCA stats from execution log.

    Args:
        ticker: filter by ticker (None = all)
        last_n: only use last N records (None = all)

    Returns dict with:
        count, mean_is, median_is, mean_spread_capture,
        combo_fill_rate, avg_walk_steps, avg_exec_seconds
    """
    entries = load_execution_log()

    if ticker:
        entries = [e for e in entries if e.get("ticker") == ticker]

    if last_n and len(entries) > last_n:
        entries = entries[-last_n:]

    if not entries:
        return {
            "count": 0,
            "mean_is": 0.0,
            "median_is": 0.0,
            "mean_spread_capture": 0.0,
            "combo_fill_rate": 0.0,
            "avg_walk_steps": 0.0,
            "avg_exec_seconds": 0.0,
        }

    import statistics

    is_vals = [e.get("tca", {}).get("implementation_shortfall", 0) for e in entries]
    sc_vals = [e.get("tca", {}).get("spread_capture", 0) for e in entries]
    walk_steps = [e.get("walk_steps", 0) for e in entries]
    exec_secs = [e.get("exec_seconds", 0) for e in entries]

    combo_attempted = [e for e in entries if e.get("combo_attempted")]
    combo_filled = [e for e in combo_attempted
                    if e.get("combo_result") == "filled"]

    return {
        "count": len(entries),
        "mean_is": round(statistics.mean(is_vals), 6) if is_vals else 0.0,
        "median_is": round(statistics.median(is_vals), 6) if is_vals else 0.0,
        "mean_spread_capture": round(statistics.mean(sc_vals), 4) if sc_vals else 0.0,
        "combo_fill_rate": round(len(combo_filled) / len(combo_attempted), 4) if combo_attempted else 0.0,
        "avg_walk_steps": round(statistics.mean(walk_steps), 2) if walk_steps else 0.0,
        "avg_exec_seconds": round(statistics.mean(exec_secs), 2) if exec_secs else 0.0,
    }
