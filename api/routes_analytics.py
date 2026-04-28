"""Analytics routes — Track Record, Straddle."""

from fastapi import APIRouter

from core.track_record import compute_track_record
from core.straddle import compute_straddle_analytics

router = APIRouter(prefix="/api")


@router.get("/track-record")
async def api_track_record():
    """Track Record — backtest + live equity curves and metrics."""
    return compute_track_record()


@router.get("/straddle")
async def api_straddle():
    """Earnings Vol Ramp — pre-earnings long straddle analytics."""
    return compute_straddle_analytics()
