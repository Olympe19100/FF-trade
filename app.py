"""
Calendar Spread Trading — Web Application

FastAPI backend + HTML/CSS/JS frontend.
Single command: python app.py -> opens browser at http://localhost:8000
"""

import asyncio
import threading
import webbrowser

# Fix Python 3.14 asyncio before importing ib_insync
try:
    asyncio.get_event_loop()
except RuntimeError:
    asyncio.set_event_loop(asyncio.new_event_loop())

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
import uvicorn

from core.config import ROOT, OUTPUT, STATIC

# Import routers
from api.routes_trading import router as trading_router
from api.routes_analytics import router as analytics_router
from api.routes_monitor import router as monitor_router
from api.routes_scanner import router as scanner_router

app = FastAPI(title="Calendar Spread Trading")

# Mount routers
app.include_router(trading_router)
app.include_router(analytics_router)
app.include_router(monitor_router)
app.include_router(scanner_router)

# Static files
app.mount("/output", StaticFiles(directory=str(OUTPUT)), name="output")
app.mount("/static", StaticFiles(directory=str(STATIC)), name="static")


@app.get("/")
async def index():
    return FileResponse(str(STATIC / "index.html"))


def _open_browser():
    """Open browser after server starts."""
    import time
    time.sleep(1.5)
    webbrowser.open("http://localhost:8000")


if __name__ == "__main__":
    from core.config import ensure_theta_terminal
    ensure_theta_terminal()
    threading.Thread(target=_open_browser, daemon=True).start()
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="warning")
