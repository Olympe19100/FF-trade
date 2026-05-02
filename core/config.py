"""
Centralized Configuration — Single Source of Truth

All constants, paths, and shared helpers used across the codebase.
Import from here instead of duplicating values in each module.
"""

import atexit
import glob
import json
import logging
import os
import shutil
import subprocess
import threading
import time
import requests
from pathlib import Path

# Load .env if python-dotenv is available
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# ── Paths ──
ROOT   = Path(__file__).resolve().parent.parent
STATE  = ROOT / "state"
OUTPUT = ROOT / "output"
CACHE  = ROOT / "cache"
DB     = ROOT / "sp500_options.db"
STATIC = ROOT / "static"

STATE.mkdir(exist_ok=True)
OUTPUT.mkdir(exist_ok=True)
CACHE.mkdir(exist_ok=True)

CONFIG_FILE    = STATE / "config.json"
PORTFOLIO_FILE = STATE / "portfolio.json"
TRADES_FILE    = STATE / "trades.json"
LOG_FILE       = STATE / "autopilot.log"

BACKTEST_TRADES_FILE = OUTPUT / "backtest_trades.csv"

# ── Strategy constants (match backtest.py) ──
MAX_POSITIONS    = 20
MAX_CONTRACTS    = 10
DEFAULT_ALLOC    = 0.04       # 4% per name
KELLY_FRAC       = 0.5        # Half Kelly
MIN_KELLY_TRADES = 50
CONTRACT_MULT    = 100
COMMISSION_LEG   = 0.65       # $/leg
SLIPPAGE_PER_LEG = 0.03       # $/leg/share slippage (Muravyev & Pearson 2020)
SLIPPAGE_BUFFER  = 0.03       # Legacy alias — use SLIPPAGE_PER_LEG * n_legs instead
CLOSE_DAYS       = 1          # Close J-1 before front expiry

# ── Execution constants ──
FILL_TIMEOUT     = 30         # Seconds to wait per price level
LMT_WALK_STEP    = 0.05       # $/share step when walking limit price
LMT_WALK_MAX     = 10         # Max price walk iterations
LMT_WALK_WAIT    = 15         # Seconds to wait at each price level

COMBO_TIMEOUT    = 90         # Adaptive algo handles price discovery
COMBO_WALK_STEP  = 0.05       # $/share step when walking combo limit price
COMBO_WALK_WAIT  = 30         # Seconds to wait at each combo price level
LEG_WALK_STEP    = 0.02       # (legacy — unused with Adaptive Algo)
LEG_WALK_WAIT    = 20         # (legacy — unused with Adaptive Algo)
LEG_MAX_WALK     = 0.15       # (legacy — unused with Adaptive Algo)
LEG_TIMEOUT      = 45         # Adaptive Urgent fills quickly
OPTIMAL_START_ET = "10:00"    # ET optimal window start
OPTIMAL_END_ET   = "15:00"    # ET optimal window end
ENFORCE_WINDOW   = True       # Block orders outside optimal window

# ── IBKR Connection ──
TWS_LIVE     = 7496
TWS_PAPER    = 7497
GW_LIVE      = 4001
GW_PAPER     = 4002
CLIENT_ID    = 7           # Unique ID for this script's connection
DEFAULT_HOST = "127.0.0.1"

# ── Scanner constants ──
DTE_COMBOS   = [(30, 60), (30, 90), (60, 90)]  # legacy — kept for retro-compat
DTE_TOL      = 7                                 # legacy — kept for retro-compat

# Dynamic DTE pair discovery (replaces fixed DTE_COMBOS in scanner + spreads)
FRONT_DTE_MIN = 15
FRONT_DTE_MAX = 75
BACK_DTE_MIN  = 40
BACK_DTE_MAX  = 120
MIN_DTE_GAP   = 20

STRIKE_PCT   = 0.10
TARGET_DELTA = 0.35
FF_THRESHOLD_DEFAULT = 0.200
MIN_COST     = 1.00
MIN_OI_LEG   = 100
MIN_MID      = 0.25
BA_PCT_MAX   = 0.10           # 10% max bid-ask spread — reject illiquid spreads
TOP_N        = 20
MAX_WORKERS  = 10

# ── EODHD ──
API_KEY  = os.getenv("EODHD_API_KEY", "")
BASE_URL = "https://eodhd.com/api"

# ── ThetaData ──
THETADATA_URL = "http://127.0.0.1:25503"
THETA_TERMINAL_JAR = ROOT / "ThetaTerminal.jar"
THETA_CREDS_FILE   = ROOT / "creds.txt"

# ThetaData WebSocket (real-time quotes for execution pricing)
THETADATA_WS_URL          = "ws://127.0.0.1:25520/v1/events"
THETADATA_WS_QUOTE_TIMEOUT  = 10.0  # seconds to wait for a single leg quote
THETADATA_WS_SPREAD_TIMEOUT = 15.0  # seconds to wait for all spread leg quotes
THETADATA_WS_RECONNECT_MAX  = 5     # max reconnect attempts before giving up


# ── Logging ──

def get_logger(name: str) -> logging.Logger:
    """Create a module-level logger with console output.

    Usage: ``log = get_logger(__name__)`` at module top-level.
    """
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter(
            "%(asctime)s [%(name)s] %(levelname)s: %(message)s",
            datefmt="%H:%M:%S",
        ))
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger


# ── Helpers ──

def load_config():
    """Load configuration from state/config.json."""
    return load_json(CONFIG_FILE, {})


def load_json(path, default=None):
    """Safe JSON loader — returns default on any error."""
    if default is None:
        default = {}
    try:
        p = Path(path)
        if p.exists():
            with open(p) as f:
                return json.load(f)
    except Exception:
        pass
    return default


def _theta_terminal_alive():
    """Return True if Theta Terminal is responding on its REST port."""
    try:
        r = requests.get(f"{THETADATA_URL}/v3/stock/list/symbols", timeout=2)
        return r.status_code == 200
    except Exception:
        return False


# Global: keep reference so the process isn't garbage-collected
_theta_proc = None
_theta_log_handle = None


def _kill_stale_theta():
    """Kill any stale Theta Terminal Java processes occupying port 25503."""
    log = get_logger("config")
    try:
        # Find PID holding port 25503
        result = subprocess.run(
            ["netstat", "-ano"],
            capture_output=True, text=True, timeout=5,
            encoding="utf-8", errors="replace",
        )
        for line in result.stdout.splitlines():
            if ":25503" in line and "LISTENING" in line:
                pid = line.strip().split()[-1]
                if pid.isdigit() and int(pid) > 0:
                    log.info("Killing stale Theta Terminal (PID %s)", pid)
                    subprocess.run(["taskkill", "/F", "/PID", pid],
                                   capture_output=True, timeout=5)
                    time.sleep(2)  # let port release
                    return True
    except Exception as ex:
        log.warning("Could not kill stale Theta Terminal: %s", ex)
    return False


def ensure_theta_terminal(timeout=45):
    """Launch Theta Terminal if not already running. Returns True if available.

    - Checks if already listening on port 25503
    - If not, kills any stale process and launches ThetaTerminal.jar
    - Waits up to ``timeout`` seconds for it to become responsive
    - Returns False (with a warning) if Java is missing or jar not found
    """
    global _theta_proc, _theta_log_handle
    log = get_logger("config")

    # Already running and healthy?
    if _theta_terminal_alive():
        log.info("Theta Terminal already running")
        return True

    # Port might be held by a zombie process — kill it
    _kill_stale_theta()

    # Find jar
    if not THETA_TERMINAL_JAR.exists():
        log.warning("%s not found — using EODHD fallback", THETA_TERMINAL_JAR)
        return False

    # Find Java — check PATH first, then common install locations
    java = shutil.which("java")
    if java is None:
        search_patterns = [
            os.path.expandvars(r"%LOCALAPPDATA%\Programs\zulu*\*\bin\java.exe"),
            os.path.expandvars(r"%LOCALAPPDATA%\Programs\Common\i4j_jres\*\*\bin\java.exe"),
            r"C:\Program Files\Java\**\bin\java.exe",
            r"C:\Program Files\Eclipse Adoptium\**\bin\java.exe",
            r"C:\Program Files\Microsoft\jdk*\bin\java.exe",
            r"C:\Program Files\Zulu\**\bin\java.exe",
        ]
        for pat in search_patterns:
            hits = glob.glob(pat, recursive=True)
            if hits:
                java = hits[0]
                break
    if java is None:
        log.warning("Java not found — install Java to use Theta Terminal")
        log.warning("Falling back to EODHD (delayed data)")
        return False

    # Credentials file
    if not THETA_CREDS_FILE.exists():
        log.warning("%s not found — create it with your ThetaData username/password",
                     THETA_CREDS_FILE)
        return False

    # Launch
    log.info("Starting Theta Terminal (%s)...", THETA_TERMINAL_JAR.name)
    _theta_log_handle = open(STATE / "theta_launch.log", "w")
    atexit.register(lambda: _theta_log_handle.close() if _theta_log_handle else None)

    try:
        _theta_proc = subprocess.Popen(
            [java, "-jar", str(THETA_TERMINAL_JAR),
             "--creds-file", str(THETA_CREDS_FILE)],
            stdout=_theta_log_handle,
            stderr=subprocess.STDOUT,
            cwd=str(ROOT),
            creationflags=subprocess.CREATE_NO_WINDOW,
        )
    except Exception as ex:
        log.warning("Failed to launch Theta Terminal: %s", ex)
        return False

    # Wait for it to become responsive
    t0 = time.time()
    while time.time() - t0 < timeout:
        if _theta_terminal_alive():
            elapsed = time.time() - t0
            log.info("Theta Terminal ready (%.1fs)", elapsed)
            return True
        time.sleep(1)

    log.warning("Theta Terminal did not respond after %ds — using EODHD fallback", timeout)
    return False


# ── Thread-safe HTTP session for scanner ──

_session = None
_session_lock = threading.Lock()


def get_http_session(pool_size: int = 10) -> requests.Session:
    """Return a thread-safe, reusable HTTP session."""
    global _session
    if _session is not None:
        return _session
    with _session_lock:
        if _session is not None:
            return _session
        s = requests.Session()
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=pool_size,
            pool_maxsize=pool_size,
            max_retries=requests.adapters.Retry(
                total=2, backoff_factor=0.5,
                status_forcelist=[429, 500, 502, 503],
            ),
        )
        s.mount("https://", adapter)
        _session = s
        return _session
