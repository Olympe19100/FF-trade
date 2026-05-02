"""
IB Gateway auto-launcher — uses IBC to start Gateway with credentials.

Flow:
1. Find IB Gateway installation on this machine
2. Find or download IBC (IB Controller)
3. Write temporary IBC config with credentials
4. Launch Gateway via IBC
5. Wait for the API socket to become available
"""

import atexit
import glob
import os
import shutil
import socket
import subprocess
import time
import zipfile
from pathlib import Path

import requests

from core.config import ROOT, STATE, get_logger

log = get_logger(__name__)

TOOLS_DIR = ROOT / "tools"
IBC_DIR = TOOLS_DIR / "ibc"
IBC_JAR = IBC_DIR / "IBC.jar"

IBC_VERSION = "3.23.0"
IBC_URL = (
    f"https://github.com/IbcAlpha/IBC/releases/download/"
    f"{IBC_VERSION}/IBCWin-{IBC_VERSION}.zip"
)

# Common IB install locations on Windows
JTS_PATHS = [
    Path("C:/Jts"),
    Path(os.path.expanduser("~/Jts")),
]

PORT_MAP = {
    "paper": 4002,
    "live": 4001,
}

_gateway_process = None
_gateway_log_handle = None


# ─────────────────────────────────────────────────
#  Discovery
# ─────────────────────────────────────────────────

def find_gateway() -> tuple:
    """Find IB Gateway or TWS installation.

    Returns (install_path, version, app_type) or (None, None, None).
    Prefers Gateway over TWS, latest version first.
    """
    for jts in JTS_PATHS:
        for app_dir, app_type in [("ibgateway", "gateway"), ("tws", "tws")]:
            app_path = jts / app_dir
            if not app_path.exists():
                continue
            versions = sorted(
                [d.name for d in app_path.iterdir()
                 if d.is_dir() and d.name.isdigit()],
                reverse=True,
            )
            if versions:
                return app_path / versions[0], versions[0], app_type
    return None, None, None


def _find_bundled_java(install_path: Path) -> str | None:
    """Find the JRE bundled with IB Gateway/TWS (install4j pref_jre.cfg).

    Gateway 1037 ships with Zulu 17 and JxBrowser (Chromium) depends on it.
    Using a different major version (e.g. 21) causes silent JxBrowser failure.
    """
    pref = install_path / ".install4j" / "pref_jre.cfg"
    if pref.exists():
        try:
            jre_dir = pref.read_text(encoding="utf-8").strip()
            java_exe = Path(jre_dir) / "bin" / "java.exe"
            if java_exe.exists():
                log.info("Using bundled JRE: %s", java_exe)
                return str(java_exe)
        except Exception:
            pass
    # Fallback: search i4j_jres directory for any JRE
    i4j_pattern = os.path.expandvars(
        r"%LOCALAPPDATA%\Programs\Common\i4j_jres\*\*\bin\java.exe"
    )
    hits = glob.glob(i4j_pattern)
    if hits:
        log.info("Using i4j JRE: %s", hits[0])
        return hits[0]
    return None


def find_java(install_path: Path | None = None) -> str | None:
    """Find Java executable. Prefers Gateway's bundled JRE over system Java."""
    # 1. Prefer the bundled JRE matching the Gateway version
    if install_path:
        bundled = _find_bundled_java(install_path)
        if bundled:
            return bundled

    # 2. System Java as fallback
    java = shutil.which("java")
    if java:
        return java
    search_patterns = [
        os.path.expandvars(r"%LOCALAPPDATA%\Programs\zulu*\*\bin\java.exe"),
        r"C:\Program Files\Java\**\bin\java.exe",
        r"C:\Program Files\Eclipse Adoptium\**\bin\java.exe",
        r"C:\Program Files\Microsoft\jdk*\bin\java.exe",
        r"C:\Program Files\Zulu\**\bin\java.exe",
    ]
    for pat in search_patterns:
        hits = glob.glob(pat, recursive=True)
        if hits:
            return hits[0]
    return None


def find_or_download_ibc() -> Path | None:
    """Find IBC jar; download from GitHub if missing."""
    if IBC_JAR.exists():
        return IBC_JAR

    log.info("Downloading IBC %s …", IBC_VERSION)
    IBC_DIR.mkdir(parents=True, exist_ok=True)

    try:
        r = requests.get(IBC_URL, timeout=60, stream=True)
        r.raise_for_status()

        zip_path = IBC_DIR / "ibc.zip"
        with open(zip_path, "wb") as f:
            for chunk in r.iter_content(8192):
                f.write(chunk)

        with zipfile.ZipFile(zip_path) as z:
            for name in z.namelist():
                if name.endswith("IBC.jar"):
                    with open(IBC_JAR, "wb") as f:
                        f.write(z.read(name))
                    break

        zip_path.unlink(missing_ok=True)

        if IBC_JAR.exists():
            log.info("IBC downloaded → %s", IBC_JAR)
            return IBC_JAR
    except Exception as ex:
        log.error("Failed to download IBC: %s", ex)

    return None


# ─────────────────────────────────────────────────
#  Config & launch
# ─────────────────────────────────────────────────

def _write_ibc_config(
    username: str, password: str, mode: str, port: int
) -> Path:
    """Write temporary IBC ini config with credentials."""
    config = f"""\
LogToConsole=yes
FIX=no
IbLoginId={username}
IbPassword={password}
TradingMode={mode}
StoreSettingsOnServer=no
MinimizeMainWindow=no
ExistingSessionDetectedAction=primary
AcceptIncomingConnectionAction=accept
ShowAllTrades=no
OverrideTwsApiPort={port}
ReadOnlyLogin=no
AcceptNonBrokerageAccountWarning=yes
AllowBlindTrading=yes
DismissPasswordExpiryWarning=no
DismissNSEComplianceNotice=yes
"""
    config_path = STATE / "ibc_config.ini"
    config_path.write_text(config)
    return config_path


def _port_open(host: str, port: int) -> bool:
    """Quick check if a TCP port is accepting connections."""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(2)
        s.connect((host, port))
        s.close()
        return True
    except (ConnectionRefusedError, socket.timeout, OSError):
        return False


def _wait_for_socket(host: str, port: int, timeout: int = 120) -> bool:
    """Block until TCP port is ready, or timeout."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if _port_open(host, port):
            return True
        time.sleep(2)
    return False


def check_prerequisites() -> dict:
    """Check what's available on this machine.

    Returns dict with booleans and paths for the UI.
    """
    gw_path, gw_ver, gw_type = find_gateway()
    java = find_java(gw_path)
    ibc = IBC_JAR.exists()
    return {
        "gateway_found": gw_path is not None,
        "gateway_path": str(gw_path) if gw_path else None,
        "gateway_version": gw_ver,
        "gateway_type": gw_type,
        "java_found": java is not None,
        "ibc_found": ibc,
    }


def _read_vmoptions(install_path: Path, app_type: str) -> list[str]:
    """Read JVM flags from the official vmoptions file.

    IB Gateway/TWS ships a .vmoptions file with required -D properties,
    memory settings (-Xmx), and GC flags (-XX:).  Without these the
    application may fail to initialise its UI.
    """
    filename = "ibgateway.vmoptions" if app_type == "gateway" else "tws.vmoptions"
    vmopts_path = install_path / filename
    if not vmopts_path.exists():
        log.warning("vmoptions file not found: %s", vmopts_path)
        return []

    flags: list[str] = []
    try:
        for line in vmopts_path.read_text(encoding="utf-8", errors="replace").splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            # Skip classpath flags (we set our own)
            if line.startswith(("-cp ", "-classpath ")):
                continue
            flags.append(line)
    except Exception as ex:
        log.warning("Failed to read %s: %s", vmopts_path, ex)

    log.info("Loaded %d JVM flags from %s", len(flags), filename)
    return flags


def launch_gateway(
    username: str, password: str, mode: str = "paper"
) -> dict:
    """Launch IB Gateway via IBC with the given credentials.

    Non-blocking: starts Gateway and returns immediately.
    Use gateway_ready() to poll for readiness (after user enters 2FA).
    """
    global _gateway_process, _gateway_log_handle

    port = PORT_MAP.get(mode, 4002)

    # Already running?
    if _port_open("127.0.0.1", port):
        return {"status": "already_running", "port": port}

    # Find Gateway
    install_path, version, app_type = find_gateway()
    if not install_path:
        return {
            "status": "error",
            "error": (
                "IB Gateway not found on this machine. "
                "Download it from: "
                "https://www.interactivebrokers.com/en/trading/ibgateway-stable.php"
            ),
        }

    # Java — prefer the bundled JRE matching Gateway's version
    java = find_java(install_path)
    if not java:
        return {
            "status": "error",
            "error": "Java not found. Install a JRE/JDK and try again.",
        }

    # IBC
    ibc_jar = find_or_download_ibc()
    if not ibc_jar:
        return {
            "status": "error",
            "error": "Could not find or download IBC. Check internet connection.",
        }

    # Config
    config_path = _write_ibc_config(username, password, mode, port)

    # Build classpath — include i4jruntime.jar (required by install4j bootstrap)
    jars_dir = install_path / "jars"
    i4j_jar = install_path / ".install4j" / "i4jruntime.jar"
    cp = f"{ibc_jar};{jars_dir}\\*"
    if i4j_jar.exists():
        cp += f";{i4j_jar}"
    main_class = (
        "ibcalpha.ibc.IbcGateway"
        if app_type == "gateway"
        else "ibcalpha.ibc.IbcTws"
    )

    # Read the real vmoptions file for -D properties and memory flags
    jvm_flags = _read_vmoptions(install_path, app_type)

    # Module access flags — exact set from Gateway's i4jparams.conf
    # Includes --add-exports for JxBrowser/Chromium and sun.awt.windows
    jvm_flags += [
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
        "--add-opens=java.base/java.io=ALL-UNNAMED",
        "--add-opens=java.base/java.nio=ALL-UNNAMED",
        "--add-opens=java.base/java.util=ALL-UNNAMED",
        "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
        "--add-exports=java.base/sun.util=ALL-UNNAMED",
        "--add-exports=java.desktop/com.sun.java.swing.plaf.motif=ALL-UNNAMED",
        "--add-opens=java.desktop/java.awt=ALL-UNNAMED",
        "--add-opens=java.desktop/java.awt.dnd=ALL-UNNAMED",
        "--add-opens=java.desktop/java.awt.event=ALL-UNNAMED",
        "--add-opens=java.desktop/javax.swing=ALL-UNNAMED",
        "--add-opens=java.desktop/javax.swing.event=ALL-UNNAMED",
        "--add-opens=java.desktop/javax.swing.plaf.basic=ALL-UNNAMED",
        "--add-opens=java.desktop/javax.swing.table=ALL-UNNAMED",
        "--add-opens=java.desktop/javax.swing.text=ALL-UNNAMED",
        "--add-opens=java.desktop/sun.awt=ALL-UNNAMED",
        "--add-exports=java.desktop/sun.awt.windows=ALL-UNNAMED",
        "--add-exports=java.desktop/sun.lwawt=ALL-UNNAMED",
        "--add-exports=java.desktop/sun.swing=ALL-UNNAMED",
        "--add-opens=jdk.management/com.sun.management.internal=ALL-UNNAMED",
    ]
    cmd = [java] + jvm_flags + ["-cp", cp, main_class, str(config_path), mode]

    log.info(
        "Launching %s v%s via IBC (port %d)…", app_type, version, port
    )

    # Working directory MUST be the JTS root (where jts.ini lives)
    # so Gateway finds its settings, SSL certs, and user directories
    jts_root = install_path.parent.parent  # e.g. C:\Jts

    try:
        _gateway_log_handle = open(STATE / "gateway_launch.log", "w")
        atexit.register(
            lambda: _gateway_log_handle.close() if _gateway_log_handle else None
        )

        _gateway_process = subprocess.Popen(
            cmd,
            stdout=_gateway_log_handle,
            stderr=subprocess.STDOUT,
            cwd=str(jts_root),
        )
    except Exception as ex:
        _cleanup_config(config_path)
        return {"status": "error", "error": f"Failed to start Gateway: {ex}"}

    # Clean up credentials file after a short delay (Gateway reads it at startup)
    import threading
    threading.Timer(10.0, _cleanup_config, args=[config_path]).start()

    log.info("Gateway process started (pid %d), waiting for 2FA…", _gateway_process.pid)
    return {
        "status": "launched",
        "port": port,
        "app_type": app_type,
        "version": version,
    }


def _ib_handshake_ok(host: str, port: int) -> bool:
    """Raw-socket IB API handshake to verify Gateway is authenticated.

    Sends the initial API handshake bytes and checks for a server version
    response.  Uses plain sockets — no ib_insync, no asyncio dependency.
    If Gateway hasn't completed 2FA yet, the connection will be dropped.
    """
    import struct
    s = None
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(4)
        s.connect((host, port))
        # IB API handshake: "API\0" + length-prefixed version range
        ver_msg = b"v100..176"
        s.sendall(b"API\0" + struct.pack(">I", len(ver_msg)) + ver_msg)
        # If Gateway is authenticated it responds with its server version
        data = s.recv(1024)
        return len(data) > 0
    except Exception:
        return False
    finally:
        if s:
            try:
                s.close()
            except Exception:
                pass


def gateway_ready(port: int = 4002) -> dict:
    """Check if Gateway is fully authenticated and accepting API connections.

    Called by frontend polling to detect when 2FA is complete.
    A simple TCP port check is not enough — Gateway opens the port before
    authentication finishes.  We do a real ib_insync handshake to confirm.
    """
    # Check if the process died first
    if _gateway_process and _gateway_process.poll() is not None:
        return {
            "ready": False,
            "port": port,
            "error": "Gateway process exited unexpectedly. Check credentials.",
        }

    # Port not even open yet → definitely not ready
    if not _port_open("127.0.0.1", port):
        return {"ready": False, "port": port}

    # Port open — verify with a real handshake
    if _ib_handshake_ok("127.0.0.1", port):
        return {"ready": True, "port": port}

    # Port open but handshake failed → still authenticating
    return {"ready": False, "port": port}


def send_2fa_code(code: str) -> dict:
    """Type a 2FA code into the IB Gateway window using Win32 SendInput.

    Finds the Gateway window, brings it to foreground, types the code,
    and presses Enter.
    """
    if os.name != "nt":
        return {"status": "error", "error": "Only supported on Windows"}

    import ctypes
    from ctypes import wintypes

    user32 = ctypes.windll.user32

    # Find IB Gateway/authentication windows
    target_hwnds = []

    def _enum_cb(hwnd, _):
        if user32.IsWindowVisible(hwnd):
            length = user32.GetWindowTextLengthW(hwnd)
            if length > 0:
                buf = ctypes.create_unicode_buffer(length + 1)
                user32.GetWindowTextW(hwnd, buf, length + 1)
                title = buf.value.lower()
                if any(kw in title for kw in [
                    "ib gateway", "ibgateway", "authentication",
                    "second factor", "security code", "two factor",
                    "login", "ib api",
                ]):
                    target_hwnds.append(hwnd)
        return True

    WNDENUMPROC = ctypes.WINFUNCTYPE(
        wintypes.BOOL, wintypes.HWND, wintypes.LPARAM
    )
    user32.EnumWindows(WNDENUMPROC(_enum_cb), 0)

    if not target_hwnds:
        return {"status": "error", "error": "IB Gateway window not found"}

    # Bring first match to foreground
    hwnd = target_hwnds[0]
    user32.ShowWindow(hwnd, 9)  # SW_RESTORE
    user32.SetForegroundWindow(hwnd)
    time.sleep(0.5)

    # Type code using SendInput (works with Java Swing)
    INPUT_KEYBOARD = 1
    KEYEVENTF_UNICODE = 0x0004
    KEYEVENTF_KEYUP = 0x0002
    VK_RETURN = 0x0D

    class KEYBDINPUT(ctypes.Structure):
        _fields_ = [
            ("wVk", wintypes.WORD),
            ("wScan", wintypes.WORD),
            ("dwFlags", wintypes.DWORD),
            ("time", wintypes.DWORD),
            ("dwExtraInfo", ctypes.POINTER(ctypes.c_ulong)),
        ]

    class INPUTUNION(ctypes.Union):
        _fields_ = [("ki", KEYBDINPUT)]

    class INPUT(ctypes.Structure):
        _fields_ = [
            ("type", wintypes.DWORD),
            ("u", INPUTUNION),
        ]

    def _send_char(ch):
        """Send a single unicode character."""
        inputs = (INPUT * 2)()
        inputs[0].type = INPUT_KEYBOARD
        inputs[0].u.ki.wScan = ord(ch)
        inputs[0].u.ki.dwFlags = KEYEVENTF_UNICODE
        inputs[1].type = INPUT_KEYBOARD
        inputs[1].u.ki.wScan = ord(ch)
        inputs[1].u.ki.dwFlags = KEYEVENTF_UNICODE | KEYEVENTF_KEYUP
        user32.SendInput(2, ctypes.byref(inputs), ctypes.sizeof(INPUT))

    def _send_vk(vk):
        """Send a virtual key press."""
        inputs = (INPUT * 2)()
        inputs[0].type = INPUT_KEYBOARD
        inputs[0].u.ki.wVk = vk
        inputs[1].type = INPUT_KEYBOARD
        inputs[1].u.ki.wVk = vk
        inputs[1].u.ki.dwFlags = KEYEVENTF_KEYUP
        user32.SendInput(2, ctypes.byref(inputs), ctypes.sizeof(INPUT))

    # Type each character
    for ch in code:
        _send_char(ch)
        time.sleep(0.05)

    time.sleep(0.2)
    _send_vk(VK_RETURN)

    log.info("Sent 2FA code (%d chars) to Gateway window", len(code))
    return {"status": "ok", "window": target_hwnds[0]}


def _cleanup_config(config_path: Path) -> None:
    """Delete the temp IBC config that contains credentials."""
    try:
        config_path.unlink(missing_ok=True)
    except Exception:
        pass
