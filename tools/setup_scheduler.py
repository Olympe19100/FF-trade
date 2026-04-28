"""
Setup Windows Task Scheduler — FF Double Calendar Autopilot

Creates 3 scheduled tasks:
    FF_Scan   — 09:00 ET weekdays → python core/autopilot.py --scan
    FF_Trade  — 10:15 ET weekdays → python core/autopilot.py --trade
    FF_Report — 16:30 ET weekdays → python core/autopilot.py --report

Usage:
    python tools/setup_scheduler.py              # Create all 3 tasks
    python tools/setup_scheduler.py --remove     # Remove all 3 tasks
    python tools/setup_scheduler.py --status     # Show task status

Requires: Run as Administrator (schtasks needs elevation).
"""

import subprocess
import sys
import os
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from core.config import ROOT

PYTHON = sys.executable  # Current Python interpreter
AUTOPILOT = ROOT / "core" / "autopilot.py"

TASKS = [
    {
        "name": "FF_Scan",
        "time": "09:00",
        "args": "--scan",
        "description": "FF Strategy: Daily EODHD scan for signals",
    },
    {
        "name": "FF_Trade",
        "time": "10:15",
        "args": "--trade",
        "description": "FF Strategy: Close expiring + Enter new positions",
    },
    {
        "name": "FF_Report",
        "time": "16:30",
        "args": "--report",
        "description": "FF Strategy: Daily portfolio report + email",
    },
]


def create_tasks():
    """Create all scheduled tasks using schtasks.exe."""
    print("=" * 60)
    print("Creating Windows Scheduled Tasks")
    print("=" * 60)
    print(f"  Python:    {PYTHON}")
    print(f"  Autopilot: {AUTOPILOT}")
    print(f"  WorkDir:   {ROOT}")
    print()

    if not AUTOPILOT.exists():
        print(f"ERROR: {AUTOPILOT} not found!")
        sys.exit(1)

    for task in TASKS:
        name = task["name"]
        time = task["time"]
        args = task["args"]
        desc = task["description"]

        # Build the command: python "path\to\autopilot.py" --flag
        program = f'"{PYTHON}"'
        arguments = f'"{AUTOPILOT}" {args}'

        # schtasks command
        # /SC WEEKLY /D MON,TUE,WED,THU,FRI = weekdays only
        cmd = [
            "schtasks", "/Create",
            "/TN", name,
            "/TR", f'{program} {arguments}',
            "/SC", "WEEKLY",
            "/D", "MON,TUE,WED,THU,FRI",
            "/ST", time,
            "/F",  # Force overwrite if exists
        ]

        print(f"  Creating: {name} @ {time} → autopilot.py {args}")
        print(f"    CMD: {' '.join(cmd)}")

        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode == 0:
            print(f"    OK")
        else:
            print(f"    FAILED: {result.stderr.strip()}")
            if "Access is denied" in result.stderr:
                print("    -> Run this script as Administrator!")

    print()
    print("Done. Verify with: schtasks /Query /TN FF_Scan")
    print("Or run: python tools/setup_scheduler.py --status")


def remove_tasks():
    """Remove all scheduled tasks."""
    print("Removing scheduled tasks...")
    for task in TASKS:
        name = task["name"]
        cmd = ["schtasks", "/Delete", "/TN", name, "/F"]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode == 0:
            print(f"  Removed: {name}")
        else:
            print(f"  {name}: {result.stderr.strip()}")


def show_status():
    """Show status of all scheduled tasks."""
    print("Task Scheduler Status:")
    print("-" * 60)
    for task in TASKS:
        name = task["name"]
        cmd = ["schtasks", "/Query", "/TN", name, "/FO", "LIST"]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode == 0:
            # Parse relevant fields
            for line in result.stdout.strip().split("\n"):
                line = line.strip()
                if any(k in line for k in ("Task Name", "Status",
                                            "Next Run Time", "Last Run Time",
                                            "Last Result")):
                    print(f"  {line}")
            print()
        else:
            print(f"  {name}: NOT FOUND")
            print()


if __name__ == "__main__":
    if "--remove" in sys.argv:
        remove_tasks()
    elif "--status" in sys.argv:
        show_status()
    else:
        create_tasks()
