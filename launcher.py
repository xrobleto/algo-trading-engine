#!/usr/bin/env python3
"""
Cross-platform launcher for the Unified Trading Engine.

Replaces Windows-only .bat launchers. Works on Windows, macOS, and Linux.

Usage:
    python launcher.py                  # Paper trading (default)
    python launcher.py --live           # Live trading
    python launcher.py --trend-only     # Trend adapter only
    python launcher.py --status         # Show engine state
"""

import argparse
import os
import subprocess
import sys
import time
from pathlib import Path


def load_env_file(env_path: Path) -> None:
    """Load a .env file into os.environ (simple KEY=VALUE parser)."""
    if not env_path.exists():
        print(f"[LAUNCHER] Warning: {env_path} not found")
        return

    with open(env_path, "r") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" not in line:
                continue
            key, _, value = line.partition("=")
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            if key and key not in os.environ:  # don't override existing env vars
                os.environ[key] = value


def get_data_dir() -> Path:
    """Resolve data directory (mirrors engine/platform.py logic)."""
    if algo_output := os.getenv("ALGO_OUTPUT_DIR"):
        return Path(algo_output)

    if sys.platform == "win32":
        base = os.getenv("LOCALAPPDATA", str(Path.home() / "AppData" / "Local"))
    elif sys.platform == "darwin":
        base = str(Path.home() / "Library" / "Application Support")
    else:
        base = os.getenv("XDG_DATA_HOME", str(Path.home() / ".local" / "share"))

    return Path(base) / "AlgoTrading"


def main():
    parser = argparse.ArgumentParser(description="Unified Trading Engine Launcher")
    parser.add_argument("--live", action="store_true", help="Live trading mode")
    parser.add_argument("--paper", action="store_true", help="Paper trading mode (default)")
    parser.add_argument("--trend-only", action="store_true", help="Run Trend adapter only")
    parser.add_argument("--status", action="store_true", help="Show engine state and exit")
    parser.add_argument("--no-restart", action="store_true", help="Don't auto-restart on crash")
    args = parser.parse_args()

    project_root = Path(__file__).resolve().parent
    strategies_dir = project_root / "strategies"
    config_dir = project_root / "config"

    # Load environment from config files
    if args.live:
        env_files = ["trend_bot_live.env", "cross_asset_bot.env"]
    else:
        env_files = ["trend_bot_paper.env", "cross_asset_bot.env"]

    for env_file in env_files:
        load_env_file(config_dir / env_file)

    # Set ALGO_OUTPUT_DIR if not already set
    if "ALGO_OUTPUT_DIR" not in os.environ:
        data_dir = get_data_dir()
        os.environ["ALGO_OUTPUT_DIR"] = str(data_dir)

    # Ensure data directories exist
    data_dir = Path(os.environ["ALGO_OUTPUT_DIR"])
    (data_dir / "data" / "state").mkdir(parents=True, exist_ok=True)
    (data_dir / "logs").mkdir(parents=True, exist_ok=True)

    # Build engine command
    cmd = [sys.executable, "-m", "engine.main"]
    if args.trend_only:
        cmd.append("--trend-only")
    if args.status:
        cmd.append("--status")

    mode = "LIVE" if args.live else "PAPER"
    print(f"[LAUNCHER] Mode: {mode}")
    print(f"[LAUNCHER] Data dir: {os.environ['ALGO_OUTPUT_DIR']}")
    print(f"[LAUNCHER] Strategy dir: {strategies_dir}")

    # Run with auto-restart (unless --status or --no-restart)
    if args.status or args.no_restart:
        result = subprocess.run(cmd, cwd=str(strategies_dir), env=os.environ)
        sys.exit(result.returncode)

    restart_delay = 15
    while True:
        print(f"[LAUNCHER] Starting engine ({mode})...")
        result = subprocess.run(cmd, cwd=str(strategies_dir), env=os.environ)

        if result.returncode == 0:
            print("[LAUNCHER] Engine exited cleanly.")
            break

        print(f"[LAUNCHER] Engine exited with code {result.returncode}. "
              f"Restarting in {restart_delay}s...")
        time.sleep(restart_delay)


if __name__ == "__main__":
    main()
