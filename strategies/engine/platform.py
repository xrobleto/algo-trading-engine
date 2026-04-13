"""
Cross-platform path resolution for the trading engine.

Supports Windows, macOS, Linux, and cloud (Railway/Docker) deployments.
Priority: ALGO_OUTPUT_DIR env var > platform-specific default.
"""

import os
import sys
from pathlib import Path


def get_data_dir() -> Path:
    """
    Cross-platform application data directory.

    Resolution order:
    1. ALGO_OUTPUT_DIR env var (cloud deployments, launchers)
    2. Platform-specific default (local dev)
       - Windows:  %LOCALAPPDATA%/AlgoTrading
       - macOS:    ~/Library/Application Support/AlgoTrading
       - Linux:    $XDG_DATA_HOME/AlgoTrading or ~/.local/share/AlgoTrading
    """
    if algo_output := os.getenv("ALGO_OUTPUT_DIR"):
        return Path(algo_output)

    if sys.platform == "win32":
        base = os.getenv("LOCALAPPDATA", str(Path.home() / "AppData" / "Local"))
    elif sys.platform == "darwin":
        base = str(Path.home() / "Library" / "Application Support")
    else:  # Linux / Railway / Docker
        base = os.getenv("XDG_DATA_HOME", str(Path.home() / ".local" / "share"))

    return Path(base) / "AlgoTrading"
