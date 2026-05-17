"""Path resolution and secret loading for Horizon.

Secrets are read from the environment, then from `horizon/.env`, then as a last
resort from the existing project's `config/trend_bot.env` (so Horizon runs
out-of-the-box on the operator's machine). No secret is ever hardcoded here.
"""

from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path
from typing import Dict, Optional

# horizon/ package directory and the repo root that contains it.
PACKAGE_DIR = Path(__file__).resolve().parent
REPO_ROOT = PACKAGE_DIR.parent


def _parse_env_file(path: Path) -> Dict[str, str]:
    out: Dict[str, str] = {}
    if not path.is_file():
        return out
    for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, val = line.partition("=")
        out[key.strip()] = val.strip().strip('"').strip("'")
    return out


@lru_cache(maxsize=1)
def _env_files() -> Dict[str, str]:
    """Merge candidate .env files (earlier files win)."""
    merged: Dict[str, str] = {}
    candidates = [
        PACKAGE_DIR / ".env",
        REPO_ROOT / "config" / "trend_bot.env",
        REPO_ROOT / "config" / "momentum_bot.env",
    ]
    for path in candidates:
        for k, v in _parse_env_file(path).items():
            merged.setdefault(k, v)
    return merged


def get_secret(name: str, default: Optional[str] = None) -> Optional[str]:
    """Resolve a secret/config value: os.environ first, then .env files."""
    if name in os.environ and os.environ[name] != "":
        return os.environ[name]
    return _env_files().get(name, default)


def require_secret(name: str) -> str:
    val = get_secret(name)
    if not val:
        raise RuntimeError(
            f"Required secret '{name}' not found. Set it in the environment "
            f"or in horizon/.env (see horizon/.env.example)."
        )
    return val


def _base_output_dir() -> Path:
    """Runtime output root. Honors ALGO_OUTPUT_DIR like the rest of the project."""
    env = os.environ.get("ALGO_OUTPUT_DIR")
    if env:
        return Path(env) / "horizon"
    return PACKAGE_DIR


def cache_dir() -> Path:
    d = PACKAGE_DIR / "data" / "cache"
    d.mkdir(parents=True, exist_ok=True)
    return d


def results_dir() -> Path:
    d = PACKAGE_DIR / "results"
    d.mkdir(parents=True, exist_ok=True)
    return d


def state_dir() -> Path:
    d = _base_output_dir() / "state"
    d.mkdir(parents=True, exist_ok=True)
    return d


def log_dir() -> Path:
    d = _base_output_dir() / "logs"
    d.mkdir(parents=True, exist_ok=True)
    return d
