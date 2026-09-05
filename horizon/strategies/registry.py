"""Strategy registry — the canonical list of Horizon strategies.

Both the backtest harness and the live engine build strategies from here, so
they always run the identical objects.
"""

from __future__ import annotations

from typing import Dict, List

from .base import Strategy
from .drift import DriftStrategy
from .pulse import PulseStrategy
from .revert import RevertStrategy
from .rotation import RotationStrategy

STRATEGY_CLASSES = [PulseStrategy, RotationStrategy, RevertStrategy, DriftStrategy]


def _params(strategy_id: str) -> dict:
    """Live constructor kwargs from config.strategy_params (single source of
    truth — the validation's BASELINE reads the same dict)."""
    from ..config import build_default_config
    return dict(build_default_config().strategy_params.get(strategy_id, {}))


def build_all() -> Dict[str, Strategy]:
    return {cls.strategy_id: cls(**_params(cls.strategy_id))
            for cls in STRATEGY_CLASSES}


def build(strategy_id: str) -> Strategy:
    for cls in STRATEGY_CLASSES:
        if cls.strategy_id == strategy_id:
            return cls(**_params(strategy_id))
    raise KeyError(f"unknown strategy: {strategy_id}")


def strategy_ids() -> List[str]:
    return [cls.strategy_id for cls in STRATEGY_CLASSES]
