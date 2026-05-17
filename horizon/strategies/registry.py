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


def build_all() -> Dict[str, Strategy]:
    return {cls.strategy_id: cls() for cls in STRATEGY_CLASSES}


def build(strategy_id: str) -> Strategy:
    for cls in STRATEGY_CLASSES:
        if cls.strategy_id == strategy_id:
            return cls()
    raise KeyError(f"unknown strategy: {strategy_id}")


def strategy_ids() -> List[str]:
    return [cls.strategy_id for cls in STRATEGY_CLASSES]
