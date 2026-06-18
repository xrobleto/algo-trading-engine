"""SIMPLE v2 — parabolic-continuation strategy (single source of truth).

The faithful backtest harness and the live executor both import from here, so
there is exactly one copy of the decision logic.
"""
from .strategy import (  # noqa: F401
    STRATEGY_ID, STRATEGY_VARIANT, ParabolicConfig,
    OpeningRange, EntryIntent, PositionState, ManageAction,
    atr, compute_opening_range, is_in_play, rank_in_play,
    entry_decision, arm_entry, position_size, manage_position,
    passes_microstructure, source_fingerprint,
)
