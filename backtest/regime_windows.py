"""
Regime windows for multi-period engine backtests.

Each tuple is (label, start_date, end_date, description). Labels are embedded in output
filenames and report headers, so keep them filesystem-safe (no spaces, no slashes).

Selected per the engine backtest plan (dazzling-swimming-shore.md) to cover a variety of
distinctly different market regimes. Dropped similar windows (2022-23 range, 2024
consolidation) to keep runtime manageable.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date


@dataclass(frozen=True)
class RegimeWindow:
    label: str
    start: date
    end: date
    description: str


REGIME_WINDOWS = [
    RegimeWindow(
        "P1_2021_Euphoria",
        date(2021, 10, 1),
        date(2021, 12, 31),
        "Late-cycle euphoria — tops form, last leg of QE bull",
    ),
    RegimeWindow(
        "P2_2022_RateShock",
        date(2022, 1, 3),
        date(2022, 6, 30),
        "Fed pivot; tech/growth crushed, 1H22 bear",
    ),
    RegimeWindow(
        "P3_2022_Capitulation",
        date(2022, 7, 1),
        date(2022, 12, 30),
        "Oct '22 low, rally into year-end",
    ),
    RegimeWindow(
        "P4_2023_BankStress",
        date(2023, 3, 1),
        date(2023, 5, 31),
        "SVB, regional bank failures, flight-to-quality",
    ),
    RegimeWindow(
        "P5_2023_AIRally",
        date(2023, 6, 1),
        date(2023, 12, 29),
        "NVDA breakout, narrow AI leadership rally",
    ),
    RegimeWindow(
        "P6_2025_Recent",
        date(2025, 10, 1),
        date(2026, 4, 1),
        "Last 6 months — most relevant to current engine config",
    ),
]


def by_label(label: str) -> RegimeWindow:
    for w in REGIME_WINDOWS:
        if w.label == label:
            return w
    raise KeyError(f"No regime window with label {label!r}")
