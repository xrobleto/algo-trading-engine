"""Portfolio drawdown risk overlay.

The Unified Engine's worst bug measured drawdown against a never-resetting
all-time peak, so a deep drawdown permanently suppressed sizing with no way
back — a death-spiral. Horizon measures drawdown against a **rolling** high and
de-risks **continuously** with a floor, so every drawdown state has a guaranteed
recovery path (DESIGN.md section 7.1).
"""

from __future__ import annotations

import pandas as pd

ROLLING_LOOKBACK = 252   # rolling reference window — NOT all-time
DD_SOFT = 0.12           # drawdown where de-risking begins
DD_HARD = 0.32           # drawdown where exposure reaches its floor
EXPOSURE_FLOOR = 0.50    # never de-risk below this — exposure can always recover


def exposure_from_drawdown(equity: pd.Series,
                           lookback: int = ROLLING_LOOKBACK,
                           dd_soft: float = DD_SOFT,
                           dd_hard: float = DD_HARD,
                           floor: float = EXPOSURE_FLOOR) -> pd.Series:
    """Exposure multiplier in [floor, 1.0] from the rolling-high drawdown.

    Because the reference is a rolling `lookback`-day high (not all-time), when
    equity climbs back to that rolling high the drawdown is zero and exposure
    returns to 1.0 — recovery is structurally guaranteed.
    """
    rolling_high = equity.rolling(lookback, min_periods=1).max()
    drawdown = equity / rolling_high - 1.0           # <= 0
    span = max(dd_hard - dd_soft, 1e-9)
    derisk = ((-drawdown - dd_soft) / span).clip(0.0, 1.0)
    return (1.0 - (1.0 - floor) * derisk).rename("exposure")


def recovery_is_guaranteed() -> bool:
    """Self-documenting invariant, asserted by tests/test_risk.py:
    a fully-recovered equity curve always yields exposure == 1.0."""
    idx = pd.date_range("2020-01-01", periods=400, freq="B")
    crash = pd.Series(range(400), index=idx, dtype=float)
    crash.iloc[200:260] = crash.iloc[200] * 0.6      # deep drawdown
    crash.iloc[260:] = crash.iloc[:140].max() + 1000  # full recovery to new highs
    exp = exposure_from_drawdown(crash)
    return bool(exp.iloc[-1] == 1.0)
