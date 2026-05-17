"""Walk-forward / multi-regime validation driver.

The engine's parameters are fixed a priori (round numbers, reasoned — not
fitted per window). Walk-forward's job here is to test *stability* across
regimes: every distinct regime — the 2008 GFC, the 2010-19 bull, the 2020
crash, the 2022 bear, 2023-26 — is an out-of-sample window, and a strategy must
behave acceptably in each (gating bar A7).
"""

from __future__ import annotations

from typing import Dict, List, Optional, Tuple

import pandas as pd

from ..config import CostModel
from ..strategies.base import Strategy
from .harness import benchmark_curve, run_strategy
from .metrics import compute_metrics

# Non-overlapping regime windows spanning the full sample.
WINDOWS: List[Tuple[str, str, str]] = [
    ("2008-2010  GFC + recovery", "2008-01-02", "2010-12-31"),
    ("2011-2013  euro crisis / chop", "2011-01-03", "2013-12-31"),
    ("2014-2016  oil bust / China", "2014-01-02", "2016-12-30"),
    ("2017-2019  late bull / 2018 Q4", "2017-01-03", "2019-12-31"),
    ("2020-2022  COVID crash + bear", "2020-01-02", "2022-12-30"),
    ("2023-2026  AI bull", "2023-01-03", "2026-05-15"),
]


def run_walkforward(strategy: Strategy, dataset: Dict[str, pd.DataFrame],
                    cost: Optional[CostModel] = None,
                    benchmark: str = "QQQ") -> List[Tuple[str, Dict[str, float]]]:
    """Return [(window_label, metrics)] for one strategy across all windows."""
    out = []
    for label, start, end in WINDOWS:
        result = run_strategy(strategy, dataset, start, end, 100_000.0, cost)
        bench = benchmark_curve(dataset, benchmark, start, end)
        out.append((label, compute_metrics(result.equity, bench)))
    return out
