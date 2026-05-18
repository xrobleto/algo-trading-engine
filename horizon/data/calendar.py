"""Trading calendar.

The calendar is derived from the data itself — Polygon emits a daily bar only
on actual trading days, so the dates present in a liquid reference symbol
(SPY) ARE the NYSE trading days. This needs no external dependency and cannot
drift out of sync with the price data the backtest uses.
"""

from __future__ import annotations

from typing import Dict, List

import pandas as pd

REFERENCE_SYMBOL = "SPY"


def trading_days(dataset: Dict[str, pd.DataFrame],
                 reference: str = REFERENCE_SYMBOL) -> List[pd.Timestamp]:
    """Ordered list of trading days from the reference symbol's bars."""
    if reference in dataset and not dataset[reference].empty:
        return list(dataset[reference].index)
    idx = None
    for df in dataset.values():
        if df is None or df.empty:
            continue
        idx = df.index if idx is None else idx.union(df.index)
    return list(idx) if idx is not None else []


def window(days: List[pd.Timestamp], start: str, end: str) -> List[pd.Timestamp]:
    """Slice an ordered trading-day list to [start, end] inclusive."""
    lo, hi = pd.Timestamp(start), pd.Timestamp(end)
    return [d for d in days if lo <= d <= hi]
