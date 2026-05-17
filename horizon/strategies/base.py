"""Strategy contract — the single source of truth.

A Strategy is a pure function of a MarketView (no-look-ahead market data) and a
mutable state dict. It returns target weights. The live engine and the backtest
harness both import and call the identical `decide()` — there is no separate
"backtest version" of any strategy.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Dict, List, Optional

import pandas as pd

# Execution modes -------------------------------------------------------------
NEXT_OPEN = "NEXT_OPEN"   # decide on close(T), fill at open(T+1), hold until exit
OVERNIGHT = "OVERNIGHT"   # decide on close(T), capture close(T) -> open(T+1)


class MarketView:
    """Read-only, no-look-ahead view of market data as of a single date.

    A MarketView physically cannot return a bar dated after `as_of`: future
    rows are sliced off before any strategy sees them. This is the structural
    guarantee against look-ahead — it is not a convention the caller must
    remember.
    """

    __slots__ = ("_dataset", "as_of")

    def __init__(self, dataset: Dict[str, pd.DataFrame], as_of):
        self._dataset = dataset
        self.as_of = pd.Timestamp(as_of)

    def history(self, symbol: str, lookback: Optional[int] = None) -> pd.DataFrame:
        """Daily bars up to and including `as_of`. Treat the result as read-only."""
        df = self._dataset.get(symbol)
        if df is None:
            raise KeyError(f"symbol not in dataset: {symbol}")
        df = df.loc[:self.as_of]
        if lookback is not None:
            df = df.iloc[-lookback:]
        return df

    def is_tradable(self, symbol: str) -> bool:
        """True if the symbol has a bar exactly on `as_of` (i.e. it exists/trades)."""
        df = self._dataset.get(symbol)
        return df is not None and self.as_of in df.index

    def trading_age(self, symbol: str) -> int:
        """Number of bars of history available up to `as_of` (warmup check)."""
        df = self._dataset.get(symbol)
        if df is None:
            return 0
        return int((df.index <= self.as_of).sum())

    def close(self, symbol: str) -> float:
        h = self.history(symbol, lookback=1)
        return float(h["close"].iloc[-1]) if len(h) else float("nan")

    def closes(self, symbol: str, lookback: Optional[int] = None) -> pd.Series:
        return self.history(symbol, lookback)["close"]

    def tr_closes(self, symbol: str, lookback: Optional[int] = None) -> pd.Series:
        """Total-return-adjusted close series — use for performance/momentum."""
        return self.history(symbol, lookback)["tr_close"]

    def bar(self, symbol: str) -> Optional[pd.Series]:
        h = self.history(symbol, lookback=1)
        return h.iloc[-1] if len(h) else None


@dataclass
class Decision:
    """A strategy's desired book, expressed as target weights.

    Each weight is a fraction of the strategy's own sleeve equity. A total above
    1.0 means leverage (only PULSE does this). An empty book means hold cash.
    """

    target_weights: Dict[str, float] = field(default_factory=dict)
    note: str = ""

    def clean(self) -> "Decision":
        self.target_weights = {s: float(w) for s, w in self.target_weights.items()
                               if abs(w) > 1e-9}
        return self


class Strategy(ABC):
    """Base class for all Horizon strategies."""

    strategy_id: str = "BASE"
    execution: str = NEXT_OPEN

    @abstractmethod
    def universe(self) -> List[str]:
        """Every symbol this strategy may hold or read."""

    @abstractmethod
    def warmup_days(self) -> int:
        """Bars of history required before the first meaningful decision."""

    @abstractmethod
    def decide(self, view: MarketView, state: dict) -> Decision:
        """Return the desired target book given data through `view.as_of`."""

    def initial_state(self) -> dict:
        return {}
