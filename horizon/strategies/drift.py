"""DRIFT — overnight seasonality on QQQ.

US index ETFs have historically earned the large majority of their return in
the overnight (close->open) session. DRIFT holds QQQ over that window when the
market is not risk-off, skipping the weekend hold. It is a near-orthogonal,
time-of-day return stream (docs/DESIGN.md section 5.4).

This is also a deliberate honesty test: independent research shows the overnight
edge shrinks at realistic fills. If DRIFT fails the gating bar once costs are
modeled, that failure is reported plainly.
"""

from __future__ import annotations

from typing import List

from . import indicators as ind
from .base import OVERNIGHT, Decision, MarketView, Strategy

ASSET = "QQQ"
MARKET = "SPY"
REGIME_SMA = 200
SKIP_WEEKDAY = 4   # Friday — do not carry the position over the weekend


class DriftStrategy(Strategy):
    strategy_id = "DRIFT"
    execution = OVERNIGHT

    def universe(self) -> List[str]:
        return [ASSET, MARKET]

    def warmup_days(self) -> int:
        return REGIME_SMA + 10

    def initial_state(self) -> dict:
        return {}

    def decide(self, view: MarketView, state: dict) -> Decision:
        # `decide` runs on the close of day T; the harness credits the
        # close(T) -> open(T+1) return. The decision strictly precedes both legs.
        if view.as_of.weekday() == SKIP_WEEKDAY:
            return Decision({}, "skip weekend")
        spy = view.closes(MARKET)
        if len(spy) < REGIME_SMA:
            return Decision({}, "warmup")
        spy_sma = ind.last(ind.sma(spy, REGIME_SMA))
        if float(spy.iloc[-1]) > spy_sma:
            return Decision({ASSET: 1.0}, "overnight long")
        return Decision({}, "risk-off")
