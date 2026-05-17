"""Market intelligence / regime layer.

Classifies the market regime from four backtestable signals — trend,
volatility, breadth, and credit — and emits bounded per-sleeve allocation
tilts. Every input has deep history and is computable live, so the regime the
backtest sees is exactly the regime the live engine would see (DESIGN.md s3, s6).

`compute_regime` is a pure function of a MarketView: no look-ahead, no network.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict

import pandas as pd

from ..strategies import indicators as ind
from ..strategies.base import MarketView

REGIMES = ["CRISIS", "RISK_OFF", "NEUTRAL", "RISK_ON"]

# Bounded per-sleeve allocation multipliers by regime (a mild tilt; the real
# risk control is PULSE's vol targeting and the portfolio drawdown overlay).
ALLOC_MULT: Dict[str, Dict[str, float]] = {
    "RISK_ON":  {"PULSE": 1.10, "ROTATION": 0.92, "REVERT": 1.06, "DRIFT": 1.05},
    "NEUTRAL":  {"PULSE": 1.00, "ROTATION": 1.00, "REVERT": 1.00, "DRIFT": 1.00},
    "RISK_OFF": {"PULSE": 0.92, "ROTATION": 1.12, "REVERT": 0.96, "DRIFT": 0.95},
    "CRISIS":   {"PULSE": 0.88, "ROTATION": 1.18, "REVERT": 0.90, "DRIFT": 0.88},
}

# Composite-score weights for the four signals.
SIGNAL_WEIGHTS = {"trend": 0.40, "vol": 0.30, "breadth": 0.15, "credit": 0.15}


@dataclass
class RegimeContext:
    as_of: pd.Timestamp
    regime: str
    score: float
    components: Dict[str, float] = field(default_factory=dict)

    def alloc_mult(self, strategy_id: str) -> float:
        return ALLOC_MULT.get(self.regime, {}).get(strategy_id, 1.0)


def _percentile(series: pd.Series, window: int) -> float:
    s = series.dropna()
    if len(s) < 20:
        return 0.5
    ref = s.iloc[-window:] if len(s) > window else s
    return float((ref < s.iloc[-1]).mean())


def _relative_score(view: MarketView, a: str, b: str, lookback: int) -> float:
    """Map the a-vs-b total-return spread over `lookback` days to 0-100."""
    ca, cb = view.tr_closes(a), view.tr_closes(b)
    if len(ca) <= lookback or len(cb) <= lookback:
        return 50.0
    ra = ca.iloc[-1] / ca.iloc[-1 - lookback] - 1.0
    rb = cb.iloc[-1] / cb.iloc[-1 - lookback] - 1.0
    return float(max(0.0, min(100.0, 50.0 + (ra - rb) * 500.0)))


def compute_regime(view: MarketView) -> RegimeContext:
    comp: Dict[str, float] = {}

    # Trend: fraction of SPY/QQQ above their 200-day SMA.
    hits = total = 0
    for sym in ("SPY", "QQQ"):
        closes = view.closes(sym)
        if len(closes) >= 200:
            total += 1
            if float(closes.iloc[-1]) > ind.last(ind.sma(closes, 200)):
                hits += 1
    comp["trend"] = (hits / total * 100.0) if total else 50.0

    # Volatility: SPY 20-day realized vol percentile (inverted — calm is good).
    spy = view.closes("SPY")
    if len(spy) >= 80:
        rv = ind.realized_vol(spy, 20)
        comp["vol"] = (1.0 - _percentile(rv, 504)) * 100.0
    else:
        comp["vol"] = 50.0

    # Breadth: equal-weight vs cap-weight S&P (the concentration signal).
    comp["breadth"] = _relative_score(view, "RSP", "SPY", 60)
    # Credit: high-yield vs treasuries (risk appetite).
    comp["credit"] = _relative_score(view, "HYG", "IEF", 60)

    score = sum(SIGNAL_WEIGHTS[k] * comp[k] for k in SIGNAL_WEIGHTS)
    if score >= 65:
        regime = "RISK_ON"
    elif score >= 45:
        regime = "NEUTRAL"
    elif score >= 30:
        regime = "RISK_OFF"
    else:
        regime = "CRISIS"
    return RegimeContext(view.as_of, regime, score, comp)
