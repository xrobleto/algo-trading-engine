"""PULSE — adaptive trend-timed leveraged growth core.

Holds QQQ with volatility-targeted leverage while the medium-term trend is
confirmed up; steps entirely into T-bills (BIL) when the trend breaks. This is
the engine's return driver — the only sleeve that can plausibly beat QQQ by a
wide margin (see docs/DESIGN.md section 5.1).

Risk control is **volatility targeting**, not a binary trend filter: when QQQ's
realized volatility rises (it always does in a crash), leverage falls
automatically — continuous, with no whipsaw. A binary EMA trend filter was
built and tested (see `use_trend_filter`); the validation found it cuts return
more than it cuts risk across 2008-2026, in-sample and out-of-sample, so it is
OFF by default. That negative result is reported honestly in docs/VALIDATION.md.

Leverage is explicit, capped at 2.0x (Alpaca Reg-T), and costed every day —
never phantom. Parameters are __init__ arguments so the validation can sweep
them for the A6 robustness test and walk-forward.
"""

from __future__ import annotations

from typing import List

from . import indicators as ind
from .base import NEXT_OPEN, Decision, MarketView, Strategy
from ..data import universe as _universe

# Sourced from data.universe — the single symbol registry — so the live
# symbol-equivalents mode (HORIZON_SYMBOL_EQUIVALENTS=1: QQQ->QQQM, BIL->SHV
# for shared-account coexistence) propagates. Defaults are unchanged.
RISK_ASSET = _universe.PULSE_RISK_ASSET
CASH_ASSET = _universe.PULSE_CASH_ASSET

# Documented default parameters (round numbers, not curve-fitted).
DEFAULT_TREND_EMA = 105        # faster than the pre-2015 200-day norm
DEFAULT_HYSTERESIS = 0.015     # +/-1.5% band around the EMA cross
DEFAULT_ABS_MOM_DAYS = 126     # 6-month absolute-momentum entry confirmation
DEFAULT_VOL_DAYS = 20          # realized-volatility lookback
# 0.22 keeps standalone PULSE drawdown within its A3 gating limit (<= 45%).
# Portfolio-level leverage (config.book_leverage) is applied on the diversified
# blend, not by over-levering this single sleeve.
DEFAULT_TARGET_VOL = 0.22      # annualized sleeve volatility target
DEFAULT_MAX_LEVERAGE = 2.0     # Alpaca Reg-T overnight maximum
DEFAULT_MIN_LEVERAGE = 0.5


class PulseStrategy(Strategy):
    strategy_id = "PULSE"
    execution = NEXT_OPEN

    def __init__(self, trend_ema: int = DEFAULT_TREND_EMA,
                 hysteresis: float = DEFAULT_HYSTERESIS,
                 abs_mom_days: int = DEFAULT_ABS_MOM_DAYS,
                 vol_days: int = DEFAULT_VOL_DAYS,
                 target_vol: float = DEFAULT_TARGET_VOL,
                 max_leverage: float = DEFAULT_MAX_LEVERAGE,
                 min_leverage: float = DEFAULT_MIN_LEVERAGE,
                 use_trend_filter: bool = False):
        self.trend_ema = int(trend_ema)
        self.hysteresis = float(hysteresis)
        self.abs_mom_days = int(abs_mom_days)
        self.vol_days = int(vol_days)
        self.target_vol = float(target_vol)
        self.max_leverage = float(max_leverage)
        self.min_leverage = float(min_leverage)
        self.use_trend_filter = bool(use_trend_filter)

    def universe(self) -> List[str]:
        return [RISK_ASSET, CASH_ASSET]

    def warmup_days(self) -> int:
        return self.trend_ema + self.abs_mom_days

    def initial_state(self) -> dict:
        return {"trend_on": False}

    def _leverage(self, closes) -> float:
        rvol = ind.last(ind.realized_vol(closes, self.vol_days))
        if rvol is None or rvol != rvol or rvol <= 0:
            lev = 1.0
        else:
            lev = self.target_vol / rvol
        return max(self.min_leverage, min(self.max_leverage, lev))

    def decide(self, view: MarketView, state: dict) -> Decision:
        closes = view.closes(RISK_ASSET)
        if len(closes) < self.trend_ema + 5:
            return Decision({}, "warmup")

        price = float(closes.iloc[-1])
        ema = ind.last(ind.ema(closes, self.trend_ema))
        if price <= 0 or ema != ema:
            return Decision({}, "no signal")

        if not self.use_trend_filter:
            lev = self._leverage(closes)
            return Decision({RISK_ASSET: round(lev, 3)}, f"always-on lev={lev:.2f}")

        mom6 = (float(price / closes.iloc[-self.abs_mom_days] - 1.0)
                if len(closes) > self.abs_mom_days else 0.0)

        # Trend state with hysteresis: enter above the upper band with positive
        # 6-month momentum; exit only on a clean break below the lower band.
        trend_on = bool(state.get("trend_on", False))
        if trend_on:
            if price < ema * (1.0 - self.hysteresis):
                trend_on = False
        else:
            if price > ema * (1.0 + self.hysteresis) and mom6 > 0.0:
                trend_on = True
        state["trend_on"] = trend_on

        if not trend_on:
            if view.is_tradable(CASH_ASSET):
                return Decision({CASH_ASSET: 1.0}, "risk-off: T-bills")
            return Decision({}, "risk-off: cash")

        lev = self._leverage(closes)
        return Decision({RISK_ASSET: round(lev, 3)}, f"trend-on lev={lev:.2f}")
