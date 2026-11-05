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

from typing import List, Optional

import numpy as np
import pandas as pd

from . import indicators as ind
from .base import NEXT_OPEN, Decision, MarketView, Strategy
from ..data import universe as _universe

# Sourced from data.universe — the single symbol registry — so the live
# symbol-equivalents mode (HORIZON_SYMBOL_EQUIVALENTS=1: QQQ->QQQM, BIL->SHV
# for shared-account coexistence) propagates. Defaults are unchanged.
RISK_ASSET = _universe.PULSE_RISK_ASSET
CASH_ASSET = _universe.PULSE_CASH_ASSET
LEVERED_ASSET = _universe.PULSE_LEVERED_ASSET

# How leverage above 1.0x is expressed:
#   "margin"      — weight QQQ > 1.0; the account borrows (needs a margin
#                   account; the validated 2026-05 configuration).
#   "levered_etf" — QQQ/QLD mix with weights summing to 1.0: w_QLD = L - 1,
#                   w_QQQ = 2 - L. No borrowing, no buying-power dependency;
#                   QLD's own financing + 0.95% ER are embedded in its price.
#                   CANDIDATE (2026-09-05 audit) — must clear the gating bar
#                   before it carries capital.
LEVERAGE_VIA_MARGIN = "margin"
LEVERAGE_VIA_ETF = "levered_etf"

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

# Recovery mode (docs/RECOVERY_STUDY_PREREG.md) — CANDIDATE, off by default
# (recovery_dd=None). After a drawdown of at least `recovery_dd` from the
# 252-session high, a close above the `recovery_sma`-day average (+hysteresis)
# confirms the recovery: PULSE then holds at least `recovery_leverage` until a
# new 252-session high (success), or until a close below the average
# (-hysteresis): the rally failed, and recovery mode may re-trigger while the
# drawdown lasts. Volatility targeting alone re-levers only after realized vol
# falls, weeks into a recovery.
RECOVERY_LOOKBACK = 252
MODE_NORMAL, MODE_ARMED, MODE_RECOVERY = 0, 1, 2


class RecoveryTracker:
    """Recovery-mode state per bar, a pure function of closes through that bar.

    Incremental: backtests call decide() once a day on a series that grows by
    one bar, so only new bars are processed. If the history changes (a restart
    on different data, a split adjustment) it recomputes from the start.
    """

    def __init__(self, dd: float, sma_days: int, hysteresis: float,
                 lookback: int = RECOVERY_LOOKBACK):
        self.dd, self.sma_days = float(dd), int(sma_days)
        self.hysteresis, self.lookback = float(hysteresis), int(lookback)
        self._reset()

    def _reset(self) -> None:
        self._modes: List[int] = []
        self._last = None
        self._armed = self._rec = False

    def modes(self, closes: pd.Series) -> List[int]:
        n, done = len(closes), len(self._modes)
        if done and (n < done or (closes.index[done - 1], float(closes.iloc[done - 1]))
                     != self._last):
            self._reset()
            done = 0
        c = closes.to_numpy(dtype=float)
        for i in range(done, n):
            high = c[max(0, i - self.lookback + 1):i + 1].max()
            if c[i] >= high:                       # new 252-session high: over
                self._armed = self._rec = False
            else:
                if c[i] <= high * (1.0 - self.dd):
                    self._armed = True
                if self._armed and i + 1 >= self.sma_days:
                    sma = c[i + 1 - self.sma_days:i + 1].mean()
                    if not self._rec and c[i] > sma * (1.0 + self.hysteresis):
                        self._rec = True
                    elif self._rec and c[i] < sma * (1.0 - self.hysteresis):
                        self._rec = False
            self._modes.append(MODE_RECOVERY if self._rec
                               else MODE_ARMED if self._armed else MODE_NORMAL)
        if n:
            self._last = (closes.index[-1], float(c[-1]))
        return self._modes


def recovery_modes(closes: pd.Series, dd: float, sma_days: int,
                   hysteresis: float = DEFAULT_HYSTERESIS) -> np.ndarray:
    """Recovery-mode state for every bar of `closes` (MODE_* values)."""
    return np.asarray(RecoveryTracker(dd, sma_days, hysteresis).modes(closes))


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
                 use_trend_filter: bool = False,
                 leverage_via: str = LEVERAGE_VIA_MARGIN,
                 recovery_dd: Optional[float] = None,
                 recovery_sma: int = 50,
                 recovery_leverage: float = 2.0):
        self.trend_ema = int(trend_ema)
        self.hysteresis = float(hysteresis)
        self.abs_mom_days = int(abs_mom_days)
        self.vol_days = int(vol_days)
        self.target_vol = float(target_vol)
        self.max_leverage = float(max_leverage)
        self.min_leverage = float(min_leverage)
        self.use_trend_filter = bool(use_trend_filter)
        if leverage_via not in (LEVERAGE_VIA_MARGIN, LEVERAGE_VIA_ETF):
            raise ValueError(f"leverage_via must be margin|levered_etf, got {leverage_via}")
        self.leverage_via = leverage_via
        self.recovery_dd = float(recovery_dd) if recovery_dd else None
        self.recovery_sma = int(recovery_sma)
        self.recovery_leverage = float(recovery_leverage)
        self._tracker = (RecoveryTracker(self.recovery_dd, self.recovery_sma, self.hysteresis)
                         if self.recovery_dd else None)

    def universe(self) -> List[str]:
        if self.leverage_via == LEVERAGE_VIA_ETF:
            return [RISK_ASSET, LEVERED_ASSET, CASH_ASSET]
        return [RISK_ASSET, CASH_ASSET]

    def _express(self, view: MarketView, lev: float, note: str) -> Decision:
        """Turn a leverage number into target weights per `leverage_via`."""
        if (self.leverage_via == LEVERAGE_VIA_ETF and lev > 1.0
                and view.is_tradable(LEVERED_ASSET)):
            lev = min(lev, 2.0)
            return Decision({RISK_ASSET: round(2.0 - lev, 3),
                             LEVERED_ASSET: round(lev - 1.0, 3)},
                            f"{note} via {LEVERED_ASSET}")
        return Decision({RISK_ASSET: round(lev, 3)}, note)

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
            note = f"always-on lev={lev:.2f}"
            if self._tracker is not None:
                modes = self._tracker.modes(closes)
                now = modes[-1]
                prev = modes[-2] if len(modes) > 1 else MODE_NORMAL
                if now == MODE_RECOVERY:
                    lev = min(self.max_leverage, max(lev, self.recovery_leverage))
                    note = f"recovery lev={lev:.2f}"
                # Recovery start, or a failed rally: act at the next open
                # instead of waiting for the no-trade band. A success exit
                # (new high) reverts to the normal rules without forcing.
                if (now == MODE_RECOVERY) != (prev == MODE_RECOVERY) and now != MODE_NORMAL:
                    state["force_rebalance"] = True
                state["recovery_mode"] = int(now)
            return self._express(view, lev, note)

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
        return self._express(view, lev, f"trend-on lev={lev:.2f}")
