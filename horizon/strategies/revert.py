"""REVERT — mean-reversion swing on oversold ETFs.

In a healthy market, buy liquid index/sector ETFs that are sharply oversold
within their own uptrend; exit into the bounce. Mean reversion is the textbook
diversifier to trend following — it earns when trend chops — and it realizes
gains every few days, which serves the monthly-withdrawal goal
(docs/DESIGN.md section 5.3).
"""

from __future__ import annotations

from typing import List

from . import indicators as ind
from .base import NEXT_OPEN, Decision, MarketView, Strategy

UNIVERSE = ["SPY", "QQQ", "IWM", "DIA",
            "XLK", "XLF", "XLV", "XLY", "XLE", "XLI", "SMH"]
MARKET = "SPY"

REGIME_SMA = 200       # market healthy only when SPY is above this
ETF_TREND_SMA = 200    # buy dips only in ETFs that are themselves in an uptrend
RSI_LEN = 2            # Connors-style short-horizon oscillator
RSI_ENTRY = 10.0       # enter when deeply oversold
RSI_EXIT = 65.0        # exit when reverted
FAST_SMA = 5           # exit when price reclaims this
TIME_STOP_DAYS = 7     # exit a stale trade
HARD_STOP = -0.08      # exit a position down 8% from its signal reference
MAX_POSITIONS = 4


class RevertStrategy(Strategy):
    strategy_id = "REVERT"
    execution = NEXT_OPEN

    def universe(self) -> List[str]:
        return list(UNIVERSE)

    def warmup_days(self) -> int:
        return REGIME_SMA + 10

    def initial_state(self) -> dict:
        return {"positions": {}}

    def decide(self, view: MarketView, state: dict) -> Decision:
        positions = state.get("positions", {})

        # --- Market regime gate -------------------------------------------
        spy = view.closes(MARKET)
        if len(spy) < REGIME_SMA:
            state["positions"] = {}
            return Decision({}, "warmup")
        spy_sma = ind.last(ind.sma(spy, REGIME_SMA))
        if not (float(spy.iloc[-1]) > spy_sma):
            state["positions"] = {}
            return Decision({}, "risk-off: flat")

        # --- Manage existing positions: age them, evaluate exits ----------
        survivors = {}
        for sym, pos in positions.items():
            if not view.is_tradable(sym):
                continue  # cannot manage — drop
            closes = view.closes(sym)
            price = float(closes.iloc[-1])
            rsi = ind.last(ind.rsi(closes, RSI_LEN))
            sma5 = ind.last(ind.sma(closes, FAST_SMA))
            days_held = int(pos.get("days_held", 0)) + 1
            entry_ref = float(pos.get("entry_ref", price))
            ret = price / entry_ref - 1.0

            exited = (
                (rsi == rsi and rsi > RSI_EXIT)
                or (sma5 == sma5 and price > sma5)
                or days_held >= TIME_STOP_DAYS
                or ret <= HARD_STOP
            )
            if not exited:
                survivors[sym] = {"entry_ref": entry_ref, "days_held": days_held}

        # --- New entries: most-oversold dips first ------------------------
        slots = MAX_POSITIONS - len(survivors)
        if slots > 0:
            candidates = []
            for sym in UNIVERSE:
                if sym in survivors or not view.is_tradable(sym):
                    continue
                closes = view.closes(sym)
                if len(closes) < ETF_TREND_SMA:
                    continue
                price = float(closes.iloc[-1])
                rsi = ind.last(ind.rsi(closes, RSI_LEN))
                sma200 = ind.last(ind.sma(closes, ETF_TREND_SMA))
                if (rsi == rsi and rsi < RSI_ENTRY
                        and sma200 == sma200 and price > sma200):
                    candidates.append((rsi, sym, price))
            candidates.sort()  # ascending RSI -> most oversold first
            for _rsi, sym, price in candidates[:slots]:
                survivors[sym] = {"entry_ref": price, "days_held": 0}

        state["positions"] = survivors
        slot_weight = 1.0 / MAX_POSITIONS
        weights = {sym: slot_weight for sym in survivors}
        return Decision(weights, f"held={len(survivors)}")
