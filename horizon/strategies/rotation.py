"""ROTATION — cross-asset dual momentum.

Each month, allocate to the strongest-momentum assets among a low-correlation
set (US growth equity, international equity, long Treasuries, gold,
commodities), but only those whose absolute momentum also beats T-bills.
Otherwise hold T-bills. The all-weather diversifier — designed to earn in the
non-equity regimes where PULSE sits in cash (docs/DESIGN.md section 5.2).

Parameters are __init__ arguments so the validation can sweep them for the A6
robustness test.
"""

from __future__ import annotations

from typing import List, Optional, Sequence

from .base import NEXT_OPEN, Decision, MarketView, Strategy
from ..data import universe as _universe

# Sourced from data.universe (single symbol registry) so live symbol-equivalents
# mode propagates (see universe.py). Defaults unchanged for backtests.
RISK_ASSETS = list(_universe.ROTATION_ASSETS)
CASH_ASSET = _universe.ROTATION_CASH_ASSET

DEFAULT_LOOKBACKS = (63, 126, 252)   # ~3, 6, 12 months
DEFAULT_TOP_N = 2


class RotationStrategy(Strategy):
    strategy_id = "ROTATION"
    execution = NEXT_OPEN

    def __init__(self, lookbacks: Sequence[int] = DEFAULT_LOOKBACKS,
                 top_n: int = DEFAULT_TOP_N, retain_rank: Optional[int] = None):
        self.lookbacks = tuple(int(x) for x in lookbacks)
        self.top_n = int(top_n)
        # Hold buffer (tax study, 2026-09-25): a current holding is kept while
        # it still ranks within the top `retain_rank` eligible assets, instead
        # of being swapped the moment it drops to #3. Longer holds turn
        # short-term gains into long-term ones. None = original rule.
        self.retain_rank = int(retain_rank) if retain_rank else None

    def universe(self) -> List[str]:
        return RISK_ASSETS + [CASH_ASSET]

    def warmup_days(self) -> int:
        return max(self.lookbacks) + 5

    def initial_state(self) -> dict:
        return {"last_rebal_month": None, "holdings": {}}

    def _blended_momentum(self, view: MarketView, symbol: str) -> Optional[float]:
        """Average total-return momentum across the lookbacks; None if thin."""
        if not view.is_tradable(symbol):
            return None
        tr = view.tr_closes(symbol)
        if len(tr) <= max(self.lookbacks):
            return None
        scores = [float(tr.iloc[-1] / tr.iloc[-1 - lb] - 1.0)
                  for lb in self.lookbacks]
        return sum(scores) / len(scores)

    def decide(self, view: MarketView, state: dict) -> Decision:
        month_key = f"{view.as_of.year}-{view.as_of.month:02d}"

        # Hold between monthly rebalances.
        if state.get("last_rebal_month") == month_key:
            return Decision(dict(state.get("holdings", {})), "hold")
        state["last_rebal_month"] = month_key

        mom = {}
        for asset in RISK_ASSETS:
            score = self._blended_momentum(view, asset)
            if score is not None:
                mom[asset] = score
        cash_mom = self._blended_momentum(view, CASH_ASSET)
        floor = cash_mom if cash_mom is not None else 0.0  # absolute-momentum gate

        eligible = sorted((a for a, m in mom.items() if m > floor),
                          key=lambda a: mom[a], reverse=True)
        selected = eligible[:self.top_n]
        if self.retain_rank and self.retain_rank > self.top_n:
            prev = [a for a in state.get("holdings", {}) if a in RISK_ASSETS]
            keep = [a for a in eligible[:self.retain_rank] if a in prev][:self.top_n]
            selected = keep + [a for a in eligible if a not in keep][:self.top_n - len(keep)]

        weights = {}
        slot = 1.0 / self.top_n
        for asset in selected:
            weights[asset] = slot
        cash_weight = 1.0 - slot * len(selected)
        if cash_weight > 1e-9 and view.is_tradable(CASH_ASSET):
            weights[CASH_ASSET] = weights.get(CASH_ASSET, 0.0) + cash_weight

        state["holdings"] = dict(weights)
        return Decision(weights, f"rebal: {selected or 'all-cash'}")
