"""Portfolio composition.

Combines the validated sleeve return streams into the engine result:
  - regime-driven allocation tilts (monthly, bounded);
  - a book-level leverage dial — the legitimate, textbook way to express the
    user's stated high risk tolerance: lever a *diversified, higher-Sharpe*
    blend rather than a single asset (risk-parity-then-leverage);
  - the rolling-drawdown risk overlay (no death-spiral), which de-levers the
    whole book in a drawdown and recovers;
  - the high-water-mark withdrawal skim.

Sleeves are an accounting overlay over one account — the model the live engine
uses. Each sleeve's standalone return stream already includes its own internal
leverage and costs; `book_leverage` is applied on top of the blend, with daily
borrow cost on the leveraged portion.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List

import pandas as pd

from ..config import EngineConfig
from ..data import calendar
from ..engine.intelligence import compute_regime
from ..engine.withdrawal import WithdrawalEngine
from ..strategies.base import MarketView
from .harness import BacktestResult

# Risk-overlay parameters (mirror engine/risk.py).
_DD_SOFT, _DD_HARD, _EXP_FLOOR, _ROLL = 0.12, 0.32, 0.50, 252


@dataclass
class PortfolioResult:
    equity: pd.Series
    book_leverage: float = 1.0
    withdrawn_total: float = 0.0
    n_withdrawals: int = 0
    regime_months: Dict[str, int] = field(default_factory=dict)
    withdrawals_applied: bool = False


def _exposure(book: List[float]) -> float:
    """Drawdown-overlay exposure from the rolling-high of recent book equity."""
    if not book:
        return 1.0
    recent = book[-_ROLL:]
    high = max(recent)
    dd = book[-1] / high - 1.0 if high > 0 else 0.0
    derisk = min(1.0, max(0.0, (-dd - _DD_SOFT) / max(_DD_HARD - _DD_SOFT, 1e-9)))
    return 1.0 - (1.0 - _EXP_FLOOR) * derisk


def _r(series: pd.Series, day) -> float:
    val = series.get(day, 0.0)
    return 0.0 if val != val else float(val)   # NaN-safe


def run_portfolio(results: Dict[str, BacktestResult],
                  base_allocations: Dict[str, float],
                  dataset: Dict[str, pd.DataFrame], start: str, end: str,
                  cfg: EngineConfig, book_leverage: float = 1.0,
                  use_regime: bool = True, use_risk_overlay: bool = True,
                  withdrawals: bool = False) -> PortfolioResult:
    sids = list(base_allocations.keys())
    returns = {s: results[s].equity.pct_change() for s in sids}
    days = calendar.window(calendar.trading_days(dataset), start, end)

    equity = cfg.starting_equity
    book: List[float] = []
    curve: Dict[pd.Timestamp, float] = {}
    wd = WithdrawalEngine(cfg.withdrawal)
    regime_months: Dict[str, int] = {}
    exposure = 1.0
    cur_month = None
    weights = dict(base_allocations)
    daily_borrow = cfg.cost.margin_rate_annual / 252.0

    for d in days:
        # Monthly: regime reallocation + withdrawal skim.
        month = (d.year, d.month)
        if month != cur_month:
            cur_month = month
            if use_regime:
                reg = compute_regime(MarketView(dataset, d))
                regime_months[reg.regime] = regime_months.get(reg.regime, 0) + 1
                raw = {s: base_allocations[s] * reg.alloc_mult(s) for s in sids}
            else:
                raw = dict(base_allocations)
            scale = sum(raw.values())
            weights = {s: raw[s] / scale for s in sids}
            if withdrawals:
                amount = wd.maybe_withdraw(d, equity)
                equity -= amount

        # Daily blended return, then book leverage + risk overlay + borrow.
        book_return = sum(weights[s] * _r(returns[s], d) for s in sids)
        eff_leverage = book_leverage * exposure
        net_return = (eff_leverage * book_return
                      - max(eff_leverage - 1.0, 0.0) * daily_borrow)
        equity *= (1.0 + net_return)
        book.append(equity)
        curve[d] = equity
        exposure = _exposure(book) if use_risk_overlay else 1.0

    return PortfolioResult(pd.Series(curve).sort_index(),
                           book_leverage=book_leverage,
                           withdrawn_total=wd.withdrawn_total,
                           n_withdrawals=len(wd.events),
                           regime_months=regime_months,
                           withdrawals_applied=withdrawals)
