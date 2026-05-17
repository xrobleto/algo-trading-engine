"""The faithful backtest harness.

Single source of truth: this harness imports and calls the production Strategy
objects' real `decide()` — it never reimplements strategy logic. A strategy
sees only a MarketView sliced at the simulated day, so look-ahead is
structurally impossible. Signals are computed on close(T); orders fill at
open(T+1) with modeled slippage; leveraged dollars accrue daily borrow cost.

`run_strategy` simulates ONE strategy standalone on its own notional — that is
what the per-strategy gating bar (A1-A7) is measured on. Portfolio composition
lives in portfolio.py.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

import pandas as pd

from ..config import CostModel
from ..data import calendar
from ..strategies.base import OVERNIGHT, MarketView, Strategy
from .costs import buy_fill, daily_borrow_rate, overnight_slippage, sell_fill


@dataclass
class Trade:
    date: pd.Timestamp
    symbol: str
    side: str            # buy | sell | overnight
    shares: float
    price: float         # fill price including slippage
    notional: float


@dataclass
class BacktestResult:
    strategy_id: str
    equity: pd.Series                       # daily equity, indexed by date
    trades: List[Trade] = field(default_factory=list)
    note: str = ""

    @property
    def daily_returns(self) -> pd.Series:
        return self.equity.pct_change().dropna()

    def annual_turnover(self) -> float:
        """Sum of traded notional / (avg equity * years) — a churn measure."""
        if self.equity.empty or not self.trades:
            return 0.0
        years = max(len(self.equity) / 252.0, 1e-9)
        traded = sum(t.notional for t in self.trades)
        avg_equity = float(self.equity.mean())
        return traded / (avg_equity * years) if avg_equity > 0 else 0.0


class _Book:
    """Mutable account state during a simulation."""

    __slots__ = ("cash", "positions")

    def __init__(self, cash: float):
        self.cash: float = cash
        self.positions: Dict[str, float] = {}   # symbol -> shares


def _price(dataset: Dict[str, pd.DataFrame], symbol: str,
           date: pd.Timestamp, field_name: str) -> Optional[float]:
    df = dataset.get(symbol)
    if df is None or date not in df.index:
        return None
    val = df.at[date, field_name]
    return float(val) if pd.notna(val) else None


def run_strategy(strategy: Strategy, dataset: Dict[str, pd.DataFrame],
                 start: str, end: str, starting_equity: float = 100_000.0,
                 cost: Optional[CostModel] = None,
                 rebalance_band: float = 0.05) -> BacktestResult:
    """Simulate one strategy standalone on its own notional.

    `rebalance_band`: the harness rebalances only when a strategy's target
    weights move by more than this versus the last executed targets. A strategy
    that re-emits the same book is held, not re-pinned daily — matching how a
    real account behaves and avoiding phantom churn.
    """
    cost = cost or CostModel()
    days = calendar.window(calendar.trading_days(dataset), start, end)
    if len(days) < 5:
        raise ValueError("backtest window too short")
    if strategy.execution == OVERNIGHT:
        return _run_overnight(strategy, dataset, days, starting_equity, cost)
    return _run_next_open(strategy, dataset, days, starting_equity, cost,
                          rebalance_band)


def _targets_changed(new: Dict[str, float], old: Optional[Dict[str, float]],
                      band: float) -> bool:
    """True if the desired book moved enough to warrant a rebalance."""
    if old is None:
        return bool(new)
    for sym in set(new) | set(old):
        if abs(new.get(sym, 0.0) - old.get(sym, 0.0)) > band:
            return True
    return False


def _execute_next_open(decision, date, dataset, book: _Book,
                       trades: List[Trade], cost: CostModel) -> None:
    """Rebalance the book toward `decision.target_weights` at `date`'s open."""
    # Equity available at the open, used to size the target weights.
    equity = book.cash
    for sym, sh in book.positions.items():
        px = _price(dataset, sym, date, "open") or _price(dataset, sym, date, "close")
        if px is not None:
            equity += sh * px
    if equity <= 0:
        return  # account ruined — stop trading

    targets = decision.target_weights
    for sym in set(book.positions) | set(targets):
        px = _price(dataset, sym, date, "open")
        if px is None or px <= 0:
            continue  # symbol not trading today — cannot rebalance it
        target_shares = (targets.get(sym, 0.0) * equity) / px
        delta = target_shares - book.positions.get(sym, 0.0)
        if abs(delta * px) < 1.0:        # $1 minimum order
            continue
        if delta > 0:
            fill = buy_fill(px, sym, cost)
            book.cash -= delta * fill + cost.commission_per_trade
        else:
            fill = sell_fill(px, sym, cost)
            book.cash -= delta * fill     # delta < 0 -> cash increases
            book.cash -= cost.commission_per_trade
        book.positions[sym] = book.positions.get(sym, 0.0) + delta
        trades.append(Trade(date, sym, "buy" if delta > 0 else "sell",
                            abs(delta), fill, abs(delta * fill)))
    book.positions = {s: sh for s, sh in book.positions.items()
                      if abs(sh) > 1e-9}


def _run_next_open(strategy, dataset, days, starting_equity, cost,
                   rebalance_band) -> BacktestResult:
    book = _Book(starting_equity)
    state = strategy.initial_state()
    trades: List[Trade] = []
    curve: Dict[pd.Timestamp, float] = {}
    pending = None
    last_targets: Optional[Dict[str, float]] = None
    borrow = daily_borrow_rate(cost)

    for T in days:
        # 1. Execute yesterday's decision at today's open — but only if the
        #    desired book actually changed. An unchanged book is held, not
        #    re-pinned (no phantom churn).
        if (pending is not None
                and _targets_changed(pending.target_weights, last_targets,
                                     rebalance_band)):
            _execute_next_open(pending, T, dataset, book, trades, cost)
            last_targets = dict(pending.target_weights)
        # 2. Margin interest on any borrowed (negative) cash, for day T.
        if book.cash < 0:
            book.cash += book.cash * borrow
        # 3. Cash dividends with ex-date T.
        for sym, sh in book.positions.items():
            div = _price(dataset, sym, T, "dividend")
            if div:
                book.cash += sh * div
        # 4. Mark to market at the close of T.
        equity = book.cash
        for sym, sh in book.positions.items():
            px = _price(dataset, sym, T, "close")
            if px is not None:
                equity += sh * px
        curve[T] = equity
        # 5. Decide for tomorrow.
        pending = strategy.decide(MarketView(dataset, T), state).clean()

    return BacktestResult(strategy.strategy_id,
                          pd.Series(curve).sort_index(), trades)


def _run_overnight(strategy, dataset, days, starting_equity, cost) -> BacktestResult:
    """DRIFT-style: capture the close(T) -> open(T+1) return, flat intraday."""
    equity = starting_equity
    state = strategy.initial_state()
    trades: List[Trade] = []
    curve: Dict[pd.Timestamp, float] = {}
    slip = overnight_slippage(cost)

    for i, T in enumerate(days):
        curve[T] = equity
        decision = strategy.decide(MarketView(dataset, T), state).clean()
        weight = sum(decision.target_weights.values())
        if weight > 0 and i + 1 < len(days):
            sym = next(iter(decision.target_weights))
            close_t = _price(dataset, sym, T, "close")
            open_t1 = _price(dataset, sym, days[i + 1], "open")
            if close_t and open_t1 and close_t > 0:
                gross = open_t1 / close_t - 1.0
                net = weight * (gross - 2.0 * slip)   # slippage on both legs
                equity *= (1.0 + net)
                trades.append(Trade(T, sym, "overnight", weight, close_t,
                                    weight * equity))
    if days:
        curve[days[-1]] = equity

    return BacktestResult(strategy.strategy_id,
                          pd.Series(curve).sort_index(), trades)


def benchmark_curve(dataset: Dict[str, pd.DataFrame], symbol: str,
                    start: str, end: str,
                    starting_equity: float = 100_000.0) -> pd.Series:
    """Buy-and-hold total-return curve for the benchmark over the window."""
    days = calendar.window(calendar.trading_days(dataset), start, end)
    df = dataset[symbol]
    tr = df.loc[df.index.isin(days), "tr_close"]
    return (tr / tr.iloc[0] * starting_equity).rename(symbol)
