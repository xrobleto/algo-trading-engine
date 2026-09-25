"""Account-level simulator — replays the ACCOUNT the way the live engine runs it.

The per-strategy harness (harness.py) simulates each sleeve on its own
notional and portfolio.py blends their returns, which implicitly rebalances
between sleeves for free every day. The live engine does something different:
one account, one combined target book, real orders. This simulator mirrors
engine/main.py: run_cycle step by step and calls the SAME functions:

  decide on close(T) with the production strategy objects and their states
  -> regime tilt + sleeve budgets (engine/sleeves.py)
  -> funding guard (engine/main.py: apply_funding_guard, cash account)
  -> order planning incl. the no-trade band (engine/main.py: plan_orders)
  -> at open(T+1): sells first, then buys sized to cash
     (engine/main.py: size_buys_to_cash), harness cost model on every fill
  -> dividends credited on the ex-date, mark to market at the close.

Every fill and dividend is recorded so backtest/tax.py can compute taxes.
Idle cash earns nothing, as in the harness and the live account.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

import pandas as pd

from ..config import EngineConfig
from ..data import calendar
from ..engine.intelligence import compute_regime
from ..engine.ledger import OwnershipLedger
from ..engine.main import apply_funding_guard, plan_orders, size_buys_to_cash
from ..engine.sleeves import SleeveManager
from ..strategies.base import MarketView, Strategy
from .costs import buy_fill, sell_fill


@dataclass
class AccountResult:
    equity: pd.Series
    trades: List[dict] = field(default_factory=list)
    dividends: List[dict] = field(default_factory=list)
    final_marks: Dict[str, float] = field(default_factory=dict)
    rebalance_days: int = 0
    decision_days: int = 0

    def turnover(self) -> float:
        yrs = max(len(self.equity) / 252.0, 1e-9)
        traded = sum(t["shares"] * t["price"] for t in self.trades)
        return traded / (float(self.equity.mean()) * yrs)


def _px(ds, sym, d, field_name) -> Optional[float]:
    df = ds.get(sym)
    if df is None or d not in df.index:
        return None
    v = df.at[d, field_name]
    return float(v) if pd.notna(v) else None


def simulate(dataset: Dict[str, pd.DataFrame], strategies: Dict[str, Strategy],
             cfg: EngineConfig, start: str, end: str,
             starting_equity: float = 100_000.0, band: float = 0.0,
             min_trade_frac: float = 0.0, states: Optional[Dict[str, dict]] = None,
             signal_override: float = 0.0, observer=None) -> AccountResult:
    """observer(date, target, holdings_mv, equity, band_triggered), if given, is
    called after every decision — for diagnostics only; it cannot alter trading."""
    sids = [s for s, c in cfg.sleeves.items() if c.enabled]
    days = calendar.window(calendar.trading_days(dataset), start, end)
    cash = float(starting_equity)
    shares: Dict[str, float] = {}
    st = {s: (states or {}).get(s, strategies[s].initial_state()) for s in sids}
    res = AccountResult(pd.Series(dtype=float))
    curve: Dict[pd.Timestamp, float] = {}
    pending_orders: List[dict] = []
    sm = SleeveManager(cfg)

    for T in days:
        # 1. Execute yesterday's plan at today's open: sells, then buys to cash.
        if pending_orders:
            for o in [o for o in pending_orders if o["side"] == "sell"]:
                px = _px(dataset, o["symbol"], T, "open")
                held = shares.get(o["symbol"], 0.0)
                q = min(o["qty"], held)
                if px is None or q <= 0:
                    continue
                f = sell_fill(px, o["symbol"], cfg.cost)
                cash += q * f
                shares[o["symbol"]] = held - q
                res.trades.append({"date": T, "symbol": o["symbol"], "side": "sell",
                                   "shares": q, "price": f})
            buys = size_buys_to_cash([o for o in pending_orders if o["side"] == "buy"], cash)
            for o in buys:
                px = _px(dataset, o["symbol"], T, "open")
                if px is None or o["qty"] <= 0:
                    continue
                f = buy_fill(px, o["symbol"], cfg.cost)
                q = min(o["qty"], max(cash, 0.0) / f)      # never borrow
                if q <= 1e-9:
                    continue
                cash -= q * f
                shares[o["symbol"]] = shares.get(o["symbol"], 0.0) + q
                res.trades.append({"date": T, "symbol": o["symbol"], "side": "buy",
                                   "shares": q, "price": f})
            shares = {s: q for s, q in shares.items() if q > 1e-9}
            pending_orders = []
            res.rebalance_days += 1

        # 2. Dividends with ex-date T.
        for sym, q in shares.items():
            dv = _px(dataset, sym, T, "dividend")
            if dv:
                cash += q * dv
                res.dividends.append({"date": T, "symbol": sym, "amount": q * dv})

        # 3. Mark to market at the close.
        mv = {}
        for sym, q in shares.items():
            px = _px(dataset, sym, T, "close")
            if px is None:
                px = float(dataset[sym]["close"].loc[:T].iloc[-1])
            mv[sym] = q * px
        equity = cash + sum(mv.values())
        curve[T] = equity

        # 4. Decide for tomorrow exactly as run_cycle does at 09:00.
        view = MarketView(dataset, T)
        regime = compute_regime(view)
        budgets = sm.budgets(equity, OwnershipLedger(), regime)
        target: Dict[str, float] = {}
        for sid in sids:
            dec = strategies[sid].decide(view, st[sid])
            for sym, w in dec.target_weights.items():
                target[sym] = target.get(sym, 0.0) + w * budgets[sid].sleeve_equity
        positions = {s: {"market_value": v} for s, v in mv.items()}
        target, _scale, _fund = apply_funding_guard(target, positions, set(), cash, 1.0)
        prices = {s: view.close(s) for s in set(target) | set(mv) if view.is_tradable(s)}
        pending_orders, _hit, _gap = plan_orders(target, mv, {}, prices, set(), equity,
                                                 band=band, min_trade_frac=min_trade_frac,
                                                 signal_override=signal_override)
        if observer is not None:
            observer(T, dict(target), dict(mv), equity, _hit)
        res.decision_days += 1

    res.equity = pd.Series(curve).sort_index()
    last = res.equity.index[-1]
    res.final_marks = {s: float(dataset[s]["close"].loc[:last].iloc[-1]) for s in shares}
    return res
