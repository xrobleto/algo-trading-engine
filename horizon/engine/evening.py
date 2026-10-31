"""The evening report: one email per market day, 30 minutes after the close.

The 9:00 AM ET cycle trades; this report tells the day's story once the day is
over, with numbers that line up:

  - closing equity priced at the same official closes as QQQ and SPY
    (cash + shares x close), so "today vs QQQ and SPY" compares like with like;
  - time-weighted performance (today, week, month, since Horizon took over the
    account) and the same comparison in dollars: what the same money would
    have made in QQQ and in SPY, deposits included on their dates;
  - this morning's trades with final fills, and why they happened;
  - a read-only preview of tomorrow's open: the next cycle's decision replayed
    on today's close with a copy of the strategy state (nothing is saved);
  - holdings, signals, and this year's realized gains for tax purposes.

Nothing here places orders or changes engine state other than the report's
own memory (drawdown steps, last regime).
"""

from __future__ import annotations

import copy
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

import pandas as pd

from ..data import cache
from ..data import universe as _universe
from ..paths import state_dir
from ..strategies.base import MarketView
from . import report as R
from .intelligence import compute_regime
from .reconciler import UNMANAGED

log = logging.getLogger("horizon.evening")
ET = ZoneInfo("America/New_York")
DUST_USD = 5.0
TAX_RATES = (0.32, 0.15)


# --- pure helpers (unit-tested) -----------------------------------------------

def closing_marks(positions: Dict[str, dict], closes: Dict[str, float]) -> Dict[str, float]:
    """Each position's value at today's official close; positions without a
    close today (dust outside the universe) keep the broker's mark."""
    out = {}
    for sym, p in positions.items():
        qty = float(p.get("qty", 0.0) or 0.0)
        out[sym] = qty * closes[sym] if sym in closes else float(p.get("market_value", 0.0))
    return out


def same_money(start: float, start_date: str, flows: Dict[str, float], series,
               day: str) -> float:
    """What `start` invested at the `start_date` close, plus each deposit (or
    withdrawal) invested at its date's close, would be worth at `day`'s close
    in `series` (a total-return close series)."""
    lvl = lambda d: float(series.loc[:pd.Timestamp(d)].iloc[-1])
    end = lvl(day)
    value = start * end / lvl(start_date)
    for d, amount in flows.items():
        if start_date < d <= day:
            value += amount * end / lvl(d)
    return value


def data_ready(dataset: Dict[str, pd.DataFrame], day: str, symbols) -> List[str]:
    """Symbols whose bar for `day` has not arrived yet (empty = ready)."""
    ts = pd.Timestamp(day)
    return sorted(s for s in symbols
                  if s not in dataset or dataset[s].empty or dataset[s].index[-1] < ts)


def tax_snapshot(activities: List[dict], marks: Dict[str, float], day: str) -> Optional[dict]:
    """This calendar year's realized gains from the account's full fill history
    (FIFO, share-matched wash sales), dividends, the federal tax those imply on
    their own, and the unrealized gain on what is held."""
    from ..backtest.tax import TaxRates, compute_taxes, year_tax
    trades, divs = [], []
    for a in activities:
        t = a.get("activity_type")
        if t == "FILL":
            trades.append({"date": pd.Timestamp(str(a["transaction_time"])[:10]),
                           "symbol": a["symbol"],
                           "side": "buy" if a["side"] == "buy" else "sell",
                           "shares": float(a["qty"]), "price": float(a["price"])})
        elif t and t.startswith("DIV"):
            divs.append({"date": pd.Timestamp(str(a.get("date"))[:10]),
                         "symbol": a.get("symbol") or "", "amount": float(a.get("net_amount") or 0)})
    if not trades:
        return None
    rates = TaxRates(*TAX_RATES)
    rep = compute_taxes(trades, divs, marks, pd.Timestamp(day), rates)
    y = pd.Timestamp(day).year
    st, lt = rep.st_net_by_year.get(y, 0.0), rep.lt_net_by_year.get(y, 0.0)
    return {"year": y, "st": st, "lt": lt, "dividends": rep.div_by_year.get(y, 0.0),
            "est_tax": year_tax(st, lt, rep.div_tax_by_year.get(y, 0.0), rates),
            "div_tax": rep.div_tax_by_year.get(y, 0.0),
            "unrealized_st": rep.unrealized_st, "unrealized_lt": rep.unrealized_lt,
            "rates": TAX_RATES}


def next_review(sessions: List[str], day: str) -> Optional[str]:
    """ROTATION reviews its picks on the first session of each month (decided
    on that close, traded the next morning)."""
    month = day[:7]
    return next((d for d in sessions if d > day and d[:7] != month), None)


# --- the report ---------------------------------------------------------------

def run_evening_report(broker, strategies, cfg, ledger, kill_switch, alerter,
                       final: bool = False, now: Optional[datetime] = None,
                       send: bool = True) -> dict:
    """Build and send today's evening report. Returns {"ready": False} when the
    day's closing prices have not arrived and `final` is False (the scheduler
    retries); otherwise a dict with the email and the figures behind it."""
    from .main import (_REPORT_STATE_FILE,
                       _applied_rebalance_token, _last_cycle_date, _load_states,
                       apply_funding_guard, apply_gross_ceiling, forced_rebalance_due,
                       load_cycle_summary, plan_orders, target_book, MAX_GROSS_ENV)
    import os

    now = now or datetime.now(ET)
    day = now.date().isoformat()
    warnings: List[str] = []

    acct = broker.get_account()
    positions = broker.get_positions()
    cash = float(acct["cash"])

    dataset = cache.load_dataset()
    held = [s for s, p in positions.items()
            if abs(float(p.get("market_value", 0.0))) >= DUST_USD and s in dataset]
    missing = data_ready(dataset, day, list(R.BENCHMARKS) + held)
    if missing and not final:
        return {"ready": False, "missing": missing}

    closes_today = {s: float(dataset[s]["close"].iloc[-1]) for s in dataset
                    if not missing and dataset[s].index[-1] == pd.Timestamp(day)}
    values = closing_marks(positions, closes_today)
    equity = cash + sum(values.values())
    if missing:
        equity = float(acct["equity"])
        warnings.append("Today's closing prices had not arrived from Polygon by "
                        f"{(now):%I:%M %p} ET, so the comparison with QQQ and SPY is "
                        f"missing and equity uses Alpaca's marks ({', '.join(missing)}).")
    holdings = {s: v for s, v in values.items() if abs(v) >= DUST_USD}

    # --- performance -----------------------------------------------------------
    perf, dollars = None, None
    if not missing:
        try:
            closes = broker.daily_closes(cfg.report_inception, day)
            if day in closes and abs(closes[day] - equity) > 1.0:
                log.warning("Alpaca close %.2f vs computed %.2f for %s", closes[day], equity, day)
            closes[day] = equity
            flows = broker.cash_flows_by_date(cfg.report_inception, day)
            bench = {b: dataset[b]["tr_close"] for b in R.BENCHMARKS}
            perf = R.compute_performance(closes, flows, bench, day, cfg.report_inception)
            start = closes.get(cfg.report_inception)
            if perf is not None and start:
                net_flows = sum(v for k, v in flows.items() if cfg.report_inception < k <= day)
                dollars = {"since": cfg.report_inception,
                           "Horizon": equity - start - net_flows,
                           **{b: same_money(start, cfg.report_inception, flows, bench[b], day)
                              - start - net_flows for b in R.BENCHMARKS}}
        except Exception as exc:
            log.warning("performance unavailable (%s)", exc)
        if perf is None:
            warnings.append("Performance figures could not be computed today (Alpaca's "
                            "account history did not load). Trading was not affected.")

    # --- this morning's cycle ----------------------------------------------------
    summary = load_cycle_summary()
    if summary.get("date") != day:
        summary = {}
        if _last_cycle_date() != day:
            warnings.append("No record of a trading cycle this morning. The engine may "
                            "not have been running at 9:00 AM ET.")
    warnings += list(summary.get("warnings") or [])
    start_utc = datetime(now.year, now.month, now.day, tzinfo=ET).astimezone(
        ZoneInfo("UTC")).strftime("%Y-%m-%dT%H:%M:%SZ")
    try:
        todays = [o for o in broker.orders_since(start_utc)
                  if (o.get("client_order_id") or "").startswith(cfg.order_namespace + "_")]
    except Exception as exc:
        log.warning("order lookup failed (%s)", exc)
        todays = []
    by_coid = {o.get("client_order_id"): o for o in todays}
    planned = {o["coid"]: o for o in summary.get("orders") or [] if o.get("coid")}
    trades = []
    for coid in list(planned) + [c for c in by_coid if c not in planned]:
        o, plan = by_coid.get(coid, {}), planned.get(coid, {})
        filled_qty = float(o.get("filled_qty") or 0)
        filled = filled_qty * float(o.get("filled_avg_price") or 0) if filled_qty else None
        status = str(o.get("status") or "unknown")
        trades.append({"symbol": o.get("symbol") or plan.get("symbol"),
                       "side": o.get("side") or plan.get("side"),
                       "notional": float(plan.get("notional") or filled or 0.0),
                       "filled": filled, "status": status})
        if status != "filled":
            warnings.append(f"The order to {trades[-1]['side']} {trades[-1]['symbol']} "
                            f"did not fully fill (status: {status.replace('_', ' ')}).")
    tripped, reason = kill_switch.is_triggered()
    if tripped:
        warnings.append(f"New buys are blocked by the kill switch ({reason}).")

    # --- tomorrow's open, replayed read-only on today's close ------------------
    sessions: List[str] = []
    try:
        sessions = [c["date"] for c in broker.calendar(
            day, (now + timedelta(days=45)).date().isoformat())]
    except Exception as exc:
        log.warning("calendar unavailable (%s)", exc)
    next_session = next((d for d in sessions if d > day), None)
    outlook, signals = None, None
    if not missing:
        try:
            view = MarketView(dataset, pd.Timestamp(day))
            regime = compute_regime(view)
            states = copy.deepcopy(_load_states(strategies))
            cash_legs = {_universe.PULSE_CASH_ASSET, _universe.ROTATION_CASH_ASSET}
            prev_rot = sorted(s for s in states.get("ROTATION", {}).get("holdings", {})
                              if s not in cash_legs)
            sizing = equity
            cap = float(os.getenv("HORIZON_CAPITAL_CAP", "0") or 0)
            if cap > 0:
                sizing = min(sizing, cap)
            target, decisions = target_book(view, regime, strategies, states, cfg, ledger, sizing)
            target, _ = apply_gross_ceiling(target, float(os.getenv(MAX_GROSS_ENV, "0") or 0))
            orphans = {e.symbol for e in ledger.active_entries(UNMANAGED)}
            pos_close = {s: {"market_value": v} for s, v in values.items()}
            target, _, _ = apply_funding_guard(target, pos_close, orphans, cash,
                                               float(acct.get("multiplier", 1.0) or 1.0))
            token = getattr(cfg, "rebalance_now_token", "")
            forced = forced_rebalance_due(token, _applied_rebalance_token(), tripped)
            orders, hit, gap = plan_orders(
                target, values, {}, closes_today, orphans, sizing,
                band=0.0 if forced else cfg.rebalance_band,
                min_trade_frac=cfg.rebalance_min_trade, block_buys=tripped,
                signal_override=cfg.rebalance_signal_override)
            rot = decisions.get("ROTATION")
            rot_now = sorted(s for s in (rot.target_weights if rot else {}) if s not in cash_legs)
            if forced and orders:
                why = getattr(cfg, "rebalance_now_note", "") or "One-time rebalance to target."
            elif orders and rot and rot.note.startswith("rebal") and set(rot_now) != set(prev_rot):
                gone = [s for s in prev_rot if s not in rot_now]
                new = [s for s in rot_now if s not in prev_rot]
                why = (f"ROTATION's monthly review swaps {' + '.join(gone) or 'T-bills'} "
                       f"for {' + '.join(new) or 'T-bills'}.")
            elif orders and gap > cfg.rebalance_band:
                why = (f"Holdings are {gap:.0%} from target, past the "
                       f"{cfg.rebalance_band:.0%} rebalance band.")
            elif orders:
                why = "A position would be fully entered or exited."
            else:
                why = None
            outlook = {"session": next_session, "gap": gap, "band": cfg.rebalance_band,
                       "reason": why,
                       "orders": [{"symbol": o["symbol"], "side": o["side"],
                                   "notional": o["notional"]} for o in orders]}
            pulse = decisions.get("PULSE")
            pulse_lev = sum(w * (2.0 if s == _universe.PULSE_LEVERED_ASSET else 1.0)
                            for s, w in (pulse.target_weights if pulse else {}).items()
                            if s not in cash_legs)
            signals = {"regime": regime.regime, "score": regime.score,
                       "pulse_lev": pulse_lev, "rotation": rot_now,
                       "next_review": next_review(sessions, day)}
        except Exception as exc:
            log.warning("outlook unavailable (%s)", exc)

    # --- taxes -------------------------------------------------------------------
    tax = None
    try:
        marks = {s: (closes_today.get(s) or float(p.get("current_price") or 0))
                 for s, p in positions.items()}
        tax = tax_snapshot(broker.all_activities(), marks, day)
    except Exception as exc:
        log.warning("tax snapshot unavailable (%s)", exc)

    # --- drawdown steps and regime memory ----------------------------------------
    state_path = state_dir() / _REPORT_STATE_FILE
    rstate = R.load_state(state_path)
    prev_regime = rstate.get("last_regime")
    crossed = None
    if perf is not None:
        rstate, crossed = R.drawdown_steps(rstate, perf)
    if signals:
        rstate["last_regime"] = signals["regime"]
    rstate["last_report_date"] = day

    cycle_note = None
    if not trades:
        if summary.get("status") == "held":
            cycle_note = (f"No trades this morning. Holdings were {summary.get('weight_gap', 0):.1%} "
                          f"from target; the engine rebalances at {cfg.rebalance_band:.0%}.")
        elif summary.get("status") in ("refused-stale", "skipped"):
            cycle_note = "No trades: this morning's cycle did not trade (see warnings)."
        else:
            cycle_note = "No trades this morning."

    subject, text, html = R.build_evening_report(
        day=day, perf=perf, equity=equity, dollars=dollars, trades=trades,
        trade_reason=summary.get("trade_reason"), cycle_note=cycle_note, outlook=outlook,
        holdings=holdings, cash=cash, signals=signals, prev_regime=prev_regime, tax=tax,
        warnings=warnings)
    if send:
        R.save_state(state_path, rstate)
        alerter.heartbeat(subject, text, html)
        if crossed:
            alerter.warning(*R.build_drawdown_alert(crossed, perf))
    log.info("evening report %s: equity $%.2f, %d trades, %d warnings", day, equity,
             len(trades), len(warnings))
    return {"ready": True, "subject": subject, "text": text, "html": html, "equity": equity,
            "perf": perf, "dollars": dollars, "outlook": outlook, "tax": tax,
            "crossed": crossed, "warnings": warnings}
