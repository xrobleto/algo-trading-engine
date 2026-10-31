"""Horizon engine — live orchestration loop.

Single source of truth: the SAME Strategy objects the backtest validated are
imported here, and their identical `decide()` drives live orders.

The engine defaults to **dry-run** — it logs the orders it would place but
submits nothing. Live trading is the user's explicit decision. Run:

    python -m horizon.engine.main --once             # one dry-run cycle
    python -m horizon.engine.main --daily             # dry-run, once/weekday 09:00 ET
    python -m horizon.engine.main --daily --live      # live (needs env confirm)
    python -m horizon.engine.main --interval 900      # dry-run loop (testing)
    python -m horizon.engine.main --flatten           # emergency: cancel + liquidate
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import time
import traceback
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional
from zoneinfo import ZoneInfo

import pandas as pd

from ..config import build_default_config
from ..data import cache, calendar
from ..data import universe as _universe
from ..paths import log_dir, state_dir
from ..strategies.base import MarketView
from ..strategies.registry import build_all
from . import report as R
from .alerts import Alerter
from .intelligence import compute_regime
from .killswitch import KillSwitch
from .ledger import OwnershipLedger
from .reconciler import UNMANAGED, reconcile
from .sleeves import SleeveManager

log = logging.getLogger("horizon.engine")

# Only the sleeves that cleared the validation gating bar trade live.
# DERIVED from the config's `enabled` flags so the trading set and the capital
# weighting can never drift apart. They did drift once (2026-08-24): REVERT and
# DRIFT stayed `enabled` after being rejected, so budgets() normalized 30% of the
# book onto sleeves that never trade, quietly diluting book_leverage 1.5 to ~1.03x
# effective. Deriving it makes config the single source of truth for BOTH.
ADMITTED_SLEEVES = [sid for sid, sc in build_default_config().sleeves.items()
                    if sc.enabled]

MIN_ORDER_USD = 1.0
DAILY_RUN_HOUR_ET = 9      # run once per weekday at 09:00 ET, before the open
# G0 — data freshness. A cycle may only trade on the last completed session.
# 4 calendar days tolerates a Friday bar on the Tuesday after a Monday holiday;
# anything older is refused with a CRITICAL alert (2026-08-24..09-04 incident:
# the container decided on a frozen Aug-24 cache for two weeks).
MAX_STALE_DAYS = 4
# Funding guard — never submit buys the account cannot fund. Targets are scaled
# to (cash + managed positions) x account multiplier, less a buffer for
# overnight gaps between the 09:00 decision and the 09:30 fill.
FUNDING_BUFFER = 0.03
# Optional hard dollar ceiling on total long targets (env HORIZON_MAX_GROSS).
# Used to hold the book at the capital-checkpoint boundary the user approved
# (CP2: cap $3,850 x book_leverage 1.5 = $5,775) while the leverage path
# (margin / levered ETF / lift the ceiling) is decided. 0 = no ceiling.
MAX_GROSS_ENV = "HORIZON_MAX_GROSS"
_STATE_FILE = "engine_state.json"
_LEDGER_FILE = "ledger.json"
_HALT_FILE = "HALT_ALL_TRADING"
_REBALANCE_TOKEN_FILE = "rebalance_token.txt"   # last one-time rebalance applied
_REPORT_STATE_FILE = "report_state.json"  # drawdown-step and regime memory for the report
_CYCLE_SUMMARY_FILE = "cycle_summary.json"  # what the morning cycle did, for the evening report
_LAST_CYCLE_FILE = "last_cycle.txt"   # records the ET date of the most recent cycle
_LAST_REPORT_FILE = "last_report.txt"  # records the ET date of the most recent evening report
REPORT_DELAY_MIN = 30      # evening report: this long after the close (data settles)
REPORT_RETRY_MIN = 10      # retry interval while the day's closing prices are missing
REPORT_GIVE_UP_MIN = 150   # after this long past the close, send with what is available


def _load_states(strategies) -> Dict[str, dict]:
    path = state_dir() / _STATE_FILE
    saved = {}
    if path.exists():
        try:
            saved = json.loads(path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            saved = {}
    return {sid: saved.get(sid, strategies[sid].initial_state())
            for sid in ADMITTED_SLEEVES}


def _save_states(states: Dict[str, dict]) -> None:
    (state_dir() / _STATE_FILE).write_text(
        json.dumps(states, indent=2, default=str), encoding="utf-8")


def _last_cycle_date() -> str:
    """ET-date of the most recent cycle attempt; '' if none. Drives the --daily
    same-day catch-up."""
    path = state_dir() / _LAST_CYCLE_FILE
    if not path.exists():
        return ""
    try:
        return path.read_text(encoding="utf-8").strip()
    except OSError:
        return ""


def _record_cycle_date() -> None:
    now_et = datetime.now(ZoneInfo("America/New_York"))
    (state_dir() / _LAST_CYCLE_FILE).write_text(
        now_et.date().isoformat(), encoding="utf-8")


def _pending_notional(broker, view) -> Dict[str, float]:
    """Signed pending notional per symbol from open orders (buys +, sells -).

    Prevents the engine from double-submitting when the previous cycle's
    orders have not yet filled — for example, an after-hours batch queued
    for the next open. The cycle's order-diff uses
    `effective = filled + pending` instead of `effective = filled`.

    Degrades gracefully: returns {} on no broker or broker error.
    """
    pending: Dict[str, float] = {}
    if broker is None:
        return pending
    try:
        open_orders = broker.get_open_orders()
    except Exception as exc:
        log.warning("open-orders fetch failed (%s) — pending-aware diff disabled",
                    exc)
        return pending
    for order in open_orders:
        sym = order.get("symbol")
        if not sym:
            continue
        qty_remaining = (float(order.get("qty", 0.0))
                         - float(order.get("filled_qty", 0.0)))
        if qty_remaining <= 0 or not view.is_tradable(sym):
            continue
        price = view.close(sym)
        if not price or price <= 0:
            continue
        signed = qty_remaining * price
        if "sell" in str(order.get("side", "")).lower():
            signed = -signed
        pending[sym] = pending.get(sym, 0.0) + signed
    return pending


def data_staleness_days(as_of, now=None) -> int:
    """Calendar days between the dataset's last bar and the last completed
    session. 0 = current. > MAX_STALE_DAYS = refuse to trade (G0)."""
    return int((cache.completed_through(now) - pd.Timestamp(as_of)).days)


def apply_funding_guard(account_target: Dict[str, float], positions: Dict[str, dict],
                        orphan_symbols: set, cash: float, multiplier: float,
                        buffer: float = FUNDING_BUFFER):
    """Scale long targets down to what the account can actually fund.

    fundable = (cash + market value of the positions this engine manages)
               x account multiplier x (1 - buffer)
    Returns (targets, scale, fundable). scale == 1.0 means untouched. A
    multiplier of 1 (no margin) makes this the hard ceiling that PULSE's
    vol-targeted leverage and book_leverage would otherwise blow through —
    without it Alpaca rejects the buys for insufficient buying power.
    """
    managed = (set(account_target) | set(positions)) - set(orphan_symbols)
    managed_mv = sum(float(positions.get(s, {}).get("market_value", 0.0))
                     for s in managed)
    fundable = (float(cash) + managed_mv) * max(float(multiplier), 1.0) * (1.0 - buffer)
    want = sum(v for v in account_target.values() if v > 0)
    if want <= 0 or fundable <= 0 or want <= fundable:
        return dict(account_target), 1.0, fundable
    scale = fundable / want
    return {s: v * scale for s, v in account_target.items()}, scale, fundable


ORPHAN_ALERT_MIN_USD = 5.0      # dust below this is logged, never emailed
FUNDING_ALERT_MARGIN = 0.02     # alert only when the clamp exceeds the buffer by this


def plan_orders(account_target: Dict[str, float], holdings_mv: Dict[str, float],
                pending: Dict[str, float], prices: Dict[str, float],
                orphan_symbols: set, equity: float, band: float = 0.0,
                min_trade_frac: float = 0.0, block_buys: bool = False,
                signal_override: float = 0.0):
    """Turn dollar targets into orders — the ONE function both the live cycle
    and the account simulator (backtest/account_sim.py) call.

    band: account-level no-trade band. If every managed symbol's target weight
      (target / equity) is within `band` of its current weight (filled +
      pending, / equity), no orders are placed. band=0 reproduces the pre-
      2026-09-25 behavior: re-pin any drift of MIN_ORDER_USD or more.
    min_trade_frac: once the band triggers, symbols whose delta is below this
      fraction of equity are left alone (no dust trades). 0 -> MIN_ORDER_USD.
    signal_override: a full entry (held ~0, target > 0) or full exit (target
      0, held > 0) whose delta is at least this fraction of equity triggers the
      band regardless of the largest gap — a strategy SIGNAL change (e.g. a
      ROTATION slot swap, ~17% of equity) must not wait for drift to exceed
      the band (tax study addendum A). 0 disables.

    Returns (orders, band_triggered, max_weight_gap).
    """
    managed = (set(account_target) | set(holdings_mv) | set(pending)) - set(orphan_symbols)
    eff = {s: holdings_mv.get(s, 0.0) + pending.get(s, 0.0) for s in managed}
    gap = 0.0
    if equity > 0:
        gap = max((abs(account_target.get(s, 0.0) - eff[s]) / equity for s in managed),
                  default=0.0)
    if band > 0 and gap <= band:
        signal = False
        if signal_override > 0 and equity > 0:
            for s in managed:
                tgt, held = account_target.get(s, 0.0), eff[s]
                crossing = (tgt <= 0.0 < held) or (held <= 0.001 * equity and tgt > 0.0)
                if crossing and abs(tgt - held) / equity >= signal_override:
                    signal = True
                    break
        if not signal:
            return [], False, gap
    min_usd = max(MIN_ORDER_USD, min_trade_frac * equity if equity > 0 else 0.0)
    orders = []
    for sym in sorted(managed):
        delta = account_target.get(sym, 0.0) - eff[sym]
        full_exit = account_target.get(sym, 0.0) <= 0.0 and eff[sym] > 0.0
        # A full exit trades down to MIN_ORDER_USD so no sliver is stranded;
        # every other adjustment must clear the min-trade size.
        if abs(delta) < (MIN_ORDER_USD if full_exit else min_usd):
            continue
        side = "buy" if delta > 0 else "sell"
        if side == "buy" and block_buys:
            continue  # kill switch blocks new exposure, never exits
        price = prices.get(sym)
        if not price or price <= 0:
            continue
        orders.append({"symbol": sym, "side": side,
                       "qty": round(abs(delta) / price, 4), "notional": abs(delta)})
    return orders, True, gap


def apply_gross_ceiling(account_target: Dict[str, float], ceiling: float):
    """Scale long targets so their sum does not exceed `ceiling` dollars."""
    want = sum(v for v in account_target.values() if v > 0)
    if ceiling <= 0 or want <= ceiling:
        return dict(account_target), 1.0
    scale = ceiling / want
    return {s: v * scale for s, v in account_target.items()}, scale


CASH_BUFFER = 0.01          # keep 1% of cash back on buys (fill-price drift)
SELL_FILL_WAIT_SEC = 30 * 60   # how long to wait for queued sells to fill
SELL_POLL_SEC = 20


def size_buys_to_cash(buys, cash: float, buffer: float = CASH_BUFFER):
    """Scale a list of buy orders proportionally so their total notional fits
    within `cash` x (1 - buffer). Returns new order dicts (qty and notional
    scaled); untouched if they already fit."""
    usable = max(0.0, float(cash)) * (1.0 - buffer)
    need = sum(o["notional"] for o in buys)
    if need <= usable or need <= 0:
        return [dict(o) for o in buys]
    scale = usable / need
    out = []
    for o in buys:
        out.append({**o, "qty": round(o["qty"] * scale, 4),
                    "notional": o["notional"] * scale})
    return out


def _wait_for_cash(broker, need: float, sells, log, max_wait: int = SELL_FILL_WAIT_SEC,
                   poll: int = SELL_POLL_SEC):
    """Return (cash, timed_out): the account's cash once it covers `need`, or
    once the queued sells have all left the open-order book, or when max_wait
    elapses (timed_out=True). Buys are sized to whatever cash is available."""
    sell_syms = {o["symbol"] for o in sells}
    # A 09:00 cycle's sells fill at the 09:30 open. A fixed 30-minute wait left
    # ~10 seconds of margin (sells filled 13:30:06 UTC against a 13:30:17
    # deadline on 2026-09-08): a slow open would have sized the buys to
    # pre-sale cash and left the book under-invested for a day. Anchor the
    # deadline to the actual open + 5 minutes when the open is near.
    try:
        until_open = float(broker.seconds_until_open())
        if 0 < until_open <= 3600:
            max_wait = max(max_wait, int(until_open) + 300)
    except Exception as exc:
        log.warning("clock unavailable (%s) — fixed %ds sell wait", exc, max_wait)
    deadline = time.time() + max_wait
    cash = 0.0
    while True:
        try:
            cash = float(broker.get_account()["cash"])
            if cash * (1.0 - CASH_BUFFER) >= need:
                return cash, False
            if sell_syms:
                open_syms = {o.get("symbol") for o in broker.get_open_orders()}
                if not (sell_syms & open_syms):
                    return cash, False   # sells done; this is all the cash there is
            else:
                return cash, False       # nothing pending that could add cash
        except Exception as exc:
            log.warning("cash poll failed (%s)", exc)
        if time.time() >= deadline:
            log.warning("waited %ds for sells to fill; sizing buys to cash $%.0f",
                        max_wait, cash)
            return cash, True
        log.info("waiting for sells to fill: cash $%.0f < need $%.0f", cash, need)
        time.sleep(poll)


def forced_rebalance_due(token: str, applied: str, kill_switch_tripped: bool) -> bool:
    """A one-time rebalance runs when the configured token is new. Never under
    the kill switch: it blocks buys, so a forced rebalance would only sell."""
    return bool(token) and token != applied and not kill_switch_tripped


def _applied_rebalance_token() -> str:
    path = state_dir() / _REBALANCE_TOKEN_FILE
    try:
        return path.read_text(encoding="utf-8").strip() if path.exists() else ""
    except OSError:
        return ""


def _record_rebalance_token(token: str) -> None:
    (state_dir() / _REBALANCE_TOKEN_FILE).write_text(token, encoding="utf-8")


def target_book(view, regime, strategies, states, cfg, ledger, equity):
    """Each admitted sleeve decides; targets are summed into one account book
    in dollars. Shared by the live cycle and the evening outlook (which passes
    a copy of the states so nothing is saved). decide() mutates `states`."""
    budgets = SleeveManager(cfg).budgets(equity, ledger, regime)
    decisions = {}
    account_target: Dict[str, float] = {}
    for sid in ADMITTED_SLEEVES:
        decision = strategies[sid].decide(view, states[sid])
        decisions[sid] = decision
        for sym, weight in decision.target_weights.items():
            account_target[sym] = (account_target.get(sym, 0.0)
                                   + weight * budgets[sid].sleeve_equity)
    return account_target, decisions


def save_cycle_summary(summary: dict) -> None:
    (state_dir() / _CYCLE_SUMMARY_FILE).write_text(
        json.dumps(summary, indent=2, default=str), encoding="utf-8")


def load_cycle_summary() -> dict:
    path = state_dir() / _CYCLE_SUMMARY_FILE
    try:
        return json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}
    except (OSError, ValueError):
        return {}


def run_cycle(broker, strategies, cfg, ledger, kill_switch, alerter,
              dry_run: bool = True) -> dict:
    """Run one engine cycle: reconcile, decide, diff vs broker, submit (or log)."""
    warnings: List[str] = []     # shown in the daily report; logged as they arise
    tripped, reason = kill_switch.is_triggered()
    if tripped:
        log.warning("KILL SWITCH active (%s) — new entries blocked", reason)
        warnings.append(f"New buys are blocked by the kill switch ({reason}). Sells "
                        f"still run. To resume, delete the HALT_ALL_TRADING file on "
                        f"the /data volume or unset HORIZON_KILL_SWITCH.")

    dataset = cache.load_dataset()
    as_of = calendar.trading_days(dataset)[-1]

    # G0: refuse to trade on stale data. Better an idle cycle than a decision
    # made on a two-week-old picture of the market.
    stale = data_staleness_days(as_of)
    if stale > MAX_STALE_DAYS:
        msg = (f"dataset as_of {as_of.date()} is {stale} days behind the last "
               f"completed session {cache.completed_through().date()} "
               f"(limit {MAX_STALE_DAYS}). No orders will be placed. Check "
               f"Polygon access / cache freshness (horizon/data/cache.py).")
        log.critical("STALE DATA — cycle refused: %s", msg)
        alerter.critical(*R.build_stale_data(str(as_of.date()),
                                             str(cache.completed_through().date()),
                                             MAX_STALE_DAYS))
        refused = {"date": datetime.now(ZoneInfo("America/New_York")).date().isoformat(),
                   "as_of": str(as_of.date()), "status": "refused-stale",
                   "stale_days": stale, "orders": [], "trade_reason": None,
                   "warnings": ["This morning's cycle refused to trade: the market data "
                                "was stale. Positions were unchanged."],
                   "orders_planned": 0, "orders_submitted": 0,
                   "mode": "STALE-DATA (refused)"}
        if not dry_run and broker is not None:
            save_cycle_summary(refused)
        return refused

    view = MarketView(dataset, as_of)
    regime = compute_regime(view)

    # Broker queries degrade gracefully — a broker hiccup must never crash a
    # cycle; it falls back to modeled equity and dry-run.
    equity = cfg.starting_equity
    positions: Dict[str, dict] = {}
    if broker is not None:
        try:
            equity = broker.get_equity()
            positions = broker.get_positions()
        except Exception as exc:
            log.warning("broker unavailable (%s) — modeled equity, dry-run", exc)
            broker, dry_run = None, True

    # Shared-account carve-out: when Horizon coexists with the Unified Engine on
    # ONE live account, HORIZON_CAPITAL_CAP bounds the equity Horizon sizes
    # against (the Unified Engine reserves the same amount via its HORIZON
    # sleeve). Without this, both engines would deploy against the full account.
    account_equity = equity     # performance is reported on the whole account
    _cap = float(os.getenv("HORIZON_CAPITAL_CAP", "0") or 0)
    if _cap > 0 and equity > _cap:
        log.info("capital cap: account equity $%.0f -> capped at $%.0f", equity, _cap)
        equity = _cap

    # Reconcile the ledger against broker truth (read-only-safe; runs in
    # dry-run too so orphans surface during testing).
    orphan_symbols: set = set()
    if broker is not None:
        rec = reconcile(broker, ledger)
        orphan_symbols = set(rec.orphan_symbols)
        if rec.orphan_symbols:
            log.warning("orphaned positions (engine will NOT trade them): %s",
                        rec.orphan_symbols)
            material = {s: positions.get(s, {}).get("market_value", 0.0)
                        for s in rec.orphan_symbols
                        if abs(positions.get(s, {}).get("market_value", 0.0))
                        >= ORPHAN_ALERT_MIN_USD}
            if material:   # dust (e.g. SGOV $0.09, XLK $0.01) is logged, not reported
                warnings.append("Positions Horizon does not own and will not trade: "
                                + ", ".join(f"{s} ${v:,.0f}" for s, v in material.items())
                                + ". Keep or sell them manually.")
        if rec.conflicts:
            kill_switch.trigger(f"ownership conflict: {rec.conflicts}")
            alerter.critical(*R.build_conflict(rec.conflicts))

    # Each admitted sleeve decides; targets are summed into an account book.
    states = _load_states(strategies)
    _cash_legs = {_universe.PULSE_CASH_ASSET, _universe.ROTATION_CASH_ASSET}
    prev_rotation = sorted(s for s in states.get("ROTATION", {}).get("holdings", {})
                           if s not in _cash_legs)
    account_target, decisions = target_book(view, regime, strategies, states, cfg,
                                            ledger, equity)
    _save_states(states)

    # Gross ceiling (checkpoint boundary) — applied before the funding guard.
    _ceiling = float(os.getenv(MAX_GROSS_ENV, "0") or 0)
    account_target, ceiling_scale = apply_gross_ceiling(account_target, _ceiling)
    if ceiling_scale < 1.0:
        log.info("gross ceiling: %s=$%.0f — targets scaled x%.3f",
                 MAX_GROSS_ENV, _ceiling, ceiling_scale)

    # Funding guard — scale targets to what the account can fund.
    funding_scale = 1.0
    if broker is not None and account_target:
        try:
            acct = broker.get_account()
            account_target, funding_scale, fundable = apply_funding_guard(
                account_target, positions, orphan_symbols,
                acct["cash"], acct.get("multiplier", 1.0))
            if funding_scale < 1.0:
                log.info("funding guard: targets scaled x%.3f — wanted $%.0f, "
                         "fundable $%.0f (cash $%.0f, multiplier %.0fx, "
                         "buffer %.0f%%)", funding_scale,
                         sum(v for v in account_target.values()) / funding_scale,
                         fundable, acct["cash"], acct.get("multiplier", 1.0),
                         FUNDING_BUFFER * 100)
            # x0.97 is the designed steady state on a cash account (the 3%
            # buffer). Only a clamp materially beyond it is worth an email.
            if funding_scale < 1.0 - FUNDING_BUFFER - FUNDING_ALERT_MARGIN:
                warnings.append(f"The account could fund only {funding_scale:.0%} of "
                                f"the target book (normal is "
                                f"{1 - FUNDING_BUFFER:.0%}). Usually unsettled cash "
                                f"or a withdrawal; worth a look if it persists.")
        except Exception as exc:
            log.warning("funding guard unavailable (%s) — unguarded", exc)

    # Pending-order awareness — open/unfilled orders count toward effective
    # exposure, so the cycle cannot double-submit when a prior batch hasn't
    # filled yet (the day-1 doubling bug: after-hours --once queued orders
    # the next morning's --daily cycle did not see).
    pending = _pending_notional(broker, view)

    holdings_mv = {s: p.get("market_value", 0.0) for s, p in positions.items()}
    prices = {s: view.close(s) for s in set(account_target) | set(holdings_mv) | set(pending)
              if view.is_tradable(s)}
    token = getattr(cfg, "rebalance_now_token", "")
    forced = forced_rebalance_due(token, _applied_rebalance_token(), tripped)
    band_now = 0.0 if forced else cfg.rebalance_band
    if forced:
        log.info("one-time rebalance %s: band bypassed for this cycle", token)
    orders, band_hit, weight_gap = plan_orders(
        account_target, holdings_mv, pending, prices, orphan_symbols, equity,
        band=band_now, min_trade_frac=cfg.rebalance_min_trade,
        block_buys=tripped, signal_override=cfg.rebalance_signal_override)
    if band_now > 0 and not band_hit:
        log.info("rebalance band: largest weight gap %.2f%% <= band %.2f%% — holding",
                 weight_gap * 100, cfg.rebalance_band * 100)

    # Sells first, buys second. On a cash account (multiplier 1) buying power
    # is settled+unsettled cash; a sell queued at 09:00 frees nothing until it
    # fills at the open, so buys funded by that sell would be rejected at
    # submission. Submit the sells, wait for them to fill, then size the buys
    # to the cash the account actually has.
    sells = [o for o in orders if o["side"] == "sell"]
    buys = [o for o in orders if o["side"] == "buy"]
    submitted = 0
    placed: List[tuple] = []          # (client_order_id or None, order dict)

    def _submit(o):
        coid = (f"{cfg.order_namespace}_{o['symbol']}_{o['side']}_"
                f"{int(time.time() * 1000)}")
        result = broker.submit_market_order(o["symbol"], o["side"],
                                            o["qty"], coid)
        ledger.register_order("ENGINE", o["symbol"], o["side"], o["qty"],
                              coid, result.get("id"), o["notional"])
        placed.append((coid, o))

    if dry_run or broker is None:
        for o in sells + buys:
            log.info("[DRY-RUN] %-4s %-5s qty=%.4f (~$%.0f)",
                     o["side"], o["symbol"], o["qty"], o["notional"])
            placed.append((None, o))
    else:
        for o in sells:
            _submit(o)
            submitted += 1
        if buys:
            need = sum(o["notional"] for o in buys)
            cash, timed_out = _wait_for_cash(broker, need, sells, log)
            sized = size_buys_to_cash(buys, cash)
            short = need - sum(o["notional"] for o in sized)
            if short > 0.02 * max(account_equity, 1.0):
                cause = ("Sells had not filled when the wait ran out"
                         if timed_out else "There was less cash than planned")
                warnings.append(f"{cause}, so buys were cut to the cash available. "
                                f"The book is about ${short:,.0f} under target until "
                                f"the next cycle.")
            for o in sized:
                if o["qty"] <= 0 or o["notional"] < MIN_ORDER_USD:
                    continue
                _submit(o)
                submitted += 1

    ledger.save(state_dir() / _LEDGER_FILE)
    if forced and not dry_run and broker is not None:
        # Recorded only once orders are in (or nothing needed trading), so a
        # cycle that fails before this point retries the rebalance next time.
        _record_rebalance_token(token)

    # The evening report (engine/evening.py) tells the day's story after the
    # close, with final fills. The morning cycle records what it did and why.
    rot = decisions.get("ROTATION")
    rotation_now = sorted(s for s in (rot.target_weights if rot else {}) if s not in _cash_legs)
    pulse = decisions.get("PULSE")
    pulse_lev = sum(w * (2.0 if s == _universe.PULSE_LEVERED_ASSET else 1.0)
                    for s, w in (pulse.target_weights if pulse else {}).items()
                    if s not in _cash_legs)
    trade_reason = None
    if placed and forced:
        trade_reason = getattr(cfg, "rebalance_now_note", "") or "One-time rebalance to target."
    elif placed:
        if rot and rot.note.startswith("rebal") and set(rotation_now) != set(prev_rotation):
            gone = [s for s in prev_rotation if s not in rotation_now]
            new = [s for s in rotation_now if s not in prev_rotation]
            trade_reason = (f"ROTATION's monthly review swapped "
                            f"{' + '.join(gone) or 'T-bills'} for {' + '.join(new) or 'T-bills'}. "
                            f"The other holdings were reset to target at the same time.")
        elif weight_gap > cfg.rebalance_band:
            trade_reason = (f"Holdings had drifted {weight_gap:.0%} from target, past "
                            f"the {cfg.rebalance_band:.0%} rebalance band.")
        else:
            trade_reason = "A position was fully entered or exited."
    if dry_run or broker is None:
        warnings.append("DRY-RUN: no real orders were placed.")

    summary = {"date": datetime.now(ZoneInfo("America/New_York")).date().isoformat(),
               "as_of": str(as_of.date()), "stale_days": stale,
               "status": "traded" if placed else "held",
               "regime": regime.regime, "regime_score": round(regime.score, 1),
               "pulse_leverage": round(pulse_lev, 3), "rotation": rotation_now,
               "equity": round(account_equity, 2), "orphans": len(orphan_symbols),
               "funding_scale": round(funding_scale, 3),
               "ceiling_scale": round(ceiling_scale, 3),
               "weight_gap": round(weight_gap, 4), "band": cfg.rebalance_band,
               "forced": bool(forced),
               "orders": [{"coid": c, "symbol": o["symbol"], "side": o["side"],
                           "notional": round(o["notional"], 2)} for c, o in placed],
               "trade_reason": trade_reason, "warnings": list(warnings),
               "orders_planned": len(orders), "orders_submitted": submitted,
               "mode": "dry-run" if (dry_run or broker is None) else "LIVE"}
    if not dry_run and broker is not None:
        save_cycle_summary(summary)
    log.info("cycle: as_of=%s regime=%s score=%.0f equity=$%.0f ceil=x%.2f "
             "fund=x%.2f gap=%.1f%% band=%.1f%% orders=%d warnings=%d %s", as_of.date(),
             regime.regime, regime.score, account_equity, ceiling_scale, funding_scale,
             weight_gap * 100, cfg.rebalance_band * 100, len(orders), len(warnings),
             summary["mode"])
    return summary


def emergency_flatten(broker, kill_switch, alerter) -> dict:
    """Cancel all orders, liquidate all positions, and trip the kill switch."""
    if broker is None:
        raise RuntimeError("emergency flatten requires a broker connection")
    # Halt first so any running engine stops opening new positions.
    halt = state_dir() / _HALT_FILE
    halt.write_text(f"emergency flatten {datetime.now(timezone.utc).isoformat()}\n",
                    encoding="utf-8")
    kill_switch.trigger("emergency flatten invoked")
    n_orders = broker.cancel_all_orders()
    n_positions = broker.close_all_positions()
    msg = (f"Cancelled {n_orders} open orders, liquidated {n_positions} "
           f"positions. Account is going flat. Kill-switch file written "
           f"({halt}) — remove it to resume trading.")
    log.critical("EMERGENCY FLATTEN: %s", msg)
    alerter.critical(*R.build_flatten(msg))
    return {"orders_cancelled": n_orders, "positions_closed": n_positions}


def is_session_day(day, broker, log=log) -> bool:
    """Should the engine cycle on `day`? Weekends never. Weekday holidays are
    skipped when the broker calendar says there is no session (a holiday
    cycle just queues sells that cannot fill and burns the fill-wait — seen
    on Labor Day 2026-09-07). If the calendar is unavailable, a weekday is
    assumed to be a session so a broker hiccup can never suppress trading."""
    if day.weekday() >= 5:
        return False
    if broker is None:
        return True
    try:
        return bool(broker.is_trading_day(day))
    except Exception as exc:
        log.warning("calendar unavailable (%s) — assuming %s is a session", exc, day)
        return True


def daily_plan(now, session, last_cycle_iso: str, last_report_iso: str,
               report_retry_at=None):
    """What the --daily loop does next. Pure, so every timing case is tested.

    now:      timezone-aware ET datetime
    session:  (open, close) ET datetimes for today, or None (weekend/holiday)
    Returns (action, wake_at): action is one of
      'cycle'        run the 9:00 trading cycle now
      'skip_cycle'   the close has passed without a cycle: too late to trade
      'report'       send the evening report now
      'report_final' send it now even if closing prices are still missing
      'record'       a non-session day: mark the cycle and report as handled
      'sleep'        nothing due; sleep until wake_at
    """
    today = now.date().isoformat()
    tomorrow = (now + timedelta(days=1)).replace(hour=0, minute=5, second=0, microsecond=0)
    if session is None:
        if last_cycle_iso != today or last_report_iso != today:
            return "record", now
        return "sleep", tomorrow
    _open, close = session
    cycle_at = now.replace(hour=DAILY_RUN_HOUR_ET, minute=0, second=0, microsecond=0)
    if last_cycle_iso != today:
        if now < cycle_at:
            return "sleep", cycle_at
        return ("cycle" if now < close else "skip_cycle"), now
    if last_report_iso != today:
        report_at = close + timedelta(minutes=REPORT_DELAY_MIN)
        due = max(report_at, report_retry_at) if report_retry_at else report_at
        if now < due:
            return "sleep", due
        final = now >= close + timedelta(minutes=REPORT_GIVE_UP_MIN)
        return ("report_final" if final else "report"), now
    return "sleep", tomorrow


def _last_report_date() -> str:
    path = state_dir() / _LAST_REPORT_FILE
    try:
        return path.read_text(encoding="utf-8").strip() if path.exists() else ""
    except OSError:
        return ""


def _record_report_date() -> None:
    now_et = datetime.now(ZoneInfo("America/New_York"))
    (state_dir() / _LAST_REPORT_FILE).write_text(now_et.date().isoformat(), encoding="utf-8")


def _session_today(broker, day):
    """Today's (open, close), or None if closed. If the calendar is unavailable
    a weekday is assumed to be a regular session, so a broker hiccup can never
    suppress trading."""
    if day.weekday() >= 5:
        return None
    et = ZoneInfo("America/New_York")
    regular = (datetime(day.year, day.month, day.day, 9, 30, tzinfo=et),
               datetime(day.year, day.month, day.day, 16, 0, tzinfo=et))
    if broker is None:
        return regular
    try:
        return broker.session_hours(day)
    except Exception as exc:
        log.warning("calendar unavailable (%s) — assuming a regular session", exc)
        return regular


def _fmt_when(dt) -> str:
    return f"{dt:%a %b} {dt.day}, {dt.hour % 12 or 12}:{dt:%M} {'AM' if dt.hour < 12 else 'PM'} ET"


def _safe_report(broker, strategies, cfg, ledger, kill_switch, alerter, final: bool):
    """Send the evening report. Returns True when the day's report is done
    (sent, or failed hard and alerted), False to retry later because the
    day's closing prices are not available yet."""
    from .evening import run_evening_report
    try:
        out = run_evening_report(broker, strategies, cfg, ledger, kill_switch, alerter,
                                 final=final)
        if out.get("ready") is False:
            log.info("evening report: closing prices not available yet — retrying "
                     "in %d min", REPORT_RETRY_MIN)
            return False
        return True
    except Exception:
        tb = traceback.format_exc()
        log.exception("evening report error")
        alerter.critical(*R.build_report_failed(tb))
        return True


def _safe_cycle(broker, strategies, cfg, ledger, kill_switch, alerter,
                dry_run) -> None:
    """Run a cycle, converting any exception into a CRITICAL alert. Marks the
    ET-day as attempted on either success or failure so the --daily catch-up
    cannot retry-loop on a persistent error."""
    try:
        run_cycle(broker, strategies, cfg, ledger, kill_switch, alerter, dry_run)
    except Exception:
        tb = traceback.format_exc()
        log.exception("cycle error")
        alerter.critical(*R.build_cycle_failed(tb))
    finally:
        _record_cycle_date()


def main() -> None:
    parser = argparse.ArgumentParser(description="Horizon Engine")
    parser.add_argument("--once", action="store_true", help="run a single cycle")
    parser.add_argument("--daily", action="store_true",
                        help="loop: one cycle per weekday at 09:00 ET")
    parser.add_argument("--interval", type=int, default=0,
                        help="loop every N seconds (testing)")
    parser.add_argument("--flatten", action="store_true",
                        help="EMERGENCY: cancel all orders and liquidate all")
    parser.add_argument("--live", action="store_true",
                        help="submit real orders (requires env confirmation)")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
        handlers=[logging.StreamHandler(),
                  logging.FileHandler(log_dir() / "engine.log",
                                      encoding="utf-8")])

    cfg = build_default_config()
    strategies = build_all()
    log.info("sleeves: %s | book_leverage %.2fx | risk overlay: NOT applied live "
             "(docs/LIMITATIONS.md #13) | funding guard: on | G0 max stale %d days",
             ADMITTED_SLEEVES, cfg.book_leverage, MAX_STALE_DAYS)
    ledger = OwnershipLedger.load(state_dir() / _LEDGER_FILE)
    ledger.prune_terminal(max_age_days=7)
    kill_switch = KillSwitch(flag_path=state_dir() / _HALT_FILE)
    alerter = Alerter()

    # --flatten always needs a real broker; otherwise connect best-effort.
    broker = None
    startup_equity: Optional[float] = None
    if args.flatten or args.live:
        from .broker import create_broker_from_env
        broker = create_broker_from_env()
        # Startup auth probe: verify the keys actually authorize against
        # Alpaca, not just that the client object constructed. Catches a
        # stale ALPACA_API_KEY env var shadowing horizon/.env — instead of
        # falling silently into dry-run on the first cycle hours later.
        key_source = ("os.environ" if "ALPACA_API_KEY" in os.environ
                      else "horizon/.env")
        api_key = (os.environ.get("ALPACA_API_KEY")
                   or "")  # for logging the prefix only
        try:
            account = broker.get_account()
            startup_equity = float(account["equity"])
            log.info("broker AUTHORIZED: paper=%s, equity=$%.2f, status=%s",
                     broker.paper, account["equity"], account["status"])
            log.info("keys resolved from %s (prefix: %s...)",
                     key_source, api_key[:6] if api_key else "(.env)")
        except Exception as exc:
            log.critical("broker AUTH FAILED at startup: %s", exc)
            log.critical("keys resolved from %s (prefix: %s...)",
                         key_source, api_key[:6] if api_key else "(.env)")
            if key_source == "os.environ":
                log.critical("A stale ALPACA_API_KEY env var is shadowing "
                             "horizon/.env. Clear it and re-run:")
                log.critical("  PowerShell: Remove-Item Env:\\ALPACA_API_KEY, "
                             "Env:\\ALPACA_SECRET_KEY")
            alerter.critical(*R.build_auth_failed(str(exc)))
            raise SystemExit(2)
    else:
        try:
            from .broker import create_broker_from_env
            broker = create_broker_from_env()
            log.info("dry-run — broker connected for equity/positions only")
        except Exception as exc:
            log.warning("no broker (%s) — using modeled equity", exc)

    if args.flatten:
        emergency_flatten(broker, kill_switch, alerter)
        return

    dry_run = not args.live
    if args.daily:
        log.info("daily mode — trading cycle at %02d:00 ET, evening report %d min "
                 "after the close, on market days", DAILY_RUN_HOUR_ET, REPORT_DELAY_MIN)
        et = ZoneInfo("America/New_York")
        _now = datetime.now(et)
        _sess = _session_today(broker, _now.date())
        _action, _wake = daily_plan(_now, _sess, _last_cycle_date(), _last_report_date())
        if _action == "cycle":
            _next = "now (catching up)"
        elif _action in ("report", "report_final"):
            _next = "evening report now"
        else:
            _next = _fmt_when(_wake) if _action == "sleep" else "next market day, 9:00 AM ET"
        # A (re)start is worth knowing about, and this send proves alert
        # delivery from the deployed network at boot rather than at the next
        # cycle. A crash loop (restartPolicy ON_FAILURE, max 10) emails at most
        # 10 times — which is exactly when you want to hear about it.
        alerter.send(*R.build_restart(_fmt_when(_now), startup_equity, _next,
                                      live=not dry_run),
                     level="INFO", dedup_minutes=0)
        retry_at = None
        while True:
            now_et = datetime.now(et)
            today_iso = now_et.date().isoformat()
            session = _session_today(broker, now_et.date())
            action, wake = daily_plan(now_et, session, _last_cycle_date(),
                                      _last_report_date(), retry_at)
            if action == "cycle":
                log.info("trading cycle for %s", today_iso)
                _safe_cycle(broker, strategies, cfg, ledger, kill_switch, alerter, dry_run)
            elif action == "skip_cycle":
                log.warning("%s: the close passed without a cycle — too late to trade "
                            "today; the next cycle trades at the next open", today_iso)
                save_cycle_summary({"date": today_iso, "status": "skipped", "orders": [],
                                    "trade_reason": None, "warnings": [
                                        "No trading cycle ran today: the engine was not "
                                        "running at 9:00 AM ET. It trades at the next open."]})
                _record_cycle_date()
            elif action in ("report", "report_final"):
                done = _safe_report(broker, strategies, cfg, ledger, kill_switch, alerter,
                                    final=(action == "report_final"))
                if done:
                    _record_report_date()
                    retry_at = None
                else:
                    retry_at = now_et + timedelta(minutes=REPORT_RETRY_MIN)
            elif action == "record":
                if now_et.weekday() < 5:
                    log.info("%s is a market holiday — no cycle, no report", today_iso)
                # Record EVERY non-session day (weekends included), so a weekend
                # restart cannot spin this loop.
                _record_cycle_date()
                _record_report_date()
            else:
                secs = max(1.0, (wake - now_et).total_seconds())
                log.info("next check %s (in %.1f hours)", _fmt_when(wake), secs / 3600.0)
                time.sleep(secs)
    elif args.interval > 0:
        log.info("interval mode — every %ds (Ctrl-C to stop)", args.interval)
        while True:
            _safe_cycle(broker, strategies, cfg, ledger, kill_switch, alerter,
                        dry_run)
            time.sleep(args.interval)
    else:
        _safe_cycle(broker, strategies, cfg, ledger, kill_switch, alerter,
                    dry_run)


if __name__ == "__main__":
    main()
