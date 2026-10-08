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
_REPORT_STATE_FILE = "report_state.json"  # drawdown-step and regime memory for the report
FILL_CONFIRM_SEC = 90                  # how long to wait for fills before reporting
_LAST_CYCLE_FILE = "last_cycle.txt"   # records the ET date of the most recent cycle


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


def _confirm_fills(broker, coids, timeout: int = FILL_CONFIRM_SEC,
                   poll: int = 5) -> Dict[str, dict]:
    """Poll submitted orders until each is terminal or `timeout` passes, so the
    report shows what actually filled. Market orders at the open fill in
    seconds; a slow fill only delays the email."""
    terminal = ("filled", "canceled", "cancelled", "expired", "rejected")
    out: Dict[str, dict] = {}
    deadline = time.time() + timeout
    while True:
        for c in coids:
            if c in out and any(t in str(out[c].get("status", "")).lower() for t in terminal) \
                    and "partially" not in str(out[c].get("status", "")).lower():
                continue
            try:
                od = broker.get_order_by_client_id(c)
                if od:
                    out[c] = od
            except Exception as exc:
                log.warning("fill check failed for %s (%s)", c, exc)
        done = all(c in out and any(t in str(out[c].get("status", "")).lower() for t in terminal)
                   and "partially" not in str(out[c].get("status", "")).lower() for c in coids)
        if done or time.time() >= deadline:
            return out
        time.sleep(poll)


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
        return {"as_of": str(as_of.date()), "stale_days": stale,
                "orders_planned": 0, "orders_submitted": 0,
                "mode": "STALE-DATA (refused)"}

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

    budgets = SleeveManager(cfg).budgets(equity, ledger, regime)

    # Each admitted sleeve decides; targets are summed into an account book.
    states = _load_states(strategies)
    _cash_legs = {_universe.PULSE_CASH_ASSET, _universe.ROTATION_CASH_ASSET}
    prev_rotation = sorted(s for s in states.get("ROTATION", {}).get("holdings", {})
                           if s not in _cash_legs)
    decisions = {}
    account_target: Dict[str, float] = {}
    for sid in ADMITTED_SLEEVES:
        decision = strategies[sid].decide(view, states[sid])
        decisions[sid] = decision
        for sym, weight in decision.target_weights.items():
            account_target[sym] = (account_target.get(sym, 0.0)
                                   + weight * budgets[sid].sleeve_equity)
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
    orders, band_hit, weight_gap = plan_orders(
        account_target, holdings_mv, pending, prices, orphan_symbols, equity,
        band=cfg.rebalance_band, min_trade_frac=cfg.rebalance_min_trade,
        block_buys=tripped, signal_override=cfg.rebalance_signal_override)
    if cfg.rebalance_band > 0 and not band_hit:
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

    # --- the daily report --------------------------------------------------
    fills = _confirm_fills(broker, [c for c, _ in placed if c]) if (
        broker is not None and not dry_run and placed) else {}
    report_orders, unfilled = [], []
    for coid, o in placed:
        od = fills.get(coid) if coid else None
        filled = None
        if od and float(od.get("filled_qty") or 0) > 0:
            filled = float(od["filled_qty"]) * float(od.get("filled_avg_price") or 0)
        status = str((od or {}).get("status", "")).lower()
        if coid and any(x in status for x in ("rejected", "canceled", "cancelled", "expired")):
            warnings.append(f"The order to {o['side']} {o['symbol']} was "
                            f"{status.split('.')[-1]} by the broker.")
        elif coid and filled is None:
            unfilled.append(o["symbol"])
        report_orders.append({"symbol": o["symbol"], "side": o["side"],
                              "notional": o["notional"], "filled": filled})
    if unfilled:
        warnings.append(f"Not filled {FILL_CONFIRM_SEC} seconds after submission: "
                        f"{', '.join(unfilled)}. Check the account.")

    # Holdings after today's trades, valued at the as_of close so they add up
    # to the reported equity (9:00 broker marks include pre-market moves).
    as_of_iso = str(as_of.date())
    cash_after = 0.0
    close_equity = account_equity    # fallback: 9:00 equity (includes pre-market moves)
    pos_after = positions
    if broker is not None:
        try:
            if placed:
                pos_after = broker.get_positions()
            acct_now = broker.get_account()
            cash_after = float(acct_now["cash"])
            close_equity = float(acct_now.get("last_equity") or account_equity)
        except Exception as exc:
            log.warning("post-trade snapshot failed (%s) — reporting pre-trade holdings", exc)
    holdings_after = {}
    for sym, p in pos_after.items():
        qty = float(p.get("qty", 0.0) or 0.0)
        mv = qty * view.close(sym) if (qty and view.is_tradable(sym)) else float(p.get("market_value", 0.0))
        if abs(mv) >= ORPHAN_ALERT_MIN_USD:
            holdings_after[sym] = mv

    # Performance, recomputed from Alpaca's daily closes every day (no running
    # tally), time-weighted so deposits and withdrawals are not returns.
    today_iso = datetime.now(ZoneInfo("America/New_York")).date().isoformat()
    perf = None
    if broker is not None:
        try:
            closes = broker.daily_closes(cfg.report_inception, today_iso)
            perf_date = as_of_iso
            if as_of_iso not in closes and closes and max(closes) < as_of_iso:
                latest = max(closes)
                if abs(closes[latest] - close_equity) > 0.01:
                    closes[as_of_iso] = close_equity   # history lags; last_equity is as_of's close
                else:
                    # last_equity still describes the previous close (Alpaca rolls it
                    # overnight), so as_of's close is not known yet: report the latest
                    # close available rather than mislabel one day's equity as another's.
                    perf_date = latest
            elif as_of_iso in closes and abs(closes[as_of_iso] - close_equity) > 1.0:
                log.warning("history close %.2f != last_equity %.2f for %s",
                            closes[as_of_iso], close_equity, as_of_iso)
            flows = broker.cash_flows_by_date(cfg.report_inception, today_iso)
            bench = {b: dataset[b]["tr_close"] for b in R.BENCHMARKS if b in dataset}
            perf = R.compute_performance(closes, flows, bench, perf_date, cfg.report_inception)
        except Exception as exc:
            log.warning("performance unavailable (%s)", exc)
        if perf is None:
            warnings.append("Performance figures could not be computed today (Alpaca's "
                            "account history did not load). Trading was not affected.")

    state_path = state_dir() / _REPORT_STATE_FILE
    rstate = R.load_state(state_path)
    prev_regime = rstate.get("last_regime")
    crossed = None
    if perf is not None:
        rstate, crossed = R.drawdown_steps(rstate, perf)
    rstate.update(last_regime=regime.regime, last_report_date=today_iso)

    rot = decisions.get("ROTATION")
    rotation_now = sorted(s for s in (rot.target_weights if rot else {}) if s not in _cash_legs)
    pulse = decisions.get("PULSE")
    pulse_lev = sum(w * (2.0 if s == _universe.PULSE_LEVERED_ASSET else 1.0)
                    for s, w in (pulse.target_weights if pulse else {}).items()
                    if s not in _cash_legs)
    trade_reason = None
    if placed:
        if rot and rot.note.startswith("rebal") and set(rotation_now) != set(prev_rotation):
            gone = [s for s in prev_rotation if s not in rotation_now]
            new = [s for s in rotation_now if s not in prev_rotation]
            trade_reason = (f"ROTATION's monthly rebalance swapped "
                            f"{' + '.join(gone) or 'T-bills'} for {' + '.join(new) or 'T-bills'}. "
                            f"The other holdings were reset to target at the same time.")
        elif weight_gap > cfg.rebalance_band:
            trade_reason = (f"Holdings had drifted {weight_gap:.0%} from target, past "
                            f"the {cfg.rebalance_band:.0%} rebalance band.")
        else:
            trade_reason = "A position was fully entered or exited."
    if dry_run or broker is None:
        warnings.append("DRY-RUN: no real orders were placed.")

    subject, body, html = R.build_daily_report(
        as_of=as_of_iso, perf=perf, equity=close_equity, orders=report_orders,
        trade_reason=trade_reason, drift=weight_gap, band=cfg.rebalance_band,
        holdings=holdings_after, cash=cash_after, regime=regime.regime,
        score=regime.score, prev_regime=prev_regime,
        pulse_lev=pulse_lev, rotation=rotation_now, warnings=warnings)

    if not dry_run and broker is not None:
        R.save_state(state_path, rstate)
        if crossed:
            alerter.warning(*R.build_drawdown_alert(crossed, perf))

    summary = {"as_of": str(as_of.date()), "stale_days": stale,
               "regime": regime.regime, "regime_score": round(regime.score, 1),
               "equity": round(account_equity, 2), "orphans": len(orphan_symbols),
               "funding_scale": round(funding_scale, 3),
               "ceiling_scale": round(ceiling_scale, 3),
               "weight_gap": round(weight_gap, 4), "band": cfg.rebalance_band,
               "orders_planned": len(orders), "orders_submitted": submitted,
               "warnings": len(warnings),
               "mode": "dry-run" if (dry_run or broker is None) else "LIVE"}
    log.info("cycle: as_of=%s regime=%s score=%.0f equity=$%.0f ceil=x%.2f "
             "fund=x%.2f gap=%.1f%% band=%.1f%% orders=%d warnings=%d %s", as_of.date(),
             regime.regime, regime.score, account_equity, ceiling_scale, funding_scale,
             weight_gap * 100, cfg.rebalance_band * 100, len(orders), len(warnings),
             summary["mode"])
    alerter.heartbeat(subject, body, html)
    summary["report_subject"], summary["report_body"], summary["report_html"] = subject, body, html
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


def daily_action(now_et, last_cycle_iso: str, session_today: bool) -> str:
    """What the --daily loop should do right now: 'cycle', 'record' (mark a
    non-session day as handled), or 'sleep'. Pure, so it is unit-tested."""
    if last_cycle_iso == now_et.date().isoformat():
        return "sleep"
    if now_et.hour < DAILY_RUN_HOUR_ET:
        return "sleep"     # never fire before 09:00 ET: sells could not fill for hours
    return "cycle" if session_today else "record"


def _seconds_until_daily_run(hour_et: int = DAILY_RUN_HOUR_ET) -> float:
    """Seconds until the next weekday run time, in US/Eastern."""
    et = ZoneInfo("America/New_York")
    now = datetime.now(et)
    target = now.replace(hour=hour_et, minute=0, second=0, microsecond=0)
    if target <= now:
        target += timedelta(days=1)
    while target.weekday() >= 5:   # Saturday=5, Sunday=6
        target += timedelta(days=1)
    return max(0.0, (target - now).total_seconds())


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
        log.info("daily mode — one cycle per weekday at %02d:00 ET "
                 "(immediate catch-up if today hasn't run yet)",
                 DAILY_RUN_HOUR_ET)
        # A (re)start is worth knowing about, and this send proves alert
        # delivery from the deployed network at boot rather than at the next
        # cycle. A crash loop (restartPolicy ON_FAILURE, max 10) emails at most
        # 10 times — which is exactly when you want to hear about it.
        _et = ZoneInfo("America/New_York")
        _now = datetime.now(_et)
        if daily_action(_now, _last_cycle_date(), True) == "cycle":
            _next = "today, now (catching up)"
        else:
            _nxt = _now + timedelta(seconds=_seconds_until_daily_run())
            _next = f"{_nxt:%a %b} {_nxt.day}, 9:00 AM ET"
        alerter.send(*R.build_restart(f"{_now:%a %b} {_now.day}, {_now:%I:%M %p} ET",
                                      startup_equity, _next, live=not dry_run),
                     level="INFO", dedup_minutes=0)
        et = ZoneInfo("America/New_York")
        while True:
            now_et = datetime.now(et)
            today_iso = now_et.date().isoformat()
            # Catch-up: if today is a session and no cycle has run today yet,
            # fire one immediately instead of sleeping to tomorrow — but only
            # at/after the scheduled hour. A restart at 00:06 ET used to fire
            # the day's cycle immediately: its sells could not fill for 9
            # hours, so the buys were sized to the cash on hand and the book
            # sat under-invested until the next cycle.
            last = _last_cycle_date()
            pre = daily_action(now_et, last, True)
            if pre != "sleep":
                action = daily_action(now_et, last,
                                      is_session_day(now_et.date(), broker))
                if action == "cycle":
                    log.info("catch-up: today (%s) has not run yet — firing now",
                             today_iso)
                    _safe_cycle(broker, strategies, cfg, ledger, kill_switch,
                                alerter, dry_run)
                else:
                    if now_et.weekday() < 5:
                        log.info("%s is a market holiday — no cycle", today_iso)
                    # Record EVERY non-session day (weekends included). The
                    # 2026-09-10 version only recorded holidays, so a weekend
                    # restart spun this loop with no sleep until Monday.
                    _record_cycle_date()
                continue   # loop back, then sleep to the next scheduled slot
            wait = _seconds_until_daily_run()
            log.info("next cycle in %.1f hours", wait / 3600.0)
            time.sleep(wait)
            # After the sleep the loop re-enters and the session check above
            # decides whether to cycle — holidays are skipped there.
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
