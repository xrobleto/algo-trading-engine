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
from typing import Dict
from zoneinfo import ZoneInfo

import pandas as pd

from ..config import build_default_config
from ..data import cache, calendar
from ..paths import log_dir, state_dir
from ..strategies.base import MarketView
from ..strategies.registry import build_all
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
                   poll: int = SELL_POLL_SEC) -> float:
    """Return the account's cash once it covers `need` or once the queued
    sells have all left the open-order book (filled/cancelled) or max_wait
    elapses. Buys are sized to whatever cash is available at that point."""
    sell_syms = {o["symbol"] for o in sells}
    deadline = time.time() + max_wait
    cash = 0.0
    while True:
        try:
            cash = float(broker.get_account()["cash"])
            if cash * (1.0 - CASH_BUFFER) >= need:
                return cash
            if sell_syms:
                open_syms = {o.get("symbol") for o in broker.get_open_orders()}
                if not (sell_syms & open_syms):
                    return cash          # sells done; this is all the cash there is
            else:
                return cash              # nothing pending that could add cash
        except Exception as exc:
            log.warning("cash poll failed (%s)", exc)
        if time.time() >= deadline:
            log.warning("waited %ds for sells to fill; sizing buys to cash $%.0f",
                        max_wait, cash)
            return cash
        log.info("waiting for sells to fill: cash $%.0f < need $%.0f", cash, need)
        time.sleep(poll)


def run_cycle(broker, strategies, cfg, ledger, kill_switch, alerter,
              dry_run: bool = True) -> dict:
    """Run one engine cycle: reconcile, decide, diff vs broker, submit (or log)."""
    tripped, reason = kill_switch.is_triggered()
    if tripped:
        log.warning("KILL SWITCH active (%s) — new entries blocked", reason)
        alerter.warning("kill switch active", f"New entries blocked: {reason}")

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
        alerter.critical("STALE DATA — Horizon cycle refused", msg)
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
            alerter.warning("orphaned positions detected",
                            f"Positions with no engine record: "
                            f"{rec.orphan_symbols}. The engine will not trade "
                            f"these — review and resolve manually.")
        if rec.conflicts:
            kill_switch.trigger(f"ownership conflict: {rec.conflicts}")
            alerter.critical("ownership conflict — kill switch tripped",
                             f"Conflicting sleeve ownership: {rec.conflicts}")

    budgets = SleeveManager(cfg).budgets(equity, ledger, regime)

    # Each admitted sleeve decides; targets are summed into an account book.
    states = _load_states(strategies)
    account_target: Dict[str, float] = {}
    for sid in ADMITTED_SLEEVES:
        decision = strategies[sid].decide(view, states[sid])
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
                log.warning("funding guard: targets scaled x%.3f — wanted $%.0f, "
                            "fundable $%.0f (cash $%.0f, multiplier %.0fx, "
                            "buffer %.0f%%)", funding_scale,
                            sum(v for v in account_target.values()) / funding_scale,
                            fundable, acct["cash"], acct.get("multiplier", 1.0),
                            FUNDING_BUFFER * 100)
                alerter.warning("funding guard engaged",
                                f"Targets scaled x{funding_scale:.3f}: the account "
                                f"(multiplier {acct.get('multiplier', 1.0):.0f}x) "
                                f"cannot fund the full vol-targeted book. Enable "
                                f"margin, lower book_leverage, or accept the clamp.")
        except Exception as exc:
            log.warning("funding guard unavailable (%s) — unguarded", exc)

    # Pending-order awareness — open/unfilled orders count toward effective
    # exposure, so the cycle cannot double-submit when a prior batch hasn't
    # filled yet (the day-1 doubling bug: after-hours --once queued orders
    # the next morning's --daily cycle did not see).
    pending = _pending_notional(broker, view)

    orders = []
    for sym in set(account_target) | set(positions) | set(pending):
        if sym in orphan_symbols:
            continue  # unmanaged — never auto-traded
        target = account_target.get(sym, 0.0)
        filled = positions.get(sym, {}).get("market_value", 0.0)
        effective = filled + pending.get(sym, 0.0)
        delta = target - effective
        if abs(delta) < MIN_ORDER_USD:
            continue
        side = "buy" if delta > 0 else "sell"
        if side == "buy" and tripped:
            continue  # kill switch blocks new exposure, never exits
        price = view.close(sym) if view.is_tradable(sym) else None
        if not price or price <= 0:
            continue
        orders.append({"symbol": sym, "side": side,
                        "qty": round(abs(delta) / price, 4),
                        "notional": abs(delta)})

    # Sells first, buys second. On a cash account (multiplier 1) buying power
    # is settled+unsettled cash; a sell queued at 09:00 frees nothing until it
    # fills at the open, so buys funded by that sell would be rejected at
    # submission. Submit the sells, wait for them to fill, then size the buys
    # to the cash the account actually has.
    sells = [o for o in orders if o["side"] == "sell"]
    buys = [o for o in orders if o["side"] == "buy"]
    submitted = 0

    def _submit(o):
        coid = (f"{cfg.order_namespace}_{o['symbol']}_{o['side']}_"
                f"{int(time.time() * 1000)}")
        result = broker.submit_market_order(o["symbol"], o["side"],
                                            o["qty"], coid)
        ledger.register_order("ENGINE", o["symbol"], o["side"], o["qty"],
                              coid, result.get("id"), o["notional"])

    if dry_run or broker is None:
        for o in sells + buys:
            log.info("[DRY-RUN] %-4s %-5s qty=%.4f (~$%.0f)",
                     o["side"], o["symbol"], o["qty"], o["notional"])
    else:
        for o in sells:
            _submit(o)
            submitted += 1
        if buys:
            need = sum(o["notional"] for o in buys)
            cash = _wait_for_cash(broker, need, sells, log)
            buys = size_buys_to_cash(buys, cash)
            for o in buys:
                if o["qty"] <= 0 or o["notional"] < MIN_ORDER_USD:
                    continue
                _submit(o)
                submitted += 1

    ledger.save(state_dir() / _LEDGER_FILE)

    summary = {"as_of": str(as_of.date()), "stale_days": stale,
               "regime": regime.regime,
               "regime_score": round(regime.score, 1),
               "equity": round(equity, 2), "orphans": len(orphan_symbols),
               "funding_scale": round(funding_scale, 3),
               "ceiling_scale": round(ceiling_scale, 3),
               "orders_planned": len(orders), "orders_submitted": submitted,
               "mode": "dry-run" if (dry_run or broker is None) else "LIVE"}
    log.info("cycle: as_of=%s regime=%s score=%.0f equity=$%.0f ceil=x%.2f "
             "fund=x%.2f orders=%d %s", as_of.date(), regime.regime, regime.score,
             equity, ceiling_scale, funding_scale, len(orders), summary["mode"])
    alerter.heartbeat(summary)
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
    alerter.critical("EMERGENCY FLATTEN executed", msg)
    return {"orders_cancelled": n_orders, "positions_closed": n_positions}


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
        alerter.critical("engine cycle failed", tb)
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
            alerter.critical(
                "Horizon startup: broker auth failed",
                f"create_broker_from_env succeeded but get_account returned:\n"
                f"  {exc}\n\n"
                f"Resolved key source: {key_source} "
                f"(prefix: {api_key[:6] if api_key else '(.env)'}...).\n\n"
                f"If the source is os.environ, a stale ALPACA_API_KEY in your "
                f"shell is shadowing horizon/.env — clear it and re-run. "
                f"Otherwise verify the keys in horizon/.env.\n\n"
                f"The engine REFUSED to start to avoid silently sitting in "
                f"dry-run.")
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
        et = ZoneInfo("America/New_York")
        while True:
            now_et = datetime.now(et)
            today_iso = now_et.date().isoformat()
            is_weekday = now_et.weekday() < 5
            # Catch-up: if today is a trading day and no cycle has run today
            # yet, fire one immediately instead of sleeping to tomorrow.
            if is_weekday and _last_cycle_date() != today_iso:
                log.info("catch-up: today (%s) has not run yet — firing now",
                         today_iso)
                _safe_cycle(broker, strategies, cfg, ledger, kill_switch,
                            alerter, dry_run)
                continue   # loop back, then sleep to the next scheduled slot
            wait = _seconds_until_daily_run()
            log.info("next cycle in %.1f hours", wait / 3600.0)
            time.sleep(wait)
            _safe_cycle(broker, strategies, cfg, ledger, kill_switch, alerter,
                        dry_run)
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
