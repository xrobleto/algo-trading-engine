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
ADMITTED_SLEEVES = ["PULSE", "ROTATION"]

MIN_ORDER_USD = 1.0
DAILY_RUN_HOUR_ET = 9      # run once per weekday at 09:00 ET, before the open
_STATE_FILE = "engine_state.json"
_LEDGER_FILE = "ledger.json"
_HALT_FILE = "HALT_ALL_TRADING"


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


def run_cycle(broker, strategies, cfg, ledger, kill_switch, alerter,
              dry_run: bool = True) -> dict:
    """Run one engine cycle: reconcile, decide, diff vs broker, submit (or log)."""
    tripped, reason = kill_switch.is_triggered()
    if tripped:
        log.warning("KILL SWITCH active (%s) — new entries blocked", reason)
        alerter.warning("kill switch active", f"New entries blocked: {reason}")

    dataset = cache.load_dataset()
    as_of = calendar.trading_days(dataset)[-1]
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

    orders = []
    for sym in set(account_target) | set(positions):
        if sym in orphan_symbols:
            continue  # unmanaged — never auto-traded
        target = account_target.get(sym, 0.0)
        current = positions.get(sym, {}).get("market_value", 0.0)
        delta = target - current
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

    submitted = 0
    for o in orders:
        coid = (f"{cfg.order_namespace}_{o['symbol']}_{o['side']}_"
                f"{int(time.time() * 1000)}")
        if dry_run or broker is None:
            log.info("[DRY-RUN] %-4s %-5s qty=%.4f (~$%.0f)",
                     o["side"], o["symbol"], o["qty"], o["notional"])
        else:
            result = broker.submit_market_order(o["symbol"], o["side"],
                                                o["qty"], coid)
            ledger.register_order("ENGINE", o["symbol"], o["side"], o["qty"],
                                  coid, result.get("id"), o["notional"])
            submitted += 1

    ledger.save(state_dir() / _LEDGER_FILE)

    summary = {"as_of": str(as_of.date()), "regime": regime.regime,
               "regime_score": round(regime.score, 1),
               "equity": round(equity, 2), "orphans": len(orphan_symbols),
               "orders_planned": len(orders), "orders_submitted": submitted,
               "mode": "dry-run" if (dry_run or broker is None) else "LIVE"}
    log.info("cycle: regime=%s score=%.0f equity=$%.0f orders=%d %s",
             regime.regime, regime.score, equity, len(orders),
             summary["mode"])
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
    """Run a cycle, converting any exception into a CRITICAL alert."""
    try:
        run_cycle(broker, strategies, cfg, ledger, kill_switch, alerter, dry_run)
    except Exception:
        tb = traceback.format_exc()
        log.exception("cycle error")
        alerter.critical("engine cycle failed", tb)


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
        log.info("daily mode — one cycle per weekday at %02d:00 ET",
                 DAILY_RUN_HOUR_ET)
        while True:
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
