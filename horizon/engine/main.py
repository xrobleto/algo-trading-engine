"""Horizon engine — live orchestration loop.

Single source of truth: the SAME Strategy objects the backtest validated are
imported here, and their identical `decide()` drives live orders. There is no
separate "live version" of any strategy.

The engine defaults to **dry-run** — it logs the orders it would place but
submits nothing. Live trading is the user's explicit decision (the brief: do
not deploy). Run:

    python -m horizon.engine.main --once            # one dry-run cycle
    python -m horizon.engine.main --interval 900    # dry-run loop, 15-min
    python -m horizon.engine.main --once --live     # submit (needs env confirm)
"""

from __future__ import annotations

import argparse
import json
import logging
import time
from datetime import datetime, timezone
from typing import Dict

from ..config import build_default_config
from ..data import cache, calendar
from ..paths import get_secret, log_dir, state_dir
from ..strategies.base import MarketView
from ..strategies.registry import build_all
from .intelligence import compute_regime
from .killswitch import KillSwitch
from .ledger import OwnershipLedger
from .reconciler import reconcile
from .sleeves import SleeveManager

log = logging.getLogger("horizon.engine")

# Only the sleeves that cleared the validation gating bar trade live.
ADMITTED_SLEEVES = ["PULSE", "ROTATION"]

MIN_ORDER_USD = 1.0
_STATE_FILE = "engine_state.json"
_LEDGER_FILE = "ledger.json"


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


def run_cycle(broker, strategies, cfg, ledger, kill_switch,
              dry_run: bool = True) -> dict:
    """Run one engine cycle: decide, diff vs broker, submit (or log)."""
    tripped, reason = kill_switch.is_triggered()
    if tripped:
        log.warning("KILL SWITCH active (%s) — new entries blocked", reason)

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

    if broker is not None and not dry_run:
        rec = reconcile(broker, ledger)
        if rec.conflicts:
            kill_switch.trigger(f"ownership conflict: {rec.conflicts}")
    ledger.save(state_dir() / _LEDGER_FILE)

    summary = {"as_of": str(as_of.date()), "regime": regime.regime,
               "regime_score": round(regime.score, 1), "equity": equity,
               "orders_planned": len(orders), "orders_submitted": submitted,
               "dry_run": dry_run}
    log.info("cycle: regime=%s score=%.0f equity=$%.0f orders=%d %s",
             regime.regime, regime.score, equity, len(orders),
             "(dry-run)" if dry_run else "(LIVE)")
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Horizon Engine")
    parser.add_argument("--once", action="store_true", help="run a single cycle")
    parser.add_argument("--interval", type=int, default=0,
                        help="loop every N seconds (0 = use --once)")
    parser.add_argument("--live", action="store_true",
                        help="submit real orders (requires env confirmation)")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
        handlers=[logging.StreamHandler(),
                  logging.FileHandler(log_dir() / "engine.log")])

    cfg = build_default_config()
    strategies = build_all()
    ledger = OwnershipLedger.load(state_dir() / _LEDGER_FILE)
    ledger.prune_terminal(max_age_days=7)
    kill_switch = KillSwitch(flag_path=state_dir() / "HALT_ALL_TRADING")

    broker = None
    if args.live:
        from .broker import create_broker_from_env
        broker = create_broker_from_env()
        log.info("LIVE mode — broker connected (paper=%s)", broker.paper)
    else:
        try:
            from .broker import create_broker_from_env
            broker = create_broker_from_env()
            log.info("dry-run mode — broker connected for equity/positions only")
        except Exception as exc:
            log.warning("no broker (%s) — using modeled equity", exc)

    dry_run = not args.live
    if args.interval > 0:
        log.info("looping every %ds (Ctrl-C to stop)", args.interval)
        while True:
            try:
                run_cycle(broker, strategies, cfg, ledger, kill_switch, dry_run)
            except Exception:
                log.exception("cycle error — continuing")
            time.sleep(args.interval)
    else:
        run_cycle(broker, strategies, cfg, ledger, kill_switch, dry_run)


if __name__ == "__main__":
    main()
