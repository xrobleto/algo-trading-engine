"""Read-only preview of the next cycle against the live account.

    railway run -s horizon-live -- python -m horizon.engine.main --preview
    python -m horizon.engine.main --preview --state-from DIR

The plain dry run (--once without --live) builds its broker from LIVE_TRADING:
with live keys and LIVE_TRADING=0 it calls the paper endpoint, is refused
(40110000), and plans a fresh book against modeled equity. A preview instead:

  - reads the live account (paper=False) through ReadOnlyBroker, which passes
    reads only and whose HTTP client sends nothing but GET. LIVE_TRADING and
    I_UNDERSTAND_LIVE_TRADING are not consulted: they gate order submission,
    which a preview never reaches;
  - stops with an error if the account cannot be read, rather than planning
    against modeled equity;
  - runs the engine's own run_cycle on a scratch copy of the cycle's state
    (ledger, strategy states, rebalance token, kill-switch file). The source
    is only read. In the horizon-live container it is the live volume; every
    write the cycle makes (ledger, states, log) lands in the copy;
  - sends no alerts.

State the source lacks is rebuilt, and the output says so:
  - no ledger: positions in the admitted sleeves' universes are treated as
    ENGINE-owned (the live engine registers its orders as ENGINE). Anything
    else is adopted as UNMANAGED by reconcile(), as live;
  - no strategy states: the strategies replay the sessions before as_of,
    which reproduces a service that cycled every session (ROTATION's monthly
    pick included);
  - no rebalance token file: a configured one-time rebalance counts as not yet
    applied, so the plan bypasses the band.
"""

from __future__ import annotations

import json
import logging
import os
import shutil
import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import Dict, Iterator, List, Optional, Tuple

from ..config import build_default_config
from ..data import cache, calendar
from ..paths import _base_output_dir, log_dir, state_dir
from ..strategies.base import MarketView
from ..strategies.registry import build_all
from . import main as M
from . import report as R
from .alerts import Alerter
from .broker import create_preview_broker_from_env
from .killswitch import KillSwitch
from .ledger import OwnershipLedger

log = logging.getLogger("horizon.preview")

# What one cycle reads from the state directory. Copied, never written back.
STATE_FILES = (M._STATE_FILE, M._LEDGER_FILE, M._REBALANCE_TOKEN_FILE, M._HALT_FILE)
# Enough sessions to start before the previous month began, so the replay
# crosses a month boundary and rebuilds ROTATION's current pick.
REPLAY_SESSIONS = 60
DUST_USD = 5.0


def source_state_dir(state_from: Optional[str] = None) -> Path:
    """Where the live state is read from: --state-from, else this engine's own
    state directory (the live volume in the horizon-live container). Unlike
    state_dir(), never creates it."""
    return Path(state_from).expanduser() if state_from else _base_output_dir() / "state"


@contextmanager
def isolated(source: Path) -> Iterator[Tuple[Path, List[str]]]:
    """Copy the cycle's state files from `source` into a new scratch root, then
    point ALGO_OUTPUT_DIR there and switch alerts off until exit, so every
    state_dir() / log_dir() write lands in the copy. Yields (root, files
    found in `source`)."""
    root = Path(tempfile.mkdtemp(prefix="horizon-preview-"))
    dest = root / "horizon" / "state"
    dest.mkdir(parents=True)
    found = []
    for name in STATE_FILES:
        if (source / name).is_file():
            shutil.copyfile(source / name, dest / name)
            found.append(name)
    saved = {k: os.environ.get(k) for k in ("ALGO_OUTPUT_DIR", "HORIZON_ALERTS_ENABLED")}
    os.environ["ALGO_OUTPUT_DIR"] = str(root)
    os.environ["HORIZON_ALERTS_ENABLED"] = "0"
    try:
        yield root, found
    finally:
        for k, v in saved.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


def _parses(path: Path) -> bool:
    """The engine's loaders start over on a bad JSON file without a word, which
    would make the preview quietly wrong; a bad copy is rebuilt instead."""
    if path.suffix != ".json":
        return True
    try:
        json.loads(path.read_text(encoding="utf-8"))
        return True
    except (OSError, ValueError):
        return False


def seed_ownership(ledger: OwnershipLedger, positions: Dict[str, dict],
                   strategies) -> List[str]:
    """No ledger: own what the admitted sleeves can hold, as ENGINE (how the
    live engine registers its orders). Returns the symbols seeded."""
    universe = set()
    for sid in M.ADMITTED_SLEEVES:
        universe.update(strategies[sid].universe())
    owned = []
    for sym, p in sorted(positions.items()):
        if sym not in universe:
            continue
        coid = f"PREVIEW_{sym}"
        qty = abs(float(p.get("qty", 0.0)))
        ledger.register_order("ENGINE", sym, "buy", qty, coid,
                              notional=abs(float(p.get("market_value", 0.0))))
        ledger.update_status(coid, "filled", fill_price=p.get("avg_entry_price"),
                             fill_qty=qty)
        owned.append(sym)
    return owned


def replay_states(strategies, dataset, as_of,
                  sessions: int = REPLAY_SESSIONS) -> Dict[str, dict]:
    """Strategy states as a service that cycled every session holds them going
    into the cycle on `as_of`: each admitted strategy decides on each of the
    `sessions` closes before it, starting from its initial state."""
    states = {sid: strategies[sid].initial_state() for sid in M.ADMITTED_SLEEVES}
    for day in [d for d in calendar.trading_days(dataset) if d < as_of][-sessions:]:
        view = MarketView(dataset, day)
        for sid in M.ADMITTED_SLEEVES:
            strategies[sid].decide(view, states[sid])
    return states


def run_preview(broker, cfg, strategies, found: List[str]) -> Tuple[dict, List[str]]:
    """One read-only cycle on the scratch state; call inside isolated(). Rebuilds
    what the copy lacks, then runs the engine's own run_cycle with preview=True.
    Returns (cycle summary, notes on state that was rebuilt)."""
    sd = state_dir()
    have = {n for n in found if _parses(sd / n)}
    notes = [f"{n} did not parse; rebuilt" for n in found if n not in have]

    ledger = (OwnershipLedger.load(sd / M._LEDGER_FILE) if M._LEDGER_FILE in have
              else OwnershipLedger())
    ledger.prune_terminal(max_age_days=7)
    if M._LEDGER_FILE not in have:
        owned = seed_ownership(ledger, broker.get_positions(), strategies)
        notes.append("no ledger; positions treated as ENGINE-owned: "
                     + (", ".join(owned) or "none"))
    if M._STATE_FILE not in have:
        dataset = cache.load_dataset()
        as_of = calendar.trading_days(dataset)[-1]
        states = replay_states(strategies, dataset, as_of)
        M._save_states(states)
        rot = states.get("ROTATION", {})
        notes.append(f"no strategy states; replayed the {REPLAY_SESSIONS} sessions before "
                     f"{as_of.date()} (ROTATION reviewed {rot.get('last_rebal_month')}, holds "
                     f"{' + '.join(sorted(rot.get('holdings', {}))) or 'nothing'})")
    token = getattr(cfg, "rebalance_now_token", "")
    if token and M._REBALANCE_TOKEN_FILE not in have:
        notes.append(f"no rebalance token file; one-time rebalance {token} counts as "
                     f"not yet applied")
    if os.getenv("HORIZON_SYMBOL_EQUIVALENTS", "0") != "1":
        notes.append("HORIZON_SYMBOL_EQUIVALENTS is not 1, so the plan uses backtest "
                     "tickers (QQQ, ...), not the live ones (QQQM, ...). Run under "
                     "`railway run -s horizon-live` for the service's settings.")

    alerter = Alerter()
    alerter.enabled = False          # isolated() already turned alerts off
    summary = M.run_cycle(broker, strategies, cfg, ledger,
                          KillSwitch(flag_path=sd / M._HALT_FILE), alerter,
                          dry_run=True, preview=True)
    return summary, notes


def format_plan(summary: dict, positions: Dict[str, dict], token: str) -> List[str]:
    """The decision, the orders, and the book before and after them."""
    if summary.get("status") == "refused-stale":
        return [f"  No plan: market data as_of {summary['as_of']} is {summary['stale_days']} "
                f"days old (limit {M.MAX_STALE_DAYS}); the live cycle would refuse too."]
    rule = (f"one-time rebalance {token}, band bypassed" if summary["forced"]
            else f"band {summary['band']:.0%}")
    orders = summary["orders"]
    out = [f"  as_of {summary['as_of']} · {summary['regime']} {summary['regime_score']:.0f} · "
           f"PULSE {summary['pulse_leverage']:.2f}x · "
           f"ROTATION {' + '.join(summary['rotation']) or 'T-bills'}",
           f"  equity ${summary['equity']:,.2f} · largest gap {summary['weight_gap']:.1%} · {rule}",
           f"  orders: {len(orders)}" + (f" ({summary['trade_reason']})"
                                         if summary["trade_reason"] else "")]
    out += [f"    {o['side']:<4} {o['symbol']:<5} ~${o['notional']:,.0f}" for o in orders]
    now = {s: float(p.get("market_value", 0.0)) for s, p in positions.items()}
    after = dict(now)
    for o in orders:
        after[o["symbol"]] = (after.get(o["symbol"], 0.0)
                              + (o["notional"] if o["side"] == "buy" else -o["notional"]))
    book = [f"{s} ${now.get(s, 0.0):,.0f}"
            + (f" -> ${v:,.0f}" if abs(v - now.get(s, 0.0)) >= 1.0 else "")
            for s, v in sorted(after.items()) if max(abs(now.get(s, 0.0)), abs(v)) >= DUST_USD]
    eq = summary["equity"] or 1.0
    ndx = lambda b: sum(v * R.NASDAQ_WEIGHTS.get(s, 0.0) for s, v in b.items()) / eq
    out += ["  book: " + (" · ".join(book) or "empty"),
            f"  Nasdaq-100 exposure {ndx(now):.2f}x -> {ndx(after):.2f}x"]
    return out


def main(state_from: Optional[str] = None) -> None:
    """--preview: one read-only cycle against the live account, printed."""
    source = source_state_dir(state_from)
    if state_from and not source.is_dir():
        raise SystemExit(f"--state-from {source}: no such directory")
    with isolated(source) as (root, found):
        logging.basicConfig(
            level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
            handlers=[logging.StreamHandler(),
                      logging.FileHandler(log_dir() / "preview.log", encoding="utf-8")])
        log.info("PREVIEW: read-only cycle against the live account; state from %s (%s), "
                 "working copy %s", source, ", ".join(found) or "none found", root)
        try:
            broker = create_preview_broker_from_env()
            account = broker.get_account()
        except Exception as exc:
            log.critical("PREVIEW: cannot read the live account (%s); no plan", exc)
            raise SystemExit(2)
        log.info("live account: paper=%s, equity $%.2f, cash $%.2f, status %s",
                 broker.paper, account["equity"], account["cash"], account["status"])
        cfg, strategies = build_default_config(), build_all()
        summary, notes = run_preview(broker, cfg, strategies, found)
        lines = ["", "Horizon PREVIEW: the next cycle against the live account. "
                     "Read-only: nothing was submitted."]
        lines += format_plan(summary, broker.get_positions(), cfg.rebalance_now_token)
        lines += [f"  warning: {w}" for w in summary.get("warnings") or []
                  if not w.startswith("PREVIEW:")]
        lines += [f"  state: {n}" for n in notes]
        lines.append(f"  state read from {source} ({', '.join(found) or 'nothing there'}); "
                     f"the cycle wrote only to {root}")
        print("\n".join(lines))
