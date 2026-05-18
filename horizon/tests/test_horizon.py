"""Horizon test suite — the invariants the brief mandates.

Run:  python -m horizon.tests.test_horizon
Each test is a plain function; failures raise AssertionError. No pytest needed.
"""

from __future__ import annotations

import sys
import tempfile
from pathlib import Path

import numpy as np
import pandas as pd

from ..backtest.harness import run_strategy
from ..data import cache
from ..engine import risk
from ..engine.ledger import OwnershipLedger
from ..strategies import registry
from ..strategies.base import MarketView


def test_no_lookahead():
    """A MarketView must never expose a bar dated after its as-of date."""
    ds = cache.load_dataset(["SPY", "QQQ"])
    days = list(ds["SPY"].index)
    mid = days[len(days) // 2]
    view = MarketView(ds, mid)
    assert view.history("SPY").index.max() <= mid, "history leaked future bar"
    assert view.closes("QQQ").index.max() <= mid, "closes leaked future bar"
    assert view.tr_closes("SPY").index.max() <= mid, "tr_closes leaked future"
    # A view as-of an early date sees strictly fewer bars than a later one.
    early = MarketView(ds, days[100])
    assert len(early.history("SPY")) < len(view.history("SPY"))


def test_single_source_of_truth():
    """The engine and the harness must run the identical strategy objects."""
    from ..engine import main as engine_main
    # The live engine builds strategies from the same registry the harness uses.
    assert engine_main.build_all is registry.build_all
    pulse = registry.build("PULSE")
    assert type(pulse).__module__ == "horizon.strategies.pulse"
    # decide() is defined in the strategy module, not reimplemented anywhere.
    assert pulse.decide.__qualname__.startswith("PulseStrategy")


def test_strategies_decide_cleanly():
    """Every strategy's decide() runs without error and returns valid weights."""
    ds = cache.load_dataset()
    view = MarketView(ds, list(ds["SPY"].index)[-1])
    for sid, strat in registry.build_all().items():
        dec = strat.decide(view, strat.initial_state())
        assert isinstance(dec.target_weights, dict)
        for sym, w in dec.target_weights.items():
            assert sym in strat.universe(), f"{sid} targeted off-universe {sym}"
            assert -3.0 < w < 3.0, f"{sid} produced an absurd weight {w}"


def test_risk_overlay_recovers():
    """The risk overlay must have a guaranteed recovery path (no death-spiral)."""
    assert risk.recovery_is_guaranteed(), "risk overlay has no recovery path"
    eq = pd.Series(np.linspace(100.0, 45.0, 300))   # deep monotone drawdown
    exp = risk.exposure_from_drawdown(eq)
    assert exp.min() >= risk.EXPOSURE_FLOOR - 1e-9, "exposure breached its floor"
    assert exp.max() <= 1.0 + 1e-9, "exposure exceeded 1.0"
    # A flat (no-drawdown) curve must keep exposure at full.
    flat = pd.Series(np.full(300, 100.0))
    assert risk.exposure_from_drawdown(flat).iloc[-1] == 1.0


def test_ledger_roundtrip():
    """The ownership ledger must survive a save/load cycle intact."""
    led = OwnershipLedger()
    led.register_order("PULSE", "QQQ", "buy", 10.0, "HZN_PULSE_QQQ_1",
                       notional=5000.0)
    led.update_status("HZN_PULSE_QQQ_1", "filled", fill_price=500.0,
                      fill_qty=10.0)
    with tempfile.TemporaryDirectory() as d:
        path = Path(d) / "ledger.json"
        led.save(path)
        reloaded = OwnershipLedger.load(path)
    e = reloaded.entries["HZN_PULSE_QQQ_1"]
    assert e.status == "filled" and e.fill_price == 500.0
    assert reloaded.deployed_notional("PULSE") == 5000.0


def test_harness_smoke():
    """The harness produces a sane, all-positive equity curve."""
    ds = cache.load_dataset()
    res = run_strategy(registry.build("PULSE"), ds, "2024-01-02", "2024-12-31",
                       100_000.0)
    assert len(res.equity) > 200, "too few daily equity points"
    assert (res.equity > 0).all(), "equity went non-positive"
    assert res.trades, "no trades executed"


class _StubBroker:
    """Minimal broker stub for reconciler tests — no network."""

    def __init__(self, positions):
        self._positions = positions

    def get_positions(self):
        return self._positions

    def get_order_by_client_id(self, client_order_id):
        return None


def test_reconciler_orphan_recovery():
    """An unrecognized broker position is adopted as a tracked UNMANAGED entry."""
    from ..engine.reconciler import UNMANAGED, reconcile
    led = OwnershipLedger()
    broker = _StubBroker({"NVDA": {"symbol": "NVDA", "qty": 5.0,
                                   "market_value": 900.0,
                                   "avg_entry_price": 180.0}})
    res = reconcile(broker, led)
    assert "NVDA" in res.orphan_symbols, "orphan not detected"
    assert led.get_owner("NVDA") == UNMANAGED, "orphan not adopted"
    reconcile(broker, led)  # a second pass must not duplicate
    active = [e for e in led.entries.values()
              if e.symbol == "NVDA" and e.is_active]
    assert len(active) == 1, "orphan recovery duplicated an entry"


def test_alerter_never_raises():
    """Alerting must never raise — a failed alert cannot crash the engine."""
    from ..engine.alerts import Alerter
    a = Alerter()
    a.enabled = False  # force log-only regardless of local SMTP config
    a.send("subject", "body", level="CRITICAL")
    a.warning("w")
    a.critical("c")
    a.heartbeat({"regime": "RISK_ON", "equity": 7400})
    a.heartbeat({"regime": "RISK_ON", "equity": 7400})  # once-per-day no-op


def test_emergency_flatten_requires_broker():
    """Emergency flatten must refuse cleanly when there is no broker."""
    from ..engine.alerts import Alerter
    from ..engine.killswitch import KillSwitch
    from ..engine.main import emergency_flatten
    alerter = Alerter()
    alerter.enabled = False
    try:
        emergency_flatten(None, KillSwitch(), alerter)
    except RuntimeError:
        return
    raise AssertionError("emergency_flatten should refuse without a broker")


def main() -> int:
    tests = [test_no_lookahead, test_single_source_of_truth,
             test_strategies_decide_cleanly, test_risk_overlay_recovers,
             test_ledger_roundtrip, test_harness_smoke,
             test_reconciler_orphan_recovery, test_alerter_never_raises,
             test_emergency_flatten_requires_broker]
    failed = 0
    for t in tests:
        try:
            t()
            print(f"PASS  {t.__name__}")
        except Exception as exc:  # noqa: BLE001
            failed += 1
            print(f"FAIL  {t.__name__}: {exc}")
    print(f"\n{len(tests) - failed}/{len(tests)} passed")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
