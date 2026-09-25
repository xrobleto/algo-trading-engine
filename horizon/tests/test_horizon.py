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


def test_pending_order_awareness():
    """The order-diff must subtract pending exposure to avoid doubling.

    Catches the day-1 regression: an after-hours --once --live submission
    queued for the next open was not visible to the next --daily cycle
    (positions=0, pending invisible), so the cycle re-submitted identical
    orders and the book doubled.
    """
    from ..engine.main import _pending_notional

    class _OrderBroker:
        def get_open_orders(self):
            return [
                {"symbol": "QQQ", "side": "buy", "qty": 10.0, "filled_qty": 0.0},
                {"symbol": "DBC", "side": "buy", "qty": 100.0, "filled_qty": 25.0},
                {"symbol": "GLD", "side": "sell", "qty": 5.0, "filled_qty": 0.0},
            ]

    class _View:
        def is_tradable(self, sym): return True
        def close(self, sym):
            return {"QQQ": 700.0, "DBC": 31.0, "GLD": 200.0}[sym]

    pending = _pending_notional(_OrderBroker(), _View())
    assert pending["QQQ"] == 7000.0, "buy 10 @ 700 should be +7000"
    assert pending["DBC"] == 75.0 * 31.0, \
        "partial fill should count only the unfilled remainder"
    assert pending["GLD"] == -1000.0, "sell 5 @ 200 should be -1000"

    # No broker / broker failure must degrade gracefully (return {}).
    assert _pending_notional(None, _View()) == {}

    class _Failing:
        def get_open_orders(self): raise RuntimeError("broker down")

    assert _pending_notional(_Failing(), _View()) == {}


def test_cache_freshness_is_evaluated_per_call():
    """G0 (2026-09-05 incident): a cache that was current when the container
    started must NOT be treated as fresh weeks later, and a frame ending on the
    last completed session must be."""
    from datetime import datetime
    from zoneinfo import ZoneInfo
    et = ZoneInfo("America/New_York")
    # Friday 2026-09-04 was the last session; Monday 09:00 ET -> Friday is current.
    assert cache.completed_through(datetime(2026, 9, 7, 9, 0, tzinfo=et)) == pd.Timestamp("2026-09-04")
    # Same Monday after the close -> Monday itself is complete.
    assert cache.completed_through(datetime(2026, 9, 7, 16, 30, tzinfo=et)) == pd.Timestamp("2026-09-07")
    # Saturday -> Friday.
    assert cache.completed_through(datetime(2026, 9, 5, 12, 0, tzinfo=et)) == pd.Timestamp("2026-09-04")
    idx_ok = pd.bdate_range("2026-08-01", "2026-09-04")
    idx_stale = pd.bdate_range("2026-08-01", "2026-08-24")   # the frozen container's cache
    now = datetime(2026, 9, 4, 9, 0, tzinfo=et)              # Sep-4 09:00 ET cycle
    assert cache.is_fresh(pd.DataFrame({"close": 1.0}, index=idx_ok[:-1]), now)   # through Sep-3
    assert not cache.is_fresh(pd.DataFrame({"close": 1.0}, index=idx_stale), now)
    assert not cache.is_fresh(pd.DataFrame(), now)
    # The old rule (within 7 days of a frozen end-date) would have passed the
    # stale frame — make sure nobody reintroduces a module-level end date.
    assert not hasattr(cache, "FETCH_END")


def test_stale_cycle_is_refused():
    """The engine refuses to trade when the dataset is older than the holiday
    tolerance, and accepts a Friday bar on the Tuesday after a Monday holiday."""
    from datetime import datetime
    from zoneinfo import ZoneInfo
    from ..engine import main as engine_main
    et = ZoneInfo("America/New_York")
    tue = datetime(2026, 9, 8, 9, 0, tzinfo=et)      # Tuesday after Labor Day
    assert engine_main.data_staleness_days(pd.Timestamp("2026-09-04"), tue) == 3   # Mon holiday not modeled -> Mon counts
    assert engine_main.data_staleness_days(pd.Timestamp("2026-09-04"), tue) <= engine_main.MAX_STALE_DAYS
    sep4 = datetime(2026, 9, 4, 9, 0, tzinfo=et)
    assert engine_main.data_staleness_days(pd.Timestamp("2026-08-24"), sep4) > engine_main.MAX_STALE_DAYS


def test_funding_guard_scales_to_account_capacity():
    """A multiplier-1 account cannot hold a 1.76x vol-targeted book; the guard
    scales targets to cash + managed positions, ignores orphans, and is a no-op
    when the book is fundable or when margin (multiplier 2) covers it."""
    from ..engine.main import apply_funding_guard
    positions = {"QQQM": {"market_value": 4900.0}, "PDBC": {"market_value": 900.0},
                 "XLK": {"market_value": 0.01}}
    target = {"QQQM": 7844.0, "PDBC": 916.0}
    scaled, scale, fundable = apply_funding_guard(target, positions, {"XLK"},
                                                  cash=1000.0, multiplier=1.0, buffer=0.03)
    assert abs(fundable - 6800.0 * 0.97) < 1e-6
    assert abs(sum(scaled.values()) - fundable) < 1e-6 and 0 < scale < 1
    assert abs(scaled["QQQM"] / scaled["PDBC"] - 7844.0 / 916.0) < 1e-6   # proportional
    # Margin account: 2x capacity covers the book untouched.
    same, scale2, _ = apply_funding_guard(target, positions, {"XLK"}, 1000.0, 2.0)
    assert scale2 == 1.0 and same == target
    # Already fundable: untouched.
    same, scale3, _ = apply_funding_guard({"QQQM": 3000.0}, positions, set(), 1000.0, 1.0)
    assert scale3 == 1.0


def test_gross_ceiling():
    from ..engine.main import apply_gross_ceiling
    t, sc = apply_gross_ceiling({"QQQM": 7844.0, "PDBC": 916.0}, 5775.0)
    assert abs(sum(t.values()) - 5775.0) < 1e-6 and abs(sc - 5775.0 / 8760.0) < 1e-9
    same, sc0 = apply_gross_ceiling({"QQQM": 3000.0}, 5775.0)
    assert sc0 == 1.0 and same == {"QQQM": 3000.0}
    same, sc1 = apply_gross_ceiling({"QQQM": 9000.0}, 0.0)   # 0 = no ceiling
    assert sc1 == 1.0


def test_buys_sized_to_cash():
    """Cash-account safety: buys that exceed available cash are scaled
    proportionally (1% buffer); buys that fit are untouched."""
    from ..engine.main import size_buys_to_cash
    buys = [{"symbol": "QLD", "side": "buy", "qty": 40.0, "notional": 3424.0},
            {"symbol": "IEFA", "side": "buy", "qty": 12.0, "notional": 1051.0}]
    sized = size_buys_to_cash(buys, cash=1000.0)
    assert abs(sum(o["notional"] for o in sized) - 990.0) < 1e-6
    assert abs(sized[0]["notional"] / sized[1]["notional"] - 3424.0 / 1051.0) < 1e-6
    assert sized[0]["qty"] < 40.0
    same = size_buys_to_cash(buys, cash=10_000.0)
    assert [o["notional"] for o in same] == [3424.0, 1051.0]


def test_plan_orders_band_and_override():
    """The no-trade band holds small drift, triggers on a large gap, lets a
    material signal change (full exit >= override) through, and still sells a
    small full exit completely once triggered."""
    from ..engine.main import plan_orders
    px = {"QQQM": 300.0, "QLD": 90.0, "IEFA": 100.0, "PDBC": 19.0}
    eq = 10_000.0
    held = {"QQQM": 3000.0, "QLD": 3000.0, "IEFA": 1700.0, "PDBC": 1700.0}
    # 1) drift of 5% of equity, band 20% -> hold
    tgt = {"QQQM": 3500.0, "QLD": 2500.0, "IEFA": 1700.0, "PDBC": 1700.0}
    o, hit, gap = plan_orders(tgt, held, {}, px, set(), eq, band=0.20, min_trade_frac=0.005)
    assert o == [] and not hit and abs(gap - 0.05) < 1e-9
    # 2) band 0 re-pins the same drift (pre-2026-09-25 behavior)
    o, hit, _ = plan_orders(tgt, held, {}, px, set(), eq, band=0.0)
    assert hit and {x["symbol"] for x in o} == {"QQQM", "QLD"}
    # 3) a ROTATION swap (17% exit, 17% entry) is below the band ...
    swap = {"QQQM": 3000.0, "QLD": 3000.0, "IEFA": 1700.0, "VGLT": 1700.0}
    o, hit, _ = plan_orders(swap, held, {}, {**px, "VGLT": 60.0}, set(), eq, band=0.20)
    assert o == [] and not hit
    # ... but the signal override lets it through, both legs
    o, hit, _ = plan_orders(swap, held, {}, {**px, "VGLT": 60.0}, set(), eq, band=0.20,
                            min_trade_frac=0.005, signal_override=0.10)
    assert hit and {(x["symbol"], x["side"]) for x in o} == {("PDBC", "sell"), ("VGLT", "buy")}
    # 4) once triggered, a $30 full exit still trades despite the 0.5% min trade
    held2 = {**held, "IAU": 30.0}
    big = {"QQQM": 6000.0, "QLD": 0.0, "IEFA": 1700.0, "PDBC": 1700.0}
    o, hit, _ = plan_orders(big, held2, {}, {**px, "IAU": 50.0}, set(), eq, band=0.20,
                            min_trade_frac=0.005)
    assert hit and ("IAU", "sell") in {(x["symbol"], x["side"]) for x in o}
    # 5) orphans are never traded
    o, _, _ = plan_orders(big, held2, {}, {**px, "IAU": 50.0}, {"IAU"}, eq, band=0.20)
    assert "IAU" not in {x["symbol"] for x in o}


def test_daily_action_timing():
    """Never cycle before 09:00 ET; record weekends and holidays so a restart
    cannot spin the loop; cycle once per session day."""
    from datetime import datetime
    from zoneinfo import ZoneInfo
    from ..engine.main import daily_action
    et = ZoneInfo("America/New_York")
    thu_0006 = datetime(2026, 9, 25, 0, 6, tzinfo=et)
    thu_0901 = datetime(2026, 9, 25, 9, 1, tzinfo=et)
    sat_1000 = datetime(2026, 9, 26, 10, 0, tzinfo=et)
    assert daily_action(thu_0006, "2026-09-24", True) == "sleep"     # the 00:06 restart case
    assert daily_action(thu_0901, "2026-09-24", True) == "cycle"
    assert daily_action(thu_0901, "2026-09-25", True) == "sleep"     # already ran today
    assert daily_action(sat_1000, "2026-09-25", False) == "record"   # weekend: no spin
    assert daily_action(sat_1000, "2026-09-26", False) == "sleep"


def test_tax_model_hand_cases():
    """FIFO, ST/LT split and the share-matched wash-sale rule on hand-computed
    cases (backtest/tax.py)."""
    from ..backtest.tax import TaxRates, compute_taxes
    R, T = TaxRates(0.32, 0.15), pd.Timestamp
    buy = lambda d, n, p: {"date": T(d), "symbol": "X", "side": "buy", "shares": n, "price": p}
    sell = lambda d, n, p: {"date": T(d), "symbol": "X", "side": "sell", "shares": n, "price": p}
    r = compute_taxes([buy("2020-01-02", 10, 100), sell("2020-03-02", 10, 110)], [], {}, T("2020-12-31"), R)
    assert abs(r.by_year[2020] - 32.0) < 1e-9                       # $100 short-term
    r = compute_taxes([buy("2019-01-02", 10, 100), sell("2020-03-02", 10, 110)], [], {}, T("2020-12-31"), R)
    assert abs(r.by_year[2020] - 15.0) < 1e-9                       # $100 long-term
    tr = [buy("2020-01-02", 10, 100), sell("2020-02-03", 10, 95), buy("2020-02-10", 2, 95)]
    r = compute_taxes(tr, [], {"X": 95.0}, T("2020-12-31"), R)
    assert abs(r.wash_disallowed - 10.0) < 1e-9                     # only 2 replacement shares
    r = compute_taxes(tr + [sell("2020-06-01", 2, 100)], [], {}, T("2020-12-31"), R)
    assert abs(r.by_year[2020]) < 1e-9                              # washed loss moved into basis


def test_rotation_hold_buffer():
    """retain_rank keeps a holding that slipped to #3; default drops it."""
    from ..strategies.rotation import RotationStrategy, RISK_ASSETS, CASH_ASSET
    a, b, c, d, e = RISK_ASSETS[:5]
    scores = {a: 0.30, b: 0.20, c: 0.10, d: 0.05, e: -0.10, CASH_ASSET: 0.0}

    class _View:
        as_of = pd.Timestamp("2026-10-01")
        def is_tradable(self, sym): return True

    def stub(strat):
        strat._blended_momentum = lambda view, sym: scores[sym]
        return strat
    held = {"last_rebal_month": "2026-09", "holdings": {a: 0.5, c: 0.5}}
    # c ranks #3: the original rule swaps it for b ...
    got = stub(RotationStrategy()).decide(_View(), dict(held)).target_weights
    assert set(got) == {a, b}
    # ... a buffer of 3 keeps it; a buffer of 2 (= top_n) is the original rule.
    got = stub(RotationStrategy(retain_rank=3)).decide(_View(), dict(held)).target_weights
    assert set(got) == {a, c}
    got = stub(RotationStrategy(retain_rank=2)).decide(_View(), dict(held)).target_weights
    assert set(got) == {a, b}
    # A holding that falls out of the top 3 is dropped even with the buffer.
    held2 = {"last_rebal_month": "2026-09", "holdings": {a: 0.5, d: 0.5}}
    got = stub(RotationStrategy(retain_rank=3)).decide(_View(), dict(held2)).target_weights
    assert set(got) == {a, b}


def test_holiday_cycle_is_skipped():
    """Weekends never cycle; a weekday the broker calendar marks as closed is
    skipped; a calendar failure falls back to 'weekday = session'."""
    from datetime import date
    from ..engine.main import is_session_day

    class Cal:
        def __init__(self, open_days, fail=False):
            self.open_days, self.fail = open_days, fail
        def is_trading_day(self, d):
            if self.fail:
                raise RuntimeError("calendar down")
            return d in self.open_days

    labor_day, tue, sat = date(2026, 9, 7), date(2026, 9, 8), date(2026, 9, 5)
    cal = Cal({tue})
    assert not is_session_day(labor_day, cal)
    assert is_session_day(tue, cal)
    assert not is_session_day(sat, cal)
    assert is_session_day(labor_day, Cal(set(), fail=True))   # fail-open on weekdays
    assert is_session_day(tue, None)                          # no broker: weekday cycles


def test_pulse_levered_etf_expression():
    """leverage_via='levered_etf' expresses L>1 as a QQQ/QLD mix summing to 1.0
    (no borrowing) and leaves L<=1 identical to the margin expression."""
    from ..strategies.pulse import PulseStrategy, LEVERED_ASSET, RISK_ASSET
    ds = cache.load_dataset([RISK_ASSET, LEVERED_ASSET, "BIL"])
    view = MarketView(ds, list(ds[RISK_ASSET].index)[-1])
    margin = PulseStrategy().decide(view, {})
    etf = PulseStrategy(leverage_via="levered_etf").decide(view, {})
    L = margin.target_weights[RISK_ASSET]
    if L > 1.0:
        assert abs(sum(etf.target_weights.values()) - 1.0) < 1e-6
        assert abs(etf.target_weights[LEVERED_ASSET] - (L - 1.0)) < 1e-3
    else:
        assert etf.target_weights == margin.target_weights
    assert LEVERED_ASSET in PulseStrategy(leverage_via="levered_etf").universe()
    assert LEVERED_ASSET not in PulseStrategy().universe()


def main() -> int:
    tests = [test_cache_freshness_is_evaluated_per_call, test_stale_cycle_is_refused,
             test_funding_guard_scales_to_account_capacity, test_gross_ceiling,
             test_buys_sized_to_cash, test_holiday_cycle_is_skipped,
             test_plan_orders_band_and_override, test_daily_action_timing,
             test_tax_model_hand_cases, test_rotation_hold_buffer,
             test_pulse_levered_etf_expression,
             test_no_lookahead, test_single_source_of_truth,
             test_strategies_decide_cleanly, test_risk_overlay_recovers,
             test_ledger_roundtrip, test_harness_smoke,
             test_reconciler_orphan_recovery, test_alerter_never_raises,
             test_emergency_flatten_requires_broker,
             test_pending_order_awareness]
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
