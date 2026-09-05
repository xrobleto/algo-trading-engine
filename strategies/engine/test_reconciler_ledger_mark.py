"""
Regression tests — the ownership ledger must stay marked to broker truth.

The bug: reconciler Step 4 was create-only. Once a symbol had a ledger entry,
every later reconcile hit `if symbol in existing_symbols: continue` and left the
recorded dollars frozen at whatever they were when the entry was born. Nothing
ever re-read the broker for a position it already knew about.

That is survivable for a sleeve trading its own book (its adapter registers a
fresh entry per order, so new dollars arrive as new entries) and wrong for one
whose positions are grown by a SEPARATE service. HORIZON is that case:
horizon-live scaled QQQM+PDBC from ~$1,250 to ~$5,748 across the 2026-08-24 and
2026-08-25 cycles, while this engine's heartbeat went on printing the
adoption-time `HORIZON: $1,272` — understating deployment by ~$4.5k and
overstating free sleeve capital by the same amount. It surfaced as cosmetic
(every homegrown sleeve is parked at 0%, so nothing consumed the number) but
`get_deployed_notional()` feeds `SleeveManager.can_open()`: a sleeve revived
against those figures would size against capital that was already spent.

The fix: `_remark_to_broker()` re-marks tracked positions on every reconcile,
into a new `OwnershipEntry.market_value` field. It is deliberately NOT folded
into `notional_at_entry` — that field is cost basis and callers read it as such
(`trend_adapter.py` recovers an entry price via `notional_at_entry / qty`).
`OwnershipEntry.exposure` prefers the mark and falls back to cost basis.

These tests pin down:
  1. A position grown by a foreign service is re-marked (the live HORIZON bug).
  2. Cost-basis fields survive the re-mark untouched.
  3. Multi-entry scale-ins split the mark proportionally, summing to the broker.
  4. Shrinking positions mark down too.
  5. Unmarked / legacy-JSON entries fall back to cost basis.
  6. The pending-order guard is not regressed into duplicate entries.
  7. The consequence that matters: sleeve availability tracks reality.

Run:
    python strategies/engine/test_reconciler_ledger_mark.py
    pytest strategies/engine/test_reconciler_ledger_mark.py
"""

from __future__ import annotations

import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))
_STRATEGIES_DIR = _REPO_ROOT / "strategies"
if str(_STRATEGIES_DIR) not in sys.path:
    sys.path.insert(0, str(_STRATEGIES_DIR))

from engine.config import build_default_config  # noqa: E402
from engine.ownership import OwnershipEntry, OwnershipLedger  # noqa: E402
from engine.reconciler import reconcile  # noqa: E402
from engine.sleeves import SleeveManager  # noqa: E402


class FakeBroker:
    """Stands in for BrokerFacade. Only implements what reconcile() calls."""

    def __init__(self, positions=None, open_orders=None, orders_by_coid=None):
        self._positions = positions or []
        self._open_orders = open_orders or []
        self._orders_by_coid = orders_by_coid or {}

    def get_all_positions(self):
        return list(self._positions)

    def get_all_open_orders(self):
        return list(self._open_orders)

    def get_order_by_client_id(self, coid):
        return self._orders_by_coid.get(coid)


def _position(symbol, qty, market_value, avg_entry_price):
    """Shaped like BrokerFacade.get_all_positions(), which casts every numeric
    field to float (broker.py:68) — the raw Alpaca SDK hands back strings."""
    return {
        "symbol": symbol,
        "qty": float(qty),
        "side": "long",
        "market_value": float(market_value),
        "avg_entry_price": float(avg_entry_price),
    }


def _adopted(ledger, symbol, qty, notional, avg_price, coid=None, strategy_id="HORIZON"):
    """A position adopted by a past reconcile — exactly what horizon-live's
    positions look like in this engine's ledger."""
    coid = coid or f"RECONCILE_{strategy_id}_{symbol}_2026-08-12T13:30:00Z"
    ledger.register_order(
        strategy_id=strategy_id, symbol=symbol, side="buy", qty=qty,
        client_order_id=coid, notional=notional,
    )
    ledger.update_status(coid, "filled", fill_price=avg_price)
    return coid


# ---------------------------------------------------------------------------
# Core regression — the live HORIZON bug
# ---------------------------------------------------------------------------

def test_foreign_service_scale_up_is_remarked() -> None:
    """QQQM adopted at ~$1,036; horizon-live then scales it to $4,836.58 in its
    own service. The engine must report the new number, not the adoption one."""
    config = build_default_config()
    ledger = OwnershipLedger()
    _adopted(ledger, "QQQM", qty=3.5484, notional=1036.00, avg_price=291.96)

    assert abs(ledger.get_deployed_notional("HORIZON") - 1036.00) < 1e-6

    broker = FakeBroker(positions=[
        _position("QQQM", "16.5438", "4836.58", "293.799404"),
    ])
    reconcile(broker, ledger, config)

    deployed = ledger.get_deployed_notional("HORIZON")
    assert abs(deployed - 4836.58) < 0.01, f"expected 4836.58, got {deployed}"
    # And no duplicate entry was invented for a symbol we already tracked.
    assert len(ledger.get_filled_entries("HORIZON")) == 1
    print("PASS: test_foreign_service_scale_up_is_remarked")


def test_remark_preserves_cost_basis_fields() -> None:
    """`notional_at_entry` and `fill_price` are cost basis. trend_adapter.py
    recovers an entry price via `notional_at_entry / qty`, so a re-mark that
    overwrote them would silently corrupt exit accounting."""
    config = build_default_config()
    ledger = OwnershipLedger()
    coid = _adopted(ledger, "QQQM", qty=3.5484, notional=1036.00, avg_price=291.96)

    broker = FakeBroker(positions=[_position("QQQM", "16.5438", "4836.58", "293.799404")])
    reconcile(broker, ledger, config)

    entry = ledger.entries[coid]
    assert abs(entry.notional_at_entry - 1036.00) < 1e-6, entry.notional_at_entry
    assert abs(entry.fill_price - 291.96) < 1e-6, entry.fill_price
    assert abs(entry.qty - 3.5484) < 1e-6, entry.qty
    # Derived entry price stays the cost basis it always was.
    assert abs((entry.notional_at_entry / entry.qty) - 291.96) < 0.01
    # Exposure, separately, is the mark.
    assert abs(entry.exposure - 4836.58) < 0.01
    print("PASS: test_remark_preserves_cost_basis_fields")


def test_remark_splits_proportionally_across_scale_in_entries() -> None:
    """A symbol may hold several ledger entries (one per scale-in order). The
    mark splits across them by current weight and sums to the broker."""
    config = build_default_config()
    ledger = OwnershipLedger()
    a = _adopted(ledger, "QQQM", qty=1.0, notional=1000.00, avg_price=290.0,
                 coid="HZN_QQQM_a")
    b = _adopted(ledger, "QQQM", qty=3.0, notional=3000.00, avg_price=295.0,
                 coid="HZN_QQQM_b")

    broker = FakeBroker(positions=[_position("QQQM", "8.0", "8000.00", "292.5")])
    reconcile(broker, ledger, config)

    ea, eb = ledger.entries[a], ledger.entries[b]
    assert abs(ea.exposure - 2000.00) < 0.01, ea.exposure
    assert abs(eb.exposure - 6000.00) < 0.01, eb.exposure
    assert abs(ledger.get_deployed_notional("HORIZON") - 8000.00) < 0.01
    print("PASS: test_remark_splits_proportionally_across_scale_in_entries")


def test_remark_handles_shrinking_position() -> None:
    """The foreign service can trim as well as add — marks must move down."""
    config = build_default_config()
    ledger = OwnershipLedger()
    _adopted(ledger, "PDBC", qty=50.0, notional=5000.00, avg_price=18.5)

    broker = FakeBroker(positions=[_position("PDBC", "12.0", "1200.00", "18.5")])
    reconcile(broker, ledger, config)

    deployed = ledger.get_deployed_notional("HORIZON")
    assert abs(deployed - 1200.00) < 0.01, f"expected 1200.00, got {deployed}"
    print("PASS: test_remark_handles_shrinking_position")


def test_remark_skipped_below_tolerance() -> None:
    """Sub-dollar drift is not worth a ledger write or a log line."""
    config = build_default_config()
    ledger = OwnershipLedger()
    coid = _adopted(ledger, "QQQM", qty=3.5, notional=1000.50, avg_price=285.86)

    broker = FakeBroker(positions=[_position("QQQM", "3.5", "1000.90", "285.86")])
    reconcile(broker, ledger, config)

    assert ledger.entries[coid].market_value is None, "should not have been marked"
    assert abs(ledger.get_deployed_notional("HORIZON") - 1000.50) < 1e-6
    print("PASS: test_remark_skipped_below_tolerance")


# ---------------------------------------------------------------------------
# Fallback / compatibility
# ---------------------------------------------------------------------------

def test_exposure_falls_back_to_cost_basis_when_never_marked() -> None:
    """Before any reconcile has observed a position, exposure is entry cost —
    the pre-fix behavior, unchanged."""
    entry = OwnershipEntry(
        strategy_id="TREND", symbol="XBI", side="buy", qty=3.0,
        client_order_id="ENG_TREND_XBI_1", status="filled", notional_at_entry=400.0,
    )
    assert entry.market_value is None
    assert abs(entry.exposure - 400.0) < 1e-6
    print("PASS: test_exposure_falls_back_to_cost_basis_when_never_marked")


def test_legacy_ledger_json_without_market_value_loads() -> None:
    """engine_ownership_live.json predates the field. It must load and behave
    exactly as before until the first reconcile marks it."""
    legacy = {
        "strategy_id": "HORIZON", "symbol": "QQQM", "side": "buy", "qty": 3.5484,
        "client_order_id": "RECONCILE_HORIZON_QQQM_old", "status": "filled",
        "notional_at_entry": 1036.00, "fill_price": 291.96,
    }
    entry = OwnershipEntry.from_dict(legacy)
    assert entry.market_value is None
    assert abs(entry.exposure - 1036.00) < 1e-6
    # And the new field round-trips once set.
    entry.market_value = 4836.58
    assert abs(OwnershipEntry.from_dict(entry.to_dict()).exposure - 4836.58) < 1e-6
    print("PASS: test_legacy_ledger_json_without_market_value_loads")


def test_pending_only_symbol_does_not_get_duplicate_synthetic() -> None:
    """Guard preserved: a symbol whose only entry is an unfilled order must not
    also gain a synthetic entry — that would double-count the position."""
    config = build_default_config()
    ledger = OwnershipLedger()
    coid = "ENG_TREND_XBI_pending_1"
    ledger.register_order(
        strategy_id="TREND", symbol="XBI", side="buy", qty=3.0,
        client_order_id=coid, notional=400.0,
    )

    broker = FakeBroker(
        positions=[_position("XBI", "3.0", "400.00", "133.33")],
        orders_by_coid={coid: {"status": "new", "filled_avg_price": None, "filled_qty": None}},
    )
    reconcile(broker, ledger, config)

    xbi_entries = [e for e in ledger.entries.values() if e.symbol == "XBI"]
    assert len(xbi_entries) == 1, f"expected 1 entry, got {len(xbi_entries)}"
    assert xbi_entries[0].status == "pending", xbi_entries[0].status
    assert ledger.get_deployed_notional("TREND") == 0.0
    print("PASS: test_pending_only_symbol_does_not_get_duplicate_synthetic")


def test_newly_adopted_position_is_marked_at_birth() -> None:
    """A first-time synthetic entry carries a mark immediately, so it does not
    need a second reconcile to become accurate."""
    config = build_default_config()
    ledger = OwnershipLedger()
    broker = FakeBroker(positions=[_position("QQQM", "16.5438", "4836.58", "293.799404")])

    # Nothing in the ledger — classification falls to HORIZON known_symbols.
    reconcile(broker, ledger, config)

    held = ledger.get_filled_entries("HORIZON")
    assert len(held) == 1, f"expected QQQM adopted via known_symbols, got {held}"
    assert held[0].market_value is not None, "synthetic entry should be marked"
    assert abs(held[0].exposure - 4836.58) < 0.01
    print("PASS: test_newly_adopted_position_is_marked_at_birth")


# ---------------------------------------------------------------------------
# The consequence that actually matters
# ---------------------------------------------------------------------------

def test_sleeve_available_reflects_remarked_exposure() -> None:
    """The live numbers. Pre-fix the engine believed HORIZON had ~$2.7k of its
    sleeve free while horizon-live had already spent it. can_open() sizes off
    exactly this, so a revived sleeve would have over-allocated."""
    config = build_default_config()
    # Pin the CP2-era scenario this test documents (HORIZON 0.60); the live
    # allocation moved to 1.00 at CP3 (2026-09-05).
    config.sleeves["HORIZON"].allocation_pct = 0.60
    ledger = OwnershipLedger()
    _adopted(ledger, "QQQM", qty=3.5484, notional=1036.00, avg_price=291.96)
    _adopted(ledger, "PDBC", qty=11.5012, notional=214.00, avg_price=18.61)

    sleeves = SleeveManager(config)
    sleeves.refresh(6725.07, ledger)
    stale = sleeves.get_context("HORIZON", ledger)
    assert stale.sleeve_used < 1500, stale.sleeve_used
    assert stale.sleeve_available > 2000, stale.sleeve_available

    broker = FakeBroker(positions=[
        _position("QQQM", "16.5438", "4836.58", "293.799404"),
        _position("PDBC", "50.1886", "911.42", "18.316709"),
    ])
    reconcile(broker, ledger, config)
    sleeves.refresh(6725.07, ledger)
    fresh = sleeves.get_context("HORIZON", ledger)

    assert abs(fresh.sleeve_used - 5748.00) < 0.05, fresh.sleeve_used
    # Sleeve equity is 0.60 x 6725.07 = 4035.04 — the book is over its sleeve,
    # so nothing is free. Pre-fix this read as $2,763 available.
    assert fresh.sleeve_available == 0.0, fresh.sleeve_available
    print("PASS: test_sleeve_available_reflects_remarked_exposure")


def test_correlation_guard_sees_remarked_exposure() -> None:
    """active_positions() feeds the cross-sleeve correlation guard; it must use
    the same marked exposure, not the frozen entry cost."""
    config = build_default_config()
    ledger = OwnershipLedger()
    _adopted(ledger, "QQQM", qty=3.5484, notional=1036.00, avg_price=291.96)

    broker = FakeBroker(positions=[_position("QQQM", "16.5438", "4836.58", "293.799404")])
    reconcile(broker, ledger, config)

    book = ledger.active_positions("HORIZON")
    assert len(book) == 1, book
    symbol, signed, sid = book[0]
    assert symbol == "QQQM" and sid == "HORIZON"
    assert abs(signed - 4836.58) < 0.01, signed
    print("PASS: test_correlation_guard_sees_remarked_exposure")


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

def main() -> int:
    tests = [
        test_foreign_service_scale_up_is_remarked,
        test_remark_preserves_cost_basis_fields,
        test_remark_splits_proportionally_across_scale_in_entries,
        test_remark_handles_shrinking_position,
        test_remark_skipped_below_tolerance,
        test_exposure_falls_back_to_cost_basis_when_never_marked,
        test_legacy_ledger_json_without_market_value_loads,
        test_pending_only_symbol_does_not_get_duplicate_synthetic,
        test_newly_adopted_position_is_marked_at_birth,
        test_sleeve_available_reflects_remarked_exposure,
        test_correlation_guard_sees_remarked_exposure,
    ]
    failures = 0
    for t in tests:
        try:
            t()
        except AssertionError as e:
            failures += 1
            print(f"FAIL: {t.__name__} — {e}")
        except Exception as e:
            failures += 1
            print(f"ERROR: {t.__name__} — {type(e).__name__}: {e}")
    if failures:
        print(f"\n{failures} failure(s) out of {len(tests)}")
        return 1
    print(f"\nAll {len(tests)} tests passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
