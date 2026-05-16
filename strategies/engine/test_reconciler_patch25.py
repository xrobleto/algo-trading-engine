"""
Regression tests for Patch 25 — reconciler must not close pending ledger entries.

The bug: `reconciler.py` Step 4 ("close active entries whose broker position
has disappeared") used the predicate `entry.is_active`, which matches
`pending` as well as `filled` / `partially_filled`. A freshly registered order
is `pending` and has no broker position yet. The periodic reconcile fires
within the same engine tick as order submission (~100 ms later), before a
marketable-limit order has filled — so it saw the pending entry's symbol
missing from broker_positions and marked it `closed` with `fill_price=None`.

Live impact: the SIMPLE NVDL scalp on 2026-05-14 filled at Alpaca and was held
~2.5h before stopping out, but `engine_ownership_live.json` recorded it as
status=closed / fill_price=None / closed_at == registered_at + 104 ms — the
trade was silently lost from the ledger.

The fix: Step 4 now only closes entries with status `filled` /
`partially_filled` (entries that actually held a position). `pending`
resolution is owned by Step 3.5, which queries Alpaca order history.

These tests pin down:
  1. A pending entry whose order has not filled is NOT closed by reconcile.
  2. Same, even when the order-history API returns None (harsher path).
  3. Full NVDL lifecycle: pending -> filled -> closed across three reconciles,
     with fill_price / fill_qty preserved on the final closed entry.
  4. The legitimate behavior is not regressed: a filled entry whose broker
     position has disappeared is still closed.

Run:
    python strategies/engine/test_reconciler_patch25.py
    pytest strategies/engine/test_reconciler_patch25.py
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
from engine.ownership import OwnershipLedger  # noqa: E402
from engine.reconciler import reconcile  # noqa: E402


# ---------------------------------------------------------------------------
# Fake broker — minimal surface used by reconcile()
# ---------------------------------------------------------------------------

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


def _coid(symbol="NVDL"):
    # Mirrors the live client_order_id shape so prefix classification works.
    return f"ENG_SIMPLE_{symbol}_scalp_20260514_144926_87a3"


def _register_pending_buy(ledger, symbol="NVDL", qty=4.0, notional=513.64):
    """Register a SIMPLE buy exactly as the adapter does — leaves it pending."""
    coid = _coid(symbol)
    ledger.register_order(
        strategy_id="SIMPLE", symbol=symbol, side="buy", qty=qty,
        client_order_id=coid, broker_order_id="brk-1", notional=notional,
    )
    return coid


# ---------------------------------------------------------------------------
# Core regression
# ---------------------------------------------------------------------------

def test_pending_entry_not_closed_when_order_unfilled() -> None:
    """A pending buy whose limit order has not yet filled must survive a
    reconcile that runs before the fill. This is the exact NVDL bug."""
    config = build_default_config()
    ledger = OwnershipLedger()
    coid = _register_pending_buy(ledger)

    # Broker state ~100ms after submission: order accepted, NOT filled,
    # no position yet.
    broker = FakeBroker(
        positions=[],
        open_orders=[{"symbol": "NVDL", "client_order_id": coid, "status": "new"}],
        orders_by_coid={coid: {"status": "new", "filled_avg_price": None, "filled_qty": None}},
    )

    reconcile(broker, ledger, config)

    entry = ledger.entries[coid]
    assert entry.status == "pending", f"expected pending, got {entry.status}"
    assert entry.closed_at is None, f"pending entry must not be closed: {entry.closed_at}"
    assert entry.fill_price is None
    print("PASS: test_pending_entry_not_closed_when_order_unfilled")


def test_pending_entry_not_closed_when_order_api_returns_none() -> None:
    """Harsher path: the order-history API returns None for the just-submitted
    order and there is no position to infer a fill from. The entry must stay
    pending — the old code closed it via the is_active sweep."""
    config = build_default_config()
    ledger = OwnershipLedger()
    coid = _register_pending_buy(ledger)

    broker = FakeBroker(
        positions=[],
        open_orders=[],
        orders_by_coid={},  # get_order_by_client_id -> None
    )

    reconcile(broker, ledger, config)

    entry = ledger.entries[coid]
    assert entry.status == "pending", f"expected pending, got {entry.status}"
    assert entry.closed_at is None
    print("PASS: test_pending_entry_not_closed_when_order_api_returns_none")


def test_nvdl_lifecycle_pending_filled_closed_retains_fill_data() -> None:
    """Full NVDL replay across three reconciles:
       #1 order unfilled        -> entry stays pending
       #2 order filled, held    -> Step 3.5 promotes to filled w/ fill data
       #3 stopped out, gone     -> Step 4 closes it, fill data preserved
    """
    config = build_default_config()
    ledger = OwnershipLedger()
    coid = _register_pending_buy(ledger)

    # ---- Reconcile #1: submitted, not yet filled ----
    broker1 = FakeBroker(
        positions=[],
        open_orders=[{"symbol": "NVDL", "client_order_id": coid, "status": "new"}],
        orders_by_coid={coid: {"status": "new", "filled_avg_price": None, "filled_qty": None}},
    )
    reconcile(broker1, ledger, config)
    assert ledger.entries[coid].status == "pending", ledger.entries[coid].status

    # ---- Reconcile #2: order filled, position live ----
    broker2 = FakeBroker(
        positions=[{"symbol": "NVDL", "qty": "4", "market_value": "513.64",
                    "avg_entry_price": "128.41"}],
        open_orders=[],
        orders_by_coid={coid: {"status": "filled", "filled_avg_price": 128.41,
                               "filled_qty": 4.0}},
    )
    reconcile(broker2, ledger, config)
    entry = ledger.entries[coid]
    assert entry.status == "filled", f"expected filled, got {entry.status}"
    assert abs(entry.fill_price - 128.41) < 1e-6, entry.fill_price
    assert abs(entry.fill_qty - 4.0) < 1e-6, entry.fill_qty

    # ---- Reconcile #3: stopped out, position gone ----
    broker3 = FakeBroker(
        positions=[],
        open_orders=[],
        orders_by_coid={coid: {"status": "filled", "filled_avg_price": 128.41,
                               "filled_qty": 4.0}},
    )
    reconcile(broker3, ledger, config)
    entry = ledger.entries[coid]
    assert entry.status == "closed", f"expected closed, got {entry.status}"
    assert entry.closed_at is not None, "closed entry must have closed_at"
    # The whole point of the fix: the closed entry still records the real fill.
    assert abs(entry.fill_price - 128.41) < 1e-6, entry.fill_price
    assert abs(entry.fill_qty - 4.0) < 1e-6, entry.fill_qty
    print("PASS: test_nvdl_lifecycle_pending_filled_closed_retains_fill_data")


def test_filled_entry_still_closed_when_position_disappears() -> None:
    """Do not regress the legitimate Step 4 behavior: a filled entry whose
    broker position is gone must still be marked closed."""
    config = build_default_config()
    ledger = OwnershipLedger()
    coid = _coid("XBI")
    ledger.register_order(
        strategy_id="TREND", symbol="XBI", side="buy", qty=3.0,
        client_order_id=f"ENG_TREND_XBI_{coid}", notional=400.0,
    )
    tcoid = f"ENG_TREND_XBI_{coid}"
    ledger.update_status(tcoid, "filled", fill_price=131.0, fill_qty=3.0)
    assert ledger.entries[tcoid].status == "filled"

    # Broker shows the position gone.
    broker = FakeBroker(positions=[], open_orders=[], orders_by_coid={})
    reconcile(broker, ledger, config)

    entry = ledger.entries[tcoid]
    assert entry.status == "closed", f"expected closed, got {entry.status}"
    assert entry.closed_at is not None
    print("PASS: test_filled_entry_still_closed_when_position_disappears")


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

def main() -> int:
    tests = [
        test_pending_entry_not_closed_when_order_unfilled,
        test_pending_entry_not_closed_when_order_api_returns_none,
        test_nvdl_lifecycle_pending_filled_closed_retains_fill_data,
        test_filled_entry_still_closed_when_position_disappears,
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
