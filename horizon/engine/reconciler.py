"""Reconcile the ownership ledger against live Alpaca state.

Each engine cycle this:
  - resolves pending orders to their true fill status,
  - closes ledger entries whose broker position has disappeared,
  - recovers orphaned positions — a broker position with no ledger entry is
    recorded as a synthetic UNMANAGED entry and surfaced (the engine will not
    auto-trade it; a human decides),
  - surfaces ownership conflicts.

So the ledger never silently drifts from broker truth.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import List

from .broker import BrokerFacade
from .ledger import OwnershipLedger

UNMANAGED = "UNMANAGED"


@dataclass
class ReconcileResult:
    pending_resolved: int = 0
    closed: int = 0
    orphans_recovered: int = 0
    orphan_symbols: List[str] = field(default_factory=list)
    conflicts: List[str] = field(default_factory=list)
    notes: List[str] = field(default_factory=list)

    @property
    def is_safe(self) -> bool:
        return not self.conflicts


def reconcile(broker: BrokerFacade, ledger: OwnershipLedger) -> ReconcileResult:
    res = ReconcileResult()
    try:
        positions = broker.get_positions()
    except Exception as exc:
        res.notes.append(f"broker positions unavailable: {exc}")
        return res

    # 1. Resolve pending entries against the broker's authoritative status.
    for coid, entry in list(ledger.entries.items()):
        if entry.status != "pending":
            continue
        order = broker.get_order_by_client_id(coid)
        if order is None:
            continue
        status = order["status"].lower()
        if "filled" in status and "partial" not in status:
            ledger.update_status(coid, "filled", order["filled_avg_price"],
                                 order["filled_qty"])
            res.pending_resolved += 1
        elif "partially_filled" in status:
            ledger.update_status(coid, "partially_filled",
                                 order["filled_avg_price"], order["filled_qty"])
        elif status in ("canceled", "cancelled", "expired", "rejected"):
            ledger.update_status(coid, "cancelled")

    # 2. Close entries that held a position which is now gone.
    for coid, entry in list(ledger.entries.items()):
        if (entry.status in ("filled", "partially_filled")
                and entry.symbol not in positions):
            ledger.update_status(coid, "closed")
            res.closed += 1

    # 3. Orphaned-position recovery. A broker position with no active ledger
    #    entry is adopted as a synthetic UNMANAGED entry — tracked so accounting
    #    is correct, but the engine will not auto-trade it (see main.py).
    active_symbols = {e.symbol for e in ledger.active_entries()}
    for symbol, pos in positions.items():
        if symbol in active_symbols:
            continue
        coid = (f"RECON_UNMANAGED_{symbol}_"
                f"{int(datetime.now(timezone.utc).timestamp())}")
        qty = abs(float(pos.get("qty", 0.0)))
        ledger.register_order(UNMANAGED, symbol, "buy", qty, coid,
                              notional=abs(float(pos.get("market_value", 0.0))))
        ledger.update_status(coid, "filled",
                             fill_price=pos.get("avg_entry_price"),
                             fill_qty=qty)
        res.orphans_recovered += 1
    res.orphan_symbols = sorted({e.symbol
                                 for e in ledger.active_entries(UNMANAGED)})

    res.conflicts = ledger.conflicts()
    ledger.last_reconciled_at = datetime.now(timezone.utc).isoformat()
    return res
