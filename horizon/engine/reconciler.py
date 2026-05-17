"""Reconcile the ownership ledger against live Alpaca state.

Resolves pending orders to their true fill status, closes ledger entries whose
broker position has disappeared, and surfaces ownership conflicts. Runs each
engine cycle so the ledger never drifts from broker truth.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import List

from .broker import BrokerFacade
from .ledger import OwnershipLedger


@dataclass
class ReconcileResult:
    pending_resolved: int = 0
    closed: int = 0
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

    # Resolve pending entries against the broker's authoritative order status.
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
                                 order["filled_avg_price"],
                                 order["filled_qty"])
        elif status in ("canceled", "cancelled", "expired", "rejected"):
            ledger.update_status(coid, "cancelled")

    # Close entries that actually held a position which is now gone.
    for coid, entry in list(ledger.entries.items()):
        if (entry.status in ("filled", "partially_filled")
                and entry.symbol not in positions):
            ledger.update_status(coid, "closed")
            res.closed += 1

    res.conflicts = ledger.conflicts()
    ledger.last_reconciled_at = datetime.now(timezone.utc).isoformat()
    return res
