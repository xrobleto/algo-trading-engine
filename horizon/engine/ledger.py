"""Ownership ledger — the single source of truth for which sleeve owns what.

Every order carries a `client_order_id` prefixed by its sleeve (`HZN_PULSE_`,
`HZN_ROT_`, ...). The ledger maps those orders to sleeves and tracks their
lifecycle. Persisted atomically to JSON so a restart recovers cleanly.

Adapted from the Unified Engine's ownership.py, with the documented design
debts fixed (no `qty*100` notional fallback; status is an explicit set).
"""

from __future__ import annotations

import json
import os
import tempfile
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

ACTIVE = ("pending", "filled", "partially_filled")
TERMINAL = ("closed", "cancelled")


@dataclass
class OwnershipEntry:
    strategy_id: str
    symbol: str
    side: str
    qty: float
    client_order_id: str
    broker_order_id: Optional[str] = None
    status: str = "pending"
    registered_at: str = ""
    notional_at_entry: float = 0.0
    fill_price: Optional[float] = None
    fill_qty: Optional[float] = None
    closed_at: Optional[str] = None

    def __post_init__(self):
        if not self.registered_at:
            self.registered_at = datetime.now(timezone.utc).isoformat()

    @property
    def is_active(self) -> bool:
        return self.status in ACTIVE

    @property
    def is_terminal(self) -> bool:
        return self.status in TERMINAL


@dataclass
class OwnershipLedger:
    entries: Dict[str, OwnershipEntry] = field(default_factory=dict)
    last_reconciled_at: str = ""
    version: int = 1

    def register_order(self, strategy_id: str, symbol: str, side: str,
                       qty: float, client_order_id: str,
                       broker_order_id: Optional[str] = None,
                       notional: float = 0.0) -> OwnershipEntry:
        if client_order_id in self.entries:
            return self.entries[client_order_id]
        entry = OwnershipEntry(strategy_id, symbol, side, qty, client_order_id,
                               broker_order_id, notional_at_entry=notional)
        self.entries[client_order_id] = entry
        return entry

    def update_status(self, client_order_id: str, status: str,
                      fill_price: Optional[float] = None,
                      fill_qty: Optional[float] = None,
                      notional: Optional[float] = None) -> Optional[OwnershipEntry]:
        entry = self.entries.get(client_order_id)
        if entry is None:
            return None
        entry.status = status
        if fill_price is not None:
            entry.fill_price = fill_price
        if fill_qty is not None:
            entry.fill_qty = fill_qty
        if notional is not None:
            entry.notional_at_entry = notional
        elif entry.fill_price and entry.fill_qty:
            entry.notional_at_entry = entry.fill_price * entry.fill_qty
        if status in TERMINAL and not entry.closed_at:
            entry.closed_at = datetime.now(timezone.utc).isoformat()
        return entry

    def active_entries(self, strategy_id: Optional[str] = None
                       ) -> List[OwnershipEntry]:
        return [e for e in self.entries.values()
                if e.is_active and (strategy_id is None
                                    or e.strategy_id == strategy_id)]

    def get_owner(self, symbol: str) -> Optional[str]:
        for e in self.entries.values():
            if e.symbol == symbol and e.is_active:
                return e.strategy_id
        return None

    def deployed_notional(self, strategy_id: str) -> float:
        return sum(e.notional_at_entry for e in self.entries.values()
                   if e.strategy_id == strategy_id and e.status == "filled")

    def conflicts(self) -> List[str]:
        owners: Dict[str, set] = {}
        for e in self.entries.values():
            if e.is_active:
                owners.setdefault(e.symbol, set()).add(e.strategy_id)
        return [sym for sym, who in owners.items() if len(who) > 1]

    def prune_terminal(self, max_age_days: int = 7) -> int:
        cutoff = datetime.now(timezone.utc).timestamp() - max_age_days * 86400
        drop = []
        for coid, e in self.entries.items():
            if e.is_terminal and e.closed_at:
                try:
                    ts = datetime.fromisoformat(e.closed_at).timestamp()
                    if ts < cutoff:
                        drop.append(coid)
                except ValueError:
                    pass
        for coid in drop:
            del self.entries[coid]
        return len(drop)

    # --- persistence ---------------------------------------------------------
    def save(self, path: Path) -> None:
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "version": self.version,
            "last_reconciled_at": self.last_reconciled_at,
            "entries": {k: asdict(v) for k, v in self.entries.items()},
        }
        fd, tmp = tempfile.mkstemp(dir=str(path.parent), suffix=".tmp")
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, sort_keys=True)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)

    @classmethod
    def load(cls, path: Path) -> "OwnershipLedger":
        path = Path(path)
        if not path.exists():
            return cls()
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            return cls()
        valid = set(OwnershipEntry.__dataclass_fields__)
        entries = {k: OwnershipEntry(**{kk: vv for kk, vv in v.items()
                                        if kk in valid})
                   for k, v in data.get("entries", {}).items()}
        return cls(entries=entries,
                   last_reconciled_at=data.get("last_reconciled_at", ""),
                   version=data.get("version", 1))
