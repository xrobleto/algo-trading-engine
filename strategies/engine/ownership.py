"""
Position/Order Ownership Ledger — the single source of truth for "who owns what."

Every order and position is tagged with a strategy_id. The ledger is persisted
to engine_ownership.json and consulted by the reconciler, sleeve manager,
and conflict prevention logic.
"""

import json
import logging
import os
import tempfile
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

log = logging.getLogger("Engine")


# =============================================================================
# DATA MODEL
# =============================================================================

@dataclass
class OwnershipEntry:
    """A single owned order/position record."""
    strategy_id: str                # "TREND" or "SIMPLE"
    symbol: str
    side: str                       # "buy" or "sell"
    qty: float
    client_order_id: str            # ENG_TREND_* or ENG_SIMPLE_*
    broker_order_id: Optional[str] = None
    status: str = "pending"         # "pending" | "filled" | "partially_filled" | "closed" | "cancelled"
    registered_at: str = ""         # ISO timestamp
    notional_at_entry: float = 0.0  # estimated $ value when order placed
    fill_price: Optional[float] = None
    fill_qty: Optional[float] = None
    closed_at: Optional[str] = None

    def __post_init__(self):
        if not self.registered_at:
            self.registered_at = datetime.now(timezone.utc).isoformat()

    @property
    def is_terminal(self) -> bool:
        return self.status in ("closed", "cancelled")

    @property
    def is_active(self) -> bool:
        return self.status in ("pending", "filled", "partially_filled")

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "OwnershipEntry":
        # Filter unknown keys for forward compatibility
        valid_fields = {f.name for f in cls.__dataclass_fields__.values()}
        filtered = {k: v for k, v in d.items() if k in valid_fields}
        return cls(**filtered)


@dataclass
class OwnershipLedger:
    """
    The ownership ledger — tracks all orders/positions by strategy.

    Keyed by client_order_id for uniqueness.
    Terminal entries are pruned after a configurable retention period.
    """
    entries: Dict[str, OwnershipEntry] = field(default_factory=dict)
    last_reconciled_at: str = ""
    version: int = 1

    # -------------------------------------------------------------------------
    # REGISTRATION
    # -------------------------------------------------------------------------

    def register_order(
        self,
        strategy_id: str,
        symbol: str,
        side: str,
        qty: float,
        client_order_id: str,
        broker_order_id: Optional[str] = None,
        notional: float = 0.0,
    ) -> OwnershipEntry:
        """Register a new order in the ledger. Returns the created entry."""
        if client_order_id in self.entries:
            existing = self.entries[client_order_id]
            log.warning(
                f"[OWNERSHIP] Duplicate registration for {client_order_id} "
                f"(existing: {existing.strategy_id}/{existing.symbol}/{existing.status})"
            )
            return existing

        entry = OwnershipEntry(
            strategy_id=strategy_id,
            symbol=symbol,
            side=side,
            qty=qty,
            client_order_id=client_order_id,
            broker_order_id=broker_order_id,
            notional_at_entry=notional,
        )
        self.entries[client_order_id] = entry
        log.info(
            f"[OWNERSHIP] Registered: {strategy_id} {side} {qty:.4f} {symbol} "
            f"| order={client_order_id} | notional=${notional:,.2f}"
        )
        return entry

    # -------------------------------------------------------------------------
    # STATUS UPDATES
    # -------------------------------------------------------------------------

    def update_status(
        self,
        client_order_id: str,
        status: str,
        fill_price: Optional[float] = None,
        fill_qty: Optional[float] = None,
        notional: Optional[float] = None,
    ) -> Optional[OwnershipEntry]:
        """Update the status of an existing entry."""
        entry = self.entries.get(client_order_id)
        if not entry:
            log.warning(f"[OWNERSHIP] Cannot update unknown order: {client_order_id}")
            return None

        old_status = entry.status
        entry.status = status
        if fill_price is not None:
            entry.fill_price = fill_price
        if fill_qty is not None:
            entry.fill_qty = fill_qty
        if notional is not None:
            entry.notional_at_entry = notional
        elif entry.fill_price and entry.fill_qty:
            # Self-heal: when fill_price + fill_qty are both known and caller
            # didn't pass an explicit notional, recompute. Prevents the qty*100
            # placeholder from outliving the real fill once the reconciler
            # learns the true avg_price (Patch 11 only fixed the adapter path).
            recomputed = entry.fill_price * entry.fill_qty
            if abs(entry.notional_at_entry - recomputed) > 0.01:
                entry.notional_at_entry = recomputed
        if status in ("closed", "cancelled"):
            entry.closed_at = datetime.now(timezone.utc).isoformat()

        log.debug(
            f"[OWNERSHIP] Status update: {entry.symbol} {client_order_id} "
            f"{old_status} -> {status}"
        )
        return entry

    def mark_closed(self, client_order_id: str) -> Optional[OwnershipEntry]:
        """Mark an entry as closed."""
        return self.update_status(client_order_id, "closed")

    def mark_cancelled(self, client_order_id: str) -> Optional[OwnershipEntry]:
        """Mark an entry as cancelled."""
        return self.update_status(client_order_id, "cancelled")

    # -------------------------------------------------------------------------
    # QUERIES
    # -------------------------------------------------------------------------

    def get_owner(self, symbol: str) -> Optional[str]:
        """Get the strategy_id that owns an active position in symbol, or None."""
        for entry in self.entries.values():
            if entry.symbol == symbol and entry.is_active:
                return entry.strategy_id
        return None

    def get_active_entries(self, strategy_id: Optional[str] = None) -> List[OwnershipEntry]:
        """Get all active (non-terminal) entries, optionally filtered by strategy."""
        results = [e for e in self.entries.values() if e.is_active]
        if strategy_id:
            results = [e for e in results if e.strategy_id == strategy_id]
        return results

    def get_active_symbols(self, strategy_id: Optional[str] = None) -> Set[str]:
        """Get set of symbols with active entries."""
        return {e.symbol for e in self.get_active_entries(strategy_id)}

    def get_filled_entries(self, strategy_id: Optional[str] = None) -> List[OwnershipEntry]:
        """Get entries with status 'filled' (active positions)."""
        results = [e for e in self.entries.values() if e.status == "filled"]
        if strategy_id:
            results = [e for e in results if e.strategy_id == strategy_id]
        return results

    def count_active_positions(self, strategy_id: str) -> int:
        """Count distinct symbols with filled entries for a strategy."""
        return len({e.symbol for e in self.get_filled_entries(strategy_id)})

    def get_deployed_notional(self, strategy_id: str) -> float:
        """Calculate total deployed notional for a strategy from filled entries."""
        return sum(e.notional_at_entry for e in self.get_filled_entries(strategy_id))

    def is_symbol_owned_by_other(self, symbol: str, strategy_id: str) -> bool:
        """Check if symbol has active entries under a DIFFERENT strategy."""
        owner = self.get_owner(symbol)
        return owner is not None and owner != strategy_id

    def has_conflicts(self) -> List[str]:
        """
        Detect symbols owned by multiple strategies.
        Returns list of conflicting symbols.
        """
        symbol_owners: Dict[str, Set[str]] = {}
        for entry in self.entries.values():
            if entry.is_active:
                if entry.symbol not in symbol_owners:
                    symbol_owners[entry.symbol] = set()
                symbol_owners[entry.symbol].add(entry.strategy_id)

        return [sym for sym, owners in symbol_owners.items() if len(owners) > 1]

    def find_by_broker_order_id(self, broker_order_id: str) -> Optional[OwnershipEntry]:
        """Look up entry by broker order ID."""
        for entry in self.entries.values():
            if entry.broker_order_id == broker_order_id:
                return entry
        return None

    # -------------------------------------------------------------------------
    # MAINTENANCE
    # -------------------------------------------------------------------------

    def prune_terminal(self, max_age_days: int = 7) -> int:
        """Remove terminal entries older than max_age_days. Returns count pruned."""
        cutoff = datetime.now(timezone.utc) - timedelta(days=max_age_days)
        to_remove = []
        for coid, entry in self.entries.items():
            if entry.is_terminal and entry.closed_at:
                try:
                    closed = datetime.fromisoformat(entry.closed_at)
                    if closed.tzinfo is None:
                        closed = closed.replace(tzinfo=timezone.utc)
                    if closed < cutoff:
                        to_remove.append(coid)
                except (ValueError, TypeError):
                    pass  # keep entries with unparseable timestamps

        for coid in to_remove:
            del self.entries[coid]

        if to_remove:
            log.info(f"[OWNERSHIP] Pruned {len(to_remove)} terminal entries older than {max_age_days}d")
        return len(to_remove)

    # -------------------------------------------------------------------------
    # PERSISTENCE
    # -------------------------------------------------------------------------

    def to_dict(self) -> dict:
        return {
            "version": self.version,
            "last_reconciled_at": self.last_reconciled_at,
            "entries": {k: v.to_dict() for k, v in self.entries.items()},
        }

    @classmethod
    def from_dict(cls, d: dict) -> "OwnershipLedger":
        ledger = cls()
        ledger.version = d.get("version", 1)
        ledger.last_reconciled_at = d.get("last_reconciled_at", "")
        raw_entries = d.get("entries", {})
        for coid, entry_dict in raw_entries.items():
            ledger.entries[coid] = OwnershipEntry.from_dict(entry_dict)
        return ledger

    def save(self, path: str) -> None:
        """Atomically save ledger to JSON file."""
        data = self.to_dict()
        dir_path = os.path.dirname(path)
        if dir_path:
            os.makedirs(dir_path, exist_ok=True)

        # Atomic write: write to temp file, then rename
        fd, tmp_path = tempfile.mkstemp(dir=dir_path, suffix=".tmp")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, sort_keys=True)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_path, path)
        except Exception:
            # Clean up temp file on error
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise

    @classmethod
    def load(cls, path: str) -> "OwnershipLedger":
        """Load ledger from JSON file. Returns empty ledger if file doesn't exist."""
        if not os.path.exists(path):
            log.info(f"[OWNERSHIP] No existing ledger at {path} — starting fresh")
            return cls()

        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            ledger = cls.from_dict(data)
            active_count = len(ledger.get_active_entries())
            log.info(
                f"[OWNERSHIP] Loaded ledger: {len(ledger.entries)} total entries, "
                f"{active_count} active"
            )
            return ledger
        except (json.JSONDecodeError, KeyError) as e:
            log.error(f"[OWNERSHIP] Failed to load ledger from {path}: {e} — starting fresh")
            return cls()
