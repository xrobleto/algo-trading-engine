"""
Base Strategy Adapter — abstract interface that all strategy adapters implement.

Each adapter wraps an existing strategy's logic and provides:
- initialize(): one-time startup (load state, init clients, reconcile)
- tick(): one iteration of the strategy's main loop
- shutdown(): graceful cleanup
"""

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Optional, Set

from engine.config import SleeveConfig
from engine.ownership import OwnershipLedger
from engine.sleeves import SleeveContext, SleeveManager

log = logging.getLogger("Engine")


class TickResult(Enum):
    """Result of a strategy tick."""
    OK = "ok"                       # tick executed normally
    SKIPPED = "skipped"             # tick skipped (rate limited, market closed, etc.)
    HALTED = "halted"               # strategy is halted (kill switch, anomaly, etc.)
    ERROR = "error"                 # tick encountered an error


@dataclass
class TickDiagnostics:
    """Optional diagnostics from a strategy tick."""
    result: TickResult
    message: str = ""
    orders_submitted: int = 0
    orders_rejected: int = 0
    positions_managed: int = 0


class StrategyAdapter(ABC):
    """
    Abstract base for strategy adapters.

    Adapters are thin wrappers around existing strategy code. They:
    1. Intercept order submissions to validate sleeve capacity
    2. Register orders in the ownership ledger
    3. Expose the strategy's tick/shutdown lifecycle to the engine
    """

    def __init__(
        self,
        config: SleeveConfig,
        ledger: OwnershipLedger,
        sleeve_manager: SleeveManager,
    ):
        self.config = config
        self.strategy_id = config.strategy_id
        self.order_prefix = config.order_prefix
        self.ledger = ledger
        self.sleeve_manager = sleeve_manager

        # Error tracking for auto-halt
        self._consecutive_errors: int = 0
        self._halted: bool = False
        self._halt_reason: Optional[str] = None

    # -------------------------------------------------------------------------
    # LIFECYCLE (must implement)
    # -------------------------------------------------------------------------

    @abstractmethod
    def initialize(self, ctx: SleeveContext) -> None:
        """
        One-time startup: load state, init clients, start WebSocket, etc.
        Called once before the first tick.
        """
        ...

    @abstractmethod
    def tick(self, ctx: SleeveContext) -> TickResult:
        """
        Execute one iteration of the strategy's main loop.
        Called by the engine on each cycle.
        """
        ...

    @abstractmethod
    def shutdown(self) -> None:
        """
        Graceful shutdown: save state, stop threads, cancel working orders.
        Called on engine shutdown or when strategy is halted.
        """
        ...

    @abstractmethod
    def get_owned_symbols(self) -> Set[str]:
        """
        Get symbols this strategy currently owns/manages.
        Used for conflict detection.
        """
        ...

    # -------------------------------------------------------------------------
    # ERROR / HALT MANAGEMENT
    # -------------------------------------------------------------------------

    def record_error(self, error: Exception) -> None:
        """Record a tick error. May auto-halt if threshold exceeded."""
        self._consecutive_errors += 1
        log.error(
            f"[{self.strategy_id}] Tick error #{self._consecutive_errors}: {error}"
        )

        if self._consecutive_errors >= self.config.max_consecutive_errors:
            self.halt(
                f"Too many consecutive errors ({self._consecutive_errors}): {error}"
            )

    def record_success(self) -> None:
        """Record a successful tick. Resets error counter."""
        self._consecutive_errors = 0

    def halt(self, reason: str) -> None:
        """Halt this strategy. Engine will stop calling tick()."""
        if self._halted:
            return
        self._halted = True
        self._halt_reason = reason
        log.critical(f"[{self.strategy_id}] STRATEGY HALTED: {reason}")

    @property
    def is_halted(self) -> bool:
        return self._halted

    @property
    def halt_reason(self) -> Optional[str]:
        return self._halt_reason

    # -------------------------------------------------------------------------
    # ORDER VALIDATION (shared helper for all adapters)
    # -------------------------------------------------------------------------

    def validate_order(
        self,
        symbol: str,
        side: str,
        qty: float,
        notional: float,
    ) -> tuple:
        """
        Validate an order against sleeve constraints.

        Returns:
            (allowed: bool, reason: str)
        """
        # Exits are always allowed (even when halted)
        if side.lower() == "sell":
            return True, "exit order — always allowed"

        # Check halted state for entries
        if self._halted:
            return False, f"strategy halted: {self._halt_reason}"

        # Market intelligence entry gate
        _ctx = getattr(self, '_current_ctx', None)
        _mctx = getattr(_ctx, 'market_context', None) if _ctx else None
        if _mctx:
            _adj = _mctx.sleeve_adjustments.get(self.strategy_id)
            if _adj and not _adj.entry_allowed:
                return False, f"intelligence: {_adj.entry_gate_reason}"

        # Delegate to sleeve manager
        return self.sleeve_manager.can_deploy(
            self.strategy_id, notional, self.ledger, symbol
        )

    def get_status(self) -> dict:
        """Get adapter status for diagnostics."""
        return {
            "strategy_id": self.strategy_id,
            "halted": self._halted,
            "halt_reason": self._halt_reason,
            "consecutive_errors": self._consecutive_errors,
            "probation": self.config.probation,
            "owned_symbols": list(self.get_owned_symbols()),
        }
