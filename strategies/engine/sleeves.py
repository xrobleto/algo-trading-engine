"""
Capital Sleeve Manager — enforces per-strategy capital allocation.

Each strategy gets a "sleeve" (a fraction of total account equity).
The sleeve manager prevents one strategy from consuming another's capital.
"""

import logging
from dataclasses import dataclass
from typing import Dict, Optional

from engine.config import EngineConfig, SleeveConfig
from engine.ownership import OwnershipLedger

log = logging.getLogger("Engine")


# =============================================================================
# SLEEVE CONTEXT (passed to adapters each tick)
# =============================================================================

@dataclass
class SleeveContext:
    """Read-only snapshot of a strategy's capital sleeve state."""
    strategy_id: str
    sleeve_equity: float            # total $ allocated to this sleeve
    sleeve_used: float              # $ currently deployed (from filled entries)
    sleeve_available: float         # sleeve_equity - sleeve_used
    total_account_equity: float     # full account equity (for reference/logging)
    config: SleeveConfig
    market_context: Optional[object] = None  # MarketContext from intelligence layer

    @property
    def utilization_pct(self) -> float:
        """Fraction of sleeve currently deployed."""
        if self.sleeve_equity <= 0:
            return 0.0
        return self.sleeve_used / self.sleeve_equity


# =============================================================================
# SLEEVE MANAGER
# =============================================================================

class SleeveManager:
    """
    Manages capital sleeve allocations across strategies.

    Computes per-strategy dollar amounts from total equity, tracks usage
    via the ownership ledger, and enforces deployment limits.
    """

    def __init__(self, config: EngineConfig):
        self.config = config
        self._total_equity: float = 0.0
        self._sleeve_equity: Dict[str, float] = {}
        self._cash_reserve: float = 0.0

    # -------------------------------------------------------------------------
    # REFRESH (called each engine tick)
    # -------------------------------------------------------------------------

    def refresh(
        self,
        total_equity: float,
        ledger: OwnershipLedger,
        allocation_overrides: Optional[Dict[str, float]] = None,
    ) -> None:
        """
        Recalculate sleeve allocations from current account equity.

        Args:
            total_equity: Total account equity from broker
            ledger: Ownership ledger for deployed capital calculation
            allocation_overrides: Optional dynamic allocation fractions from
                the intelligence layer (e.g. {"TREND": 0.62, ...}).
                When provided, these replace the static config values.
        """
        self._total_equity = total_equity
        self._cash_reserve = total_equity * self.config.cash_reserve_pct

        for strategy_id, sleeve_config in self.config.sleeves.items():
            alloc = sleeve_config.allocation_pct
            if allocation_overrides and strategy_id in allocation_overrides:
                alloc = allocation_overrides[strategy_id]
            self._sleeve_equity[strategy_id] = total_equity * alloc

    # -------------------------------------------------------------------------
    # QUERIES
    # -------------------------------------------------------------------------

    def get_context(self, strategy_id: str, ledger: OwnershipLedger) -> SleeveContext:
        """Build a SleeveContext snapshot for a strategy."""
        config = self.config.sleeves.get(strategy_id)
        if not config:
            raise ValueError(f"Unknown strategy: {strategy_id}")

        sleeve_eq = self._sleeve_equity.get(strategy_id, 0.0)
        sleeve_used = ledger.get_deployed_notional(strategy_id)
        sleeve_avail = max(0.0, sleeve_eq - sleeve_used)

        return SleeveContext(
            strategy_id=strategy_id,
            sleeve_equity=sleeve_eq,
            sleeve_used=sleeve_used,
            sleeve_available=sleeve_avail,
            total_account_equity=self._total_equity,
            config=config,
        )

    def can_deploy(
        self,
        strategy_id: str,
        notional: float,
        ledger: OwnershipLedger,
        symbol: Optional[str] = None,
    ) -> tuple:
        """
        Check if a strategy can deploy additional capital.

        Args:
            strategy_id: Strategy requesting deployment
            notional: Dollar amount of proposed order
            ledger: Current ownership ledger
            symbol: Symbol being traded (for cross-strategy conflict check)

        Returns:
            (allowed: bool, reason: str)
        """
        config = self.config.sleeves.get(strategy_id)
        if not config:
            return False, f"unknown strategy: {strategy_id}"

        # Check 1: Symbol not owned by another strategy
        if symbol and ledger.is_symbol_owned_by_other(symbol, strategy_id):
            owner = ledger.get_owner(symbol)
            return False, f"{symbol} already owned by {owner}"

        # Check 2: Position count limit
        if config.max_positions is not None:
            current_count = ledger.count_active_positions(strategy_id)
            if current_count >= config.max_positions:
                return False, (
                    f"{strategy_id} at position limit: "
                    f"{current_count}/{config.max_positions}"
                )

        # Check 3: Sleeve capacity
        ctx = self.get_context(strategy_id, ledger)
        if notional > ctx.sleeve_available:
            return False, (
                f"{strategy_id} sleeve insufficient: "
                f"need ${notional:,.2f}, available ${ctx.sleeve_available:,.2f} "
                f"(used ${ctx.sleeve_used:,.2f} of ${ctx.sleeve_equity:,.2f})"
            )

        return True, "ok"

    @property
    def total_equity(self) -> float:
        return self._total_equity

    @property
    def cash_reserve(self) -> float:
        return self._cash_reserve

    # -------------------------------------------------------------------------
    # DIAGNOSTICS
    # -------------------------------------------------------------------------

    def get_summary(self, ledger: OwnershipLedger) -> dict:
        """Build a diagnostic summary of all sleeves."""
        summary = {
            "total_equity": self._total_equity,
            "cash_reserve": self._cash_reserve,
            "sleeves": {},
        }
        for strategy_id in self.config.sleeves:
            ctx = self.get_context(strategy_id, ledger)
            summary["sleeves"][strategy_id] = {
                "allocation_pct": ctx.config.allocation_pct,
                "equity": ctx.sleeve_equity,
                "used": ctx.sleeve_used,
                "available": ctx.sleeve_available,
                "utilization_pct": ctx.utilization_pct,
                "positions": ledger.count_active_positions(strategy_id),
                "max_positions": ctx.config.max_positions,
                "probation": ctx.config.probation,
            }
        return summary
