"""Sleeve capital accounting.

Splits total account equity into per-strategy dollar budgets. Sleeves are an
accounting overlay over one Alpaca account — the same model the backtest
portfolio uses. Regime tilts and the book-leverage dial are applied on top.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional

from ..config import EngineConfig
from .intelligence import RegimeContext
from .ledger import OwnershipLedger


@dataclass
class SleeveBudget:
    strategy_id: str
    sleeve_equity: float        # dollars allocated to this sleeve
    deployed: float             # dollars currently in filled positions
    available: float            # sleeve_equity - deployed


class SleeveManager:
    def __init__(self, cfg: EngineConfig):
        self.cfg = cfg

    def budgets(self, total_equity: float, ledger: OwnershipLedger,
                regime: Optional[RegimeContext] = None) -> Dict[str, SleeveBudget]:
        """Per-sleeve dollar budgets, regime-tilted and book-levered."""
        sleeves = {s: c for s, c in self.cfg.sleeves.items() if c.enabled}
        raw = {}
        for sid, conf in sleeves.items():
            mult = regime.alloc_mult(sid) if regime else 1.0
            raw[sid] = conf.base_allocation * mult
        scale = sum(raw.values()) or 1.0
        out: Dict[str, SleeveBudget] = {}
        for sid in sleeves:
            weight = raw[sid] / scale
            equity = total_equity * weight * self.cfg.book_leverage
            deployed = ledger.deployed_notional(sid)
            out[sid] = SleeveBudget(sid, equity, deployed,
                                    max(0.0, equity - deployed))
        return out
