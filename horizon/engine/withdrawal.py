"""High-water-mark monthly profit skim — the user's income mechanism.

Tracks the high-water-mark of *gross* value (current equity + everything
already withdrawn). When gross value sets a new high beyond a small buffer, a
fraction of the new gain is skimmed. In down months nothing is withdrawn. The
engine only *signals* a withdrawal; it never moves money itself.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from ..config import WithdrawalConfig


@dataclass
class WithdrawalEvent:
    date: object
    amount: float
    equity_after: float


@dataclass
class WithdrawalEngine:
    config: WithdrawalConfig
    withdrawn_total: float = 0.0
    peak_gross: Optional[float] = None
    events: List[WithdrawalEvent] = field(default_factory=list)

    def maybe_withdraw(self, date, equity: float) -> float:
        """Consider a withdrawal at `date`; return the amount withdrawn (>= 0)."""
        gross = equity + self.withdrawn_total          # value as if never withdrawn
        if self.peak_gross is None:
            self.peak_gross = gross
        if not self.config.enabled:
            self.peak_gross = max(self.peak_gross, gross)
            return 0.0

        threshold = self.peak_gross * (1.0 + self.config.buffer_pct)
        if gross <= threshold:
            self.peak_gross = max(self.peak_gross, gross)
            return 0.0

        amount = self.config.skim_fraction * (gross - self.peak_gross)
        if amount < self.config.min_withdrawal:
            self.peak_gross = max(self.peak_gross, gross)
            return 0.0

        self.withdrawn_total += amount
        equity_after = equity - amount
        self.events.append(WithdrawalEvent(date, amount, equity_after))
        # Lock in the new high-water-mark of gross value.
        self.peak_gross = equity_after + self.withdrawn_total
        return amount
