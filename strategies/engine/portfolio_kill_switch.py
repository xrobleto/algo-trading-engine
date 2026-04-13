"""
Portfolio-Level Kill Switch — halts ALL strategies on critical safety issues.

Triggers on:
- File-based kill switch (HALT_ALL_TRADING file exists)
- Environment variable (KILL_SWITCH=1)
- Ownership conflicts (same symbol owned by multiple strategies)
- State corruption detected by engine

When triggered:
- Blocks ALL new entries across ALL strategies
- Exits are still allowed (strategies can close positions)
- Requires manual intervention to clear
"""

import logging
import os
from datetime import datetime, timezone
from typing import Optional, Tuple

log = logging.getLogger("Engine")


class PortfolioKillSwitch:
    """
    Portfolio-level kill switch that blocks all new trading when triggered.

    Different from per-strategy kill switches:
    - This halts the ENTIRE engine (all strategies)
    - Per-strategy kill switches only halt one strategy
    - This fires on cross-strategy issues (conflicts, corruption)
    """

    def __init__(self, kill_switch_path: str = "", env_var: str = "KILL_SWITCH"):
        self._kill_switch_path = kill_switch_path
        self._env_var = env_var
        self._triggered = False
        self._trigger_reason: Optional[str] = None
        self._triggered_at: Optional[str] = None
        self._exits_allowed = True  # always allow exits even when killed

    # -------------------------------------------------------------------------
    # TRIGGER
    # -------------------------------------------------------------------------

    def trigger(self, reason: str) -> None:
        """Manually trigger the kill switch."""
        if self._triggered:
            log.warning(f"[KILL_SWITCH] Already triggered. New reason: {reason}")
            return

        self._triggered = True
        self._trigger_reason = reason
        self._triggered_at = datetime.now(timezone.utc).isoformat()

        log.critical("=" * 60)
        log.critical(f"PORTFOLIO KILL SWITCH TRIGGERED: {reason}")
        log.critical(f"ALL new entries BLOCKED. Exits still allowed.")
        log.critical(f"Clear by removing {self._kill_switch_path} or setting {self._env_var}=0")
        log.critical("=" * 60)

    # -------------------------------------------------------------------------
    # CHECK
    # -------------------------------------------------------------------------

    def is_triggered(self) -> Tuple[bool, Optional[str]]:
        """
        Check if kill switch is active.

        Checks:
        1. Internal trigger flag (from code-detected issues)
        2. File-based trigger (HALT_ALL_TRADING file exists)
        3. Environment variable (KILL_SWITCH=1)

        Returns:
            (is_triggered: bool, reason: str or None)
        """
        # Already triggered programmatically
        if self._triggered:
            return True, self._trigger_reason

        # File-based trigger
        if self._kill_switch_path and os.path.exists(self._kill_switch_path):
            reason = f"Kill switch file exists: {self._kill_switch_path}"
            self.trigger(reason)
            return True, reason

        # Environment variable trigger
        if os.getenv(self._env_var, "0") == "1":
            reason = f"Kill switch env var {self._env_var}=1"
            self.trigger(reason)
            return True, reason

        return False, None

    # -------------------------------------------------------------------------
    # PROPERTIES
    # -------------------------------------------------------------------------

    @property
    def triggered(self) -> bool:
        return self._triggered

    @property
    def reason(self) -> Optional[str]:
        return self._trigger_reason

    @property
    def exits_allowed(self) -> bool:
        """Exits are always allowed even when kill switch is active."""
        return self._exits_allowed

    # -------------------------------------------------------------------------
    # RESET
    # -------------------------------------------------------------------------

    def reset(self) -> None:
        """
        Reset the kill switch. Requires manual intervention.

        NOTE: This does NOT remove the kill switch file or env var.
        Those must be cleared separately before reset will stick.
        """
        # Don't reset if external triggers still active
        if self._kill_switch_path and os.path.exists(self._kill_switch_path):
            log.warning(
                f"[KILL_SWITCH] Cannot reset — file still exists: {self._kill_switch_path}"
            )
            return

        if os.getenv(self._env_var, "0") == "1":
            log.warning(
                f"[KILL_SWITCH] Cannot reset — env var {self._env_var} still set to 1"
            )
            return

        self._triggered = False
        self._trigger_reason = None
        self._triggered_at = None
        log.info("[KILL_SWITCH] Reset successfully. Trading resumed.")

    def get_status(self) -> dict:
        """Get kill switch status for diagnostics."""
        return {
            "triggered": self._triggered,
            "reason": self._trigger_reason,
            "triggered_at": self._triggered_at,
            "exits_allowed": self._exits_allowed,
            "file_path": self._kill_switch_path,
            "env_var": self._env_var,
        }
