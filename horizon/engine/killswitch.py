"""Portfolio kill switch.

Blocks all new entries; never blocks exits; never kills the process (a crash
loop on a hosted runtime is worse than an idle one). Triggered by a file, an
env var, or programmatically.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Optional, Tuple


class KillSwitch:
    def __init__(self, flag_path: Optional[Path] = None,
                 env_var: str = "HORIZON_KILL_SWITCH"):
        self.flag_path = flag_path
        self.env_var = env_var
        self._tripped = False
        self._reason = ""

    def trigger(self, reason: str) -> None:
        self._tripped = True
        self._reason = reason

    def is_triggered(self) -> Tuple[bool, str]:
        if self._tripped:
            return True, self._reason
        if self.flag_path and self.flag_path.exists():
            return True, f"kill-switch file present: {self.flag_path}"
        if os.environ.get(self.env_var, "0") == "1":
            return True, f"{self.env_var}=1"
        return False, ""

    def reset(self) -> bool:
        """Clear a programmatic trigger; refuses while file/env still active."""
        if self.flag_path and self.flag_path.exists():
            return False
        if os.environ.get(self.env_var, "0") == "1":
            return False
        self._tripped = False
        self._reason = ""
        return True
