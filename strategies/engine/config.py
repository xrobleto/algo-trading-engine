"""
Engine Configuration — sleeve allocations, order prefixes, and operational settings.

All engine-level config lives here. Strategy-specific config stays in the strategy files.
"""

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Optional, Set


# =============================================================================
# SLEEVE CONFIGURATION
# =============================================================================

@dataclass
class SleeveConfig:
    """Capital sleeve configuration for a single strategy."""
    strategy_id: str                    # "TREND" or "SIMPLE"
    allocation_pct: float               # fraction of total equity (e.g., 0.85)
    order_prefix: str                   # client_order_id prefix (e.g., "ENG_TREND_")
    legacy_prefixes: tuple = ()         # old prefixes to recognize during migration

    # Position limits
    max_positions: Optional[int] = None  # None = unlimited (strategy manages internally)
    max_daily_loss_pct: float = 0.10     # max daily loss as fraction of sleeve equity

    # Known symbols for reconciliation fallback (classify positions with no order prefix)
    known_symbols: Set[str] = field(default_factory=set)

    # Probation controls
    probation: bool = False              # enable strict probation mode
    auto_halt_on_anomaly: bool = False   # halt strategy on execution anomaly
    max_fill_deviation_pct: float = 0.02 # halt if fill deviates >2% from expected
    max_consecutive_errors: int = 5      # halt after N consecutive tick errors

    def all_prefixes(self) -> tuple:
        """All recognized prefixes (current + legacy) for reconciliation."""
        return (self.order_prefix,) + self.legacy_prefixes


# =============================================================================
# ENGINE CONFIGURATION
# =============================================================================

@dataclass
class EngineConfig:
    """Top-level engine configuration."""
    sleeves: Dict[str, SleeveConfig] = field(default_factory=dict)
    cash_reserve_pct: float = 0.10      # fraction of equity held as cash buffer

    # State paths (resolved at runtime)
    state_dir: str = ""                  # set from ALGO_OUTPUT_DIR env var
    ownership_file: str = "engine_ownership.json"
    engine_log_file: str = "engine_heartbeat.jsonl"

    # Portfolio-level safety
    kill_switch_file: str = "HALT_ALL_TRADING"
    reconcile_on_startup: bool = True

    # Cross-sleeve correlated-exposure guard (prevents hidden beta stacking across sleeves)
    correlation_guard_enabled: bool = True
    correlation_cap: float = 1.25       # max net risk-on exposure (× total equity) across ALL sleeves
    cluster_cap: float = 0.50           # max gross beta-weighted exposure per correlated CLUSTER
                                        # (× equity). Aug-2026 audit: −$590 semis loss occurred entirely
                                        # under the net-beta cap; concentration must be capped per-cluster.

    # Engine loop timing
    tick_interval_sec: float = 5.0       # main loop sleep
    reconcile_interval_sec: float = 60.0   # periodic reconciliation cadence (1 min) — drives pending→filled ledger freshness (see Patch 9)

    # Live trading safety
    live_trading: bool = False
    live_confirmation: str = ""

    def __post_init__(self):
        if not self.state_dir:
            algo_output = os.getenv("ALGO_OUTPUT_DIR", "")
            if algo_output:
                self.state_dir = str(Path(algo_output) / "data" / "state")
            else:
                from engine.platform import get_data_dir
                self.state_dir = str(get_data_dir() / "data" / "state")

        # Separate ownership files by mode to prevent live/paper cross-contamination
        mode = "live" if self.live_trading else "paper"
        if self.ownership_file == "engine_ownership.json":
            self.ownership_file = f"engine_ownership_{mode}.json"

    @property
    def ownership_path(self) -> str:
        return str(Path(self.state_dir) / self.ownership_file)

    @property
    def kill_switch_path(self) -> str:
        return str(Path(self.state_dir) / self.kill_switch_file)

    @property
    def engine_log_path(self) -> str:
        return str(Path(self.state_dir).parent.parent / "logs" / self.engine_log_file)


# =============================================================================
# DEFAULT CONFIGURATION
# =============================================================================

def build_default_config() -> EngineConfig:
    """Build the default engine config with Trend + Simple sleeves."""

    # Trend Bot's known ETF universe (for reconciliation fallback)
    trend_known_symbols = {
        # Equity ETFs
        "SPY", "QQQ", "IWM",
        # Sector ETFs
        "XLK", "XLF", "XLE", "XLV", "XLI", "XLY", "XLC",
        "SMH", "IBB", "XHB",
        # Factor ETFs
        "MTUM", "QUAL",
        # Leveraged ETFs
        "TQQQ", "UPRO", "SOXL", "TECL", "FAS",
        # Momentum/thematic ETFs
        "ARKK", "XBI", "KWEB", "SOXX", "IGV", "CIBR", "SKYY",
        # Defensive ETFs — IEF, TLT, GLD, DBC moved to CROSSASSET sleeve
        # Cash equivalents
        "SGOV", "BIL",
    }

    crossasset_known_symbols = {
        # Rates (long + inverse)
        "TLT", "IEF", "SHY", "TBT",
        # Commodities (long + inverse)
        "GLD", "SLV", "DBC", "USO", "UNG", "DBA", "GLL",
        # FX proxies
        "UUP", "FXE", "FXY",
    }

    # HORIZON reserve sleeve (2026-08-12): capital carved out for the Horizon
    # engine's 30-day live validation (PULSE+ROTATION — the only strategies to
    # pass a pre-registered faithful walk-forward bar). Horizon runs as a
    # SEPARATE Railway service on THIS account, capped by HORIZON_CAPITAL_CAP.
    # This sleeve has NO adapter — it exists so (a) the reconciler classifies
    # Horizon's HZN_* orders/positions instead of kill-switching on them, and
    # (b) the allocation is reserved away from TREND/CROSSASSET sizing.
    # Horizon trades index-equivalent ETFs the unified sleeves never touch.
    horizon_sleeve = SleeveConfig(
        strategy_id="HORIZON",
        allocation_pct=1.00,        # CP3 (2026-09-05): 0.60 -> 1.00 on validated evidence (Candidate B cleared the full pre-registered bar; horizon/docs/VALIDATION.md). HORIZON_CAPITAL_CAP removed; Horizon sizes against the whole account, bounded by its funding guard. History: CP1 0.20->0.40, CP2 0.40->0.60 with cap $3,850.
        order_prefix="HZN_",
        known_symbols={"QQQM", "IEFA", "VGLT", "IAU", "PDBC", "SHV",
                       "QLD"},      # CP3: PULSE expresses leverage via QLD
        max_positions=None,
        max_daily_loss_pct=1.0,     # not engine-enforced; Horizon has its own risk layer
        probation=False,
        auto_halt_on_anomaly=False,
    )

    trend_sleeve = SleeveConfig(
        strategy_id="TREND",
        allocation_pct=0.00,        # CP2 (2026-08-23): RETIRED. Failed faithful validation (trend_faithful_verdict_2026-08-12.md: ~-1.6% chained 2019-2026 vs SPY +140%; 4/5 criteria failed) and cumulative live PF 0.56. Registered at $0 so a residual book can still be flattened; adapter skips once flat. Flip back to a nonzero value only with new validated evidence.
        order_prefix="ENG_TREND_",
        legacy_prefixes=("TBOT_",),
        known_symbols=trend_known_symbols,
        max_positions=None,         # Trend bot manages its own position count
        max_daily_loss_pct=0.10,    # 10% of sleeve
        probation=False,
        auto_halt_on_anomaly=False,
    )

    simple_sleeve = SleeveConfig(
        strategy_id="SIMPLE",
        allocation_pct=0.00,        # 2026-06-16: PARKED. Faithful redesign (parabolic-continuation ORB) failed the pre-registered bar — 0/7 regime windows positive on a single-source-of-truth harness; live PF 0.19, −$53. Allocation redeployed to TREND/CROSSASSET. Registered at $0 so it flattens any residual position but opens nothing (can_deploy blocks on zero sleeve_available). Flip back to 0.10 to revive. See project_notes/simple_v2_validation_2026-06-15.md.
        order_prefix="ENG_SIMPLE_",
        legacy_prefixes=("dir_",),
        # Phase A: off probation after 16 trading days of zero positions
        # under the 1-position/2%-loss/80%-size probation regime. Restores
        # simple_bot defaults — MAX_POSITIONS=4, POSITION_SIZE_PCT=0.35,
        # MAX_DAILY_LOSS_PCT=0.04 — so up to 4 concurrent positions at 35%
        # of sleeve each. Worst-case daily loss is bounded by the 4% cap
        # (~$60 on a $1.5k sleeve). auto_halt_on_anomaly stays on as a
        # fill-quality safety belt.
        max_positions=None,         # Bot manages internally (default 4)
        max_daily_loss_pct=0.04,    # Match simple_bot default (no probation cap)
        probation=False,
        auto_halt_on_anomaly=True,
        max_fill_deviation_pct=0.02,
    )

    crossasset_sleeve = SleeveConfig(
        strategy_id="CROSSASSET",
        allocation_pct=0.00,        # CP1 (pre-approved): RETIRED. Live PF 0.41 cumulative; role redundant with Horizon's validated ROTATION. Book flattened before this deploys (see cp1_runbook). Registered at $0 for classification.
        order_prefix="ENG_XASSET_",
        legacy_prefixes=("XABOT_",),
        known_symbols=crossasset_known_symbols,
        max_positions=None,         # Bot manages internally (~15 ETFs max)
        max_daily_loss_pct=0.15,    # 15% of sleeve (matches portfolio trailing stop)
        probation=False,
        auto_halt_on_anomaly=False,
    )

    # CP3 allocation check: 0.00 (TREND retired) + 0.00 (SIMPLE parked)
    #   + 0.00 (CROSSASSET retired) + 1.00 (HORIZON, the only validated
    #   strategies) + 0.00 cash = 1.00. Horizon keeps its own ~3% funding
    #   buffer; the engine reserves nothing.
    config = EngineConfig(
        sleeves={
            "TREND": trend_sleeve,
            "SIMPLE": simple_sleeve,
            "CROSSASSET": crossasset_sleeve,
            "HORIZON": horizon_sleeve,
        },
        cash_reserve_pct=0.00,      # CP3 (2026-09-05): rolled into HORIZON
        live_trading=os.getenv("LIVE_TRADING", "0") == "1",
        live_confirmation=os.getenv("I_UNDERSTAND_LIVE_TRADING", ""),
    )

    return config


def validate_config(config: EngineConfig) -> None:
    """Validate config or raise RuntimeError."""
    total_alloc = sum(s.allocation_pct for s in config.sleeves.values()) + config.cash_reserve_pct
    if abs(total_alloc - 1.0) > 0.001:
        raise RuntimeError(
            f"Sleeve allocations + cash reserve must sum to 1.0, got {total_alloc:.3f}. "
            f"Sleeves: {[(s.strategy_id, s.allocation_pct) for s in config.sleeves.values()]}, "
            f"cash_reserve: {config.cash_reserve_pct}"
        )

    for sid, sleeve in config.sleeves.items():
        if sleeve.allocation_pct < 0 or sleeve.allocation_pct > 1:
            raise RuntimeError(f"Sleeve {sid} allocation must be 0-1, got {sleeve.allocation_pct}")
        if not sleeve.order_prefix:
            raise RuntimeError(f"Sleeve {sid} must have an order_prefix")

    if config.live_trading and config.live_confirmation.upper() != "YES":
        raise RuntimeError(
            "LIVE_TRADING=1 requires I_UNDERSTAND_LIVE_TRADING=YES. "
            "Set this env var to confirm live trading."
        )

    # Ensure state directory exists
    Path(config.state_dir).mkdir(parents=True, exist_ok=True)
