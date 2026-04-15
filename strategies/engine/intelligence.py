"""
Market Intelligence Layer — fuses alternative data into a shared MarketContext.

Polls existing data sources (FRED macro, Polymarket, Reddit, news, event calendar)
every 10 minutes, produces a MarketContext object that the engine uses to:
1. Dynamically adjust sleeve allocations (±10% of base)
2. Scale position sizing via risk multipliers (0.5x–1.5x)
3. Gate new entries in CRISIS regime (block SIMPLE, allow exits)

Design principles:
- Every data source is error-isolated — one failure → neutral fallback
- All fusion is deterministic, rules-based, auditable (no LLMs)
- MarketContext is immutable after creation, not persisted to disk
- If the entire layer fails, strategies run exactly as before
"""

import dataclasses
import json
import logging
import os
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from engine.config import EngineConfig

# ---------------------------------------------------------------------------
# PATH SETUP — import existing utilities and ai_manager modules
# ---------------------------------------------------------------------------
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent  # G:\My Drive\Algo_Trading
_UTILITIES_PATH = str(_PROJECT_ROOT / "utilities")
_AI_MANAGER_PATH = str(_PROJECT_ROOT / "ai_manager" / "src")

if _UTILITIES_PATH not in sys.path:
    sys.path.insert(0, _UTILITIES_PATH)
if _AI_MANAGER_PATH not in sys.path:
    sys.path.insert(0, _AI_MANAGER_PATH)

log = logging.getLogger("Engine")


# =============================================================================
# ENUMS & DATACLASSES
# =============================================================================

class MarketRegime(str, Enum):
    RISK_ON = "RISK_ON"
    CAUTIOUS = "CAUTIOUS"
    RISK_OFF = "RISK_OFF"
    CRISIS = "CRISIS"


@dataclass
class SourceScore:
    """Individual data source contribution to the regime decision."""
    source: str           # "macro", "polymarket", "reddit", "news", "event_calendar"
    score: float          # 0-100 (50 = neutral)
    weight: float         # weight used in fusion
    raw_data: dict        # source-specific data for audit trail
    available: bool       # whether the source responded successfully
    fetch_time_ms: float = 0.0  # how long the fetch took


@dataclass
class SleeveAdjustment:
    """Per-sleeve allocation and risk adjustments."""
    strategy_id: str
    base_allocation: float         # static base from config (e.g. 0.65)
    adjusted_allocation: float     # after intelligence adjustment
    allocation_multiplier: float   # applied to base (0.5-1.5 range)
    risk_multiplier: float         # 0.5-1.5 for position sizing
    entry_allowed: bool            # False = block new entries
    entry_gate_reason: str         # why entries are blocked, or "ok"
    # WS4/WS6: cumulative list of modifier reasons applied this refresh
    # e.g. ["regime:CRISIS x0.70", "chop:x0.75 (score=0.58)"]
    adjustment_reasons: List[str] = field(default_factory=list)


@dataclass
class MarketContext:
    """
    Unified market intelligence snapshot.
    Refreshed every 10 minutes. Immutable after creation.
    """
    # Regime
    regime: MarketRegime
    regime_score: float            # 0-100 composite
    regime_changed: bool           # True if regime changed from previous refresh

    # Per-sleeve adjustments
    sleeve_adjustments: Dict[str, SleeveAdjustment]

    # Global
    global_risk_multiplier: float  # 0.5-1.5
    cash_reserve_pct: float        # effective cash reserve (>= config minimum)

    # Source transparency
    source_scores: List[SourceScore]

    # Timestamps
    timestamp: datetime
    refresh_count: int

    # Upcoming events
    next_event_name: Optional[str] = None
    next_event_date: Optional[str] = None
    next_event_risk: Optional[str] = None

    @property
    def is_defensive(self) -> bool:
        return self.regime in (MarketRegime.RISK_OFF, MarketRegime.CRISIS)


# =============================================================================
# CONSTANTS
# =============================================================================

# Source weights (sum to 1.0)
SOURCE_WEIGHTS = {
    "macro": 0.35,
    "polymarket": 0.20,
    "event_calendar": 0.20,
    "news": 0.15,
    "reddit": 0.10,
}

# Regime thresholds (on 0-100 composite score)
REGIME_THRESHOLDS = {
    MarketRegime.RISK_ON: 65,     # score >= 65
    MarketRegime.CAUTIOUS: 50,    # 50 <= score < 65
    MarketRegime.RISK_OFF: 35,    # 35 <= score < 50
    # CRISIS: score < 35
}

# Global risk multiplier by regime
REGIME_RISK_MULTIPLIERS = {
    MarketRegime.RISK_ON: 1.2,
    MarketRegime.CAUTIOUS: 1.0,
    MarketRegime.RISK_OFF: 0.7,
    MarketRegime.CRISIS: 0.5,
}

# Per-sleeve allocation multipliers by regime
SLEEVE_ALLOCATION_MULTIPLIERS = {
    MarketRegime.RISK_ON:   {"TREND": 1.15, "SIMPLE": 1.10, "CROSSASSET": 0.85},
    MarketRegime.CAUTIOUS:  {"TREND": 1.00, "SIMPLE": 1.00, "CROSSASSET": 1.00},
    MarketRegime.RISK_OFF:  {"TREND": 0.85, "SIMPLE": 0.85, "CROSSASSET": 1.15},
    MarketRegime.CRISIS:    {"TREND": 0.70, "SIMPLE": 0.65, "CROSSASSET": 1.30},
}

# Per-sleeve risk multipliers by regime
SLEEVE_RISK_MULTIPLIERS = {
    MarketRegime.RISK_ON:   {"TREND": 1.20, "SIMPLE": 1.10, "CROSSASSET": 1.00},
    MarketRegime.CAUTIOUS:  {"TREND": 1.00, "SIMPLE": 1.00, "CROSSASSET": 1.00},
    MarketRegime.RISK_OFF:  {"TREND": 0.80, "SIMPLE": 0.75, "CROSSASSET": 1.10},
    MarketRegime.CRISIS:    {"TREND": 0.60, "SIMPLE": 0.50, "CROSSASSET": 1.20},
}

# Guardrails
ALLOCATION_SWING_PCT = 0.10      # max ±10% of base
RISK_MULTIPLIER_MIN = 0.5
RISK_MULTIPLIER_MAX = 1.5
CASH_RESERVE_FLOOR = 0.02        # hard minimum 2% cash
MAX_ALLOCATION_VELOCITY = 0.02   # max 2% absolute change per refresh

# Polymarket risk level → score mapping
POLYMARKET_RISK_SCORES = {
    "LOW": 75,
    "MEDIUM": 50,
    "HIGH": 25,
    "EXTREME": 10,
}

# Stale context auto-revert (30 minutes)
STALE_CONTEXT_SEC = 1800

# =============================================================================
# WS3 + WS4: MARKET-STRUCTURE MODIFIERS (breadth gate + chop dampener)
# =============================================================================
# Feature-flagged via env vars. Default OFF until validated in paper-trade.
# See reports/engine_backtest_2026-04-14.md §Recommendations and
# backtest/_runs/engine_regime_2026-04-14/composite/replay/replay_summary.md
# for the decision trail behind these modifiers.

MARKET_STRUCTURE_GATE_ENABLED = os.getenv("INTEL_MARKET_STRUCTURE_GATE", "0") == "1"
CHOP_DAMPENER_ENABLED = os.getenv("INTEL_CHOP_DAMPENER", "0") == "1"

# WS3 tunables (replay-validated thresholds, see replay_summary §WS3+WS4 tuning)
SIMPLE_BREADTH_THRESHOLD_SCORE = 0.75   # narrowness_score threshold
SIMPLE_BREADTH_SUSTAIN_DAYS = 10        # must be sustained N days
SIMPLE_BREADTH_WINDOW = 63              # gap lookback window

# WS4 tunables
CHOP_RAMP_LO = 0.40           # below this, no dampening
CHOP_RAMP_HI = 0.60           # at or above, full dampening
TREND_CHOP_FLOOR = 0.60       # minimum TREND multiplier when fully dampened

# Polygon daily-bar history needed for narrowness (63d window + 10d sustain + buffer)
_STRUCTURE_HISTORY_DAYS = 120


# =============================================================================
# MARKET INTELLIGENCE LAYER
# =============================================================================

class MarketIntelligenceLayer:
    """
    Fuses alternative data sources into a unified MarketContext.

    Polls FRED macro, Polymarket, Reddit, news, and event calendar
    every refresh_interval_sec, producing a MarketContext that the engine
    uses for dynamic allocation, risk modulation, and entry filtering.
    """

    def __init__(
        self,
        engine_config: EngineConfig,
        refresh_interval_sec: int = 600,
    ):
        self._config = engine_config
        self._refresh_interval = refresh_interval_sec
        self._last_refresh_at: float = 0.0
        self._refresh_count: int = 0
        self._current_ctx: Optional[MarketContext] = None
        self._previous_regime: Optional[MarketRegime] = None

        # Track previous allocations for velocity limiting
        self._previous_allocations: Dict[str, float] = {}

        # Lazily initialized data source clients
        self._macro_analyzer = None
        self._polymarket_client = None
        self._reddit_provider = None
        self._event_calendar: Optional[dict] = None

        # WS3/WS4 microstructure signal state (refreshed once per daily bar)
        self._ms_last_fetch_at: float = 0.0
        self._ms_cache: Dict[str, Any] = {
            "narrowness_score": 0.5,
            "narrowness_sustained": False,
            "chop_score": 0.0,
            "available": False,
            "fetch_time_ms": 0.0,
        }

        # Intelligence-specific logger
        self._intel_log = logging.getLogger("Intelligence")
        self._setup_logging()

        # JSONL decision log
        self._jsonl_path = self._get_log_dir() / "intelligence_decisions.jsonl"

    # -------------------------------------------------------------------------
    # LOGGING SETUP
    # -------------------------------------------------------------------------

    def _get_log_dir(self) -> Path:
        from engine.platform import get_data_dir
        log_dir = get_data_dir() / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        return log_dir

    def _setup_logging(self) -> None:
        if self._intel_log.handlers:
            return  # already configured (e.g. reused across restarts)

        from logging.handlers import RotatingFileHandler

        log_dir = self._get_log_dir()
        handler = RotatingFileHandler(
            str(log_dir / "intelligence.log"),
            maxBytes=20 * 1024 * 1024,
            backupCount=3,
            encoding="utf-8",
        )
        handler.setFormatter(logging.Formatter(
            '[%(asctime)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S'
        ))
        self._intel_log.addHandler(handler)
        self._intel_log.setLevel(logging.INFO)

    # -------------------------------------------------------------------------
    # PUBLIC API
    # -------------------------------------------------------------------------

    def should_refresh(self) -> bool:
        """Check if enough time has elapsed since last refresh."""
        return (time.time() - self._last_refresh_at) >= self._refresh_interval

    def get_context(self) -> Optional[MarketContext]:
        """Return the most recent MarketContext."""
        # Auto-revert to CAUTIOUS if context is stale
        if (self._current_ctx
                and (time.time() - self._last_refresh_at) > STALE_CONTEXT_SEC):
            log.warning("[INTELLIGENCE] Context stale (>30 min) — reverting to CAUTIOUS")
            self._current_ctx = self._build_neutral_context()
        return self._current_ctx

    def refresh(self) -> MarketContext:
        """
        Poll all data sources, fuse signals, produce a new MarketContext.
        Each data source is wrapped in try/except for error isolation.
        """
        self._refresh_count += 1
        t0 = time.time()

        # Fetch all sources (each error-isolated)
        scores = [
            self._fetch_macro(),
            self._fetch_polymarket(),
            self._fetch_event_calendar(),
            self._fetch_news(),
            self._fetch_reddit(),
        ]

        # Fuse into composite score
        composite = self._fuse_scores(scores)

        # Map to regime
        regime = self._score_to_regime(composite)
        regime_changed = (self._previous_regime is not None
                          and regime != self._previous_regime)
        self._previous_regime = regime

        # WS3/WS4: refresh microstructure signals if enabled (cheap — once per refresh)
        if MARKET_STRUCTURE_GATE_ENABLED or CHOP_DAMPENER_ENABLED:
            self._refresh_market_structure()

        # Compute per-sleeve adjustments
        sleeve_adjustments = self._compute_sleeve_adjustments(regime)

        # Global risk multiplier
        global_risk = REGIME_RISK_MULTIPLIERS.get(regime, 1.0)
        global_risk = max(RISK_MULTIPLIER_MIN, min(RISK_MULTIPLIER_MAX, global_risk))

        # Effective cash reserve
        cash = max(self._config.cash_reserve_pct, CASH_RESERVE_FLOOR)

        # Find next event
        next_name, next_date, next_risk = self._get_next_event()

        ctx = MarketContext(
            regime=regime,
            regime_score=composite,
            regime_changed=regime_changed,
            sleeve_adjustments=sleeve_adjustments,
            global_risk_multiplier=global_risk,
            cash_reserve_pct=cash,
            source_scores=scores,
            timestamp=datetime.now(timezone.utc),
            refresh_count=self._refresh_count,
            next_event_name=next_name,
            next_event_date=next_date,
            next_event_risk=next_risk,
        )

        self._current_ctx = ctx
        self._last_refresh_at = time.time()

        # Log the decision
        elapsed_ms = (time.time() - t0) * 1000
        self._log_refresh(ctx, elapsed_ms)

        return ctx

    # -------------------------------------------------------------------------
    # SIGNAL FUSION
    # -------------------------------------------------------------------------

    def _fuse_scores(self, scores: List[SourceScore]) -> float:
        """Weighted average of available source scores."""
        total_weight = 0.0
        weighted_sum = 0.0

        for s in scores:
            if s.available:
                weighted_sum += s.score * s.weight
                total_weight += s.weight
            # Unavailable sources are skipped; remaining weights renormalize

        if total_weight <= 0:
            return 50.0  # all sources failed → neutral

        return weighted_sum / total_weight

    @staticmethod
    def _score_to_regime(score: float) -> MarketRegime:
        if score >= REGIME_THRESHOLDS[MarketRegime.RISK_ON]:
            return MarketRegime.RISK_ON
        elif score >= REGIME_THRESHOLDS[MarketRegime.CAUTIOUS]:
            return MarketRegime.CAUTIOUS
        elif score >= REGIME_THRESHOLDS[MarketRegime.RISK_OFF]:
            return MarketRegime.RISK_OFF
        else:
            return MarketRegime.CRISIS

    # -------------------------------------------------------------------------
    # SLEEVE ADJUSTMENTS
    # -------------------------------------------------------------------------

    def _compute_sleeve_adjustments(
        self, regime: MarketRegime
    ) -> Dict[str, SleeveAdjustment]:
        """Compute bounded, velocity-limited per-sleeve adjustments.

        Pipeline:
          1. Base regime multiplier from SLEEVE_ALLOCATION_MULTIPLIERS
          2. WS3 SIMPLE breadth gate modifier (if enabled + sustained narrow + RISK_ON/CAUTIOUS)
          3. WS4 TREND chop dampener modifier (if enabled)
          4. Bound within ±ALLOCATION_SWING_PCT of base
          5. Apply 2%/refresh velocity limit
          6. Normalize with cash reserve
        """
        adjustments = {}
        raw_allocations = {}

        # Snapshot microstructure signals once per refresh
        ms = self._ms_cache if (MARKET_STRUCTURE_GATE_ENABLED or CHOP_DAMPENER_ENABLED) else None

        for strategy_id, sleeve_config in self._config.sleeves.items():
            base = sleeve_config.allocation_pct
            reasons: List[str] = []

            # Allocation multiplier from regime table
            alloc_mult = SLEEVE_ALLOCATION_MULTIPLIERS.get(
                regime, {}
            ).get(strategy_id, 1.0)
            reasons.append(f"regime:{regime.value}:x{alloc_mult:.2f}")

            # WS3: SIMPLE breadth gate modifier (alloc haircut in addition to entry gate)
            if (MARKET_STRUCTURE_GATE_ENABLED
                    and strategy_id == "SIMPLE"
                    and ms is not None
                    and ms.get("available")
                    and ms.get("narrowness_sustained")
                    and regime in (MarketRegime.RISK_ON, MarketRegime.CAUTIOUS)):
                # Alloc haircut alongside entry gate — reinforces reduction
                gate_mod = 0.5
                alloc_mult *= gate_mod
                reasons.append(
                    f"breadth_gate:x{gate_mod:.2f} (narrow_score="
                    f"{ms.get('narrowness_score', 0.0):.2f})"
                )

            # WS4: TREND chop dampener
            if (CHOP_DAMPENER_ENABLED
                    and strategy_id == "TREND"
                    and ms is not None
                    and ms.get("available")):
                chop_mod = self._chop_dampener(float(ms.get("chop_score", 0.0)))
                if chop_mod < 1.0:
                    alloc_mult *= chop_mod
                    reasons.append(
                        f"chop_dampener:x{chop_mod:.2f} (chop_score="
                        f"{ms.get('chop_score', 0.0):.2f})"
                    )

            # Bound allocation within ±ALLOCATION_SWING_PCT of base
            adjusted = self._bound_allocation(base, alloc_mult, ALLOCATION_SWING_PCT)

            # Velocity limit: max 2% absolute change per refresh
            prev = self._previous_allocations.get(strategy_id, base)
            adjusted = self._apply_velocity_limit(prev, adjusted, MAX_ALLOCATION_VELOCITY)

            raw_allocations[strategy_id] = adjusted

            # Risk multiplier
            risk_mult = SLEEVE_RISK_MULTIPLIERS.get(
                regime, {}
            ).get(strategy_id, 1.0)
            risk_mult = max(RISK_MULTIPLIER_MIN, min(RISK_MULTIPLIER_MAX, risk_mult))

            # Entry gate (may consume WS3 microstructure signal for SIMPLE)
            entry_allowed, gate_reason = self._compute_entry_gate(regime, strategy_id, ms)

            adjustments[strategy_id] = SleeveAdjustment(
                strategy_id=strategy_id,
                base_allocation=base,
                adjusted_allocation=adjusted,
                allocation_multiplier=alloc_mult,
                risk_multiplier=risk_mult,
                entry_allowed=entry_allowed,
                entry_gate_reason=gate_reason,
                adjustment_reasons=reasons,
            )

        # Normalize: ensure allocations + cash ≤ 1.0
        normalized, effective_cash = self._normalize_allocations(
            raw_allocations,
            self._config.cash_reserve_pct,
        )

        # Apply normalized values back
        for sid, adj in adjustments.items():
            adj.adjusted_allocation = normalized[sid]

        # Save for velocity limiting next refresh
        self._previous_allocations = dict(normalized)

        return adjustments

    @staticmethod
    def _bound_allocation(base: float, multiplier: float, swing: float) -> float:
        adjusted = base * multiplier
        lower = base * (1.0 - swing)
        upper = base * (1.0 + swing)
        return max(lower, min(upper, adjusted))

    @staticmethod
    def _apply_velocity_limit(current: float, target: float, max_delta: float) -> float:
        delta = target - current
        if abs(delta) > max_delta:
            return current + (max_delta if delta > 0 else -max_delta)
        return target

    @staticmethod
    def _normalize_allocations(
        allocations: Dict[str, float],
        base_cash_reserve: float,
    ) -> Tuple[Dict[str, float], float]:
        effective_cash = max(base_cash_reserve, CASH_RESERVE_FLOOR)
        total_alloc = sum(allocations.values())
        max_alloc = 1.0 - effective_cash

        if total_alloc > max_alloc:
            scale = max_alloc / total_alloc
            allocations = {k: v * scale for k, v in allocations.items()}

        return allocations, effective_cash

    @staticmethod
    def _compute_entry_gate(
        regime: MarketRegime,
        strategy_id: str,
        microstructure: Optional[Dict[str, Any]] = None,
    ) -> Tuple[bool, str]:
        # Existing CRISIS gate
        if regime == MarketRegime.CRISIS and strategy_id == "SIMPLE":
            return False, "CRISIS regime: intraday entries blocked"
        # WS3: sustained-narrow breadth gate for SIMPLE in "looks calm but isn't" regimes
        if (MARKET_STRUCTURE_GATE_ENABLED
                and strategy_id == "SIMPLE"
                and microstructure is not None
                and microstructure.get("available")
                and microstructure.get("narrowness_sustained")
                and regime in (MarketRegime.RISK_ON, MarketRegime.CAUTIOUS)):
            nscore = microstructure.get("narrowness_score", 0.0)
            return (
                False,
                f"breadth_narrow: sustained narrow leadership detected "
                f"(score={nscore:.2f}, regime={regime.value})",
            )
        return True, "ok"

    @staticmethod
    def _chop_dampener(chop_score: float) -> float:
        """WS4 dampener: ramp 1.0 at chop<=CHOP_RAMP_LO down to TREND_CHOP_FLOOR at >=CHOP_RAMP_HI."""
        if chop_score <= CHOP_RAMP_LO:
            return 1.0
        if chop_score >= CHOP_RAMP_HI:
            return TREND_CHOP_FLOOR
        frac = (chop_score - CHOP_RAMP_LO) / (CHOP_RAMP_HI - CHOP_RAMP_LO)
        return 1.0 - frac * (1.0 - TREND_CHOP_FLOOR)

    # -------------------------------------------------------------------------
    # WS3/WS4: MARKET STRUCTURE SIGNAL FETCHER
    # -------------------------------------------------------------------------

    def _refresh_market_structure(self) -> None:
        """
        Refresh narrowness (WS3) and chop (WS4) signals from Polygon daily bars.
        Cached on the instance and refreshed at most once per ~4 hours (daily
        bars don't change faster than that). On failure, signals are marked
        unavailable and modifiers become no-ops.
        """
        # Refresh at most every 4 hours
        if (time.time() - self._ms_last_fetch_at) < 4 * 3600 and self._ms_cache.get("available"):
            return

        t0 = time.time()
        try:
            import pandas as pd
            import requests
            from datetime import date, timedelta

            api_key = os.getenv("POLYGON_API_KEY", "")
            if not api_key:
                raise RuntimeError("no POLYGON_API_KEY for market-structure fetch")

            # Lazy import of shared WS2 utilities
            _SHARED_PATH = str(_PROJECT_ROOT / "strategies")
            if _SHARED_PATH not in sys.path:
                sys.path.insert(0, _SHARED_PATH)
            from shared.market_regime import (  # type: ignore
                narrowness_score as _narrow_score,
                narrowness_sustained as _narrow_sust,
                chop_score as _chop_score,
            )

            end = date.today()
            start = end - timedelta(days=_STRUCTURE_HISTORY_DAYS + 90)  # extra buffer

            def _fetch_daily(ticker: str) -> "pd.DataFrame":
                url = (
                    f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/"
                    f"{start.isoformat()}/{end.isoformat()}?adjusted=true&sort=asc&limit=5000"
                    f"&apiKey={api_key}"
                )
                resp = requests.get(url, timeout=15)
                resp.raise_for_status()
                results = resp.json().get("results", []) or []
                if not results:
                    raise RuntimeError(f"no daily bars for {ticker}")
                rows = [
                    {
                        "ts": pd.Timestamp(r["t"], unit="ms").date(),
                        "high": float(r["h"]),
                        "low": float(r["l"]),
                        "close": float(r["c"]),
                    }
                    for r in results
                ]
                return pd.DataFrame(rows)

            spy = _fetch_daily("SPY")
            rsp = _fetch_daily("RSP")

            spy_close = pd.Series(spy["close"].values, name="SPY")
            rsp_close = pd.Series(rsp["close"].values, name="RSP")

            n_score = float(_narrow_score(
                spy_close, rsp_close, window=SIMPLE_BREADTH_WINDOW,
            ))
            n_sust = bool(_narrow_sust(
                spy_close, rsp_close,
                threshold_score=SIMPLE_BREADTH_THRESHOLD_SCORE,
                sustain_days=SIMPLE_BREADTH_SUSTAIN_DAYS,
                window=SIMPLE_BREADTH_WINDOW,
            ))
            c_score = float(_chop_score(spy[["high", "low", "close"]]))

            self._ms_cache = {
                "narrowness_score": n_score,
                "narrowness_sustained": n_sust,
                "chop_score": c_score,
                "available": True,
                "fetch_time_ms": (time.time() - t0) * 1000,
                "spy_bar_count": int(len(spy)),
            }
            self._ms_last_fetch_at = time.time()
            self._intel_log.info(
                f"[MS] narrowness_score={n_score:.3f} sustained={n_sust} "
                f"chop={c_score:.3f} (bars={len(spy)})"
            )
        except Exception as e:
            self._intel_log.warning(f"[MS] market-structure fetch failed: {e}")
            self._ms_cache = {
                "narrowness_score": 0.5,
                "narrowness_sustained": False,
                "chop_score": 0.0,
                "available": False,
                "fetch_time_ms": (time.time() - t0) * 1000,
                "error": str(e),
            }

    # -------------------------------------------------------------------------
    # DATA SOURCE FETCHERS (each error-isolated)
    # -------------------------------------------------------------------------

    def _fetch_macro(self) -> SourceScore:
        """Fetch FRED macro indicators via existing MacroAnalyzer."""
        t0 = time.time()
        try:
            if self._macro_analyzer is None:
                from providers.fred_client import FREDClient
                from signals.macro import MacroAnalyzer
                fred = FREDClient()
                self._macro_analyzer = MacroAnalyzer(fred_client=fred)

            ctx = self._macro_analyzer.get_macro_context()
            return SourceScore(
                source="macro",
                score=ctx.overall_score,
                weight=SOURCE_WEIGHTS["macro"],
                raw_data={
                    "regime": ctx.regime,
                    "alerts": ctx.alerts[:3] if ctx.alerts else [],
                    "summary": ctx.summary[:200] if ctx.summary else "",
                },
                available=True,
                fetch_time_ms=(time.time() - t0) * 1000,
            )
        except Exception as e:
            self._intel_log.warning(f"[FETCH] macro failed: {e}")
            return SourceScore(
                source="macro", score=50.0, weight=SOURCE_WEIGHTS["macro"],
                raw_data={"error": str(e)}, available=False,
                fetch_time_ms=(time.time() - t0) * 1000,
            )

    def _fetch_polymarket(self) -> SourceScore:
        """Fetch prediction market sentiment via existing PolymarketClient."""
        t0 = time.time()
        try:
            if self._polymarket_client is None:
                from polymarket_client import PolymarketClient
                self._polymarket_client = PolymarketClient()

            sentiment = self._polymarket_client.get_market_sentiment()
            risk_level = sentiment.overall_risk_level
            score = POLYMARKET_RISK_SCORES.get(risk_level, 50)

            return SourceScore(
                source="polymarket",
                score=score,
                weight=SOURCE_WEIGHTS["polymarket"],
                raw_data={
                    "risk_level": risk_level,
                    "recession_prob": sentiment.recession_prob,
                    "fed_dovish_prob": sentiment.fed_dovish_prob,
                    "fed_hawkish_prob": sentiment.fed_hawkish_prob,
                    "high_volatility_prob": sentiment.high_volatility_prob,
                },
                available=True,
                fetch_time_ms=(time.time() - t0) * 1000,
            )
        except Exception as e:
            self._intel_log.warning(f"[FETCH] polymarket failed: {e}")
            return SourceScore(
                source="polymarket", score=50.0, weight=SOURCE_WEIGHTS["polymarket"],
                raw_data={"error": str(e)}, available=False,
                fetch_time_ms=(time.time() - t0) * 1000,
            )

    def _fetch_event_calendar(self) -> SourceScore:
        """Load event_calendar.yaml and score based on upcoming event proximity."""
        t0 = time.time()
        try:
            if self._event_calendar is None:
                import yaml
                cal_path = _PROJECT_ROOT / "config" / "event_calendar.yaml"
                if cal_path.exists():
                    with open(cal_path, "r") as f:
                        self._event_calendar = yaml.safe_load(f)
                else:
                    self._event_calendar = {}

            cal = self._event_calendar
            events = cal.get("calendar", {}).get("events", {})
            risk_config = cal.get("event_risk", {})
            lead_days = risk_config.get("lead_days", {})

            from datetime import date, timedelta
            today = date.today()
            score = 50.0  # neutral baseline
            nearest_event = None
            nearest_days = 999

            for date_str, event_info in events.items():
                try:
                    event_date = date.fromisoformat(date_str)
                except (ValueError, TypeError):
                    continue

                risk = event_info.get("risk", "none")
                event_lead = lead_days.get(risk, 0)
                days_until = (event_date - today).days

                # Check if event is within its lead window
                if -1 <= days_until <= event_lead + 1:
                    # Event is imminent or just happened
                    if risk == "high":
                        score -= 15
                    elif risk == "medium":
                        score -= 10
                    elif risk == "low":
                        score -= 5

                # Track nearest upcoming event
                if days_until >= 0 and days_until < nearest_days:
                    nearest_days = days_until
                    nearest_event = event_info

            score = max(10.0, min(90.0, score))

            raw = {"score_adjustment": 50.0 - score}
            if nearest_event:
                raw["next_event"] = nearest_event.get("name", "")
                raw["next_event_days"] = nearest_days
                raw["next_event_risk"] = nearest_event.get("risk", "")

            return SourceScore(
                source="event_calendar",
                score=score,
                weight=SOURCE_WEIGHTS["event_calendar"],
                raw_data=raw,
                available=True,
                fetch_time_ms=(time.time() - t0) * 1000,
            )
        except Exception as e:
            self._intel_log.warning(f"[FETCH] event_calendar failed: {e}")
            return SourceScore(
                source="event_calendar", score=50.0,
                weight=SOURCE_WEIGHTS["event_calendar"],
                raw_data={"error": str(e)}, available=False,
                fetch_time_ms=(time.time() - t0) * 1000,
            )

    def _fetch_news(self) -> SourceScore:
        """Fetch SPY/QQQ news via Polygon as a broad market sentiment proxy."""
        t0 = time.time()
        try:
            import requests

            api_key = os.getenv("POLYGON_API_KEY", "")
            if not api_key:
                return SourceScore(
                    source="news", score=50.0, weight=SOURCE_WEIGHTS["news"],
                    raw_data={"error": "no POLYGON_API_KEY"}, available=False,
                    fetch_time_ms=(time.time() - t0) * 1000,
                )

            from enhanced_news_filter import enhanced_score_news

            # Query broad market news (SPY as proxy)
            url = (
                f"https://api.polygon.io/v2/reference/news"
                f"?ticker=SPY&limit=10&apiKey={api_key}"
            )
            resp = requests.get(url, timeout=10)
            resp.raise_for_status()
            news_json = resp.json()

            sentiment_score, articles = enhanced_score_news(
                news_json, lookback_hours=24, symbol="SPY",
                min_relevance_score=0.2, max_articles=10,
            )

            # Convert sentiment (-1 to 1 scale) to 0-100
            if sentiment_score is not None:
                score = 50.0 + (sentiment_score * 30.0)
                score = max(10.0, min(90.0, score))
            else:
                score = 50.0

            return SourceScore(
                source="news",
                score=score,
                weight=SOURCE_WEIGHTS["news"],
                raw_data={
                    "sentiment_raw": sentiment_score,
                    "article_count": len(articles),
                    "top_headline": articles[0]["title"] if articles else "",
                },
                available=True,
                fetch_time_ms=(time.time() - t0) * 1000,
            )
        except Exception as e:
            self._intel_log.warning(f"[FETCH] news failed: {e}")
            return SourceScore(
                source="news", score=50.0, weight=SOURCE_WEIGHTS["news"],
                raw_data={"error": str(e)}, available=False,
                fetch_time_ms=(time.time() - t0) * 1000,
            )

    def _fetch_reddit(self) -> SourceScore:
        """Fetch Reddit sentiment via existing RedditSentimentProvider."""
        t0 = time.time()
        try:
            if self._reddit_provider is None:
                from reddit_sentiment import RedditSentimentProvider
                self._reddit_provider = RedditSentimentProvider()

            trending = self._reddit_provider.get_trending_tickers(limit=30)

            if not trending:
                return SourceScore(
                    source="reddit", score=50.0, weight=SOURCE_WEIGHTS["reddit"],
                    raw_data={"note": "no trending data"}, available=True,
                    fetch_time_ms=(time.time() - t0) * 1000,
                )

            # Aggregate sentiment across top trending tickers
            sentiments = [t.sentiment for t in trending if t.sentiment is not None]
            if sentiments:
                mean_sentiment = sum(sentiments) / len(sentiments)
                # Convert -1..1 to 0-100 scale
                score = 50.0 + (mean_sentiment * 30.0)
                score = max(20.0, min(80.0, score))
            else:
                mean_sentiment = 0.0
                score = 50.0

            bullish = [t for t in trending if getattr(t, 'is_bullish', False)]
            bearish = [t for t in trending if getattr(t, 'is_bearish', False)]

            return SourceScore(
                source="reddit",
                score=score,
                weight=SOURCE_WEIGHTS["reddit"],
                raw_data={
                    "trending_count": len(trending),
                    "mean_sentiment": round(mean_sentiment, 3),
                    "bullish_count": len(bullish),
                    "bearish_count": len(bearish),
                    "top_bullish": [t.ticker for t in bullish[:3]],
                    "top_bearish": [t.ticker for t in bearish[:3]],
                },
                available=True,
                fetch_time_ms=(time.time() - t0) * 1000,
            )
        except Exception as e:
            self._intel_log.warning(f"[FETCH] reddit failed: {e}")
            return SourceScore(
                source="reddit", score=50.0, weight=SOURCE_WEIGHTS["reddit"],
                raw_data={"error": str(e)}, available=False,
                fetch_time_ms=(time.time() - t0) * 1000,
            )

    # -------------------------------------------------------------------------
    # HELPERS
    # -------------------------------------------------------------------------

    def _get_next_event(self) -> Tuple[Optional[str], Optional[str], Optional[str]]:
        """Get the next upcoming event from the calendar."""
        if not self._event_calendar:
            return None, None, None

        events = self._event_calendar.get("calendar", {}).get("events", {})
        from datetime import date
        today = date.today()

        nearest_name = None
        nearest_date_str = None
        nearest_risk = None
        nearest_days = 999

        for date_str, info in events.items():
            try:
                d = date.fromisoformat(date_str)
            except (ValueError, TypeError):
                continue
            days_until = (d - today).days
            if 0 <= days_until < nearest_days:
                nearest_days = days_until
                nearest_name = info.get("name")
                nearest_date_str = date_str
                nearest_risk = info.get("risk")

        return nearest_name, nearest_date_str, nearest_risk

    def _build_neutral_context(self) -> MarketContext:
        """Build a neutral (no-change) context for fallback."""
        adjustments = {}
        for sid, sc in self._config.sleeves.items():
            adjustments[sid] = SleeveAdjustment(
                strategy_id=sid,
                base_allocation=sc.allocation_pct,
                adjusted_allocation=sc.allocation_pct,
                allocation_multiplier=1.0,
                risk_multiplier=1.0,
                entry_allowed=True,
                entry_gate_reason="ok (neutral fallback)",
            )
        return MarketContext(
            regime=MarketRegime.CAUTIOUS,
            regime_score=50.0,
            regime_changed=False,
            sleeve_adjustments=adjustments,
            global_risk_multiplier=1.0,
            cash_reserve_pct=self._config.cash_reserve_pct,
            source_scores=[],
            timestamp=datetime.now(timezone.utc),
            refresh_count=self._refresh_count,
        )

    # -------------------------------------------------------------------------
    # LOGGING
    # -------------------------------------------------------------------------

    def _log_refresh(self, ctx: MarketContext, elapsed_ms: float) -> None:
        """Log every refresh decision for auditability."""
        # Human-readable log
        sources_str = "  ".join(
            f"{s.source}={s.score:.0f}(w={s.weight},{'ok' if s.available else 'FAIL'})"
            for s in ctx.source_scores
        )
        alloc_str = "  ".join(
            f"{sid}: {adj.base_allocation:.3f}->{adj.adjusted_allocation:.3f}"
            for sid, adj in ctx.sleeve_adjustments.items()
        )
        risk_str = "  ".join(
            f"{sid}={adj.risk_multiplier:.2f}"
            for sid, adj in ctx.sleeve_adjustments.items()
        )
        gate_str = "  ".join(
            f"{sid}={'ok' if adj.entry_allowed else 'BLOCKED:' + adj.entry_gate_reason}"
            for sid, adj in ctx.sleeve_adjustments.items()
        )

        self._intel_log.info(
            f"REFRESH #{ctx.refresh_count} | regime={ctx.regime.value} "
            f"{'(CHANGED!)' if ctx.regime_changed else ''} | "
            f"score={ctx.regime_score:.1f} | {elapsed_ms:.0f}ms"
        )
        self._intel_log.info(f"  Sources: {sources_str}")
        if ctx.next_event_name:
            self._intel_log.info(
                f"  Next event: {ctx.next_event_name} on {ctx.next_event_date} "
                f"({ctx.next_event_risk} risk)"
            )
        self._intel_log.info(f"  Allocations: {alloc_str} | cash={ctx.cash_reserve_pct:.3f}")
        self._intel_log.info(f"  Risk multipliers: {risk_str}")
        self._intel_log.info(f"  Entry gates: {gate_str}")

        # Also log to main engine log on regime changes
        if ctx.regime_changed:
            log.warning(
                f"[INTELLIGENCE] Regime changed to {ctx.regime.value} "
                f"(score={ctx.regime_score:.1f})"
            )

        # JSONL structured log
        try:
            record = {
                "_event": "refresh",
                "_ts": ctx.timestamp.isoformat(),
                "refresh_count": ctx.refresh_count,
                "regime": ctx.regime.value,
                "regime_score": round(ctx.regime_score, 2),
                "regime_changed": ctx.regime_changed,
                "global_risk_multiplier": ctx.global_risk_multiplier,
                "sources": {
                    s.source: {
                        "score": round(s.score, 1),
                        "available": s.available,
                        "fetch_ms": round(s.fetch_time_ms, 0),
                    }
                    for s in ctx.source_scores
                },
                "allocations": {
                    sid: round(adj.adjusted_allocation, 4)
                    for sid, adj in ctx.sleeve_adjustments.items()
                },
                "risk_multipliers": {
                    sid: round(adj.risk_multiplier, 2)
                    for sid, adj in ctx.sleeve_adjustments.items()
                },
                "entry_gates": {
                    sid: adj.entry_allowed
                    for sid, adj in ctx.sleeve_adjustments.items()
                },
                "gate_reasons": {
                    sid: adj.entry_gate_reason
                    for sid, adj in ctx.sleeve_adjustments.items()
                    if not adj.entry_allowed
                },
                "adjustment_reasons": {
                    sid: adj.adjustment_reasons
                    for sid, adj in ctx.sleeve_adjustments.items()
                    if adj.adjustment_reasons
                },
                "market_structure": {
                    "enabled": MARKET_STRUCTURE_GATE_ENABLED or CHOP_DAMPENER_ENABLED,
                    "gate_enabled": MARKET_STRUCTURE_GATE_ENABLED,
                    "chop_enabled": CHOP_DAMPENER_ENABLED,
                    "available": self._ms_cache.get("available", False),
                    "narrowness_score": round(self._ms_cache.get("narrowness_score", 0.5), 3),
                    "narrowness_sustained": self._ms_cache.get("narrowness_sustained", False),
                    "chop_score": round(self._ms_cache.get("chop_score", 0.0), 3),
                },
                "elapsed_ms": round(elapsed_ms, 0),
            }
            with open(self._jsonl_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(record) + "\n")
        except Exception:
            pass  # logging failure must never crash the engine
