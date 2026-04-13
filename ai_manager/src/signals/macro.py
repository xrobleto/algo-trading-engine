"""
Macro Analysis Module

Analyzes macroeconomic indicators and their impact on portfolio.
Uses FRED data for economic time series.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Any

from ..utils.logging import get_logger
from ..utils.typing import MacroIndicator
from ..providers.fred_client import FREDClient

logger = get_logger(__name__)


@dataclass
class MacroContext:
    """Overall macro context for investment decisions."""
    indicators: List[MacroIndicator]
    overall_score: float  # 0-100 (50 = neutral)
    regime: str  # "risk_on", "risk_off", "neutral"
    alerts: List[str]
    summary: str
    last_updated: datetime


# Regime detection thresholds
REGIME_THRESHOLDS = {
    "VIXCLS": {
        "risk_off": 25,  # VIX above 25 = risk-off
        "risk_on": 15,   # VIX below 15 = risk-on
    },
    "T10Y2Y": {
        "risk_off": -0.3,  # Inverted yield curve = risk-off
        "risk_on": 1.0,     # Steep curve = risk-on
    },
}

# Indicator score weights
INDICATOR_WEIGHTS = {
    "VIXCLS": 0.25,
    "T10Y2Y": 0.20,
    "DFF": 0.15,
    "UNRATE": 0.15,
    "T10YIE": 0.10,
    "default": 0.05,
}


class MacroAnalyzer:
    """
    Macro analysis engine.

    Fetches and analyzes macro indicators to determine market regime.
    """

    def __init__(
        self,
        fred_client: Optional[FREDClient] = None,
        indicator_configs: Optional[List[Dict[str, Any]]] = None
    ):
        """
        Initialize macro analyzer.

        Args:
            fred_client: FRED client for data
            indicator_configs: List of indicator configurations
        """
        self.fred = fred_client
        self.indicator_configs = indicator_configs or [
            {"series_id": "VIXCLS", "name": "VIX", "alert_threshold_high": 25, "alert_threshold_low": 12},
            {"series_id": "DFF", "name": "Fed Funds Rate"},
            {"series_id": "T10Y2Y", "name": "10Y-2Y Spread", "alert_threshold_low": -0.5},
            {"series_id": "UNRATE", "name": "Unemployment Rate"},
        ]

    def get_macro_context(self) -> MacroContext:
        """
        Get current macro context.

        Returns:
            MacroContext with indicators, regime, and analysis
        """
        if not self.fred or not self.fred.is_available:
            logger.warning("FRED client not available - returning neutral macro context")
            return MacroContext(
                indicators=[],
                overall_score=50.0,
                regime="neutral",
                alerts=["FRED data unavailable"],
                summary="Unable to fetch macro data. Using neutral assumption.",
                last_updated=datetime.now(),
            )

        # Fetch indicators
        indicators = self.fred.get_multiple_indicators(self.indicator_configs)

        if not indicators:
            return MacroContext(
                indicators=[],
                overall_score=50.0,
                regime="neutral",
                alerts=["No macro data received"],
                summary="Unable to fetch macro data. Using neutral assumption.",
                last_updated=datetime.now(),
            )

        # Calculate overall score
        overall_score = self._calculate_overall_score(indicators)

        # Determine regime
        regime = self._determine_regime(indicators)

        # Collect alerts
        alerts = [ind.alert_reason for ind in indicators if ind.alert_triggered]

        # Generate summary
        summary = self._generate_summary(indicators, regime, overall_score)

        return MacroContext(
            indicators=indicators,
            overall_score=overall_score,
            regime=regime,
            alerts=alerts,
            summary=summary,
            last_updated=datetime.now(),
        )

    def _calculate_overall_score(self, indicators: List[MacroIndicator]) -> float:
        """
        Calculate overall macro score from indicators.

        Score of 50 = neutral, >50 = favorable, <50 = unfavorable
        """
        if not indicators:
            return 50.0

        weighted_scores = []
        total_weight = 0

        for ind in indicators:
            weight = INDICATOR_WEIGHTS.get(ind.series_id, INDICATOR_WEIGHTS["default"])
            score = self._indicator_to_score(ind)
            weighted_scores.append(score * weight)
            total_weight += weight

        if total_weight == 0:
            return 50.0

        return sum(weighted_scores) / total_weight

    def _indicator_to_score(self, indicator: MacroIndicator) -> float:
        """Convert an indicator to a 0-100 score."""
        series_id = indicator.series_id
        value = indicator.value

        # VIX: Lower is better (score inversely)
        if series_id == "VIXCLS":
            if value >= 35:
                return 10
            elif value >= 25:
                return 30
            elif value >= 20:
                return 45
            elif value >= 15:
                return 60
            elif value >= 12:
                return 75
            else:
                return 85

        # Yield curve spread: Positive is better
        elif series_id == "T10Y2Y":
            if value < -0.5:
                return 15  # Deeply inverted
            elif value < 0:
                return 30  # Inverted
            elif value < 0.5:
                return 45  # Flat
            elif value < 1.5:
                return 65  # Normal
            else:
                return 75  # Steep

        # Fed Funds Rate: Context dependent, use neutral
        elif series_id == "DFF":
            # Rate changes matter more than absolute level
            if indicator.change is not None:
                if indicator.change > 0.25:
                    return 40  # Hiking
                elif indicator.change < -0.25:
                    return 60  # Cutting
            return 50  # Stable

        # Unemployment: Lower is better (but not too low)
        elif series_id == "UNRATE":
            if value < 3.5:
                return 60  # Very low (possible overheating)
            elif value < 4.5:
                return 70  # Healthy
            elif value < 5.5:
                return 55  # Moderate
            elif value < 7.0:
                return 40  # Elevated
            else:
                return 25  # High

        # Default: neutral
        return 50.0

    def _determine_regime(self, indicators: List[MacroIndicator]) -> str:
        """Determine market regime from indicators."""
        risk_signals = 0
        favorable_signals = 0

        for ind in indicators:
            series_id = ind.series_id
            value = ind.value

            thresholds = REGIME_THRESHOLDS.get(series_id)
            if not thresholds:
                continue

            risk_off_thresh = thresholds.get("risk_off")
            risk_on_thresh = thresholds.get("risk_on")

            if series_id == "VIXCLS":
                if value >= risk_off_thresh:
                    risk_signals += 2  # Strong signal
                elif value <= risk_on_thresh:
                    favorable_signals += 1
            elif series_id == "T10Y2Y":
                if value <= risk_off_thresh:
                    risk_signals += 2  # Yield curve inversion
                elif value >= risk_on_thresh:
                    favorable_signals += 1

        # Determine regime
        if risk_signals >= 2:
            return "risk_off"
        elif favorable_signals >= 2 and risk_signals == 0:
            return "risk_on"
        else:
            return "neutral"

    def _generate_summary(
        self,
        indicators: List[MacroIndicator],
        regime: str,
        score: float
    ) -> str:
        """Generate human-readable summary."""
        # Find key indicators
        vix = next((i for i in indicators if i.series_id == "VIXCLS"), None)
        spread = next((i for i in indicators if i.series_id == "T10Y2Y"), None)
        ff_rate = next((i for i in indicators if i.series_id == "DFF"), None)

        parts = []

        # Regime description
        if regime == "risk_off":
            parts.append("Macro environment suggests caution.")
        elif regime == "risk_on":
            parts.append("Macro environment is favorable.")
        else:
            parts.append("Macro environment is mixed.")

        # Key metrics
        if vix:
            if vix.value >= 25:
                parts.append(f"VIX elevated at {vix.value:.1f}.")
            elif vix.value <= 15:
                parts.append(f"VIX low at {vix.value:.1f}, indicating complacency.")
            else:
                parts.append(f"VIX at {vix.value:.1f}.")

        if spread:
            if spread.value < 0:
                parts.append(f"Yield curve inverted ({spread.value:.2f}%).")
            else:
                parts.append(f"Yield curve spread: {spread.value:.2f}%.")

        if ff_rate and ff_rate.change is not None:
            if abs(ff_rate.change) > 0.1:
                direction = "higher" if ff_rate.change > 0 else "lower"
                parts.append(f"Fed Funds Rate {direction} at {ff_rate.value:.2f}%.")

        return " ".join(parts)


def get_macro_context(
    fred_client: Optional[FREDClient] = None,
    indicator_configs: Optional[List[Dict[str, Any]]] = None
) -> MacroContext:
    """
    Convenience function to get macro context.

    Args:
        fred_client: FRED client
        indicator_configs: Indicator configurations

    Returns:
        MacroContext object
    """
    analyzer = MacroAnalyzer(
        fred_client=fred_client,
        indicator_configs=indicator_configs
    )
    return analyzer.get_macro_context()


def macro_impacts_sector(
    macro_context: MacroContext,
    sector: str
) -> Dict[str, Any]:
    """
    Determine how macro environment impacts a specific sector.

    Args:
        macro_context: Current macro context
        sector: Sector name

    Returns:
        Dict with impact assessment
    """
    # Sector sensitivity to macro factors
    sector_sensitivities = {
        "Technology": {
            "rate_sensitive": True,
            "growth_sensitive": True,
            "defensive": False,
        },
        "Financial Services": {
            "rate_sensitive": True,  # Benefits from higher rates
            "growth_sensitive": True,
            "defensive": False,
        },
        "Healthcare": {
            "rate_sensitive": False,
            "growth_sensitive": False,
            "defensive": True,
        },
        "Consumer Defensive": {
            "rate_sensitive": False,
            "growth_sensitive": False,
            "defensive": True,
        },
        "Utilities": {
            "rate_sensitive": True,  # Hurt by higher rates
            "growth_sensitive": False,
            "defensive": True,
        },
        "Energy": {
            "rate_sensitive": False,
            "growth_sensitive": True,
            "defensive": False,
        },
        "Consumer Cyclical": {
            "rate_sensitive": True,
            "growth_sensitive": True,
            "defensive": False,
        },
    }

    sensitivity = sector_sensitivities.get(sector, {
        "rate_sensitive": False,
        "growth_sensitive": True,
        "defensive": False,
    })

    # Assess impact
    impact_score = 50.0  # Neutral baseline
    impacts = []

    # Rate sensitivity
    ff_rate = next(
        (i for i in macro_context.indicators if i.series_id == "DFF"),
        None
    )
    if sensitivity["rate_sensitive"] and ff_rate:
        if ff_rate.change and ff_rate.change > 0.1:
            if sector == "Financial Services":
                impact_score += 5
                impacts.append("Higher rates benefit financials")
            else:
                impact_score -= 5
                impacts.append("Higher rates headwind for rate-sensitive sector")

    # Regime impact
    if macro_context.regime == "risk_off":
        if sensitivity["defensive"]:
            impact_score += 10
            impacts.append("Defensive sector favored in risk-off")
        else:
            impact_score -= 10
            impacts.append("Growth sector faces headwinds in risk-off")
    elif macro_context.regime == "risk_on":
        if sensitivity["growth_sensitive"] and not sensitivity["defensive"]:
            impact_score += 10
            impacts.append("Growth sector favored in risk-on")

    return {
        "sector": sector,
        "impact_score": impact_score,
        "impacts": impacts,
        "sensitivity": sensitivity,
    }
