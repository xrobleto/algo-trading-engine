"""
Portfolio Risk Analysis Module

Computes portfolio-level risk metrics and concentration analysis.
All calculations are deterministic.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any
import math

from ..utils.logging import get_logger
from ..utils.typing import Holding, PortfolioSnapshot, TechnicalSignal

logger = get_logger(__name__)


@dataclass
class RiskAssessment:
    """Complete risk assessment for the portfolio."""
    # Scores
    overall_risk_score: float  # 0-100 (higher = more risk)
    concentration_score: float
    volatility_score: float
    correlation_score: float
    technical_risk_score: float
    drawdown_score: float

    # Flags
    concentration_warning: bool
    sector_concentration_warning: bool
    high_volatility_warning: bool
    high_correlation_warning: bool
    drawdown_warning: bool

    # Details
    risk_factors: List[str]
    recommendations: List[str]

    # Metadata
    timestamp: datetime


@dataclass
class ConcentrationAnalysis:
    """Portfolio concentration analysis."""
    top_holding_pct: float
    top_3_holdings_pct: float
    top_5_holdings_pct: float
    hhi_index: float  # Herfindahl-Hirschman Index
    effective_positions: float  # 1 / HHI
    sector_concentrations: Dict[str, float]
    max_sector_pct: float
    max_sector_name: str


class RiskAnalyzer:
    """
    Portfolio risk analysis engine.

    Computes concentration, volatility, correlation, and other risk metrics.
    """

    def __init__(
        self,
        max_single_position_pct: float = 20.0,
        max_sector_pct: float = 40.0,
        drawdown_alert_pct: float = -8.0,
        correlation_concern_threshold: float = 0.85,
        min_position_value: float = 50.0
    ):
        """
        Initialize risk analyzer.

        Args:
            max_single_position_pct: Max position size threshold
            max_sector_pct: Max sector concentration threshold
            drawdown_alert_pct: Drawdown % that triggers alert
            correlation_concern_threshold: Correlation level for concern
            min_position_value: Min value to include in analysis
        """
        self.max_single_position_pct = max_single_position_pct
        self.max_sector_pct = max_sector_pct
        self.drawdown_alert_pct = drawdown_alert_pct
        self.correlation_concern_threshold = correlation_concern_threshold
        self.min_position_value = min_position_value

    def analyze(
        self,
        snapshot: PortfolioSnapshot,
        technical_signals: Optional[Dict[str, TechnicalSignal]] = None
    ) -> RiskAssessment:
        """
        Perform complete risk analysis.

        Args:
            snapshot: Current portfolio snapshot
            technical_signals: Technical signals for holdings

        Returns:
            RiskAssessment with all metrics
        """
        # Filter to meaningful positions
        holdings = [
            h for h in snapshot.holdings
            if h.current_value and h.current_value >= self.min_position_value
        ]

        if not holdings:
            return self._empty_assessment()

        # Concentration analysis
        concentration = self._analyze_concentration(holdings, snapshot.total_value)

        # Volatility analysis
        volatility_score = self._analyze_volatility(holdings, technical_signals)

        # Correlation analysis
        correlation_score, high_correlations = self._analyze_correlations(holdings, technical_signals)

        # Technical risk (breakdowns, etc.)
        technical_risk = self._analyze_technical_risk(holdings, technical_signals)

        # Drawdown analysis
        drawdown_score = self._analyze_drawdown(snapshot)

        # Combine into overall score
        concentration_score = self._concentration_to_score(concentration)

        overall_risk_score = (
            concentration_score * 0.30 +
            volatility_score * 0.25 +
            correlation_score * 0.15 +
            technical_risk * 0.20 +
            drawdown_score * 0.10
        )

        # Determine warnings
        concentration_warning = (
            concentration.top_holding_pct > self.max_single_position_pct or
            concentration.top_3_holdings_pct > 60
        )

        sector_warning = concentration.max_sector_pct > self.max_sector_pct

        volatility_warning = volatility_score > 70

        correlation_warning = len(high_correlations) > 0

        drawdown_warning = drawdown_score > 70

        # Generate risk factors and recommendations
        risk_factors = []
        recommendations = []

        if concentration_warning:
            if concentration.top_holding_pct > self.max_single_position_pct:
                risk_factors.append(
                    f"Top holding at {concentration.top_holding_pct:.1f}% "
                    f"(max: {self.max_single_position_pct}%)"
                )
                recommendations.append("Consider trimming largest position")

        if sector_warning:
            risk_factors.append(
                f"{concentration.max_sector_name} sector at {concentration.max_sector_pct:.1f}%"
            )
            recommendations.append(f"Consider diversifying away from {concentration.max_sector_name}")

        if volatility_warning:
            risk_factors.append("Portfolio volatility elevated")
            recommendations.append("Review high-volatility positions")

        if correlation_warning:
            risk_factors.append(f"{len(high_correlations)} highly correlated position pairs")
            recommendations.append("Diversification may be limited")

        if drawdown_warning:
            risk_factors.append(f"Portfolio drawdown concerns")
            recommendations.append("Review stop-loss levels")

        return RiskAssessment(
            overall_risk_score=overall_risk_score,
            concentration_score=concentration_score,
            volatility_score=volatility_score,
            correlation_score=correlation_score,
            technical_risk_score=technical_risk,
            drawdown_score=drawdown_score,
            concentration_warning=concentration_warning,
            sector_concentration_warning=sector_warning,
            high_volatility_warning=volatility_warning,
            high_correlation_warning=correlation_warning,
            drawdown_warning=drawdown_warning,
            risk_factors=risk_factors,
            recommendations=recommendations,
            timestamp=datetime.now(),
        )

    def _analyze_concentration(
        self,
        holdings: List[Holding],
        total_value: float
    ) -> ConcentrationAnalysis:
        """Analyze portfolio concentration."""
        if not holdings or total_value <= 0:
            return ConcentrationAnalysis(
                top_holding_pct=0,
                top_3_holdings_pct=0,
                top_5_holdings_pct=0,
                hhi_index=0,
                effective_positions=0,
                sector_concentrations={},
                max_sector_pct=0,
                max_sector_name="Unknown",
            )

        # Sort by value
        sorted_holdings = sorted(
            holdings,
            key=lambda h: h.current_value or 0,
            reverse=True
        )

        # Position concentration
        weights = [(h.current_value or 0) / total_value * 100 for h in sorted_holdings]

        top_1 = weights[0] if len(weights) >= 1 else 0
        top_3 = sum(weights[:3]) if len(weights) >= 3 else sum(weights)
        top_5 = sum(weights[:5]) if len(weights) >= 5 else sum(weights)

        # HHI Index (sum of squared weights)
        hhi = sum((w / 100) ** 2 for w in weights)
        effective_positions = 1 / hhi if hhi > 0 else len(holdings)

        # Sector concentration
        sector_values: Dict[str, float] = {}
        for h in holdings:
            sector = h.sector or "Unknown"
            sector_values[sector] = sector_values.get(sector, 0) + (h.current_value or 0)

        sector_pcts = {
            sector: (value / total_value) * 100
            for sector, value in sector_values.items()
        }

        max_sector_name = max(sector_pcts.keys(), key=lambda k: sector_pcts[k]) if sector_pcts else "Unknown"
        max_sector_pct = sector_pcts.get(max_sector_name, 0)

        return ConcentrationAnalysis(
            top_holding_pct=top_1,
            top_3_holdings_pct=top_3,
            top_5_holdings_pct=top_5,
            hhi_index=hhi,
            effective_positions=effective_positions,
            sector_concentrations=sector_pcts,
            max_sector_pct=max_sector_pct,
            max_sector_name=max_sector_name,
        )

    def _concentration_to_score(self, concentration: ConcentrationAnalysis) -> float:
        """Convert concentration analysis to risk score (0-100)."""
        score = 20.0  # Baseline

        # Top holding penalty
        if concentration.top_holding_pct > 25:
            score += 30
        elif concentration.top_holding_pct > 20:
            score += 20
        elif concentration.top_holding_pct > 15:
            score += 10

        # Top 3 penalty
        if concentration.top_3_holdings_pct > 70:
            score += 20
        elif concentration.top_3_holdings_pct > 60:
            score += 10

        # HHI penalty (higher HHI = more concentrated)
        if concentration.hhi_index > 0.15:
            score += 20
        elif concentration.hhi_index > 0.10:
            score += 10

        # Sector penalty
        if concentration.max_sector_pct > 50:
            score += 20
        elif concentration.max_sector_pct > 40:
            score += 10

        return min(100, score)

    def _analyze_volatility(
        self,
        holdings: List[Holding],
        technical_signals: Optional[Dict[str, TechnicalSignal]]
    ) -> float:
        """Analyze portfolio volatility. Returns score 0-100."""
        if not technical_signals:
            return 50.0  # Neutral if no data

        volatilities = []
        for h in holdings:
            signal = technical_signals.get(h.symbol)
            if signal and signal.volatility_20d:
                # Weight by position size
                weight = (h.current_value or 0) if h.current_value else 1
                volatilities.append((signal.volatility_20d, weight))

        if not volatilities:
            return 50.0

        # Weighted average volatility
        total_weight = sum(w for _, w in volatilities)
        if total_weight <= 0:
            return 50.0

        weighted_vol = sum(v * w for v, w in volatilities) / total_weight

        # Convert to score (higher vol = higher risk)
        # Typical range: 15-50% annualized
        if weighted_vol > 50:
            return 90
        elif weighted_vol > 40:
            return 75
        elif weighted_vol > 30:
            return 60
        elif weighted_vol > 20:
            return 45
        else:
            return 30

    def _analyze_correlations(
        self,
        holdings: List[Holding],
        technical_signals: Optional[Dict[str, TechnicalSignal]]
    ) -> Tuple[float, List[Tuple[str, str, float]]]:
        """
        Analyze correlations between holdings.

        Returns:
            Tuple of (score 0-100, list of high correlation pairs)
        """
        # Without price history, we can only estimate based on sector
        # Same sector implies higher correlation
        high_correlations = []

        sector_groups: Dict[str, List[str]] = {}
        for h in holdings:
            sector = h.sector or "Unknown"
            if sector not in sector_groups:
                sector_groups[sector] = []
            sector_groups[sector].append(h.symbol)

        # Flag large same-sector clusters as potential correlation concern
        for sector, symbols in sector_groups.items():
            if len(symbols) >= 3 and sector != "Unknown":
                # These are likely correlated
                for i in range(len(symbols)):
                    for j in range(i + 1, len(symbols)):
                        high_correlations.append((symbols[i], symbols[j], 0.8))  # Assumed correlation

        # Score based on correlation concerns
        if len(high_correlations) >= 5:
            score = 80
        elif len(high_correlations) >= 3:
            score = 65
        elif len(high_correlations) >= 1:
            score = 50
        else:
            score = 30

        return score, high_correlations

    def _analyze_technical_risk(
        self,
        holdings: List[Holding],
        technical_signals: Optional[Dict[str, TechnicalSignal]]
    ) -> float:
        """Analyze technical risk factors. Returns score 0-100."""
        if not technical_signals:
            return 50.0

        risk_points = 0
        total_weight = 0

        for h in holdings:
            signal = technical_signals.get(h.symbol)
            if not signal:
                continue

            weight = h.current_value or 1
            total_weight += weight
            position_risk = 0

            # Below key moving averages
            if signal.above_sma_200 is False:
                position_risk += 20  # Below 200 SMA is bearish
            if signal.above_sma_50 is False:
                position_risk += 10
            if signal.above_sma_20 is False:
                position_risk += 5

            # Death cross
            if signal.death_cross:
                position_risk += 25

            # Overbought RSI (potential reversal risk)
            if signal.rsi_14 and signal.rsi_14 > 75:
                position_risk += 15
            elif signal.rsi_14 and signal.rsi_14 < 25:
                position_risk += 10  # Oversold can also be risky (falling knife)

            # Negative momentum
            if signal.change_5d_pct and signal.change_5d_pct < -10:
                position_risk += 15
            elif signal.change_5d_pct and signal.change_5d_pct < -5:
                position_risk += 5

            risk_points += position_risk * weight

        if total_weight <= 0:
            return 50.0

        weighted_risk = risk_points / total_weight
        return min(100, weighted_risk)

    def _analyze_drawdown(self, snapshot: PortfolioSnapshot) -> float:
        """Analyze portfolio drawdown. Returns score 0-100."""
        # Use unrealized P/L as proxy for drawdown from cost
        if snapshot.total_cost_basis <= 0:
            return 30.0  # Low risk if no cost basis

        pnl_pct = (snapshot.total_unrealized_pnl / snapshot.total_cost_basis) * 100

        # Convert to risk score
        if pnl_pct <= -15:
            return 90  # Deep drawdown
        elif pnl_pct <= -10:
            return 75
        elif pnl_pct <= -5:
            return 60
        elif pnl_pct <= 0:
            return 45
        elif pnl_pct <= 10:
            return 30
        else:
            return 20  # In profit = lower drawdown risk

    def _empty_assessment(self) -> RiskAssessment:
        """Return empty risk assessment when no data."""
        return RiskAssessment(
            overall_risk_score=0,
            concentration_score=0,
            volatility_score=0,
            correlation_score=0,
            technical_risk_score=0,
            drawdown_score=0,
            concentration_warning=False,
            sector_concentration_warning=False,
            high_volatility_warning=False,
            high_correlation_warning=False,
            drawdown_warning=False,
            risk_factors=[],
            recommendations=[],
            timestamp=datetime.now(),
        )


def compute_portfolio_risk(
    snapshot: PortfolioSnapshot,
    technical_signals: Optional[Dict[str, TechnicalSignal]] = None,
    max_single_position_pct: float = 20.0,
    max_sector_pct: float = 40.0,
    drawdown_alert_pct: float = -8.0
) -> RiskAssessment:
    """
    Convenience function to compute portfolio risk.

    Args:
        snapshot: Portfolio snapshot
        technical_signals: Technical signals for holdings
        max_single_position_pct: Max position threshold
        max_sector_pct: Max sector threshold
        drawdown_alert_pct: Drawdown alert threshold

    Returns:
        RiskAssessment object
    """
    analyzer = RiskAnalyzer(
        max_single_position_pct=max_single_position_pct,
        max_sector_pct=max_sector_pct,
        drawdown_alert_pct=drawdown_alert_pct
    )
    return analyzer.analyze(snapshot, technical_signals)
