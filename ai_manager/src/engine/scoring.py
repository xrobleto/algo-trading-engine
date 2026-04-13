"""
Scoring Engine

Computes risk alert and opportunity scores using deterministic rules.
LLM is NOT used for scoring - only for summarization later.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Any

from ..utils.logging import get_logger
from ..utils.typing import (
    PortfolioSnapshot, TechnicalSignal, Holding, ActionType,
    NewsSentiment, SignalStrength
)
from ..signals.risk import RiskAssessment
from ..signals.macro import MacroContext
from ..providers.tradingview_alerts import TradingViewAlert

logger = get_logger(__name__)


@dataclass
class ScoreResult:
    """Result of scoring computation."""
    # Main scores (0-100)
    risk_alert_score: float
    opportunity_score: float

    # Component scores
    risk_components: Dict[str, float]
    opportunity_components: Dict[str, float]

    # Per-ticker scores
    ticker_scores: Dict[str, Dict[str, float]]

    # Top opportunities and risks
    top_opportunities: List[str]  # Tickers with highest opportunity
    top_risks: List[str]  # Tickers with highest risk

    # Flags
    should_alert: bool
    alert_type: str  # "risk", "opportunity", "both", "none"

    # Metadata
    timestamp: datetime
    computation_details: Dict[str, Any] = field(default_factory=dict)


class ScoringEngine:
    """
    Deterministic scoring engine.

    Computes two independent scores:
    1. Risk Alert Score (0-100): Higher = more risk
    2. Opportunity Score (0-100): Higher = better opportunity

    All calculations are rule-based. No LLM involvement.
    """

    def __init__(
        self,
        weights: Optional[Dict[str, float]] = None,
        risk_score_min: float = 70,
        opportunity_score_min: float = 75,
        max_single_position_pct: float = 20,
        max_sector_pct: float = 40,
    ):
        """
        Initialize scoring engine.

        Args:
            weights: Signal weights (technical, news, macro, portfolio_risk)
            risk_score_min: Minimum risk score to trigger alert
            opportunity_score_min: Minimum opportunity score to trigger alert
            max_single_position_pct: Max position size for concentration alert
            max_sector_pct: Max sector exposure for concentration alert
        """
        self.weights = weights or {
            "technical": 0.35,
            "news": 0.25,
            "macro": 0.15,
            "fundamentals": 0.10,
            "portfolio_risk": 0.15,
        }
        self.risk_score_min = risk_score_min
        self.opportunity_score_min = opportunity_score_min
        self.max_single_position_pct = max_single_position_pct
        self.max_sector_pct = max_sector_pct

    def compute(
        self,
        snapshot: PortfolioSnapshot,
        technical_signals: Dict[str, TechnicalSignal],
        news_analyses: Dict[str, Dict[str, Any]],
        risk_assessment: RiskAssessment,
        macro_context: Optional[MacroContext] = None,
        tv_alerts: Optional[List[TradingViewAlert]] = None,
    ) -> ScoreResult:
        """
        Compute all scores.

        Args:
            snapshot: Current portfolio snapshot
            technical_signals: Technical signals by ticker
            news_analyses: News analysis by ticker
            risk_assessment: Portfolio risk assessment
            macro_context: Macro environment context
            tv_alerts: Recent TradingView alerts

        Returns:
            ScoreResult with all computed scores
        """
        # === RISK ALERT SCORE ===
        risk_components = {}

        # Portfolio concentration risk
        risk_components["concentration"] = self._score_concentration_risk(snapshot)

        # Volatility risk
        risk_components["volatility"] = self._score_volatility_risk(technical_signals, snapshot)

        # Technical breakdown risk
        risk_components["technical_breakdown"] = self._score_technical_breakdown_risk(
            technical_signals, snapshot
        )

        # News risk
        risk_components["news_risk"] = self._score_news_risk(news_analyses)

        # Macro risk
        risk_components["macro_risk"] = self._score_macro_risk(macro_context)

        # Drawdown risk
        risk_components["drawdown"] = self._score_drawdown_risk(snapshot, risk_assessment)

        # Weighted risk score
        risk_alert_score = (
            risk_components["concentration"] * 0.20 +
            risk_components["volatility"] * 0.15 +
            risk_components["technical_breakdown"] * 0.25 +
            risk_components["news_risk"] * 0.15 +
            risk_components["macro_risk"] * 0.10 +
            risk_components["drawdown"] * 0.15
        )

        # === OPPORTUNITY SCORE ===
        opportunity_components = {}

        # Technical opportunity (trend + pullback + confirmation)
        opportunity_components["technical"] = self._score_technical_opportunity(
            technical_signals, snapshot
        )

        # News opportunity (positive catalysts)
        opportunity_components["news_catalyst"] = self._score_news_opportunity(news_analyses)

        # Macro tailwind
        opportunity_components["macro_tailwind"] = self._score_macro_opportunity(macro_context)

        # TradingView alert boost
        opportunity_components["tv_alerts"] = self._score_tv_alerts(tv_alerts, snapshot)

        # Sector rotation opportunity
        opportunity_components["sector_rotation"] = self._score_sector_rotation(
            technical_signals, snapshot
        )

        # Weighted opportunity score
        opportunity_score = (
            opportunity_components["technical"] * 0.40 +
            opportunity_components["news_catalyst"] * 0.25 +
            opportunity_components["macro_tailwind"] * 0.15 +
            opportunity_components["tv_alerts"] * 0.10 +
            opportunity_components["sector_rotation"] * 0.10
        )

        # === PER-TICKER SCORES ===
        ticker_scores = self._compute_ticker_scores(
            snapshot, technical_signals, news_analyses
        )

        # === DETERMINE TOP OPPORTUNITIES AND RISKS ===
        sorted_by_opportunity = sorted(
            ticker_scores.items(),
            key=lambda x: x[1].get("opportunity", 0),
            reverse=True
        )
        top_opportunities = [t for t, _ in sorted_by_opportunity[:5]]

        sorted_by_risk = sorted(
            ticker_scores.items(),
            key=lambda x: x[1].get("risk", 0),
            reverse=True
        )
        top_risks = [t for t, _ in sorted_by_risk[:5]]

        # === DETERMINE IF SHOULD ALERT ===
        should_alert = (
            risk_alert_score >= self.risk_score_min or
            opportunity_score >= self.opportunity_score_min
        )

        if risk_alert_score >= self.risk_score_min and opportunity_score >= self.opportunity_score_min:
            alert_type = "both"
        elif risk_alert_score >= self.risk_score_min:
            alert_type = "risk"
        elif opportunity_score >= self.opportunity_score_min:
            alert_type = "opportunity"
        else:
            alert_type = "none"

        return ScoreResult(
            risk_alert_score=risk_alert_score,
            opportunity_score=opportunity_score,
            risk_components=risk_components,
            opportunity_components=opportunity_components,
            ticker_scores=ticker_scores,
            top_opportunities=top_opportunities,
            top_risks=top_risks,
            should_alert=should_alert,
            alert_type=alert_type,
            timestamp=datetime.now(),
            computation_details={
                "num_holdings": len(snapshot.holdings),
                "total_value": snapshot.total_value,
                "risk_threshold": self.risk_score_min,
                "opportunity_threshold": self.opportunity_score_min,
            }
        )

    # =========================================================
    # RISK SCORING COMPONENTS
    # =========================================================

    def _score_concentration_risk(self, snapshot: PortfolioSnapshot) -> float:
        """Score concentration risk (0-100)."""
        score = 20.0  # Baseline

        if snapshot.top_holding_pct > self.max_single_position_pct:
            score += 30
        elif snapshot.top_holding_pct > self.max_single_position_pct * 0.75:
            score += 15

        if snapshot.top_3_holdings_pct > 70:
            score += 25
        elif snapshot.top_3_holdings_pct > 60:
            score += 10

        # Sector concentration
        max_sector = max(snapshot.sector_allocations.values()) if snapshot.sector_allocations else 0
        if max_sector > self.max_sector_pct:
            score += 25
        elif max_sector > self.max_sector_pct * 0.75:
            score += 10

        return min(100, score)

    def _score_volatility_risk(
        self,
        technical_signals: Dict[str, TechnicalSignal],
        snapshot: PortfolioSnapshot
    ) -> float:
        """Score volatility risk (0-100)."""
        if not technical_signals:
            return 50.0

        volatilities = []
        for h in snapshot.holdings:
            signal = technical_signals.get(h.symbol)
            if signal and signal.volatility_20d:
                weight = (h.current_value or 0) / snapshot.total_value if snapshot.total_value > 0 else 0
                volatilities.append((signal.volatility_20d, weight))

        if not volatilities:
            return 50.0

        total_weight = sum(w for _, w in volatilities)
        if total_weight == 0:
            # No valid weights (all holdings have $0 value) - return neutral
            return 50.0

        weighted_vol = sum(v * w for v, w in volatilities) / total_weight

        # Convert to score
        if weighted_vol > 50:
            return 90
        elif weighted_vol > 40:
            return 75
        elif weighted_vol > 30:
            return 60
        elif weighted_vol > 20:
            return 40
        else:
            return 25

    def _score_technical_breakdown_risk(
        self,
        technical_signals: Dict[str, TechnicalSignal],
        snapshot: PortfolioSnapshot
    ) -> float:
        """Score risk from technical breakdowns (0-100)."""
        if not technical_signals:
            return 50.0

        breakdown_scores = []
        total_weight = 0

        for h in snapshot.holdings:
            signal = technical_signals.get(h.symbol)
            if not signal:
                continue

            weight = (h.current_value or 0) / snapshot.total_value if snapshot.total_value > 0 else 0
            total_weight += weight

            position_score = 30.0  # Baseline

            # Below key MAs
            if signal.above_sma_200 is False:
                position_score += 25
            if signal.above_sma_50 is False:
                position_score += 15
            if signal.above_sma_20 is False:
                position_score += 10

            # Death cross
            if signal.death_cross:
                position_score += 20

            # Negative momentum
            if signal.change_5d_pct and signal.change_5d_pct < -10:
                position_score += 15

            # Support breakdown
            if signal.support_level and signal.price < signal.support_level:
                position_score += 20

            breakdown_scores.append(position_score * weight)

        if total_weight <= 0:
            return 50.0

        return min(100, sum(breakdown_scores) / total_weight)

    def _score_news_risk(self, news_analyses: Dict[str, Dict[str, Any]]) -> float:
        """Score risk from negative news (0-100)."""
        if not news_analyses:
            return 30.0

        negative_count = 0
        total = 0

        for symbol, analysis in news_analyses.items():
            sentiment = analysis.get("overall_sentiment")
            if sentiment in [NewsSentiment.NEGATIVE, NewsSentiment.VERY_NEGATIVE]:
                negative_count += 1
                if sentiment == NewsSentiment.VERY_NEGATIVE:
                    negative_count += 1  # Extra weight
            total += 1

        if total == 0:
            return 30.0

        negative_ratio = negative_count / total
        return min(100, 30 + negative_ratio * 70)

    def _score_macro_risk(self, macro_context: Optional[MacroContext]) -> float:
        """Score macro environment risk (0-100)."""
        if not macro_context:
            return 50.0

        if macro_context.regime == "risk_off":
            return 75
        elif macro_context.regime == "risk_on":
            return 25
        else:
            # Use overall score inverse
            return 100 - macro_context.overall_score

    def _score_drawdown_risk(
        self,
        snapshot: PortfolioSnapshot,
        risk_assessment: RiskAssessment
    ) -> float:
        """Score drawdown risk (0-100)."""
        return risk_assessment.drawdown_score

    # =========================================================
    # OPPORTUNITY SCORING COMPONENTS
    # =========================================================

    def _score_technical_opportunity(
        self,
        technical_signals: Dict[str, TechnicalSignal],
        snapshot: PortfolioSnapshot
    ) -> float:
        """Score technical opportunities (0-100)."""
        if not technical_signals:
            return 50.0

        opportunity_scores = []
        total_weight = 0

        for h in snapshot.holdings:
            signal = technical_signals.get(h.symbol)
            if not signal:
                continue

            weight = (h.current_value or 0) / snapshot.total_value if snapshot.total_value > 0 else 0
            total_weight += weight

            position_score = 40.0  # Baseline

            # Strong trend
            if signal.above_sma_20 and signal.above_sma_50 and signal.above_sma_200:
                position_score += 20

            # Golden cross
            if signal.golden_cross:
                position_score += 15

            # Pullback to support
            if signal.support_level and signal.price:
                distance_to_support = (signal.price - signal.support_level) / signal.price
                if 0 < distance_to_support < 0.03:  # Within 3% of support
                    position_score += 15

            # RSI oversold bounce
            if signal.rsi_14 and signal.rsi_14 < 35 and signal.change_1d_pct and signal.change_1d_pct > 0:
                position_score += 10

            # Positive momentum
            if signal.change_5d_pct and signal.change_5d_pct > 5:
                position_score += 10

            opportunity_scores.append(position_score * weight)

        if total_weight <= 0:
            return 50.0

        return min(100, sum(opportunity_scores) / total_weight)

    def _score_news_opportunity(self, news_analyses: Dict[str, Dict[str, Any]]) -> float:
        """Score opportunity from positive news (0-100)."""
        if not news_analyses:
            return 40.0

        positive_count = 0
        catalyst_count = 0
        total = 0

        for symbol, analysis in news_analyses.items():
            sentiment = analysis.get("overall_sentiment")
            has_catalyst = analysis.get("has_catalyst", False)

            if sentiment in [NewsSentiment.POSITIVE, NewsSentiment.VERY_POSITIVE]:
                positive_count += 1
                if sentiment == NewsSentiment.VERY_POSITIVE:
                    positive_count += 1

            if has_catalyst:
                catalyst_count += 1

            total += 1

        if total == 0:
            return 40.0

        positive_ratio = positive_count / total
        catalyst_ratio = catalyst_count / total

        return min(100, 40 + positive_ratio * 40 + catalyst_ratio * 20)

    def _score_macro_opportunity(self, macro_context: Optional[MacroContext]) -> float:
        """Score macro tailwind opportunity (0-100)."""
        if not macro_context:
            return 50.0

        if macro_context.regime == "risk_on":
            return 75
        elif macro_context.regime == "risk_off":
            return 30
        else:
            return macro_context.overall_score

    def _score_tv_alerts(
        self,
        tv_alerts: Optional[List[TradingViewAlert]],
        snapshot: PortfolioSnapshot
    ) -> float:
        """Score boost from TradingView alerts (0-100)."""
        if not tv_alerts:
            return 40.0

        # Get holdings symbols
        holdings_symbols = {h.symbol for h in snapshot.holdings}

        # Count relevant alerts
        relevant_alerts = [
            a for a in tv_alerts
            if a.ticker in holdings_symbols
        ]

        if not relevant_alerts:
            return 40.0

        # Check for bullish alerts
        bullish_count = 0
        for alert in relevant_alerts:
            if "bullish" in alert.tags or "buy" in alert.tags or "breakout" in alert.tags:
                bullish_count += 1

        # More bullish alerts = higher score
        return min(100, 40 + bullish_count * 15)

    def _score_sector_rotation(
        self,
        technical_signals: Dict[str, TechnicalSignal],
        snapshot: PortfolioSnapshot
    ) -> float:
        """Score sector rotation opportunity (0-100)."""
        if not technical_signals or not snapshot.sector_allocations:
            return 50.0

        # Calculate average signal score by sector
        sector_scores: Dict[str, List[float]] = {}

        for h in snapshot.holdings:
            signal = technical_signals.get(h.symbol)
            if not signal:
                continue

            sector = h.sector or "Unknown"
            if sector not in sector_scores:
                sector_scores[sector] = []

            sector_scores[sector].append(signal.signal_score)

        if not sector_scores:
            return 50.0

        # Average score per sector
        sector_averages = {
            sector: sum(scores) / len(scores)
            for sector, scores in sector_scores.items()
        }

        # Portfolio is well-positioned if we're overweight strong sectors
        weighted_score = 0
        total_weight = 0

        for sector, alloc_pct in snapshot.sector_allocations.items():
            sector_avg = sector_averages.get(sector, 50)
            weighted_score += sector_avg * alloc_pct
            total_weight += alloc_pct

        if total_weight <= 0:
            return 50.0

        return weighted_score / total_weight

    # =========================================================
    # PER-TICKER SCORES
    # =========================================================

    def _compute_ticker_scores(
        self,
        snapshot: PortfolioSnapshot,
        technical_signals: Dict[str, TechnicalSignal],
        news_analyses: Dict[str, Dict[str, Any]]
    ) -> Dict[str, Dict[str, float]]:
        """Compute scores for each ticker."""
        ticker_scores = {}

        for h in snapshot.holdings:
            symbol = h.symbol
            scores = {"opportunity": 50.0, "risk": 50.0}

            # Technical score
            signal = technical_signals.get(symbol)
            if signal:
                scores["technical"] = signal.signal_score
                scores["opportunity"] = (scores["opportunity"] + signal.signal_score) / 2

                # Risk from technical
                tech_risk = 50.0
                if signal.above_sma_200 is False:
                    tech_risk += 20
                if signal.death_cross:
                    tech_risk += 15
                if signal.change_5d_pct and signal.change_5d_pct < -10:
                    tech_risk += 15
                scores["risk"] = (scores["risk"] + tech_risk) / 2

            # News score
            news = news_analyses.get(symbol, {})
            if news:
                news_score = news.get("news_score", 50)
                scores["news"] = news_score

                if news.get("overall_sentiment") in [NewsSentiment.POSITIVE, NewsSentiment.VERY_POSITIVE]:
                    scores["opportunity"] += 10
                elif news.get("overall_sentiment") in [NewsSentiment.NEGATIVE, NewsSentiment.VERY_NEGATIVE]:
                    scores["risk"] += 15

            ticker_scores[symbol] = scores

        return ticker_scores


def compute_scores(
    snapshot: PortfolioSnapshot,
    technical_signals: Dict[str, TechnicalSignal],
    news_analyses: Dict[str, Dict[str, Any]],
    risk_assessment: RiskAssessment,
    macro_context: Optional[MacroContext] = None,
    tv_alerts: Optional[List[TradingViewAlert]] = None,
    weights: Optional[Dict[str, float]] = None,
    risk_score_min: float = 70,
    opportunity_score_min: float = 75,
) -> ScoreResult:
    """
    Convenience function to compute scores.

    Args:
        snapshot: Portfolio snapshot
        technical_signals: Technical signals
        news_analyses: News analyses
        risk_assessment: Risk assessment
        macro_context: Macro context
        tv_alerts: TradingView alerts
        weights: Signal weights
        risk_score_min: Risk threshold
        opportunity_score_min: Opportunity threshold

    Returns:
        ScoreResult
    """
    engine = ScoringEngine(
        weights=weights,
        risk_score_min=risk_score_min,
        opportunity_score_min=opportunity_score_min
    )
    return engine.compute(
        snapshot=snapshot,
        technical_signals=technical_signals,
        news_analyses=news_analyses,
        risk_assessment=risk_assessment,
        macro_context=macro_context,
        tv_alerts=tv_alerts
    )
