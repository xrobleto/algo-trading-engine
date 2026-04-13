"""
Recommendation Engine

Generates actionable recommendations based on scores and analysis.
Recommendations are rule-based; LLM is used only for narrative generation.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Any

from ..utils.logging import get_logger
from ..utils.typing import (
    PortfolioSnapshot, TechnicalSignal, ActionRecommendation,
    ActionType, Urgency, NewsSentiment, SignalStrength, EvidencePacket
)
from ..signals.risk import RiskAssessment
from ..signals.macro import MacroContext
from .scoring import ScoreResult

logger = get_logger(__name__)


@dataclass
class RecommendationResult:
    """Result of recommendation generation."""
    recommendations: List[ActionRecommendation]
    portfolio_notes: List[str]
    evidence_packet: EvidencePacket
    should_send_email: bool
    email_priority: str  # "high", "medium", "low"
    timestamp: datetime


class RecommendationEngine:
    """
    Generates actionable recommendations.

    Rules-based logic determines what actions to recommend.
    LLM is used later only for narrative summarization.
    """

    def __init__(
        self,
        max_recommendations: int = 7,
        high_confidence_threshold: int = 80,
        max_single_position_pct: float = 20,
        max_sector_pct: float = 40,
    ):
        """
        Initialize recommendation engine.

        Args:
            max_recommendations: Max recommendations per email
            high_confidence_threshold: Confidence level for "high" rating
            max_single_position_pct: Threshold for concentration warnings
            max_sector_pct: Threshold for sector concentration
        """
        self.max_recommendations = max_recommendations
        self.high_confidence_threshold = high_confidence_threshold
        self.max_single_position_pct = max_single_position_pct
        self.max_sector_pct = max_sector_pct

    def generate(
        self,
        snapshot: PortfolioSnapshot,
        score_result: ScoreResult,
        technical_signals: Dict[str, TechnicalSignal],
        news_analyses: Dict[str, Dict[str, Any]],
        risk_assessment: RiskAssessment,
        macro_context: Optional[MacroContext] = None,
    ) -> RecommendationResult:
        """
        Generate recommendations.

        Args:
            snapshot: Current portfolio
            score_result: Computed scores
            technical_signals: Technical signals
            news_analyses: News analyses
            risk_assessment: Risk assessment
            macro_context: Macro context

        Returns:
            RecommendationResult with all recommendations
        """
        recommendations = []
        portfolio_notes = []

        # === RISK-BASED RECOMMENDATIONS (Priority) ===
        if score_result.alert_type in ["risk", "both"]:
            risk_recs = self._generate_risk_recommendations(
                snapshot, score_result, technical_signals, risk_assessment
            )
            recommendations.extend(risk_recs)

        # === OPPORTUNITY-BASED RECOMMENDATIONS ===
        if score_result.alert_type in ["opportunity", "both"]:
            opp_recs = self._generate_opportunity_recommendations(
                snapshot, score_result, technical_signals, news_analyses, macro_context
            )
            recommendations.extend(opp_recs)

        # === CONCENTRATION WARNINGS ===
        concentration_notes = self._check_concentration(snapshot)
        portfolio_notes.extend(concentration_notes)

        # === MACRO NOTES ===
        if macro_context and macro_context.alerts:
            portfolio_notes.append(f"Macro alerts: {', '.join(macro_context.alerts)}")

        # === RISK FACTOR NOTES ===
        if risk_assessment.risk_factors:
            portfolio_notes.extend(risk_assessment.risk_factors[:3])

        # === SORT AND LIMIT RECOMMENDATIONS ===
        # Sort by urgency then confidence
        urgency_order = {Urgency.HIGH: 0, Urgency.MED: 1, Urgency.LOW: 2}
        recommendations.sort(key=lambda r: (urgency_order[r.urgency], -r.confidence))
        recommendations = recommendations[:self.max_recommendations]

        # === BUILD EVIDENCE PACKET ===
        evidence_packet = self._build_evidence_packet(
            snapshot, score_result, technical_signals, news_analyses,
            macro_context, risk_assessment
        )

        # === DETERMINE EMAIL PRIORITY ===
        should_send = score_result.should_alert
        email_priority = self._determine_email_priority(score_result, recommendations)

        return RecommendationResult(
            recommendations=recommendations,
            portfolio_notes=portfolio_notes[:5],
            evidence_packet=evidence_packet,
            should_send_email=should_send,
            email_priority=email_priority,
            timestamp=datetime.now(),
        )

    def _generate_risk_recommendations(
        self,
        snapshot: PortfolioSnapshot,
        score_result: ScoreResult,
        technical_signals: Dict[str, TechnicalSignal],
        risk_assessment: RiskAssessment,
    ) -> List[ActionRecommendation]:
        """Generate risk-based recommendations."""
        recommendations = []

        for symbol in score_result.top_risks:
            holding = snapshot.get_holding(symbol)
            if not holding:
                continue

            signal = technical_signals.get(symbol)
            ticker_score = score_result.ticker_scores.get(symbol, {})
            risk_score = ticker_score.get("risk", 50)

            # Skip low risk positions
            if risk_score < 60:
                continue

            action = ActionType.REVIEW
            urgency = Urgency.LOW
            rationale = []
            risks = []
            key_levels = {}

            # Determine action based on risk factors
            # Technical breakdown -> TRIM or SET_STOP
            if signal:
                if signal.above_sma_200 is False:
                    action = ActionType.TRIM
                    urgency = Urgency.MED
                    rationale.append("Trading below 200-day moving average")
                    risks.append("Could indicate longer-term trend change")

                if signal.death_cross:
                    action = ActionType.TRIM
                    urgency = Urgency.HIGH
                    rationale.append("Death cross detected (50 SMA crossed below 200 SMA)")

                if signal.change_5d_pct and signal.change_5d_pct < -15:
                    action = ActionType.SET_STOP if action == ActionType.REVIEW else action
                    urgency = Urgency.MED
                    rationale.append(f"Significant decline: {signal.change_5d_pct:.1f}% in 5 days")

                # Key levels
                key_levels["support"] = signal.support_level
                key_levels["resistance"] = signal.resistance_level
                if signal.support_level:
                    key_levels["stop"] = signal.support_level * 0.97  # 3% below support

            # Concentration risk -> TRIM
            weight = holding.weight_pct(snapshot.total_value)
            if weight > self.max_single_position_pct:
                action = ActionType.TRIM
                urgency = max(urgency, Urgency.MED)
                rationale.append(f"Position at {weight:.1f}% exceeds {self.max_single_position_pct}% limit")

            # Calculate confidence
            confidence = min(95, 50 + int(risk_score / 2))
            if urgency == Urgency.HIGH:
                confidence = min(95, confidence + 10)

            # Time horizon
            time_horizon = 7 if urgency == Urgency.HIGH else 14 if urgency == Urgency.MED else 30

            # Add general risks
            risks.append("Market conditions could deteriorate further")
            if not risks:
                risks.append("Position may recover - timing risk exists")

            recommendations.append(ActionRecommendation(
                ticker=symbol,
                action=action,
                urgency=urgency,
                confidence=confidence,
                time_horizon_days=time_horizon,
                rationale_bullets=rationale[:4],
                risks=risks[:3],
                key_levels=key_levels,
                risk_score=risk_score,
                overall_score=risk_score,
            ))

        return recommendations

    def _generate_opportunity_recommendations(
        self,
        snapshot: PortfolioSnapshot,
        score_result: ScoreResult,
        technical_signals: Dict[str, TechnicalSignal],
        news_analyses: Dict[str, Dict[str, Any]],
        macro_context: Optional[MacroContext],
    ) -> List[ActionRecommendation]:
        """Generate opportunity-based recommendations."""
        recommendations = []

        for symbol in score_result.top_opportunities:
            holding = snapshot.get_holding(symbol)
            if not holding:
                continue

            signal = technical_signals.get(symbol)
            news = news_analyses.get(symbol, {})
            ticker_score = score_result.ticker_scores.get(symbol, {})
            opp_score = ticker_score.get("opportunity", 50)

            # Skip low opportunity positions
            if opp_score < 55:
                continue

            action = ActionType.HOLD
            urgency = Urgency.LOW
            rationale = []
            risks = []
            key_levels = {}

            # Determine action based on opportunity factors
            if signal:
                # Strong uptrend -> HOLD or ADD
                if signal.signal_strength in [SignalStrength.STRONG_BULLISH, SignalStrength.BULLISH]:
                    action = ActionType.HOLD
                    rationale.append(f"Technical strength: {signal.signal_strength.value}")

                # Golden cross -> ADD consideration
                if signal.golden_cross:
                    action = ActionType.ADD
                    urgency = Urgency.MED
                    rationale.append("Golden cross: 50 SMA crossed above 200 SMA")

                # Pullback to support -> ADD
                if signal.support_level and signal.price:
                    dist_to_support = (signal.price - signal.support_level) / signal.price
                    if 0 < dist_to_support < 0.03:
                        action = ActionType.ADD
                        urgency = Urgency.MED
                        rationale.append(f"Near support level at ${signal.support_level:.2f}")

                # Set targets
                key_levels["support"] = signal.support_level
                key_levels["resistance"] = signal.resistance_level
                if signal.resistance_level:
                    key_levels["target"] = signal.resistance_level

            # Positive news catalyst -> ADD consideration
            if news:
                news_sentiment = news.get("overall_sentiment")
                if news_sentiment == NewsSentiment.VERY_POSITIVE:
                    action = ActionType.ADD
                    urgency = max(urgency, Urgency.MED)
                    rationale.append("Very positive news catalyst")
                elif news_sentiment == NewsSentiment.POSITIVE:
                    rationale.append("Positive news sentiment")

                if news.get("has_catalyst"):
                    catalyst_type = news.get("primary_catalyst")
                    rationale.append(f"Active catalyst: {catalyst_type}")

            # Macro tailwind
            if macro_context and macro_context.regime == "risk_on":
                rationale.append("Macro environment favorable (risk-on)")

            # Check if position is already large
            weight = holding.weight_pct(snapshot.total_value)
            if weight > self.max_single_position_pct * 0.8:
                action = ActionType.HOLD  # Don't add to already large position
                rationale.append(f"Position already at {weight:.1f}% - holding size")

            # If nothing actionable, suggest HOLD
            if action == ActionType.HOLD and not rationale:
                rationale.append("Position performing well - continue holding")

            # Calculate confidence
            confidence = min(95, 40 + int(opp_score / 2))

            # Time horizon based on signal
            time_horizon = 30 if action == ActionType.ADD else 60

            # Add risks
            risks.append("Market conditions could change rapidly")
            if signal and signal.rsi_14 and signal.rsi_14 > 70:
                risks.append("RSI indicates overbought conditions")

            recommendations.append(ActionRecommendation(
                ticker=symbol,
                action=action,
                urgency=urgency,
                confidence=confidence,
                time_horizon_days=time_horizon,
                rationale_bullets=rationale[:4],
                risks=risks[:3],
                key_levels=key_levels,
                technical_score=ticker_score.get("technical", 50),
                news_score=ticker_score.get("news", 50),
                overall_score=opp_score,
            ))

        return recommendations

    def _check_concentration(self, snapshot: PortfolioSnapshot) -> List[str]:
        """Check for concentration issues and return notes."""
        notes = []

        if snapshot.top_holding_pct > self.max_single_position_pct:
            notes.append(
                f"Top position at {snapshot.top_holding_pct:.1f}% "
                f"exceeds {self.max_single_position_pct}% limit"
            )

        if snapshot.top_3_holdings_pct > 60:
            notes.append(
                f"Top 3 holdings at {snapshot.top_3_holdings_pct:.1f}% - "
                "consider diversifying"
            )

        # Sector concentration
        for sector, pct in snapshot.sector_allocations.items():
            if pct > self.max_sector_pct:
                notes.append(f"{sector} sector at {pct:.1f}% exceeds {self.max_sector_pct}% limit")

        return notes

    def _build_evidence_packet(
        self,
        snapshot: PortfolioSnapshot,
        score_result: ScoreResult,
        technical_signals: Dict[str, TechnicalSignal],
        news_analyses: Dict[str, Dict[str, Any]],
        macro_context: Optional[MacroContext],
        risk_assessment: RiskAssessment,
    ) -> EvidencePacket:
        """Build evidence packet for LLM."""
        # Top holdings summary
        top_holdings = []
        for h in snapshot.holdings[:10]:
            top_holdings.append({
                "symbol": h.symbol,
                "weight_pct": h.weight_pct(snapshot.total_value),
                "pnl_pct": h.unrealized_pnl_pct,
                "current_price": h.current_price,
                "sector": h.sector,
            })

        # Per-ticker signals
        ticker_signals = {}
        for symbol, signal in technical_signals.items():
            ticker_signals[symbol] = {
                "technical_score": signal.signal_score,
                "signal_strength": signal.signal_strength.value,
                "rsi": signal.rsi_14,
                "trend": "bullish" if signal.above_sma_50 else "bearish",
                "support": signal.support_level,
                "resistance": signal.resistance_level,
                "change_5d": signal.change_5d_pct,
            }

        # Top news
        top_news = []
        for symbol, analysis in news_analyses.items():
            for news_item in analysis.get("news_items", [])[:2]:
                top_news.append({
                    "ticker": symbol,
                    "title": news_item.title[:100],
                    "url": news_item.url,
                    "published_at": news_item.published_at.isoformat(),
                    "sentiment": news_item.sentiment.value,
                })

        # Macro indicators
        macro_indicators = []
        if macro_context:
            for ind in macro_context.indicators:
                macro_indicators.append({
                    "name": ind.name,
                    "value": ind.value,
                    "change": ind.change,
                    "alert": ind.alert_triggered,
                })

        # Concentration flags
        concentration_flags = []
        if snapshot.top_holding_pct > self.max_single_position_pct:
            concentration_flags.append(f"Top holding {snapshot.top_holding_pct:.1f}%")
        if risk_assessment.sector_concentration_warning:
            concentration_flags.append("Sector concentration elevated")

        return EvidencePacket(
            total_value=snapshot.total_value,
            top_holdings=top_holdings,
            concentration_flags=concentration_flags,
            risk_alert_score=score_result.risk_alert_score,
            opportunity_score=score_result.opportunity_score,
            ticker_signals=ticker_signals,
            top_news=top_news[:10],
            macro_indicators=macro_indicators,
            tv_alerts=[],  # Add if available
            max_single_position_pct=self.max_single_position_pct,
            max_sector_pct=self.max_sector_pct,
            data_timestamps={
                "portfolio": datetime.now().isoformat(),
                "technical": datetime.now().isoformat(),
                "news": datetime.now().isoformat(),
                "macro": macro_context.last_updated.isoformat() if macro_context else None,
            },
        )

    def _determine_email_priority(
        self,
        score_result: ScoreResult,
        recommendations: List[ActionRecommendation]
    ) -> str:
        """Determine email priority level."""
        # Check for high urgency recommendations
        high_urgency_count = sum(1 for r in recommendations if r.urgency == Urgency.HIGH)

        if high_urgency_count >= 2 or score_result.risk_alert_score >= 85:
            return "high"
        elif high_urgency_count >= 1 or score_result.risk_alert_score >= 75:
            return "medium"
        else:
            return "low"


def generate_recommendations(
    snapshot: PortfolioSnapshot,
    score_result: ScoreResult,
    technical_signals: Dict[str, TechnicalSignal],
    news_analyses: Dict[str, Dict[str, Any]],
    risk_assessment: RiskAssessment,
    macro_context: Optional[MacroContext] = None,
    max_recommendations: int = 7,
) -> RecommendationResult:
    """
    Convenience function to generate recommendations.

    Args:
        snapshot: Portfolio snapshot
        score_result: Computed scores
        technical_signals: Technical signals
        news_analyses: News analyses
        risk_assessment: Risk assessment
        macro_context: Macro context
        max_recommendations: Max recommendations

    Returns:
        RecommendationResult
    """
    engine = RecommendationEngine(max_recommendations=max_recommendations)
    return engine.generate(
        snapshot=snapshot,
        score_result=score_result,
        technical_signals=technical_signals,
        news_analyses=news_analyses,
        risk_assessment=risk_assessment,
        macro_context=macro_context,
    )
