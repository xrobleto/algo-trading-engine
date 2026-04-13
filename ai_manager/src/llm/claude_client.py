"""
Claude LLM Client

Handles LLM interactions for narrative generation.
LLM is used ONLY for summarization - NOT for computing scores or inventing data.
"""

import json
import os
from datetime import datetime
from typing import Dict, List, Optional, Any

from ..utils.logging import get_logger
from ..utils.retry import retry_with_backoff
from ..utils.typing import (
    EvidencePacket, LLMResponse, ActionRecommendation, ActionType, Urgency
)

logger = get_logger(__name__)

# Try to import anthropic
try:
    import anthropic
    ANTHROPIC_AVAILABLE = True
except ImportError:
    ANTHROPIC_AVAILABLE = False
    logger.warning("anthropic package not installed - LLM features disabled")


# System prompt for the LLM
SYSTEM_PROMPT = """You are a professional investment analyst assistant. Your role is to summarize portfolio data and generate readable narratives for investment recommendations.

CRITICAL RULES:
1. You must NOT invent any data, prices, dates, or metrics. Only use what is provided.
2. You must NOT compute indicators or scores - those are pre-computed and provided to you.
3. Your job is to SUMMARIZE and EXPLAIN the pre-computed analysis in human-readable form.
4. Be concise and professional. Avoid hype or guarantees.
5. Always mention uncertainty and risks alongside opportunities.
6. Output MUST be valid JSON matching the specified schema.

You will receive an "evidence packet" with:
- Pre-computed scores (risk_alert_score, opportunity_score)
- Per-ticker signals (already computed)
- Top news items (titles and URLs)
- Macro indicators
- Portfolio constraints

Your task: Generate a professional narrative that explains the analysis to the portfolio owner.
"""

JSON_SCHEMA_PROMPT = """
Output MUST be valid JSON with this exact structure:
{
  "executive_summary": "1-3 sentence summary of what changed and why it matters",
  "top_actions": [
    {
      "ticker": "SYMBOL",
      "action": "ADD|TRIM|HOLD|HEDGE|SET_STOP|TAKE_PROFIT|REVIEW",
      "urgency": "LOW|MED|HIGH",
      "rationale_bullets": ["reason 1", "reason 2"],
      "key_levels": {"support": number|null, "resistance": number|null, "stop": number|null, "target": number|null},
      "risks": ["risk 1", "risk 2"],
      "confidence": integer 0-100,
      "time_horizon_days": integer 1-180
    }
  ],
  "portfolio_notes": ["note 1", "note 2"],
  "citations": [
    {"type": "news", "title": "string", "url": "string|null", "published_at": "ISO-8601|null", "tickers": ["SYM"]}
  ],
  "data_freshness": {"massive": "ISO-8601|null", "alpaca": "ISO-8601|null", "fred": "ISO-8601|null", "tradingview": "ISO-8601|null"}
}

IMPORTANT: Your response must be ONLY the JSON object. No markdown, no explanation, just the JSON.
"""


class ClaudeClient:
    """
    Claude LLM client for generating narratives.

    Strictly controlled to prevent hallucination:
    - Only receives pre-computed evidence
    - Must output structured JSON
    - Validated with pydantic
    - Falls back to rule-based output on failure
    """

    def __init__(
        self,
        api_key: Optional[str] = None,
        model: str = "claude-sonnet-4-20250514",
        temperature: float = 0.2,
        max_tokens: int = 2000,
        max_retries: int = 2,
        retry_delay: float = 2.0,
    ):
        """
        Initialize Claude client.

        Args:
            api_key: Anthropic API key (or from env)
            model: Model to use
            temperature: Temperature for generation
            max_tokens: Max output tokens
            max_retries: Retry attempts on failure
            retry_delay: Delay between retries
        """
        self.api_key = api_key or os.environ.get("ANTHROPIC_API_KEY")
        self.model = model
        self.temperature = temperature
        self.max_tokens = max_tokens
        self.max_retries = max_retries
        self.retry_delay = retry_delay

        self._client = None
        self._enabled = False

        if not ANTHROPIC_AVAILABLE:
            logger.warning("Anthropic package not available")
            return

        if not self.api_key:
            logger.warning("No Anthropic API key - LLM disabled")
            return

        try:
            self._client = anthropic.Anthropic(api_key=self.api_key)
            self._enabled = True
            logger.info(f"Claude client initialized with model {self.model}")
        except Exception as e:
            logger.error(f"Failed to initialize Anthropic client: {e}")

    @property
    def is_available(self) -> bool:
        """Check if client is available."""
        return self._enabled and self._client is not None

    def generate_response(
        self,
        evidence_packet: EvidencePacket,
        recommendations: List[ActionRecommendation],
    ) -> Optional[LLMResponse]:
        """
        Generate LLM response from evidence packet.

        Args:
            evidence_packet: Pre-computed evidence
            recommendations: Pre-computed recommendations

        Returns:
            Validated LLMResponse or None on failure
        """
        if not self.is_available:
            logger.warning("LLM not available, skipping generation")
            return None

        # Build user prompt with evidence
        user_prompt = self._build_prompt(evidence_packet, recommendations)

        # Try to generate and parse response
        for attempt in range(self.max_retries + 1):
            try:
                response_text = self._call_api(user_prompt)
                if not response_text:
                    continue

                # Parse and validate JSON
                parsed = self._parse_response(response_text)
                if parsed:
                    return parsed

                # If parsing failed, try repair on next attempt
                if attempt < self.max_retries:
                    logger.warning(f"LLM response parsing failed, attempt {attempt + 1}")
                    user_prompt = self._build_repair_prompt(response_text)

            except Exception as e:
                logger.error(f"LLM generation failed: {e}")

        logger.error("All LLM attempts failed, returning None")
        return None

    def _build_prompt(
        self,
        evidence_packet: EvidencePacket,
        recommendations: List[ActionRecommendation],
    ) -> str:
        """Build the user prompt with evidence."""
        # Convert recommendations to dict format
        rec_dicts = []
        for rec in recommendations:
            rec_dicts.append({
                "ticker": rec.ticker,
                "action": rec.action.value,
                "urgency": rec.urgency.value,
                "confidence": rec.confidence,
                "rationale": rec.rationale_bullets,
                "key_levels": rec.key_levels,
            })

        prompt = f"""
Here is the evidence packet for analysis:

PORTFOLIO SUMMARY:
- Total Value: ${evidence_packet.total_value:,.2f}
- Risk Alert Score: {evidence_packet.risk_alert_score:.1f}/100
- Opportunity Score: {evidence_packet.opportunity_score:.1f}/100

TOP HOLDINGS:
{json.dumps(evidence_packet.top_holdings[:7], indent=2)}

CONCENTRATION FLAGS:
{json.dumps(evidence_packet.concentration_flags, indent=2)}

TICKER SIGNALS (pre-computed):
{json.dumps(dict(list(evidence_packet.ticker_signals.items())[:7]), indent=2)}

TOP NEWS:
{json.dumps(evidence_packet.top_news[:8], indent=2)}

MACRO INDICATORS:
{json.dumps(evidence_packet.macro_indicators, indent=2)}

PRE-COMPUTED RECOMMENDATIONS (use these as basis):
{json.dumps(rec_dicts, indent=2)}

DATA TIMESTAMPS:
{json.dumps(evidence_packet.data_timestamps, indent=2)}

{JSON_SCHEMA_PROMPT}

Generate the JSON response now:
"""
        return prompt

    def _build_repair_prompt(self, failed_response: str) -> str:
        """Build a repair prompt when parsing failed."""
        return f"""
Your previous response was not valid JSON. Here it is:
{failed_response[:1000]}

Please provide ONLY valid JSON matching the schema. No markdown, no explanation, just the JSON object.

{JSON_SCHEMA_PROMPT}
"""

    @retry_with_backoff(max_retries=2, base_delay=2.0, exceptions=(Exception,))
    def _call_api(self, user_prompt: str) -> Optional[str]:
        """Call the Anthropic API."""
        if not self._client:
            return None

        message = self._client.messages.create(
            model=self.model,
            max_tokens=self.max_tokens,
            temperature=self.temperature,
            system=SYSTEM_PROMPT,
            messages=[
                {"role": "user", "content": user_prompt}
            ]
        )

        if message.content and len(message.content) > 0:
            return message.content[0].text

        return None

    def _parse_response(self, response_text: str) -> Optional[LLMResponse]:
        """Parse and validate LLM response."""
        # Clean up response (remove markdown if present)
        text = response_text.strip()
        if text.startswith("```json"):
            text = text[7:]
        if text.startswith("```"):
            text = text[3:]
        if text.endswith("```"):
            text = text[:-3]
        text = text.strip()

        try:
            data = json.loads(text)
            validated = LLMResponse(**data)
            return validated
        except json.JSONDecodeError as e:
            logger.warning(f"JSON decode error: {e}")
            return None
        except Exception as e:
            logger.warning(f"Validation error: {e}")
            return None


def generate_email_content(
    evidence_packet: EvidencePacket,
    recommendations: List[ActionRecommendation],
    api_key: Optional[str] = None,
) -> Optional[LLMResponse]:
    """
    Convenience function to generate email content.

    Args:
        evidence_packet: Pre-computed evidence
        recommendations: Pre-computed recommendations
        api_key: Optional API key override

    Returns:
        LLMResponse or None
    """
    client = ClaudeClient(api_key=api_key)
    return client.generate_response(evidence_packet, recommendations)


def generate_fallback_content(
    evidence_packet: EvidencePacket,
    recommendations: List[ActionRecommendation],
) -> Dict[str, Any]:
    """
    Generate fallback content when LLM fails.

    This produces a rule-based email without LLM narrative.

    Args:
        evidence_packet: Pre-computed evidence
        recommendations: Pre-computed recommendations

    Returns:
        Dict mimicking LLMResponse structure
    """
    # Generate simple executive summary
    if evidence_packet.risk_alert_score >= 70:
        summary = (
            f"Risk Alert: Portfolio risk score elevated at {evidence_packet.risk_alert_score:.0f}/100. "
            f"Review recommended actions below."
        )
    elif evidence_packet.opportunity_score >= 75:
        summary = (
            f"Opportunity Alert: Portfolio opportunity score at {evidence_packet.opportunity_score:.0f}/100. "
            f"Consider the recommended actions."
        )
    else:
        summary = (
            f"Portfolio Update: Risk score {evidence_packet.risk_alert_score:.0f}/100, "
            f"Opportunity score {evidence_packet.opportunity_score:.0f}/100."
        )

    # Convert recommendations to LLM format
    top_actions = []
    for rec in recommendations[:7]:
        top_actions.append({
            "ticker": rec.ticker,
            "action": rec.action.value,
            "urgency": rec.urgency.value,
            "rationale_bullets": rec.rationale_bullets,
            "key_levels": rec.key_levels,
            "risks": rec.risks,
            "confidence": rec.confidence,
            "time_horizon_days": rec.time_horizon_days,
        })

    # Portfolio notes
    portfolio_notes = evidence_packet.concentration_flags[:3]

    # Citations from news
    citations = []
    for news in evidence_packet.top_news[:5]:
        citations.append({
            "type": "news",
            "title": news.get("title", ""),
            "url": news.get("url"),
            "published_at": news.get("published_at"),
            "tickers": [news.get("ticker")] if news.get("ticker") else [],
        })

    return {
        "executive_summary": summary,
        "top_actions": top_actions,
        "portfolio_notes": portfolio_notes,
        "citations": citations,
        "data_freshness": evidence_packet.data_timestamps,
    }
