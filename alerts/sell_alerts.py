#!/usr/bin/env python3
"""
sell_alerts.py - HYSA Funding Sell Alert Engine
================================================

Monitors portfolio positions and sends email alerts when sell conditions are met,
helping fund a HYSA (High Yield Savings Account) target.

Features:
- Separate thresholds for "strength" (sell into gains) vs "riskoff" (cut losses)
- HTML + plain-text multipart email with summary table and per-ticker details
- Per-ticker cooldown (not global) to prevent alert spam
- Tax awareness: warns when positions are near long-term capital gains threshold
- Proper DST handling via zoneinfo + holiday awareness
- Robinhood activity CSV support (estimated cost basis from buy/sell netting)
- API key redaction in logs for security

Important:
- Does NOT place orders; alerts only.
- Robinhood activity-based positions are estimates (no true tax lots / FIFO).
- Cost basis from activity CSV may differ from actual broker records.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import logging
import math
import os
import re
import time
from dataclasses import dataclass
from email.message import EmailMessage
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import yaml
from dotenv import load_dotenv

# --- Timezone Constants ---
ET = ZoneInfo("America/New_York")  # Handles DST automatically

# Load config from .env file if present
_env_path = Path(__file__).parent / "sell_alerts.env"
if _env_path.exists():
    load_dotenv(_env_path)


# -----------------------------
# Utilities
# -----------------------------

def now_utc() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)

def parse_date(s: str) -> dt.date:
    return dt.date.fromisoformat(s.strip())

def iso(dt_obj: dt.datetime) -> str:
    return dt_obj.isoformat()

def safe_float(x: Any) -> Optional[float]:
    if x is None:
        return None
    if isinstance(x, (int, float)) and not (isinstance(x, float) and math.isnan(x)):
        return float(x)
    s = str(x).strip()
    if s == "" or s.lower() in {"nan", "none", "null", "--"}:
        return None
    s = s.replace(",", "")
    neg = False
    if s.startswith("(") and s.endswith(")"):
        neg = True
        s = s[1:-1]
    s = re.sub(r"[^0-9.\-]", "", s)
    if s in {"", "-", "."}:
        return None
    try:
        v = float(s)
        return -v if neg else v
    except ValueError:
        return None

def sha1_text(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()

def redact_api_keys(text: str) -> str:
    """
    Redact potential API keys from text to prevent accidental logging.

    Patterns redacted:
    - apiKey=... query params
    - Bearer tokens
    - Common API key formats (alphanumeric 20+ chars)
    """
    import re
    # Redact apiKey query parameter
    text = re.sub(r'apiKey=[A-Za-z0-9_\-]+', 'apiKey=REDACTED', text)
    # Redact Authorization headers
    text = re.sub(r'Bearer\s+[A-Za-z0-9_\-\.]+', 'Bearer REDACTED', text)
    # Redact long alphanumeric strings that look like keys (20+ chars)
    text = re.sub(r'\b[A-Za-z0-9]{20,}\b', '[REDACTED_KEY]', text)
    return text

def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)

def in_us_market_hours_et(now: dt.datetime) -> bool:
    """
    Check if current time is within US market regular trading hours.

    Uses zoneinfo for proper DST handling. Does NOT check holidays -
    for production use, integrate a holiday calendar (e.g., pandas_market_calendars).
    """
    # Convert to Eastern Time (handles DST automatically)
    t = now.astimezone(ET)

    # Weekend check
    if t.weekday() >= 5:
        return False

    # RTH: 9:30 AM - 4:00 PM ET
    start = t.replace(hour=9, minute=30, second=0, microsecond=0)
    end = t.replace(hour=16, minute=0, second=0, microsecond=0)
    return start <= t <= end


# --- US Market Holidays (rule-based; no hard-coded years) ---
# Note: This covers the standard full-day NYSE holidays and observed dates.
# It does NOT currently model NYSE early closes (e.g., day after Thanksgiving).
from functools import lru_cache

def _observed(d: dt.date) -> dt.date:
    # If holiday falls on Saturday -> observed Friday; Sunday -> observed Monday.
    if d.weekday() == 5:
        return d - dt.timedelta(days=1)
    if d.weekday() == 6:
        return d + dt.timedelta(days=1)
    return d

def _nth_weekday(year: int, month: int, weekday: int, n: int) -> dt.date:
    # weekday: Monday=0 ... Sunday=6; n: 1=first, 2=second, ...
    first = dt.date(year, month, 1)
    offset = (weekday - first.weekday()) % 7
    return first + dt.timedelta(days=offset + 7 * (n - 1))

def _last_weekday(year: int, month: int, weekday: int) -> dt.date:
    # Last given weekday in a month
    if month == 12:
        last = dt.date(year, 12, 31)
    else:
        last = dt.date(year, month + 1, 1) - dt.timedelta(days=1)
    offset = (last.weekday() - weekday) % 7
    return last - dt.timedelta(days=offset)

def _easter_sunday(year: int) -> dt.date:
    # Anonymous Gregorian algorithm
    a = year % 19
    b = year // 100
    c = year % 100
    d = b // 4
    e = b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i = c // 4
    k = c % 4
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    month = (h + l - 7 * m + 114) // 31
    day = ((h + l - 7 * m + 114) % 31) + 1
    return dt.date(year, month, day)

@lru_cache(maxsize=32)
def nyse_holidays(year: int) -> set[dt.date]:
    hol: set[dt.date] = set()

    # New Year's Day (and observed)
    ny = dt.date(year, 1, 1)
    obs_ny = _observed(ny)
    if obs_ny.year == year:
        hol.add(obs_ny)
    # Include Dec 31 observed for next year's Jan 1 (when Jan 1 is Saturday)
    ny_next = dt.date(year + 1, 1, 1)
    obs_ny_next = _observed(ny_next)
    if obs_ny_next.year == year:
        hol.add(obs_ny_next)

    # Martin Luther King Jr. Day: 3rd Monday in January
    hol.add(_nth_weekday(year, 1, weekday=0, n=3))

    # Washington's Birthday / Presidents Day: 3rd Monday in February
    hol.add(_nth_weekday(year, 2, weekday=0, n=3))

    # Good Friday: Friday before Easter Sunday (2 days before)
    easter = _easter_sunday(year)
    hol.add(easter - dt.timedelta(days=2))

    # Memorial Day: last Monday in May
    hol.add(_last_weekday(year, 5, weekday=0))

    # Juneteenth: June 19 (and observed)
    hol.add(_observed(dt.date(year, 6, 19)))

    # Independence Day: July 4 (and observed)
    hol.add(_observed(dt.date(year, 7, 4)))

    # Labor Day: 1st Monday in September
    hol.add(_nth_weekday(year, 9, weekday=0, n=1))

    # Thanksgiving Day: 4th Thursday in November
    hol.add(_nth_weekday(year, 11, weekday=3, n=4))

    # Christmas Day: Dec 25 (and observed)
    hol.add(_observed(dt.date(year, 12, 25)))

    return hol

def is_market_holiday(date: dt.date) -> bool:
    """Check if a date is a standard NYSE full-day holiday (observed)."""
    return date in nyse_holidays(date.year)

def is_market_open(now: dt.datetime) -> bool:
    """True if within RTH (ET), not weekend, and not a holiday."""
    t = now.astimezone(ET)
    if t.weekday() >= 5:
        return False
    if is_market_holiday(t.date()):
        return False
    return in_us_market_hours_et(now)

def money(x: Optional[float], decimals: int = 0) -> str:
    if x is None:
        return "—"
    fmt = f"{{:,.{decimals}f}}"
    return "$" + fmt.format(x)

def pct(x: Optional[float], decimals: int = 1) -> str:
    if x is None:
        return "—"
    return f"{x*100:.{decimals}f}%"

def esc_html(s: str) -> str:
    return (s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
              .replace('"', "&quot;").replace("'", "&#39;"))


# -----------------------------
# Data Models
# -----------------------------

@dataclass
class Position:
    broker: str
    account: str
    symbol: str
    quantity: float
    avg_cost: Optional[float] = None
    cost_basis_total: Optional[float] = None
    market_value_total: Optional[float] = None
    # Tax awareness: earliest known purchase date for holding period calculation
    earliest_purchase_date: Optional[dt.date] = None

    @property
    def normalized_symbol(self) -> str:
        return self.symbol.strip().upper()

    def infer_cost_basis_total(self) -> Optional[float]:
        if self.cost_basis_total is not None:
            return self.cost_basis_total
        if self.avg_cost is not None and self.quantity is not None:
            return float(self.avg_cost) * float(self.quantity)
        return None

    def holding_period_days(self, as_of: dt.date = None) -> Optional[int]:
        """Calculate days held since earliest purchase."""
        if self.earliest_purchase_date is None:
            return None
        as_of = as_of or dt.date.today()
        return (as_of - self.earliest_purchase_date).days

    def is_long_term(self, as_of: dt.date = None) -> Optional[bool]:
        """
        Check if position qualifies for long-term capital gains (held > 1 year).

        Returns None if purchase date unknown.
        """
        days = self.holding_period_days(as_of)
        if days is None:
            return None
        return days > 365

    def days_until_long_term(self, as_of: dt.date = None) -> Optional[int]:
        """
        Days remaining until position becomes long-term.

        Returns None if unknown, 0 if already long-term, positive if still short-term.
        """
        days = self.holding_period_days(as_of)
        if days is None:
            return None
        remaining = 366 - days  # Need > 365 days
        return max(0, remaining)

@dataclass
class HysaPlan:
    goal_usd: float
    quit_date: dt.date
    current_hysa_usd: float
    monthly_targets: List[Tuple[dt.date, float]]

@dataclass
class TickerAnalysis:
    symbol: str
    last_price: Optional[float]
    day_close: Optional[float]
    ema20: Optional[float]
    ema50: Optional[float]
    rsi14: Optional[float]
    atr14: Optional[float]
    boll_upper: Optional[float]
    boll_lower: Optional[float]
    drawdown_20d: Optional[float]
    news_sentiment_score: Optional[float]
    news_items: List[Dict[str, Any]]  # ENHANCED: Full article details (title, description, date, etc)
    score: float
    mode: str  # "strength" | "riskoff" | "none"
    reasons: List[str]
    # NOTE: company_name removed - was never populated from any API

@dataclass
class SellCandidate:
    symbol: str
    score: float
    mode: str
    suggested_shares: float
    est_proceeds: float
    suggested_limit: Optional[float]
    reasons: List[str]
    # Tax awareness
    is_long_term: Optional[bool] = None  # True = long-term gains, False = short-term
    days_until_long_term: Optional[int] = None  # Days until LTCG eligible (0 = already LT)


@dataclass
class AISellAssessment:
    """AI-generated comprehensive assessment of a sell candidate."""
    # Core assessment
    conviction: str  # HIGH, MEDIUM, LOW
    conviction_score: int  # 1-100 numeric score for visual meter
    headline: str  # One-line summary for quick scanning
    urgency: str  # "Sell Now", "Can Wait", "Watch Closely"
    urgency_reason: str  # Brief explanation of urgency
    reasoning: str  # 2-3 sentence summary

    # Risk analysis
    risk_if_hold: str  # What could happen if you don't sell
    downside_scenario: str  # "If drops to support at $X, you lose $Y"
    downside_pct: float  # Estimated downside percentage

    # Action recommendation
    position_action: str  # "Full exit", "Trim 50%", "Small trim 25%"
    suggested_exit_price: Optional[float]  # Suggested limit price

    # Opportunity cost
    opportunity_cost: str  # "This capital could earn X in HYSA"

    # Tax considerations
    tax_note: Optional[str]  # Tax-aware recommendation
    tax_impact: str  # "Short-term gains taxed at income rate" etc.

    # Company/Market context
    company_context: str  # Brief company description
    recent_developments: str  # Key recent news summary
    sector_trend: str  # How sector is performing
    analyst_sentiment: str  # General analyst sentiment

    # Timing factors
    upcoming_events: List[str]  # Earnings, ex-div dates, etc.
    hold_factors: List[str]  # Reasons you might want to hold


@dataclass
class MarketContext:
    """Current market conditions for sell decision context."""
    spy_price: float
    spy_change_pct: float
    spy_trend: str  # "bullish", "bearish", "neutral"
    vix_level: float
    vix_context: str  # "low", "normal", "elevated", "high"
    market_regime: str  # "risk-on", "risk-off", "mixed"


# -----------------------------
# AI Sell Analyzer (Enhanced with Research)
# -----------------------------

class AISellAnalyzer:
    """
    Enhanced AI analyzer for sell candidates using Claude with comprehensive research.

    Multi-step analysis:
    1. Gather market context (SPY, VIX, regime)
    2. Research each candidate (company info, news, catalysts)
    3. Synthesize into actionable sell recommendations
    """

    def __init__(self):
        self.anthropic_key = os.environ.get("ANTHROPIC_API_KEY", "")
        self.polygon_key = os.environ.get("POLYGON_API_KEY", "")

    def analyze_candidates(
        self,
        candidates: List[SellCandidate],
        analyses: Dict[str, TickerAnalysis],
        hysa_shortfall: float = 0.0,
        hysa_rate: float = 0.045  # Current HYSA APY for opportunity cost
    ) -> Dict[str, AISellAssessment]:
        """
        Analyze sell candidates with comprehensive AI research.

        Args:
            candidates: List of sell candidates
            analyses: Technical analysis for each ticker
            hysa_shortfall: How much cash is needed for HYSA goal
            hysa_rate: Current HYSA APY (for opportunity cost calculation)

        Returns:
            Dict mapping symbol to AISellAssessment
        """
        if not self.anthropic_key:
            logging.warning("No ANTHROPIC_API_KEY found, skipping AI analysis")
            return {}

        if not candidates:
            return {}

        try:
            # Step 1: Get market context
            market_ctx = self._get_market_context()
            logging.info(f"Market context: {market_ctx.market_regime}, VIX={market_ctx.vix_level:.1f}")

            # Step 2: Analyze each candidate
            assessments = {}
            for candidate in candidates[:5]:  # Limit to top 5 for API efficiency
                analysis = analyses.get(candidate.symbol)
                if not analysis:
                    continue

                try:
                    assessment = self._analyze_single_candidate(
                        candidate, analysis, market_ctx, hysa_shortfall, hysa_rate
                    )
                    if assessment:
                        assessments[candidate.symbol] = assessment
                except Exception as e:
                    logging.warning(f"Failed to analyze {candidate.symbol}: {e}")
                    continue

            return assessments
        except Exception as e:
            logging.error("AI analysis failed: %s", redact_api_keys(str(e)))
            return {}

    def _get_market_context(self) -> MarketContext:
        """Fetch current market conditions for context."""
        try:
            spy_data = self._get_polygon_snapshot("SPY")
            vix_data = self._get_polygon_snapshot("VIX") or {}

            spy_price = spy_data.get("ticker", {}).get("day", {}).get("c", 0)
            spy_prev = spy_data.get("ticker", {}).get("prevDay", {}).get("c", 0)
            spy_change = ((spy_price - spy_prev) / spy_prev * 100) if spy_prev else 0

            vix_price = vix_data.get("ticker", {}).get("day", {}).get("c", 15)

            # Determine market regime
            if spy_change > 0.5 and vix_price < 18:
                market_regime = "risk-on"
                spy_trend = "bullish"
            elif spy_change < -0.5 or vix_price > 25:
                market_regime = "risk-off"
                spy_trend = "bearish"
            else:
                market_regime = "mixed"
                spy_trend = "neutral"

            # VIX context
            if vix_price < 15:
                vix_context = "low"
            elif vix_price < 20:
                vix_context = "normal"
            elif vix_price < 30:
                vix_context = "elevated"
            else:
                vix_context = "high"

            return MarketContext(
                spy_price=spy_price,
                spy_change_pct=spy_change,
                spy_trend=spy_trend,
                vix_level=vix_price,
                vix_context=vix_context,
                market_regime=market_regime
            )
        except Exception as e:
            logging.warning(f"Failed to get market context: {e}")
            return MarketContext(
                spy_price=0, spy_change_pct=0, spy_trend="unknown",
                vix_level=0, vix_context="unknown", market_regime="unknown"
            )

    def _get_polygon_snapshot(self, symbol: str) -> Dict[str, Any]:
        """Get snapshot data from Polygon."""
        if not self.polygon_key:
            return {}
        try:
            url = f"https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers/{symbol}"
            resp = requests.get(url, params={"apiKey": self.polygon_key}, timeout=10)
            if resp.status_code == 200:
                return resp.json()
        except Exception:
            pass
        return {}

    def _analyze_single_candidate(
        self,
        candidate: SellCandidate,
        analysis: TickerAnalysis,
        market_ctx: MarketContext,
        hysa_shortfall: float,
        hysa_rate: float
    ) -> Optional[AISellAssessment]:
        """Perform comprehensive AI analysis on a single sell candidate."""

        prompt = self._build_research_prompt(candidate, analysis, market_ctx, hysa_shortfall, hysa_rate)

        try:
            response = requests.post(
                "https://api.anthropic.com/v1/messages",
                headers={
                    "x-api-key": self.anthropic_key,
                    "anthropic-version": "2023-06-01",
                    "content-type": "application/json"
                },
                json={
                    "model": "claude-sonnet-4-20250514",
                    "max_tokens": 2500,
                    "messages": [{"role": "user", "content": prompt}]
                },
                timeout=45
            )

            if response.status_code != 200:
                logging.error(f"Claude API error for {candidate.symbol}: {response.status_code}")
                return None

            data = response.json()
            content = data.get("content", [{}])[0].get("text", "")
            return self._parse_enhanced_response(content, candidate)

        except requests.exceptions.Timeout:
            logging.error(f"Claude API timeout for {candidate.symbol}")
            return None
        except Exception as e:
            logging.error(f"Claude API error for {candidate.symbol}: {e}")
            return None

    def _build_research_prompt(
        self,
        candidate: SellCandidate,
        analysis: TickerAnalysis,
        market_ctx: MarketContext,
        hysa_shortfall: float,
        hysa_rate: float
    ) -> str:
        """Build comprehensive research prompt for sell analysis."""

        px = analysis.last_price if analysis.last_price is not None else analysis.day_close

        # Format full news content
        news_details = "No recent news available."
        if analysis.news_items:
            news_parts = []
            for n in analysis.news_items[:5]:
                title = n.get("title", "")
                desc = n.get("description", "")
                pub = n.get("published_str", "")
                sentiment = n.get("sentiment", "")
                if title:
                    news_parts.append(f"- [{pub}] {title}")
                    if desc:
                        news_parts.append(f"  {desc[:300]}...")
                    if sentiment:
                        news_parts.append(f"  Sentiment: {sentiment}")
            news_details = "\n".join(news_parts) if news_parts else "No recent news available."

        # Tax status
        tax_status = "Unknown holding period"
        if candidate.is_long_term is True:
            tax_status = "LONG-TERM (held >1 year) - favorable 0/15/20% rate"
        elif candidate.is_long_term is False:
            if candidate.days_until_long_term and candidate.days_until_long_term > 0:
                tax_status = f"SHORT-TERM ({candidate.days_until_long_term} days until LTCG) - taxed at ordinary income rate"
            else:
                tax_status = "SHORT-TERM - taxed at ordinary income rate"

        # Calculate opportunity cost
        annual_hysa_return = candidate.est_proceeds * hysa_rate
        monthly_hysa_return = annual_hysa_return / 12

        # Calculate potential downside to support levels
        support_price = analysis.ema50 if analysis.ema50 else (analysis.ema20 * 0.95 if analysis.ema20 else px * 0.9)
        downside_to_support = ((px - support_price) / px * 100) if px > 0 else 0
        dollar_at_risk = candidate.est_proceeds * (downside_to_support / 100)

        return f"""You are an expert investment analyst preparing a concise SELL recommendation report.

ANALYZE THIS POSITION: {candidate.symbol}

=== POSITION DATA ===
Current Price: ${px:.2f}
Mode: {candidate.mode.upper()} ({"selling into strength/taking profits" if candidate.mode == "strength" else "cutting losses/risk management"})
Sell Score: {candidate.score:.0f}/100
Suggested Shares to Sell: {candidate.suggested_shares:.2f}
Estimated Proceeds: ${candidate.est_proceeds:,.0f}

=== TAX STATUS ===
{tax_status}

=== TECHNICAL INDICATORS ===
RSI(14): {f'{analysis.rsi14:.1f}' if analysis.rsi14 else 'N/A'} {"(overbought >70)" if analysis.rsi14 and analysis.rsi14 > 70 else "(oversold <30)" if analysis.rsi14 and analysis.rsi14 < 30 else ""}
EMA20: ${analysis.ema20:.2f if analysis.ema20 else 'N/A'}
EMA50: ${analysis.ema50:.2f if analysis.ema50 else 'N/A'}
Bollinger Upper: ${analysis.boll_upper:.2f if analysis.boll_upper else 'N/A'}
Bollinger Lower: ${analysis.boll_lower:.2f if analysis.boll_lower else 'N/A'}
20-Day Drawdown: {f'{analysis.drawdown_20d*100:.1f}%' if analysis.drawdown_20d else 'N/A'}
News Sentiment Score: {f'{analysis.news_sentiment_score:.2f}' if analysis.news_sentiment_score else 'N/A'}

=== SIGNAL REASONS ===
{chr(10).join('- ' + r for r in candidate.reasons[:5])}

=== RECENT NEWS ===
{news_details}

=== MARKET CONTEXT ===
SPY: ${market_ctx.spy_price:.2f} ({market_ctx.spy_change_pct:+.1f}%)
Market Trend: {market_ctx.spy_trend.upper()}
VIX: {market_ctx.vix_level:.1f} ({market_ctx.vix_context})
Market Regime: {market_ctx.market_regime.upper()}

=== OPPORTUNITY COST ===
If sold, ${candidate.est_proceeds:,.0f} in HYSA at {hysa_rate*100:.1f}% APY would earn:
- ${annual_hysa_return:,.0f}/year
- ${monthly_hysa_return:,.0f}/month (guaranteed)

=== DOWNSIDE RISK ===
Estimated support level: ${support_price:.2f}
If price drops to support: -{downside_to_support:.1f}% (${dollar_at_risk:,.0f} at risk)

=== HYSA FUNDING CONTEXT ===
{"Need $" + f"{hysa_shortfall:,.0f} to meet next HYSA milestone. This sale would cover " + f"{min(100, candidate.est_proceeds/hysa_shortfall*100):.0f}% of that goal." if hysa_shortfall > 0 else "No immediate HYSA funding pressure."}

=== YOUR TASK ===
Provide a comprehensive but CONCISE sell analysis. Use your knowledge to add context about this company, sector trends, and any relevant factors.

Respond in this EXACT format (each field on its own line):

HEADLINE: [One compelling sentence summarizing the sell decision - max 15 words]
CONVICTION: [HIGH/MEDIUM/LOW]
SCORE: [1-100 numeric conviction score]
URGENCY: [Sell Now/Can Wait/Watch Closely]
URGENCY_REASON: [Brief 1-sentence explanation of timing]
REASONING: [2-3 sentences explaining your assessment]
RISK_IF_HOLD: [1-2 sentences: What could go wrong if they don't sell]
DOWNSIDE_SCENARIO: [e.g., "If drops to $X support, loses $Y (Z%)"]
DOWNSIDE_PCT: [number only, e.g., 8.5]
POSITION_ACTION: [Full exit/Trim 50%/Small trim 25%]
SUGGESTED_EXIT_PRICE: [suggested limit price or "Market"]
OPPORTUNITY_COST: [1 sentence about HYSA alternative]
TAX_NOTE: [Tax consideration or "N/A"]
TAX_IMPACT: [Brief note on tax treatment]
COMPANY_CONTEXT: [1-2 sentences: What does this company do?]
RECENT_DEVELOPMENTS: [1-2 sentences summarizing key recent events]
SECTOR_TREND: [1 sentence on sector performance]
ANALYST_SENTIMENT: [Brief note on analyst/market sentiment or "Unknown"]
UPCOMING_EVENTS: [comma-separated list of events like "Earnings Feb 15, Ex-div Mar 1" or "None identified"]
HOLD_FACTORS: [comma-separated reasons to potentially hold, or "None"]

Be direct and actionable. Focus on whether to sell NOW or wait.
"""

    def _parse_enhanced_response(self, content: str, candidate: SellCandidate) -> Optional[AISellAssessment]:
        """Parse enhanced AI response into AISellAssessment."""
        # Initialize with defaults
        assessment = {
            "conviction": "MEDIUM",
            "conviction_score": 50,
            "headline": f"{candidate.symbol} shows sell signal",
            "urgency": "Watch Closely",
            "urgency_reason": "",
            "reasoning": "",
            "risk_if_hold": "",
            "downside_scenario": "",
            "downside_pct": 0.0,
            "position_action": "Trim 50%",
            "suggested_exit_price": None,
            "opportunity_cost": "",
            "tax_note": None,
            "tax_impact": "",
            "company_context": "",
            "recent_developments": "",
            "sector_trend": "",
            "analyst_sentiment": "Unknown",
            "upcoming_events": [],
            "hold_factors": []
        }

        # Parse each field
        for line in content.split("\n"):
            line = line.strip()
            if not line or ":" not in line:
                continue

            key, _, value = line.partition(":")
            key = key.strip().upper().replace(" ", "_")
            value = value.strip()

            if key == "HEADLINE":
                assessment["headline"] = value
            elif key == "CONVICTION":
                if value.upper() in ("HIGH", "MEDIUM", "LOW"):
                    assessment["conviction"] = value.upper()
            elif key == "SCORE":
                try:
                    score = int(re.search(r'\d+', value).group())
                    assessment["conviction_score"] = max(1, min(100, score))
                except (AttributeError, ValueError):
                    pass
            elif key == "URGENCY":
                if value in ("Sell Now", "Can Wait", "Watch Closely"):
                    assessment["urgency"] = value
            elif key == "URGENCY_REASON":
                assessment["urgency_reason"] = value
            elif key == "REASONING":
                assessment["reasoning"] = value
            elif key == "RISK_IF_HOLD":
                assessment["risk_if_hold"] = value
            elif key == "DOWNSIDE_SCENARIO":
                assessment["downside_scenario"] = value
            elif key == "DOWNSIDE_PCT":
                try:
                    assessment["downside_pct"] = float(re.search(r'[\d.]+', value).group())
                except (AttributeError, ValueError):
                    pass
            elif key == "POSITION_ACTION":
                if value in ("Full exit", "Trim 50%", "Small trim 25%"):
                    assessment["position_action"] = value
            elif key == "SUGGESTED_EXIT_PRICE":
                if value.lower() != "market":
                    try:
                        price = float(re.search(r'[\d.]+', value).group())
                        assessment["suggested_exit_price"] = price
                    except (AttributeError, ValueError):
                        pass
            elif key == "OPPORTUNITY_COST":
                assessment["opportunity_cost"] = value
            elif key == "TAX_NOTE":
                if value.upper() != "N/A":
                    assessment["tax_note"] = value
            elif key == "TAX_IMPACT":
                assessment["tax_impact"] = value
            elif key == "COMPANY_CONTEXT":
                assessment["company_context"] = value
            elif key == "RECENT_DEVELOPMENTS":
                assessment["recent_developments"] = value
            elif key == "SECTOR_TREND":
                assessment["sector_trend"] = value
            elif key == "ANALYST_SENTIMENT":
                assessment["analyst_sentiment"] = value
            elif key == "UPCOMING_EVENTS":
                if value.lower() not in ("none", "none identified", "n/a"):
                    assessment["upcoming_events"] = [e.strip() for e in value.split(",") if e.strip()][:3]
            elif key == "HOLD_FACTORS":
                if value.lower() not in ("none", "n/a"):
                    assessment["hold_factors"] = [f.strip() for f in value.split(",") if f.strip()][:3]

        return AISellAssessment(
            conviction=assessment["conviction"],
            conviction_score=assessment["conviction_score"],
            headline=assessment["headline"],
            urgency=assessment["urgency"],
            urgency_reason=assessment["urgency_reason"],
            reasoning=assessment["reasoning"],
            risk_if_hold=assessment["risk_if_hold"],
            downside_scenario=assessment["downside_scenario"],
            downside_pct=assessment["downside_pct"],
            position_action=assessment["position_action"],
            suggested_exit_price=assessment["suggested_exit_price"],
            opportunity_cost=assessment["opportunity_cost"],
            tax_note=assessment["tax_note"],
            tax_impact=assessment["tax_impact"],
            company_context=assessment["company_context"],
            recent_developments=assessment["recent_developments"],
            sector_trend=assessment["sector_trend"],
            analyst_sentiment=assessment["analyst_sentiment"],
            upcoming_events=assessment["upcoming_events"],
            hold_factors=assessment["hold_factors"]
        )


# -----------------------------
# Polygon / Massive Client
# -----------------------------

class PolygonClient:
    def __init__(self, api_key: str, base_url: str, cache_dir: Path, timeout: int = 20):
        self._api_key = api_key  # Private to avoid accidental logging
        self.base_url = base_url.rstrip("/")
        self.cache_dir = cache_dir
        self.timeout = timeout
        self.session = requests.Session()


        # Retry/backoff for transient API errors (429/5xx)

        retry = Retry(

            total=3,

            connect=3,

            read=3,

            status=3,

            backoff_factor=0.8,

            status_forcelist=(429, 500, 502, 503, 504),

            allowed_methods=frozenset(["GET"]),

            respect_retry_after_header=True,

            raise_on_status=False,

        )

        adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)

        self.session.mount("https://", adapter)

        self.session.mount("http://", adapter)

    @property
    def api_key(self) -> str:
        """API key accessor - use sparingly to minimize exposure."""
        return self._api_key

    def __repr__(self) -> str:
        """Safe repr that doesn't expose API key."""
        return f"PolygonClient(base_url={self.base_url!r}, cache_dir={self.cache_dir!r})"

    def _get(self, path: str, params: Optional[Dict[str, Any]] = None, cache_ttl_sec: int = 0) -> Dict[str, Any]:
        params = dict(params or {})
        params["apiKey"] = self.api_key
        url = f"{self.base_url}{path}"

        cache_key = None
        if cache_ttl_sec > 0:
            cache_key = sha1_text(url + "?" + "&".join([f"{k}={params[k]}" for k in sorted(params.keys())]))
            cache_path = self.cache_dir / f"{cache_key}.json"
            if cache_path.exists():
                age = time.time() - cache_path.stat().st_mtime
                if age <= cache_ttl_sec:
                    try:
                        with open(cache_path, "r", encoding="utf-8") as f:
                            content = f.read().strip()
                            if content:  # Only parse if non-empty
                                return json.loads(content)
                            # Empty file - try to delete and fetch fresh
                            try:
                                cache_path.unlink(missing_ok=True)
                            except (PermissionError, OSError):
                                pass  # File locked by another process, just fetch fresh
                    except (json.JSONDecodeError, ValueError):
                        # Corrupted cache - try to delete and fetch fresh
                        try:
                            cache_path.unlink(missing_ok=True)
                        except (PermissionError, OSError):
                            pass  # File locked by another process, just fetch fresh

        r = self.session.get(url, params=params, timeout=self.timeout)
        r.raise_for_status()
        data = r.json()

        if cache_ttl_sec > 0 and cache_key:
            cache_path = self.cache_dir / f"{cache_key}.json"
            try:
                with open(cache_path, "w", encoding="utf-8") as f:
                    json.dump(data, f)
            except (PermissionError, OSError):
                pass  # Can't write cache, continue without caching

        return data

    def get_daily_aggs(self, symbol: str, from_date: dt.date, to_date: dt.date) -> pd.DataFrame:
        path = f"/v2/aggs/ticker/{symbol}/range/1/day/{from_date.isoformat()}/{to_date.isoformat()}"
        data = self._get(path, params={"adjusted": "true", "sort": "asc", "limit": 50000}, cache_ttl_sec=300)
        results = data.get("results", []) or []
        if not results:
            return pd.DataFrame()
        df = pd.DataFrame(results)
        df["date"] = pd.to_datetime(df["t"], unit="ms", utc=True).dt.date
        df.rename(columns={"o": "open", "h": "high", "l": "low", "c": "close", "v": "volume"}, inplace=True)
        return df[["date", "open", "high", "low", "close", "volume"]].copy()

    def get_snapshot(self, symbol: str) -> Dict[str, Any]:
        path = f"/v2/snapshot/locale/us/markets/stocks/tickers/{symbol}"
        return self._get(path, params={}, cache_ttl_sec=10)

    def get_news(self, symbol: str, limit: int = 20) -> Dict[str, Any]:
        path = "/v2/reference/news"
        # Increase limit to account for post-filtering (some articles won't be relevant)
        return self._get(
            path,
            params={
                "ticker": symbol,
                "limit": int(limit * 3),  # Request 3x to ensure enough after filtering
                "order": "desc",
                "sort": "published_utc"
            },
            cache_ttl_sec=300
        )


# -----------------------------
# Indicators
# -----------------------------

def ema(series: pd.Series, span: int) -> pd.Series:
    return series.ewm(span=span, adjust=False).mean()

def rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta = series.diff()
    up = delta.clip(lower=0.0)
    down = (-delta).clip(lower=0.0)
    roll_up = up.ewm(alpha=1/period, adjust=False).mean()
    roll_down = down.ewm(alpha=1/period, adjust=False).mean()
    rs = roll_up / roll_down.replace(0, np.nan)
    return 100 - (100 / (1 + rs))

def atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    high = df["high"].astype(float)
    low = df["low"].astype(float)
    close = df["close"].astype(float)
    prev_close = close.shift(1)
    tr = pd.concat([
        (high - low),
        (high - prev_close).abs(),
        (low - prev_close).abs()
    ], axis=1).max(axis=1)
    return tr.ewm(alpha=1/period, adjust=False).mean()

def bollinger(series: pd.Series, window: int = 20, num_std: float = 2.0) -> Tuple[pd.Series, pd.Series]:
    ma = series.rolling(window=window).mean()
    sd = series.rolling(window=window).std(ddof=0)
    upper = ma + num_std * sd
    lower = ma - num_std * sd
    return upper, lower


# -----------------------------
# HYSA planning
# -----------------------------

def build_hysa_plan(goal: float, quit_date: dt.date, current_hysa: float, today: dt.date) -> HysaPlan:
    deadlines: List[dt.date] = []
    cursor = dt.date(today.year, today.month, 1)
    while cursor <= quit_date:
        next_month = (cursor.replace(day=28) + dt.timedelta(days=4)).replace(day=1)
        month_end = next_month - dt.timedelta(days=1)
        if month_end >= today and month_end < quit_date:
            deadlines.append(month_end)
        cursor = next_month
    if quit_date not in deadlines:
        deadlines.append(quit_date)

    remaining = max(goal - current_hysa, 0.0)
    n = len(deadlines)
    monthly_targets = []
    for i, d in enumerate(deadlines, start=1):
        target = current_hysa + (remaining * (i / n)) if n > 0 else goal
        monthly_targets.append((d, round(target, 2)))

    return HysaPlan(goal_usd=goal, quit_date=quit_date, current_hysa_usd=current_hysa, monthly_targets=monthly_targets)

def next_target(plan: HysaPlan, current_hysa: float, today: dt.date) -> Tuple[dt.date, float, float]:
    for d, tgt in plan.monthly_targets:
        if d >= today:
            shortfall = max(tgt - current_hysa, 0.0)
            return d, tgt, round(shortfall, 2)
    d, tgt = plan.monthly_targets[-1]
    shortfall = max(tgt - current_hysa, 0.0)
    return d, tgt, round(shortfall, 2)


# -----------------------------
# Broker ingestion
# -----------------------------

def load_positions_from_positions_csv(
    file_path: str,
    delimiter: str,
    broker_name: str,
    column_map: Dict[str, str],
) -> List[Position]:
    p = Path(file_path)
    if not p.exists():
        logging.warning("Broker file not found: %s", file_path)
        return []

    df = pd.read_csv(p, delimiter=delimiter)
    df.rename(columns={c: c.strip() for c in df.columns}, inplace=True)

    def col(field: str) -> Optional[str]:
        return column_map.get(field)

    required = ["symbol", "quantity"]
    for r in required:
        if not col(r) or col(r) not in df.columns:
            raise ValueError(f"{broker_name}: missing required mapped column '{r}' -> '{col(r)}' not found in CSV columns.")

    out: List[Position] = []
    for _, row in df.iterrows():
        sym = str(row[col("symbol")]).strip().upper()
        if sym == "" or sym.lower() in {"cash", "sweep", "mmf"}:
            continue
        qty = safe_float(row[col("quantity")])
        if qty is None or qty == 0:
            continue

        avg_cost = safe_float(row[col("avg_cost")]) if col("avg_cost") and col("avg_cost") in df.columns else None
        cb_total = safe_float(row[col("cost_basis_total")]) if col("cost_basis_total") and col("cost_basis_total") in df.columns else None
        mv_total = safe_float(row[col("market_value_total")]) if col("market_value_total") and col("market_value_total") in df.columns else None
        acct = str(row[col("account_name")]).strip() if col("account_name") and col("account_name") in df.columns else ""

        out.append(Position(
            broker=broker_name,
            account=acct,
            symbol=sym,
            quantity=float(qty),
            avg_cost=avg_cost,
            cost_basis_total=cb_total,
            market_value_total=mv_total
        ))
    return out

def load_positions_from_robinhood_activity_csv(
    file_path: str,
    delimiter: str,
    broker_name: str,
    column_map: Dict[str, str],
) -> List[Position]:
    """
    Reconstruct holdings from Robinhood activity export by netting Buy/Sell.
    Uses moving-average cost (approx) because the export doesn't provide tax lots.

    KNOWN LIMITATIONS (estimates only, may not match actual account):
    - Cost basis uses simple moving average, not FIFO/specific lot
    - Transfers-in (ACATS) have unknown cost basis and are EXCLUDED
    - Stock splits, mergers, and corporate actions may not be handled
    - Dividend reinvestments may not parse correctly
    - Wash sale adjustments are NOT applied
    - Options exercises/assignments are NOT handled

    For accurate cost basis, use Robinhood's tax documents or positions export.
    """
    p = Path(file_path)
    if not p.exists():
        logging.warning("Broker file not found: %s", file_path)
        return []

    df = pd.read_csv(p, delimiter=delimiter)
    df.rename(columns={c: c.strip() for c in df.columns}, inplace=True)

    def col(field: str) -> str:
        name = column_map.get(field)
        if not name or name not in df.columns:
            raise ValueError(f"{broker_name} Robinhood activity: missing mapped column '{field}' -> '{name}'")
        return name

    sym_col = col("instrument")
    code_col = col("trans_code")
    qty_col = col("quantity")
    price_col = col("price")
    amt_col = col("amount")

    df["__sym"] = df[sym_col].astype(str).str.strip().str.upper()
    df["__code"] = df[code_col].astype(str).str.strip()
    df["__qty"] = df[qty_col].apply(safe_float)
    df["__price"] = df[price_col].apply(safe_float)
    df["__amt"] = df[amt_col].apply(safe_float)

    df = df[df["__sym"].ne("")].copy()
    df = df[df["__code"].isin(["Buy", "Sell"])].copy()
    df = df[df["__qty"].notna()].copy()

    if df.empty:
        logging.warning("Robinhood activity file loaded but no Buy/Sell rows detected.")
        return []

    if "activity_date" in column_map and column_map["activity_date"] in df.columns:
        try:
            df["__dt"] = pd.to_datetime(df[column_map["activity_date"]], errors="coerce")
            df = df.sort_values("__dt")
        except Exception:
            pass

    # Track: shares, cost, and earliest purchase date for tax purposes
    holdings: Dict[str, Dict[str, Any]] = {}
    for _, row in df.iterrows():
        sym = row["__sym"]
        code = row["__code"]
        qty = float(row["__qty"])
        price = row["__price"]
        amt = row["__amt"]
        row_date = row.get("__dt")  # May be NaT or None

        if amt is not None:
            cash = float(amt)
        else:
            cash = float(qty) * float(price) if price is not None else 0.0

        h = holdings.setdefault(sym, {"shares": 0.0, "cost": 0.0, "earliest_date": None})

        if code == "Buy":
            buy_cost = abs(cash)
            h["shares"] += qty
            h["cost"] += buy_cost

            # Track earliest purchase date for long-term gains calculation
            if row_date is not None and pd.notna(row_date):
                buy_date = row_date.date() if hasattr(row_date, 'date') else row_date
                if h["earliest_date"] is None or buy_date < h["earliest_date"]:
                    h["earliest_date"] = buy_date

        elif code == "Sell":
            if h["shares"] <= 0:
                continue
            sell_qty = min(qty, h["shares"])
            avg_cost = h["cost"] / h["shares"] if h["shares"] > 0 else 0.0
            h["shares"] -= sell_qty
            h["cost"] -= avg_cost * sell_qty
            # Note: earliest_date is preserved (FIFO would be more accurate but requires lot tracking)

    positions: List[Position] = []
    for sym, h in holdings.items():
        shares = h["shares"]
        cost = h["cost"]
        if shares <= 0.0000001:
            continue
        avg_cost = cost / shares if shares > 0 else None
        positions.append(Position(
            broker=broker_name,
            account="",
            symbol=sym,
            quantity=float(shares),
            avg_cost=float(avg_cost) if avg_cost is not None else None,
            cost_basis_total=float(cost) if cost is not None else None,
            market_value_total=None,
            earliest_purchase_date=h.get("earliest_date")
        ))

    if positions:
        logging.warning(
            "%s: Reconstructed %d positions from activity CSV. "
            "Cost basis is ESTIMATED (moving avg). May not match actual account. "
            "Transfers-in, splits, wash sales NOT handled.",
            broker_name, len(positions)
        )

    return positions

def load_all_positions(cfg: Dict[str, Any]) -> List[Position]:
    brokers = cfg.get("brokers", [])
    all_pos: List[Position] = []
    for b in brokers:
        if not b.get("enabled", True):
            continue
        broker_name = b["broker_name"]
        broker_type = b.get("broker_type", "positions_csv")
        if broker_type == "robinhood_activity":
            all_pos.extend(load_positions_from_robinhood_activity_csv(
                file_path=b["file_path"],
                delimiter=b.get("delimiter", ","),
                broker_name=broker_name,
                column_map=b.get("column_map", {}),
            ))
        else:
            all_pos.extend(load_positions_from_positions_csv(
                file_path=b["file_path"],
                delimiter=b.get("delimiter", ","),
                broker_name=broker_name,
                column_map=b.get("column_map", {}),
            ))
    return all_pos

def aggregate_positions(positions: List[Position]) -> pd.DataFrame:
    rows = []
    for p in positions:
        rows.append({
            "symbol": p.normalized_symbol,
            "quantity": p.quantity,
            "cost_basis_total": p.infer_cost_basis_total(),
            "avg_cost": p.avg_cost,
            "earliest_purchase_date": p.earliest_purchase_date
        })
    df = pd.DataFrame(rows)
    if df.empty:
        return df

    def wavg_cost(g: pd.DataFrame) -> Optional[float]:
        cb = g["cost_basis_total"].dropna()
        qty = g.loc[cb.index, "quantity"]
        if cb.empty or qty.sum() == 0:
            return None
        return float(cb.sum() / qty.sum())

    def earliest_date(g: pd.DataFrame) -> Optional[dt.date]:
        """Get the earliest purchase date across all lots."""
        dates = g["earliest_purchase_date"].dropna()
        if dates.empty:
            return None
        return min(dates)

    grouped = []
    for sym, g in df.groupby("symbol"):
        qty = float(g["quantity"].sum())
        cb_total = float(g["cost_basis_total"].sum()) if g["cost_basis_total"].notna().any() else None
        avg_cost = wavg_cost(g)
        earliest = earliest_date(g)
        grouped.append({
            "symbol": sym,
            "quantity": qty,
            "cost_basis_total": cb_total,
            "avg_cost": avg_cost,
            "earliest_purchase_date": earliest
        })
    return pd.DataFrame(grouped).sort_values("symbol").reset_index(drop=True)


# -----------------------------
# News scoring
# -----------------------------

def score_news(news_json: Dict[str, Any], lookback_hours: int, symbol: str = "") -> Tuple[Optional[float], List[Dict[str, Any]]]:
    """
    Score news sentiment and extract relevant articles with full details.

    ENHANCED VERSION: Returns detailed article info including descriptions, not just headlines.

    Args:
        news_json: Polygon news API response
        lookback_hours: Only consider news from this many hours ago
        symbol: Ticker symbol to filter for (only include news mentioning this symbol)

    Returns:
        Tuple of (sentiment_score, news_items)
        news_items is list of dicts with: title, description, published_str, relevance_score, sentiment
    """
    results = news_json.get("results", []) or []
    if not results:
        return None, []

    cutoff = now_utc() - dt.timedelta(hours=lookback_hours)
    sentiments = []
    articles = []

    for r in results:
        title = (r.get("title") or "").strip()
        description = (r.get("description") or "").strip()

        if not title:
            continue

        # Parse publish date
        published = r.get("published_utc")
        try:
            published_dt = dt.datetime.fromisoformat(published.replace("Z", "+00:00")) if published else None
        except Exception:
            published_dt = None

        # Skip old articles
        if published_dt and published_dt < cutoff:
            continue

        # ENHANCED: Calculate relevance score (0-1) instead of binary filter
        relevance_score = 0.0
        if symbol:
            symbol_upper = symbol.upper()
            tickers_in_article = r.get("tickers", []) or []
            title_upper = title.upper()
            desc_upper = description.upper()

            # Primary ticker match (strongest signal)
            if tickers_in_article and tickers_in_article[0].upper() == symbol_upper:
                relevance_score += 0.5

            # Mentioned in title
            if symbol_upper in title_upper:
                words_in_title = title_upper.split()
                if symbol_upper in words_in_title:
                    relevance_score += 0.4  # Standalone word
                else:
                    relevance_score += 0.2  # Part of word

            # Mentioned in description
            if symbol_upper in desc_upper:
                words_in_desc = desc_upper.split()
                if symbol_upper in words_in_desc:
                    relevance_score += 0.2
                else:
                    relevance_score += 0.1

            # Position in ticker list
            try:
                ticker_index = next(i for i, t in enumerate(tickers_in_article) if t.upper() == symbol_upper)
                if ticker_index == 0:
                    relevance_score += 0.3
                elif ticker_index == 1:
                    relevance_score += 0.2
                elif ticker_index == 2:
                    relevance_score += 0.1
            except StopIteration:
                pass

            relevance_score = min(relevance_score, 1.0)

            # Skip if not relevant enough (threshold: 0.3 = moderate relevance)
            if relevance_score < 0.3:
                continue

        # Format publish time
        published_str = ""
        if published_dt:
            now = now_utc()
            delta = now - published_dt
            if delta.days > 0:
                published_str = f"{delta.days}d ago"
            elif delta.seconds >= 3600:
                hours = delta.seconds // 3600
                published_str = f"{hours}h ago"
            else:
                minutes = delta.seconds // 60
                published_str = f"{minutes}m ago"

        # Extract sentiment
        article_sentiment = None
        insights = r.get("insights") or []
        for ins in insights:
            s = (ins.get("sentiment") or ins.get("sentiment_reasoning") or "").lower()
            if "positive" in s and "not" not in s:
                sentiments.append(1.0)
                article_sentiment = "positive"
                break
            elif "negative" in s:
                sentiments.append(-1.0)
                article_sentiment = "negative"
                break
            elif "neutral" in s:
                sentiments.append(0.0)
                article_sentiment = "neutral"
                break

        # Truncate long descriptions
        if len(description) > 300:
            description = description[:297] + "..."

        articles.append({
            "title": title,
            "description": description if description else None,
            "published_str": published_str,
            "relevance_score": relevance_score,
            "sentiment": article_sentiment
        })

    # Sort by relevance (descending)
    articles.sort(key=lambda x: x["relevance_score"], reverse=True)

    # Calculate sentiment score
    if sentiments:
        score = float(np.clip(np.mean(sentiments), -1.0, 1.0))
        return score, articles[:5]  # Return top 5 most relevant
    return None, articles[:5]


# -----------------------------
# Ticker analysis & scoring
# -----------------------------

def analyze_ticker(client: PolygonClient, symbol: str, lookback_days: int, cfg: Dict[str, Any]) -> TickerAnalysis:
    today = dt.date.today()
    from_date = today - dt.timedelta(days=lookback_days)
    df = client.get_daily_aggs(symbol, from_date, today)
    if df.empty or len(df) < 60:
        return TickerAnalysis(
            symbol=symbol,
            last_price=None, day_close=None,
            ema20=None, ema50=None, rsi14=None, atr14=None,
            boll_upper=None, boll_lower=None,
            drawdown_20d=None,
            news_sentiment_score=None,
            news_items=[],
            score=0.0, mode="none",
            reasons=["Insufficient price history from data provider."]
        )

    df = df.sort_values("date").reset_index(drop=True)
    close = df["close"].astype(float)

    ema20_s = ema(close, 20)
    ema50_s = ema(close, 50)
    rsi14_s = rsi(close, 14)
    atr14_s = atr(df, 14)
    boll_u, boll_l = bollinger(close, 20, 2)

    rolling_high_20 = close.rolling(20).max()
    dd_20 = (close / rolling_high_20) - 1.0
    high_20d = float(rolling_high_20.iloc[-1])

    snap = client.get_snapshot(symbol)
    last_price = None
    try:
        t = snap.get("ticker", {}) or {}
        last_trade = t.get("lastTrade") or {}
        last_price = safe_float(last_trade.get("p"))
        if last_price is None:
            day = t.get("day") or {}
            last_price = safe_float(day.get("c"))
    except Exception:
        last_price = None

    news = client.get_news(symbol, limit=int(cfg["news_limit"]))
    news_score, news_items = score_news(news, int(cfg["news_lookback_hours"]), symbol=symbol)

    ema20_v = float(ema20_s.iloc[-1])
    ema50_v = float(ema50_s.iloc[-1])
    rsi_v = float(rsi14_s.iloc[-1]) if not np.isnan(rsi14_s.iloc[-1]) else None
    atr_v = float(atr14_s.iloc[-1]) if not np.isnan(atr14_s.iloc[-1]) else None
    boll_u_v = float(boll_u.iloc[-1]) if not np.isnan(boll_u.iloc[-1]) else None
    boll_l_v = float(boll_l.iloc[-1]) if not np.isnan(boll_l.iloc[-1]) else None
    dd20_v = float(dd_20.iloc[-1]) if not np.isnan(dd_20.iloc[-1]) else None
    day_close = float(close.iloc[-1])

    px = last_price if last_price is not None else day_close

    score = 0.0
    reasons: List[str] = []
    mode = "none"

    extend_pct = (px / ema20_v) - 1.0 if ema20_v else 0.0

    if rsi_v is not None:
        if rsi_v >= cfg["rsi_very_overbought"]:
            score += 35
            reasons.append(f"RSI {rsi_v:.1f} - very overbought (≥{cfg['rsi_very_overbought']}) - prime opportunity to trim")
        elif rsi_v >= cfg["rsi_overbought"]:
            score += 25
            reasons.append(f"RSI {rsi_v:.1f} - overbought (≥{cfg['rsi_overbought']}) - strength sell signal")

    if extend_pct >= cfg["extend_over_ema20_pct"]:
        score += 25
        reasons.append(f"Extended {extend_pct*100:+.1f}% above EMA20 ({money(ema20_v,2)}) - take some off the table")

    if boll_u_v is not None and px >= boll_u_v:
        score += 15
        pct_vs_band = ((px - boll_u_v) / boll_u_v) * 100
        reasons.append(f"At/above upper Bollinger ({money(boll_u_v,2)}, {pct_vs_band:+.1f}%) - strength/exhaustion")

    if ema50_v and px > ema50_v:
        score += 10
        ema50_pct = ((px - ema50_v) / ema50_v) * 100
        reasons.append(f"Above EMA50 by {ema50_pct:.1f}% - uptrend intact, selling into strength")
    elif ema50_v and px < ema50_v:
        score += 15
        ema50_pct = ((px - ema50_v) / ema50_v) * 100
        reasons.append(f"Below EMA50 by {abs(ema50_pct):.1f}% - trend weakening, risk-off")

    if dd20_v is not None and dd20_v <= -abs(cfg["drawdown_from_20d_high_pct"]):
        score += 20
        reasons.append(f"Down {abs(dd20_v)*100:.1f}% from 20D high ({money(high_20d,2)}) - significant pullback")

    if news_score is not None:
        if news_score <= -0.3:
            score += 15
            reasons.append(f"⚠ News sentiment: {news_score:+.2f} (negative skew - risk-off signal)")
        elif news_score >= 0.3:
            score += 5
            reasons.append(f"News sentiment: {news_score:+.2f} (positive - sell into strength)")

    strength_points = 0
    if rsi_v is not None and rsi_v >= cfg["rsi_overbought"]:
        strength_points += 1
    if extend_pct >= cfg["extend_over_ema20_pct"]:
        strength_points += 1
    if boll_u_v is not None and px >= boll_u_v:
        strength_points += 1
    if ema50_v and px > ema50_v:
        strength_points += 1

    riskoff_points = 0
    if ema50_v and px < ema50_v:
        riskoff_points += 1
    if dd20_v is not None and dd20_v <= -abs(cfg["drawdown_from_20d_high_pct"]):
        riskoff_points += 1
    if news_score is not None and news_score <= -0.3:
        riskoff_points += 1

    if strength_points >= 2 and (ema50_v and px > ema50_v):
        mode = "strength"
    elif riskoff_points >= 2:
        mode = "riskoff"

    score = float(np.clip(score, 0, 100))

    return TickerAnalysis(
        symbol=symbol,
        last_price=last_price,
        day_close=day_close,
        ema20=ema20_v,
        ema50=ema50_v,
        rsi14=rsi_v,
        atr14=atr_v,
        boll_upper=boll_u_v,
        boll_lower=boll_l_v,
        drawdown_20d=dd20_v,
        news_sentiment_score=news_score,
        news_items=news_items,
        score=score,
        mode=mode,
        reasons=reasons if reasons else ["No strong signals."]
    )


# -----------------------------
# Alert state
# -----------------------------

def load_state(state_file: Path) -> Dict[str, Any]:
    """Load alert state from disk.

    Resilient to missing/corrupt files; returns a sane default if unreadable.
    """
    if not state_file.exists():
        return {"sent": {}}

    try:
        with open(state_file, "r", encoding="utf-8") as f:
            raw = json.load(f)
        if not isinstance(raw, dict):
            raise ValueError("state is not an object")
        sent = raw.get("sent")
        if sent is None or not isinstance(sent, dict):
            raw["sent"] = {}
        return raw
    except Exception as e:
        logging.warning("State file %s unreadable (%s); starting fresh.", state_file, e)
        # Preserve the bad file for debugging, but don't crash the bot.
        try:
            ts = int(time.time())
            bad = state_file.with_suffix(state_file.suffix + f".corrupt.{ts}")
            state_file.replace(bad)
        except Exception:
            pass
        return {"sent": {}}

def save_state(state_file: Path, state: Dict[str, Any]) -> None:
    """Atomic state write to avoid partial/corrupt JSON on crash."""
    ensure_dir(state_file.parent)
    tmp = state_file.with_suffix(state_file.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)
    os.replace(tmp, state_file)

def should_alert(state: Dict[str, Any], symbol: str, ticker_hash: str, cooldown_min: int) -> bool:
    """
    Check if we should send an alert for this ticker.

    FIXED: Now uses per-ticker hash instead of global payload hash.
    Each ticker's cooldown is independent.

    Args:
        state: Alert state dict
        symbol: Ticker symbol
        ticker_hash: Hash of this ticker's specific analysis (not all candidates)
        cooldown_min: Minutes before re-alerting same ticker
    """
    sent = state.get("sent", {}).get(symbol)
    if not sent:
        return True

    last_ts = sent.get("ts_utc")
    last_hash = sent.get("hash")

    try:
        last_dt = dt.datetime.fromisoformat(last_ts)
    except Exception:
        last_dt = None

    # Same analysis = don't re-alert
    if last_hash == ticker_hash:
        return False

    # No valid timestamp = alert
    if last_dt is None:
        return True

    # Within cooldown period = don't alert
    if now_utc() - last_dt < dt.timedelta(minutes=cooldown_min):
        return False

    return True


def compute_ticker_hash(candidate: SellCandidate) -> str:
    """
    Compute a hash for a single ticker's analysis.

    FIXED: Per-ticker hash instead of global list hash.
    This ensures that changes to one ticker don't reset another's cooldown.
    """
    # Hash the key fields that would indicate a meaningful change
    data = {
        "symbol": candidate.symbol,
        "score": round(candidate.score, 1),
        "mode": candidate.mode,
        "suggested_shares": round(candidate.suggested_shares, 4),
        "reasons": candidate.reasons[:3],  # Top 3 reasons
    }
    return sha1_text(json.dumps(data, sort_keys=True))


def mark_alert(state: Dict[str, Any], symbol: str, ticker_hash: str) -> None:
    """Mark that an alert was sent for this ticker."""
    state.setdefault("sent", {})[symbol] = {"ts_utc": iso(now_utc()), "hash": ticker_hash}


# -----------------------------
# Email
# -----------------------------

def send_email(subject: str, text_body: str, html_body: str) -> None:
    host = os.environ.get("SMTP_HOST")
    port = int(os.environ.get("SMTP_PORT", "587"))
    user = os.environ.get("SMTP_USER")
    pwd = os.environ.get("SMTP_PASS")
    from_addr = os.environ.get("EMAIL_FROM")
    to_addr = os.environ.get("EMAIL_TO")

    missing = [k for k, v in {
        "SMTP_HOST": host, "SMTP_USER": user, "SMTP_PASS": pwd,
        "EMAIL_FROM": from_addr, "EMAIL_TO": to_addr
    }.items() if not v]
    if missing:
        raise RuntimeError(f"Missing email env vars: {missing}")

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = from_addr
    msg["To"] = to_addr

    # multipart/alternative: text + html
    msg.set_content(text_body)
    msg.add_alternative(html_body, subtype="html")

    import smtplib
    with smtplib.SMTP(host, port) as s:
        s.starttls()
        s.login(user, pwd)
        s.send_message(msg)


# -----------------------------
# Candidate generation
# -----------------------------

def propose_sell_candidates(
    positions_df: pd.DataFrame,
    analyses: Dict[str, TickerAnalysis],
    cash_needed: float,
    cfg: Dict[str, Any]
) -> List[SellCandidate]:
    candidates: List[SellCandidate] = []
    if positions_df.empty:
        return candidates

    limit_discount_bps = float(cfg["limit_discount_bps"]) / 10000.0
    min_strength = cfg.get("min_score_to_alert_strength", cfg.get("min_score_to_alert", 70))
    min_riskoff = cfg.get("min_score_to_alert_riskoff", cfg.get("min_score_to_alert", 70))

    for _, row in positions_df.iterrows():
        sym = str(row["symbol"]).strip().upper()
        qty = float(row["quantity"])
        avg_cost = safe_float(row.get("avg_cost"))
        analysis = analyses.get(sym)
        if not analysis:
            continue

        px = analysis.last_price if analysis.last_price is not None else analysis.day_close
        if px is None or px <= 0:
            continue

        # Threshold by mode
        # FIXED: mode="none" (neutral) should NOT be treated as risk-off
        # Only "strength" and "riskoff" modes should trigger alerts
        if analysis.mode == "strength":
            min_required = min_strength
        elif analysis.mode == "riskoff":
            min_required = min_riskoff
        else:
            # mode="none" - no clear signal, skip this ticker
            continue

        if analysis.score < min_required:
            continue

        unreal_gain_pct = None
        if avg_cost is not None and avg_cost > 0:
            unreal_gain_pct = (px / avg_cost) - 1.0

        if analysis.mode == "strength":
            min_gain = float(cfg.get("min_unrealized_gain_pct_for_strength_sell", 0.05))
            if unreal_gain_pct is not None and unreal_gain_pct < min_gain:
                continue

        tranche = cfg["tranche_plan"]["strength"] if analysis.mode == "strength" else cfg["tranche_plan"]["riskoff"]
        tranche_frac = float(tranche[0])
        suggested_shares = (math.floor(qty * tranche_frac) if qty >= 1 else qty * tranche_frac)
        suggested_shares = max(1.0, suggested_shares) if qty >= 1 else suggested_shares
        suggested_shares = min(suggested_shares, qty)

        # IMPROVED: Use bid price for more realistic proceeds estimate
        # Selling at market typically fills at or near bid, not last price
        # Apply a small discount to account for slippage on larger orders
        bid_discount = 0.001  # 0.1% slippage estimate
        realistic_fill = px * (1.0 - bid_discount)
        est_proceeds = suggested_shares * realistic_fill

        # Suggested limit is slightly below last price for quick fills
        suggested_limit = px * (1.0 - limit_discount_bps)

        # === TAX AWARENESS ===
        earliest_date = row.get("earliest_purchase_date")
        is_lt = None
        days_to_lt = None

        if earliest_date is not None and pd.notna(earliest_date):
            today = dt.date.today()
            days_held = (today - earliest_date).days
            is_lt = days_held > 365
            days_to_lt = max(0, 366 - days_held)

            # Add tax warning to reasons if near long-term threshold
            if days_to_lt > 0 and days_to_lt <= 30:
                analysis.reasons.insert(0, f"⚠️ TAX: {days_to_lt} days until LTCG eligible!")

        candidates.append(SellCandidate(
            symbol=sym,
            score=analysis.score,
            mode=analysis.mode,
            suggested_shares=float(suggested_shares),
            est_proceeds=float(est_proceeds),
            suggested_limit=float(suggested_limit),
            reasons=analysis.reasons,
            is_long_term=is_lt,
            days_until_long_term=days_to_lt
        ))

    candidates.sort(key=lambda c: (c.score, c.est_proceeds), reverse=True)

    if cash_needed > 0:
        picked: List[SellCandidate] = []
        running = 0.0
        for c in candidates:
            picked.append(c)
            running += c.est_proceeds
            if running >= cash_needed:
                break
        return picked[: int(cfg["max_positions_in_email"])]

    return candidates[: int(cfg["max_positions_in_email"])]


# -----------------------------
# Formatting (Text + HTML)
# -----------------------------

def format_hysa_plan_text(plan: HysaPlan, max_lines: int = 6) -> str:
    lines = ["HYSA Monthly Targets (next):"]
    shown = 0
    today = dt.date.today()
    for d, tgt in plan.monthly_targets:
        if d < today:
            continue
        lines.append(f"  • {d.isoformat()}: {money(tgt, 0)}")
        shown += 1
        if shown >= max_lines:
            break
    if shown == 0:
        lines.append("  • (no upcoming targets)")
    return "\n".join(lines)

def build_candidates_table_html(candidates: List[SellCandidate], analyses: Dict[str, TickerAnalysis]) -> str:
    rows = []
    for c in candidates:
        a = analyses[c.symbol]
        px = a.last_price if a.last_price is not None else a.day_close
        dd = a.drawdown_20d
        rsi14 = a.rsi14
        mode_badge = (
            '<span style="display:inline-block;padding:2px 8px;border-radius:999px;background:#E8F5E9;color:#1B5E20;font-size:12px;">Strength</span>'
            if c.mode == "strength" else
            '<span style="display:inline-block;padding:2px 8px;border-radius:999px;background:#FFF3E0;color:#E65100;font-size:12px;">Risk-off</span>'
            if c.mode == "riskoff" else
            '<span style="display:inline-block;padding:2px 8px;border-radius:999px;background:#ECEFF1;color:#37474F;font-size:12px;">Neutral</span>'
        )

        rows.append(f"""
          <tr>
            <td style="padding:8px 6px;border-bottom:1px solid #eee;font-family:Arial,sans-serif;">
              <div style="font-size:13px;font-weight:700;">{esc_html(c.symbol)}</div>
              <div style="margin-top:3px;">{mode_badge}</div>
            </td>
            <td style="padding:8px 6px;border-bottom:1px solid #eee;font-family:Arial,sans-serif;text-align:right;">
              <div style="font-size:13px;font-weight:700;">{c.score:.0f}</div>
            </td>
            <td style="padding:8px 6px;border-bottom:1px solid #eee;font-family:Arial,sans-serif;text-align:right;">
              <div style="font-size:13px;font-weight:700;">{money(px, 2)}</div>
            </td>
            <td style="padding:8px 6px;border-bottom:1px solid #eee;font-family:Arial,sans-serif;text-align:right;">
              <div style="font-size:13px;font-weight:700;">{c.suggested_shares:.2f}</div>
            </td>
            <td style="padding:8px 6px;border-bottom:1px solid #eee;font-family:Arial,sans-serif;text-align:right;">
              <div style="font-size:13px;font-weight:700;">{money(c.est_proceeds, 0)}</div>
            </td>
            <td style="padding:8px 6px;border-bottom:1px solid #eee;font-family:Arial,sans-serif;text-align:right;">
              <div style="font-size:13px;font-weight:700;">{('—' if rsi14 is None else f'{rsi14:.1f}')}</div>
            </td>
            <td style="padding:8px 6px;border-bottom:1px solid #eee;font-family:Arial,sans-serif;text-align:right;">
              <div style="font-size:13px;font-weight:700;">{pct(dd, 1)}</div>
            </td>
          </tr>
        """)

    # Wrap table in scrollable container for mobile
    return f"""
    <div style="overflow-x:auto;-webkit-overflow-scrolling:touch;">
      <table cellpadding="0" cellspacing="0" style="min-width:500px;width:100%;border:1px solid #eee;border-radius:12px;overflow:hidden;">
        <thead>
          <tr style="background:#fafafa;">
            <th style="padding:8px 6px;text-align:left;font-family:Arial,sans-serif;font-size:11px;color:#444;white-space:nowrap;">Ticker</th>
            <th style="padding:8px 6px;text-align:right;font-family:Arial,sans-serif;font-size:11px;color:#444;white-space:nowrap;">Score</th>
            <th style="padding:8px 6px;text-align:right;font-family:Arial,sans-serif;font-size:11px;color:#444;white-space:nowrap;">Last</th>
            <th style="padding:8px 6px;text-align:right;font-family:Arial,sans-serif;font-size:11px;color:#444;white-space:nowrap;">Shares</th>
            <th style="padding:8px 6px;text-align:right;font-family:Arial,sans-serif;font-size:11px;color:#444;white-space:nowrap;">Proceeds</th>
            <th style="padding:8px 6px;text-align:right;font-family:Arial,sans-serif;font-size:11px;color:#444;white-space:nowrap;">RSI</th>
            <th style="padding:8px 6px;text-align:right;font-family:Arial,sans-serif;font-size:11px;color:#444;white-space:nowrap;">DD20</th>
          </tr>
        </thead>
        <tbody>
          {''.join(rows)}
        </tbody>
      </table>
    </div>
    """

def format_email(
    plan: HysaPlan,
    deadline: dt.date,
    target: float,
    shortfall: float,
    current_hysa: float,
    candidates: List[SellCandidate],
    analyses: Dict[str, TickerAnalysis],
    cfg: Dict[str, Any] = None,
    ai_assessments: Optional[Dict[str, AISellAssessment]] = None
) -> Tuple[str, str, str]:
    """Format email content as a mobile-friendly quick rundown for sell decisions.

    Features:
    - AI headline and urgency front and center for quick scanning
    - Visual urgency meter with color-coded progress bar
    - Downside risk visualization
    - Opportunity cost (HYSA alternative) display
    - Tax impact awareness
    - Hold factors vs sell factors comparison
    - Mobile-friendly responsive design
    """
    ai_assessments = ai_assessments or {}

    # Build subject with urgency indicator
    risk_level = cfg.get('risk_level', 1) if cfg else 1
    urgent_count = sum(1 for s in candidates if ai_assessments.get(s.symbol) and ai_assessments[s.symbol].urgency == "Sell Now")

    if urgent_count > 0:
        subject = f"[URGENT] {urgent_count} Sell Now | Need {money(shortfall,0)}"
    elif risk_level == 1:
        subject = f"[Sell Alert] {len(candidates)} Opportunit{'ies' if len(candidates) != 1 else 'y'} | Need {money(shortfall,0)}"
    elif risk_level == 2:
        subject = f"[Sell Alert] {len(candidates)} Window{'s' if len(candidates) != 1 else ''} | Need {money(shortfall,0)}"
    else:
        subject = f"[Sell Ideas] {len(candidates)} Potential | Need {money(shortfall,0)}"

    # Helper functions for visual elements
    def urgency_color(urgency: str) -> Tuple[str, str, str]:
        """Return (bg_color, text_color, bar_color) for urgency level."""
        if urgency == "Sell Now":
            return ("#FFEBEE", "#B71C1C", "#C62828")
        elif urgency == "Can Wait":
            return ("#FFF3E0", "#E65100", "#F57C00")
        else:  # Watch Closely
            return ("#E3F2FD", "#0D47A1", "#1976D2")

    def conviction_bar(score: int, color: str) -> str:
        """Generate a visual conviction bar (0-100)."""
        width = max(5, min(100, score))
        return f'''
        <div style="background:#e0e0e0;border-radius:4px;height:8px;width:100%;margin:4px 0;">
            <div style="background:{color};border-radius:4px;height:8px;width:{width}%;"></div>
        </div>
        '''

    def downside_bar(downside_pct: float) -> str:
        """Generate a visual downside risk bar showing current vs potential loss."""
        if downside_pct <= 0:
            return ""
        # Show what you have vs what you could lose
        safe_width = max(0, min(100, 100 - downside_pct))
        risk_width = min(100, downside_pct)
        return f'''
        <div style="margin:8px 0;">
            <div style="font-size:10px;color:#888;margin-bottom:2px;">Downside Risk</div>
            <div style="display:flex;border-radius:4px;overflow:hidden;height:16px;">
                <div style="background:#66bb6a;width:{safe_width}%;display:flex;align-items:center;justify-content:center;">
                    <span style="color:white;font-size:9px;font-weight:bold;">Keep</span>
                </div>
                <div style="background:#ef5350;width:{risk_width}%;display:flex;align-items:center;justify-content:center;">
                    <span style="color:white;font-size:9px;font-weight:bold;">-{downside_pct:.1f}%</span>
                </div>
            </div>
        </div>
        '''

    # Plain text version
    text_lines = [
        "=" * 50,
        "SELL ALERT - QUICK RUNDOWN",
        "=" * 50,
        f"HYSA: {money(current_hysa,0)} / {money(target,0)} (need {money(shortfall,0)})",
        f"Deadline: {deadline.isoformat()}",
        ""
    ]

    for c in candidates:
        a = analyses[c.symbol]
        ai = ai_assessments.get(c.symbol)
        px = a.last_price if a.last_price is not None else a.day_close

        text_lines.append(f">>> {c.symbol} @ {money(px, 2)} <<<")
        if ai:
            text_lines.append(f"AI: {ai.conviction} ({ai.conviction_score}/100) | {ai.urgency}")
            text_lines.append(f"HEADLINE: {ai.headline}")
            text_lines.append(f"ACTION: {ai.position_action}")
            text_lines.append(f"REASONING: {ai.reasoning}")
            if ai.risk_if_hold:
                text_lines.append(f"RISK IF HOLD: {ai.risk_if_hold}")
            if ai.downside_scenario:
                text_lines.append(f"DOWNSIDE: {ai.downside_scenario}")
            if ai.opportunity_cost:
                text_lines.append(f"OPPORTUNITY: {ai.opportunity_cost}")
            if ai.tax_note:
                text_lines.append(f"TAX: {ai.tax_note}")
            if ai.upcoming_events:
                text_lines.append(f"EVENTS: {', '.join(ai.upcoming_events)}")
            if ai.hold_factors:
                text_lines.append(f"HOLD FACTORS: {', '.join(ai.hold_factors)}")
        else:
            text_lines.append(f"Mode: {c.mode.upper()} | Score: {c.score:.0f}")

        text_lines.append(f"PROCEEDS: {money(c.est_proceeds, 0)} ({c.suggested_shares:.2f} shares)")
        text_lines.append("-" * 50)
        text_lines.append("")

    text_lines.append("Review taxes and holding periods before selling.")
    text_body = "\n".join(text_lines)

    # HTML - Mobile-friendly quick rundown format
    cards_html = []
    for c in candidates:
        a = analyses[c.symbol]
        ai = ai_assessments.get(c.symbol)
        px = a.last_price if a.last_price is not None else a.day_close

        # Mode badge
        mode_colors = {
            "strength": ("#E8F5E9", "#1B5E20"),
            "riskoff": ("#FFEBEE", "#B71C1C"),
        }
        mode_bg, mode_color = mode_colors.get(c.mode, ("#ECEFF1", "#455A64"))
        mode_label = "Take Profits" if c.mode == "strength" else "Cut Losses"
        mode_badge = f'<span style="background:{mode_bg};color:{mode_color};padding:3px 10px;border-radius:12px;font-size:11px;font-weight:600;">{mode_label}</span>'

        # Tax badge
        tax_badge = ""
        if c.is_long_term is True:
            tax_badge = '<span style="background:#E8F5E9;color:#2E7D32;padding:3px 8px;border-radius:12px;font-size:10px;font-weight:600;margin-left:4px;">LTCG</span>'
        elif c.is_long_term is False:
            if c.days_until_long_term and c.days_until_long_term > 0 and c.days_until_long_term <= 30:
                tax_badge = f'<span style="background:#FFF3E0;color:#E65100;padding:3px 8px;border-radius:12px;font-size:10px;font-weight:600;margin-left:4px;">{c.days_until_long_term}d to LTCG</span>'
            else:
                tax_badge = '<span style="background:#FFEBEE;color:#C62828;padding:3px 8px;border-radius:12px;font-size:10px;font-weight:600;margin-left:4px;">STCG</span>'

        if ai:
            bg_color, text_color, bar_color = urgency_color(ai.urgency)

            # Urgency section with meter
            urgency_section = f'''
            <div style="background:{bg_color};border-radius:8px;padding:12px;margin-bottom:12px;">
                <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:4px;">
                    <span style="font-weight:bold;color:{text_color};font-size:13px;">{ai.urgency.upper()}</span>
                    <span style="font-weight:bold;color:{text_color};font-size:18px;">{ai.conviction}</span>
                </div>
                {conviction_bar(ai.conviction_score, bar_color)}
                <div style="color:{text_color};font-size:11px;text-align:right;">{ai.conviction_score}/100</div>
                {f'<div style="color:{text_color};font-size:11px;margin-top:4px;">{esc_html(ai.urgency_reason)}</div>' if ai.urgency_reason else ''}
            </div>
            '''

            # AI Headline
            headline_html = f'''
            <div style="font-size:16px;font-weight:600;color:#1a1a1a;line-height:1.4;margin-bottom:12px;padding:10px;background:#f8f9fa;border-left:4px solid {bar_color};border-radius:0 6px 6px 0;">
                {esc_html(ai.headline)}
            </div>
            '''

            # Company context
            context_html = ""
            if ai.company_context:
                context_html = f'''
                <div style="font-size:12px;color:#666;margin-bottom:12px;padding:8px;background:#fafafa;border-radius:4px;">
                    <span style="font-weight:bold;color:#888;">ABOUT:</span> {esc_html(ai.company_context)}
                </div>
                '''

            # Risk if hold vs Opportunity cost comparison
            comparison_html = f'''
            <div style="margin-bottom:12px;">
                <div style="display:flex;flex-wrap:wrap;gap:8px;">
                    <div style="flex:1;min-width:140px;background:#FFEBEE;border-radius:6px;padding:10px;">
                        <div style="font-weight:bold;color:#C62828;font-size:11px;margin-bottom:4px;">RISK IF HOLD</div>
                        <div style="font-size:12px;color:#B71C1C;line-height:1.4;">{esc_html(ai.risk_if_hold) if ai.risk_if_hold else "—"}</div>
                    </div>
                    <div style="flex:1;min-width:140px;background:#E8F5E9;border-radius:6px;padding:10px;">
                        <div style="font-weight:bold;color:#2E7D32;font-size:11px;margin-bottom:4px;">OPPORTUNITY</div>
                        <div style="font-size:12px;color:#1B5E20;line-height:1.4;">{esc_html(ai.opportunity_cost) if ai.opportunity_cost else "Capital could earn in HYSA"}</div>
                    </div>
                </div>
            </div>
            '''

            # Downside scenario with visual bar
            downside_html = ""
            if ai.downside_scenario or ai.downside_pct > 0:
                downside_html = f'''
                <div style="margin-bottom:12px;padding:10px;background:#FFF8E1;border-radius:6px;">
                    <div style="font-weight:bold;color:#F57F17;font-size:11px;margin-bottom:4px;">DOWNSIDE SCENARIO</div>
                    <div style="font-size:12px;color:#E65100;line-height:1.4;">{esc_html(ai.downside_scenario) if ai.downside_scenario else f"Potential {ai.downside_pct:.1f}% loss"}</div>
                    {downside_bar(ai.downside_pct)}
                </div>
                '''

            # Upcoming events (if any)
            events_html = ""
            if ai.upcoming_events:
                event_badges = "".join([f'<span style="background:#E3F2FD;color:#1565C0;padding:3px 8px;border-radius:12px;font-size:11px;margin:2px;display:inline-block;">{esc_html(evt)}</span>' for evt in ai.upcoming_events[:3]])
                events_html = f'''
                <div style="margin-bottom:10px;">
                    <div style="font-weight:bold;color:#1565C0;font-size:11px;margin-bottom:4px;">UPCOMING EVENTS</div>
                    {event_badges}
                </div>
                '''

            # Hold factors (reasons to potentially wait)
            hold_factors_html = ""
            if ai.hold_factors:
                hold_badges = "".join([f'<span style="background:#FFF3E0;color:#E65100;padding:3px 8px;border-radius:12px;font-size:11px;margin:2px;display:inline-block;">{esc_html(f)}</span>' for f in ai.hold_factors[:3]])
                hold_factors_html = f'''
                <div style="margin-bottom:10px;">
                    <div style="font-weight:bold;color:#E65100;font-size:11px;margin-bottom:4px;">REASONS TO WAIT</div>
                    {hold_badges}
                </div>
                '''

            # Tax impact
            tax_html = ""
            if ai.tax_note or ai.tax_impact:
                tax_content = ai.tax_note or ai.tax_impact
                tax_html = f'''
                <div style="margin-bottom:10px;padding:8px;background:#FFF8E1;border-radius:6px;border-left:3px solid #FFA000;">
                    <div style="font-size:11px;color:#F57F17;"><b>TAX:</b> {esc_html(tax_content)}</div>
                </div>
                '''

            # Action badges
            action_bg = "#C62828" if ai.position_action == "Full exit" else "#F57C00" if "50%" in ai.position_action else "#1976D2"
            action_html = f'''
            <div style="display:flex;gap:8px;margin-bottom:12px;flex-wrap:wrap;">
                <div style="background:{action_bg};color:white;padding:6px 12px;border-radius:16px;font-size:12px;font-weight:bold;">{esc_html(ai.position_action)}</div>
                {f'<div style="background:#455A64;color:white;padding:6px 12px;border-radius:16px;font-size:12px;font-weight:bold;">Limit: {money(ai.suggested_exit_price, 2)}</div>' if ai.suggested_exit_price else ''}
            </div>
            '''
        else:
            # Fallback for no AI assessment
            urgency_section = ""
            headline_html = ""
            context_html = ""
            comparison_html = ""
            downside_html = ""
            events_html = ""
            hold_factors_html = ""
            tax_html = ""
            action_html = ""

        # Proceeds section with visual
        proceeds_section = f'''
        <div style="background:#f8f9fa;border-radius:8px;padding:12px;margin-bottom:12px;">
            <div style="display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:8px;">
                <div>
                    <div style="font-size:11px;color:#888;">PROCEEDS</div>
                    <div style="font-size:24px;font-weight:bold;color:#2E7D32;">{money(c.est_proceeds, 0)}</div>
                </div>
                <div style="text-align:right;">
                    <div style="font-size:11px;color:#888;">SHARES</div>
                    <div style="font-size:18px;font-weight:bold;color:#1a1a1a;">{c.suggested_shares:.2f}</div>
                </div>
            </div>
            <div style="font-size:11px;color:#666;margin-top:8px;">
                Price: {money(px, 2)} | Limit: {money(c.suggested_limit, 2) if c.suggested_limit else "Market"}
            </div>
        </div>
        '''

        # Key metrics row
        metrics_parts = []
        if a.rsi14:
            rsi_color = "#C62828" if a.rsi14 > 70 else "#2E7D32" if a.rsi14 < 30 else "#666"
            metrics_parts.append(f'<span style="color:{rsi_color};">RSI {a.rsi14:.0f}</span>')
        if a.drawdown_20d:
            dd_color = "#C62828" if a.drawdown_20d < -0.1 else "#666"
            metrics_parts.append(f'<span style="color:{dd_color};">DD {pct(a.drawdown_20d, 1)}</span>')
        if a.news_sentiment_score:
            sent_color = "#2E7D32" if a.news_sentiment_score > 0.1 else "#C62828" if a.news_sentiment_score < -0.1 else "#666"
            metrics_parts.append(f'<span style="color:{sent_color};">Sentiment {a.news_sentiment_score:.2f}</span>')
        metrics_html = f'''
        <div style="font-size:11px;color:#888;margin-bottom:12px;display:flex;gap:12px;flex-wrap:wrap;">
            {" ".join(metrics_parts)}
        </div>
        ''' if metrics_parts else ""

        # Collapsible technical reasons
        reasons_html = ""
        if c.reasons and not ai:
            reasons_items = [f'<li style="margin:2px 0;">{esc_html(r)}</li>' for r in c.reasons[:4]]
            reasons_html = f'''
            <details style="margin-top:8px;">
                <summary style="font-size:11px;color:#888;cursor:pointer;">Technical Details</summary>
                <ul style="margin:8px 0 0 16px;padding:0;font-size:11px;color:#666;line-height:1.5;">
                    {"".join(reasons_items)}
                </ul>
            </details>
            '''

        # Assemble the card
        cards_html.append(f'''
        <div style="border:1px solid #e0e0e0;border-radius:12px;padding:16px;margin-bottom:16px;background:#fff;box-shadow:0 2px 4px rgba(0,0,0,0.05);">
            <!-- Header -->
            <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:12px;flex-wrap:wrap;gap:8px;">
                <div style="font-size:22px;font-weight:bold;color:#1a1a1a;">{esc_html(c.symbol)}</div>
                <div>{mode_badge}{tax_badge}</div>
            </div>

            <!-- Urgency meter -->
            {urgency_section}

            <!-- AI Headline -->
            {headline_html}

            <!-- Company context -->
            {context_html}

            <!-- Risk vs Opportunity -->
            {comparison_html}

            <!-- Downside scenario -->
            {downside_html}

            <!-- Upcoming events -->
            {events_html}

            <!-- Hold factors -->
            {hold_factors_html}

            <!-- Tax note -->
            {tax_html}

            <!-- Action badges -->
            {action_html}

            <!-- Proceeds section -->
            {proceeds_section}

            <!-- Key metrics -->
            {metrics_html}

            <!-- Technical reasons (collapsible) -->
            {reasons_html}
        </div>
        ''')

    # HYSA funding progress header
    progress_pct = min(100, (current_hysa / target * 100)) if target > 0 else 0

    html_body = f'''
    <!DOCTYPE html>
    <html>
    <head>
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
    </head>
    <body style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;margin:0;padding:0;background:#f5f5f5;">
        <div style="max-width:500px;margin:0 auto;padding:16px;">
            <!-- Header -->
            <div style="background:linear-gradient(135deg,#1a237e,#283593);color:white;border-radius:12px;padding:16px;margin-bottom:16px;">
                <div style="font-size:12px;opacity:0.8;letter-spacing:0.05em;">HYSA FUNDING ALERT</div>
                <div style="font-size:22px;font-weight:bold;margin-top:4px;">Need {money(shortfall, 0)}</div>
                <div style="font-size:12px;opacity:0.8;margin-top:4px;">by {deadline.isoformat()}</div>

                <!-- Progress bar -->
                <div style="margin-top:12px;">
                    <div style="display:flex;justify-content:space-between;font-size:11px;opacity:0.9;margin-bottom:4px;">
                        <span>{money(current_hysa, 0)}</span>
                        <span>{money(target, 0)}</span>
                    </div>
                    <div style="background:rgba(255,255,255,0.2);border-radius:4px;height:10px;">
                        <div style="background:linear-gradient(90deg,#4CAF50,#8BC34A);border-radius:4px;height:10px;width:{progress_pct}%;"></div>
                    </div>
                    <div style="font-size:11px;opacity:0.8;margin-top:4px;text-align:center;">{progress_pct:.0f}% funded</div>
                </div>

                <!-- Stats row -->
                <div style="display:flex;gap:8px;margin-top:12px;flex-wrap:wrap;">
                    <div style="flex:1;min-width:80px;background:rgba(255,255,255,0.1);border-radius:8px;padding:8px;text-align:center;">
                        <div style="font-size:11px;opacity:0.8;">Candidates</div>
                        <div style="font-size:18px;font-weight:bold;">{len(candidates)}</div>
                    </div>
                    <div style="flex:1;min-width:80px;background:rgba(255,255,255,0.1);border-radius:8px;padding:8px;text-align:center;">
                        <div style="font-size:11px;opacity:0.8;">Total Proceeds</div>
                        <div style="font-size:18px;font-weight:bold;">{money(sum(c.est_proceeds for c in candidates), 0)}</div>
                    </div>
                </div>
            </div>

            <!-- Cards -->
            {''.join(cards_html)}

            <!-- Footer -->
            <p style="color:#999;font-size:11px;text-align:center;margin-top:16px;padding:16px;background:#fff;border-radius:8px;">
                AI-powered analysis. Review taxes and holding periods before selling. Not financial advice.
            </p>
        </div>
    </body>
    </html>
    '''

    return subject, text_body, html_body


# -----------------------------
# Main
# -----------------------------

def setup_logging(log_file: Path) -> None:
    ensure_dir(log_file.parent)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),
            logging.StreamHandler()
        ],
    )

def load_config(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True, help="Path to config.yaml")
    parser.add_argument("--once", action="store_true", help="Run one analysis cycle and exit.")
    args = parser.parse_args()

    load_dotenv()
    cfg = load_config(args.config)

    paths = cfg["paths"]
    # When ALGO_OUTPUT_DIR is set, redirect state/cache/logs to local (non-synced) dir
    _output_dir = Path(os.getenv("ALGO_OUTPUT_DIR", "")) if os.getenv("ALGO_OUTPUT_DIR") else None
    state_file = (_output_dir / paths["state_file"]) if _output_dir else Path(paths["state_file"])
    cache_dir = (_output_dir / paths["cache_dir"]) if _output_dir else Path(paths["cache_dir"])
    log_file = (_output_dir / paths["log_file"]) if _output_dir else Path(paths["log_file"])
    ensure_dir(cache_dir)
    setup_logging(log_file)

    api_key = os.environ.get("POLYGON_API_KEY")
    if not api_key:
        raise RuntimeError("Missing POLYGON_API_KEY in environment (.env).")

    polygon_cfg = cfg.get("polygon", {})
    client = PolygonClient(api_key=api_key, base_url=polygon_cfg.get("base_url", "https://api.polygon.io"), cache_dir=cache_dir)

    plan_cfg = cfg["hysa"]
    analysis_cfg = cfg["analysis"]
    quit_date = parse_date(plan_cfg["quit_date"])
    goal = float(plan_cfg["goal_usd"])

    state = load_state(state_file)

    def run_cycle() -> None:
        raw_positions = load_all_positions(cfg)
        pos_df = aggregate_positions(raw_positions)
        if pos_df.empty:
            logging.warning("No positions loaded. Check broker CSV paths and column mappings.")
            return

        logging.info("Loaded %d aggregated symbols: %s", len(pos_df), ", ".join(pos_df["symbol"].tolist()))

        today = dt.date.today()
        current_hysa = float(plan_cfg.get("current_hysa_usd", 0.0))
        plan = build_hysa_plan(goal=goal, quit_date=quit_date, current_hysa=current_hysa, today=today)
        deadline, target, shortfall = next_target(plan, current_hysa=current_hysa, today=today)

        logging.info("HYSA current=%.2f target=%.2f by %s shortfall=%.2f",
                     current_hysa, target, deadline.isoformat(), shortfall)

        analyses: Dict[str, TickerAnalysis] = {}
        for sym in pos_df["symbol"].tolist():
            try:
                analyses[sym] = analyze_ticker(
                    client=client,
                    symbol=sym,
                    lookback_days=int(analysis_cfg["daily_lookback_days"]),
                    cfg=analysis_cfg
                )
                logging.info("Analyzed %s score=%.0f mode=%s", sym, analyses[sym].score, analyses[sym].mode)
            except Exception as e:
                # SECURITY: Redact potential API keys from exception messages
                error_msg = redact_api_keys(str(e))
                logging.error("Analysis failed for %s: %s", sym, error_msg)

        if shortfall <= 0:
            logging.info("No HYSA shortfall for next milestone. No email sent.")
            return

        candidates = propose_sell_candidates(
            positions_df=pos_df,
            analyses=analyses,
            cash_needed=shortfall,
            cfg=analysis_cfg
        )
        if not candidates:
            logging.info("No candidates met alert thresholds.")
            return

        # FIXED: Use per-ticker hash instead of global payload hash
        cooldown_min = int(analysis_cfg["ticker_alert_cooldown_minutes"])
        eligible = []
        ticker_hashes = {}  # Store hashes for marking later

        for c in candidates:
            ticker_hash = compute_ticker_hash(c)
            ticker_hashes[c.symbol] = ticker_hash
            if should_alert(state, c.symbol, ticker_hash, cooldown_min):
                eligible.append(c)

        if not eligible:
            logging.info("Candidates exist but all are within cooldown or identical payload.")
            return

        # Run AI analysis on eligible candidates
        ai_assessments = None
        if os.environ.get("ANTHROPIC_API_KEY"):
            try:
                ai_analyzer = AISellAnalyzer()
                ai_assessments = ai_analyzer.analyze_candidates(eligible, analyses, hysa_shortfall=shortfall)
                logging.info("AI analysis complete for %d candidate(s).", len(ai_assessments))
            except Exception as e:
                logging.warning("AI analysis failed (continuing without): %s", redact_api_keys(str(e)))

        subject, text_body, html_body = format_email(
            plan=plan,
            deadline=deadline,
            target=target,
            shortfall=shortfall,
            current_hysa=current_hysa,
            candidates=eligible,
            analyses=analyses,
            cfg=cfg,
            ai_assessments=ai_assessments
        )

        subj_prefix = cfg.get("email", {}).get("subject_prefix", "[HYSA Phase 1 Sell Alert]")
        subject = f"{subj_prefix} {subject}"

        send_email(subject, text_body, html_body)
        logging.info("Email sent with %d candidate(s).", len(eligible))

        for c in eligible:
            mark_alert(state, c.symbol, ticker_hashes[c.symbol])
        save_state(state_file, state)

    if args.once:
        run_cycle()
        return

    while True:
        try:
            run_cycle()
        except Exception as e:
            # SECURITY: Redact potential API keys from exception messages
            error_msg = redact_api_keys(str(e))
            logging.error("Cycle error: %s", error_msg)
            # Log traceback separately with redaction
            import traceback
            tb = redact_api_keys(traceback.format_exc())
            logging.debug("Traceback:\n%s", tb)

        interval = int(analysis_cfg["interval_seconds"]) if is_market_open(now_utc()) else int(analysis_cfg["off_hours_interval_seconds"])
        time.sleep(max(interval, 30))

if __name__ == "__main__":
    main()
