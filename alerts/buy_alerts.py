#!/usr/bin/env python3
"""
buy_alerts.py - Portfolio Buy Alert Engine
==========================================

Monitors watchlist and market conditions to send email alerts when buy conditions
are met, helping identify entry opportunities for portfolio building.

Features:
- Technical analysis: RSI oversold, Bollinger bands, EMA support tests
- Trend context: EMA alignment, 200 EMA slope, falling knife detection
- Volume confirmation: elevated volume on pullback = accumulation signal
- News sentiment: Polygon API with relevance scoring
- Hash-based deduplication: only re-alert when signal conditions change
- Proper DST handling via zoneinfo + rule-based holiday awareness
- HTML + plain-text multipart email with detailed analysis
- Idea mode: signal monitoring without position sizing (cash=0)
- API key redaction in logs for security

Important:
- Does NOT place orders; alerts only.
- Technical signals are heuristic-based, not guaranteed entries.
- Holiday calendar is rule-based; rare one-off closures (e.g., days of mourning)
  are not covered without an external market calendar source.
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
import sys
import time
from dataclasses import dataclass
from email.message import EmailMessage
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import requests
import yaml
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# --- Timezone Constants ---
ET = ZoneInfo("America/New_York")  # Handles DST automatically

# Load config from .env file if present
_env_path = Path(__file__).parent / "buy_alerts.env"
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
    """Redact potential API keys from text to prevent accidental logging."""
    text = re.sub(r'apiKey=[A-Za-z0-9_\-]+', 'apiKey=REDACTED', text)
    text = re.sub(r'Bearer\s+[A-Za-z0-9_\-\.]+', 'Bearer REDACTED', text)
    text = re.sub(r'\b[A-Za-z0-9]{20,}\b', '[REDACTED_KEY]', text)
    return text

def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)

def in_us_market_hours_et(now: dt.datetime) -> bool:
    """Check if current time is within US market regular trading hours."""
    t = now.astimezone(ET)
    if t.weekday() >= 5:
        return False
    start = t.replace(hour=9, minute=30, second=0, microsecond=0)
    end = t.replace(hour=16, minute=0, second=0, microsecond=0)
    return start <= t <= end

# --- US Market Holiday Calculation (rule-based, no hard-coded dates) ---
def _nth_weekday_of_month(year: int, month: int, weekday: int, n: int) -> dt.date:
    """Get the nth occurrence of a weekday in a month (1-indexed)."""
    first = dt.date(year, month, 1)
    first_weekday = first.weekday()
    days_until = (weekday - first_weekday) % 7
    return first + dt.timedelta(days=days_until + (n - 1) * 7)

def _last_weekday_of_month(year: int, month: int, weekday: int) -> dt.date:
    """Get the last occurrence of a weekday in a month."""
    if month == 12:
        next_month = dt.date(year + 1, 1, 1)
    else:
        next_month = dt.date(year, month + 1, 1)
    last_day = next_month - dt.timedelta(days=1)
    days_back = (last_day.weekday() - weekday) % 7
    return last_day - dt.timedelta(days=days_back)

def _observed_holiday(date: dt.date) -> dt.date:
    """If holiday falls on weekend, return observed date (Fri/Mon)."""
    if date.weekday() == 5:  # Saturday -> Friday
        return date - dt.timedelta(days=1)
    elif date.weekday() == 6:  # Sunday -> Monday
        return date + dt.timedelta(days=1)
    return date

def _compute_easter(year: int) -> dt.date:
    """Compute Easter Sunday using the Anonymous Gregorian algorithm."""
    a = year % 19
    b, c = divmod(year, 100)
    d, e = divmod(b, 4)
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i, k = divmod(c, 4)
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    month = (h + l - 7 * m + 114) // 31
    day = ((h + l - 7 * m + 114) % 31) + 1
    return dt.date(year, month, day)

def get_market_holidays(year: int) -> set:
    """Compute NYSE market holidays for a given year."""
    holidays = set()

    # New Year's Day (Jan 1)
    holidays.add(_observed_holiday(dt.date(year, 1, 1)))

    # MLK Day (3rd Monday of January)
    holidays.add(_nth_weekday_of_month(year, 1, 0, 3))

    # Presidents Day (3rd Monday of February)
    holidays.add(_nth_weekday_of_month(year, 2, 0, 3))

    # Good Friday (Friday before Easter)
    easter = _compute_easter(year)
    holidays.add(easter - dt.timedelta(days=2))

    # Memorial Day (last Monday of May)
    holidays.add(_last_weekday_of_month(year, 5, 0))

    # Juneteenth (June 19)
    holidays.add(_observed_holiday(dt.date(year, 6, 19)))

    # Independence Day (July 4)
    holidays.add(_observed_holiday(dt.date(year, 7, 4)))

    # Labor Day (1st Monday of September)
    holidays.add(_nth_weekday_of_month(year, 9, 0, 1))

    # Thanksgiving (4th Thursday of November)
    holidays.add(_nth_weekday_of_month(year, 11, 3, 4))

    # Christmas (Dec 25)
    holidays.add(_observed_holiday(dt.date(year, 12, 25)))

    return holidays

# Cache holidays for current and next year
_HOLIDAY_CACHE: Dict[int, set] = {}

def is_market_holiday(date: dt.date) -> bool:
    """Check if date is a market holiday (cached, auto-extends).

    FIX: Also checks adjacent year to handle observed holidays that cross
    year boundaries (e.g., Jan 1 on Saturday -> observed Dec 31).
    """
    year = date.year
    # Cache current year
    if year not in _HOLIDAY_CACHE:
        _HOLIDAY_CACHE[year] = get_market_holidays(year)
    # Check current year
    if date in _HOLIDAY_CACHE[year]:
        return True
    # FIX: Check adjacent years for observed holidays crossing year boundary
    # (e.g., Jan 1 on Saturday -> observed Dec 31 of prior year)
    if date.month == 12 and date.day == 31:
        next_year = year + 1
        if next_year not in _HOLIDAY_CACHE:
            _HOLIDAY_CACHE[next_year] = get_market_holidays(next_year)
        if date in _HOLIDAY_CACHE[next_year]:
            return True
    elif date.month == 1 and date.day in (1, 2):
        prev_year = year - 1
        if prev_year not in _HOLIDAY_CACHE:
            _HOLIDAY_CACHE[prev_year] = get_market_holidays(prev_year)
        if date in _HOLIDAY_CACHE[prev_year]:
            return True
    return False

def is_market_open(now: dt.datetime) -> bool:
    t = now.astimezone(ET)
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
class WatchlistItem:
    """A ticker on the watchlist to monitor for buy signals."""
    symbol: str
    target_price: Optional[float] = None  # Buy at or below this price
    max_position_pct: float = 0.05  # Max % of portfolio
    notes: str = ""

@dataclass
class TickerAnalysis:
    """Analysis results for a ticker."""
    symbol: str
    last_price: Optional[float]
    day_close: Optional[float]
    ema20: Optional[float]
    ema50: Optional[float]
    ema200: Optional[float]
    rsi14: Optional[float]
    atr14: Optional[float]
    boll_upper: Optional[float]
    boll_lower: Optional[float]
    distance_from_52w_low_pct: Optional[float]
    distance_from_52w_high_pct: Optional[float]
    news_sentiment_score: Optional[float]
    news_items: List[Dict[str, Any]]
    score: float
    signal_type: str  # "oversold" | "support_test" | "value" | "none"
    reasons: List[str]
    # Company info
    company_name: Optional[str] = None  # Full company name from Polygon
    # Trend context fields
    # trend_aligned: None = insufficient history, True = above 200 EMA, False = below 200 EMA
    trend_aligned: Optional[bool] = None
    ema_slope_200: Optional[float] = None  # 200 EMA slope (positive = uptrend)
    ema_bullish_alignment: bool = False  # True if EMA20 > EMA50 > EMA200
    # Volume confirmation
    # Note: During market hours, this is YESTERDAY's volume (latest completed bar)
    volume_ratio: Optional[float] = None  # Latest completed day's volume / 20-day avg
    volume_confirmed: bool = False  # True if volume elevated on pullback
    # Falling knife flag (hard guardrail)
    falling_knife: bool = False  # True if below declining 200 EMA - score capped
    # Recent swing levels for TP calculation
    recent_high_20d: Optional[float] = None  # Highest high in last 20 days
    recent_low_20d: Optional[float] = None  # Lowest low in last 20 days

@dataclass
class BuyCandidate:
    """A ticker that meets buy criteria."""
    symbol: str
    score: float
    signal_type: str
    current_price: float
    target_price: Optional[float]
    suggested_shares: Optional[int]  # None for idea-only mode (cash=0)
    est_cost: Optional[float]  # None for idea-only mode
    suggested_limit: Optional[float]
    reasons: List[str]
    # Trend context for display
    # None = unknown (insufficient 200D history), True = above 200 EMA, False = below
    trend_aligned: Optional[bool] = None
    counter_trend_warning: bool = False  # True if falling knife (below declining 200 EMA)
    # Take profit targets
    take_profit_1: Optional[float] = None  # Conservative TP (1 ATR or EMA20)
    take_profit_2: Optional[float] = None  # Moderate TP (Fib 0.618 or EMA50)
    take_profit_3: Optional[float] = None  # Aggressive TP (Fib 1.0 or recent high)
    stop_loss: Optional[float] = None  # Suggested stop loss


@dataclass
class AIBuyAssessment:
    """AI-generated comprehensive assessment of a buy candidate."""
    conviction: str  # HIGH, MEDIUM, LOW
    conviction_score: int  # 1-100 numeric score
    headline: str  # One-line summary for quick scan
    reasoning: str  # 2-3 sentence summary
    bull_case: str  # Why this could work
    bear_case: str  # What could go wrong
    risk_factors: List[str]  # Key risks as bullet points
    catalysts: List[str]  # Upcoming catalysts/events
    position_sizing: str  # "Full size", "Half size", "Small starter"
    entry_strategy: str  # "Enter now", "Wait for confirmation", "Scale in"
    # Research-based additions
    company_context: str  # Brief company description and sector
    recent_developments: str  # Key recent news/events summary
    analyst_sentiment: str  # General analyst/market sentiment
    earnings_context: str  # Upcoming earnings or recent results
    sector_trend: str  # How sector is performing


@dataclass
class MarketContext:
    """Current market conditions for context."""
    spy_price: float
    spy_change_pct: float
    spy_trend: str  # "bullish", "bearish", "neutral"
    vix_level: float
    vix_context: str  # "low", "elevated", "high", "extreme"
    market_regime: str  # "risk-on", "risk-off", "mixed"
    sector_leaders: List[str]
    sector_laggards: List[str]


# -----------------------------
# AI Buy Analyzer (Enhanced with Web Research)
# -----------------------------

class AIBuyAnalyzer:
    """
    Enhanced AI analyzer using Claude with web search for comprehensive research.

    Multi-step analysis:
    1. Gather market context (SPY, VIX, sectors)
    2. Research each candidate (company info, recent news, catalysts)
    3. Synthesize everything into actionable recommendations
    """

    def __init__(self):
        self.anthropic_key = os.environ.get("ANTHROPIC_API_KEY", "")
        self.polygon_key = os.environ.get("POLYGON_API_KEY", "")

    def analyze_candidates(
        self,
        candidates: List[BuyCandidate],
        analyses: Dict[str, TickerAnalysis]
    ) -> Dict[str, AIBuyAssessment]:
        """Analyze all buy candidates with comprehensive AI research."""
        if not self.anthropic_key:
            logging.info("No ANTHROPIC_API_KEY - skipping AI analysis")
            return {}

        if not candidates:
            return {}

        try:
            # Step 1: Get market context
            market_ctx = self._get_market_context()
            logging.info(f"Market context: {market_ctx.market_regime}, VIX={market_ctx.vix_level:.1f}")

            # Step 2: Research and analyze each candidate
            assessments = {}
            for candidate in candidates[:5]:  # Limit to top 5 for API efficiency
                analysis = analyses.get(candidate.symbol)
                if not analysis:
                    continue

                try:
                    assessment = self._analyze_single_candidate(
                        candidate, analysis, market_ctx
                    )
                    if assessment:
                        assessments[candidate.symbol] = assessment
                except Exception as e:
                    logging.warning(f"Failed to analyze {candidate.symbol}: {e}")
                    continue

            return assessments

        except Exception as e:
            logging.error(f"AI analysis failed: {e}")
            return {}

    def _get_market_context(self) -> MarketContext:
        """Fetch current market conditions for context."""
        try:
            # Get SPY snapshot from Polygon
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
                market_regime=market_regime,
                sector_leaders=[],
                sector_laggards=[]
            )
        except Exception as e:
            logging.warning(f"Failed to get market context: {e}")
            return MarketContext(
                spy_price=0, spy_change_pct=0, spy_trend="unknown",
                vix_level=0, vix_context="unknown", market_regime="unknown",
                sector_leaders=[], sector_laggards=[]
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
        candidate: BuyCandidate,
        analysis: TickerAnalysis,
        market_ctx: MarketContext
    ) -> Optional[AIBuyAssessment]:
        """Perform comprehensive AI analysis on a single candidate with web research."""

        # Build comprehensive prompt with research request
        prompt = self._build_research_prompt(candidate, analysis, market_ctx)

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
            return self._parse_enhanced_response(content, candidate.symbol)

        except requests.exceptions.Timeout:
            logging.error(f"Claude API timeout for {candidate.symbol}")
            return None
        except Exception as e:
            logging.error(f"Claude API error for {candidate.symbol}: {e}")
            return None

    def _build_research_prompt(
        self,
        candidate: BuyCandidate,
        analysis: TickerAnalysis,
        market_ctx: MarketContext
    ) -> str:
        """Build comprehensive research prompt for a single candidate."""

        # Format full news content (not just headlines)
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

        # Calculate risk/reward metrics
        risk_pct = ((candidate.current_price - candidate.stop_loss) / candidate.current_price * 100) if candidate.stop_loss else 0
        reward_pct = ((candidate.take_profit_1 - candidate.current_price) / candidate.current_price * 100) if candidate.take_profit_1 else 0
        rr_ratio = (reward_pct / risk_pct) if risk_pct > 0 else 0

        return f"""You are an expert investment analyst preparing a concise buy recommendation report.

ANALYZE THIS STOCK: {candidate.symbol}

=== TECHNICAL DATA ===
Current Price: ${candidate.current_price:.2f}
Signal Type: {candidate.signal_type.upper()}
Technical Score: {candidate.score:.0f}/100

RSI(14): {analysis.rsi14:.1f if analysis.rsi14 else 'N/A'}
EMA20: ${analysis.ema20:.2f if analysis.ema20 else 'N/A'}
EMA50: ${analysis.ema50:.2f if analysis.ema50 else 'N/A'}
EMA200: ${analysis.ema200:.2f if analysis.ema200 else 'N/A'}
ATR(14): ${analysis.atr14:.2f if analysis.atr14 else 'N/A'}

Trend Aligned (above 200 EMA): {analysis.trend_aligned}
EMA Bullish Stack (20>50>200): {analysis.ema_bullish_alignment}
Falling Knife Warning: {analysis.falling_knife}

Volume Ratio (vs 20-day avg): {analysis.volume_ratio:.1f}x if analysis.volume_ratio else 'N/A'
Volume Confirmed: {analysis.volume_confirmed}

52-Week Position:
- {analysis.distance_from_52w_low_pct:.1f}% above 52W low if analysis.distance_from_52w_low_pct else 'N/A'
- {analysis.distance_from_52w_high_pct:.1f}% below 52W high if analysis.distance_from_52w_high_pct else 'N/A'

=== PRICE TARGETS ===
Entry: ${candidate.current_price:.2f}
Stop Loss: ${candidate.stop_loss:.2f if candidate.stop_loss else 'N/A'} ({risk_pct:.1f}% risk)
Take Profit 1: ${candidate.take_profit_1:.2f if candidate.take_profit_1 else 'N/A'} ({reward_pct:.1f}% reward)
Take Profit 2: ${candidate.take_profit_2:.2f if candidate.take_profit_2 else 'N/A'}
Risk/Reward Ratio: {rr_ratio:.1f}:1

=== SIGNAL REASONS ===
{chr(10).join('- ' + r for r in candidate.reasons[:5])}

=== RECENT NEWS ===
{news_details}

=== MARKET CONTEXT ===
SPY: ${market_ctx.spy_price:.2f} ({market_ctx.spy_change_pct:+.1f}%)
Market Trend: {market_ctx.spy_trend.upper()}
VIX: {market_ctx.vix_level:.1f} ({market_ctx.vix_context})
Market Regime: {market_ctx.market_regime.upper()}

=== YOUR TASK ===
Based on the data above and your knowledge, provide a comprehensive but CONCISE analysis.
Use your knowledge to add context about this company, its sector, and any relevant factors.

Respond in this EXACT format (each field on its own line):

HEADLINE: [One compelling sentence summarizing the opportunity - max 15 words]
CONVICTION: [HIGH/MEDIUM/LOW]
SCORE: [1-100 numeric conviction score]
COMPANY_CONTEXT: [1-2 sentences: What does this company do? What sector? Market cap category?]
REASONING: [2-3 sentences explaining your conviction level]
BULL_CASE: [1-2 sentences: Why this trade could work well]
BEAR_CASE: [1-2 sentences: What could go wrong]
RISKS: [comma-separated list of 3 key risks]
CATALYSTS: [comma-separated list of any upcoming catalysts - earnings, FDA, etc. or "None identified"]
RECENT_DEVELOPMENTS: [1-2 sentences summarizing key recent news/developments]
ANALYST_SENTIMENT: [Brief note on analyst/market sentiment if known, or "Unknown"]
EARNINGS_CONTEXT: [Upcoming earnings date if known, or recent results summary, or "Unknown"]
SECTOR_TREND: [How is this sector performing? 1 sentence]
POSITION_SIZE: [Full size/Half size/Small starter]
ENTRY: [Enter now/Wait for confirmation/Scale in]

Be direct and actionable. Focus on what matters for the buy decision.
"""

    def _parse_enhanced_response(self, content: str, symbol: str) -> Optional[AIBuyAssessment]:
        """Parse enhanced AI response into AIBuyAssessment."""
        # Initialize with defaults
        assessment = {
            "conviction": "MEDIUM",
            "conviction_score": 50,
            "headline": f"{symbol} shows technical buy signal",
            "reasoning": "",
            "bull_case": "",
            "bear_case": "",
            "risk_factors": [],
            "catalysts": [],
            "position_sizing": "Half size",
            "entry_strategy": "Wait for confirmation",
            "company_context": "",
            "recent_developments": "",
            "analyst_sentiment": "Unknown",
            "earnings_context": "Unknown",
            "sector_trend": ""
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
            elif key == "COMPANY_CONTEXT":
                assessment["company_context"] = value
            elif key == "REASONING":
                assessment["reasoning"] = value
            elif key == "BULL_CASE":
                assessment["bull_case"] = value
            elif key == "BEAR_CASE":
                assessment["bear_case"] = value
            elif key == "RISKS":
                assessment["risk_factors"] = [r.strip() for r in value.split(",") if r.strip()][:4]
            elif key == "CATALYSTS":
                if value.lower() not in ("none", "none identified", "n/a", "unknown"):
                    assessment["catalysts"] = [c.strip() for c in value.split(",") if c.strip()][:3]
            elif key == "RECENT_DEVELOPMENTS":
                assessment["recent_developments"] = value
            elif key == "ANALYST_SENTIMENT":
                assessment["analyst_sentiment"] = value
            elif key == "EARNINGS_CONTEXT":
                assessment["earnings_context"] = value
            elif key == "SECTOR_TREND":
                assessment["sector_trend"] = value
            elif key == "POSITION_SIZE":
                assessment["position_sizing"] = value
            elif key == "ENTRY":
                assessment["entry_strategy"] = value

        return AIBuyAssessment(
            conviction=assessment["conviction"],
            conviction_score=assessment["conviction_score"],
            headline=assessment["headline"],
            reasoning=assessment["reasoning"],
            bull_case=assessment["bull_case"],
            bear_case=assessment["bear_case"],
            risk_factors=assessment["risk_factors"],
            catalysts=assessment["catalysts"],
            position_sizing=assessment["position_sizing"],
            entry_strategy=assessment["entry_strategy"],
            company_context=assessment["company_context"],
            recent_developments=assessment["recent_developments"],
            analyst_sentiment=assessment["analyst_sentiment"],
            earnings_context=assessment["earnings_context"],
            sector_trend=assessment["sector_trend"]
        )


# -----------------------------
# Polygon Client
# -----------------------------

class PolygonClient:
    def __init__(self, api_key: str, base_url: str, cache_dir: Path, timeout: int = 20):
        self._api_key = api_key
        self.base_url = base_url.rstrip("/")
        self.cache_dir = cache_dir
        self.timeout = timeout

        # Configure session with retry/backoff for transient errors
        self.session = requests.Session()
        retry_strategy = Retry(
            total=3,
            backoff_factor=1.0,  # 1s, 2s, 4s
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

    @property
    def api_key(self) -> str:
        return self._api_key

    def __repr__(self) -> str:
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
                            if content:
                                return json.loads(content)
                    except (json.JSONDecodeError, ValueError):
                        pass

        r = self.session.get(url, params=params, timeout=self.timeout)
        r.raise_for_status()
        data = r.json()

        if cache_ttl_sec > 0 and cache_key:
            cache_path = self.cache_dir / f"{cache_key}.json"
            try:
                with open(cache_path, "w", encoding="utf-8") as f:
                    json.dump(data, f)
            except (PermissionError, OSError):
                pass

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
        return self._get(
            path,
            params={"ticker": symbol, "limit": int(limit * 3), "order": "desc", "sort": "published_utc"},
            cache_ttl_sec=300
        )

    def get_ticker_details(self, symbol: str) -> Dict[str, Any]:
        """Get ticker details including company name."""
        path = f"/v3/reference/tickers/{symbol}"
        return self._get(path, params={}, cache_ttl_sec=86400)  # Cache for 24 hours


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
# News Scoring
# -----------------------------

def score_news(news_json: Dict[str, Any], lookback_hours: int, symbol: str = "") -> Tuple[Optional[float], List[Dict[str, Any]]]:
    """Score news sentiment and extract relevant articles."""
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

        published = r.get("published_utc")
        try:
            published_dt = dt.datetime.fromisoformat(published.replace("Z", "+00:00")) if published else None
        except Exception:
            published_dt = None

        if published_dt and published_dt < cutoff:
            continue

        # Calculate relevance score
        relevance_score = 0.0
        if symbol:
            symbol_upper = symbol.upper()
            tickers_in_article = [t.upper() for t in (r.get("tickers", []) or [])]
            title_upper = title.upper()

            # Primary ticker in article (most relevant)
            if tickers_in_article and tickers_in_article[0] == symbol_upper:
                relevance_score += 0.5
            # Symbol anywhere in tickers list
            elif symbol_upper in tickers_in_article:
                relevance_score += 0.3

            # FIX: Use regex word boundary to match "AAPL", "$AAPL", "(AAPL)", "AAPL," etc.
            # Pattern matches symbol surrounded by word boundaries or common punctuation
            symbol_pattern = rf'(?:^|[\s\$\(\[\{{\,])({re.escape(symbol_upper)})(?:[\s\)\]\}}\,\.\:\;]|$)'
            if re.search(symbol_pattern, title_upper):
                relevance_score += 0.4

            relevance_score = min(relevance_score, 1.0)
            if relevance_score < 0.3:
                continue

        # Format publish time
        published_str = ""
        if published_dt:
            delta = now_utc() - published_dt
            if delta.days > 0:
                published_str = f"{delta.days}d ago"
            elif delta.seconds >= 3600:
                published_str = f"{delta.seconds // 3600}h ago"
            else:
                published_str = f"{delta.seconds // 60}m ago"

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

        # Keep longer descriptions for better context (truncated in HTML if needed)
        if len(description) > 500:
            description = description[:497] + "..."

        articles.append({
            "title": title,
            "description": description if description else None,
            "published_str": published_str,
            "relevance_score": relevance_score,
            "sentiment": article_sentiment
        })

    articles.sort(key=lambda x: x["relevance_score"], reverse=True)

    if sentiments:
        score = float(np.clip(np.mean(sentiments), -1.0, 1.0))
        return score, articles[:5]
    return None, articles[:5]


# -----------------------------
# Ticker Analysis
# -----------------------------

def analyze_ticker(client: PolygonClient, symbol: str, lookback_days: int, cfg: Dict[str, Any]) -> TickerAnalysis:
    """Analyze a ticker for buy signals.

    Includes:
    - Technical indicators (RSI, EMA, Bollinger, ATR)
    - Trend context (EMA alignment, slope, price vs 200 EMA)
    - Volume confirmation (elevated volume on pullback)
    - News sentiment
    """
    # Use ET date to prevent rare off-by-one issues when machine TZ != ET
    today = now_utc().astimezone(ET).date()
    from_date = today - dt.timedelta(days=lookback_days)

    # FIX: During market hours, today's daily bar is incomplete and can distort
    # RSI/EMA/Bollinger calculations. Use only completed bars (up to yesterday)
    # for indicators, while still using snapshot for current price.
    if is_market_open(now_utc()):
        indicator_end_date = today - dt.timedelta(days=1)
    else:
        indicator_end_date = today

    df = client.get_daily_aggs(symbol, from_date, indicator_end_date)

    if df.empty or len(df) < 60:
        return TickerAnalysis(
            symbol=symbol,
            last_price=None, day_close=None,
            ema20=None, ema50=None, ema200=None, rsi14=None, atr14=None,
            boll_upper=None, boll_lower=None,
            distance_from_52w_low_pct=None, distance_from_52w_high_pct=None,
            news_sentiment_score=None, news_items=[],
            score=0.0, signal_type="none",
            reasons=["Insufficient price history from data provider."]
        )

    df = df.sort_values("date").reset_index(drop=True)
    close = df["close"].astype(float)
    volume = df["volume"].astype(float)

    # Calculate indicators
    ema20_s = ema(close, 20)
    ema50_s = ema(close, 50)
    ema200_s = ema(close, 200) if len(df) >= 200 else pd.Series([np.nan] * len(df))
    rsi14_s = rsi(close, 14)
    atr14_s = atr(df, 14)
    boll_u, boll_l = bollinger(close, 20, 2)

    # 52-week high/low
    high_52w = close.tail(252).max() if len(df) >= 252 else close.max()
    low_52w = close.tail(252).min() if len(df) >= 252 else close.min()

    # Recent 20-day high/low for Fibonacci TP calculations
    recent_high_20d = float(df["high"].tail(20).max()) if "high" in df.columns and len(df) >= 20 else None
    recent_low_20d = float(df["low"].tail(20).min()) if "low" in df.columns and len(df) >= 20 else None

    # Get current price
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

    # Get news
    news = client.get_news(symbol, limit=int(cfg.get("news_limit", 20)))
    news_score, news_items = score_news(news, int(cfg.get("news_lookback_hours", 72)), symbol=symbol)

    # Get company name
    company_name = None
    try:
        ticker_details = client.get_ticker_details(symbol)
        results = ticker_details.get("results", {}) or {}
        company_name = results.get("name")
    except Exception:
        pass

    # Extract values
    ema20_v = float(ema20_s.iloc[-1])
    ema50_v = float(ema50_s.iloc[-1])
    ema200_v = float(ema200_s.iloc[-1]) if not np.isnan(ema200_s.iloc[-1]) else None
    rsi_v = float(rsi14_s.iloc[-1]) if not np.isnan(rsi14_s.iloc[-1]) else None
    atr_v = float(atr14_s.iloc[-1]) if not np.isnan(atr14_s.iloc[-1]) else None
    boll_u_v = float(boll_u.iloc[-1]) if not np.isnan(boll_u.iloc[-1]) else None
    boll_l_v = float(boll_l.iloc[-1]) if not np.isnan(boll_l.iloc[-1]) else None
    day_close = float(close.iloc[-1])

    px = last_price if last_price is not None else day_close

    # Distance from 52w high/low
    dist_52w_low = ((px - low_52w) / low_52w) if low_52w > 0 else None
    dist_52w_high = ((px - high_52w) / high_52w) if high_52w > 0 else None

    # === TREND CONTEXT ===
    # Determine if we're buying with or against the trend
    # trend_aligned: None = insufficient history, True = above 200 EMA, False = below
    trend_aligned: Optional[bool] = None  # Unknown by default (insufficient data)
    ema_slope_200 = None
    ema_bullish_alignment = False
    falling_knife = False  # Hard guardrail flag

    if ema200_v:
        # Price above 200 EMA = with trend
        trend_aligned = px > ema200_v

        # Calculate 200 EMA slope (20-day change as % of price)
        if len(ema200_s) >= 20 and not np.isnan(ema200_s.iloc[-20]):
            ema200_20d_ago = float(ema200_s.iloc[-20])
            if ema200_20d_ago > 0:
                ema_slope_200 = (ema200_v - ema200_20d_ago) / ema200_20d_ago

        # FALLING KNIFE DETECTION: Below declining 200 EMA
        if not trend_aligned and ema_slope_200 is not None and ema_slope_200 < -0.02:
            falling_knife = True

    # EMA bullish alignment: 20 > 50 > 200
    if ema20_v and ema50_v and ema200_v:
        ema_bullish_alignment = ema20_v > ema50_v > ema200_v

    # === VOLUME CONFIRMATION ===
    # Check if volume is elevated on the pullback (accumulation)
    # Note: During market hours, this is YESTERDAY's volume (latest completed bar)
    volume_ratio = None
    volume_confirmed = False

    if len(volume) >= 20:
        avg_volume_20d = volume.tail(20).mean()
        latest_volume = volume.iloc[-1]  # Latest completed day's volume
        if avg_volume_20d > 0:
            volume_ratio = latest_volume / avg_volume_20d
            # Volume > 1.5x average = elevated (potential accumulation)
            volume_confirmed = volume_ratio >= 1.5

    # === SCORING ===
    score = 0.0
    reasons: List[str] = []
    signal_type = "none"

    # RSI Oversold
    rsi_oversold = cfg.get("rsi_oversold", 30)
    rsi_very_oversold = cfg.get("rsi_very_oversold", 25)

    if rsi_v is not None:
        if rsi_v <= rsi_very_oversold:
            score += 35
            reasons.append(f"RSI {rsi_v:.1f} - very oversold (≤{rsi_very_oversold}) - potential bounce")
            signal_type = "oversold"
        elif rsi_v <= rsi_oversold:
            score += 25
            reasons.append(f"RSI {rsi_v:.1f} - oversold (≤{rsi_oversold}) - watch for reversal")
            signal_type = "oversold"

    # Testing lower Bollinger band (support)
    if boll_l_v is not None and px <= boll_l_v * 1.01:
        score += 20
        pct_below = ((boll_l_v - px) / boll_l_v) * 100
        reasons.append(f"At/below lower Bollinger ({money(boll_l_v,2)}, {pct_below:.1f}% below) - support test")
        if signal_type == "none":
            signal_type = "support_test"

    # Testing key moving averages
    if ema200_v and abs(px - ema200_v) / ema200_v <= 0.02:
        score += 15
        reasons.append(f"Testing 200 EMA ({money(ema200_v,2)}) - major support level")
        if signal_type == "none":
            signal_type = "support_test"

    if ema50_v and abs(px - ema50_v) / ema50_v <= 0.02:
        score += 10
        reasons.append(f"Testing 50 EMA ({money(ema50_v,2)}) - intermediate support")

    # Near 52-week low (value opportunity)
    if dist_52w_low is not None and dist_52w_low <= 0.10:
        score += 15
        reasons.append(f"Within 10% of 52-week low ({money(low_52w,2)}) - value zone")
        if signal_type == "none":
            signal_type = "value"

    # Positive news on pullback
    if news_score is not None and news_score >= 0.3 and rsi_v and rsi_v < 40:
        score += 10
        reasons.append(f"Positive news ({news_score:+.2f}) during pullback - contrarian opportunity")

    # Negative news = caution
    if news_score is not None and news_score <= -0.3:
        score -= 10
        reasons.append(f"Negative news sentiment ({news_score:+.2f}) - proceed with caution")

    # === NEW: VOLUME CONFIRMATION BONUS ===
    if volume_confirmed and signal_type in ("oversold", "support_test"):
        score += 10
        reasons.append(f"Elevated volume ({volume_ratio:.1f}x avg) - accumulation signal")

    # === TREND CONTEXT ADJUSTMENTS ===
    # Penalize counter-trend trades (buying below declining 200 EMA)
    counter_trend_penalty = float(cfg.get("counter_trend_penalty", 15))
    falling_knife_max_score = float(cfg.get("falling_knife_max_score", 49))  # Hard cap

    # Check for reversal hints (needed for falling knife exception)
    # Reversal hint: RSI rising (today > yesterday) OR price back above lower Bollinger
    has_reversal_hint = False
    if len(rsi14_s) >= 2 and not np.isnan(rsi14_s.iloc[-2]):
        rsi_prev = float(rsi14_s.iloc[-2])
        if rsi_v is not None and rsi_v > rsi_prev:
            has_reversal_hint = True  # RSI rising
    if boll_l_v is not None and px > boll_l_v:
        has_reversal_hint = True  # Price back above lower Bollinger

    if trend_aligned is None:
        # Unknown trend (insufficient history) - mild caution
        reasons.append("Trend: Unknown (insufficient 200-day history)")
    elif not trend_aligned:
        # Below 200 EMA
        if falling_knife:
            # HARD GUARDRAIL: Below DECLINING 200 EMA = falling knife
            # Cap score unless we have BOTH volume confirmation AND reversal hint
            # (volume alone could be distribution; reversal hint suggests accumulation)
            score -= counter_trend_penalty
            if volume_confirmed and has_reversal_hint:
                # Exception: volume + reversal hint = potential bottom
                reasons.append(f"⚠️ COUNTER-TREND: Below declining 200 EMA, but volume + reversal hint")
            else:
                # Hard cap: can't alert on falling knives without confirmation
                score = min(score, falling_knife_max_score)
                if volume_confirmed:
                    reasons.append(f"⚠️ FALLING KNIFE: High volume but no reversal hint - could be distribution")
                else:
                    reasons.append(f"⚠️ FALLING KNIFE: Below declining 200 EMA - score capped at {int(falling_knife_max_score)}")
        else:
            # Below but 200 EMA flat/rising = less severe
            score -= counter_trend_penalty * 0.5
            reasons.append(f"Caution: Below 200 EMA - counter-trend trade")

    # Bonus for buying with trend (EMA bullish alignment)
    if trend_aligned is True and ema_bullish_alignment:
        score += 5
        reasons.append("Trend aligned: EMA20 > EMA50 > EMA200")

    score = float(np.clip(score, 0, 100))

    return TickerAnalysis(
        symbol=symbol,
        last_price=last_price,
        day_close=day_close,
        ema20=ema20_v,
        ema50=ema50_v,
        ema200=ema200_v,
        rsi14=rsi_v,
        atr14=atr_v,
        boll_upper=boll_u_v,
        boll_lower=boll_l_v,
        distance_from_52w_low_pct=dist_52w_low,
        distance_from_52w_high_pct=dist_52w_high,
        news_sentiment_score=news_score,
        news_items=news_items,
        score=score,
        signal_type=signal_type,
        reasons=reasons if reasons else ["No strong buy signals."],
        company_name=company_name,
        trend_aligned=trend_aligned,
        ema_slope_200=ema_slope_200,
        ema_bullish_alignment=ema_bullish_alignment,
        volume_ratio=volume_ratio,
        volume_confirmed=volume_confirmed,
        falling_knife=falling_knife,
        recent_high_20d=recent_high_20d,
        recent_low_20d=recent_low_20d,
    )


# -----------------------------
# Alert State
# -----------------------------

def load_state(state_file: Path) -> Dict[str, Any]:
    """Load state with fallback to empty on corruption."""
    if not state_file.exists():
        return {"sent": {}}
    try:
        with open(state_file, "r", encoding="utf-8") as f:
            content = f.read().strip()
            if not content:
                return {"sent": {}}
            data = json.loads(content)
            # Validate structure
            if not isinstance(data, dict):
                logging.warning("State file has invalid structure, resetting")
                return {"sent": {}}
            return data
    except (json.JSONDecodeError, ValueError) as e:
        logging.warning("State file corrupted (%s), resetting to empty", e)
        return {"sent": {}}
    except (PermissionError, OSError) as e:
        logging.error("Cannot read state file: %s", e)
        return {"sent": {}}

def save_state(state_file: Path, state: Dict[str, Any]) -> None:
    """Save state atomically (write to temp, then replace)."""
    ensure_dir(state_file.parent)
    temp_file = state_file.with_suffix(".tmp")
    try:
        with open(temp_file, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2)
            f.flush()
            os.fsync(f.fileno())
        # Atomic replace (works on POSIX; on Windows, may need to remove first)
        if os.name == "nt" and state_file.exists():
            state_file.unlink()
        temp_file.replace(state_file)
    except (PermissionError, OSError) as e:
        logging.error("Failed to save state: %s", e)
        # Clean up temp file if it exists
        if temp_file.exists():
            try:
                temp_file.unlink()
            except OSError:
                pass

def compute_ticker_hash(candidate: BuyCandidate, analysis: Optional[TickerAnalysis] = None) -> str:
    """
    Compute a hash representing signal MATERIALITY, not sizing or text.

    Uses WIDE bucket values and boolean flags to reduce sensitivity to minor fluctuations:
    - Score bucket (20-point buckets: 40-59, 60-79, 80-100)
    - RSI bucket (10-point buckets: 20-29, 30-39, etc.)
    - Signal type
    - Boolean indicator states (at support, trend aligned, etc.)

    Does NOT include:
    - suggested_shares (changes with cash/price)
    - reasons/text strings (embed jittery numbers like "RSI 29.9")
    - raw price values
    """
    # Use 20-point score buckets (40-59, 60-79, 80-100) to reduce noise
    score_bucket = int(candidate.score // 20) * 20

    data = {
        "symbol": candidate.symbol,
        "score_bucket": score_bucket,
        "signal_type": candidate.signal_type,
    }

    # Add key indicator states if analysis provided (boolean flags are stable)
    if analysis:
        # RSI in 10-point buckets (20-29, 30-39, etc.) - wider to reduce noise
        if analysis.rsi14 is not None:
            data["rsi_bucket"] = int(analysis.rsi14 // 10) * 10

        # Use the price consistently for all comparisons
        px = analysis.last_price if analysis.last_price is not None else analysis.day_close

        # Near key levels (boolean flags with wider buffer for noise resistance)
        # FIX: Convert numpy bool_ to Python bool for JSON serialization
        if analysis.boll_lower and px:
            # Wider 5% buffer to reduce flipping
            data["at_lower_boll"] = bool(px <= analysis.boll_lower * 1.05)
        if analysis.ema200 and px:
            # Wider 5% buffer for "near" detection
            data["near_200ema"] = bool(abs(px - analysis.ema200) / analysis.ema200 <= 0.05)
            data["above_200ema"] = bool(px > analysis.ema200)

        # EMA alignment flags for trend state
        if analysis.ema20 and analysis.ema50 and analysis.ema200:
            data["ema_bullish"] = bool(analysis.ema20 > analysis.ema50 > analysis.ema200)
            data["ema_bearish"] = bool(analysis.ema20 < analysis.ema50 < analysis.ema200)

        # 52-week position (bucket: near_low, middle, near_high)
        if analysis.distance_from_52w_low_pct is not None:
            if analysis.distance_from_52w_low_pct <= 0.10:
                data["52w_zone"] = "near_low"
            elif analysis.distance_from_52w_high_pct is not None and analysis.distance_from_52w_high_pct >= -0.10:
                data["52w_zone"] = "near_high"
            else:
                data["52w_zone"] = "middle"

        # Volume confirmation flag
        data["volume_confirmed"] = bool(analysis.volume_confirmed)

    return sha1_text(json.dumps(data, sort_keys=True))

def should_alert(state: Dict[str, Any], symbol: str, ticker_hash: str, min_cooldown_hours: int = 8) -> bool:
    """Check if we should send an alert for this ticker.

    Uses BOTH hash-based deduplication AND time-based cooldown:
    1. If never alerted before -> alert
    2. If hash unchanged (same signal conditions) -> never alert again
    3. If hash changed BUT within cooldown period -> wait (prevents rapid-fire alerts)
    4. If hash changed AND cooldown expired -> alert

    The hash captures signal materiality (score bucket, RSI bucket, signal type,
    key indicator states) so we only re-alert when conditions meaningfully change.

    The cooldown prevents rapid-fire alerts when indicators are bouncing around
    bucket boundaries (e.g., RSI 29 -> 31 -> 29 within hours).

    Args:
        state: Alert state dictionary
        symbol: Ticker symbol
        ticker_hash: Computed hash of current signal conditions
        min_cooldown_hours: Minimum hours between alerts for same ticker (default 8)
    """
    sent = state.get("sent", {}).get(symbol)
    if not sent:
        return True

    last_hash = sent.get("hash")
    last_ts_str = sent.get("ts_utc")

    # If hash unchanged, never re-alert (signal conditions haven't changed)
    if last_hash == ticker_hash:
        return False

    # Hash changed - check cooldown period
    if last_ts_str and min_cooldown_hours > 0:
        try:
            last_ts = dt.datetime.fromisoformat(last_ts_str.replace("Z", "+00:00"))
            hours_since = (now_utc() - last_ts).total_seconds() / 3600
            if hours_since < min_cooldown_hours:
                # Still in cooldown - don't alert even though conditions changed
                return False
        except (ValueError, TypeError):
            # Can't parse timestamp - allow alert
            pass

    # Hash changed AND cooldown expired -> alert
    return True

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

    msg.set_content(text_body)
    msg.add_alternative(html_body, subtype="html")

    import smtplib
    with smtplib.SMTP(host, port) as s:
        s.starttls()
        s.login(user, pwd)
        s.send_message(msg)


# -----------------------------
# Candidate Generation
# -----------------------------

def propose_buy_candidates(
    watchlist: List[WatchlistItem],
    analyses: Dict[str, TickerAnalysis],
    available_cash: float,
    cfg: Dict[str, Any]
) -> List[BuyCandidate]:
    """Generate buy candidates from watchlist based on analysis.

    Supports "idea mode" when available_cash <= 0:
    - Still emits candidates that meet scoring thresholds
    - Sets suggested_shares and est_cost to None (display as "N/A")
    - Still provides suggested_limit for reference
    - Useful for signal monitoring when not actively buying
    """
    candidates: List[BuyCandidate] = []

    min_score = cfg.get("min_score_to_alert", 50)
    idea_mode = available_cash <= 0  # No cash = idea-only mode

    for item in watchlist:
        sym = item.symbol.upper()
        analysis = analyses.get(sym)
        if not analysis:
            continue

        if analysis.score < min_score:
            continue

        if analysis.signal_type == "none":
            continue

        px = analysis.last_price if analysis.last_price is not None else analysis.day_close
        if px is None or px <= 0:
            continue

        # Check target price if set
        if item.target_price and px > item.target_price:
            continue

        # Calculate position size (or None for idea mode)
        suggested_shares: Optional[int] = None
        est_cost: Optional[float] = None

        # FIX: Always calculate suggested_limit - useful even in idea mode
        limit_premium_bps = float(cfg.get("limit_premium_bps", 10)) / 10000.0
        suggested_limit = px * (1.0 + limit_premium_bps)

        if not idea_mode:
            max_position = available_cash * item.max_position_pct
            suggested_shares = int(max_position / px)
            if suggested_shares <= 0:
                # Not enough cash for even 1 share - still include as idea
                suggested_shares = None
                est_cost = None
            else:
                est_cost = suggested_shares * px

        # Determine trend context flags for display
        # FIX: Preserve None to distinguish "unknown" from "aligned"
        trend_aligned = analysis.trend_aligned  # None, True, or False
        counter_trend_warning = analysis.falling_knife  # Use the guardrail flag

        # Calculate Take Profit targets using Fibonacci levels and ATR
        tp1, tp2, tp3, stop_loss = None, None, None, None
        if analysis.atr14 and px:
            atr_val = analysis.atr14
            # Stop loss: 1.5 ATR below entry (or use Bollinger lower)
            if analysis.boll_lower and analysis.boll_lower < px:
                stop_loss = round(analysis.boll_lower, 2)
            else:
                stop_loss = round(px - (1.5 * atr_val), 2)

            # TP1 (Conservative): 1 ATR above entry or EMA20
            tp1_atr = px + atr_val
            tp1_ema = analysis.ema20 if analysis.ema20 and analysis.ema20 > px else None
            tp1 = round(max(tp1_atr, tp1_ema) if tp1_ema else tp1_atr, 2)

            # TP2 (Moderate): Fibonacci 0.618 retracement from recent swing or EMA50
            if analysis.recent_high_20d and analysis.recent_low_20d:
                swing_range = analysis.recent_high_20d - analysis.recent_low_20d
                fib_618 = analysis.recent_low_20d + (swing_range * 0.618)
                tp2_ema = analysis.ema50 if analysis.ema50 and analysis.ema50 > px else None
                tp2 = round(max(fib_618, tp2_ema) if tp2_ema else fib_618, 2)
                if tp2 <= tp1:
                    tp2 = round(px + (2 * atr_val), 2)  # Fallback: 2 ATR
            else:
                tp2 = round(px + (2 * atr_val), 2)

            # TP3 (Aggressive): Recent 20-day high or Bollinger upper
            if analysis.recent_high_20d and analysis.recent_high_20d > px:
                tp3 = round(analysis.recent_high_20d, 2)
            elif analysis.boll_upper and analysis.boll_upper > px:
                tp3 = round(analysis.boll_upper, 2)
            else:
                tp3 = round(px + (3 * atr_val), 2)

            # Ensure TP levels are in ascending order
            if tp2 and tp1 and tp2 <= tp1:
                tp2 = round(tp1 * 1.02, 2)
            if tp3 and tp2 and tp3 <= tp2:
                tp3 = round(tp2 * 1.02, 2)

        candidates.append(BuyCandidate(
            symbol=sym,
            score=analysis.score,
            signal_type=analysis.signal_type,
            current_price=px,
            target_price=item.target_price,
            suggested_shares=suggested_shares,
            est_cost=est_cost,
            suggested_limit=suggested_limit,
            reasons=analysis.reasons,
            trend_aligned=trend_aligned,
            counter_trend_warning=counter_trend_warning,
            take_profit_1=tp1,
            take_profit_2=tp2,
            take_profit_3=tp3,
            stop_loss=stop_loss,
        ))

    candidates.sort(key=lambda c: c.score, reverse=True)
    return candidates[:int(cfg.get("max_positions_in_email", 10))]


# -----------------------------
# Email Formatting
# -----------------------------

def format_email(
    candidates: List[BuyCandidate],
    analyses: Dict[str, TickerAnalysis],
    available_cash: float,
    cfg: Dict[str, Any],
    ai_assessments: Optional[Dict[str, AIBuyAssessment]] = None
) -> Tuple[str, str, str]:
    """Format email content as a mobile-friendly quick rundown.

    Supports both normal mode (with position sizing) and idea mode (cash=0).
    Features:
    - AI headline and conviction front and center for quick scanning
    - Visual conviction meter/bars
    - Bull/bear cases for quick decision making
    - Catalysts and risk factors prominently displayed
    - Mobile-friendly responsive design with proper stacking
    - Color-coded elements for at-a-glance assessment
    """
    ai_assessments = ai_assessments or {}
    email_cfg = cfg.get("email", {})
    prefix = email_cfg.get("subject_prefix_buy", "[Buy Alert]")
    idea_mode = available_cash <= 0

    # Build subject with top conviction if available
    if ai_assessments:
        top_ai = list(ai_assessments.values())[0] if ai_assessments else None
        top_symbol = list(ai_assessments.keys())[0] if ai_assessments else ""
        if top_ai and top_ai.conviction == "HIGH":
            subject = f"{prefix} HIGH CONVICTION: {top_symbol}"
        else:
            count_text = f"{len(candidates)} Idea{'s' if len(candidates) != 1 else ''}" if idea_mode else f"{len(candidates)} Opportunit{'ies' if len(candidates) != 1 else 'y'}"
            subject = f"{prefix} {count_text}"
    else:
        count_text = f"{len(candidates)} Idea{'s' if len(candidates) != 1 else ''}" if idea_mode else f"{len(candidates)} Opportunit{'ies' if len(candidates) != 1 else 'y'}"
        subject = f"{prefix} {count_text}"

    # Helper functions
    def fmt_cost(cost: Optional[float]) -> str:
        return money(cost, 0) if cost is not None else "N/A"

    def fmt_limit(limit: Optional[float]) -> str:
        return money(limit, 2) if limit is not None else "N/A"

    def conviction_color(conv: str) -> Tuple[str, str, str]:
        """Return (bg_color, text_color, bar_color) for conviction level."""
        if conv == "HIGH":
            return ("#E8F5E9", "#1B5E20", "#2E7D32")
        elif conv == "MEDIUM":
            return ("#FFF3E0", "#E65100", "#F57C00")
        else:  # LOW
            return ("#FFEBEE", "#B71C1C", "#C62828")

    def conviction_bar(score: int, color: str) -> str:
        """Generate a visual conviction bar (0-100)."""
        width = max(5, min(100, score))
        return f'''
        <div style="background:#e0e0e0;border-radius:4px;height:8px;width:100%;margin:4px 0;">
            <div style="background:{color};border-radius:4px;height:8px;width:{width}%;"></div>
        </div>
        '''

    def risk_reward_bar(risk_pct: float, reward_pct: float) -> str:
        """Generate a visual risk/reward bar."""
        total = abs(risk_pct) + abs(reward_pct)
        if total == 0:
            return ""
        risk_width = int((abs(risk_pct) / total) * 100)
        reward_width = 100 - risk_width
        return f'''
        <div style="display:flex;border-radius:4px;overflow:hidden;height:12px;margin:8px 0;">
            <div style="background:#ef5350;width:{risk_width}%;display:flex;align-items:center;justify-content:center;">
                <span style="color:white;font-size:9px;font-weight:bold;">{abs(risk_pct):.1f}%</span>
            </div>
            <div style="background:#66bb6a;width:{reward_width}%;display:flex;align-items:center;justify-content:center;">
                <span style="color:white;font-size:9px;font-weight:bold;">{reward_pct:.1f}%</span>
            </div>
        </div>
        '''

    # Plain text version
    text_lines = [
        "=" * 50,
        "BUY ALERTS" + (" (IDEA MODE)" if idea_mode else ""),
        "=" * 50,
        ""
    ]

    for c in candidates:
        a = analyses[c.symbol]
        ai = ai_assessments.get(c.symbol)

        # Header with company name
        company_str = f" - {a.company_name}" if a.company_name else ""
        text_lines.append(f">>> {c.symbol}{company_str} @ {money(c.current_price, 2)} <<<")
        if ai:
            text_lines.append(f"AI CONVICTION: {ai.conviction} ({ai.conviction_score}/100)")
            text_lines.append(f"HEADLINE: {ai.headline}")
            text_lines.append("")
            if ai.company_context:
                text_lines.append(f"ABOUT: {ai.company_context}")
            text_lines.append(f"BULL CASE: {ai.bull_case}")
            text_lines.append(f"BEAR CASE: {ai.bear_case}")
            if ai.catalysts:
                text_lines.append(f"CATALYSTS: {', '.join(ai.catalysts)}")
            if ai.risk_factors:
                text_lines.append(f"RISKS: {', '.join(ai.risk_factors)}")
            text_lines.append(f"ENTRY: {ai.entry_strategy} | SIZE: {ai.position_sizing}")
        else:
            text_lines.append(f"Signal: {c.signal_type.upper()} | Score: {c.score:.0f}/100")
            # Add technical context when no AI
            if c.reasons:
                text_lines.append("WHY THIS SIGNAL:")
                for reason in c.reasons[:4]:
                    text_lines.append(f"  • {reason}")

        # Price targets - show all levels
        text_lines.append("")
        if c.stop_loss:
            risk_pct = ((c.current_price - c.stop_loss) / c.current_price) * 100 if c.current_price else 0
            text_lines.append(f"STOP LOSS: {money(c.stop_loss, 2)} (-{risk_pct:.1f}%)")
        tp_parts = []
        if c.take_profit_1:
            tp1_pct = ((c.take_profit_1 - c.current_price) / c.current_price) * 100 if c.current_price else 0
            tp_parts.append(f"TP1: {money(c.take_profit_1, 2)} (+{tp1_pct:.1f}%)")
        if c.take_profit_2:
            tp2_pct = ((c.take_profit_2 - c.current_price) / c.current_price) * 100 if c.current_price else 0
            tp_parts.append(f"TP2: {money(c.take_profit_2, 2)} (+{tp2_pct:.1f}%)")
        if c.take_profit_3:
            tp3_pct = ((c.take_profit_3 - c.current_price) / c.current_price) * 100 if c.current_price else 0
            tp_parts.append(f"TP3: {money(c.take_profit_3, 2)} (+{tp3_pct:.1f}%)")
        if tp_parts:
            text_lines.append(" | ".join(tp_parts))

        # Technical indicators
        text_lines.append("")
        indicators = []
        if a.rsi14:
            indicators.append(f"RSI: {a.rsi14:.0f}")
        if a.volume_ratio:
            indicators.append(f"Vol: {a.volume_ratio:.1f}x")
        if a.ema_bullish_alignment:
            indicators.append("EMA Aligned")
        if indicators:
            text_lines.append(" | ".join(indicators))

        # EMA levels
        if a.ema20 or a.ema50 or a.ema200:
            ema_parts = []
            if a.ema20:
                ema_parts.append(f"EMA20: {money(a.ema20, 2)}")
            if a.ema50:
                ema_parts.append(f"EMA50: {money(a.ema50, 2)}")
            if a.ema200:
                ema_parts.append(f"EMA200: {money(a.ema200, 2)}")
            text_lines.append(" | ".join(ema_parts))

        # 52-week context
        if a.distance_from_52w_low_pct is not None and a.distance_from_52w_high_pct is not None:
            text_lines.append(f"52W: {a.distance_from_52w_low_pct:.0f}% from low | {a.distance_from_52w_high_pct:.0f}% from high")

        # News headlines
        if a.news_items and len(a.news_items) > 0:
            text_lines.append("")
            text_lines.append("RECENT NEWS:")
            for news in a.news_items[:2]:
                title = news.get("title", "")[:60]
                if title:
                    text_lines.append(f"  • {title}")

        text_lines.append("-" * 50)
        text_lines.append("")

    text_lines.append("Do your own research. This is not financial advice.")
    text_body = "\n".join(text_lines)

    # HTML - Mobile-friendly quick rundown format
    cards_html = []
    for c in candidates:
        a = analyses[c.symbol]
        ai = ai_assessments.get(c.symbol)

        # Calculate risk/reward percentages
        risk_pct = ((c.current_price - c.stop_loss) / c.current_price * 100) if c.stop_loss else 0
        reward_pct = ((c.take_profit_1 - c.current_price) / c.current_price * 100) if c.take_profit_1 else 0
        rr_ratio = (reward_pct / risk_pct) if risk_pct > 0 else 0

        # Conviction styling
        if ai:
            bg_color, text_color, bar_color = conviction_color(ai.conviction)
            conviction_section = f'''
            <div style="background:{bg_color};border-radius:8px;padding:12px;margin-bottom:12px;">
                <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:4px;">
                    <span style="font-weight:bold;color:{text_color};font-size:13px;">AI CONVICTION</span>
                    <span style="font-weight:bold;color:{text_color};font-size:18px;">{ai.conviction}</span>
                </div>
                {conviction_bar(ai.conviction_score, bar_color)}
                <div style="color:{text_color};font-size:11px;text-align:right;">{ai.conviction_score}/100</div>
            </div>
            '''

            # AI Headline - the key hook
            headline_html = f'''
            <div style="font-size:16px;font-weight:600;color:#1a1a1a;line-height:1.4;margin-bottom:12px;padding:10px;background:#f8f9fa;border-left:4px solid {bar_color};border-radius:0 6px 6px 0;">
                {esc_html(ai.headline)}
            </div>
            '''

            # Company context (if available)
            context_html = ""
            if ai.company_context:
                context_html = f'''
                <div style="font-size:12px;color:#666;margin-bottom:12px;padding:8px;background:#fafafa;border-radius:4px;">
                    <span style="font-weight:bold;color:#888;">ABOUT:</span> {esc_html(ai.company_context)}
                </div>
                '''

            # Bull/Bear cases side by side (stacks on mobile)
            bull_bear_html = f'''
            <div style="margin-bottom:12px;">
                <div style="display:flex;flex-wrap:wrap;gap:8px;">
                    <div style="flex:1;min-width:140px;background:#E8F5E9;border-radius:6px;padding:10px;">
                        <div style="font-weight:bold;color:#2E7D32;font-size:11px;margin-bottom:4px;">BULL CASE</div>
                        <div style="font-size:12px;color:#1B5E20;line-height:1.4;">{esc_html(ai.bull_case) if ai.bull_case else "—"}</div>
                    </div>
                    <div style="flex:1;min-width:140px;background:#FFEBEE;border-radius:6px;padding:10px;">
                        <div style="font-weight:bold;color:#C62828;font-size:11px;margin-bottom:4px;">BEAR CASE</div>
                        <div style="font-size:12px;color:#B71C1C;line-height:1.4;">{esc_html(ai.bear_case) if ai.bear_case else "—"}</div>
                    </div>
                </div>
            </div>
            '''

            # Catalysts (if any)
            catalysts_html = ""
            if ai.catalysts:
                catalyst_badges = "".join([f'<span style="background:#E3F2FD;color:#1565C0;padding:3px 8px;border-radius:12px;font-size:11px;margin:2px;display:inline-block;">{esc_html(cat)}</span>' for cat in ai.catalysts[:3]])
                catalysts_html = f'''
                <div style="margin-bottom:10px;">
                    <div style="font-weight:bold;color:#1565C0;font-size:11px;margin-bottom:4px;">CATALYSTS</div>
                    {catalyst_badges}
                </div>
                '''

            # Risks
            risks_html = ""
            if ai.risk_factors:
                risk_badges = "".join([f'<span style="background:#FFF3E0;color:#E65100;padding:3px 8px;border-radius:12px;font-size:11px;margin:2px;display:inline-block;">{esc_html(risk)}</span>' for risk in ai.risk_factors[:3]])
                risks_html = f'''
                <div style="margin-bottom:10px;">
                    <div style="font-weight:bold;color:#E65100;font-size:11px;margin-bottom:4px;">KEY RISKS</div>
                    {risk_badges}
                </div>
                '''

            # Entry strategy badge
            entry_bg = "#2E7D32" if "now" in ai.entry_strategy.lower() else "#F57C00" if "wait" in ai.entry_strategy.lower() else "#1565C0"
            entry_html = f'''
            <div style="display:flex;gap:8px;margin-bottom:12px;flex-wrap:wrap;">
                <div style="background:{entry_bg};color:white;padding:6px 12px;border-radius:16px;font-size:12px;font-weight:bold;">{esc_html(ai.entry_strategy)}</div>
                <div style="background:#455A64;color:white;padding:6px 12px;border-radius:16px;font-size:12px;font-weight:bold;">{esc_html(ai.position_sizing)}</div>
            </div>
            '''
        else:
            # Fallback for no AI assessment
            conviction_section = ""
            headline_html = ""
            context_html = ""
            bull_bear_html = ""
            catalysts_html = ""
            risks_html = ""
            entry_html = ""

        # Signal badge
        signal_colors = {
            "oversold": ("#E3F2FD", "#1565C0"),
            "support_test": ("#E8F5E9", "#2E7D32"),
            "value": ("#FFF3E0", "#EF6C00"),
            "pullback": ("#E8F5E9", "#2E7D32"),
        }
        sig_bg, sig_color = signal_colors.get(c.signal_type, ("#ECEFF1", "#455A64"))
        signal_badge = f'<span style="background:{sig_bg};color:{sig_color};padding:3px 10px;border-radius:12px;font-size:11px;font-weight:600;">{c.signal_type.upper()}</span>'

        # Trend warning badge
        trend_badge = ""
        if c.counter_trend_warning:
            trend_badge = '<span style="background:#FFEBEE;color:#C62828;padding:3px 10px;border-radius:12px;font-size:11px;font-weight:600;margin-left:4px;">FALLING KNIFE</span>'
        elif c.trend_aligned is False:
            trend_badge = '<span style="background:#FFF8E1;color:#F57F17;padding:3px 10px;border-radius:12px;font-size:11px;font-weight:600;margin-left:4px;">BELOW 200 EMA</span>'

        # Score color based on strength
        score_color = "#2E7D32" if c.score >= 70 else "#1565C0" if c.score >= 50 else "#F57C00"

        # Risk/reward bar with explanation
        rr_bar_html = ""
        if risk_pct > 0 or reward_pct > 0:
            rr_bar_html = f'''
            {risk_reward_bar(risk_pct, reward_pct)}
            <div style="font-size:10px;color:#888;text-align:center;margin-top:2px;">
                <span style="color:#ef5350;">◀ Risk to Stop</span> | <span style="color:#66bb6a;">Reward to Target ▶</span>
            </div>
            '''

        # Price action section with visual risk/reward
        price_section = f'''
        <div style="background:#f8f9fa;border-radius:8px;padding:12px;margin-bottom:12px;">
            <div style="display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:8px;">
                <div>
                    <div style="font-size:11px;color:#888;">ENTRY PRICE</div>
                    <div style="font-size:24px;font-weight:bold;color:#1a1a1a;">{money(c.current_price, 2)}</div>
                </div>
                <div style="text-align:right;">
                    <div style="font-size:11px;color:#888;">SIGNAL STRENGTH</div>
                    <div style="font-size:24px;font-weight:bold;color:{score_color};">{c.score:.0f}<span style="font-size:14px;color:#888;">/100</span></div>
                </div>
            </div>
            {rr_bar_html}
            <div style="display:flex;justify-content:space-between;font-size:11px;color:#666;margin-top:8px;">
                <span>Stop: {money(c.stop_loss, 2) if c.stop_loss else "—"}</span>
                <span style="font-weight:600;">Risk:Reward {rr_ratio:.1f}:1</span>
                <span>Target: {money(c.take_profit_1, 2) if c.take_profit_1 else "—"}</span>
            </div>
        </div>
        '''

        # Position sizing (compact)
        if c.suggested_shares is not None:
            sizing_html = f'''
            <div style="display:flex;justify-content:space-between;align-items:center;padding:8px 12px;background:#E3F2FD;border-radius:6px;margin-bottom:12px;">
                <span style="font-size:12px;color:#1565C0;">Suggested:</span>
                <span style="font-weight:bold;color:#1565C0;">{c.suggested_shares:,} shares ({fmt_cost(c.est_cost)})</span>
            </div>
            '''
        else:
            sizing_html = f'''
            <div style="display:flex;justify-content:space-between;align-items:center;padding:8px 12px;background:#f5f5f5;border-radius:6px;margin-bottom:12px;">
                <span style="font-size:12px;color:#666;">Limit:</span>
                <span style="font-weight:bold;color:#1a1a1a;">{fmt_limit(c.suggested_limit)} (idea mode)</span>
            </div>
            '''

        # Key metrics row - RSI, Volume, EMA alignment
        metrics_parts = []
        if a.rsi14:
            rsi_color = "#C62828" if a.rsi14 < 30 else "#2E7D32" if a.rsi14 > 70 else "#666"
            metrics_parts.append(f'<span style="color:{rsi_color};">RSI {a.rsi14:.0f}</span>')
        if a.volume_ratio:
            vol_color = "#2E7D32" if a.volume_ratio > 1.5 else "#666"
            metrics_parts.append(f'<span style="color:{vol_color};">Vol {a.volume_ratio:.1f}x</span>')
        if a.ema_bullish_alignment:
            metrics_parts.append('<span style="color:#2E7D32;">EMA Aligned</span>')
        metrics_html = f'''
        <div style="font-size:11px;color:#888;margin-bottom:12px;display:flex;gap:12px;flex-wrap:wrap;">
            {" ".join(metrics_parts)}
        </div>
        ''' if metrics_parts else ""

        # === ENHANCED: 52-Week Context ===
        week52_html = ""
        if a.distance_from_52w_low_pct is not None or a.distance_from_52w_high_pct is not None:
            low_dist = a.distance_from_52w_low_pct or 0
            high_dist = a.distance_from_52w_high_pct or 0
            # Color based on position in range
            if low_dist < 10:
                range_color = "#C62828"  # Red - near lows
                range_desc = "Near 52W Low"
                range_tip = "Stock is near its lowest price in the past year - could be oversold or in trouble"
            elif high_dist < 10:
                range_color = "#2E7D32"  # Green - near highs
                range_desc = "Near 52W High"
                range_tip = "Stock is near its highest price in the past year - showing strength"
            else:
                range_color = "#666"
                range_desc = "Mid-Range"
                range_tip = "Stock is in the middle of its yearly trading range"
            week52_html = f'''
            <div style="padding:8px 12px;background:#fafafa;border-radius:6px;margin-bottom:8px;font-size:11px;">
                <div style="display:flex;justify-content:space-between;align-items:center;">
                    <span style="color:#888;">📊 52-Week Range</span>
                    <span><span style="color:{range_color};font-weight:600;">{range_desc}</span></span>
                </div>
                <div style="color:#999;font-size:10px;margin-top:4px;">{range_tip}</div>
                <div style="color:#666;font-size:10px;margin-top:2px;">{low_dist:.0f}% above yearly low · {abs(high_dist):.0f}% below yearly high</div>
            </div>
            '''

        # === ENHANCED: EMA Levels with Explanations ===
        ema_html = ""
        ema_items = []
        ema_explanations = []
        if a.ema20 and c.current_price:
            pct_from_20 = ((c.current_price - a.ema20) / a.ema20) * 100
            ema_items.append(f'EMA20: {money(a.ema20, 2)} ({pct_from_20:+.1f}%)')
        if a.ema50 and c.current_price:
            pct_from_50 = ((c.current_price - a.ema50) / a.ema50) * 100
            ema_items.append(f'EMA50: {money(a.ema50, 2)} ({pct_from_50:+.1f}%)')
        if a.ema200 and c.current_price:
            pct_from_200 = ((c.current_price - a.ema200) / a.ema200) * 100
            color_200 = "#2E7D32" if pct_from_200 > 0 else "#C62828"
            ema_items.append(f'<span style="color:{color_200};">EMA200: {money(a.ema200, 2)} ({pct_from_200:+.1f}%)</span>')
            if pct_from_200 > 0:
                ema_explanations.append("Price above 200-day average = long-term uptrend")
            else:
                ema_explanations.append("Price below 200-day average = potential downtrend")
        if ema_items:
            ema_explanation_text = f'<div style="color:#999;font-size:10px;margin-top:4px;">{" · ".join(ema_explanations)}</div>' if ema_explanations else ""
            ema_html = f'''
            <div style="font-size:11px;color:#666;margin-bottom:8px;padding:8px 12px;background:#fafafa;border-radius:6px;">
                <div style="color:#888;margin-bottom:4px;font-weight:600;">📈 Moving Averages <span style="font-weight:normal;font-size:10px;">(price smoothed over time)</span></div>
                <div style="display:flex;gap:16px;flex-wrap:wrap;">{" · ".join(ema_items)}</div>
                {ema_explanation_text}
            </div>
            '''

        # === ENHANCED: Bollinger Band Context ===
        boll_html = ""
        if a.boll_lower and a.boll_upper and c.current_price:
            boll_range = a.boll_upper - a.boll_lower
            if boll_range > 0:
                position_in_bands = (c.current_price - a.boll_lower) / boll_range * 100
                if position_in_bands < 20:
                    boll_desc = "Near Lower Band"
                    boll_color = "#C62828"
                    boll_tip = "Price near lower band often signals oversold - potential bounce opportunity"
                elif position_in_bands > 80:
                    boll_desc = "Near Upper Band"
                    boll_color = "#2E7D32"
                    boll_tip = "Price near upper band may indicate overbought - could see resistance"
                else:
                    boll_desc = "Mid-Bands"
                    boll_color = "#666"
                    boll_tip = "Price in middle of normal range - neutral territory"
                boll_html = f'''
                <div style="padding:8px 12px;background:#fafafa;border-radius:6px;margin-bottom:8px;font-size:11px;">
                    <div style="display:flex;justify-content:space-between;align-items:center;">
                        <span style="color:#888;">📉 Bollinger Bands <span style="font-size:10px;">(volatility range)</span></span>
                        <span style="color:{boll_color};font-weight:600;">{boll_desc}</span>
                    </div>
                    <div style="color:#999;font-size:10px;margin-top:4px;">{boll_tip}</div>
                    <div style="color:#666;font-size:10px;margin-top:2px;">Range: {money(a.boll_lower, 2)} – {money(a.boll_upper, 2)}</div>
                </div>
                '''

        # === ENHANCED: ATR Context (expected daily move) ===
        atr_html = ""
        if a.atr14 and c.current_price:
            atr_pct = (a.atr14 / c.current_price) * 100
            volatility_desc = "High volatility" if atr_pct > 3 else "Moderate volatility" if atr_pct > 1.5 else "Low volatility"
            atr_html = f'''
            <div style="padding:8px 12px;background:#fafafa;border-radius:6px;margin-bottom:8px;font-size:11px;">
                <div style="display:flex;justify-content:space-between;align-items:center;">
                    <span style="color:#888;">📊 Expected Daily Move</span>
                    <span style="color:#666;">±{money(a.atr14, 2)} ({atr_pct:.1f}%)</span>
                </div>
                <div style="color:#999;font-size:10px;margin-top:4px;">{volatility_desc} - this is how much the stock typically moves in a day</div>
            </div>
            '''

        # === ENHANCED: All Take Profit Levels with Explanations ===
        tp_html = ""
        tp_items = []
        if c.take_profit_1 and c.current_price:
            tp1_pct = ((c.take_profit_1 - c.current_price) / c.current_price) * 100
            tp_items.append(f'<span style="color:#2E7D32;">TP1: {money(c.take_profit_1, 2)} (+{tp1_pct:.1f}%)</span>')
        if c.take_profit_2 and c.current_price:
            tp2_pct = ((c.take_profit_2 - c.current_price) / c.current_price) * 100
            tp_items.append(f'<span style="color:#1565C0;">TP2: {money(c.take_profit_2, 2)} (+{tp2_pct:.1f}%)</span>')
        if c.take_profit_3 and c.current_price:
            tp3_pct = ((c.take_profit_3 - c.current_price) / c.current_price) * 100
            tp_items.append(f'<span style="color:#7B1FA2;">TP3: {money(c.take_profit_3, 2)} (+{tp3_pct:.1f}%)</span>')
        if len(tp_items) > 1:
            tp_html = f'''
            <div style="font-size:11px;margin-bottom:8px;padding:8px 12px;background:#E8F5E9;border-radius:6px;">
                <div style="color:#2E7D32;margin-bottom:4px;font-weight:600;">🎯 Take Profit Targets <span style="font-weight:normal;font-size:10px;">(price goals to sell at)</span></div>
                <div style="display:flex;gap:16px;flex-wrap:wrap;">{" · ".join(tp_items)}</div>
                <div style="color:#999;font-size:10px;margin-top:4px;">TP1 = Conservative · TP2 = Moderate · TP3 = Aggressive</div>
            </div>
            '''

        # === ENHANCED: Recent News with Summaries ===
        news_html = ""
        if a.news_items and len(a.news_items) > 0:
            news_cards = []
            for news in a.news_items[:3]:  # Show up to 3 articles
                title = news.get("title", "")
                description = news.get("description", "")
                if title:
                    # Truncate title if too long
                    display_title = title[:100] + "..." if len(title) > 100 else title
                    # Get summary/description (truncate to 150 chars)
                    summary = ""
                    if description:
                        summary = description[:150] + "..." if len(description) > 150 else description
                        summary = f'<div style="font-size:11px;color:#666;margin-top:4px;line-height:1.4;">{esc_html(summary)}</div>'
                    news_cards.append(f'''
                    <div style="margin-bottom:8px;padding:8px;background:#fff;border-radius:4px;">
                        <div style="font-size:12px;color:#1a1a1a;font-weight:500;line-height:1.3;">{esc_html(display_title)}</div>
                        {summary}
                    </div>
                    ''')
            if news_cards:
                sentiment_text = ""
                sentiment_emoji = "📰"
                if a.news_sentiment_score is not None:
                    if a.news_sentiment_score > 0.2:
                        sentiment_text = '<span style="color:#2E7D32;font-weight:600;">Positive Sentiment</span>'
                        sentiment_emoji = "📈"
                    elif a.news_sentiment_score < -0.2:
                        sentiment_text = '<span style="color:#C62828;font-weight:600;">Negative Sentiment</span>'
                        sentiment_emoji = "📉"
                    else:
                        sentiment_text = '<span style="color:#666;">Neutral Sentiment</span>'
                news_html = f'''
                <div style="margin-bottom:8px;padding:10px 12px;background:#FFF8E1;border-radius:6px;border-left:3px solid #F9A825;">
                    <div style="font-size:11px;color:#F57F17;margin-bottom:8px;font-weight:600;">{sentiment_emoji} Recent News {sentiment_text}</div>
                    {"".join(news_cards)}
                </div>
                '''

        # Technical reasons - show expanded by default when no AI assessment
        reasons_html = ""
        if c.reasons:
            reasons_items = [f'<li style="margin:2px 0;">{esc_html(r)}</li>' for r in c.reasons[:6]]
            if ai:
                # With AI: keep collapsed since AI provides better context
                reasons_html = f'''
                <details style="margin-top:8px;">
                    <summary style="font-size:11px;color:#888;cursor:pointer;">Technical Details</summary>
                    <ul style="margin:8px 0 0 16px;padding:0;font-size:11px;color:#666;line-height:1.5;">
                        {"".join(reasons_items)}
                    </ul>
                </details>
                '''
            else:
                # Without AI: show expanded for more context
                reasons_html = f'''
                <div style="margin-bottom:8px;padding:10px 12px;background:#E3F2FD;border-radius:6px;border-left:3px solid #1565C0;">
                    <div style="font-size:11px;color:#1565C0;margin-bottom:6px;font-weight:600;">Why This Signal</div>
                    <ul style="margin:0;padding:0 0 0 16px;font-size:11px;color:#455A64;line-height:1.5;">
                        {"".join(reasons_items)}
                    </ul>
                </div>
                '''

        # Company name display
        company_display = ""
        if a.company_name:
            company_display = f'<div style="font-size:12px;color:#666;margin-top:2px;">{esc_html(a.company_name)}</div>'

        # Assemble the card
        cards_html.append(f'''
        <div style="border:1px solid #e0e0e0;border-radius:12px;padding:16px;margin-bottom:16px;background:#fff;box-shadow:0 2px 4px rgba(0,0,0,0.05);">
            <!-- Header with company name -->
            <div style="display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:12px;flex-wrap:wrap;gap:8px;">
                <div>
                    <div style="font-size:22px;font-weight:bold;color:#1a1a1a;">{esc_html(c.symbol)}</div>
                    {company_display}
                </div>
                <div>{signal_badge}{trend_badge}</div>
            </div>

            <!-- AI Conviction meter -->
            {conviction_section}

            <!-- AI Headline -->
            {headline_html}

            <!-- Company context -->
            {context_html}

            <!-- Bull/Bear cases -->
            {bull_bear_html}

            <!-- Catalysts -->
            {catalysts_html}

            <!-- Risks -->
            {risks_html}

            <!-- Entry strategy -->
            {entry_html}

            <!-- Price section with risk/reward bar -->
            {price_section}

            <!-- Position sizing -->
            {sizing_html}

            <!-- Key metrics -->
            {metrics_html}

            <!-- 52-Week Context -->
            {week52_html}

            <!-- EMA Levels -->
            {ema_html}

            <!-- Bollinger Bands -->
            {boll_html}

            <!-- ATR Context -->
            {atr_html}

            <!-- Take Profit Targets -->
            {tp_html}

            <!-- Recent News -->
            {news_html}

            <!-- Technical reasons -->
            {reasons_html}
        </div>
        ''')

    # Header
    header_text = '<p style="color:#666;font-size:13px;margin-bottom:16px;">Signal monitoring mode</p>' if idea_mode else f'<p style="color:#666;font-size:13px;margin-bottom:16px;">Available: {money(available_cash, 0)}</p>'

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
            <div style="text-align:center;margin-bottom:16px;">
                <h1 style="color:#1a1a1a;font-size:20px;margin:0 0 4px 0;">📈 Buy Alerts</h1>
                {header_text}
            </div>

            <!-- Cards -->
            {''.join(cards_html)}

            <!-- Quick Reference Guide -->
            <details style="margin-top:16px;background:#fff;border-radius:8px;padding:12px;">
                <summary style="font-size:12px;color:#1565C0;cursor:pointer;font-weight:600;">📚 Quick Reference Guide</summary>
                <div style="font-size:11px;color:#666;margin-top:12px;line-height:1.6;">
                    <div style="margin-bottom:8px;"><strong>Signal Types:</strong></div>
                    <ul style="margin:0 0 12px 16px;padding:0;">
                        <li><strong>OVERSOLD</strong> - Stock has dropped significantly and may bounce back</li>
                        <li><strong>SUPPORT TEST</strong> - Price testing a level where it previously bounced</li>
                        <li><strong>VALUE</strong> - Trading below fair value based on fundamentals</li>
                    </ul>
                    <div style="margin-bottom:8px;"><strong>Key Terms:</strong></div>
                    <ul style="margin:0 0 12px 16px;padding:0;">
                        <li><strong>RSI</strong> - Measures if stock is overbought (>70) or oversold (<30)</li>
                        <li><strong>EMA</strong> - Moving average that smooths price over time (20/50/200 days)</li>
                        <li><strong>Stop Loss</strong> - Price to sell at to limit your loss if trade goes wrong</li>
                        <li><strong>Take Profit (TP)</strong> - Target prices to sell at for gains</li>
                        <li><strong>Risk:Reward</strong> - Ratio of potential loss vs gain (higher = better)</li>
                    </ul>
                    <div style="margin-bottom:8px;"><strong>Score Meaning:</strong></div>
                    <ul style="margin:0 0 0 16px;padding:0;">
                        <li><strong>70+</strong> - Strong signal, multiple factors align</li>
                        <li><strong>50-69</strong> - Moderate signal, worth considering</li>
                        <li><strong>&lt;50</strong> - Weak signal, proceed with caution</li>
                    </ul>
                </div>
            </details>

            <!-- Footer -->
            <p style="color:#999;font-size:11px;text-align:center;margin-top:16px;padding:16px;background:#fff;border-radius:8px;">
                Technical analysis signals only. Always do your own research before investing.
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
    config_path = Path(path).resolve()
    with open(config_path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    # Store config directory for resolving relative paths
    cfg["_config_dir"] = config_path.parent
    return cfg

def load_watchlist(cfg: Dict[str, Any]) -> List[WatchlistItem]:
    """
    Load watchlist from config.

    Supports multiple config formats:
    1. Direct watchlist: cfg["watchlist"] = [{symbol: "AAPL", ...}, ...]
    2. Universe section: cfg["universe"] with source: "file" | "list" | "reddit" | "hybrid"

    Hybrid mode combines file + reddit trending for best coverage.
    """
    items = []
    seen_symbols: set = set()

    def add_symbol(sym: str, notes: str = ""):
        """Add symbol if not already seen."""
        sym = sym.upper().strip()
        if sym and sym not in seen_symbols:
            seen_symbols.add(sym)
            items.append(WatchlistItem(
                symbol=sym,
                target_price=None,
                max_position_pct=0.05,
                notes=notes
            ))

    # Check for direct watchlist first (backwards compatible)
    if "watchlist" in cfg:
        for w in cfg["watchlist"]:
            items.append(WatchlistItem(
                symbol=w["symbol"].upper().strip(),
                target_price=w.get("target_price"),
                max_position_pct=w.get("max_position_pct", 0.05),
                notes=w.get("notes", "")
            ))
        return items

    # Check for universe section (hysa_phase1 config format)
    universe = cfg.get("universe", {})
    if not universe:
        return items

    source = universe.get("source", "list")
    config_dir = Path(cfg.get("_config_dir", "."))

    # === Source: file ===
    if source in ("file", "hybrid"):
        file_path = universe.get("file_path", "")
        if file_path:
            universe_file = config_dir / file_path
            if universe_file.exists():
                with open(universe_file, "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if line and not line.startswith("#"):
                            add_symbol(line, "from_file")
                logging.info("Loaded %d symbols from universe file: %s", len(seen_symbols), universe_file)
            else:
                logging.warning("Universe file not found: %s", universe_file)

    # === Source: list ===
    elif source == "list":
        for s in universe.get("list", []):
            if s:
                add_symbol(s)

    # === Source: reddit or hybrid ===
    if source in ("reddit", "hybrid"):
        reddit_cfg = universe.get("reddit", {})
        include_reddit = universe.get("include_reddit_trending", source == "reddit")

        if include_reddit:
            try:
                from reddit_sentiment import RedditSentimentProvider

                reddit_limit = reddit_cfg.get("limit", universe.get("reddit_trending_limit", 30))
                min_mentions = reddit_cfg.get("min_mentions", universe.get("min_mentions", 20))
                cache_dir = str(config_dir / "state" / "cache")

                logging.info("Fetching Reddit trending stocks (limit=%d, min_mentions=%d)...",
                            reddit_limit, min_mentions)

                provider = RedditSentimentProvider(
                    cache_ttl_minutes=30,
                    cache_dir=cache_dir,
                    min_mentions=min_mentions
                )

                reddit_stocks = provider.get_trending_tickers(limit=reddit_limit)
                reddit_count_before = len(seen_symbols)

                # Filter options
                only_bullish = reddit_cfg.get("only_bullish", False)
                exclude_bearish = reddit_cfg.get("exclude_bearish", True)
                min_sentiment = reddit_cfg.get("min_sentiment", -0.5)

                for stock in reddit_stocks:
                    # Apply filters
                    if only_bullish and not stock.is_bullish:
                        continue
                    if exclude_bearish and stock.is_bearish:
                        continue
                    if stock.sentiment < min_sentiment:
                        continue

                    note = f"reddit:{stock.mentions}mentions"
                    if stock.sentiment_label != "neutral":
                        note += f",{stock.sentiment_label}"
                    add_symbol(stock.ticker, note)

                reddit_added = len(seen_symbols) - reddit_count_before
                logging.info("Added %d symbols from Reddit trending", reddit_added)

            except ImportError:
                logging.warning("reddit_sentiment module not found - skipping Reddit integration")
            except Exception as e:
                logging.warning("Failed to fetch Reddit trending: %s", e)

    logging.info("Total universe: %d symbols", len(items))
    return items


def validate_config(cfg: Dict[str, Any]) -> List[str]:
    """
    Validate config and return list of errors. Empty list = valid.
    Fail-fast: catches misconfigurations at startup rather than at runtime.
    """
    errors = []

    # Check for watchlist/universe presence
    has_watchlist = bool(cfg.get("watchlist"))
    has_universe = bool(cfg.get("universe", {}).get("source"))
    if not has_watchlist and not has_universe:
        errors.append("Config must have either 'watchlist:' or 'universe:' section with symbols")

    # Check universe file exists if source=file
    universe = cfg.get("universe", {})
    if universe.get("source") == "file":
        file_path = universe.get("file_path", "")
        if not file_path:
            errors.append("universe.source='file' but universe.file_path is empty")
        else:
            config_dir = cfg.get("_config_dir", Path("."))
            if not (config_dir / file_path).exists():
                errors.append(f"Universe file not found: {config_dir / file_path}")

    # Check required email env vars if email is enabled
    # FIX: Email is DISABLED by default unless email: section exists in config
    # This allows local testing and CI runs without SMTP configuration
    email_cfg = cfg.get("email")
    if email_cfg is not None:
        # email: section exists -> default enabled=True unless explicitly false
        email_enabled = email_cfg.get("enabled", True)
        if email_enabled:
            missing_env = []
            # These are the actual env vars used by send_email()
            for var in ["SMTP_HOST", "SMTP_USER", "SMTP_PASS", "EMAIL_FROM", "EMAIL_TO"]:
                if not os.environ.get(var):
                    missing_env.append(var)
            if missing_env:
                errors.append(f"Email enabled but missing env vars: {', '.join(missing_env)} (set email.enabled: false to disable)")
    # else: no email: section = email disabled, no validation needed

    # Check Polygon API key
    if not os.environ.get("POLYGON_API_KEY"):
        errors.append("Missing POLYGON_API_KEY environment variable")

    return errors


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True, help="Path to config.yaml")
    parser.add_argument("--once", action="store_true", help="Run one analysis cycle and exit.")
    args = parser.parse_args()

    load_dotenv()
    cfg = load_config(args.config)
    config_dir = cfg["_config_dir"]

    # Validate config early (fail-fast)
    validation_errors = validate_config(cfg)
    if validation_errors:
        print("Configuration errors:", file=sys.stderr)
        for err in validation_errors:
            print(f"  - {err}", file=sys.stderr)
        sys.exit(1)

    paths = cfg.get("paths", {})
    # Support buy-specific paths (hysa_phase1 format) with fallback to generic paths
    # When ALGO_OUTPUT_DIR is set, redirect state/cache/logs to local (non-synced) dir
    _output_dir = Path(os.getenv("ALGO_OUTPUT_DIR", "")) if os.getenv("ALGO_OUTPUT_DIR") else config_dir
    state_file = _output_dir / paths.get("buy_state_file", paths.get("state_file", "state/buy_alert_state.json"))
    cache_dir = _output_dir / paths.get("cache_dir", "state/cache")
    log_file = _output_dir / paths.get("buy_log_file", paths.get("log_file", "logs/buy_alerts.log"))
    ensure_dir(cache_dir)
    setup_logging(log_file)

    api_key = os.environ.get("POLYGON_API_KEY")
    # Note: validate_config already checks this, but keep explicit check for clarity
    if not api_key:
        raise RuntimeError("Missing POLYGON_API_KEY in environment (.env).")

    polygon_cfg = cfg.get("polygon", {})
    client = PolygonClient(
        api_key=api_key,
        base_url=polygon_cfg.get("base_url", "https://api.polygon.io"),
        cache_dir=cache_dir
    )

    # Support both flat config and nested buy: section
    buy_cfg = cfg.get("buy", {})
    analysis_cfg = cfg.get("analysis", {})
    # Merge buy section into analysis for threshold lookups
    merged_cfg = {**analysis_cfg, **buy_cfg}
    available_cash = float(buy_cfg.get("cash_budget_usd", cfg.get("available_cash", 10000)))
    watchlist = load_watchlist(cfg)

    state = load_state(state_file)

    def run_cycle() -> None:
        if not watchlist:
            logging.warning("No watchlist items configured.")
            return

        logging.info("Analyzing %d watchlist items...", len(watchlist))

        analyses: Dict[str, TickerAnalysis] = {}
        for item in watchlist:
            try:
                analyses[item.symbol] = analyze_ticker(
                    client=client,
                    symbol=item.symbol,
                    lookback_days=int(merged_cfg.get("daily_lookback_days", 365)),
                    cfg=merged_cfg
                )
                logging.info("Analyzed %s score=%.0f signal=%s",
                           item.symbol, analyses[item.symbol].score, analyses[item.symbol].signal_type)
            except Exception as e:
                error_msg = redact_api_keys(str(e))
                logging.error("Analysis failed for %s: %s", item.symbol, error_msg)

        candidates = propose_buy_candidates(
            watchlist=watchlist,
            analyses=analyses,
            available_cash=available_cash,
            cfg=merged_cfg
        )

        if not candidates:
            logging.info("No buy candidates met alert thresholds.")
            return

        # Use hash-based deduplication WITH time-based cooldown
        # Cooldown prevents rapid-fire alerts when indicators bounce around bucket boundaries
        min_cooldown_hours = int(merged_cfg.get("alert_cooldown_hours", 8))
        eligible = []
        ticker_hashes = {}

        for c in candidates:
            # Pass analysis to hash for indicator-based deduplication
            ticker_hash = compute_ticker_hash(c, analyses.get(c.symbol))
            ticker_hashes[c.symbol] = ticker_hash
            if should_alert(state, c.symbol, ticker_hash, min_cooldown_hours):
                eligible.append(c)
            else:
                logging.debug("%s: Skipped (hash unchanged or within %dh cooldown)", c.symbol, min_cooldown_hours)

        if not eligible:
            logging.info("Candidates exist but all have unchanged signal conditions or within cooldown.")
            return

        # Run AI analysis on eligible candidates
        ai_assessments = None
        if os.environ.get("ANTHROPIC_API_KEY"):
            try:
                ai_analyzer = AIBuyAnalyzer()
                ai_assessments = ai_analyzer.analyze_candidates(eligible, analyses)
                logging.info("AI analysis complete for %d candidate(s).", len(ai_assessments))
            except Exception as e:
                logging.warning("AI analysis failed (continuing without): %s", str(e))

        subject, text_body, html_body = format_email(
            candidates=eligible,
            analyses=analyses,
            available_cash=available_cash,
            cfg=cfg,
            ai_assessments=ai_assessments
        )

        # Check if email is enabled
        # FIX: Email disabled by default unless email: section exists in config
        email_cfg = cfg.get("email")
        email_enabled = email_cfg.get("enabled", True) if email_cfg is not None else False

        if email_enabled:
            send_email(subject, text_body, html_body)
            logging.info("Email sent with %d candidate(s).", len(eligible))
        else:
            logging.info("Email disabled - would have sent %d candidate(s): %s",
                        len(eligible), ", ".join(c.symbol for c in eligible))

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
            error_msg = redact_api_keys(str(e))
            logging.error("Cycle error: %s", error_msg)
            import traceback
            tb = redact_api_keys(traceback.format_exc())
            logging.debug("Traceback:\n%s", tb)

        interval = int(analysis_cfg.get("interval_seconds", 300)) if is_market_open(now_utc()) else int(analysis_cfg.get("off_hours_interval_seconds", 3600))
        time.sleep(max(interval, 30))


if __name__ == "__main__":
    main()
