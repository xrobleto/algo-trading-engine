#!/usr/bin/env python3
"""
Swing Trading Daily Newsletter

Generates a comprehensive daily newsletter for swing traders including:
- Market context (SPY/QQQ trend, VIXY volatility, sector rotation, breadth)
- Portfolio review (open positions, P&L, key levels)
- Watchlist & new setups (bull flags, pullbacks, breakouts)
- Risk dashboard (exposure, correlation, earnings calendar)

Run daily before market open (e.g., 6:00 AM ET via cron/task scheduler)
"""

import os
import sys
import logging
import datetime as dt
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from email.message import EmailMessage
import smtplib
import json
import hashlib

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import pandas as pd
import numpy as np
import yaml
from zoneinfo import ZoneInfo
import yfinance as yf

# ============================================================================
# CONFIGURATION
# ============================================================================

SCRIPT_DIR = Path(__file__).parent
BASE_DIR = SCRIPT_DIR.parent  # Algo_Trading root
_output_root = Path(os.getenv("ALGO_OUTPUT_DIR", "")) if os.getenv("ALGO_OUTPUT_DIR") else BASE_DIR
CONFIG_PATH = BASE_DIR / "config" / "alerts_config.yaml"
ENV_FILE = BASE_DIR / "config" / "alerts.env"
STATE_FILE = _output_root / "data" / "state" / "swing_newsletter_state.json"
CACHE_DIR = _output_root / "data" / "cache"
LOG_FILE = _output_root / "logs" / "swing_newsletter.log"

# Ensure directories exist
(_output_root / "data" / "state").mkdir(parents=True, exist_ok=True)
(_output_root / "logs").mkdir(exist_ok=True)
CACHE_DIR.mkdir(exist_ok=True)

# Timezone
ET = ZoneInfo("America/New_York")
CT = ZoneInfo("America/Chicago")

# Market indices for context
MARKET_INDICES = ["SPY", "QQQ", "IWM", "DIA"]
SECTOR_ETFS = {
    "XLK": "Technology",
    "XLF": "Financials",
    "XLE": "Energy",
    "XLV": "Healthcare",
    "XLI": "Industrials",
    "XLC": "Communications",
    "XLY": "Consumer Disc",
    "XLP": "Consumer Staples",
    "XLU": "Utilities",
    "XLB": "Materials",
    "XLRE": "Real Estate"
}

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


# ============================================================================
# LOAD ENVIRONMENT VARIABLES
# ============================================================================

def load_env_file(env_path: Path) -> None:
    """Load environment variables from file."""
    if env_path.exists():
        with open(env_path) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, val = line.split("=", 1)
                    os.environ.setdefault(key.strip(), val.strip())

load_env_file(ENV_FILE)
load_env_file(SCRIPT_DIR / ".env")


# ============================================================================
# DATA CLASSES
# ============================================================================

@dataclass
class MarketContext:
    """Overall market conditions."""
    spy_price: float = 0.0
    spy_change_pct: float = 0.0
    spy_vs_20ma: float = 0.0  # % above/below 20 MA
    spy_vs_50ma: float = 0.0
    spy_vs_200ma: float = 0.0
    spy_trend: str = "NEUTRAL"  # BULLISH, BEARISH, NEUTRAL

    qqq_price: float = 0.0
    qqq_change_pct: float = 0.0
    qqq_trend: str = "NEUTRAL"

    vix: float = 0.0
    vix_change_pct: float = 0.0
    vix_regime: str = "NORMAL"  # LOW (<15), NORMAL (15-25), ELEVATED (25-35), HIGH (>35)

    breadth_advance_pct: float = 0.0  # % of stocks above 50 MA (approx)

    sector_leaders: List[Tuple[str, str, float]] = field(default_factory=list)  # (ETF, Name, % change)
    sector_laggards: List[Tuple[str, str, float]] = field(default_factory=list)

    # Polymarket prediction market sentiment
    polymarket_risk_level: str = "N/A"  # LOW, MEDIUM, HIGH, EXTREME
    polymarket_recession_prob: Optional[float] = None
    polymarket_fed_dovish_prob: Optional[float] = None
    polymarket_fed_hawkish_prob: Optional[float] = None
    polymarket_market_bullish_prob: Optional[float] = None
    polymarket_key_markets: List[Tuple[str, float]] = field(default_factory=list)  # (question, yes_prob)


@dataclass
class SwingPosition:
    """Current swing trade position."""
    symbol: str
    shares: float
    avg_cost: float
    current_price: float = 0.0
    market_value: float = 0.0
    unrealized_pnl: float = 0.0
    unrealized_pnl_pct: float = 0.0
    days_held: int = 0
    entry_date: Optional[dt.date] = None

    # Key levels
    stop_loss: Optional[float] = None
    target_1: Optional[float] = None
    target_2: Optional[float] = None

    # Distance from levels
    distance_to_stop_pct: Optional[float] = None
    distance_to_target_pct: Optional[float] = None

    # Alerts
    near_stop: bool = False  # Within 3% of stop (but still above)
    stop_breached: bool = False  # Price has fallen BELOW stop loss
    near_target: bool = False  # Within 3% of target
    earnings_soon: bool = False  # Earnings within 5 days
    earnings_date: Optional[str] = None

    # Technical
    rsi: Optional[float] = None
    trend_status: str = "NEUTRAL"  # STRONG, WEAKENING, NEUTRAL

    # Attention score (higher = needs more attention)
    attention_score: float = 0.0
    attention_reasons: List[str] = field(default_factory=list)


@dataclass
class WatchlistSetup:
    """Potential swing trade setup."""
    symbol: str
    setup_type: str  # "bull_flag", "pullback_to_support", "breakout", "ema_bounce"
    current_price: float

    # Entry zone
    entry_low: float = 0.0
    entry_high: float = 0.0

    # Targets & stops
    stop_loss: float = 0.0
    target_1: float = 0.0
    target_2: float = 0.0
    risk_reward: float = 0.0

    # Technical context
    ema20: Optional[float] = None
    ema50: Optional[float] = None
    ema200: Optional[float] = None
    rsi: Optional[float] = None
    relative_strength: Optional[float] = None  # vs SPY
    volume_ratio: Optional[float] = None  # vs 20d avg

    # Triggers
    trigger_price: Optional[float] = None  # Price that confirms setup
    trigger_condition: str = ""  # "break above $X", "hold support at $X"

    # Score
    score: float = 0.0
    reasons: List[str] = field(default_factory=list)

    # News/sentiment
    news_sentiment: Optional[float] = None
    earnings_date: Optional[str] = None


@dataclass
class RiskDashboard:
    """Portfolio risk metrics."""
    total_equity: float = 0.0
    cash_available: float = 0.0
    positions_count: int = 0

    # Exposure
    gross_exposure: float = 0.0
    gross_exposure_pct: float = 0.0

    # Risk metrics
    total_risk_if_all_stops_hit: float = 0.0
    risk_pct_of_equity: float = 0.0

    # Correlation warning
    sector_concentration: Dict[str, float] = field(default_factory=dict)
    correlation_warning: bool = False
    correlation_message: str = ""

    # Earnings risk
    positions_with_earnings_soon: List[str] = field(default_factory=list)


@dataclass
class AIAnalysis:
    """AI-generated portfolio analysis and recommendations."""
    executive_summary: str = ""
    top_actions: List[str] = field(default_factory=list)  # Priority action items
    positions_to_cut: List[Tuple[str, str]] = field(default_factory=list)  # (symbol, reason)
    positions_to_trim: List[Tuple[str, str]] = field(default_factory=list)  # (symbol, reason)
    positions_to_hold: List[Tuple[str, str]] = field(default_factory=list)  # (symbol, reason)
    market_outlook: str = ""
    risk_warnings: List[str] = field(default_factory=list)
    position_notes: Dict[str, str] = field(default_factory=dict)  # symbol -> AI note


# ============================================================================
# AI ANALYSIS CLIENT (Claude with GPT fallback)
# ============================================================================

class AIAnalyzer:
    """Generate AI-powered portfolio analysis using Claude or GPT."""

    def __init__(self):
        self.anthropic_key = os.environ.get("ANTHROPIC_API_KEY", "")
        self.openai_key = os.environ.get("OPENAI_API_KEY", "")
        self.session = requests.Session()

    def _call_claude(self, prompt: str, max_tokens: int = 2000) -> Optional[str]:
        """Call Anthropic Claude API."""
        if not self.anthropic_key:
            return None

        try:
            resp = self.session.post(
                "https://api.anthropic.com/v1/messages",
                headers={
                    "x-api-key": self.anthropic_key,
                    "anthropic-version": "2023-06-01",
                    "content-type": "application/json"
                },
                json={
                    "model": "claude-sonnet-4-20250514",
                    "max_tokens": max_tokens,
                    "messages": [{"role": "user", "content": prompt}]
                },
                timeout=60
            )
            resp.raise_for_status()
            data = resp.json()
            return data.get("content", [{}])[0].get("text", "")
        except Exception as e:
            logger.warning(f"Claude API error: {e}")
            return None

    def _call_gpt(self, prompt: str, max_tokens: int = 2000) -> Optional[str]:
        """Call OpenAI GPT API as fallback."""
        if not self.openai_key:
            return None

        try:
            resp = self.session.post(
                "https://api.openai.com/v1/chat/completions",
                headers={
                    "Authorization": f"Bearer {self.openai_key}",
                    "Content-Type": "application/json"
                },
                json={
                    "model": "gpt-4o",
                    "max_tokens": max_tokens,
                    "messages": [{"role": "user", "content": prompt}]
                },
                timeout=60
            )
            resp.raise_for_status()
            data = resp.json()
            return data.get("choices", [{}])[0].get("message", {}).get("content", "")
        except Exception as e:
            logger.warning(f"GPT API error: {e}")
            return None

    def _call_llm(self, prompt: str) -> str:
        """Call Claude first, fallback to GPT."""
        result = self._call_claude(prompt)
        if result:
            logger.info("AI analysis generated using Claude")
            return result

        result = self._call_gpt(prompt)
        if result:
            logger.info("AI analysis generated using GPT (fallback)")
            return result

        logger.warning("No AI API available - skipping AI analysis")
        return ""

    def analyze_portfolio(
        self,
        market_ctx: 'MarketContext',
        positions: List['SwingPosition'],
        risk_dash: 'RiskDashboard'
    ) -> AIAnalysis:
        """Generate comprehensive AI analysis of the portfolio."""
        analysis = AIAnalysis()

        if not self.anthropic_key and not self.openai_key:
            logger.warning("No AI API keys configured - skipping AI analysis")
            return analysis

        # Build the analysis prompt
        prompt = self._build_portfolio_prompt(market_ctx, positions, risk_dash)
        response = self._call_llm(prompt)

        if response:
            analysis = self._parse_ai_response(response, positions)

        return analysis

    def _build_portfolio_prompt(
        self,
        market_ctx: 'MarketContext',
        positions: List['SwingPosition'],
        risk_dash: 'RiskDashboard'
    ) -> str:
        """Build the prompt for portfolio analysis."""

        # Summarize positions for the prompt
        position_summaries = []
        for p in positions[:30]:  # Limit to top 30 by attention score
            rsi_str = f"{p.rsi:.0f}" if p.rsi else "N/A"
            position_summaries.append(
                f"- {p.symbol}: {p.shares:.1f} shares @ ${p.avg_cost:.2f}, "
                f"current ${p.current_price:.2f}, P&L {p.unrealized_pnl_pct:+.1f}%, "
                f"held {p.days_held}d, RSI {rsi_str}, "
                f"trend {p.trend_status}, attention {p.attention_score:.0f}"
                f"{' [NEAR STOP]' if p.near_stop else ''}"
                f"{' [NEAR TARGET]' if p.near_target else ''}"
            )

        positions_text = "\n".join(position_summaries)

        # Count categories
        losers = [p for p in positions if p.unrealized_pnl_pct < -10]
        big_winners = [p for p in positions if p.unrealized_pnl_pct > 50]
        near_stops = [p for p in positions if p.near_stop]

        prompt = f"""You are an expert swing trading advisor. Analyze this portfolio and provide actionable recommendations.

## MARKET CONDITIONS
- SPY: ${market_ctx.spy_price:.2f} ({market_ctx.spy_change_pct:+.1f}%), Trend: {market_ctx.spy_trend}
- QQQ: ${market_ctx.qqq_price:.2f} ({market_ctx.qqq_change_pct:+.1f}%), Trend: {market_ctx.qqq_trend}
- VIXY: ${market_ctx.vix:.2f} ({market_ctx.vix_regime})
- Leading sectors: {', '.join(f"{name} ({pct:+.1f}%)" for _, name, pct in market_ctx.sector_leaders[:3])}
- Lagging sectors: {', '.join(f"{name} ({pct:+.1f}%)" for _, name, pct in market_ctx.sector_laggards[:3])}

## PREDICTION MARKET SENTIMENT (Polymarket - real-money betting)
- Overall Risk Level: {market_ctx.polymarket_risk_level}
- Fed Rate Cut Probability: {f"{market_ctx.polymarket_fed_dovish_prob*100:.1f}%" if market_ctx.polymarket_fed_dovish_prob else "N/A"}
- Fed Rate Hike Probability: {f"{market_ctx.polymarket_fed_hawkish_prob*100:.1f}%" if market_ctx.polymarket_fed_hawkish_prob else "N/A"}
- Recession Probability: {f"{market_ctx.polymarket_recession_prob*100:.1f}%" if market_ctx.polymarket_recession_prob else "N/A"}
- Market Bullish Probability: {f"{market_ctx.polymarket_market_bullish_prob*100:.1f}%" if market_ctx.polymarket_market_bullish_prob else "N/A"}
{f"- Key prediction markets: " + ", ".join(f"{q[:40]}... ({p*100:.0f}%)" for q, p in market_ctx.polymarket_key_markets[:3]) if market_ctx.polymarket_key_markets else ""}

## PORTFOLIO OVERVIEW
- Total positions: {len(positions)}
- Gross exposure: ${risk_dash.gross_exposure:,.0f} ({risk_dash.gross_exposure_pct:.1f}%)
- Portfolio heat (risk if stops hit): ${risk_dash.total_risk_if_all_stops_hit:,.0f} ({risk_dash.risk_pct_of_equity:.1f}%)
- Positions down >10%: {len(losers)}
- Positions up >50%: {len(big_winners)}
- Positions near stop loss: {len(near_stops)}

## TOP POSITIONS (by attention score - higher = needs action)
{positions_text}

## YOUR TASK
Provide a BALANCED analysis (not too conservative, not too aggressive). Focus on:
1. Risk management - which losers to cut
2. Profit taking - which winners to trim (but don't sell everything)
3. Position management - what to hold and why

Respond in this EXACT format (use these exact headers):

### EXECUTIVE SUMMARY
[2-3 sentence overview of portfolio health and market alignment]

### TOP 3 ACTIONS TODAY
1. [Most important action]
2. [Second action]
3. [Third action]

### POSITIONS TO CUT (sell entirely)
- [SYMBOL]: [brief reason]
- [SYMBOL]: [brief reason]
(or "None" if no positions need cutting)

### POSITIONS TO TRIM (sell partial)
- [SYMBOL]: [brief reason, suggest % to trim]
- [SYMBOL]: [brief reason, suggest % to trim]
(or "None" if no positions need trimming)

### POSITIONS TO HOLD
- [SYMBOL]: [brief reason why it's still a good hold]
- [SYMBOL]: [brief reason]
(List 3-5 best holds)

### MARKET OUTLOOK
[1-2 sentences on what the market conditions mean for swing trading today]

### RISK WARNINGS
- [Any specific risks to watch]
- [Concentration issues, correlation, etc.]
(or "None" if no major warnings)

Be specific with symbols. Be concise. Focus on actionable advice."""

        return prompt

    def _parse_ai_response(self, response: str, positions: List['SwingPosition']) -> AIAnalysis:
        """Parse the AI response into structured data."""
        analysis = AIAnalysis()

        # Extract sections using simple parsing
        sections = {
            "executive_summary": "",
            "top_actions": [],
            "positions_to_cut": [],
            "positions_to_trim": [],
            "positions_to_hold": [],
            "market_outlook": "",
            "risk_warnings": []
        }

        current_section = None
        lines = response.split("\n")

        for line in lines:
            line_lower = line.lower().strip()

            # Detect section headers
            if "executive summary" in line_lower:
                current_section = "executive_summary"
            elif "top 3 actions" in line_lower or "top actions" in line_lower:
                current_section = "top_actions"
            elif "positions to cut" in line_lower:
                current_section = "positions_to_cut"
            elif "positions to trim" in line_lower:
                current_section = "positions_to_trim"
            elif "positions to hold" in line_lower:
                current_section = "positions_to_hold"
            elif "market outlook" in line_lower:
                current_section = "market_outlook"
            elif "risk warnings" in line_lower or "risk warning" in line_lower:
                current_section = "risk_warnings"
            elif current_section and line.strip():
                # Add content to current section
                content = line.strip()
                if content.startswith("#"):
                    continue

                if current_section == "executive_summary":
                    sections["executive_summary"] += content + " "
                elif current_section == "market_outlook":
                    sections["market_outlook"] += content + " "
                elif current_section in ["top_actions", "risk_warnings"]:
                    if content.startswith(("-", "1", "2", "3", "*")):
                        # Clean up the line
                        clean = content.lstrip("-*0123456789. ")
                        if clean and clean.lower() != "none":
                            sections[current_section].append(clean)
                elif current_section in ["positions_to_cut", "positions_to_trim", "positions_to_hold"]:
                    if content.startswith(("-", "*")) and ":" in content:
                        # Extract symbol and reason
                        clean = content.lstrip("-* ")
                        if ":" in clean:
                            parts = clean.split(":", 1)
                            symbol = parts[0].strip().upper()
                            reason = parts[1].strip() if len(parts) > 1 else ""
                            if symbol and symbol.lower() != "none":
                                sections[current_section].append((symbol, reason))

        # Build the analysis object
        analysis.executive_summary = sections["executive_summary"].strip()
        analysis.top_actions = sections["top_actions"][:5]
        analysis.positions_to_cut = sections["positions_to_cut"][:10]
        analysis.positions_to_trim = sections["positions_to_trim"][:10]
        analysis.positions_to_hold = sections["positions_to_hold"][:10]
        analysis.market_outlook = sections["market_outlook"].strip()
        analysis.risk_warnings = sections["risk_warnings"][:5]

        # Build position notes dict for easy lookup
        for symbol, reason in analysis.positions_to_cut:
            analysis.position_notes[symbol] = f"CUT: {reason}"
        for symbol, reason in analysis.positions_to_trim:
            analysis.position_notes[symbol] = f"TRIM: {reason}"
        for symbol, reason in analysis.positions_to_hold:
            analysis.position_notes[symbol] = f"HOLD: {reason}"

        return analysis


# ============================================================================
# POLYGON API CLIENT
# ============================================================================

class PolygonClient:
    """Polygon.io API client with caching."""

    def __init__(self):
        self.api_key = os.environ.get("POLYGON_API_KEY", "")
        self.base_url = "https://api.polygon.io"
        self.session = requests.Session()

        # Retry strategy
        retry = Retry(total=3, backoff_factor=1.0, status_forcelist=[429, 500, 502, 503, 504])
        adapter = HTTPAdapter(max_retries=retry)
        self.session.mount("https://", adapter)

    def _cache_key(self, path: str, params: dict) -> str:
        content = f"{path}:{json.dumps(params, sort_keys=True)}"
        return hashlib.sha1(content.encode()).hexdigest()

    def _get_cached(self, cache_key: str, ttl_sec: int) -> Optional[dict]:
        cache_file = CACHE_DIR / f"{cache_key}.json"
        if cache_file.exists():
            age = dt.datetime.now().timestamp() - cache_file.stat().st_mtime
            if age < ttl_sec:
                try:
                    return json.loads(cache_file.read_text())
                except Exception:
                    pass
        return None

    def _set_cache(self, cache_key: str, data: dict) -> None:
        cache_file = CACHE_DIR / f"{cache_key}.json"
        try:
            cache_file.write_text(json.dumps(data))
        except Exception:
            pass

    def _get(self, path: str, params: Optional[dict] = None, cache_ttl_sec: int = 300, suppress_errors: bool = False) -> dict:
        params = params or {}
        params["apiKey"] = self.api_key

        # Check cache
        cache_key = self._cache_key(path, {k: v for k, v in params.items() if k != "apiKey"})
        if cache_ttl_sec > 0:
            cached = self._get_cached(cache_key, cache_ttl_sec)
            if cached:
                return cached

        url = f"{self.base_url}{path}"
        try:
            resp = self.session.get(url, params=params, timeout=15)
            resp.raise_for_status()
            data = resp.json()

            if cache_ttl_sec > 0:
                self._set_cache(cache_key, data)

            return data
        except Exception as e:
            if not suppress_errors:
                logger.error(f"Polygon API error: {path} - {e}")
            return {}

    def get_snapshot(self, symbol: str) -> dict:
        """Get current snapshot for a symbol."""
        return self._get(f"/v2/snapshot/locale/us/markets/stocks/tickers/{symbol}", cache_ttl_sec=60)

    def get_daily_bars(self, symbol: str, days: int = 252) -> pd.DataFrame:
        """Get daily OHLCV bars."""
        end_date = dt.date.today()
        start_date = end_date - dt.timedelta(days=days + 50)  # Extra for MA calculation

        data = self._get(
            f"/v2/aggs/ticker/{symbol}/range/1/day/{start_date}/{end_date}",
            params={"adjusted": "true", "sort": "asc", "limit": 1000},
            cache_ttl_sec=3600  # Cache for 1 hour
        )

        results = data.get("results", [])
        if not results:
            return pd.DataFrame()

        df = pd.DataFrame(results)
        df["date"] = pd.to_datetime(df["t"], unit="ms").dt.date
        df = df.rename(columns={"o": "open", "h": "high", "l": "low", "c": "close", "v": "volume"})
        return df[["date", "open", "high", "low", "close", "volume"]]

    def get_news(self, symbol: str, limit: int = 10) -> List[dict]:
        """Get recent news for a symbol."""
        data = self._get(
            "/v2/reference/news",
            params={"ticker": symbol, "limit": limit, "order": "desc"},
            cache_ttl_sec=1800
        )
        return data.get("results", [])

    def get_vixy(self) -> float:
        """
        Get current VIXY price as volatility indicator.

        Uses VIXY directly instead of trying to convert to VIX.
        VIXY thresholds (calibrated for VIXY price, not VIX):
        - VIXY < 20 = LOW volatility
        - VIXY 20-30 = NORMAL volatility
        - VIXY 30-40 = ELEVATED volatility
        - VIXY > 40 = HIGH volatility
        """
        # PRIMARY: Get VIXY directly from Polygon
        try:
            snapshot = self.get_snapshot("VIXY")
            if snapshot and "ticker" in snapshot:
                vixy = float(snapshot["ticker"].get("day", {}).get("c", 0) or
                            snapshot["ticker"].get("prevDay", {}).get("c", 0))
                if vixy > 0:
                    return vixy
        except Exception as e:
            logger.debug(f"Polygon VIXY fetch failed: {e}")

        # FALLBACK: Yahoo Finance VIXY
        try:
            import logging as _logging
            yf_logger = _logging.getLogger("yfinance")
            old_level = yf_logger.level
            yf_logger.setLevel(_logging.CRITICAL)
            try:
                df = yf.download("VIXY", period="5d", progress=False, auto_adjust=True)
                if not df.empty:
                    vixy_val = float(df["Close"].iloc[-1])
                    if vixy_val > 0:
                        return vixy_val
            finally:
                yf_logger.setLevel(old_level)
        except Exception as e:
            logger.debug(f"Yahoo Finance VIXY fetch failed: {e}")

        # Last resort default (mid-range VIXY)
        return 25.0

    # Alias for backward compatibility
    def get_vix(self) -> float:
        """Alias for get_vixy() - returns VIXY price directly."""
        return self.get_vixy()


# ============================================================================
# TECHNICAL ANALYSIS HELPERS
# ============================================================================

def ema(series: pd.Series, span: int) -> pd.Series:
    """Exponential moving average."""
    return series.ewm(span=span, adjust=False).mean()


def sma(series: pd.Series, window: int) -> pd.Series:
    """Simple moving average."""
    return series.rolling(window=window).mean()


def rsi(series: pd.Series, period: int = 14) -> pd.Series:
    """Relative Strength Index."""
    delta = series.diff()
    up = delta.clip(lower=0.0)
    down = (-delta).clip(lower=0.0)
    roll_up = up.ewm(alpha=1/period, adjust=False).mean()
    roll_down = down.ewm(alpha=1/period, adjust=False).mean()
    rs = roll_up / roll_down.replace(0, np.nan)
    return 100 - (100 / (1 + rs))


def atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    """Average True Range."""
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


def relative_strength(symbol_df: pd.DataFrame, benchmark_df: pd.DataFrame, period: int = 20) -> float:
    """Calculate relative strength vs benchmark over period."""
    if len(symbol_df) < period or len(benchmark_df) < period:
        return 0.0

    symbol_return = (symbol_df["close"].iloc[-1] / symbol_df["close"].iloc[-period] - 1) * 100
    bench_return = (benchmark_df["close"].iloc[-1] / benchmark_df["close"].iloc[-period] - 1) * 100

    return symbol_return - bench_return


# ============================================================================
# MARKET CONTEXT ANALYSIS
# ============================================================================

def analyze_market_context(polygon: PolygonClient) -> MarketContext:
    """Analyze overall market conditions."""
    ctx = MarketContext()

    # SPY analysis
    spy_df = polygon.get_daily_bars("SPY", days=252)
    if not spy_df.empty:
        spy_df["ema20"] = ema(spy_df["close"], 20)
        spy_df["ema50"] = ema(spy_df["close"], 50)
        spy_df["ema200"] = ema(spy_df["close"], 200)

        latest = spy_df.iloc[-1]
        prev = spy_df.iloc[-2] if len(spy_df) > 1 else latest

        ctx.spy_price = float(latest["close"])
        ctx.spy_change_pct = (latest["close"] / prev["close"] - 1) * 100
        ctx.spy_vs_20ma = (latest["close"] / latest["ema20"] - 1) * 100
        ctx.spy_vs_50ma = (latest["close"] / latest["ema50"] - 1) * 100
        ctx.spy_vs_200ma = (latest["close"] / latest["ema200"] - 1) * 100

        # Determine trend
        if latest["close"] > latest["ema20"] > latest["ema50"] > latest["ema200"]:
            ctx.spy_trend = "BULLISH"
        elif latest["close"] < latest["ema20"] < latest["ema50"] < latest["ema200"]:
            ctx.spy_trend = "BEARISH"
        elif latest["close"] > latest["ema200"]:
            ctx.spy_trend = "NEUTRAL_BULLISH"
        else:
            ctx.spy_trend = "NEUTRAL_BEARISH"

    # QQQ analysis
    qqq_df = polygon.get_daily_bars("QQQ", days=252)
    if not qqq_df.empty:
        qqq_df["ema20"] = ema(qqq_df["close"], 20)
        qqq_df["ema50"] = ema(qqq_df["close"], 50)
        qqq_df["ema200"] = ema(qqq_df["close"], 200)

        latest = qqq_df.iloc[-1]
        prev = qqq_df.iloc[-2] if len(qqq_df) > 1 else latest

        ctx.qqq_price = float(latest["close"])
        ctx.qqq_change_pct = (latest["close"] / prev["close"] - 1) * 100

        if latest["close"] > latest["ema20"] > latest["ema50"]:
            ctx.qqq_trend = "BULLISH"
        elif latest["close"] < latest["ema20"] < latest["ema50"]:
            ctx.qqq_trend = "BEARISH"
        else:
            ctx.qqq_trend = "NEUTRAL"

    # VIXY analysis (using VIXY price directly, not converted to VIX)
    # VIXY thresholds are calibrated for VIXY price:
    #   VIXY < 20 = LOW volatility
    #   VIXY 20-30 = NORMAL volatility
    #   VIXY 30-40 = ELEVATED volatility
    #   VIXY > 40 = HIGH volatility
    ctx.vix = polygon.get_vixy()  # Note: field still named 'vix' for compatibility
    if ctx.vix < 20:
        ctx.vix_regime = "LOW"
    elif ctx.vix < 30:
        ctx.vix_regime = "NORMAL"
    elif ctx.vix < 40:
        ctx.vix_regime = "ELEVATED"
    else:
        ctx.vix_regime = "HIGH"

    # Sector rotation
    sector_performance = []
    for etf, name in SECTOR_ETFS.items():
        df = polygon.get_daily_bars(etf, days=30)
        if not df.empty and len(df) >= 5:
            # 5-day performance
            perf = (df["close"].iloc[-1] / df["close"].iloc[-5] - 1) * 100
            sector_performance.append((etf, name, perf))

    sector_performance.sort(key=lambda x: x[2], reverse=True)
    ctx.sector_leaders = sector_performance[:3]
    ctx.sector_laggards = sector_performance[-3:]

    # Polymarket prediction market sentiment
    try:
        from polymarket_client import PolymarketClient
        poly_client = PolymarketClient()
        sentiment = poly_client.get_market_sentiment()

        ctx.polymarket_risk_level = sentiment.overall_risk_level
        ctx.polymarket_recession_prob = sentiment.recession_prob
        ctx.polymarket_fed_dovish_prob = sentiment.fed_dovish_prob
        ctx.polymarket_fed_hawkish_prob = sentiment.fed_hawkish_prob
        ctx.polymarket_market_bullish_prob = sentiment.market_bullish_prob

        # Store top 5 key markets for display
        ctx.polymarket_key_markets = [
            (m.question[:60], m.yes_price)
            for m in sentiment.raw_markets[:5]
        ]
        logger.info(f"Polymarket sentiment: {ctx.polymarket_risk_level} risk level")
    except Exception as e:
        logger.warning(f"Failed to fetch Polymarket sentiment: {e}")
        ctx.polymarket_risk_level = "N/A"

    return ctx


# ============================================================================
# POSITION LOADING
# ============================================================================

def load_positions_from_robinhood_csv(file_path: Path) -> List[SwingPosition]:
    """Load positions from Robinhood activity CSV.

    Robinhood CSV columns: Activity Date, Process Date, Settle Date, Instrument,
    Description, Trans Code, Quantity, Price, Amount

    Trans Code values: Buy, Sell, CDIV (cash dividend), MDIV (manufactured div), etc.
    """
    if not file_path.exists():
        logger.warning(f"Robinhood CSV not found: {file_path}")
        return []

    try:
        df = pd.read_csv(file_path)
    except Exception as e:
        logger.error(f"Failed to read Robinhood CSV: {e}")
        return []

    # Robinhood specific columns
    holdings: Dict[str, Dict] = {}

    # Expected Robinhood columns
    symbol_col = "Instrument"
    trans_col = "Trans Code"
    qty_col = "Quantity"
    price_col = "Price"
    date_col = "Activity Date"

    # Verify columns exist
    required_cols = [symbol_col, trans_col, qty_col, price_col]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        logger.error(f"Missing columns in Robinhood CSV: {missing}. Found: {list(df.columns)}")
        return []

    # Sort by date (oldest first) - critical for correct cost basis calculation
    # Robinhood exports newest-first, but we need to process buys before sells
    if date_col in df.columns:
        df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
        df = df.sort_values(date_col, ascending=True)

    for _, row in df.iterrows():
        symbol = str(row[symbol_col]).strip().upper()
        if not symbol or symbol == "NAN" or pd.isna(row[symbol_col]):
            continue

        trans_code = str(row[trans_col]).strip().lower() if pd.notna(row[trans_col]) else ""

        # Process Buy, Sell, and SPL (stock split) transactions
        if trans_code not in ["buy", "sell", "spl"]:
            continue

        try:
            qty = float(row[qty_col]) if pd.notna(row[qty_col]) else 0
            # Price might have $ and commas
            price_str = str(row[price_col]).replace("$", "").replace(",", "").strip()
            price = float(price_str) if price_str and price_str != "nan" else 0
        except (ValueError, TypeError):
            continue

        if qty <= 0:
            continue

        # For splits, price is empty but we still need to process
        if trans_code != "spl" and price <= 0:
            continue

        if symbol not in holdings:
            holdings[symbol] = {"shares": 0.0, "cost": 0.0, "first_date": None}

        if trans_code == "buy":
            holdings[symbol]["shares"] += qty
            holdings[symbol]["cost"] += qty * price
            if date_col in df.columns and holdings[symbol]["first_date"] is None:
                try:
                    holdings[symbol]["first_date"] = pd.to_datetime(row[date_col]).date()
                except Exception:
                    pass
        elif trans_code == "sell":
            # Reduce cost basis proportionally before reducing shares
            if holdings[symbol]["shares"] > 0:
                avg = holdings[symbol]["cost"] / holdings[symbol]["shares"]
                holdings[symbol]["shares"] -= qty
                holdings[symbol]["cost"] = avg * max(holdings[symbol]["shares"], 0)
            else:
                holdings[symbol]["shares"] -= qty
        elif trans_code == "spl":
            # Stock split - add shares without changing cost basis
            # This keeps total cost the same but increases share count
            # resulting in lower average cost per share
            holdings[symbol]["shares"] += qty
            logger.info(f"Stock split: {symbol} received {qty} additional shares")

    # Convert to SwingPosition objects
    positions = []
    for symbol, data in holdings.items():
        if data["shares"] > 0.01:  # Skip closed positions
            avg_cost = data["cost"] / data["shares"] if data["shares"] > 0 else 0
            pos = SwingPosition(
                symbol=symbol,
                shares=data["shares"],
                avg_cost=avg_cost,
                entry_date=data["first_date"]
            )
            if pos.entry_date:
                pos.days_held = (dt.date.today() - pos.entry_date).days
            positions.append(pos)

    logger.info(f"Parsed {len(positions)} positions from Robinhood CSV")
    return positions


def calculate_attention_score(pos: SwingPosition) -> Tuple[float, List[str]]:
    """
    Calculate attention score for a position. Higher score = needs more attention.

    Score ranges:
    - 80-100: CRITICAL - Immediate action needed (stop breached, huge loss)
    - 60-79: HIGH - Review today
    - 40-59: MODERATE - Monitor closely
    - 0-39: LOW - Position is healthy
    """
    score = 0.0
    reasons = []

    # CRITICAL: Stop loss BREACHED (price fell below stop)
    if pos.stop_breached:
        score += 50
        breach_pct = abs(pos.distance_to_stop_pct) if pos.distance_to_stop_pct else 0
        reasons.append(f"STOP BREACHED ({breach_pct:.1f}% below)")

    # HIGH: Near stop loss (within 3% but still above)
    elif pos.near_stop:
        score += 35
        reasons.append("Near stop loss")

    # HIGH: Earnings soon
    if pos.earnings_soon:
        score += 30
        reasons.append(f"Earnings {pos.earnings_date or 'soon'}")

    # HIGH: Weakening trend (price below EMA20)
    if pos.trend_status == "WEAKENING":
        score += 20
        reasons.append("Trend weakening")

    # MODERATE: Large unrealized loss (> 8%)
    if pos.unrealized_pnl_pct < -20:
        score += 25
        reasons.append(f"Down {pos.unrealized_pnl_pct:.1f}%")
    elif pos.unrealized_pnl_pct < -8:
        score += 15
        reasons.append(f"Down {pos.unrealized_pnl_pct:.1f}%")
    elif pos.unrealized_pnl_pct < -5:
        score += 10
        reasons.append(f"Down {pos.unrealized_pnl_pct:.1f}%")

    # MODERATE: Extended hold without progress (> 15 days and < 5% gain)
    if pos.days_held > 15 and pos.unrealized_pnl_pct < 5:
        score += 10
        reasons.append(f"Stale ({pos.days_held}d)")

    # MODERATE: Near target (profit taking opportunity)
    if pos.near_target:
        score += 15
        reasons.append("Near profit target")

    # LOW: Large unrealized gain (consider trimming)
    if pos.unrealized_pnl_pct > 50:
        score += 15
        reasons.append(f"Up {pos.unrealized_pnl_pct:.1f}% - consider trim")
    elif pos.unrealized_pnl_pct > 20:
        score += 10
        reasons.append(f"Up {pos.unrealized_pnl_pct:.1f}% - consider trim")

    # RSI extremes
    if pos.rsi is not None:
        if pos.rsi > 75:
            score += 5
            reasons.append(f"RSI overbought ({pos.rsi:.0f})")
        elif pos.rsi < 30:
            score += 5
            reasons.append(f"RSI oversold ({pos.rsi:.0f})")

    return min(score, 100), reasons


def enrich_positions(positions: List[SwingPosition], polygon: PolygonClient) -> List[SwingPosition]:
    """Add current prices, P&L, and technical data to positions."""
    for pos in positions:
        # Get current price
        snapshot = polygon.get_snapshot(pos.symbol)
        if snapshot and "ticker" in snapshot:
            ticker_data = snapshot["ticker"]
            pos.current_price = float(ticker_data.get("day", {}).get("c", 0) or
                                     ticker_data.get("prevDay", {}).get("c", 0) or pos.avg_cost)
        else:
            pos.current_price = pos.avg_cost  # Fallback

        # Calculate P&L
        pos.market_value = pos.shares * pos.current_price
        pos.unrealized_pnl = pos.market_value - (pos.shares * pos.avg_cost)
        pos.unrealized_pnl_pct = (pos.current_price / pos.avg_cost - 1) * 100 if pos.avg_cost > 0 else 0

        # Get technical data
        df = polygon.get_daily_bars(pos.symbol, days=60)
        if not df.empty:
            df["rsi"] = rsi(df["close"])
            df["atr"] = atr(df)
            df["ema20"] = ema(df["close"], 20)

            latest = df.iloc[-1]
            pos.rsi = float(latest["rsi"]) if pd.notna(latest["rsi"]) else None

            atr_val = float(latest["atr"]) if pd.notna(latest["atr"]) else pos.current_price * 0.02

            # Calculate entry-based stop first
            entry_based_stop = max(pos.avg_cost - 2 * atr_val, pos.avg_cost * 0.92)

            # Check if entry-based stop makes sense relative to current price
            # If stop is ABOVE current price OR more than 15% below current price,
            # the entry is stale/split-adjusted - use current price as basis instead
            stop_vs_current_pct = (pos.current_price / entry_based_stop - 1) * 100 if entry_based_stop > 0 else 0

            # stop_vs_current_pct: positive = current price above stop (normal)
            #                      negative = current price below stop (breached)
            # If current price is below stop (negative) OR stop is >15% below current,
            # the entry-based stop is useless - recalculate from current price
            use_current_price_basis = stop_vs_current_pct < 0 or stop_vs_current_pct > 15

            if use_current_price_basis:
                # Entry-based stop doesn't make sense - use current price as basis
                pos.stop_loss = pos.current_price * 0.92  # 8% below current
                pos.target_1 = pos.current_price * 1.12   # 12% above current
                pos.target_2 = pos.current_price * 1.20   # 20% above current
            else:
                # Normal calculation: use entry-based stop
                pos.stop_loss = entry_based_stop

                # Estimate targets (1.5R and 2.5R)
                risk = pos.avg_cost - pos.stop_loss
                pos.target_1 = pos.avg_cost + 1.5 * risk
                pos.target_2 = pos.avg_cost + 2.5 * risk

            # Distance calculations
            # distance_to_stop_pct: positive = above stop, negative = below stop (breached)
            if pos.stop_loss and pos.stop_loss > 0:
                pos.distance_to_stop_pct = (pos.current_price / pos.stop_loss - 1) * 100

                # STOP BREACHED: price has fallen below stop loss
                if pos.distance_to_stop_pct < 0:
                    pos.stop_breached = True
                    pos.near_stop = False
                # NEAR STOP: price is above stop but within 3%
                elif pos.distance_to_stop_pct < 3:
                    pos.near_stop = True
                    pos.stop_breached = False
                else:
                    pos.near_stop = False
                    pos.stop_breached = False

            if pos.target_1 and pos.current_price > 0:
                pos.distance_to_target_pct = (pos.target_1 / pos.current_price - 1) * 100
                # NEAR TARGET: within 3% of target (target is above current price)
                pos.near_target = 0 <= pos.distance_to_target_pct < 3

            # Trend status based on price vs EMAs
            if pos.current_price > float(latest["ema20"]) and pos.unrealized_pnl_pct > 5:
                pos.trend_status = "STRONG"
            elif pos.current_price < float(latest["ema20"]):
                pos.trend_status = "WEAKENING"

        # Calculate attention score for this position
        pos.attention_score, pos.attention_reasons = calculate_attention_score(pos)

    # Sort by attention score (highest first)
    positions.sort(key=lambda p: p.attention_score, reverse=True)

    return positions


# ============================================================================
# WATCHLIST SCANNING
# ============================================================================

def load_watchlist(file_path: Path) -> List[str]:
    """Load watchlist from file."""
    if not file_path.exists():
        return []

    symbols = []
    with open(file_path) as f:
        for line in f:
            line = line.strip().upper()
            if line and not line.startswith("#"):
                symbols.append(line.split()[0])  # Handle "AAPL # comment" format

    return symbols


def scan_for_setups(watchlist: List[str], polygon: PolygonClient, spy_df: pd.DataFrame) -> List[WatchlistSetup]:
    """Scan watchlist for swing trade setups."""
    setups = []

    for symbol in watchlist:
        try:
            df = polygon.get_daily_bars(symbol, days=60).copy()
            if df.empty or len(df) < 20:
                continue

            # Calculate indicators
            df["ema9"] = ema(df["close"], 9)
            df["ema20"] = ema(df["close"], 20)
            df["ema50"] = ema(df["close"], 50)
            df["ema200"] = ema(df["close"], 200) if len(df) >= 200 else df["close"]
            df["rsi"] = rsi(df["close"])
            df["atr"] = atr(df)
            df["vol_sma"] = sma(df["volume"], 20)

            latest = df.iloc[-1]
            prev = df.iloc[-2]

            price = float(latest["close"])
            ema20_val = float(latest["ema20"])
            ema50_val = float(latest["ema50"])
            rsi_val = float(latest["rsi"]) if pd.notna(latest["rsi"]) else 50
            atr_val = float(latest["atr"]) if pd.notna(latest["atr"]) else price * 0.02
            vol_ratio = float(latest["volume"] / latest["vol_sma"]) if latest["vol_sma"] > 0 else 1

            # Calculate relative strength vs SPY
            rs = relative_strength(df, spy_df)

            setup = None

            # 1. PULLBACK TO EMA20 (in uptrend)
            if (price > ema50_val and  # Above 50 EMA (uptrend)
                abs(price / ema20_val - 1) < 0.02 and  # Within 2% of EMA20
                rsi_val < 45 and  # RSI pulled back
                rs > 0):  # Outperforming SPY

                setup = WatchlistSetup(
                    symbol=symbol,
                    setup_type="pullback_to_ema20",
                    current_price=price,
                    entry_low=ema20_val * 0.99,
                    entry_high=ema20_val * 1.01,
                    stop_loss=ema20_val - 2 * atr_val,
                    target_1=price + 1.5 * (price - (ema20_val - 2 * atr_val)),
                    target_2=price + 2.5 * (price - (ema20_val - 2 * atr_val)),
                    trigger_condition=f"Hold above ${ema20_val:.2f} (EMA20)",
                    reasons=["Pullback to rising EMA20", f"RSI at {rsi_val:.0f} (oversold bounce)", f"RS vs SPY: +{rs:.1f}%"]
                )

            # 2. BULL FLAG (consolidation after strong move)
            elif len(df) >= 10:
                # Check for recent strong move (>5% in last 10 days) followed by tight consolidation
                move_start = df.iloc[-10]["close"]
                move_high = df.iloc[-5:]["high"].max()
                move_low = df.iloc[-5:]["low"].min()
                consolidation_range = (move_high - move_low) / price

                if (price / move_start > 1.05 and  # 5%+ move up
                    consolidation_range < 0.05 and  # Tight 5% consolidation
                    price > ema20_val):  # Still above EMA20

                    setup = WatchlistSetup(
                        symbol=symbol,
                        setup_type="bull_flag",
                        current_price=price,
                        entry_low=move_high,
                        entry_high=move_high * 1.01,
                        stop_loss=move_low - atr_val,
                        target_1=move_high + (move_high - move_start),  # Measured move
                        target_2=move_high + 1.5 * (move_high - move_start),
                        trigger_price=move_high,
                        trigger_condition=f"Break above ${move_high:.2f}",
                        reasons=["Bull flag pattern", f"Consolidation range: {consolidation_range*100:.1f}%", "Waiting for breakout"]
                    )

            # 3. OVERSOLD BOUNCE (RSI < 30 in uptrend)
            if (rsi_val < 35 and
                price > float(latest.get("ema200", price * 0.9)) and  # Still above 200 EMA
                vol_ratio > 1.2):  # Volume picking up

                setup = WatchlistSetup(
                    symbol=symbol,
                    setup_type="oversold_bounce",
                    current_price=price,
                    entry_low=price * 0.99,
                    entry_high=price * 1.01,
                    stop_loss=price - 2 * atr_val,
                    target_1=ema20_val,
                    target_2=ema50_val,
                    trigger_condition="RSI reversal with volume",
                    reasons=[f"RSI extremely oversold at {rsi_val:.0f}", f"Volume {vol_ratio:.1f}x average", "Above 200 EMA (long-term uptrend)"]
                )

            if setup:
                # Calculate risk/reward
                risk = setup.current_price - setup.stop_loss
                reward = setup.target_1 - setup.current_price
                setup.risk_reward = reward / risk if risk > 0 else 0

                # Add common data
                setup.ema20 = ema20_val
                setup.ema50 = ema50_val
                setup.rsi = rsi_val
                setup.relative_strength = rs
                setup.volume_ratio = vol_ratio

                # Score the setup
                setup.score = calculate_setup_score(setup)

                if setup.score >= 60 and setup.risk_reward >= 1.5:
                    setups.append(setup)

        except Exception as e:
            logger.warning(f"Error scanning {symbol}: {e}")
            continue

    # Sort by score
    setups.sort(key=lambda x: x.score, reverse=True)
    return setups[:10]  # Top 10 setups


def calculate_setup_score(setup: WatchlistSetup) -> float:
    """Score a setup from 0-100."""
    score = 50  # Base score

    # Risk/reward bonus
    if setup.risk_reward >= 3:
        score += 20
    elif setup.risk_reward >= 2:
        score += 15
    elif setup.risk_reward >= 1.5:
        score += 10

    # RSI bonus (prefer 30-45 for long setups)
    if setup.rsi:
        if 30 <= setup.rsi <= 45:
            score += 15
        elif setup.rsi < 30:
            score += 10  # Very oversold, might be falling knife

    # Relative strength bonus
    if setup.relative_strength and setup.relative_strength > 5:
        score += 10
    elif setup.relative_strength and setup.relative_strength > 0:
        score += 5

    # Volume confirmation
    if setup.volume_ratio and setup.volume_ratio > 1.5:
        score += 10

    return min(score, 100)


# ============================================================================
# RISK DASHBOARD
# ============================================================================

def calculate_risk_dashboard(positions: List[SwingPosition], total_equity: float, cash: float) -> RiskDashboard:
    """Calculate portfolio risk metrics."""
    dashboard = RiskDashboard(
        total_equity=total_equity,
        cash_available=cash,
        positions_count=len(positions)
    )

    # Gross exposure
    dashboard.gross_exposure = sum(p.market_value for p in positions)
    dashboard.gross_exposure_pct = (dashboard.gross_exposure / total_equity * 100) if total_equity > 0 else 0

    # Total risk if all stops hit
    total_risk = 0
    for pos in positions:
        if pos.stop_loss and pos.current_price > 0:
            risk_per_share = pos.current_price - pos.stop_loss
            total_risk += risk_per_share * pos.shares

    dashboard.total_risk_if_all_stops_hit = total_risk
    dashboard.risk_pct_of_equity = (total_risk / total_equity * 100) if total_equity > 0 else 0

    # Positions with earnings soon (placeholder - would need earnings calendar API)
    # For now, just flag it as a feature to add

    return dashboard


# ============================================================================
# HTML EMAIL GENERATION
# ============================================================================

def format_money(x: float, decimals: int = 0) -> str:
    """Format as money."""
    if x >= 0:
        return f"${x:,.{decimals}f}"
    return f"-${abs(x):,.{decimals}f}"


def format_pct(x: float, decimals: int = 1) -> str:
    """Format as percentage."""
    return f"{x:+.{decimals}f}%"


def trend_badge(trend: str) -> str:
    """Generate HTML badge for trend."""
    colors = {
        "BULLISH": "#22c55e",
        "NEUTRAL_BULLISH": "#84cc16",
        "NEUTRAL": "#6b7280",
        "NEUTRAL_BEARISH": "#f59e0b",
        "BEARISH": "#ef4444",
        "STRONG": "#22c55e",
        "WEAKENING": "#f59e0b",
        "LOW": "#22c55e",
        "NORMAL": "#6b7280",
        "ELEVATED": "#f59e0b",
        "HIGH": "#ef4444"
    }
    color = colors.get(trend, "#6b7280")
    return f'<span style="background:{color};color:white;padding:2px 8px;border-radius:4px;font-size:12px;">{trend}</span>'


def generate_html_email(
    market_ctx: MarketContext,
    positions: List[SwingPosition],
    setups: List[WatchlistSetup],
    risk_dash: RiskDashboard,
    date_str: str,
    ai_analysis: Optional[AIAnalysis] = None
) -> str:
    """Generate the HTML newsletter."""

    # Build AI Analysis section if available
    ai_section = ""
    if ai_analysis and ai_analysis.executive_summary:
        ai_section = _render_ai_analysis(ai_analysis)

    html = f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Swing Trading Newsletter - {date_str}</title>
</head>
<body style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; max-width: 700px; margin: 0 auto; padding: 20px; background: #f8fafc; color: #1e293b;">

    <div style="background: linear-gradient(135deg, #1e3a8a, #1e40af); color: white; padding: 20px; border-radius: 12px; margin-bottom: 20px;">
        <h1 style="margin: 0 0 5px 0; font-size: 24px;">Swing Trading Newsletter</h1>
        <p style="margin: 0; opacity: 0.9; font-size: 14px;">{date_str} - Pre-Market Briefing</p>
    </div>

    <!-- MARKET CONTEXT -->
    <div style="background: white; padding: 20px; border-radius: 12px; margin-bottom: 20px; box-shadow: 0 1px 3px rgba(0,0,0,0.1);">
        <h2 style="margin: 0 0 15px 0; font-size: 18px; border-bottom: 2px solid #3b82f6; padding-bottom: 10px;">Market Context</h2>

        <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 15px; margin-bottom: 15px;">
            <div style="background: #f1f5f9; padding: 12px; border-radius: 8px;">
                <div style="font-size: 12px; color: #64748b; margin-bottom: 4px;">SPY</div>
                <div style="font-size: 20px; font-weight: 600;">{format_money(market_ctx.spy_price, 2)}</div>
                <div style="font-size: 14px; color: {'#22c55e' if market_ctx.spy_change_pct >= 0 else '#ef4444'};">{format_pct(market_ctx.spy_change_pct)}</div>
                <div style="margin-top: 8px;">{trend_badge(market_ctx.spy_trend)}</div>
            </div>
            <div style="background: #f1f5f9; padding: 12px; border-radius: 8px;">
                <div style="font-size: 12px; color: #64748b; margin-bottom: 4px;">QQQ</div>
                <div style="font-size: 20px; font-weight: 600;">{format_money(market_ctx.qqq_price, 2)}</div>
                <div style="font-size: 14px; color: {'#22c55e' if market_ctx.qqq_change_pct >= 0 else '#ef4444'};">{format_pct(market_ctx.qqq_change_pct)}</div>
                <div style="margin-top: 8px;">{trend_badge(market_ctx.qqq_trend)}</div>
            </div>
        </div>

        <div style="background: #f1f5f9; padding: 12px; border-radius: 8px; margin-bottom: 15px;">
            <div style="display: flex; justify-content: space-between; align-items: center;">
                <div>
                    <div style="font-size: 12px; color: #64748b;">VIXY (Volatility)</div>
                    <div style="font-size: 20px; font-weight: 600;">{market_ctx.vix:.1f}</div>
                </div>
                <div>{trend_badge(market_ctx.vix_regime)}</div>
            </div>
        </div>

        <div style="font-size: 13px; color: #475569; margin-bottom: 10px;">
            <strong>SPY vs Moving Averages:</strong>
            20 MA: {format_pct(market_ctx.spy_vs_20ma)} |
            50 MA: {format_pct(market_ctx.spy_vs_50ma)} |
            200 MA: {format_pct(market_ctx.spy_vs_200ma)}
        </div>

        <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 10px; font-size: 13px;">
            <div>
                <div style="color: #22c55e; font-weight: 600; margin-bottom: 5px;">Leading Sectors</div>
                {"".join(f'<div>{name}: {format_pct(pct)}</div>' for etf, name, pct in market_ctx.sector_leaders)}
            </div>
            <div>
                <div style="color: #ef4444; font-weight: 600; margin-bottom: 5px;">Lagging Sectors</div>
                {"".join(f'<div>{name}: {format_pct(pct)}</div>' for etf, name, pct in market_ctx.sector_laggards)}
            </div>
        </div>
    </div>

    <!-- PREDICTION MARKET SENTIMENT -->
    {_render_polymarket_section(market_ctx) if market_ctx.polymarket_risk_level != "N/A" else ""}

    {ai_section}

    <!-- PORTFOLIO REVIEW -->
    <div style="background: white; padding: 20px; border-radius: 12px; margin-bottom: 20px; box-shadow: 0 1px 3px rgba(0,0,0,0.1);">
        <h2 style="margin: 0 0 15px 0; font-size: 18px; border-bottom: 2px solid #3b82f6; padding-bottom: 10px;">Portfolio Review ({len(positions)} Positions)</h2>

        {"".join(_render_position(p, ai_analysis.position_notes.get(p.symbol, "") if ai_analysis else "") for p in positions) if positions else '<p style="color: #64748b; text-align: center; padding: 20px;">No open positions</p>'}
    </div>

    <!-- WATCHLIST SETUPS -->
    <div style="background: white; padding: 20px; border-radius: 12px; margin-bottom: 20px; box-shadow: 0 1px 3px rgba(0,0,0,0.1);">
        <h2 style="margin: 0 0 15px 0; font-size: 18px; border-bottom: 2px solid #3b82f6; padding-bottom: 10px;">Watchlist Setups ({len(setups)} Found)</h2>

        {"".join(_render_setup(s) for s in setups) if setups else '<p style="color: #64748b; text-align: center; padding: 20px;">No setups meeting criteria today</p>'}
    </div>

    <!-- RISK DASHBOARD -->
    <div style="background: white; padding: 20px; border-radius: 12px; margin-bottom: 20px; box-shadow: 0 1px 3px rgba(0,0,0,0.1);">
        <h2 style="margin: 0 0 15px 0; font-size: 18px; border-bottom: 2px solid #3b82f6; padding-bottom: 10px;">Risk Dashboard</h2>

        <div style="display: grid; grid-template-columns: repeat(2, 1fr); gap: 10px; font-size: 14px;">
            <div style="background: #f1f5f9; padding: 10px; border-radius: 6px;">
                <div style="color: #64748b; font-size: 12px;">Gross Exposure</div>
                <div style="font-weight: 600;">{format_money(risk_dash.gross_exposure)} ({risk_dash.gross_exposure_pct:.1f}%)</div>
            </div>
            <div style="background: #f1f5f9; padding: 10px; border-radius: 6px;">
                <div style="color: #64748b; font-size: 12px;">Cash Available</div>
                <div style="font-weight: 600;">{format_money(risk_dash.cash_available)}</div>
            </div>
            <div style="background: {'#fef2f2' if risk_dash.risk_pct_of_equity > 5 else '#f1f5f9'}; padding: 10px; border-radius: 6px;">
                <div style="color: #64748b; font-size: 12px;">Portfolio Heat</div>
                <div style="font-weight: 600; color: {'#ef4444' if risk_dash.risk_pct_of_equity > 5 else 'inherit'};">
                    {format_money(risk_dash.total_risk_if_all_stops_hit)} ({risk_dash.risk_pct_of_equity:.1f}%)
                </div>
            </div>
            <div style="background: #f1f5f9; padding: 10px; border-radius: 6px;">
                <div style="color: #64748b; font-size: 12px;">Open Positions</div>
                <div style="font-weight: 600;">{risk_dash.positions_count}</div>
            </div>
        </div>

        {f'<div style="margin-top: 15px; padding: 10px; background: #fef3c7; border-radius: 6px; font-size: 13px;"><strong>Warning:</strong> {risk_dash.correlation_message}</div>' if risk_dash.correlation_warning else ''}
    </div>

    <div style="text-align: center; font-size: 12px; color: #94a3b8; padding: 20px;">
        Generated at {dt.datetime.now(ET).strftime('%I:%M %p ET')} | Swing Trading Newsletter
    </div>

</body>
</html>
"""
    return html


def _format_shares(shares: float) -> str:
    """Format share count - show decimals for fractional shares."""
    if shares >= 1:
        return f"{shares:.0f}"
    elif shares >= 0.01:
        return f"{shares:.2f}"
    else:
        return f"{shares:.4f}"


def _render_polymarket_section(ctx: MarketContext) -> str:
    """Render the Polymarket prediction market sentiment section."""

    # Risk level color coding
    risk_colors = {
        "LOW": "#22c55e",      # Green
        "MEDIUM": "#f59e0b",   # Yellow/Amber
        "HIGH": "#ef4444",     # Red
        "EXTREME": "#7c2d12",  # Dark red
    }
    risk_color = risk_colors.get(ctx.polymarket_risk_level, "#6b7280")

    # Format probability values
    def fmt_prob(val):
        if val is None:
            return "N/A"
        return f"{val*100:.1f}%"

    # Build key markets list
    markets_html = ""
    if ctx.polymarket_key_markets:
        markets_html = '<div style="margin-top: 10px; font-size: 12px; color: #64748b;"><strong>Key Markets:</strong><ul style="margin: 5px 0 0 0; padding-left: 16px;">'
        for question, prob in ctx.polymarket_key_markets[:4]:
            markets_html += f'<li>{question}... <strong>{prob*100:.1f}%</strong></li>'
        markets_html += '</ul></div>'

    return f"""
    <div style="background: linear-gradient(135deg, #fefce8, #fef3c7); padding: 20px; border-radius: 12px; margin-bottom: 20px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); border: 1px solid #fde047;">
        <h2 style="margin: 0 0 15px 0; font-size: 18px; border-bottom: 2px solid #eab308; padding-bottom: 10px; display: flex; align-items: center; flex-wrap: wrap; gap: 8px;">
            <span>Prediction Market Sentiment</span>
            <span style="font-size: 11px; background: #eab308; color: white; padding: 2px 8px; border-radius: 4px; white-space: nowrap;">Polymarket</span>
        </h2>

        <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 15px;">
            <div>
                <div style="font-size: 12px; color: #64748b;">Overall Risk Level</div>
                <div style="font-size: 24px; font-weight: 700; color: {risk_color};">{ctx.polymarket_risk_level}</div>
            </div>
            <div style="background: {risk_color}; color: white; padding: 8px 16px; border-radius: 8px; font-weight: 600;">
                {"Caution" if ctx.polymarket_risk_level in ["HIGH", "EXTREME"] else "Normal" if ctx.polymarket_risk_level == "MEDIUM" else "Clear"}
            </div>
        </div>

        <div style="display: grid; grid-template-columns: repeat(2, 1fr); gap: 10px; font-size: 13px;">
            <div style="background: white; padding: 10px; border-radius: 6px;">
                <div style="color: #64748b; font-size: 11px;">Fed Rate Cut</div>
                <div style="font-weight: 600;">{fmt_prob(ctx.polymarket_fed_dovish_prob)}</div>
            </div>
            <div style="background: white; padding: 10px; border-radius: 6px;">
                <div style="color: #64748b; font-size: 11px;">Fed Rate Hike</div>
                <div style="font-weight: 600;">{fmt_prob(ctx.polymarket_fed_hawkish_prob)}</div>
            </div>
            <div style="background: white; padding: 10px; border-radius: 6px;">
                <div style="color: #64748b; font-size: 11px;">Recession Prob</div>
                <div style="font-weight: 600; color: {'#ef4444' if ctx.polymarket_recession_prob and ctx.polymarket_recession_prob > 0.3 else 'inherit'};">
                    {fmt_prob(ctx.polymarket_recession_prob)}
                </div>
            </div>
            <div style="background: white; padding: 10px; border-radius: 6px;">
                <div style="color: #64748b; font-size: 11px;">Market Bullish</div>
                <div style="font-weight: 600;">{fmt_prob(ctx.polymarket_market_bullish_prob)}</div>
            </div>
        </div>

        {markets_html}

        <div style="margin-top: 10px; font-size: 11px; color: #92400e; font-style: italic;">
            Data from Polymarket prediction markets - reflects real-money betting on future events
        </div>
    </div>
    """


def _render_ai_analysis(ai: AIAnalysis) -> str:
    """Render the AI analysis section."""

    # Top actions
    actions_html = ""
    if ai.top_actions:
        actions_html = '<div style="margin-bottom: 15px;"><strong style="color: #1e40af;">Top 3 Actions Today:</strong><ol style="margin: 8px 0 0 0; padding-left: 20px;">'
        for action in ai.top_actions[:3]:
            actions_html += f'<li style="margin-bottom: 4px;">{action}</li>'
        actions_html += '</ol></div>'

    # Positions to cut
    cut_html = ""
    if ai.positions_to_cut:
        cut_html = '<div style="background: #fef2f2; padding: 10px; border-radius: 6px; margin-bottom: 10px;"><strong style="color: #991b1b;">Consider Cutting:</strong><ul style="margin: 5px 0 0 0; padding-left: 20px;">'
        for symbol, reason in ai.positions_to_cut[:5]:
            cut_html += f'<li><strong>{symbol}</strong>: {reason}</li>'
        cut_html += '</ul></div>'

    # Positions to trim
    trim_html = ""
    if ai.positions_to_trim:
        trim_html = '<div style="background: #fef3c7; padding: 10px; border-radius: 6px; margin-bottom: 10px;"><strong style="color: #92400e;">Consider Trimming:</strong><ul style="margin: 5px 0 0 0; padding-left: 20px;">'
        for symbol, reason in ai.positions_to_trim[:5]:
            trim_html += f'<li><strong>{symbol}</strong>: {reason}</li>'
        trim_html += '</ul></div>'

    # Positions to hold
    hold_html = ""
    if ai.positions_to_hold:
        hold_html = '<div style="background: #f0fdf4; padding: 10px; border-radius: 6px; margin-bottom: 10px;"><strong style="color: #166534;">Hold With Confidence:</strong><ul style="margin: 5px 0 0 0; padding-left: 20px;">'
        for symbol, reason in ai.positions_to_hold[:5]:
            hold_html += f'<li><strong>{symbol}</strong>: {reason}</li>'
        hold_html += '</ul></div>'

    # Risk warnings
    warnings_html = ""
    if ai.risk_warnings:
        warnings_html = '<div style="background: #fef2f2; border-left: 3px solid #ef4444; padding: 10px; margin-top: 10px;"><strong style="color: #991b1b;">Risk Warnings:</strong><ul style="margin: 5px 0 0 0; padding-left: 20px;">'
        for warning in ai.risk_warnings[:3]:
            warnings_html += f'<li>{warning}</li>'
        warnings_html += '</ul></div>'

    # Market outlook
    outlook_html = ""
    if ai.market_outlook:
        outlook_html = f'<div style="background: #f1f5f9; padding: 10px; border-radius: 6px; margin-top: 10px; font-size: 13px;"><strong>Market Outlook:</strong> {ai.market_outlook}</div>'

    return f"""
    <!-- AI ANALYSIS -->
    <div style="background: linear-gradient(135deg, #f8fafc, #e0e7ff); padding: 20px; border-radius: 12px; margin-bottom: 20px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); border: 1px solid #c7d2fe;">
        <h2 style="margin: 0 0 15px 0; font-size: 18px; border-bottom: 2px solid #6366f1; padding-bottom: 10px; display: flex; align-items: center;">
            <span style="margin-right: 8px;">AI Analysis</span>
            <span style="font-size: 11px; background: #6366f1; color: white; padding: 2px 8px; border-radius: 4px;">Claude</span>
        </h2>

        <div style="background: white; padding: 15px; border-radius: 8px; margin-bottom: 15px;">
            <div style="font-size: 14px; line-height: 1.5; color: #1e293b;">{ai.executive_summary}</div>
        </div>

        {actions_html}
        {cut_html}
        {trim_html}
        {hold_html}
        {outlook_html}
        {warnings_html}
    </div>
    """


def _render_position(p: SwingPosition, ai_note: str = "") -> str:
    """Render a single position row."""
    pnl_color = "#22c55e" if p.unrealized_pnl >= 0 else "#ef4444"

    # Attention score badge color
    if p.attention_score >= 60:
        attention_color = "#ef4444"  # Red - critical/high
        attention_bg = "#fef2f2"
    elif p.attention_score >= 40:
        attention_color = "#f59e0b"  # Orange - moderate
        attention_bg = "#fef3c7"
    else:
        attention_color = "#22c55e"  # Green - healthy
        attention_bg = "#f0fdf4"

    # AI recommendation badge
    ai_badge = ""
    if ai_note:
        if ai_note.startswith("CUT:"):
            ai_badge = '<span style="background:#991b1b;color:white;padding:2px 6px;border-radius:4px;font-size:11px;font-weight:600;margin-left:8px;">AI: CUT</span>'
        elif ai_note.startswith("TRIM:"):
            ai_badge = '<span style="background:#92400e;color:white;padding:2px 6px;border-radius:4px;font-size:11px;font-weight:600;margin-left:8px;">AI: TRIM</span>'
        elif ai_note.startswith("HOLD:"):
            ai_badge = '<span style="background:#166534;color:white;padding:2px 6px;border-radius:4px;font-size:11px;font-weight:600;margin-left:8px;">AI: HOLD</span>'

    # Border color based on attention
    border_color = "#ef4444" if p.attention_score >= 60 else "#f59e0b" if p.attention_score >= 40 else "#e2e8f0"

    alerts = []
    # STOP BREACHED is most critical - show first with dark red
    if p.stop_breached:
        alerts.append('<span style="background:#991b1b;color:white;padding:2px 6px;border-radius:4px;font-size:11px;font-weight:600;">STOP BREACHED</span>')
    elif p.near_stop:
        alerts.append('<span style="background:#fef2f2;color:#ef4444;padding:2px 6px;border-radius:4px;font-size:11px;">NEAR STOP</span>')
    if p.near_target:
        alerts.append('<span style="background:#f0fdf4;color:#22c55e;padding:2px 6px;border-radius:4px;font-size:11px;">NEAR TARGET</span>')
    if p.earnings_soon:
        alerts.append('<span style="background:#fef3c7;color:#d97706;padding:2px 6px;border-radius:4px;font-size:11px;">EARNINGS SOON</span>')

    # Attention reasons
    attention_reasons_html = ""
    if p.attention_reasons:
        attention_reasons_html = f'<div style="font-size: 12px; color: {attention_color}; margin-top: 6px; font-style: italic;">{" | ".join(p.attention_reasons)}</div>'

    # Format distance to stop - show as positive "above stop" or negative "below stop"
    stop_distance_str = ""
    if p.distance_to_stop_pct is not None:
        if p.distance_to_stop_pct >= 0:
            stop_distance_str = f"+{p.distance_to_stop_pct:.1f}%"
        else:
            stop_distance_str = f"{p.distance_to_stop_pct:.1f}%"

    # AI note text (shown below position details)
    ai_note_html = ""
    if ai_note:
        # Extract just the reason part (after "CUT:", "TRIM:", or "HOLD:")
        note_parts = ai_note.split(":", 1)
        note_text = note_parts[1].strip() if len(note_parts) > 1 else ai_note
        ai_note_html = f'<div style="font-size: 12px; color: #6366f1; margin-top: 6px; padding: 6px 8px; background: #e0e7ff; border-radius: 4px;"><strong>AI:</strong> {note_text}</div>'

    return f"""
    <div style="border: 2px solid {border_color}; border-radius: 8px; padding: 12px; margin-bottom: 10px;">
        <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px;">
            <div>
                <span style="font-weight: 600; font-size: 16px;">{p.symbol}</span>
                <span style="background:{attention_bg};color:{attention_color};padding:2px 8px;border-radius:4px;font-size:11px;margin-left:8px;font-weight:600;">
                    {int(p.attention_score)}
                </span>{ai_badge}
                <span style="color: #64748b; font-size: 13px; margin-left: 8px;">{_format_shares(p.shares)} shares @ {format_money(p.avg_cost, 2)}</span>
            </div>
            <div style="text-align: right;">
                <div style="font-weight: 600; color: {pnl_color};">{format_money(p.unrealized_pnl, 2)} ({format_pct(p.unrealized_pnl_pct)})</div>
                <div style="font-size: 12px; color: #64748b;">{p.days_held} days held</div>
            </div>
        </div>
        <div style="font-size: 13px; color: #475569; margin-bottom: 8px;">
            Current: <strong>{format_money(p.current_price, 2)}</strong> |
            Stop: {format_money(p.stop_loss, 2) if p.stop_loss else 'N/A'} ({stop_distance_str if stop_distance_str else 'N/A'}) |
            Target: {format_money(p.target_1, 2) if p.target_1 else 'N/A'}
        </div>
        <div style="display: flex; gap: 5px; flex-wrap: wrap;">
            {trend_badge(p.trend_status)}
            {f'<span style="background:#f1f5f9;padding:2px 6px;border-radius:4px;font-size:11px;">RSI {p.rsi:.0f}</span>' if p.rsi else ''}
            {"".join(alerts)}
        </div>
        {attention_reasons_html}
        {ai_note_html}
    </div>
    """


def _render_setup(s: WatchlistSetup) -> str:
    """Render a single setup row."""
    setup_colors = {
        "pullback_to_ema20": "#3b82f6",
        "bull_flag": "#8b5cf6",
        "oversold_bounce": "#22c55e",
        "breakout": "#f59e0b"
    }
    color = setup_colors.get(s.setup_type, "#6b7280")

    return f"""
    <div style="border: 1px solid #e2e8f0; border-radius: 8px; padding: 12px; margin-bottom: 10px;">
        <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px;">
            <div>
                <span style="font-weight: 600; font-size: 16px;">{s.symbol}</span>
                <span style="background:{color};color:white;padding:2px 8px;border-radius:4px;font-size:11px;margin-left:8px;">{s.setup_type.replace('_', ' ').upper()}</span>
            </div>
            <div style="text-align: right;">
                <div style="font-weight: 600;">{format_money(s.current_price, 2)}</div>
                <div style="font-size: 12px; color: #64748b;">Score: {s.score:.0f}/100</div>
            </div>
        </div>
        <div style="font-size: 13px; color: #475569; margin-bottom: 8px;">
            <strong>Entry:</strong> {format_money(s.entry_low, 2)} - {format_money(s.entry_high, 2)} |
            <strong>Stop:</strong> {format_money(s.stop_loss, 2)} |
            <strong>Target:</strong> {format_money(s.target_1, 2)} |
            <strong>R:R:</strong> {s.risk_reward:.1f}:1
        </div>
        <div style="font-size: 13px; color: #64748b; margin-bottom: 8px;">
            <strong>Trigger:</strong> {s.trigger_condition}
        </div>
        <div style="font-size: 12px; color: #475569;">
            {"<br>".join(f"- {r}" for r in s.reasons)}
        </div>
        <div style="display: flex; gap: 5px; flex-wrap: wrap; margin-top: 8px;">
            {f'<span style="background:#f1f5f9;padding:2px 6px;border-radius:4px;font-size:11px;">RSI {s.rsi:.0f}</span>' if s.rsi else ''}
            {f'<span style="background:#f1f5f9;padding:2px 6px;border-radius:4px;font-size:11px;">RS {s.relative_strength:+.1f}%</span>' if s.relative_strength else ''}
            {f'<span style="background:#f1f5f9;padding:2px 6px;border-radius:4px;font-size:11px;">Vol {s.volume_ratio:.1f}x</span>' if s.volume_ratio else ''}
        </div>
    </div>
    """


# ============================================================================
# EMAIL SENDING
# ============================================================================

def send_email(subject: str, html_body: str, text_body: str = "") -> bool:
    """Send the newsletter email."""
    host = os.environ.get("SMTP_HOST", "smtp.gmail.com")
    port = int(os.environ.get("SMTP_PORT", "587"))
    user = os.environ.get("SMTP_USER")
    pwd = os.environ.get("SMTP_PASS")
    from_addr = os.environ.get("EMAIL_FROM")
    to_addr = os.environ.get("EMAIL_TO")

    if not all([user, pwd, from_addr, to_addr]):
        logger.error("Email configuration incomplete - check environment variables")
        return False

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = from_addr
    msg["To"] = to_addr

    # Plain text fallback
    if not text_body:
        text_body = "Please view this email in an HTML-capable email client."
    msg.set_content(text_body)
    msg.add_alternative(html_body, subtype="html")

    try:
        with smtplib.SMTP(host, port) as server:
            server.starttls()
            server.login(user, pwd)
            server.send_message(msg)
        logger.info(f"Newsletter sent successfully to {to_addr}")
        return True
    except Exception as e:
        logger.error(f"Failed to send email: {e}")
        return False


# ============================================================================
# MAIN
# ============================================================================

def main():
    """Generate and send the swing trading newsletter."""
    logger.info("=" * 60)
    logger.info("SWING TRADING NEWSLETTER - Starting generation")
    logger.info("=" * 60)

    # Initialize Polygon client
    polygon = PolygonClient()

    if not polygon.api_key:
        logger.error("POLYGON_API_KEY not set - cannot generate newsletter")
        return

    # Load config
    config = {}
    if CONFIG_PATH.exists():
        try:
            with open(CONFIG_PATH, encoding="utf-8") as f:
                config = yaml.safe_load(f) or {}
        except Exception as e:
            logger.warning(f"Could not load config.yaml: {e}")

    # 1. Analyze market context
    logger.info("Analyzing market context...")
    market_ctx = analyze_market_context(polygon)
    logger.info(f"SPY: {market_ctx.spy_price:.2f} ({market_ctx.spy_trend}) | VIXY: ${market_ctx.vix:.2f} ({market_ctx.vix_regime})")

    # 2. Load and enrich positions
    logger.info("Loading positions...")
    robinhood_csv = SCRIPT_DIR / "inputs" / "robinhood_activity.csv"
    positions = load_positions_from_robinhood_csv(robinhood_csv)
    positions = enrich_positions(positions, polygon)
    logger.info(f"Loaded {len(positions)} open positions")

    # 3. Scan watchlist for setups
    logger.info("Scanning watchlist for setups...")
    watchlist_file = SCRIPT_DIR / "inputs" / "buy_universe.txt"
    watchlist = load_watchlist(watchlist_file)

    # Get SPY data for relative strength calculations
    spy_df = polygon.get_daily_bars("SPY", days=60)

    setups = scan_for_setups(watchlist, polygon, spy_df)
    logger.info(f"Found {len(setups)} setups meeting criteria")

    # 4. Calculate risk dashboard
    logger.info("Calculating risk metrics...")
    # Estimate total equity (would need broker API for real value)
    total_equity = sum(p.market_value for p in positions) + 50000  # Assume $50k cash as placeholder
    cash = 50000  # Placeholder
    risk_dash = calculate_risk_dashboard(positions, total_equity, cash)

    # 5. Generate AI analysis (Claude first, GPT fallback)
    logger.info("Generating AI analysis...")
    ai_analyzer = AIAnalyzer()
    ai_analysis = ai_analyzer.analyze_portfolio(market_ctx, positions, risk_dash)
    if ai_analysis.executive_summary:
        logger.info("AI analysis generated successfully")
    else:
        logger.info("AI analysis skipped (no API keys or API error)")

    # 6. Generate and send email
    date_str = dt.datetime.now(ET).strftime("%A, %B %d, %Y")
    html = generate_html_email(market_ctx, positions, setups, risk_dash, date_str, ai_analysis)

    subject = f"[Swing Newsletter] {dt.datetime.now(ET).strftime('%m/%d')} - SPY {market_ctx.spy_trend} | VIXY ${market_ctx.vix:.0f}"

    if send_email(subject, html):
        logger.info("Newsletter generated and sent successfully!")
    else:
        # Save to file as backup
        output_file = SCRIPT_DIR / "logs" / f"newsletter_{dt.datetime.now().strftime('%Y%m%d')}.html"
        output_file.write_text(html)
        logger.info(f"Newsletter saved to {output_file}")

    logger.info("=" * 60)
    logger.info("SWING TRADING NEWSLETTER - Complete")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
