"""
Small Cap Momentum Scanner v2.0
===============================

A standalone scanner that continuously searches for small cap momentum setups
and displays them in an easy-to-read format for human decision-making.

This script DOES NOT execute trades - it only scans and displays opportunities.
Use smallcap_executor.py to execute trades on selected setups.

Features:
1. Pre-market gap scanning (7:00 AM - 9:30 AM ET)
2. RTH pattern detection (bull flags, consolidations)
3. Signal quality grading (A+, A, B, C)
4. NEWS INTEGRATION with sentiment analysis
5. Color-coded display (green=good, purple=great, red=bad)
6. PrettyTable formatted output
7. ASCII mini-charts for quick visual assessment
8. Pre-calculated entry/stop/target levels

Usage:
    python smallcap_scanner.py

Press Ctrl+C to exit.

Version: 2.0.0
"""

import os
import sys
import time
import requests
import pandas as pd
import numpy as np
import winsound  # Windows sound alerts
from datetime import datetime, timedelta, time as dt_time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple, Set
from enum import Enum
from zoneinfo import ZoneInfo
from pathlib import Path

# Load config from .env file if present
try:
    from dotenv import load_dotenv
    _env_path = Path(__file__).parent.parent / "config" / "smallcap_scanner.env"
    if _env_path.exists():
        load_dotenv(_env_path)
except ImportError:
    pass  # dotenv not installed, rely on environment variables

# Enable ANSI color support on Windows
if sys.platform == "win32":
    # Enable Virtual Terminal Processing for ANSI escape codes
    try:
        import ctypes
        kernel32 = ctypes.windll.kernel32
        # Enable ANSI escape sequence processing
        kernel32.SetConsoleMode(kernel32.GetStdHandle(-11), 7)
    except Exception:
        pass  # If it fails, colors won't work but script will still run

# Try to import prettytable, fall back to simple formatting if not available
try:
    from prettytable import PrettyTable, SINGLE_BORDER
    PRETTYTABLE_AVAILABLE = True
except ImportError:
    PRETTYTABLE_AVAILABLE = False
    print("Note: Install prettytable for better formatting: pip install prettytable")

# Try to import Reddit sentiment provider
try:
    # Add utilities directory to path to access reddit_sentiment module
    UTILITIES_PATH = os.path.join(os.path.dirname(__file__), "..", "utilities")
    if os.path.exists(UTILITIES_PATH):
        sys.path.insert(0, UTILITIES_PATH)
    from reddit_sentiment import RedditSentimentProvider, RedditStock
    REDDIT_AVAILABLE = True
except ImportError:
    REDDIT_AVAILABLE = False
    RedditSentimentProvider = None
    RedditStock = None

# ============================================================
# CONFIGURATION
# ============================================================

ET = ZoneInfo("America/New_York")
POLYGON_API_KEY = os.getenv("POLYGON_API_KEY", "")

# --- Display Settings ---
REFRESH_INTERVAL_SEC = 15       # How often to refresh the scanner
MAX_DISPLAY_SETUPS = 8          # Maximum setups to display
CLEAR_SCREEN = True             # Clear screen on refresh
NEWS_MAX_AGE_HOURS = 24         # Only show news from last 24 hours
NEWS_MAX_PER_TICKER = 3         # Max news items per ticker

# --- Setup History (keep setups visible after they stop qualifying) ---
SETUP_HISTORY_MINUTES = 5       # Keep A+/A setups visible for 5 minutes after last qualifying
SHOW_EXPIRED_SETUPS = True      # Show recently-expired setups in a separate section

# --- Hot List Mode (monitor A+ setups for optimal entry timing) ---
HOT_LIST_ENABLED = True                 # Enable Hot List tracking mode
HOT_LIST_RESCAN_SEC = 30                # Re-scan hot list symbols every 30 seconds
HOT_LIST_MAX_AGE_MINUTES = 60           # Remove from hot list after 60 minutes of no triggers
HOT_LIST_MIN_GRADE = "A+"               # Minimum grade to add to hot list ("A+" or "A")
_scanner_output = Path(os.getenv("ALGO_OUTPUT_DIR", "")) if os.getenv("ALGO_OUTPUT_DIR") else Path(__file__).parent
HOT_LIST_PERSIST_FILE = str(_scanner_output / "data" / "state" / "hot_list.json")

# --- Hot List Entry Triggers ---
# HOD Break: Price breaks above high of day
HOT_LIST_HOD_BREAK_BUFFER_PCT = 0.2     # Break HOD by at least 0.2%
# Key Level Break: Price breaks a psychological level ($0.50 or $1.00)
HOT_LIST_KEY_LEVEL_BUFFER_PCT = 0.3     # Break key level by at least 0.3%
# Micro Pullback: Price pulls back 1-3% then bounces
HOT_LIST_MICRO_PB_MIN_PCT = 1.0         # Minimum pullback %
HOT_LIST_MICRO_PB_MAX_PCT = 3.0         # Maximum pullback %
HOT_LIST_MICRO_PB_BOUNCE_PCT = 0.5      # Bounce at least 0.5% from pullback low
# Spread Tightening: Spread narrows significantly (execution quality improving)
HOT_LIST_SPREAD_TIGHT_BPS = 30          # Spread tightens to 30bps or below
# Volume Surge: Current bar volume is 2x+ prior 5-bar average
HOT_LIST_VOLUME_SURGE_MULT = 2.0        # 2x volume = surge trigger

# --- Sound Alerts ---
SOUND_ALERT_ENABLED = True      # Play sound when new A+ setup appears
SOUND_ALERT_FOR_A = False       # Also play sound for A setups (not just A+)

# Track which hot list triggers have already played sounds (to avoid repeat alerts)
# Format: {(symbol, trigger_type, trigger_count), ...}
_sounded_hot_list_triggers: set = set()

# --- TradingView Integration ---
TRADINGVIEW_BASE_URL = "https://www.tradingview.com/chart/?symbol="  # Quick link URL

# --- Reddit Sentiment Settings ---
ENABLE_REDDIT_SENTIMENT = True  # Enable Reddit sentiment integration
REDDIT_CACHE_MINUTES = 30       # How often to refresh Reddit data
REDDIT_TRENDING_BOOST = 0.10    # Confidence boost for Reddit-trending stocks (+10%)
REDDIT_BULLISH_BOOST = 0.15     # Additional boost for bullish sentiment (+15%)
REDDIT_BEARISH_PENALTY = 0.10   # Confidence penalty for bearish sentiment (-10%)
REDDIT_MIN_MENTIONS = 20        # Minimum mentions to consider trending

# --- Universe Filters ---
MIN_PRICE = 1.00                # Minimum stock price
MAX_PRICE = 20.00               # Maximum stock price (small cap focus)
SWEET_SPOT_MIN = 5.00           # Ideal price range low
SWEET_SPOT_MAX = 10.00          # Ideal price range high
MIN_PCT_CHANGE = 10.0           # Minimum % gain from prev close
MIN_RELATIVE_VOLUME = 5.0       # RVOL >= 5x average
MIN_ABSOLUTE_VOLUME = 500_000   # Minimum shares traded today
MAX_SHARES_OUTSTANDING = 10_000_000  # Maximum float (10M) - true float when available
# NOTE: Using Massive/Polygon float endpoint (GET /stocks/vX/float) for true free float.
# This is the actual number of shares freely tradable, excluding insider holdings and
# restricted shares. Falls back to shares_outstanding if float data unavailable.

# --- Session Timing ---
PREMARKET_START = dt_time(7, 0)   # 7:00 AM ET - start scanning
PREMARKET_END = dt_time(9, 30)    # 9:30 AM ET - market open
RTH_START = dt_time(9, 30)        # Regular trading hours start
TRADING_END = dt_time(11, 0)      # "Prime time" ends (first 90 mins)
SCAN_END = dt_time(16, 0)         # Stop scanning at market close (4 PM)

# --- Market Direction Tracker ---
# Tracks SPY/QQQ momentum to help time breakout entries
MARKET_TRACKER_ENABLED = True           # Enable market direction display
MARKET_TRACKER_SYMBOLS = ["SPY", "QQQ"] # Symbols to track
MARKET_TRACKER_LOOKBACK_SHORT = 5       # Short-term lookback (minutes)
MARKET_TRACKER_LOOKBACK_LONG = 15       # Longer-term lookback (minutes)
MARKET_TRACKER_CACHE_SEC = 30           # Cache data for 30 seconds

# --- Pattern Detection ---
FLAG_POLE_MIN_PCT = 6.0         # Minimum pole move %
FLAG_MAX_RETRACE_PCT = 30.0     # Max flag retracement of pole
FLAG_MIN_BARS = 3               # Minimum consolidation bars
FLAG_MAX_BARS = 7               # Maximum consolidation bars

# --- ATR-Based Stops (V7 BEST_MIX) ---
ATR_PERIOD = 14
ATR_STOP_MULT = 4.0             # Stop = 4x ATR
MIN_STOP_DISTANCE_PCT = 3.5     # Minimum stop distance %

# --- Execution Quality Thresholds ---
MAX_SPREAD_BPS_A_PLUS = 50      # Max 50bps spread for A+ (0.50%)
MAX_SPREAD_BPS_A = 80           # Max 80bps spread for A (0.80%)
MAX_SPREAD_BPS_B = 120          # Max 120bps spread for B (1.20%)
MIN_VOLUME_ACCEL_A_PLUS = 1.3   # Volume must be 1.3x prior 5-bar avg for A+

# --- ADX Trend Strength Filter for A+ ---
# ADX > 25 indicates strong trend - required for A+ setups
# This filters out choppy/consolidating stocks that lack directional conviction
ADX_PERIOD = 14                 # Standard ADX calculation period
MIN_ADX_A_PLUS = 25.0           # Minimum ADX for A+ grade (strong trend)
FLOAT_ROTATION_BOOST_THRESHOLD = 0.5  # 50% float rotation = high squeeze potential
FLOAT_ROTATION_MAX_BOOST = 1.0  # 100%+ float rotation = maximum boost

# --- Exit Targets (V7 BEST_MIX) ---
TP1_R_MULTIPLE = 1.0            # First take profit at 1.0R
TP2_R_MULTIPLE = 2.5            # Second take profit at 2.5R
TRAIL_DISTANCE_PCT = 2.0        # Trail 2.0% behind high

# --- Signal Grading Criteria ---
# A+ = Multi-timeframe momentum alignment (1m/5m/15m all positive and strong)
# This is the highest probability setup - rare but worth waiting for
GRADE_A_PLUS_MOMENTUM = {
    "min_chg_1m": 0.5,           # 1m change > +0.5% (fresh buying pressure)
    "min_chg_5m": 1.5,           # 5m change > +1.5% (short-term trend)
    "min_chg_15m": 2.5,          # 15m change > +2.5% (intermediate trend)
    "min_1m_contribution": 0.25, # 1m must be at least 25% of 5m (not stalling)
}
# Base criteria (still required for A+)
GRADE_A_PLUS = {
    "min_gap_pct": 12.0,         # Slightly relaxed since momentum alignment is key
    "min_rvol": 6.0,
    "min_pole_pct": 6.0,
    "max_retrace_pct": 30.0,
}
# A grade - previous A+ criteria (strong setup but missing momentum alignment)
GRADE_A = {
    "min_gap_pct": 15.0,
    "min_rvol": 8.0,
    "min_pole_pct": 8.0,
    "max_retrace_pct": 25.0,
}
# B grade - previous A criteria
GRADE_B = {
    "min_gap_pct": 10.0,
    "min_rvol": 5.0,
    "min_pole_pct": 6.0,
    "max_retrace_pct": 30.0,
}
# C grade - previous B criteria (marginal setups)
GRADE_C = {
    "min_gap_pct": 7.0,
    "min_rvol": 3.0,
    "min_pole_pct": 5.0,
    "max_retrace_pct": 40.0,
}

# --- News Sentiment Keywords ---
# Keywords that indicate BULLISH catalysts
BULLISH_KEYWORDS = [
    "fda approval", "fda approved", "approved", "breakthrough",
    "partnership", "contract", "deal", "acquisition", "merger",
    "beat", "beats", "exceeded", "surpassed", "record",
    "upgrade", "upgraded", "buy rating", "outperform",
    "revenue growth", "profit", "profitable", "earnings beat",
    "patent", "granted", "launch", "launched", "new product",
    "clinical trial", "positive results", "successful",
    "regulatory approval", "cleared", "expanded", "expansion",
    "raised guidance", "raises", "strong demand", "orders"
]

# Keywords that indicate VERY BULLISH (purple/great)
GREAT_KEYWORDS = [
    "fda approval", "fda approved", "breakthrough designation",
    "acquisition", "merger", "buyout", "takeover",
    "10x", "100%", "doubled", "tripled", "record high",
    "massive", "huge contract", "billion", "major partnership"
]

# Keywords that indicate BEARISH catalysts (avoid)
BEARISH_KEYWORDS = [
    "lawsuit", "sued", "litigation", "fraud", "investigation",
    "fda reject", "rejected", "failed", "failure", "miss",
    "downgrade", "downgraded", "sell rating", "underperform",
    "loss", "losses", "decline", "declining", "fell",
    "dilution", "offering", "secondary", "shelf registration",
    "debt", "bankruptcy", "default", "delisting", "warning",
    "recall", "suspended", "halt", "halted", "concern"
]

# --- Catalyst Type Keywords (for classification) ---
CATALYST_TYPE_KEYWORDS = {
    "FDA": [
        "fda", "approval", "approved", "clinical trial", "phase 1", "phase 2",
        "phase 3", "drug", "therapy", "treatment", "breakthrough designation",
        "fast track", "priority review", "nda", "bla", "anda", "pdufa"
    ],
    "EARNINGS": [
        "earnings", "eps", "revenue", "quarterly", "q1", "q2", "q3", "q4",
        "guidance", "forecast", "beat", "miss", "profit", "results",
        "fiscal", "annual report"
    ],
    "CONTRACT": [
        "contract", "partnership", "agreement", "deal", "collaboration",
        "license", "licensing", "supply", "distribution", "awarded"
    ],
    "MERGER": [
        "acquisition", "acquire", "merger", "buyout", "takeover", "bid",
        "offer to purchase", "tender offer", "going private"
    ],
    "OFFERING": [
        "offering", "secondary", "dilution", "shelf", "registered direct",
        "public offering", "private placement", "atm", "at-the-market",
        "stock sale", "share issuance"
    ],
    "COMPLIANCE": [
        "reverse split", "delisting", "nasdaq", "compliance", "deficiency",
        "minimum bid", "listing requirements"
    ],
    "LEGAL": [
        "lawsuit", "litigation", "sued", "settlement", "investigation",
        "subpoena", "sec", "doj", "fraud"
    ],
    "PRODUCT": [
        "launch", "product", "release", "new technology", "patent",
        "innovation", "breakthrough", "unveil"
    ]
}


# ============================================================
# ANSI COLOR CODES
# ============================================================

class Colors:
    """ANSI color codes for terminal output."""
    RESET = "\033[0m"
    BOLD = "\033[1m"
    DIM = "\033[2m"

    # Standard colors
    RED = "\033[91m"
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    BLUE = "\033[94m"
    PURPLE = "\033[95m"  # Magenta/Purple for GREAT
    CYAN = "\033[96m"
    WHITE = "\033[97m"
    GRAY = "\033[90m"

    # Background colors
    BG_RED = "\033[41m"
    BG_GREEN = "\033[42m"
    BG_PURPLE = "\033[45m"

    @staticmethod
    def colorize(text: str, color: str) -> str:
        """Wrap text with color codes."""
        return f"{color}{text}{Colors.RESET}"


class Box:
    """Unicode box-drawing characters for tables."""
    # Single line box
    H = "─"      # Horizontal
    V = "│"      # Vertical
    TL = "┌"     # Top-left
    TR = "┐"     # Top-right
    BL = "└"     # Bottom-left
    BR = "┘"     # Bottom-right
    LT = "├"     # Left-T
    RT = "┤"     # Right-T
    TT = "┬"     # Top-T
    BT = "┴"     # Bottom-T
    X = "┼"      # Cross

    # Double line box (for headers)
    DH = "═"     # Double horizontal
    DV = "║"     # Double vertical
    DTL = "╔"    # Double top-left
    DTR = "╗"    # Double top-right
    DBL = "╚"    # Double bottom-left
    DBR = "╝"    # Double bottom-right

    @staticmethod
    def hline(width: int, left: str = "", right: str = "", char: str = "─") -> str:
        """Create a horizontal line with optional corners."""
        return f"{left}{char * width}{right}"

    @staticmethod
    def row(cells: List[str], widths: List[int], sep: str = "│") -> str:
        """Create a table row with separators."""
        parts = []
        for cell, width in zip(cells, widths):
            # Strip ANSI codes to get actual length
            stripped = ""
            i = 0
            while i < len(cell):
                if cell[i] == '\033':
                    while i < len(cell) and cell[i] != 'm':
                        i += 1
                    i += 1
                else:
                    stripped += cell[i]
                    i += 1
            padding = width - len(stripped)
            parts.append(cell + " " * max(0, padding))
        return f"{sep} {f' {sep} '.join(parts)} {sep}"


class ScreenBuffer:
    """Buffer for building screen output to prevent flashing."""
    def __init__(self):
        self.lines: List[str] = []

    def add(self, text: str = ""):
        """Add a line to the buffer."""
        self.lines.append(text)

    def clear(self):
        """Clear the buffer."""
        self.lines = []

    def render(self) -> str:
        """Get the complete output as a string."""
        return "\n".join(self.lines)

    def print(self):
        """Print the buffer contents."""
        print(self.render())


# Global screen buffer
screen = ScreenBuffer()


# ============================================================
# DATA STRUCTURES
# ============================================================

class SignalGrade(Enum):
    A_PLUS = "A+"
    A = "A"
    B = "B"
    C = "C"


class PatternType(Enum):
    BULL_FLAG = "BULL_FLAG"
    CONSOLIDATION = "CONSOLIDATION"
    BREAKOUT = "BREAKOUT"
    NONE = "NONE"


class NewsSentiment(Enum):
    GREAT = "GREAT"      # Purple - very bullish catalyst
    GOOD = "GOOD"        # Green - bullish
    NEUTRAL = "NEUTRAL"  # White - no clear direction
    BAD = "BAD"          # Red - bearish, avoid


class CatalystType(Enum):
    """Type of news catalyst driving the move."""
    FDA = "FDA"                    # FDA approval, clinical trial results
    EARNINGS = "EARNINGS"          # Earnings beat/miss, guidance
    CONTRACT = "CONTRACT"          # Contract wins, partnerships
    MERGER = "MERGER"              # M&A, acquisition, buyout
    OFFERING = "OFFERING"          # Secondary offering, dilution (WARNING)
    COMPLIANCE = "COMPLIANCE"      # Reverse split, delisting risk
    LEGAL = "LEGAL"                # Lawsuit, investigation
    PRODUCT = "PRODUCT"            # Product launch, new tech
    UNKNOWN = "UNKNOWN"            # No clear catalyst type


class KeyLevelState(Enum):
    """Price behavior relative to key psychological level."""
    ABOVE = "ABOVE"                # Trading above key level
    AT = "AT"                      # At the key level (within 1%)
    BELOW = "BELOW"                # Trading below key level
    RECLAIMING = "RECLAIMING"      # Was below, now pushing back above
    REJECTING = "REJECTING"        # Was above, now failing at level


class HotListStage(Enum):
    """Stage of a Hot List entry - tracks readiness for entry."""
    WATCHING = "WATCHING"          # Initial stage - monitoring for setup
    CONSOLIDATING = "CONSOLIDATING"  # In a consolidation/pullback phase
    SETUP = "SETUP"                # Setup forming - getting ready for trigger
    TRIGGER = "TRIGGER"            # Entry trigger fired! Time to act
    COOLING = "COOLING"            # Had a trigger but missed/passed, cooling off
    REMOVED = "REMOVED"            # Marked for removal (stale or invalidated)


class HotListTrigger(Enum):
    """Type of entry trigger that fired."""
    HOD_BREAK = "HOD_BREAK"        # Broke above high of day
    KEY_LEVEL = "KEY_LEVEL"        # Broke above key psychological level
    MICRO_PB = "MICRO_PB"          # Micro pullback bounce
    SPREAD_TIGHT = "SPREAD_TIGHT"  # Spread tightened significantly
    VOLUME_SURGE = "VOLUME_SURGE"  # Volume surge detected
    NONE = "NONE"                  # No trigger yet


@dataclass
class HotListEntry:
    """
    A tracked A+/A setup being monitored for optimal entry timing.

    Hot List entries go through stages:
    WATCHING → CONSOLIDATING → SETUP → TRIGGER → COOLING → REMOVED

    The entry includes historical price data to detect triggers and track
    the setup's progression toward an entry point.
    """
    symbol: str
    grade: SignalGrade
    added_time: datetime
    last_update: datetime

    # Stage tracking
    stage: HotListStage = HotListStage.WATCHING
    stage_changed_at: datetime = field(default_factory=lambda: datetime.now(ET))

    # Entry trigger info
    trigger_type: HotListTrigger = HotListTrigger.NONE
    trigger_time: Optional[datetime] = None
    trigger_price: float = 0.0
    trigger_count: int = 0          # How many triggers have fired

    # Price tracking for trigger detection
    entry_price_at_add: float = 0.0  # Price when added to hot list
    hod_at_add: float = 0.0          # HOD when added
    current_price: float = 0.0
    current_hod: float = 0.0
    pullback_low: float = 0.0        # Lowest price during pullback
    last_spread_bps: float = 0.0     # Last observed spread

    # Key levels
    nearest_key_level: float = 0.0
    next_resistance: float = 0.0

    # Setup info snapshot
    gap_pct: float = 0.0
    rvol: float = 0.0
    confidence: float = 0.0
    catalyst_type: str = ""
    news_sentiment: str = ""

    # Tracking
    times_triggered: int = 0         # Total triggers fired since added
    last_trigger_age_sec: int = 0    # Seconds since last trigger

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON persistence."""
        return {
            "symbol": self.symbol,
            "grade": self.grade.value,
            "added_time": self.added_time.isoformat(),
            "last_update": self.last_update.isoformat(),
            "stage": self.stage.value,
            "stage_changed_at": self.stage_changed_at.isoformat(),
            "trigger_type": self.trigger_type.value,
            "trigger_time": self.trigger_time.isoformat() if self.trigger_time else None,
            "trigger_price": self.trigger_price,
            "trigger_count": self.trigger_count,
            "entry_price_at_add": self.entry_price_at_add,
            "hod_at_add": self.hod_at_add,
            "current_price": self.current_price,
            "current_hod": self.current_hod,
            "pullback_low": self.pullback_low,
            "last_spread_bps": self.last_spread_bps,
            "nearest_key_level": self.nearest_key_level,
            "next_resistance": self.next_resistance,
            "gap_pct": self.gap_pct,
            "rvol": self.rvol,
            "confidence": self.confidence,
            "catalyst_type": self.catalyst_type,
            "news_sentiment": self.news_sentiment,
            "times_triggered": self.times_triggered,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "HotListEntry":
        """Create from dictionary (JSON deserialization)."""
        return cls(
            symbol=data["symbol"],
            grade=SignalGrade(data["grade"]),
            added_time=datetime.fromisoformat(data["added_time"]),
            last_update=datetime.fromisoformat(data["last_update"]),
            stage=HotListStage(data.get("stage", "WATCHING")),
            stage_changed_at=datetime.fromisoformat(data["stage_changed_at"]) if data.get("stage_changed_at") else datetime.now(ET),
            trigger_type=HotListTrigger(data.get("trigger_type", "NONE")),
            trigger_time=datetime.fromisoformat(data["trigger_time"]) if data.get("trigger_time") else None,
            trigger_price=data.get("trigger_price", 0.0),
            trigger_count=data.get("trigger_count", 0),
            entry_price_at_add=data.get("entry_price_at_add", 0.0),
            hod_at_add=data.get("hod_at_add", 0.0),
            current_price=data.get("current_price", 0.0),
            current_hod=data.get("current_hod", 0.0),
            pullback_low=data.get("pullback_low", 0.0),
            last_spread_bps=data.get("last_spread_bps", 0.0),
            nearest_key_level=data.get("nearest_key_level", 0.0),
            next_resistance=data.get("next_resistance", 0.0),
            gap_pct=data.get("gap_pct", 0.0),
            rvol=data.get("rvol", 0.0),
            confidence=data.get("confidence", 0.0),
            catalyst_type=data.get("catalyst_type", ""),
            news_sentiment=data.get("news_sentiment", ""),
            times_triggered=data.get("times_triggered", 0),
        )

    def age_str(self) -> str:
        """Get human-readable age since added."""
        now = datetime.now(ET)
        delta = now - self.added_time
        minutes = int(delta.total_seconds() / 60)
        if minutes < 60:
            return f"{minutes}m"
        hours = minutes // 60
        return f"{hours}h{minutes % 60}m"

    def stage_age_str(self) -> str:
        """Get human-readable time in current stage."""
        now = datetime.now(ET)
        delta = now - self.stage_changed_at
        seconds = int(delta.total_seconds())
        if seconds < 60:
            return f"{seconds}s"
        minutes = seconds // 60
        if minutes < 60:
            return f"{minutes}m"
        hours = minutes // 60
        return f"{hours}h{minutes % 60}m"


@dataclass
class NewsItem:
    """A news article with sentiment and catalyst type."""
    title: str
    published: datetime
    source: str
    sentiment: NewsSentiment
    catalyst_type: CatalystType = CatalystType.UNKNOWN
    url: str = ""

    def age_str(self) -> str:
        """Get human-readable age string."""
        now = datetime.now(ET)
        if self.published.tzinfo is None:
            # Assume UTC if no timezone
            published_et = self.published.replace(tzinfo=ZoneInfo("UTC")).astimezone(ET)
        else:
            published_et = self.published.astimezone(ET)

        delta = now - published_et
        hours = delta.total_seconds() / 3600

        if hours < 1:
            mins = int(delta.total_seconds() / 60)
            return f"{mins}m ago"
        elif hours < 24:
            return f"{int(hours)}h ago"
        else:
            days = int(hours / 24)
            return f"{days}d ago"


@dataclass
class StockCandidate:
    """A stock that passed initial scanner filters."""
    symbol: str
    price: float
    pct_change: float
    volume: int
    rel_volume: float
    shares_outstanding: Optional[int] = None  # NOT true float - see note in config
    premarket_volume: int = 0                  # Pre-market volume (separate tracking)
    dollar_volume: float = 0.0                 # price * volume for liquidity check
    score: float = 0.0


@dataclass
class WatchlistCandidate:
    """A near-miss candidate being watched - close to meeting criteria."""
    symbol: str
    price: float
    pct_change: float
    volume: int
    rel_volume: float
    shares_outstanding: Optional[int] = None  # NOT true float
    float_shares: Optional[int] = None  # True float (if available)

    # What's needed to qualify
    needs_gap_pct: float = 0.0       # Additional gap % needed (0 = meets criteria)
    needs_rvol: float = 0.0          # Additional RVOL needed (0 = meets criteria)
    needs_volume: int = 0            # Additional volume needed (0 = meets criteria)

    # Readiness score (0-100) - how close to qualifying
    readiness_pct: float = 0.0

    # Quality score (0-100) - weighted score for ranking
    quality_score: float = 0.0

    # What's blocking entry
    blocking_reasons: List[str] = field(default_factory=list)

    def get_status_str(self) -> str:
        """Get a short status of what's needed to qualify."""
        parts = []
        if self.needs_gap_pct > 0:
            parts.append(f"Need +{self.needs_gap_pct:.1f}% gap")
        if self.needs_rvol > 0:
            parts.append(f"Need +{self.needs_rvol:.1f}x RVOL")
        if self.needs_volume > 0:
            vol_k = self.needs_volume / 1000
            parts.append(f"Need +{vol_k:.0f}K vol")

        if not parts:
            # All criteria met - likely waiting for pattern/pullback
            return "Awaiting pullback setup"

        # If only one thing needed, show it simply
        if len(parts) == 1:
            return parts[0]

        # Multiple needs - show most important (gap > rvol > volume)
        return parts[0]

    def calculate_quality_score(self) -> float:
        """
        Calculate weighted quality score (0-100) for ranking watchlist candidates.

        Weights:
        - Readiness: 30% - How close to meeting entry criteria
        - RVOL: 25% - Higher relative volume = more interest
        - Float: 20% - Tighter float = more explosive
        - Gap %: 15% - Stronger gap = more momentum
        - Price: 10% - Sweet spot $2-10 = bonus
        """
        score = 0.0

        # 1. Readiness (30%) - already 0-100
        readiness_component = self.readiness_pct * 0.30

        # 2. RVOL (25%) - scale 0-10x to 0-100, cap at 10x
        rvol_normalized = min(self.rel_volume / 10.0, 1.0) * 100
        rvol_component = rvol_normalized * 0.25

        # 3. Shares Outstanding (20%) - lower is better, scale inversely
        # < 1M = 100, 1-5M = 80, 5-10M = 60, 10-15M = 40, > 15M = 20
        # NOTE: This is shares outstanding, not true float
        if self.shares_outstanding is None:
            float_score = 50  # Unknown gets middle score (conservative)
        elif self.shares_outstanding < 1_000_000:
            float_score = 100
        elif self.shares_outstanding < 5_000_000:
            float_score = 80
        elif self.shares_outstanding < 10_000_000:
            float_score = 60
        elif self.shares_outstanding < 15_000_000:
            float_score = 40
        else:
            float_score = 20
        float_component = float_score * 0.20

        # 4. Gap % (15%) - scale 0-50% to 0-100, cap at 50%
        gap_normalized = min(self.pct_change / 50.0, 1.0) * 100
        gap_component = gap_normalized * 0.15

        # 5. Price sweet spot (10%) - $2-10 is ideal
        if 2.0 <= self.price <= 10.0:
            price_score = 100  # Sweet spot
        elif 1.0 <= self.price < 2.0 or 10.0 < self.price <= 20.0:
            price_score = 70   # Acceptable
        else:
            price_score = 40   # Less ideal
        price_component = price_score * 0.10

        # Total score
        score = readiness_component + rvol_component + float_component + gap_component + price_component
        self.quality_score = round(score, 1)
        return self.quality_score


@dataclass
class SetupInfo:
    """Complete setup information for display."""
    symbol: str
    grade: SignalGrade
    pattern: PatternType

    # Current data
    price: float
    gap_pct: float
    rvol: float
    shares_outstanding: Optional[int]  # NOT true float

    # --- KEY LEVEL DATA (NEW) ---
    nearest_key_level: float = 0.0       # Nearest $0.50 or $1.00 level
    distance_to_key_cents: float = 0.0   # Distance in cents
    distance_to_key_pct: float = 0.0     # Distance as percentage
    key_level_state: KeyLevelState = KeyLevelState.ABOVE
    next_resistance: float = 0.0         # Next key level above
    hod: float = 0.0                     # High of day
    pmh: float = 0.0                     # Pre-market high

    # --- EXECUTION REALITY (NEW) ---
    bid: float = 0.0
    ask: float = 0.0
    spread_pct: float = 0.0              # Bid-ask spread as % of price
    dollar_volume: float = 0.0           # price * volume
    float_rotation: float = 0.0          # volume / shares_outstanding
    volume_accel: float = 0.0            # Last 1m volume vs prior 5m avg

    # --- TIME-OF-DAY RVOL (NEW) ---
    rvol_tod: float = 0.0                # Time-normalized RVOL
    premarket_volume: int = 0            # Separate premarket volume

    # --- SHORT-TERM MOMENTUM (1m/5m/15m change) ---
    chg_1m: float = 0.0                  # Price change % over last 1 minute
    chg_5m: float = 0.0                  # Price change % over last 5 minutes
    chg_15m: float = 0.0                 # Price change % over last 15 minutes

    # Pattern details
    pole_pct: float = 0.0
    retrace_pct: float = 0.0
    consol_bars: int = 0

    # Levels (pre-calculated) - now structure-based
    entry_price: float = 0.0
    stop_price: float = 0.0              # Structure stop (flag_low - buffer)
    stop_pct: float = 0.0
    tp1_price: float = 0.0
    tp2_price: float = 0.0
    risk_per_share: float = 0.0
    structure_stop: float = 0.0          # The actual pattern-based stop level
    atr_stop: float = 0.0                # ATR-based stop for comparison

    # Technical
    atr: float = 0.0

    # Price history for mini-chart (last 15 bars)
    price_history: List[float] = field(default_factory=list)

    # News - now with catalyst type
    news_items: List[NewsItem] = field(default_factory=list)
    news_sentiment: NewsSentiment = NewsSentiment.NEUTRAL
    catalyst_type: CatalystType = CatalystType.UNKNOWN
    has_catalyst: bool = False

    # Reddit sentiment
    reddit_mentions: int = 0
    reddit_sentiment: float = 0.0        # -1 to 1 scale
    reddit_sentiment_label: str = ""     # bullish, bearish, neutral
    reddit_trending: bool = False

    # Timestamp
    timestamp: datetime = field(default_factory=lambda: datetime.now(ET))

    # Confidence score 0-1
    confidence: float = 0.0

    # Trigger description for the setup
    trigger_desc: str = ""               # e.g., "Break $4.00", "HOD break", "Micro PB"

    # History tracking (for recently-expired setups)
    expired_minutes_ago: int = 0         # Minutes since setup last qualified (0 = currently active)


# ============================================================
# NEWS FETCHER
# ============================================================

class NewsFetcher:
    """Fetches and analyzes news from Polygon."""

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.cache: Dict[str, Tuple[datetime, List[NewsItem]]] = {}
        self.cache_duration = timedelta(minutes=5)

    def get_news(self, symbol: str) -> List[NewsItem]:
        """Get recent news for a symbol."""
        # Check cache
        if symbol in self.cache:
            cached_time, cached_news = self.cache[symbol]
            if datetime.now(ET) - cached_time < self.cache_duration:
                return cached_news

        # Fetch from API
        news_items = self._fetch_news(symbol)
        self.cache[symbol] = (datetime.now(ET), news_items)
        return news_items

    def _fetch_news(self, symbol: str) -> List[NewsItem]:
        """Fetch news from Polygon API."""
        if not self.api_key:
            return []

        url = "https://api.polygon.io/v2/reference/news"

        # Get news from last 48 hours (extended window for better coverage)
        now = datetime.now(ET)
        published_after = (now - timedelta(hours=NEWS_MAX_AGE_HOURS * 2)).strftime("%Y-%m-%dT%H:%M:%S")

        params = {
            "ticker": symbol,
            "published_utc.gte": published_after,
            "order": "desc",
            "limit": NEWS_MAX_PER_TICKER * 3,  # Fetch extra to filter
            "sort": "published_utc",
            "apiKey": self.api_key
        }

        try:
            resp = requests.get(url, params=params, timeout=10)
            if resp.status_code == 401:
                # API key issue - silently return empty
                return []
            if resp.status_code != 200:
                return []

            data = resp.json()
            results = data.get("results", [])

            news_items = []
            for article in results[:NEWS_MAX_PER_TICKER]:
                title = article.get("title", "")
                published_str = article.get("published_utc", "")
                source = article.get("publisher", {}).get("name", "Unknown")
                article_url = article.get("article_url", "")

                # Parse published date
                try:
                    published = datetime.fromisoformat(published_str.replace("Z", "+00:00"))
                except:
                    published = datetime.now(ET)

                # Analyze sentiment and classify catalyst type
                sentiment = self._analyze_sentiment(title)
                catalyst_type = self._classify_catalyst(title)

                news_items.append(NewsItem(
                    title=title,
                    published=published,
                    source=source,
                    sentiment=sentiment,
                    catalyst_type=catalyst_type,
                    url=article_url
                ))

            return news_items

        except requests.exceptions.Timeout:
            return []
        except requests.exceptions.RequestException:
            return []
        except Exception:
            return []

    def _analyze_sentiment(self, title: str) -> NewsSentiment:
        """Analyze news title for sentiment."""
        title_lower = title.lower()

        # Check for GREAT (very bullish) keywords first
        for keyword in GREAT_KEYWORDS:
            if keyword in title_lower:
                return NewsSentiment.GREAT

        # Check for bearish keywords
        for keyword in BEARISH_KEYWORDS:
            if keyword in title_lower:
                return NewsSentiment.BAD

        # Check for bullish keywords
        for keyword in BULLISH_KEYWORDS:
            if keyword in title_lower:
                return NewsSentiment.GOOD

        return NewsSentiment.NEUTRAL

    def _classify_catalyst(self, title: str) -> CatalystType:
        """Classify the type of catalyst from news title."""
        title_lower = title.lower()

        # Check each catalyst type
        for catalyst_type, keywords in CATALYST_TYPE_KEYWORDS.items():
            for keyword in keywords:
                if keyword in title_lower:
                    return CatalystType[catalyst_type]

        return CatalystType.UNKNOWN

    def get_overall_sentiment(self, news_items: List[NewsItem]) -> Tuple[NewsSentiment, bool, CatalystType]:
        """Get overall sentiment, whether there's a catalyst, and primary catalyst type."""
        if not news_items:
            return NewsSentiment.NEUTRAL, False, CatalystType.UNKNOWN

        # Count sentiments
        great_count = sum(1 for n in news_items if n.sentiment == NewsSentiment.GREAT)
        good_count = sum(1 for n in news_items if n.sentiment == NewsSentiment.GOOD)
        bad_count = sum(1 for n in news_items if n.sentiment == NewsSentiment.BAD)

        # Get primary catalyst type (from most recent news with a known type)
        primary_catalyst = CatalystType.UNKNOWN
        for news in news_items:
            if news.catalyst_type != CatalystType.UNKNOWN:
                primary_catalyst = news.catalyst_type
                break

        # Any GREAT news is a strong catalyst
        if great_count > 0:
            return NewsSentiment.GREAT, True, primary_catalyst

        # Any BAD news is a warning
        if bad_count > 0:
            return NewsSentiment.BAD, True, primary_catalyst

        # Good news is a positive catalyst
        if good_count > 0:
            return NewsSentiment.GOOD, True, primary_catalyst

        return NewsSentiment.NEUTRAL, False, primary_catalyst


# ============================================================
# MARKET DIRECTION TRACKER
# ============================================================

@dataclass
class MarketMomentum:
    """Momentum data for a single market index."""
    symbol: str
    current_price: float
    change_5m_pct: float      # % change over last 5 minutes
    change_15m_pct: float     # % change over last 15 minutes
    direction: str            # "UP", "DOWN", or "FLAT"
    streak_minutes: int       # How many minutes in current direction
    last_update: datetime


class MarketTracker:
    """
    Tracks SPY/QQQ momentum to help time breakout entries.

    Breakouts have higher success rates when the broad market is trending up.
    This tracker provides real-time direction and streak information.
    """

    def __init__(self, polygon_key: str):
        self.polygon_key = polygon_key
        self._cache: Dict[str, Tuple[datetime, MarketMomentum]] = {}
        self._price_history: Dict[str, List[Tuple[datetime, float]]] = {}  # symbol -> [(time, price), ...]
        self._streak_start: Dict[str, Tuple[datetime, str]] = {}  # symbol -> (start_time, direction)

    def get_market_momentum(self) -> List[MarketMomentum]:
        """Get current momentum for all tracked market indices."""
        if not MARKET_TRACKER_ENABLED:
            return []

        results = []
        now = datetime.now(ET)

        for symbol in MARKET_TRACKER_SYMBOLS:
            # Check cache
            if symbol in self._cache:
                cache_time, cached_data = self._cache[symbol]
                if (now - cache_time).total_seconds() < MARKET_TRACKER_CACHE_SEC:
                    results.append(cached_data)
                    continue

            # Fetch fresh data
            momentum = self._fetch_momentum(symbol)
            if momentum:
                self._cache[symbol] = (now, momentum)
                results.append(momentum)

        return results

    def _fetch_momentum(self, symbol: str) -> Optional[MarketMomentum]:
        """Fetch momentum data for a symbol using 1-minute bars."""
        now = datetime.now(ET)
        today = now.date()

        # Fetch today's 1-minute bars
        url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/minute/{today}/{today}"
        params = {
            "adjusted": "true",
            "sort": "asc",
            "limit": 500,
            "apiKey": self.polygon_key
        }

        try:
            resp = requests.get(url, params=params, timeout=10)
            if resp.status_code != 200:
                return None

            results = resp.json().get("results", [])
            if not results or len(results) < 2:
                return None

            # Get the most recent bars
            current_price = results[-1]["c"]
            current_time = datetime.fromtimestamp(results[-1]["t"] / 1000, tz=ET)

            # Calculate 5-minute change
            price_5m_ago = self._get_price_n_minutes_ago(results, 5)
            change_5m_pct = ((current_price - price_5m_ago) / price_5m_ago * 100) if price_5m_ago else 0

            # Calculate 15-minute change
            price_15m_ago = self._get_price_n_minutes_ago(results, 15)
            change_15m_pct = ((current_price - price_15m_ago) / price_15m_ago * 100) if price_15m_ago else 0

            # Determine direction (use 5-minute change as primary)
            if change_5m_pct > 0.05:
                direction = "UP"
            elif change_5m_pct < -0.05:
                direction = "DOWN"
            else:
                direction = "FLAT"

            # Track streak
            streak_minutes = self._update_streak(symbol, direction, current_time)

            # Update price history for streak tracking
            self._update_price_history(symbol, current_time, current_price)

            return MarketMomentum(
                symbol=symbol,
                current_price=current_price,
                change_5m_pct=change_5m_pct,
                change_15m_pct=change_15m_pct,
                direction=direction,
                streak_minutes=streak_minutes,
                last_update=now
            )

        except Exception:
            return None

    def _get_price_n_minutes_ago(self, bars: List[dict], minutes: int) -> Optional[float]:
        """Get price from N minutes ago from bar data."""
        if not bars:
            return None

        now_ts = bars[-1]["t"]
        target_ts = now_ts - (minutes * 60 * 1000)  # Convert minutes to milliseconds

        # Find the bar closest to target time
        for bar in reversed(bars):
            if bar["t"] <= target_ts:
                return bar["c"]

        # If we don't have enough data, use the oldest bar we have
        return bars[0]["c"] if bars else None

    def _update_streak(self, symbol: str, current_direction: str, current_time: datetime) -> int:
        """Update and return the streak duration in minutes."""
        if symbol not in self._streak_start:
            self._streak_start[symbol] = (current_time, current_direction)
            return 0

        start_time, last_direction = self._streak_start[symbol]

        # If direction changed, reset streak
        if current_direction != last_direction and current_direction != "FLAT":
            self._streak_start[symbol] = (current_time, current_direction)
            return 0

        # If currently flat, keep the previous streak info but show 0
        if current_direction == "FLAT":
            return 0

        # Calculate streak duration
        streak_seconds = (current_time - start_time).total_seconds()
        return max(0, int(streak_seconds / 60))

    def _update_price_history(self, symbol: str, time: datetime, price: float):
        """Keep a rolling window of price history."""
        if symbol not in self._price_history:
            self._price_history[symbol] = []

        self._price_history[symbol].append((time, price))

        # Keep only last 30 minutes of data
        cutoff = time - timedelta(minutes=30)
        self._price_history[symbol] = [
            (t, p) for t, p in self._price_history[symbol] if t > cutoff
        ]

    def get_market_status(self) -> Tuple[str, str]:
        """
        Get overall market status and color.

        Returns:
            (status_text, color_code) - e.g., ("BULLISH", Colors.GREEN)
        """
        momentum_list = self.get_market_momentum()
        if not momentum_list:
            return ("UNKNOWN", Colors.GRAY)

        up_count = sum(1 for m in momentum_list if m.direction == "UP")
        down_count = sum(1 for m in momentum_list if m.direction == "DOWN")

        if up_count == len(momentum_list):
            return ("BULLISH", Colors.GREEN)
        elif down_count == len(momentum_list):
            return ("BEARISH", Colors.RED)
        else:
            return ("MIXED", Colors.YELLOW)


def format_market_indicator(tracker: 'MarketTracker') -> str:
    """Format the market direction indicator for display."""
    if not MARKET_TRACKER_ENABLED:
        return ""

    momentum_list = tracker.get_market_momentum()
    if not momentum_list:
        return ""

    parts = []
    for m in momentum_list:
        # Direction arrow and color
        if m.direction == "UP":
            arrow = "▲"
            color = Colors.GREEN
        elif m.direction == "DOWN":
            arrow = "▼"
            color = Colors.RED
        else:
            arrow = "►"
            color = Colors.YELLOW

        # Format: SPY ▲+0.24% (18m)
        change_str = f"+{m.change_5m_pct:.2f}%" if m.change_5m_pct >= 0 else f"{m.change_5m_pct:.2f}%"
        streak_str = f"({m.streak_minutes}m)" if m.streak_minutes > 0 else ""

        part = Colors.colorize(f"{m.symbol} {arrow}{change_str} {streak_str}".strip(), color)
        parts.append(part)

    # Overall status indicator
    status, status_color = tracker.get_market_status()
    status_indicator = Colors.colorize(f"[{status}]", status_color + Colors.BOLD)

    return " | ".join(parts) + f"  {status_indicator}"


# ============================================================
# SCANNER CLASS
# ============================================================

class SmallCapScanner:
    """Scans for small cap momentum setups."""

    def __init__(self):
        self.polygon_key = POLYGON_API_KEY
        self.candidates: List[StockCandidate] = []
        self.setups: List[SetupInfo] = []
        self.watchlist: List[WatchlistCandidate] = []  # Near-miss candidates
        self.last_scan_time: Optional[datetime] = None
        self.scan_count = 0
        self.news_fetcher = NewsFetcher(POLYGON_API_KEY)

        # --- Market Direction Tracker (SPY/QQQ momentum) ---
        self.market_tracker = MarketTracker(POLYGON_API_KEY) if MARKET_TRACKER_ENABLED else None

        # --- Setup History (keep A+/A setups visible for a while after they stop qualifying) ---
        # Key: ticker, Value: (SetupInfo, last_qualified_time)
        self._setup_history: Dict[str, Tuple[SetupInfo, datetime]] = {}

        # --- Reddit Sentiment Provider ---
        self.reddit_provider = None
        self._reddit_cache: Dict[str, RedditStock] = {}
        self._reddit_last_fetch: Optional[datetime] = None
        if ENABLE_REDDIT_SENTIMENT and REDDIT_AVAILABLE:
            try:
                self.reddit_provider = RedditSentimentProvider(
                    cache_ttl_minutes=REDDIT_CACHE_MINUTES,
                    min_mentions=REDDIT_MIN_MENTIONS
                )
                print(f"[INFO] Reddit sentiment provider initialized")
            except Exception as e:
                print(f"[WARN] Failed to initialize Reddit provider: {e}")

        # --- CACHING (to avoid rate limits) ---
        self._avg_volume_cache: Dict[str, Tuple[datetime, float]] = {}
        self._shares_cache: Dict[str, Tuple[datetime, Optional[int]]] = {}
        self._float_cache: Dict[str, Tuple[datetime, Optional[int]]] = {}  # True float from Massive API
        self._snapshot_cache: Dict[str, Tuple[datetime, dict]] = {}
        self._cache_ttl_minutes = 5  # Cache data for 5 minutes

        # --- HOT LIST (monitor A+/A setups for optimal entry) ---
        self._hot_list: Dict[str, HotListEntry] = {}  # symbol -> HotListEntry
        self._hot_list_last_rescan: Optional[datetime] = None
        self._hot_list_file = Path(HOT_LIST_PERSIST_FILE)
        self._hot_list_file.parent.mkdir(parents=True, exist_ok=True)
        self._load_hot_list()  # Load persisted hot list on startup

    # ============================================================
    # HOT LIST MANAGEMENT
    # ============================================================

    def _load_hot_list(self):
        """Load hot list from JSON file on startup."""
        if not HOT_LIST_ENABLED:
            return

        try:
            if self._hot_list_file.exists():
                import json
                with open(self._hot_list_file, "r") as f:
                    data = json.load(f)

                # Only load entries from today (reset daily)
                today = datetime.now(ET).date()
                loaded_count = 0
                for entry_data in data.get("entries", []):
                    try:
                        entry = HotListEntry.from_dict(entry_data)
                        # Only keep if added today
                        if entry.added_time.date() == today:
                            self._hot_list[entry.symbol] = entry
                            loaded_count += 1
                    except Exception as e:
                        print(f"[WARN] Failed to load hot list entry: {e}")

                if loaded_count > 0:
                    print(f"[INFO] Loaded {loaded_count} hot list entries from disk")
        except Exception as e:
            print(f"[WARN] Failed to load hot list: {e}")

    def _save_hot_list(self):
        """Save hot list to JSON file for persistence."""
        if not HOT_LIST_ENABLED:
            return

        try:
            import json
            data = {
                "saved_at": datetime.now(ET).isoformat(),
                "entries": [entry.to_dict() for entry in self._hot_list.values()]
            }
            with open(self._hot_list_file, "w") as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            print(f"[WARN] Failed to save hot list: {e}")

    def add_to_hot_list(self, setup: SetupInfo) -> bool:
        """
        Add a setup to the hot list if it meets criteria.

        Returns True if added, False if already exists or doesn't qualify.
        """
        if not HOT_LIST_ENABLED:
            return False

        # Check minimum grade
        min_grade = SignalGrade.A_PLUS if HOT_LIST_MIN_GRADE == "A+" else SignalGrade.A
        if setup.grade not in (SignalGrade.A_PLUS, SignalGrade.A):
            return False
        if min_grade == SignalGrade.A_PLUS and setup.grade != SignalGrade.A_PLUS:
            return False

        # Already in hot list?
        if setup.symbol in self._hot_list:
            # Update existing entry with fresh data
            self._update_hot_list_entry(setup)
            return False

        # Create new entry
        now = datetime.now(ET)
        entry = HotListEntry(
            symbol=setup.symbol,
            grade=setup.grade,
            added_time=now,
            last_update=now,
            stage=HotListStage.WATCHING,
            stage_changed_at=now,
            entry_price_at_add=setup.price,
            hod_at_add=setup.hod,
            current_price=setup.price,
            current_hod=setup.hod,
            pullback_low=setup.price,  # Initialize to current price
            last_spread_bps=setup.spread_pct * 100,
            nearest_key_level=setup.nearest_key_level,
            next_resistance=setup.next_resistance,
            gap_pct=setup.gap_pct,
            rvol=setup.rvol,
            confidence=setup.confidence,
            catalyst_type=setup.catalyst_type.value if setup.catalyst_type else "",
            news_sentiment=setup.news_sentiment.value if setup.news_sentiment else "",
        )

        self._hot_list[setup.symbol] = entry
        self._save_hot_list()
        return True

    def _update_hot_list_entry(self, setup: SetupInfo):
        """Update an existing hot list entry with fresh data from a scan."""
        if setup.symbol not in self._hot_list:
            return

        entry = self._hot_list[setup.symbol]
        now = datetime.now(ET)

        # Update price tracking
        old_price = entry.current_price
        entry.current_price = setup.price
        entry.current_hod = max(entry.current_hod, setup.hod)
        entry.last_spread_bps = setup.spread_pct * 100
        entry.last_update = now

        # Track pullback low
        if setup.price < entry.pullback_low:
            entry.pullback_low = setup.price

        # Update key levels if price has moved significantly
        entry.nearest_key_level = setup.nearest_key_level
        entry.next_resistance = setup.next_resistance

        # Check for triggers
        self._check_hot_list_triggers(entry, old_price)

    def _check_hot_list_triggers(self, entry: HotListEntry, old_price: float):
        """Check if any entry triggers have fired for a hot list entry."""
        now = datetime.now(ET)
        triggered = False
        trigger_type = HotListTrigger.NONE

        # 1. HOD Break - price breaks above high of day
        if entry.current_hod > entry.hod_at_add:
            hod_break_pct = ((entry.current_price - entry.hod_at_add) / entry.hod_at_add) * 100
            if hod_break_pct >= HOT_LIST_HOD_BREAK_BUFFER_PCT:
                if entry.current_price > entry.hod_at_add and old_price <= entry.hod_at_add:
                    triggered = True
                    trigger_type = HotListTrigger.HOD_BREAK
                    entry.hod_at_add = entry.current_hod  # Update HOD reference

        # 2. Key Level Break - price breaks above key psychological level
        if not triggered and entry.next_resistance > 0:
            key_break_pct = ((entry.current_price - entry.next_resistance) / entry.next_resistance) * 100
            if key_break_pct >= HOT_LIST_KEY_LEVEL_BUFFER_PCT:
                if entry.current_price > entry.next_resistance and old_price <= entry.next_resistance:
                    triggered = True
                    trigger_type = HotListTrigger.KEY_LEVEL

        # 3. Micro Pullback Bounce
        if not triggered and entry.pullback_low < entry.entry_price_at_add:
            pullback_pct = ((entry.entry_price_at_add - entry.pullback_low) / entry.entry_price_at_add) * 100
            if HOT_LIST_MICRO_PB_MIN_PCT <= pullback_pct <= HOT_LIST_MICRO_PB_MAX_PCT:
                bounce_pct = ((entry.current_price - entry.pullback_low) / entry.pullback_low) * 100
                if bounce_pct >= HOT_LIST_MICRO_PB_BOUNCE_PCT and entry.current_price > old_price:
                    triggered = True
                    trigger_type = HotListTrigger.MICRO_PB

        # 4. Spread Tightening
        if not triggered and entry.last_spread_bps <= HOT_LIST_SPREAD_TIGHT_BPS:
            # Only trigger if spread was previously wider
            if entry.stage != HotListStage.TRIGGER:
                triggered = True
                trigger_type = HotListTrigger.SPREAD_TIGHT

        # Handle trigger
        if triggered:
            entry.trigger_type = trigger_type
            entry.trigger_time = now
            entry.trigger_price = entry.current_price
            entry.trigger_count += 1
            entry.times_triggered += 1

            # Transition to TRIGGER stage
            if entry.stage != HotListStage.TRIGGER:
                entry.stage = HotListStage.TRIGGER
                entry.stage_changed_at = now

            self._save_hot_list()

    def rescan_hot_list(self):
        """Re-scan hot list symbols for trigger updates."""
        if not HOT_LIST_ENABLED or not self._hot_list:
            return

        now = datetime.now(ET)

        # Only rescan at configured interval
        if self._hot_list_last_rescan:
            elapsed = (now - self._hot_list_last_rescan).total_seconds()
            if elapsed < HOT_LIST_RESCAN_SEC:
                return

        self._hot_list_last_rescan = now

        # Fetch snapshots for all hot list symbols
        symbols = list(self._hot_list.keys())
        for symbol in symbols:
            entry = self._hot_list.get(symbol)
            if not entry:
                continue

            # Check for stale entries
            age_minutes = (now - entry.added_time).total_seconds() / 60
            if age_minutes > HOT_LIST_MAX_AGE_MINUTES:
                entry.stage = HotListStage.REMOVED
                del self._hot_list[symbol]
                continue

            # Fetch current snapshot
            snapshot = self._get_symbol_snapshot(symbol)
            if not snapshot:
                continue

            # Update entry with fresh data
            old_price = entry.current_price
            day_data = snapshot.get("day", {})
            entry.current_price = day_data.get("c", entry.current_price)
            entry.current_hod = day_data.get("h", entry.current_hod)

            # Get spread from quote if available
            quote = snapshot.get("lastQuote", {})
            if quote:
                bid = quote.get("p", 0)
                ask = quote.get("P", 0)
                if bid > 0 and ask > 0:
                    spread_pct = ((ask - bid) / ask) * 100
                    entry.last_spread_bps = spread_pct * 100

            # Track pullback low
            if entry.current_price < entry.pullback_low:
                entry.pullback_low = entry.current_price

            # Check triggers
            self._check_hot_list_triggers(entry, old_price)

            # Age out TRIGGER stage after some time
            if entry.stage == HotListStage.TRIGGER:
                trigger_age = (now - entry.stage_changed_at).total_seconds()
                if trigger_age > 300:  # 5 minutes
                    entry.stage = HotListStage.COOLING
                    entry.stage_changed_at = now

            entry.last_update = now

        self._save_hot_list()

    def _get_symbol_snapshot(self, symbol: str) -> Optional[dict]:
        """Get snapshot data for a symbol (with caching)."""
        now = datetime.now(ET)

        # Check cache
        if symbol in self._snapshot_cache:
            cached_time, cached_data = self._snapshot_cache[symbol]
            cache_age = (now - cached_time).total_seconds()
            if cache_age < 30:  # 30 second cache for hot list
                return cached_data

        # Fetch from API
        url = f"https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers/{symbol}"
        params = {"apiKey": self.polygon_key}

        try:
            resp = requests.get(url, params=params, timeout=5)
            if resp.status_code == 200:
                data = resp.json().get("ticker", {})
                self._snapshot_cache[symbol] = (now, data)
                return data
        except Exception:
            pass

        return None

    def get_hot_list(self) -> List[HotListEntry]:
        """Get all hot list entries sorted by stage priority and trigger time."""
        if not HOT_LIST_ENABLED:
            return []

        # Sort: TRIGGER first, then by recency
        stage_priority = {
            HotListStage.TRIGGER: 0,
            HotListStage.SETUP: 1,
            HotListStage.CONSOLIDATING: 2,
            HotListStage.WATCHING: 3,
            HotListStage.COOLING: 4,
            HotListStage.REMOVED: 5,
        }

        entries = [e for e in self._hot_list.values() if e.stage != HotListStage.REMOVED]
        entries.sort(key=lambda e: (stage_priority.get(e.stage, 99), -e.times_triggered, e.added_time))
        return entries

    def remove_from_hot_list(self, symbol: str):
        """Remove a symbol from the hot list."""
        if symbol in self._hot_list:
            del self._hot_list[symbol]
            self._save_hot_list()

    # ============================================================
    # KEY LEVEL ENGINE
    # ============================================================

    @staticmethod
    def _get_nearest_key_level(price: float) -> Tuple[float, float, float]:
        """
        Get the nearest psychological key level ($0.50 or $1.00 intervals).

        Returns: (nearest_level, distance_cents, next_resistance)
        """
        # For prices under $10, use $0.50 intervals
        # For prices $10+, use $1.00 intervals
        if price < 10:
            interval = 0.50
        else:
            interval = 1.00

        # Find nearest level below and above
        level_below = (price // interval) * interval
        level_above = level_below + interval

        # Which is closer?
        dist_below = price - level_below
        dist_above = level_above - price

        if dist_below <= dist_above:
            nearest = level_below
            distance_cents = dist_below * 100
        else:
            nearest = level_above
            distance_cents = -dist_above * 100  # Negative = above the level

        # Next resistance is always the level above current price
        next_resistance = level_above

        return nearest, distance_cents, next_resistance

    @staticmethod
    def _determine_key_level_state(price: float, nearest_level: float,
                                    recent_prices: List[float]) -> KeyLevelState:
        """
        Determine if price is reclaiming, rejecting, or holding a key level.

        Args:
            price: Current price
            nearest_level: The nearest key level
            recent_prices: Last 5-10 price points for context
        """
        if not recent_prices or len(recent_prices) < 3:
            # Not enough data, just report position
            pct_from_level = abs(price - nearest_level) / nearest_level
            if pct_from_level < 0.01:  # Within 1%
                return KeyLevelState.AT
            return KeyLevelState.ABOVE if price > nearest_level else KeyLevelState.BELOW

        # Check recent price action relative to level
        pct_from_level = abs(price - nearest_level) / nearest_level

        # At the level (within 1%)
        if pct_from_level < 0.01:
            # Check if coming from below or above
            avg_recent = sum(recent_prices[-5:]) / len(recent_prices[-5:])
            if avg_recent < nearest_level and price >= nearest_level:
                return KeyLevelState.RECLAIMING
            elif avg_recent > nearest_level and price <= nearest_level:
                return KeyLevelState.REJECTING
            return KeyLevelState.AT

        # Above the level
        if price > nearest_level:
            # Check if we were recently below
            below_count = sum(1 for p in recent_prices[-5:] if p < nearest_level)
            if below_count >= 2:
                return KeyLevelState.RECLAIMING
            return KeyLevelState.ABOVE

        # Below the level
        else:
            # Check if we were recently above
            above_count = sum(1 for p in recent_prices[-5:] if p > nearest_level)
            if above_count >= 2:
                return KeyLevelState.REJECTING
            return KeyLevelState.BELOW

    # ============================================================
    # REDDIT SENTIMENT
    # ============================================================

    def _refresh_reddit_cache(self):
        """Refresh Reddit trending data if stale."""
        if not self.reddit_provider:
            return

        now = datetime.now()
        if (self._reddit_last_fetch and
            (now - self._reddit_last_fetch).total_seconds() < REDDIT_CACHE_MINUTES * 60):
            return  # Cache still fresh

        try:
            trending = self.reddit_provider.get_trending_tickers(limit=100)
            self._reddit_cache = {stock.ticker: stock for stock in trending}
            self._reddit_last_fetch = now
        except Exception as e:
            print(f"[WARN] Reddit fetch failed: {e}")

    def _get_reddit_data(self, symbol: str) -> Optional[RedditStock]:
        """Get Reddit data for a symbol."""
        if not self.reddit_provider:
            return None

        self._refresh_reddit_cache()
        return self._reddit_cache.get(symbol.upper())

    def _apply_reddit_sentiment(self, setup: SetupInfo) -> float:
        """
        Apply Reddit sentiment to a setup. Returns confidence adjustment.

        - Trending on Reddit: +10% confidence
        - Bullish sentiment: +15% confidence
        - Bearish sentiment: -10% confidence
        """
        reddit_data = self._get_reddit_data(setup.symbol)
        if not reddit_data:
            return 0.0

        # Update setup with Reddit data
        setup.reddit_mentions = reddit_data.mentions
        setup.reddit_sentiment = reddit_data.sentiment
        setup.reddit_sentiment_label = reddit_data.sentiment_label
        setup.reddit_trending = reddit_data.mentions >= REDDIT_MIN_MENTIONS

        # Calculate confidence adjustment
        adjustment = 0.0

        # Boost for trending
        if setup.reddit_trending:
            adjustment += REDDIT_TRENDING_BOOST

        # Additional boost/penalty based on sentiment
        if reddit_data.is_bullish:
            adjustment += REDDIT_BULLISH_BOOST
        elif reddit_data.is_bearish:
            adjustment -= REDDIT_BEARISH_PENALTY

        return adjustment

    def scan(self) -> Tuple[List[SetupInfo], List[SetupInfo]]:
        """Run a full scan and return (current_setups, recently_expired_setups)."""
        self.scan_count += 1
        self.last_scan_time = datetime.now(ET)
        now = datetime.now(ET)

        # Step 1: Get top gainers
        gainers = self._get_top_gainers()
        if not gainers:
            # Even with no gainers, return any recently-expired setups
            expired = self._get_expired_setups()
            return [], expired

        # Step 2: Filter candidates and track near-misses
        self.candidates = []
        self.watchlist = []
        for g in gainers:
            candidate = self._evaluate_candidate(g)
            if candidate:
                self.candidates.append(candidate)
            else:
                # Check if it's a near-miss worth watching
                watchlist_candidate = self._evaluate_watchlist_candidate(g)
                if watchlist_candidate:
                    self.watchlist.append(watchlist_candidate)

        # Step 3: Analyze patterns and get news for each candidate
        self.setups = []
        current_tickers = set()
        for candidate in self.candidates[:15]:  # Limit API calls
            setup = self._analyze_setup(candidate)
            if setup and setup.grade != SignalGrade.C:
                # Get news for this setup
                news_items = self.news_fetcher.get_news(candidate.symbol)
                setup.news_items = news_items
                sentiment, has_catalyst, catalyst_type = self.news_fetcher.get_overall_sentiment(news_items)
                setup.news_sentiment = sentiment
                setup.has_catalyst = has_catalyst
                setup.catalyst_type = catalyst_type

                # Apply Reddit sentiment and adjust confidence
                reddit_adjustment = self._apply_reddit_sentiment(setup)
                if reddit_adjustment != 0:
                    setup.confidence = min(1.0, max(0.0, setup.confidence + reddit_adjustment))

                self.setups.append(setup)
                current_tickers.add(setup.symbol)

                # Update history for A+ and A setups
                if setup.grade in (SignalGrade.A_PLUS, SignalGrade.A):
                    self._setup_history[setup.symbol] = (setup, now)

        # Sort by grade, then by news sentiment, then confidence
        grade_order = {SignalGrade.A_PLUS: 0, SignalGrade.A: 1, SignalGrade.B: 2, SignalGrade.C: 3}
        sentiment_order = {NewsSentiment.GREAT: 0, NewsSentiment.GOOD: 1,
                          NewsSentiment.NEUTRAL: 2, NewsSentiment.BAD: 3}

        self.setups.sort(key=lambda s: (
            grade_order[s.grade],
            sentiment_order[s.news_sentiment],
            -s.confidence
        ))

        # Get recently-expired setups (A+/A that no longer qualify but are within history window)
        expired = self._get_expired_setups(exclude_tickers=current_tickers)

        return self.setups[:MAX_DISPLAY_SETUPS], expired

    def _get_expired_setups(self, exclude_tickers: set = None) -> List[SetupInfo]:
        """Get setups that recently expired (no longer qualifying but within history window)."""
        if not SHOW_EXPIRED_SETUPS:
            return []

        now = datetime.now(ET)
        expired = []
        to_remove = []

        for ticker, (setup, last_qualified) in self._setup_history.items():
            # Skip if currently qualifying
            if exclude_tickers and ticker in exclude_tickers:
                continue

            # Check if within history window
            age_minutes = (now - last_qualified).total_seconds() / 60
            if age_minutes <= SETUP_HISTORY_MINUTES:
                # Mark how long ago it was last seen
                setup.expired_minutes_ago = int(age_minutes)
                expired.append(setup)
            else:
                # Too old, mark for removal
                to_remove.append(ticker)

        # Clean up old entries
        for ticker in to_remove:
            del self._setup_history[ticker]

        # Sort by how recently they expired (most recent first)
        expired.sort(key=lambda s: getattr(s, 'expired_minutes_ago', 999))

        return expired

    def _get_top_gainers(self) -> List[dict]:
        """Fetch top gainers from Polygon."""
        url = "https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/gainers"
        params = {"apiKey": self.polygon_key}

        try:
            resp = requests.get(url, params=params, timeout=10)
            if resp.status_code != 200:
                return []
            return resp.json().get("tickers", [])
        except Exception:
            return []

    def _evaluate_candidate(self, ticker_data: dict) -> Optional[StockCandidate]:
        """Evaluate if a ticker meets criteria."""
        try:
            ticker = ticker_data.get("ticker", "")
            day_data = ticker_data.get("day", {})
            prev_day = ticker_data.get("prevDay", {})

            price = day_data.get("c", 0)
            volume = day_data.get("v", 0)
            prev_close = prev_day.get("c", 0)

            if prev_close <= 0 or price <= 0:
                return None

            pct_change = ((price - prev_close) / prev_close) * 100

            # Filters
            if price < MIN_PRICE or price > MAX_PRICE:
                return None
            if pct_change < MIN_PCT_CHANGE:
                return None
            if volume < MIN_ABSOLUTE_VOLUME:
                return None

            # RVOL
            avg_volume = self._get_avg_volume(ticker)
            if avg_volume <= 0:
                return None
            rel_volume = volume / avg_volume
            if rel_volume < MIN_RELATIVE_VOLUME:
                return None

            # Get true float (falls back to shares_outstanding if unavailable)
            true_float = self._get_true_float(ticker)
            if true_float and true_float > MAX_SHARES_OUTSTANDING:
                return None

            # Also get shares_outstanding for reference
            shares_outstanding = self._get_shares_outstanding(ticker)

            # Calculate dollar volume
            dollar_volume = price * volume

            # Score - use true float for scoring
            score = self._calculate_score(price, pct_change, rel_volume, true_float)

            return StockCandidate(
                symbol=ticker,
                price=price,
                pct_change=pct_change,
                volume=volume,
                rel_volume=rel_volume,
                shares_outstanding=true_float,  # Use true float as primary metric
                dollar_volume=dollar_volume,
                score=score
            )
        except Exception:
            return None

    def _evaluate_watchlist_candidate(self, ticker_data: dict) -> Optional[WatchlistCandidate]:
        """Evaluate if a ticker is close to meeting criteria (near-miss)."""
        try:
            ticker = ticker_data.get("ticker", "")
            day_data = ticker_data.get("day", {})
            prev_day = ticker_data.get("prevDay", {})

            price = day_data.get("c", 0)
            volume = day_data.get("v", 0)
            prev_close = prev_day.get("c", 0)

            if prev_close <= 0 or price <= 0:
                return None

            pct_change = ((price - prev_close) / prev_close) * 100

            # Must be in valid price range
            if price < MIN_PRICE or price > MAX_PRICE:
                return None

            # Must have at least 5% gap (half of min requirement)
            if pct_change < MIN_PCT_CHANGE * 0.5:
                return None

            # Get average volume for RVOL calculation
            avg_volume = self._get_avg_volume(ticker)
            rel_volume = volume / avg_volume if avg_volume > 0 else 0

            # Must have at least half the required RVOL
            if rel_volume < MIN_RELATIVE_VOLUME * 0.4:
                return None

            # Get true float (allow slightly higher for watchlist)
            true_float = self._get_true_float(ticker)
            if true_float and true_float > MAX_SHARES_OUTSTANDING * 1.5:
                return None

            # Also get shares_outstanding for reference
            shares_outstanding = self._get_shares_outstanding(ticker)

            # Calculate what's needed to qualify
            blocking_reasons = []
            needs_gap = max(0, MIN_PCT_CHANGE - pct_change)
            needs_rvol = max(0, MIN_RELATIVE_VOLUME - rel_volume)
            needs_vol = max(0, MIN_ABSOLUTE_VOLUME - volume)

            if needs_gap > 0:
                blocking_reasons.append(f"Gap: {pct_change:.1f}% (need {MIN_PCT_CHANGE:.0f}%)")
            if needs_rvol > 0:
                blocking_reasons.append(f"RVOL: {rel_volume:.1f}x (need {MIN_RELATIVE_VOLUME:.0f}x)")
            if needs_vol > 0:
                blocking_reasons.append(f"Volume: {volume/1000:.0f}K (need {MIN_ABSOLUTE_VOLUME/1000:.0f}K)")

            # Calculate readiness percentage (how close to qualifying)
            gap_readiness = min(100, (pct_change / MIN_PCT_CHANGE) * 100)
            rvol_readiness = min(100, (rel_volume / MIN_RELATIVE_VOLUME) * 100)
            vol_readiness = min(100, (volume / MIN_ABSOLUTE_VOLUME) * 100)

            # Weighted average - gap and rvol are more important
            readiness_pct = (gap_readiness * 0.4 + rvol_readiness * 0.4 + vol_readiness * 0.2)

            # Only include if at least 60% ready
            if readiness_pct < 60:
                return None

            candidate = WatchlistCandidate(
                symbol=ticker,
                price=price,
                pct_change=pct_change,
                volume=volume,
                rel_volume=rel_volume,
                shares_outstanding=shares_outstanding,
                float_shares=true_float,  # Use true float from Massive API
                needs_gap_pct=needs_gap,
                needs_rvol=needs_rvol,
                needs_volume=int(needs_vol),
                readiness_pct=readiness_pct,
                blocking_reasons=blocking_reasons
            )

            # Calculate quality score for ranking
            candidate.calculate_quality_score()

            return candidate
        except Exception:
            return None

    def _get_avg_volume(self, symbol: str) -> float:
        """Get 20-day average volume with caching."""
        # Check cache first
        if symbol in self._avg_volume_cache:
            cached_time, cached_value = self._avg_volume_cache[symbol]
            if datetime.now(ET) - cached_time < timedelta(minutes=self._cache_ttl_minutes):
                return cached_value

        end_date = datetime.now(ET).date()
        start_date = end_date - timedelta(days=30)

        url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/day/{start_date}/{end_date}"
        params = {"adjusted": "true", "sort": "desc", "limit": 20, "apiKey": self.polygon_key}

        try:
            resp = requests.get(url, params=params, timeout=10)
            if resp.status_code != 200:
                return 0
            results = resp.json().get("results", [])
            if len(results) < 5:
                return 0
            volumes = [r.get("v", 0) for r in results]
            avg_vol = sum(volumes) / len(volumes)

            # Cache the result
            self._avg_volume_cache[symbol] = (datetime.now(ET), avg_vol)
            return avg_vol
        except Exception:
            return 0

    def _get_shares_outstanding(self, symbol: str) -> Optional[int]:
        """Get shares outstanding (NOT true float) with caching."""
        # Check cache first
        if symbol in self._shares_cache:
            cached_time, cached_value = self._shares_cache[symbol]
            if datetime.now(ET) - cached_time < timedelta(minutes=self._cache_ttl_minutes):
                return cached_value

        url = f"https://api.polygon.io/v3/reference/tickers/{symbol}"
        params = {"apiKey": self.polygon_key}

        try:
            resp = requests.get(url, params=params, timeout=10)
            if resp.status_code != 200:
                return None
            results = resp.json().get("results", {})
            shares = results.get("share_class_shares_outstanding")

            # Cache the result
            self._shares_cache[symbol] = (datetime.now(ET), shares)
            return shares
        except Exception:
            return None

    def _get_true_float(self, symbol: str) -> Optional[int]:
        """Get true float (free float) from Massive/Polygon API with caching.

        This returns the actual number of shares freely tradable in the market,
        excluding insider holdings, restricted shares, etc.
        Falls back to shares_outstanding if float endpoint unavailable.
        """
        # Check cache first
        if symbol in self._float_cache:
            cached_time, cached_value = self._float_cache[symbol]
            if datetime.now(ET) - cached_time < timedelta(minutes=self._cache_ttl_minutes):
                return cached_value

        # Try Massive/Polygon float endpoint first (shorter timeout for speed)
        url = "https://api.polygon.io/stocks/vX/float"
        params = {
            "apiKey": self.polygon_key,
            "ticker": symbol
        }

        try:
            resp = requests.get(url, params=params, timeout=3)  # Short timeout
            if resp.status_code == 200:
                results = resp.json().get("results", [])
                if results and len(results) > 0:
                    free_float = results[0].get("free_float")
                    if free_float:
                        self._float_cache[symbol] = (datetime.now(ET), free_float)
                        return free_float
        except Exception:
            pass  # Silently fall through to shares_outstanding

        # Fallback: use shares_outstanding (already has its own cache)
        fallback = self._get_shares_outstanding(symbol)
        self._float_cache[symbol] = (datetime.now(ET), fallback)
        return fallback

    def _get_snapshot_data(self, symbol: str) -> Optional[dict]:
        """Get snapshot data with caching (for bid/ask, etc)."""
        # Check cache first
        if symbol in self._snapshot_cache:
            cached_time, cached_value = self._snapshot_cache[symbol]
            # Shorter TTL for real-time data (1 minute)
            if datetime.now(ET) - cached_time < timedelta(minutes=1):
                return cached_value

        url = f"https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers/{symbol}"
        params = {"apiKey": self.polygon_key}

        try:
            resp = requests.get(url, params=params, timeout=10)
            if resp.status_code != 200:
                return None
            snapshot = resp.json().get("ticker", {})

            # Cache the result
            self._snapshot_cache[symbol] = (datetime.now(ET), snapshot)
            return snapshot
        except Exception:
            return None

    def _calculate_score(self, price: float, pct_change: float,
                        rel_volume: float, float_shares: Optional[int]) -> float:
        """Calculate composite score."""
        score = 0.0
        score += min(pct_change, 100) * 0.4
        score += min(rel_volume, 20) * 1.5
        if SWEET_SPOT_MIN <= price <= SWEET_SPOT_MAX:
            score += 15
        if float_shares:
            if float_shares < 2_000_000:
                score += 15
            elif float_shares < 5_000_000:
                score += 10
        return score

    def _analyze_setup(self, candidate: StockCandidate) -> Optional[SetupInfo]:
        """Analyze a candidate for pattern setups with enhanced features."""
        # Get intraday bars
        df = self._get_intraday_bars(candidate.symbol)
        if df is None or len(df) < 20:
            return None

        # --- KEY LEVEL ANALYSIS ---
        nearest_level, distance_cents, next_resistance = self._get_nearest_key_level(candidate.price)
        distance_pct = (abs(distance_cents) / 100) / candidate.price * 100 if candidate.price > 0 else 0
        recent_prices = df["close"].tail(10).tolist()
        key_level_state = self._determine_key_level_state(candidate.price, nearest_level, recent_prices)

        # --- SHORT-TERM MOMENTUM (1m/5m/15m change) ---
        current_price = df["close"].iloc[-1]
        chg_1m = 0.0
        chg_5m = 0.0
        chg_15m = 0.0
        if len(df) >= 2:
            price_1m_ago = df["close"].iloc[-2]
            chg_1m = ((current_price - price_1m_ago) / price_1m_ago) * 100 if price_1m_ago > 0 else 0.0
        if len(df) >= 5:
            price_5m_ago = df["close"].iloc[-5]
            chg_5m = ((current_price - price_5m_ago) / price_5m_ago) * 100 if price_5m_ago > 0 else 0.0
        if len(df) >= 15:
            price_15m_ago = df["close"].iloc[-15]
            chg_15m = ((current_price - price_15m_ago) / price_15m_ago) * 100 if price_15m_ago > 0 else 0.0

        # --- HOD / PMH ---
        hod = df["high"].max()  # High of day from intraday data
        # PMH would require pre-market data - approximate with early bars
        now = datetime.now(ET)
        if now.time() < RTH_START:
            pmh = hod  # In premarket, PMH = current HOD
        else:
            # Try to get pre-market high from early bars (before 9:30)
            premarket_mask = df["timestamp"].dt.tz_convert(ET).dt.time < RTH_START
            if premarket_mask.any():
                pmh = df.loc[premarket_mask, "high"].max()
            else:
                pmh = hod * 0.98  # Fallback estimate

        # --- PATTERN DETECTION ---
        # Try bull flag first
        pattern_info = self._detect_bull_flag(df)

        # Try micro pullback if no bull flag
        if pattern_info is None:
            pattern_info = self._detect_micro_pullback(df, nearest_level)

        # Try HOD/PMH break
        if pattern_info is None:
            pattern_info = self._detect_hod_break(df, hod, pmh)

        # Default if no pattern found
        if pattern_info is None:
            pattern_info = {
                "pattern": PatternType.NONE,
                "pole_pct": 0,
                "retrace_pct": 0,
                "consol_bars": 0,
                "entry_price": candidate.price,
                "flag_high": candidate.price,
                "flag_low": candidate.price * 0.97,
                "trigger_desc": "Gap momentum"
            }

        # --- STRUCTURE-BASED STOP (primary) vs ATR STOP (cap) ---
        atr = self._calculate_atr(df)
        entry_price = pattern_info["entry_price"]

        # Structure stop = pattern low - small buffer
        structure_stop = pattern_info["flag_low"] - (atr * 0.5)  # Half ATR buffer below pattern low

        # ATR stop = 4x ATR (as sanity cap)
        atr_stop = entry_price - (atr * ATR_STOP_MULT)

        # Use structure stop unless it's too wide (> ATR cap)
        # If structure stop implies > 8% risk, use ATR stop instead
        structure_risk_pct = (entry_price - structure_stop) / entry_price * 100
        if structure_risk_pct > 8.0:
            stop_price = atr_stop
            stop_pct = (entry_price - atr_stop) / entry_price * 100
        else:
            stop_price = structure_stop
            stop_pct = structure_risk_pct

        # Minimum stop distance
        min_stop_distance = entry_price * (MIN_STOP_DISTANCE_PCT / 100)
        if entry_price - stop_price < min_stop_distance:
            stop_price = entry_price - min_stop_distance
            stop_pct = MIN_STOP_DISTANCE_PCT

        risk_per_share = entry_price - stop_price
        tp1_price = entry_price + (risk_per_share * TP1_R_MULTIPLE)
        tp2_price = entry_price + (risk_per_share * TP2_R_MULTIPLE)

        # --- EXECUTION REALITY (bid/ask, spread, dollar volume) ---
        snapshot = self._get_snapshot_data(candidate.symbol)
        bid, ask, spread_pct = 0.0, 0.0, 0.0
        if snapshot:
            last_quote = snapshot.get("lastQuote", {})
            bid = last_quote.get("p", 0) or last_quote.get("P", 0) or 0
            ask = last_quote.get("P", 0) or last_quote.get("p", 0) or 0
            if bid > 0 and ask > 0 and ask > bid:
                spread_pct = (ask - bid) / ((ask + bid) / 2) * 100
            elif candidate.price > 0:
                # Estimate spread if not available
                spread_pct = 0.5 if candidate.price > 5 else 1.0

        # Dollar volume
        dollar_volume = candidate.price * candidate.volume

        # Float rotation (volume / shares outstanding)
        float_rotation = 0.0
        if candidate.shares_outstanding and candidate.shares_outstanding > 0:
            float_rotation = candidate.volume / candidate.shares_outstanding

        # Volume acceleration (last bar vs prior 5 bars avg)
        volume_accel = 1.0
        if len(df) >= 6:
            last_vol = df["volume"].iloc[-1]
            prior_avg = df["volume"].iloc[-6:-1].mean()
            if prior_avg > 0:
                volume_accel = last_vol / prior_avg

        # Calculate ADX for trend strength (required for A+ grade)
        adx = self._calculate_adx(df)

        # --- GRADE THE SETUP (with execution quality + ADX) ---
        grade = self._grade_setup(
            gap_pct=candidate.pct_change,
            rvol=candidate.rel_volume,
            pole_pct=pattern_info["pole_pct"],
            retrace_pct=pattern_info["retrace_pct"],
            pattern=pattern_info["pattern"],
            chg_1m=chg_1m,
            chg_5m=chg_5m,
            chg_15m=chg_15m,
            spread_pct=spread_pct,
            volume_accel=volume_accel,
            adx=adx
        )

        # --- CONFIDENCE SCORE (with execution quality) ---
        confidence = self._calculate_confidence(
            pattern_info["pattern"],
            candidate.rel_volume,
            pattern_info["pole_pct"],
            pattern_info["retrace_pct"],
            candidate.shares_outstanding,
            float_rotation=float_rotation,
            volume_accel=volume_accel,
            spread_pct=spread_pct
        )

        # --- TRIGGER DESCRIPTION ---
        trigger_desc = pattern_info.get("trigger_desc", "")
        if not trigger_desc:
            if key_level_state == KeyLevelState.RECLAIMING:
                trigger_desc = f"Reclaim ${nearest_level:.2f}"
            elif pattern_info["pattern"] == PatternType.BULL_FLAG:
                trigger_desc = f"Flag break ${entry_price:.2f}"
            else:
                trigger_desc = "Gap momentum"

        # Price history for mini-chart
        price_history = df["close"].tail(15).tolist()

        return SetupInfo(
            symbol=candidate.symbol,
            grade=grade,
            pattern=pattern_info["pattern"],
            price=candidate.price,
            gap_pct=candidate.pct_change,
            rvol=candidate.rel_volume,
            shares_outstanding=candidate.shares_outstanding,

            # Key levels
            nearest_key_level=nearest_level,
            distance_to_key_cents=distance_cents,
            distance_to_key_pct=distance_pct,
            key_level_state=key_level_state,
            next_resistance=next_resistance,
            hod=hod,
            pmh=pmh,

            # Execution reality
            bid=bid,
            ask=ask,
            spread_pct=spread_pct,
            dollar_volume=dollar_volume,
            float_rotation=float_rotation,
            volume_accel=volume_accel,

            # Short-term momentum
            chg_1m=chg_1m,
            chg_5m=chg_5m,
            chg_15m=chg_15m,

            # Pattern details
            pole_pct=pattern_info["pole_pct"],
            retrace_pct=pattern_info["retrace_pct"],
            consol_bars=pattern_info["consol_bars"],

            # Levels - structure-based
            entry_price=entry_price,
            stop_price=stop_price,
            stop_pct=stop_pct,
            tp1_price=tp1_price,
            tp2_price=tp2_price,
            risk_per_share=risk_per_share,
            structure_stop=structure_stop,
            atr_stop=atr_stop,
            atr=atr,

            # Chart
            price_history=price_history,

            # Trigger
            trigger_desc=trigger_desc,
            confidence=confidence
        )

    def _get_intraday_bars(self, symbol: str) -> Optional[pd.DataFrame]:
        """Get 1-minute intraday bars."""
        now = datetime.now(ET)
        today = now.date()

        url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/minute/{today}/{today}"
        params = {
            "adjusted": "true",
            "sort": "asc",
            "limit": 500,
            "apiKey": self.polygon_key
        }

        try:
            resp = requests.get(url, params=params, timeout=10)
            if resp.status_code != 200:
                return None

            results = resp.json().get("results", [])
            if not results:
                return None

            df = pd.DataFrame(results)
            df.columns = [c.lower() for c in df.columns]
            df["timestamp"] = pd.to_datetime(df["t"], unit="ms", utc=True)
            df = df.rename(columns={"o": "open", "h": "high", "l": "low", "c": "close", "v": "volume"})

            return df
        except Exception:
            return None

    def _detect_bull_flag(self, df: pd.DataFrame) -> Optional[dict]:
        """Detect bull flag pattern."""
        if len(df) < 20:
            return None

        try:
            segment = df.iloc[-15:-5]
            if len(segment) < 5:
                return None

            pole_start_idx = segment["low"].idxmin()
            pole_low = df.loc[pole_start_idx, "low"]

            after_low = df.loc[pole_start_idx:]
            if len(after_low) < 5:
                return None

            pole_high_segment = after_low.iloc[:-3]
            if len(pole_high_segment) < 2:
                return None

            pole_high = pole_high_segment["high"].max()
            pole_high_idx = pole_high_segment["high"].idxmax()

            pole_move = ((pole_high - pole_low) / pole_low) * 100

            if pole_move < FLAG_POLE_MIN_PCT:
                return None

            flag_bars = df.loc[pole_high_idx:]
            if len(flag_bars) < FLAG_MIN_BARS or len(flag_bars) > FLAG_MAX_BARS + 3:
                return None

            flag_high = flag_bars["high"].max()
            flag_low = flag_bars["low"].min()

            if flag_high > pole_high * 1.02:
                return None

            if pole_high - pole_low <= 0:
                return None
            retracement_pct = ((pole_high - flag_low) / (pole_high - pole_low)) * 100

            if retracement_pct > FLAG_MAX_RETRACE_PCT + 10:
                return None

            entry_price = flag_high + 0.02

            return {
                "pattern": PatternType.BULL_FLAG,
                "pole_pct": pole_move,
                "retrace_pct": retracement_pct,
                "consol_bars": len(flag_bars),
                "entry_price": entry_price,
                "flag_high": flag_high,
                "flag_low": flag_low,
                "trigger_desc": f"Flag break ${entry_price:.2f}"
            }
        except Exception:
            return None

    def _detect_micro_pullback(self, df: pd.DataFrame, key_level: float) -> Optional[dict]:
        """
        Detect micro pullback pattern near a key level.

        A micro pullback is:
        - Price ran up to/through a key level
        - Small pullback (1-3 bars, < 2% retrace)
        - Now curling back up near the level
        """
        if len(df) < 10:
            return None

        try:
            recent = df.iloc[-10:]
            current_price = recent["close"].iloc[-1]
            recent_high = recent["high"].max()
            recent_low = recent["low"].iloc[-5:].min()  # Low in last 5 bars

            # Must have made a recent high near or above key level
            if recent_high < key_level * 0.98:
                return None

            # Current price should be within 2% of recent high (tight pullback)
            pullback_pct = (recent_high - current_price) / recent_high * 100
            if pullback_pct > 3.0 or pullback_pct < 0.3:
                return None

            # Check for curl pattern (last 2-3 bars making higher lows)
            last_3_lows = recent["low"].iloc[-3:].tolist()
            if not (last_3_lows[-1] >= last_3_lows[-2] * 0.998):  # Allow tiny tolerance
                return None

            # Entry just above recent candle high
            entry_price = recent["high"].iloc[-1] + 0.02

            return {
                "pattern": PatternType.CONSOLIDATION,
                "pole_pct": (recent_high - recent["low"].min()) / recent["low"].min() * 100,
                "retrace_pct": pullback_pct,
                "consol_bars": 3,
                "entry_price": entry_price,
                "flag_high": recent["high"].iloc[-1],
                "flag_low": recent_low,
                "trigger_desc": f"Micro PB ${key_level:.2f}"
            }
        except Exception:
            return None

    def _detect_hod_break(self, df: pd.DataFrame, hod: float, pmh: float) -> Optional[dict]:
        """
        Detect HOD (High of Day) or PMH (Pre-Market High) break setup.

        A breakout setup where price is consolidating just below HOD/PMH
        and preparing to break.
        """
        if len(df) < 10:
            return None

        try:
            current_price = df["close"].iloc[-1]
            recent_high = df["high"].iloc[-5:].max()

            # Determine which level is more relevant
            # If current price is closer to PMH, use PMH; else use HOD
            dist_to_hod = abs(current_price - hod) / hod
            dist_to_pmh = abs(current_price - pmh) / pmh

            if dist_to_pmh < dist_to_hod and pmh > 0:
                target_level = pmh
                level_name = "PMH"
            else:
                target_level = hod
                level_name = "HOD"

            # Price must be within 2% of target level (consolidating just below)
            pct_from_level = (target_level - current_price) / target_level * 100
            if pct_from_level < -0.5 or pct_from_level > 2.5:
                return None  # Too far above or below

            # Check for consolidation (tight range in last 5 bars)
            last_5 = df.iloc[-5:]
            range_pct = (last_5["high"].max() - last_5["low"].min()) / last_5["low"].min() * 100
            if range_pct > 4.0:
                return None  # Not tight enough consolidation

            # Entry just above the target level
            entry_price = target_level + 0.02

            # Stop below recent consolidation low
            consol_low = last_5["low"].min()

            return {
                "pattern": PatternType.BREAKOUT,
                "pole_pct": (hod - df["low"].min()) / df["low"].min() * 100,
                "retrace_pct": pct_from_level,
                "consol_bars": 5,
                "entry_price": entry_price,
                "flag_high": target_level,
                "flag_low": consol_low,
                "trigger_desc": f"{level_name} break ${target_level:.2f}"
            }
        except Exception:
            return None

    def _calculate_atr(self, df: pd.DataFrame, period: int = 14) -> float:
        """Calculate ATR."""
        if len(df) < period + 1:
            return (df["high"] - df["low"]).mean()

        high = df["high"]
        low = df["low"]
        close = df["close"]

        tr1 = high - low
        tr2 = abs(high - close.shift(1))
        tr3 = abs(low - close.shift(1))

        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        atr = tr.rolling(window=period).mean()

        return atr.iloc[-1] if not pd.isna(atr.iloc[-1]) else (high - low).mean()

    def _calculate_adx(self, df: pd.DataFrame, period: int = ADX_PERIOD) -> float:
        """
        Calculate Average Directional Index (ADX) for trend strength.

        ADX measures trend strength (not direction):
        - ADX > 25: Strong trend (bullish or bearish)
        - ADX 20-25: Emerging trend
        - ADX < 20: Weak/no trend (choppy)

        For A+ setups, we require ADX > 25 to confirm the stock is trending,
        not just chopping around despite high volume.
        """
        if len(df) < period * 2:
            return 0.0  # Not enough data

        high = df["high"]
        low = df["low"]
        close = df["close"]

        # True Range
        tr1 = high - low
        tr2 = abs(high - close.shift(1))
        tr3 = abs(low - close.shift(1))
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

        # Directional Movement
        up_move = high - high.shift(1)
        down_move = low.shift(1) - low

        # +DM and -DM
        plus_dm = up_move.where((up_move > down_move) & (up_move > 0), 0.0)
        minus_dm = down_move.where((down_move > up_move) & (down_move > 0), 0.0)

        # Smoothed TR, +DM, -DM using Wilder's smoothing
        atr = tr.ewm(alpha=1/period, min_periods=period, adjust=False).mean()
        plus_dm_smooth = plus_dm.ewm(alpha=1/period, min_periods=period, adjust=False).mean()
        minus_dm_smooth = minus_dm.ewm(alpha=1/period, min_periods=period, adjust=False).mean()

        # +DI and -DI
        plus_di = 100 * (plus_dm_smooth / atr)
        minus_di = 100 * (minus_dm_smooth / atr)

        # DX and ADX
        dx = 100 * abs(plus_di - minus_di) / (plus_di + minus_di).replace(0, 1)
        adx = dx.ewm(alpha=1/period, min_periods=period, adjust=False).mean()

        return adx.iloc[-1] if not pd.isna(adx.iloc[-1]) else 0.0

    def _grade_setup(self, gap_pct: float, rvol: float, pole_pct: float,
                    retrace_pct: float, pattern: PatternType,
                    chg_1m: float = 0.0, chg_5m: float = 0.0, chg_15m: float = 0.0,
                    spread_pct: float = 0.0, volume_accel: float = 1.0,
                    adx: float = 0.0) -> SignalGrade:
        """
        Grade the setup quality with execution quality checks.

        A+ = Multi-timeframe momentum alignment (1m/5m/15m all positive and accelerating)
             + meets base technical criteria + good execution quality (tight spread, volume accel)
             + ADX > 25 (strong trend confirmation)
        A  = Strong technical setup (previous A+ criteria) but missing momentum/execution quality
        B  = Good setup (previous A criteria)
        C  = Marginal setup (previous B criteria) or no valid pattern or poor execution quality

        Execution Quality Gates:
        - Spread: A+ requires ≤50bps, A requires ≤80bps, B requires ≤120bps
        - Volume Acceleration: A+ requires ≥1.3x (volume surging into breakout)
        - ADX: A+ requires ≥25 (confirms strong directional trend, filters out choppy action)
        """
        if pattern == PatternType.NONE:
            return SignalGrade.C

        # Convert spread from % to bps for comparison
        spread_bps = spread_pct * 100  # 0.5% = 50bps

        # --- Check for A+ (Multi-timeframe momentum alignment + execution quality + ADX) ---
        # All three timeframes must be positive and meet thresholds
        momentum_aligned = (
            chg_1m >= GRADE_A_PLUS_MOMENTUM["min_chg_1m"] and
            chg_5m >= GRADE_A_PLUS_MOMENTUM["min_chg_5m"] and
            chg_15m >= GRADE_A_PLUS_MOMENTUM["min_chg_15m"]
        )

        # 1m must be contributing (not stalling) - at least 25% of 5m change
        not_stalling = (
            chg_5m > 0 and
            chg_1m >= chg_5m * GRADE_A_PLUS_MOMENTUM["min_1m_contribution"]
        )

        # Must also meet base technical criteria for A+
        base_criteria_met = (
            gap_pct >= GRADE_A_PLUS["min_gap_pct"] and
            rvol >= GRADE_A_PLUS["min_rvol"] and
            pole_pct >= GRADE_A_PLUS["min_pole_pct"] and
            retrace_pct <= GRADE_A_PLUS["max_retrace_pct"]
        )

        # Execution quality for A+: tight spread + volume accelerating + strong trend
        execution_quality_a_plus = (
            spread_bps <= MAX_SPREAD_BPS_A_PLUS and
            volume_accel >= MIN_VOLUME_ACCEL_A_PLUS and
            adx >= MIN_ADX_A_PLUS  # ADX > 25 confirms strong directional trend
        )

        if momentum_aligned and not_stalling and base_criteria_met and execution_quality_a_plus:
            return SignalGrade.A_PLUS

        # --- A grade (strong technicals, previous A+ thresholds) ---
        # A grade also requires reasonable spread
        execution_quality_a = spread_bps <= MAX_SPREAD_BPS_A

        if (gap_pct >= GRADE_A["min_gap_pct"] and
            rvol >= GRADE_A["min_rvol"] and
            pole_pct >= GRADE_A["min_pole_pct"] and
            retrace_pct <= GRADE_A["max_retrace_pct"] and
            execution_quality_a):
            return SignalGrade.A

        # --- B grade (good setup, previous A thresholds) ---
        # B grade has more lenient spread requirement
        execution_quality_b = spread_bps <= MAX_SPREAD_BPS_B

        if (gap_pct >= GRADE_B["min_gap_pct"] and
            rvol >= GRADE_B["min_rvol"] and
            pole_pct >= GRADE_B["min_pole_pct"] and
            retrace_pct <= GRADE_B["max_retrace_pct"] and
            execution_quality_b):
            return SignalGrade.B

        # --- C grade (marginal setup, previous B thresholds) ---
        # C grade = either marginal technicals OR poor execution quality
        if (gap_pct >= GRADE_C["min_gap_pct"] and
            rvol >= GRADE_C["min_rvol"] and
            pole_pct >= GRADE_C["min_pole_pct"] and
            retrace_pct <= GRADE_C["max_retrace_pct"]):
            return SignalGrade.C

        return SignalGrade.C

    def _calculate_confidence(self, pattern: PatternType, rvol: float,
                             pole_pct: float, retrace_pct: float,
                             shares_outstanding: Optional[int],
                             float_rotation: float = 0.0,
                             volume_accel: float = 1.0,
                             spread_pct: float = 0.0) -> float:
        """
        Calculate confidence score 0-1 with execution quality factors.

        Factors:
        - Pattern type (base 0.5 for valid pattern, 0.3 for none)
        - RVOL: +0.10-0.15 for high relative volume
        - Pole strength: +0.05-0.10 for strong pole move
        - Tight retrace: +0.05-0.10 for shallow pullback
        - Low float: +0.05-0.10 for tight float
        - Float rotation: +0.05-0.15 for high float turnover (squeeze signal)
        - Volume acceleration: +0.05-0.10 for surging volume
        - Tight spread: +0.05 for good execution quality
        """
        if pattern == PatternType.NONE:
            return 0.3

        confidence = 0.5

        # RVOL boost
        if rvol >= 10:
            confidence += 0.15
        elif rvol >= 7:
            confidence += 0.10

        # Pole strength boost
        if pole_pct >= 10:
            confidence += 0.10
        elif pole_pct >= 7:
            confidence += 0.05

        # Tight retrace boost
        if retrace_pct <= 20:
            confidence += 0.10
        elif retrace_pct <= 25:
            confidence += 0.05

        # Low float boost (NOTE: This is shares outstanding, not true float)
        if shares_outstanding:
            if shares_outstanding < 3_000_000:
                confidence += 0.10
            elif shares_outstanding < 5_000_000:
                confidence += 0.05

        # --- EXECUTION QUALITY FACTORS ---

        # Float rotation boost (volume / float) - high turnover = squeeze potential
        # 50% rotation = +0.05, 100%+ rotation = +0.15
        if float_rotation >= FLOAT_ROTATION_MAX_BOOST:
            confidence += 0.15  # Float has turned over 100%+ = major squeeze
        elif float_rotation >= FLOAT_ROTATION_BOOST_THRESHOLD:
            # Scale linearly: 0.5 -> 0.05, 1.0 -> 0.15
            rotation_boost = 0.05 + (float_rotation - FLOAT_ROTATION_BOOST_THRESHOLD) * 0.20
            confidence += min(rotation_boost, 0.15)

        # Volume acceleration boost (current bar vs prior 5 bars)
        # 1.5x = +0.05, 2.0x+ = +0.10
        if volume_accel >= 2.0:
            confidence += 0.10  # Volume doubling into breakout
        elif volume_accel >= 1.5:
            confidence += 0.05  # Solid volume increase

        # Tight spread boost (good execution quality)
        spread_bps = spread_pct * 100
        if spread_bps <= 30:  # Very tight spread (≤0.30%)
            confidence += 0.05
        elif spread_bps > MAX_SPREAD_BPS_B:  # Wide spread penalty
            confidence -= 0.05

        return min(max(confidence, 0.1), 1.0)  # Clamp between 0.1 and 1.0


# ============================================================
# DISPLAY FUNCTIONS
# ============================================================

def clear_screen():
    """Clear the terminal screen."""
    if CLEAR_SCREEN:
        # Use os.system for reliable Windows clearing
        # ANSI codes don't work reliably in all Windows terminals
        import os
        os.system('cls' if os.name == 'nt' else 'clear')


def play_alert_sound(is_a_plus: bool = True):
    """
    Play a sound alert for NEW A+ setups appearing in scan.

    Sound: Two ascending tones (low → high) - "something new appeared"
    """
    if not SOUND_ALERT_ENABLED:
        return

    try:
        if is_a_plus:
            # A+ setup: Two ascending tones (new setup alert)
            winsound.Beep(600, 120)   # Lower tone
            winsound.Beep(900, 180)   # Higher tone
        else:
            # A setup: Single soft tone
            winsound.Beep(500, 100)
    except Exception:
        pass  # Silently fail if sound doesn't work


def play_hot_list_trigger_sound():
    """
    Play a sound alert for Hot List A+ TRIGGER events.

    Sound: Three quick high tones - "action needed NOW"
    Distinctly different from new setup sound to indicate urgency.
    """
    if not SOUND_ALERT_ENABLED:
        return

    try:
        # Three quick high-pitched beeps - urgent action signal
        winsound.Beep(1200, 80)
        winsound.Beep(1200, 80)
        winsound.Beep(1500, 120)  # Final higher tone
    except Exception:
        pass  # Silently fail if sound doesn't work


def get_tradingview_url(symbol: str) -> str:
    """Get TradingView chart URL for a symbol."""
    return f"{TRADINGVIEW_BASE_URL}{symbol}"


def format_float(value: Optional[int]) -> str:
    """Format float shares for display."""
    if value is None:
        return "N/A"
    if value >= 1_000_000:
        return f"{value / 1_000_000:.1f}M"
    elif value >= 1_000:
        return f"{value / 1_000:.0f}K"
    return str(value)


def make_mini_chart(prices: List[float], width: int = 12) -> str:
    """Create simple text-based trend indicator from price history."""
    if not prices or len(prices) < 2:
        return "-" * width

    # Calculate overall trend and volatility
    start_price = prices[0]
    end_price = prices[-1]
    high = max(prices)
    low = min(prices)

    pct_change = ((end_price - start_price) / start_price) * 100 if start_price > 0 else 0
    volatility = ((high - low) / start_price) * 100 if start_price > 0 else 0

    # Determine trend character
    if pct_change > 2:
        trend_char = "+"
    elif pct_change > 0:
        trend_char = "^"
    elif pct_change < -2:
        trend_char = "v"
    elif pct_change < 0:
        trend_char = "-"
    else:
        trend_char = "="

    # Create simple visual: show overall direction with length indicating strength
    strength = min(int(abs(pct_change) / 0.5), width - 2)  # Each 0.5% = 1 char
    strength = max(1, strength)

    if pct_change > 0:
        chart = ">" * strength
    elif pct_change < 0:
        chart = "<" * strength
    else:
        chart = "=" * strength

    # Pad to width
    chart = chart.ljust(width, " ")

    return chart[:width]


def get_grade_color(grade: SignalGrade) -> str:
    """Get color for grade."""
    colors = {
        SignalGrade.A_PLUS: Colors.PURPLE,  # Purple for best
        SignalGrade.A: Colors.GREEN,
        SignalGrade.B: Colors.YELLOW,
        SignalGrade.C: Colors.RED,
    }
    return colors.get(grade, Colors.RESET)


def get_sentiment_color(sentiment: NewsSentiment) -> str:
    """Get color for sentiment."""
    colors = {
        NewsSentiment.GREAT: Colors.PURPLE,  # Purple for GREAT
        NewsSentiment.GOOD: Colors.GREEN,
        NewsSentiment.NEUTRAL: Colors.WHITE,
        NewsSentiment.BAD: Colors.RED,
    }
    return colors.get(sentiment, Colors.RESET)


def get_sentiment_icon(sentiment: NewsSentiment) -> str:
    """Get icon for sentiment."""
    icons = {
        NewsSentiment.GREAT: "***",  # Three stars
        NewsSentiment.GOOD: "++",
        NewsSentiment.NEUTRAL: "--",
        NewsSentiment.BAD: "XX",
    }
    return icons.get(sentiment, "--")


def colorize_gap(gap_pct: float) -> str:
    """Color-code gap percentage based on A+/A/B thresholds."""
    value_str = f"+{gap_pct:.1f}%"
    if gap_pct >= GRADE_A_PLUS["min_gap_pct"]:
        return Colors.colorize(value_str, Colors.PURPLE)  # A+ level
    elif gap_pct >= GRADE_A["min_gap_pct"]:
        return Colors.colorize(value_str, Colors.GREEN)   # A level
    elif gap_pct >= GRADE_B["min_gap_pct"]:
        return Colors.colorize(value_str, Colors.YELLOW)  # B level
    else:
        return Colors.colorize(value_str, Colors.RED)     # Below B


def colorize_momentum(change_pct: float) -> str:
    """Color-code short-term momentum change (5m/15m).
    Green = positive momentum (bullish)
    Red = negative momentum (bearish)
    Brighter = stronger move
    """
    if change_pct >= 3.0:
        return Colors.colorize(f"+{change_pct:.1f}%", Colors.GREEN + Colors.BOLD)
    elif change_pct >= 1.0:
        return Colors.colorize(f"+{change_pct:.1f}%", Colors.GREEN)
    elif change_pct > 0:
        return Colors.colorize(f"+{change_pct:.1f}%", Colors.WHITE)
    elif change_pct <= -3.0:
        return Colors.colorize(f"{change_pct:.1f}%", Colors.RED + Colors.BOLD)
    elif change_pct <= -1.0:
        return Colors.colorize(f"{change_pct:.1f}%", Colors.RED)
    else:
        return Colors.colorize(f"{change_pct:.1f}%", Colors.WHITE)


def colorize_rvol(rvol: float) -> str:
    """Color-code RVOL based on A+/A/B thresholds."""
    value_str = f"{rvol:.1f}x"
    if rvol >= GRADE_A_PLUS["min_rvol"]:
        return Colors.colorize(value_str, Colors.PURPLE)  # A+ level
    elif rvol >= GRADE_A["min_rvol"]:
        return Colors.colorize(value_str, Colors.GREEN)   # A level
    elif rvol >= GRADE_B["min_rvol"]:
        return Colors.colorize(value_str, Colors.YELLOW)  # B level
    else:
        return Colors.colorize(value_str, Colors.RED)     # Below B


def colorize_shares_outstanding(shares: Optional[int]) -> str:
    """Color-code shares outstanding - lower is better for small caps."""
    if shares is None:
        return "N/A"

    value_str = format_float(shares)
    if shares < 2_000_000:
        return Colors.colorize(value_str, Colors.PURPLE)  # Excellent - very low
    elif shares < 5_000_000:
        return Colors.colorize(value_str, Colors.GREEN)   # Good - low
    elif shares <= 10_000_000:
        return Colors.colorize(value_str, Colors.YELLOW)  # Acceptable
    else:
        return Colors.colorize(value_str, Colors.RED)     # Too high


def colorize_spread(spread_pct: float) -> str:
    """Color-code bid-ask spread - lower is better."""
    value_str = f"{spread_pct:.1f}%"
    if spread_pct <= 0.3:
        return Colors.colorize(value_str, Colors.GREEN)   # Tight spread
    elif spread_pct <= 0.7:
        return Colors.colorize(value_str, Colors.YELLOW)  # Acceptable
    else:
        return Colors.colorize(value_str, Colors.RED)     # Wide spread


def colorize_key_level_state(state: KeyLevelState) -> str:
    """Color-code key level state."""
    state_map = {
        KeyLevelState.RECLAIMING: ("+RCL", Colors.GREEN),
        KeyLevelState.ABOVE: ("ABV", Colors.CYAN),
        KeyLevelState.AT: ("@LVL", Colors.YELLOW),
        KeyLevelState.BELOW: ("BLW", Colors.GRAY),
        KeyLevelState.REJECTING: ("-REJ", Colors.RED),
    }
    text, color = state_map.get(state, ("???", Colors.WHITE))
    return Colors.colorize(text, color)


def format_catalyst_type(catalyst_type: CatalystType) -> str:
    """Format catalyst type for display."""
    type_map = {
        CatalystType.FDA: Colors.colorize("FDA", Colors.PURPLE),
        CatalystType.EARNINGS: Colors.colorize("ERN", Colors.CYAN),
        CatalystType.CONTRACT: Colors.colorize("CTR", Colors.GREEN),
        CatalystType.MERGER: Colors.colorize("M&A", Colors.PURPLE),
        CatalystType.OFFERING: Colors.colorize("OFF", Colors.RED),  # Warning!
        CatalystType.COMPLIANCE: Colors.colorize("CMP", Colors.YELLOW),
        CatalystType.LEGAL: Colors.colorize("LEG", Colors.RED),
        CatalystType.PRODUCT: Colors.colorize("PRD", Colors.GREEN),
        CatalystType.UNKNOWN: Colors.colorize("---", Colors.GRAY),
    }
    return type_map.get(catalyst_type, "---")


def colorize_pattern(pattern: PatternType, pole_pct: float, retrace_pct: float) -> str:
    """Color-code pattern based on quality."""
    if pattern == PatternType.NONE:
        return Colors.colorize("Gap Only", Colors.YELLOW)

    pattern_str = pattern.value.replace("_", " ").title()[:10]

    # Grade based on pole strength and retrace tightness
    if pole_pct >= GRADE_A_PLUS["min_pole_pct"] and retrace_pct <= GRADE_A_PLUS["max_retrace_pct"]:
        return Colors.colorize(pattern_str, Colors.PURPLE)
    elif pole_pct >= GRADE_A["min_pole_pct"] and retrace_pct <= GRADE_A["max_retrace_pct"]:
        return Colors.colorize(pattern_str, Colors.GREEN)
    elif pole_pct >= GRADE_B["min_pole_pct"] and retrace_pct <= GRADE_B["max_retrace_pct"]:
        return Colors.colorize(pattern_str, Colors.YELLOW)
    else:
        return Colors.colorize(pattern_str, Colors.RED)


def display_header(scanner: SmallCapScanner):
    """Display the scanner header."""
    now = datetime.now(ET)
    time_str = now.strftime("%I:%M:%S %p ET")

    current_time = now.time()
    if current_time < PREMARKET_END:
        session = "PRE-MARKET"
        session_color = Colors.YELLOW
    elif current_time < TRADING_END:
        session = "PRIME TIME"
        session_color = Colors.GREEN
    elif current_time < SCAN_END:
        session = "INTRADAY"
        session_color = Colors.CYAN
    else:
        session = "AFTER HOURS"
        session_color = Colors.RED

    print("=" * 80)
    print(f" SMALLCAP MOMENTUM SCANNER v2.0  |  {time_str}  |  "
          f"{Colors.colorize(session, session_color + Colors.BOLD)}  |  Scan #{scanner.scan_count}")

    # Display market direction indicator
    if scanner.market_tracker and MARKET_TRACKER_ENABLED:
        market_indicator = format_market_indicator(scanner.market_tracker)
        if market_indicator:
            print(f" MARKET: {market_indicator}")

    print("=" * 80)
    print()
    print(f" {Colors.colorize('■ GREAT', Colors.PURPLE)}   "
          f"{Colors.colorize('■ GOOD', Colors.GREEN)}   "
          f"{Colors.colorize('■ NEUTRAL', Colors.YELLOW)}   "
          f"{Colors.colorize('■ AVOID', Colors.RED)}")
    print()


def display_setups_table(setups: List[SetupInfo]):
    """Display setups in a formatted table."""
    if PRETTYTABLE_AVAILABLE:
        display_setups_prettytable(setups)
    else:
        display_setups_simple(setups)


def display_setups_prettytable(setups: List[SetupInfo]):
    """Display setups using PrettyTable with color-coded criteria and new columns."""
    # Main setup table - with key level and trigger
    table = PrettyTable()
    table.field_names = ["Grd", "Symbol", "Price", "1m%", "5m%", "15m%", "Gap%", "RVOL", "Shrs",
                        "KeyLvl", "Trigger", "Entry", "Stop%", "Cat", "News"]
    table.align = "l"
    table.align["Price"] = "r"
    table.align["1m%"] = "r"
    table.align["5m%"] = "r"
    table.align["15m%"] = "r"
    table.align["Gap%"] = "r"
    table.align["RVOL"] = "r"
    table.align["Stop%"] = "r"

    for setup in setups:
        # Color-code the grade
        grade_color = get_grade_color(setup.grade)
        grade_str = Colors.colorize(f"[{setup.grade.value:>2}]", grade_color)

        # Color-code criteria values
        chg_1m_str = colorize_momentum(setup.chg_1m)
        chg_5m_str = colorize_momentum(setup.chg_5m)
        chg_15m_str = colorize_momentum(setup.chg_15m)
        gap_str = colorize_gap(setup.gap_pct)
        rvol_str = colorize_rvol(setup.rvol)
        shares_str = colorize_shares_outstanding(setup.shares_outstanding)

        # Key level info
        key_level_str = colorize_key_level_state(setup.key_level_state)

        # Trigger description (truncate if too long)
        trigger_str = setup.trigger_desc[:14] if setup.trigger_desc else "---"

        # Catalyst type
        catalyst_str = format_catalyst_type(setup.catalyst_type)

        # News sentiment
        sentiment_color = get_sentiment_color(setup.news_sentiment)
        news_icon = get_sentiment_icon(setup.news_sentiment)
        news_str = Colors.colorize(news_icon, sentiment_color)
        if setup.has_catalyst:
            news_str = Colors.colorize(f"{news_icon}!", sentiment_color + Colors.BOLD)

        table.add_row([
            grade_str,
            setup.symbol,
            f"${setup.price:.2f}",
            chg_1m_str,
            chg_5m_str,
            chg_15m_str,
            gap_str,
            rvol_str,
            shares_str,
            key_level_str,
            trigger_str,
            f"${setup.entry_price:.2f}",
            f"{setup.stop_pct:.1f}%",
            catalyst_str,
            news_str
        ])

    print(table)
    print()

    # Secondary table with execution reality data - ALL CRITERIA COLOR CODED
    print(f" {Colors.colorize('EXECUTION REALITY', Colors.CYAN + Colors.BOLD)}")
    exec_table = PrettyTable()
    exec_table.field_names = ["Symbol", "Spread%", "$Vol", "FloatRot", "VolAccel", "HOD", "PMH", "NextRes"]
    exec_table.align = "l"

    for setup in setups:
        # Spread - GREEN=tight, YELLOW=ok, RED=wide
        spread_str = colorize_spread(setup.spread_pct)

        # Dollar volume - GREEN=high liquidity, YELLOW=moderate, RED=low
        if setup.dollar_volume >= 1_000_000:
            dvol_val = setup.dollar_volume / 1_000_000
            dvol_fmt = f"${dvol_val:.1f}M"
            if dvol_val >= 50:
                dvol_str = Colors.colorize(dvol_fmt, Colors.GREEN)
            elif dvol_val >= 10:
                dvol_str = Colors.colorize(dvol_fmt, Colors.YELLOW)
            else:
                dvol_str = dvol_fmt
        else:
            dvol_fmt = f"${setup.dollar_volume/1_000:.0f}K"
            dvol_str = Colors.colorize(dvol_fmt, Colors.RED)  # Low volume warning

        # Float rotation - GREEN=high (multiple rotations), YELLOW=good, WHITE=low
        if setup.float_rotation > 0:
            rot_fmt = f"{setup.float_rotation:.1f}x"
            if setup.float_rotation >= 2.0:
                rot_str = Colors.colorize(rot_fmt, Colors.GREEN)
            elif setup.float_rotation >= 1.0:
                rot_str = Colors.colorize(rot_fmt, Colors.YELLOW)
            else:
                rot_str = rot_fmt
        else:
            rot_str = Colors.colorize("N/A", Colors.GRAY)

        # Volume acceleration - GREEN=accelerating, YELLOW=steady, WHITE=slowing
        vaccel_fmt = f"{setup.volume_accel:.1f}x"
        if setup.volume_accel >= 2.0:
            vaccel_str = Colors.colorize(vaccel_fmt, Colors.GREEN)
        elif setup.volume_accel >= 1.5:
            vaccel_str = Colors.colorize(vaccel_fmt, Colors.YELLOW)
        elif setup.volume_accel < 0.8:
            vaccel_str = Colors.colorize(vaccel_fmt, Colors.RED)
        else:
            vaccel_str = vaccel_fmt

        # HOD - Color based on proximity to current price (near HOD = GREEN)
        hod_pct_from_current = ((setup.hod - setup.price) / setup.price * 100) if setup.price > 0 else 0
        hod_fmt = f"${setup.hod:.2f}"
        if hod_pct_from_current <= 1.0:  # Within 1% of HOD
            hod_str = Colors.colorize(hod_fmt, Colors.GREEN)
        elif hod_pct_from_current <= 3.0:  # Within 3% of HOD
            hod_str = Colors.colorize(hod_fmt, Colors.YELLOW)
        else:
            hod_str = hod_fmt

        # PMH (Pre-market high) - Color if price is above/near it
        pmh_fmt = f"${setup.pmh:.2f}"
        if setup.price >= setup.pmh:
            pmh_str = Colors.colorize(pmh_fmt, Colors.GREEN)  # Above PMH is bullish
        elif setup.price >= setup.pmh * 0.97:
            pmh_str = Colors.colorize(pmh_fmt, Colors.YELLOW)  # Near PMH
        else:
            pmh_str = pmh_fmt

        # Next resistance - Color based on how close it is (reward potential)
        res_pct = ((setup.next_resistance - setup.price) / setup.price * 100) if setup.price > 0 else 0
        res_fmt = f"${setup.next_resistance:.2f}"
        if res_pct >= 5.0:  # Good upside potential
            res_str = Colors.colorize(res_fmt, Colors.GREEN)
        elif res_pct >= 2.0:
            res_str = Colors.colorize(res_fmt, Colors.YELLOW)
        else:
            res_str = Colors.colorize(res_fmt, Colors.RED)  # Resistance too close

        exec_table.add_row([
            setup.symbol,
            spread_str,
            dvol_str,
            rot_str,
            vaccel_str,
            hod_str,
            pmh_str,
            res_str
        ])

    print(exec_table)
    print()

    # News details section - simple format
    print(f" {Colors.colorize('NEWS CATALYSTS', Colors.CYAN + Colors.BOLD)}")
    print("-" * 60)

    has_news = False
    symbols_checked = []
    for setup in setups:
        symbols_checked.append(setup.symbol)
        if setup.news_items:
            has_news = True
            sentiment_color = get_sentiment_color(setup.news_sentiment)
            symbol_str = Colors.colorize(setup.symbol, sentiment_color + Colors.BOLD)

            if setup.news_sentiment == NewsSentiment.GREAT:
                status = Colors.colorize("STRONG CATALYST", Colors.PURPLE + Colors.BOLD)
            elif setup.news_sentiment == NewsSentiment.GOOD:
                status = Colors.colorize("Positive News", Colors.GREEN)
            elif setup.news_sentiment == NewsSentiment.BAD:
                status = Colors.colorize("WARNING - Negative", Colors.RED + Colors.BOLD)
            else:
                status = Colors.colorize("Recent news (neutral)", Colors.GRAY)

            print(f" {symbol_str}: {status}")

            for news in setup.news_items[:2]:  # Show top 2 news items
                news_color = get_sentiment_color(news.sentiment)
                title = news.title[:65] + "..." if len(news.title) > 65 else news.title
                bullet = Colors.colorize("*", news_color)
                print(f"   {bullet} [{news.age_str()}] {title}")
        else:
            # Show that we checked but found no news
            symbol_str = Colors.colorize(setup.symbol, Colors.GRAY)
            print(f" {symbol_str}: {Colors.colorize('No recent news (48h)', Colors.GRAY)}")

    if not symbols_checked:
        print(f" {Colors.colorize('No setups to check for news', Colors.GRAY)}")

    # Reddit trending section
    reddit_setups = [s for s in setups if s.reddit_trending]
    if reddit_setups:
        print()
        print(f" {Colors.colorize('REDDIT TRENDING', Colors.PURPLE + Colors.BOLD)}")
        print("-" * 60)
        for setup in reddit_setups:
            if setup.reddit_sentiment > 0.1:
                sentiment_str = Colors.colorize(f"BULLISH (+{setup.reddit_sentiment:.2f})", Colors.GREEN)
            elif setup.reddit_sentiment < -0.1:
                sentiment_str = Colors.colorize(f"BEARISH ({setup.reddit_sentiment:.2f})", Colors.RED)
            else:
                sentiment_str = Colors.colorize(f"NEUTRAL ({setup.reddit_sentiment:.2f})", Colors.YELLOW)
            print(f" {Colors.colorize(setup.symbol, Colors.PURPLE + Colors.BOLD)}: "
                  f"{setup.reddit_mentions} mentions | {sentiment_str}")

    print()


def display_setups_simple(setups: List[SetupInfo]):
    """Display setups with simple formatting (fallback) - with color-coded criteria."""
    print(f"{'Grd':<6} {'Sym':<6} {'Price':>8} {'1m%':>6} {'5m%':>6} {'15m%':>6} {'Gap%':>7} {'RVOL':>5} "
          f"{'Shrs':>5} {'KeyLvl':<6} {'Trigger':<12} {'Entry':>8} {'Cat':<4} {'News':<4}")
    print("-" * 115)

    for setup in setups:
        # Color-code grade
        grade_color = get_grade_color(setup.grade)
        grade_str = Colors.colorize(f"[{setup.grade.value:>2}]", grade_color)

        # Color-code criteria
        chg_1m_str = colorize_momentum(setup.chg_1m)
        chg_5m_str = colorize_momentum(setup.chg_5m)
        chg_15m_str = colorize_momentum(setup.chg_15m)
        gap_str = colorize_gap(setup.gap_pct)
        rvol_str = colorize_rvol(setup.rvol)
        shares_str = colorize_shares_outstanding(setup.shares_outstanding)
        key_level_str = colorize_key_level_state(setup.key_level_state)
        trigger_str = (setup.trigger_desc[:10] + "..") if len(setup.trigger_desc) > 12 else setup.trigger_desc
        catalyst_str = format_catalyst_type(setup.catalyst_type)

        # News sentiment
        sentiment_color = get_sentiment_color(setup.news_sentiment)
        news_icon = get_sentiment_icon(setup.news_sentiment)
        news_str = Colors.colorize(news_icon, sentiment_color)

        print(f"{grade_str:<6} "
              f"{setup.symbol:<6} "
              f"${setup.price:>7.2f} "
              f"{chg_1m_str:>6} "
              f"{chg_5m_str:>6} "
              f"{chg_15m_str:>6} "
              f"{gap_str:>7} "
              f"{rvol_str:>5} "
              f"{shares_str:>5} "
              f"{key_level_str:<6} "
              f"{trigger_str:<12} "
              f"${setup.entry_price:>7.2f} "
              f"{catalyst_str:<4} "
              f"{news_str:<4}")

    print()

    # News section - simple format
    print(f" {Colors.colorize('NEWS CATALYSTS', Colors.CYAN + Colors.BOLD)}")
    print("-" * 60)

    has_news = False
    for setup in setups:
        if setup.news_items:
            has_news = True
            sentiment_color = get_sentiment_color(setup.news_sentiment)
            symbol_str = Colors.colorize(setup.symbol, sentiment_color + Colors.BOLD)

            if setup.news_sentiment == NewsSentiment.GREAT:
                status = Colors.colorize("STRONG CATALYST", Colors.PURPLE + Colors.BOLD)
            elif setup.news_sentiment == NewsSentiment.GOOD:
                status = Colors.colorize("Positive News", Colors.GREEN)
            elif setup.news_sentiment == NewsSentiment.BAD:
                status = Colors.colorize("WARNING", Colors.RED + Colors.BOLD)
            else:
                status = Colors.colorize("No clear catalyst", Colors.GRAY)

            print(f" {symbol_str}: {status}")

            for news in setup.news_items[:2]:
                news_color = get_sentiment_color(news.sentiment)
                title = news.title[:65] + "..." if len(news.title) > 65 else news.title
                bullet = Colors.colorize("*", news_color)
                print(f"   {bullet} [{news.age_str()}] {title}")

    if not has_news:
        print(f" {Colors.colorize('No recent news for displayed setups', Colors.GRAY)}")

    print()


def display_charts(setups: List[SetupInfo]):
    """Display price action summary for each setup."""
    print(f" {Colors.colorize('PRICE ACTION', Colors.CYAN + Colors.BOLD)} (Recent trend)")
    print("-" * 60)

    for setup in setups[:5]:  # Show charts for top 5
        # Calculate price metrics
        if setup.price_history and len(setup.price_history) >= 2:
            start_price = setup.price_history[0]
            end_price = setup.price_history[-1]
            high = max(setup.price_history)
            low = min(setup.price_history)

            pct_change = ((end_price - start_price) / start_price) * 100 if start_price > 0 else 0

            # Determine trend
            if setup.price_history and len(setup.price_history) >= 5:
                recent = setup.price_history[-5:]
                trend = "UP" if recent[-1] > recent[0] else "DOWN"
            else:
                trend = "UP" if pct_change > 0 else "DOWN" if pct_change < 0 else "FLAT"

            # Color based on trend
            if trend == "UP":
                trend_str = Colors.colorize(f"UP   +{abs(pct_change):.1f}%", Colors.GREEN)
                arrow = Colors.colorize(">>", Colors.GREEN)
            elif trend == "DOWN":
                trend_str = Colors.colorize(f"DOWN -{abs(pct_change):.1f}%", Colors.RED)
                arrow = Colors.colorize("<<", Colors.RED)
            else:
                trend_str = Colors.colorize(f"FLAT  {pct_change:+.1f}%", Colors.YELLOW)
                arrow = Colors.colorize("==", Colors.YELLOW)

            # Range indicator
            range_pct = ((high - low) / start_price) * 100 if start_price > 0 else 0
            range_str = f"Range: {range_pct:.1f}%"

            print(f" {setup.symbol:<5} {arrow} {trend_str:<18} | {range_str}")
        else:
            print(f" {setup.symbol:<5} {Colors.colorize('No data', Colors.GRAY)}")

    print()


def display_tradingview_links(setups: List[SetupInfo]):
    """Display TradingView quick links for easy access."""
    if not setups:
        return

    print(f" {Colors.colorize('TRADINGVIEW QUICK LINKS', Colors.CYAN + Colors.BOLD)}")
    print("-" * 60)

    for setup in setups[:6]:  # Show links for top 6
        url = get_tradingview_url(setup.symbol)
        grade_color = get_grade_color(setup.grade)
        grade_str = Colors.colorize(f"[{setup.grade.value:>2}]", grade_color)

        # Show symbol with grade and URL
        print(f" {grade_str} {setup.symbol:<6} {Colors.colorize(url, Colors.GRAY)}")

    print()
    print(f"  {Colors.colorize('Tip:', Colors.CYAN)} Copy/paste URL or Ctrl+Click in some terminals")
    print()


def display_footer():
    """Display footer with instructions."""
    print("=" * 80)
    print(f" {Colors.colorize('GRADES:', Colors.BOLD)} "
          f"{Colors.colorize('A+', Colors.PURPLE)}=Best  "
          f"{Colors.colorize('A', Colors.GREEN)}=Strong  "
          f"{Colors.colorize('B', Colors.YELLOW)}=Average  |  "
          f"{Colors.colorize('NEWS:', Colors.BOLD)} "
          f"{Colors.colorize('***', Colors.PURPLE)}=Catalyst  "
          f"{Colors.colorize('++', Colors.GREEN)}=Positive  "
          f"{Colors.colorize('--', Colors.GRAY)}=Neutral  "
          f"{Colors.colorize('XX', Colors.RED)}=Avoid")
    print(f" Execute: smallcap_executor.py <TICKER>  |  "
          f"{Colors.colorize('Ctrl+C', Colors.YELLOW)} Exit  |  "
          f"Auto-refresh: {Colors.colorize(f'{REFRESH_INTERVAL_SEC}s', Colors.GREEN)}")
    print("=" * 80)


def display_no_setups():
    """Display message when no setups found."""
    print()
    print(f"  {Colors.colorize('No qualified setups found at this time.', Colors.YELLOW)}")
    print()
    print(f"  {Colors.colorize('Waiting for:', Colors.CYAN)}")
    print(f"    - Top % gainers with 10%+ move")
    print(f"    - RVOL >= 5x average")
    print(f"    - Float <= 10M shares")
    print(f"    - Bull flag pattern forming")
    print()


def display_watchlist(watchlist: List[WatchlistCandidate]):
    """Display top 3 near-miss candidates ranked by quality score."""
    if not watchlist:
        return

    # Sort by QUALITY SCORE (not just readiness) and take top 3
    sorted_watchlist = sorted(watchlist, key=lambda x: x.quality_score, reverse=True)[:3]

    if not sorted_watchlist:
        return

    print(f" {Colors.colorize('WATCHLIST', Colors.CYAN + Colors.BOLD)} - Top Candidates by Quality Score")
    print()

    if PRETTYTABLE_AVAILABLE:
        table = PrettyTable()
        table.field_names = ["Symbol", "Score", "Price", "Gap%", "RVOL", "Float", "Needs to Qualify"]
        table.align = "l"
        table.align["Score"] = "r"
        table.align["Price"] = "r"
        table.align["Gap%"] = "r"
        table.align["RVOL"] = "r"
        table.align["Float"] = "r"

        for candidate in sorted_watchlist:
            # Color the quality score
            if candidate.quality_score >= 70:
                score_color = Colors.GREEN
            elif candidate.quality_score >= 55:
                score_color = Colors.YELLOW
            else:
                score_color = Colors.WHITE

            score_str = Colors.colorize(f"{candidate.quality_score:.0f}", score_color)

            # Color the gap% - GREEN=strong gap, YELLOW=moderate, WHITE=small
            gap_fmt = f"{candidate.pct_change:.1f}%"
            if candidate.pct_change >= 20:
                gap_str = Colors.colorize(gap_fmt, Colors.GREEN)
            elif candidate.pct_change >= 10:
                gap_str = Colors.colorize(gap_fmt, Colors.YELLOW)
            else:
                gap_str = gap_fmt

            # Color the RVOL - GREEN=high, YELLOW=moderate, WHITE=low
            rvol_fmt = f"{candidate.rel_volume:.1f}x"
            if candidate.rel_volume >= 10:
                rvol_str = Colors.colorize(rvol_fmt, Colors.GREEN)
            elif candidate.rel_volume >= 5:
                rvol_str = Colors.colorize(rvol_fmt, Colors.YELLOW)
            else:
                rvol_str = rvol_fmt

            # Format float - GREEN=low float (<5M), YELLOW=moderate, WHITE=high
            if candidate.float_shares:
                float_m = candidate.float_shares / 1_000_000
                float_fmt = format_float(candidate.float_shares)
                if float_m <= 5:
                    float_str = Colors.colorize(float_fmt, Colors.GREEN)
                elif float_m <= 15:
                    float_str = Colors.colorize(float_fmt, Colors.YELLOW)
                else:
                    float_str = float_fmt
            else:
                float_str = Colors.colorize("N/A", Colors.GRAY)

            # Format what's needed - color based on how close to qualifying
            needs_str = candidate.get_status_str()
            if len(needs_str) > 22:
                needs_str = needs_str[:19] + "..."

            # Color the needs string based on content
            if "forming" in needs_str.lower() or "close" in needs_str.lower():
                needs_str = Colors.colorize(needs_str, Colors.YELLOW)
            elif "more" in needs_str.lower() or "need" in needs_str.lower():
                needs_str = Colors.colorize(needs_str, Colors.GRAY)

            table.add_row([
                candidate.symbol,
                score_str,
                f"${candidate.price:.2f}",
                gap_str,
                rvol_str,
                float_str,
                needs_str
            ])

        print(table)
    else:
        # Fallback simple display
        print(f" {'Symbol':<8} {'Score':>6} {'Price':>8} {'Gap%':>8} {'RVOL':>6} {'Float':>6} {'Needs':<22}")
        print("-" * 75)
        for candidate in sorted_watchlist:
            if candidate.quality_score >= 70:
                score_color = Colors.GREEN
            elif candidate.quality_score >= 55:
                score_color = Colors.YELLOW
            else:
                score_color = Colors.WHITE
            score_str = Colors.colorize(f"{candidate.quality_score:.0f}", score_color)
            float_str = format_float(candidate.float_shares)
            needs_str = candidate.get_status_str()[:22]
            print(f" {candidate.symbol:<8} {score_str:>6} ${candidate.price:>7.2f} {candidate.pct_change:>7.1f}% "
                  f"{candidate.rel_volume:>5.1f}x {float_str:>6} {needs_str:<22}")

    print()
    print(f"  {Colors.colorize('Score:', Colors.CYAN)} Weighted by RVOL(25%) + Float(20%) + Gap(15%) + Price(10%) + Readiness(30%)")
    print()


def display_expired_setups(expired: List[SetupInfo]):
    """Display recently-expired setups that may still be worth checking in TradingView."""
    if not expired:
        return

    print(f" {Colors.colorize('RECENTLY SEEN', Colors.YELLOW + Colors.BOLD)} - A+/A setups that stopped qualifying (still worth checking)")
    print()

    if PRETTYTABLE_AVAILABLE:
        table = PrettyTable()
        table.field_names = ["Symbol", "Grade", "Price", "Gap%", "RVOL", "Last Seen", "Entry", "Stop"]
        table.align = "l"
        table.align["Price"] = "r"
        table.align["Gap%"] = "r"
        table.align["RVOL"] = "r"
        table.align["Entry"] = "r"
        table.align["Stop"] = "r"

        for setup in expired[:4]:  # Show max 4 expired setups
            # Color-code the grade
            grade_color = get_grade_color(setup.grade)
            grade_str = Colors.colorize(f"[{setup.grade.value:>2}]", grade_color)

            # Time since last seen
            mins_ago = getattr(setup, 'expired_minutes_ago', 0)
            if mins_ago <= 1:
                time_str = Colors.colorize("Just now", Colors.GREEN)
            elif mins_ago <= 2:
                time_str = Colors.colorize(f"{mins_ago}m ago", Colors.YELLOW)
            else:
                time_str = Colors.colorize(f"{mins_ago}m ago", Colors.GRAY)

            table.add_row([
                setup.symbol,
                grade_str,
                f"${setup.price:.2f}",
                f"{setup.gap_pct:.1f}%",
                f"{setup.rvol:.1f}x",
                time_str,
                f"${setup.entry_price:.2f}",
                f"${setup.stop_price:.2f}"
            ])

        print(table)
    else:
        # Fallback simple display
        print(f" {'Symbol':<8} {'Grade':<6} {'Price':>8} {'Gap%':>8} {'RVOL':>6} {'Seen':>10}")
        print("-" * 55)
        for setup in expired[:4]:
            grade_color = get_grade_color(setup.grade)
            grade_str = Colors.colorize(f"[{setup.grade.value:>2}]", grade_color)
            mins_ago = getattr(setup, 'expired_minutes_ago', 0)
            time_str = f"{mins_ago}m ago" if mins_ago > 0 else "Just now"
            print(f" {setup.symbol:<8} {grade_str:<6} ${setup.price:>7.2f} {setup.gap_pct:>7.1f}% "
                  f"{setup.rvol:>5.1f}x {time_str:>10}")

    print()
    print(f"  {Colors.colorize('Tip:', Colors.CYAN)} These setups recently qualified - check TradingView to see if still valid!")
    print()


def display_hot_list(hot_list: List[HotListEntry]):
    """
    Display the Hot List - A+/A setups being monitored for optimal entry timing.

    Shows stage, triggers fired, and current price action for each tracked setup.
    When SOUND_ALERT_FOR_A is False, only A+ entries are displayed.
    """
    if not HOT_LIST_ENABLED or not hot_list:
        return

    # Filter to A+ only if SOUND_ALERT_FOR_A is disabled (reduces noise)
    if not SOUND_ALERT_FOR_A:
        hot_list = [e for e in hot_list if e.grade == SignalGrade.A_PLUS]
        if not hot_list:
            return  # Nothing to show

    # Count triggers (only A+ for sound alerts)
    trigger_count = sum(1 for e in hot_list if e.stage == HotListStage.TRIGGER)

    # Check for NEW A+ triggers that haven't sounded yet
    global _sounded_hot_list_triggers
    new_a_plus_triggers = []
    for e in hot_list:
        if e.stage == HotListStage.TRIGGER and e.grade == SignalGrade.A_PLUS:
            trigger_key = (e.symbol, e.trigger_type, e.trigger_count)
            if trigger_key not in _sounded_hot_list_triggers:
                new_a_plus_triggers.append(e)
                _sounded_hot_list_triggers.add(trigger_key)

    # Clean up old triggers that are no longer in the hot list
    current_symbols = {e.symbol for e in hot_list}
    _sounded_hot_list_triggers = {t for t in _sounded_hot_list_triggers if t[0] in current_symbols}

    # Header with trigger count
    if trigger_count > 0:
        header = Colors.colorize(f'HOT LIST ({trigger_count} TRIGGER)', Colors.PURPLE + Colors.BOLD)
        # Play distinct sound only for NEW A+ triggers (not on every refresh)
        if new_a_plus_triggers:
            play_hot_list_trigger_sound()
    else:
        header = Colors.colorize('HOT LIST', Colors.CYAN + Colors.BOLD)

    print(f" {header} - Monitoring {len(hot_list)} setups for entry triggers")
    print()

    if PRETTYTABLE_AVAILABLE:
        table = PrettyTable()
        table.field_names = ["Symbol", "Grade", "Stage", "Trigger", "Price", "vs Add", "Age", "Triggers"]
        table.align = "l"
        table.align["Price"] = "r"
        table.align["vs Add"] = "r"
        table.align["Triggers"] = "r"

        for entry in hot_list[:8]:  # Show max 8 entries
            # Color-code the grade
            grade_color = Colors.PURPLE if entry.grade == SignalGrade.A_PLUS else Colors.GREEN
            grade_str = Colors.colorize(entry.grade.value, grade_color)

            # Color-code the stage
            stage_colors = {
                HotListStage.TRIGGER: Colors.PURPLE + Colors.BOLD,
                HotListStage.SETUP: Colors.GREEN,
                HotListStage.CONSOLIDATING: Colors.YELLOW,
                HotListStage.WATCHING: Colors.WHITE,
                HotListStage.COOLING: Colors.GRAY,
            }
            stage_color = stage_colors.get(entry.stage, Colors.WHITE)
            stage_str = Colors.colorize(entry.stage.value[:8], stage_color)

            # Trigger type
            if entry.trigger_type != HotListTrigger.NONE:
                trigger_names = {
                    HotListTrigger.HOD_BREAK: "HOD BRK",
                    HotListTrigger.KEY_LEVEL: "KEY LVL",
                    HotListTrigger.MICRO_PB: "MICRO PB",
                    HotListTrigger.SPREAD_TIGHT: "SPREAD",
                    HotListTrigger.VOLUME_SURGE: "VOL SURGE",
                }
                trigger_str = Colors.colorize(
                    trigger_names.get(entry.trigger_type, "???"),
                    Colors.PURPLE + Colors.BOLD if entry.stage == HotListStage.TRIGGER else Colors.GRAY
                )
            else:
                trigger_str = Colors.colorize("-", Colors.GRAY)

            # Price change vs add price
            if entry.entry_price_at_add > 0:
                price_change_pct = ((entry.current_price - entry.entry_price_at_add) / entry.entry_price_at_add) * 100
                if price_change_pct > 0:
                    vs_add_str = Colors.colorize(f"+{price_change_pct:.1f}%", Colors.GREEN)
                elif price_change_pct < -2:
                    vs_add_str = Colors.colorize(f"{price_change_pct:.1f}%", Colors.RED)
                else:
                    vs_add_str = f"{price_change_pct:.1f}%"
            else:
                vs_add_str = "-"

            # Trigger count
            if entry.times_triggered > 0:
                trig_count_str = Colors.colorize(str(entry.times_triggered), Colors.PURPLE)
            else:
                trig_count_str = Colors.colorize("0", Colors.GRAY)

            table.add_row([
                entry.symbol,
                grade_str,
                stage_str,
                trigger_str,
                f"${entry.current_price:.2f}",
                vs_add_str,
                entry.age_str(),
                trig_count_str
            ])

        print(table)

        # Show legend
        print()
        print(f"  {Colors.colorize('Stages:', Colors.CYAN)} "
              f"{Colors.colorize('TRIGGER', Colors.PURPLE)}=Entry now | "
              f"{Colors.colorize('SETUP', Colors.GREEN)}=Ready | "
              f"{Colors.colorize('CONSOLIDATING', Colors.YELLOW)}=Pullback | "
              f"WATCHING=Monitoring")

    else:
        # Fallback simple display
        print(f" {'Symbol':<8} {'Grd':<4} {'Stage':<12} {'Trigger':<10} {'Price':>8} {'vs Add':>8} {'Age':>6}")
        print("-" * 65)
        for entry in hot_list[:8]:
            grade_color = Colors.PURPLE if entry.grade == SignalGrade.A_PLUS else Colors.GREEN
            grade_str = Colors.colorize(entry.grade.value, grade_color)

            # Price change
            if entry.entry_price_at_add > 0:
                price_change_pct = ((entry.current_price - entry.entry_price_at_add) / entry.entry_price_at_add) * 100
                vs_add_str = f"{price_change_pct:+.1f}%"
            else:
                vs_add_str = "-"

            trigger_name = entry.trigger_type.value[:9] if entry.trigger_type != HotListTrigger.NONE else "-"

            print(f" {entry.symbol:<8} {grade_str:<4} {entry.stage.value:<12} {trigger_name:<10} "
                  f"${entry.current_price:>7.2f} {vs_add_str:>8} {entry.age_str():>6}")

    print()


def display_error(message: str):
    """Display error message."""
    print()
    print(f"  {Colors.colorize(f'ERROR: {message}', Colors.RED)}")
    print()


# ============================================================
# MAIN LOOP
# ============================================================

def main():
    """Main scanner loop."""
    # Check for API key
    if not POLYGON_API_KEY:
        print(Colors.colorize("ERROR: POLYGON_API_KEY environment variable not set", Colors.RED))
        print("Set it with: set POLYGON_API_KEY=your_key_here")
        sys.exit(1)

    scanner = SmallCapScanner()

    # Track seen A+ setups to avoid repeat alerts (reset daily)
    seen_a_plus: Set[str] = set()
    seen_a: Set[str] = set()
    last_reset_date = datetime.now(ET).date()

    print("Starting Small Cap Scanner v2.0 with News Integration...")
    print(f"Refresh interval: {REFRESH_INTERVAL_SEC} seconds")
    if SOUND_ALERT_ENABLED:
        print(f"Sound alerts: ENABLED for A+ setups")
    if HOT_LIST_ENABLED:
        print(f"Hot List: ENABLED (rescan every {HOT_LIST_RESCAN_SEC}s, min grade: {HOT_LIST_MIN_GRADE})")
    print()

    try:
        while True:
            # Check if within scanning hours
            now = datetime.now(ET)
            current_time = now.time()

            if current_time < PREMARKET_START:
                clear_screen()
                display_header(scanner)
                print(f"\n  Market opens at 9:30 AM ET")
                print(f"  Pre-market scanning starts at 7:00 AM ET")
                print(f"  Current time: {now.strftime('%I:%M %p ET')}")
                print(f"\n  Waiting...")
                time.sleep(60)
                continue

            if current_time > SCAN_END:
                clear_screen()
                display_header(scanner)
                print(f"\n  Scanning ended for today (after {SCAN_END.strftime('%I:%M %p')})")
                print(f"  Best setups are in the first 90 minutes")
                print(f"\n  Come back tomorrow at 7:00 AM ET!")
                break

            # Reset seen setups at start of new day
            today = datetime.now(ET).date()
            if today != last_reset_date:
                seen_a_plus.clear()
                seen_a.clear()
                last_reset_date = today

            # Run scan (no "Scanning..." message or pre-clear - keeps display stable)
            try:
                setups, expired_setups = scanner.scan()
            except Exception as e:
                clear_screen()
                display_header(scanner)
                display_error(str(e))
                time.sleep(REFRESH_INTERVAL_SEC)
                continue

            # Check for NEW A+ setups and play alert sound
            new_a_plus = []
            new_a = []
            hot_list_added = []
            for setup in setups:
                if setup.grade == SignalGrade.A_PLUS and setup.symbol not in seen_a_plus:
                    new_a_plus.append(setup.symbol)
                    seen_a_plus.add(setup.symbol)
                elif setup.grade == SignalGrade.A and setup.symbol not in seen_a:
                    new_a.append(setup.symbol)
                    seen_a.add(setup.symbol)

                # Add qualifying setups to Hot List (A+ only, or A+ and A if SOUND_ALERT_FOR_A enabled)
                if HOT_LIST_ENABLED:
                    if setup.grade == SignalGrade.A_PLUS or (setup.grade == SignalGrade.A and SOUND_ALERT_FOR_A):
                        if scanner.add_to_hot_list(setup):
                            hot_list_added.append(setup.symbol)

            # Play sound for new A+ setups (prioritize A+ over A)
            if new_a_plus:
                play_alert_sound(is_a_plus=True)
            elif new_a and SOUND_ALERT_FOR_A:
                play_alert_sound(is_a_plus=False)

            # Rescan hot list for trigger updates
            if HOT_LIST_ENABLED:
                scanner.rescan_hot_list()

            # Clear and display results in one shot (minimizes flashing)
            clear_screen()
            display_header(scanner)

            # Show NEW alert banner if there are new setups
            if new_a_plus:
                print(f"\n  {Colors.colorize('*** NEW A+ SETUP: ' + ', '.join(new_a_plus) + ' ***', Colors.PURPLE + Colors.BOLD)}")
            elif new_a and SOUND_ALERT_FOR_A:
                print(f"\n  {Colors.colorize('* NEW A SETUP: ' + ', '.join(new_a) + ' *', Colors.GREEN + Colors.BOLD)}")

            if not setups:
                display_no_setups()
            else:
                display_setups_table(setups)
                display_charts(setups)
                display_tradingview_links(setups)

            # Show Hot List (A+/A setups being monitored for entry triggers)
            if HOT_LIST_ENABLED:
                hot_list = scanner.get_hot_list()
                display_hot_list(hot_list)

            # Show recently-expired setups (A+/A that stopped qualifying)
            display_expired_setups(expired_setups)

            # Always show watchlist (near-miss candidates) if any exist
            display_watchlist(scanner.watchlist)

            display_footer()

            # Wait for next refresh
            time.sleep(REFRESH_INTERVAL_SEC)

    except KeyboardInterrupt:
        print("\n\nScanner stopped by user.")
        sys.exit(0)


if __name__ == "__main__":
    main()
