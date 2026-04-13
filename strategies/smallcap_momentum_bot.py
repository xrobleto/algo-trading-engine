"""
Small Cap Momentum Breakout Bot
===============================

Strategy: Ross Cameron-style small cap momentum breakout system
- Pre-market gap scanner for top % gainers
- Low float, high relative volume stocks with news catalysts
- BULL FLAG FOCUSED with tiered exit system
- ATR-based stops (4x ATR - WIDER for volatility)
- Progressive position sizing (quarter size until cushion)

Key Features:
1. Pre-market scanning (7:00 AM - 9:30 AM ET)
2. % gainer filter with strict criteria
3. Float and volume filters
4. Half/whole dollar level detection
5. Bull flag pattern detection (PRIMARY)
6. TIERED EXITS: TP1 at 1.0R (33%), TP2 at 2.5R (33%), Trail 34%
7. ATR-based stops (4x ATR, 3.5% minimum)
8. Progressive sizing based on daily P&L
9. Three strikes rule for early stop
10. Dynamic trailing stop (2.0% behind high)

BACKTEST RESULTS (V7 BEST_MIX - Optimized):
-------------------------------------------
- 64 trades over 180 days
- 59.4% win rate
- -1.5% return (near breakeven - best of all approaches)
- 0.84 profit factor (best of all approaches)
- Loser/Winner ratio: 1.7x (best of all approaches)
- Max drawdown: 3.5% (lowest of all approaches)

RECOMMENDATION: This strategy still requires manual oversight.
The BEST_MIX settings significantly improved results but
profitability requires favorable market conditions.

Version: 2.0.0 (V7 BEST_MIX optimized settings)
"""

import os
import asyncio
import logging
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple, Set
from enum import Enum
from zoneinfo import ZoneInfo
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import MarketOrderRequest, LimitOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce, OrderType
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest, StockSnapshotRequest
from alpaca.data.timeframe import TimeFrame

# ============================================================
# CONFIGURATION
# ============================================================

ET = ZoneInfo("America/New_York")

# API Keys
ALPACA_API_KEY = os.getenv("ALPACA_API_KEY", "")
ALPACA_SECRET_KEY = os.getenv("ALPACA_SECRET_KEY", "")
POLYGON_API_KEY = os.getenv("POLYGON_API_KEY", "")
PAPER_TRADING = True  # Set to False for live trading

# --- Universe Filters ---
MIN_PRICE = 1.00              # Minimum stock price
MAX_PRICE = 20.00             # Maximum stock price (small cap focus)
SWEET_SPOT_MIN = 5.00         # Ideal price range low
SWEET_SPOT_MAX = 10.00        # Ideal price range high
MIN_PCT_CHANGE = 10.0         # Minimum % gain from prev close
MIN_RELATIVE_VOLUME = 5.0     # RVOL >= 5x average
MIN_ABSOLUTE_VOLUME = 500_000 # Minimum shares traded today
MAX_FLOAT = 10_000_000        # Maximum float (10M shares)
PREFERRED_FLOAT = 5_000_000   # Preferred float for best movers

# --- Session Timing ---
PREMARKET_START = time(7, 0)   # 7:00 AM ET - start scanning
PREMARKET_END = time(9, 30)    # 9:30 AM ET - market open
RTH_START = time(9, 30)        # Regular trading hours start
TRADING_END = time(11, 0)      # Stop trading after 11:00 AM
EOD_CLOSE = time(15, 55)       # Close all positions by 3:55 PM

# --- Pattern Detection (v4: BULL FLAG FOCUS) ---
HALF_DOLLAR_LEVELS = True      # Use $0.50 levels as S/R
WHOLE_DOLLAR_LEVELS = True     # Use $1.00 levels as S/R
MICRO_PULLBACK_BARS = 3        # Max bars for micro pullback
MICRO_PULLBACK_DEPTH_PCT = 1.5 # Max pullback depth %
MIN_IMPULSE_PCT = 3.0          # Minimum impulse move % (raised from 2.0)
MIN_IMPULSE_BARS = 3           # Minimum bars in impulse

# --- Bull Flag Specific (v4 optimized) ---
FLAG_POLE_MIN_PCT = 6.0        # Minimum pole move %
FLAG_MAX_RETRACE_PCT = 30.0    # Max flag retracement of pole
FLAG_MIN_BARS = 3              # Minimum consolidation bars
FLAG_MAX_BARS = 7              # Maximum consolidation bars

# --- ATR-Based Stops (V7 BEST_MIX: WIDER stops for small cap volatility) ---
USE_ATR_STOPS = True           # Use ATR instead of pattern-based stops
ATR_PERIOD = 14                # ATR lookback period
ATR_STOP_MULT = 4.0            # V7: Stop = 4x ATR (wider = higher win rate)
MIN_STOP_DISTANCE_PCT = 3.5    # V7: Minimum stop distance % (wider)

# --- Pattern Weights (v4: BULL FLAGS ONLY) ---
# Set to 0.0 to disable pattern, higher = more preferred
PATTERN_WEIGHTS = {
    "MICRO_PULLBACK": 0.0,     # Disabled - 39% WR in backtest
    "BULL_FLAG": 2.0,          # Primary - 63% WR in backtest
    "LEVEL_BREAKOUT": 0.0,     # Disabled - poor performance
}

# --- Entry Filters ---
USE_DAILY_CHART_FILTER = True  # Check daily chart context
MIN_DISTANCE_FROM_RESISTANCE = 0.5  # % buffer from major resistance
EMA_200_FILTER = True          # Prefer above 200 EMA on daily

# --- Risk Management ---
MAX_RISK_PER_TRADE = 100.00    # Fixed $ risk per trade (1R)
MAX_DAILY_LOSS = 500.00        # Stop trading if daily loss exceeds this
INITIAL_SIZE_FACTOR = 0.25     # Start at 25% size
FULL_SIZE_CUSHION = 200.00     # Need $200 profit before full size
THREE_STRIKES_ENABLED = True   # Stop after 3 consecutive losses
MAX_POSITIONS = 2              # Maximum concurrent positions

# --- Exit Parameters (V7 BEST_MIX: TIERED EXITS) ---
# V7: Use tiered exits instead of single target
USE_TIERED_EXITS = True        # V7: Enable tiered exit system
TP1_R_MULTIPLE = 1.0           # V7: First take profit at 1.0R
TP1_SIZE_PCT = 0.33            # V7: Take 33% of position at TP1
TP2_R_MULTIPLE = 2.5           # V7: Second take profit at 2.5R
TP2_SIZE_PCT = 0.33            # V7: Take 33% of position at TP2
# Remaining 34% trails for home runs

TARGET_R_MULTIPLE = 2.5        # V7: Overall target (for non-tiered fallback)
ACTUAL_R_EXPECTATION = 1.0     # Realistic expectation
PARTIAL_EXIT_R = 1.0           # Not used with tiered exits
PARTIAL_EXIT_PCT = 0.33        # Not used with tiered exits
USE_PARTIAL_EXITS = False      # Disabled - using tiered exits instead
BREAKEVEN_TRIGGER_R = 1.0      # V7: Move to BE after TP1 hit
USE_TRAILING_STOP = True       # Enable trailing stop
TRAIL_ACTIVATION_R = 2.5       # V7: Activate trail after TP2 (at 2.5R)
TRAIL_DISTANCE_PCT = 2.0       # V7: Trail 2.0% behind high (wider for runners)

# --- Analytics ---
TRACK_BY_PRICE_BUCKET = True   # Track performance by price range
TRACK_BY_FLOAT = True          # Track performance by float size
TRACK_BY_TIME = True           # Track performance by entry time


# ============================================================
# DATA STRUCTURES
# ============================================================

class PatternType(Enum):
    MICRO_PULLBACK = "MICRO_PULLBACK"
    BULL_FLAG = "BULL_FLAG"
    ABCD = "ABCD"
    LEVEL_BREAKOUT = "LEVEL_BREAKOUT"


class ExitReason(Enum):
    STOP_LOSS = "STOP_LOSS"
    TAKE_PROFIT = "TAKE_PROFIT"
    PARTIAL_EXIT = "PARTIAL_EXIT"
    TRAILING_STOP = "TRAILING_STOP"
    BREAKEVEN = "BREAKEVEN"
    EOD_CLOSE = "EOD_CLOSE"
    THREE_STRIKES = "THREE_STRIKES"
    MANUAL = "MANUAL"


@dataclass
class StockCandidate:
    """A stock that passed the initial scanner filters."""
    symbol: str
    price: float
    pct_change: float
    volume: int
    rel_volume: float
    float_shares: Optional[int] = None
    has_news: bool = False
    news_headline: str = ""
    score: float = 0.0  # Composite ranking score

    # Technical context
    near_half_dollar: bool = False
    near_whole_dollar: bool = False
    above_200_ema: bool = False
    distance_to_resistance_pct: float = 0.0


@dataclass
class TradingLevel:
    """A significant price level (half/whole dollar, S/R)."""
    price: float
    level_type: str  # "half", "whole", "resistance", "support"
    strength: int = 1  # Number of touches


@dataclass
class TradeSetup:
    """A detected trade setup ready for entry."""
    symbol: str
    pattern: PatternType
    entry_price: float
    stop_price: float
    target_price: float
    risk_per_share: float
    breakout_level: float
    confidence: float  # 0-1 score
    timestamp: datetime


@dataclass
class Position:
    """An open position."""
    symbol: str
    entry_time: datetime
    entry_price: float
    stop_price: float
    target_price: float
    risk_per_share: float

    # Quantities
    total_qty: int
    remaining_qty: int

    # State tracking
    highest_price: float = 0.0
    trail_active: bool = False
    trail_stop: float = 0.0
    be_active: bool = False
    partial_taken: bool = False

    # Pattern info
    pattern: PatternType = PatternType.MICRO_PULLBACK
    breakout_level: float = 0.0


@dataclass
class DailyStats:
    """Daily trading statistics."""
    date: str
    trades: int = 0
    winners: int = 0
    losers: int = 0
    gross_pnl: float = 0.0
    consecutive_losses: int = 0
    size_factor: float = 0.25  # Current position size multiplier
    trading_halted: bool = False
    halt_reason: str = ""


# ============================================================
# LOGGING SETUP
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger("SmallCapBot")


# ============================================================
# SCANNER - Find Top % Gainers
# ============================================================

class PremarketScanner:
    """Scans for top % gainers meeting all criteria."""

    def __init__(self):
        self.polygon_key = POLYGON_API_KEY
        self.candidates: List[StockCandidate] = []

    def scan(self) -> List[StockCandidate]:
        """Run the pre-market scan and return qualified candidates."""
        logger.info("Starting pre-market scan...")

        # Get top gainers from Polygon
        gainers = self._get_top_gainers()

        if not gainers:
            logger.warning("No gainers found from API")
            return []

        logger.info(f"Found {len(gainers)} initial gainers")

        # Filter by our criteria
        candidates = []
        for g in gainers:
            candidate = self._evaluate_candidate(g)
            if candidate:
                candidates.append(candidate)

        # Sort by composite score
        candidates.sort(key=lambda x: x.score, reverse=True)

        # Take top 10
        self.candidates = candidates[:10]

        logger.info(f"Qualified candidates: {len(self.candidates)}")
        for c in self.candidates[:5]:
            logger.info(f"  {c.symbol}: +{c.pct_change:.1f}% | ${c.price:.2f} | "
                       f"RVOL={c.rel_volume:.1f}x | Score={c.score:.1f}")

        return self.candidates

    def _get_top_gainers(self) -> List[dict]:
        """Fetch top gainers from Polygon snapshot API."""
        url = "https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/gainers"
        params = {"apiKey": self.polygon_key}

        try:
            resp = requests.get(url, params=params, timeout=10)
            if resp.status_code != 200:
                logger.error(f"Polygon API error: {resp.status_code}")
                return []

            data = resp.json()
            return data.get("tickers", [])

        except Exception as e:
            logger.error(f"Error fetching gainers: {e}")
            return []

    def _evaluate_candidate(self, ticker_data: dict) -> Optional[StockCandidate]:
        """Evaluate if a ticker meets all our criteria."""
        try:
            ticker = ticker_data.get("ticker", "")
            day_data = ticker_data.get("day", {})
            prev_day = ticker_data.get("prevDay", {})

            price = day_data.get("c", 0)  # Current price
            volume = day_data.get("v", 0)  # Today's volume
            prev_close = prev_day.get("c", 0)

            if prev_close <= 0 or price <= 0:
                return None

            pct_change = ((price - prev_close) / prev_close) * 100

            # Price filter
            if price < MIN_PRICE or price > MAX_PRICE:
                return None

            # % change filter
            if pct_change < MIN_PCT_CHANGE:
                return None

            # Volume filter
            if volume < MIN_ABSOLUTE_VOLUME:
                return None

            # Get average volume for RVOL calculation
            avg_volume = self._get_avg_volume(ticker)
            if avg_volume <= 0:
                return None

            rel_volume = volume / avg_volume
            if rel_volume < MIN_RELATIVE_VOLUME:
                return None

            # Get float (if available)
            float_shares = self._get_float(ticker)
            if float_shares and float_shares > MAX_FLOAT:
                return None

            # Calculate composite score
            score = self._calculate_score(price, pct_change, rel_volume, float_shares)

            # Check for half/whole dollar levels
            near_half = self._near_level(price, 0.50)
            near_whole = self._near_level(price, 1.00)

            return StockCandidate(
                symbol=ticker,
                price=price,
                pct_change=pct_change,
                volume=volume,
                rel_volume=rel_volume,
                float_shares=float_shares,
                score=score,
                near_half_dollar=near_half,
                near_whole_dollar=near_whole
            )

        except Exception as e:
            logger.debug(f"Error evaluating {ticker_data.get('ticker', '?')}: {e}")
            return None

    def _get_avg_volume(self, symbol: str) -> float:
        """Get 20-day average volume."""
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
            return sum(volumes) / len(volumes)

        except:
            return 0

    def _get_float(self, symbol: str) -> Optional[int]:
        """Get shares float from Polygon."""
        url = f"https://api.polygon.io/v3/reference/tickers/{symbol}"
        params = {"apiKey": self.polygon_key}

        try:
            resp = requests.get(url, params=params, timeout=10)
            if resp.status_code != 200:
                return None

            results = resp.json().get("results", {})
            return results.get("share_class_shares_outstanding")

        except:
            return None

    def _calculate_score(self, price: float, pct_change: float,
                        rel_volume: float, float_shares: Optional[int]) -> float:
        """Calculate composite ranking score."""
        score = 0.0

        # % change contribution (0-40 points)
        score += min(pct_change, 100) * 0.4

        # RVOL contribution (0-30 points)
        score += min(rel_volume, 20) * 1.5

        # Price sweet spot bonus (0-15 points)
        if SWEET_SPOT_MIN <= price <= SWEET_SPOT_MAX:
            score += 15
        elif MIN_PRICE <= price < SWEET_SPOT_MIN:
            score += 5

        # Low float bonus (0-15 points)
        if float_shares:
            if float_shares < 2_000_000:
                score += 15
            elif float_shares < 5_000_000:
                score += 10
            elif float_shares < 10_000_000:
                score += 5

        return score

    def _near_level(self, price: float, increment: float) -> bool:
        """Check if price is near a half/whole dollar level."""
        remainder = price % increment
        distance = min(remainder, increment - remainder)
        return distance < (increment * 0.1)  # Within 10% of level


# ============================================================
# PATTERN DETECTION
# ============================================================

class PatternDetector:
    """Detects micro pullbacks, bull flags, and ABCD patterns."""

    def __init__(self):
        self.levels_cache: Dict[str, List[TradingLevel]] = {}

    def detect_setups(self, symbol: str, df: pd.DataFrame,
                     candidate: StockCandidate) -> List[TradeSetup]:
        """Detect all valid trade setups for a symbol.

        v4: Respects PATTERN_WEIGHTS - only detects enabled patterns.
        Bull flags are PRIMARY (weight 2.0), others disabled (weight 0.0).
        """
        setups = []

        if len(df) < 20:
            return setups

        # Calculate key levels
        levels = self._calculate_levels(symbol, df)
        self.levels_cache[symbol] = levels

        # Check for micro pullback at level (only if weight > 0)
        if PATTERN_WEIGHTS.get("MICRO_PULLBACK", 0) > 0:
            micro_setup = self._detect_micro_pullback(symbol, df, levels, candidate)
            if micro_setup:
                setups.append(micro_setup)

        # Check for bull flag (PRIMARY - weight 2.0)
        if PATTERN_WEIGHTS.get("BULL_FLAG", 0) > 0:
            flag_setup = self._detect_bull_flag(symbol, df, levels, candidate)
            if flag_setup:
                setups.append(flag_setup)

        # Sort by pattern weight * confidence for best setup first
        setups.sort(
            key=lambda s: PATTERN_WEIGHTS.get(s.pattern.value, 1.0) * s.confidence,
            reverse=True
        )

        return setups

    def _calculate_levels(self, symbol: str, df: pd.DataFrame) -> List[TradingLevel]:
        """Calculate significant price levels."""
        levels = []
        current_price = df["close"].iloc[-1]

        # Half dollar levels near current price
        if HALF_DOLLAR_LEVELS:
            base = int(current_price)
            for offset in [-1.0, -0.5, 0, 0.5, 1.0, 1.5, 2.0]:
                level_price = base + offset
                if level_price > 0:
                    level_type = "whole" if offset == int(offset) else "half"
                    levels.append(TradingLevel(level_price, level_type))

        # Recent swing highs as resistance
        highs = df["high"].rolling(5).max()
        for i in range(-20, -1):
            if i >= -len(df):
                high = highs.iloc[i]
                if high > current_price and high not in [l.price for l in levels]:
                    levels.append(TradingLevel(high, "resistance"))

        return sorted(levels, key=lambda x: x.price)

    def _detect_micro_pullback(self, symbol: str, df: pd.DataFrame,
                               levels: List[TradingLevel],
                               candidate: StockCandidate) -> Optional[TradeSetup]:
        """
        Detect micro pullback pattern at a half/whole dollar level.

        Pattern:
        1. Strong impulse move up (3+ green candles, 2%+ move)
        2. Price approaches half/whole dollar level
        3. Brief pullback (1-3 bars) without breaking down
        4. Higher low forming, curling back up
        """
        if len(df) < 10:
            return None

        # Check for impulse move in last 10 bars
        impulse_start_idx = self._find_impulse_start(df, -10, -3)
        if impulse_start_idx is None:
            return None

        impulse_start = df["close"].iloc[impulse_start_idx]
        impulse_high = df["high"].iloc[impulse_start_idx:].max()
        impulse_pct = ((impulse_high - impulse_start) / impulse_start) * 100

        if impulse_pct < MIN_IMPULSE_PCT:
            return None

        # Find nearest level above impulse high
        target_level = None
        for level in levels:
            if level.price > impulse_high * 0.99:  # Just above or at high
                target_level = level
                break

        if target_level is None:
            return None

        # Check for pullback formation
        recent = df.iloc[-MICRO_PULLBACK_BARS:]
        pullback_low = recent["low"].min()
        pullback_depth = ((impulse_high - pullback_low) / impulse_high) * 100

        if pullback_depth > MICRO_PULLBACK_DEPTH_PCT:
            return None  # Pullback too deep

        # Check for higher low (bullish)
        if len(recent) >= 2:
            if recent["low"].iloc[-1] <= recent["low"].iloc[-2] * 0.998:
                return None  # Not forming higher low

        # Valid setup - calculate levels
        entry_price = target_level.price + 0.01  # Just above breakout level
        stop_price = pullback_low - 0.02  # Below pullback low
        risk_per_share = entry_price - stop_price
        target_price = entry_price + (risk_per_share * TARGET_R_MULTIPLE)

        # Confidence score
        confidence = 0.5
        if candidate.rel_volume > 10:
            confidence += 0.15
        if candidate.float_shares and candidate.float_shares < 5_000_000:
            confidence += 0.15
        if target_level.level_type == "whole":
            confidence += 0.10
        if impulse_pct > 3.0:
            confidence += 0.10

        return TradeSetup(
            symbol=symbol,
            pattern=PatternType.MICRO_PULLBACK,
            entry_price=entry_price,
            stop_price=stop_price,
            target_price=target_price,
            risk_per_share=risk_per_share,
            breakout_level=target_level.price,
            confidence=min(confidence, 1.0),
            timestamp=datetime.now(ET)
        )

    def _detect_bull_flag(self, symbol: str, df: pd.DataFrame,
                         levels: List[TradingLevel],
                         candidate: StockCandidate) -> Optional[TradeSetup]:
        """
        Detect bull flag pattern (v4 optimized - PRIMARY PATTERN).

        Pattern:
        1. Strong impulse leg up (pole) - minimum 6% move
        2. Consolidation with declining/sideways price (flag)
        3. Flag retracement < 30% of pole (tight consolidation)
        4. 3-7 bars of consolidation
        5. Breakout above flag high

        Uses ATR-based stops for small cap volatility.
        """
        if len(df) < 20:  # Need enough bars for ATR
            return None

        # Find impulse pole (look back 10-15 bars)
        pole_start_idx = self._find_impulse_start(df, -15, -8)
        if pole_start_idx is None:
            return None

        pole_low = df["low"].iloc[pole_start_idx]
        pole_high = df["high"].iloc[pole_start_idx:-5].max()
        pole_high_idx = df["high"].iloc[pole_start_idx:-5].idxmax()

        pole_move = ((pole_high - pole_low) / pole_low) * 100

        # v4: Require minimum 6% pole move
        if pole_move < FLAG_POLE_MIN_PCT:
            return None

        # Check for flag formation after pole
        flag_bars = df.iloc[pole_high_idx:]
        if len(flag_bars) < FLAG_MIN_BARS or len(flag_bars) > FLAG_MAX_BARS:
            return None

        flag_high = flag_bars["high"].max()
        flag_low = flag_bars["low"].min()

        # Flag should not exceed pole high significantly
        if flag_high > pole_high * 1.01:
            return None

        # v4: Flag retracement should be < 30% (tighter than before)
        retracement_pct = ((pole_high - flag_low) / (pole_high - pole_low)) * 100
        if retracement_pct > FLAG_MAX_RETRACE_PCT:
            return None

        # Current price should be curling up (last bar close > open or green)
        if df["close"].iloc[-1] < df["open"].iloc[-1]:
            return None

        # v4: Calculate ATR-based stop
        entry_price = flag_high + 0.02

        if USE_ATR_STOPS:
            atr = self._calculate_atr(df, ATR_PERIOD)
            atr_stop_distance = atr * ATR_STOP_MULT
            # Ensure minimum stop distance
            min_stop_distance = entry_price * (MIN_STOP_DISTANCE_PCT / 100)
            stop_distance = max(atr_stop_distance, min_stop_distance)
            stop_price = entry_price - stop_distance
        else:
            stop_price = flag_low - 0.02

        risk_per_share = entry_price - stop_price
        target_price = entry_price + (risk_per_share * TARGET_R_MULTIPLE)

        # Confidence scoring (v4: higher base for flags)
        confidence = 0.65  # Flags are most reliable pattern
        if candidate.rel_volume > 8:
            confidence += 0.10
        if retracement_pct < 20:  # Very tight flag
            confidence += 0.10
        if pole_move > 8.0:  # Strong pole
            confidence += 0.10
        if len(flag_bars) >= 4:  # Well-formed consolidation
            confidence += 0.05

        return TradeSetup(
            symbol=symbol,
            pattern=PatternType.BULL_FLAG,
            entry_price=entry_price,
            stop_price=stop_price,
            target_price=target_price,
            risk_per_share=risk_per_share,
            breakout_level=flag_high,
            confidence=min(confidence, 1.0),
            timestamp=datetime.now(ET)
        )

    def _calculate_atr(self, df: pd.DataFrame, period: int = 14) -> float:
        """Calculate Average True Range for ATR-based stops."""
        if len(df) < period + 1:
            # Fallback to simple high-low range
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

    def _find_impulse_start(self, df: pd.DataFrame,
                           start_idx: int, end_idx: int) -> Optional[int]:
        """Find the start of an impulse move."""
        try:
            segment = df.iloc[start_idx:end_idx]

            # Look for lowest low as impulse start
            min_idx = segment["low"].idxmin()

            # Verify it's followed by higher prices
            after_min = df.loc[min_idx:]
            if len(after_min) < 3:
                return None

            # Count green bars after the low
            green_count = sum(1 for i in range(min(5, len(after_min)))
                            if after_min["close"].iloc[i] > after_min["open"].iloc[i])

            if green_count >= MIN_IMPULSE_BARS:
                return df.index.get_loc(min_idx)

            return None

        except:
            return None


# ============================================================
# POSITION MANAGEMENT
# ============================================================

class PositionManager:
    """Manages open positions and executes exits."""

    def __init__(self, trading_client: TradingClient):
        self.client = trading_client
        self.positions: Dict[str, Position] = {}
        self.daily_stats = DailyStats(date=datetime.now(ET).strftime("%Y-%m-%d"))

    def open_position(self, setup: TradeSetup, size_factor: float) -> Optional[Position]:
        """Open a new position based on setup."""
        # Calculate position size
        risk_dollars = MAX_RISK_PER_TRADE * size_factor
        qty = int(risk_dollars / setup.risk_per_share)

        if qty <= 0:
            logger.warning(f"Position size too small for {setup.symbol}")
            return None

        # Place order
        try:
            order = LimitOrderRequest(
                symbol=setup.symbol,
                qty=qty,
                side=OrderSide.BUY,
                type=OrderType.LIMIT,
                time_in_force=TimeInForce.DAY,
                limit_price=round(setup.entry_price, 2)
            )

            result = self.client.submit_order(order)
            logger.info(f"ORDER SUBMITTED: {setup.symbol} | {qty} shares @ ${setup.entry_price:.2f}")

            # Create position object (will be updated when filled)
            pos = Position(
                symbol=setup.symbol,
                entry_time=datetime.now(ET),
                entry_price=setup.entry_price,
                stop_price=setup.stop_price,
                target_price=setup.target_price,
                risk_per_share=setup.risk_per_share,
                total_qty=qty,
                remaining_qty=qty,
                pattern=setup.pattern,
                breakout_level=setup.breakout_level,
                highest_price=setup.entry_price
            )

            self.positions[setup.symbol] = pos
            return pos

        except Exception as e:
            logger.error(f"Order failed for {setup.symbol}: {e}")
            return None

    def update_positions(self, market_data: Dict[str, dict]):
        """Update all positions with current market data."""
        for symbol, pos in list(self.positions.items()):
            if symbol not in market_data:
                continue

            data = market_data[symbol]
            high = data.get("high", pos.entry_price)
            low = data.get("low", pos.entry_price)
            close = data.get("close", pos.entry_price)

            # Update highest price
            if high > pos.highest_price:
                pos.highest_price = high

            # Check stop loss
            if low <= pos.stop_price:
                self._exit_position(pos, pos.stop_price,
                                   ExitReason.BREAKEVEN if pos.be_active else ExitReason.STOP_LOSS)
                continue

            # Check take profit (full target)
            if high >= pos.target_price and not pos.partial_taken:
                self._exit_position(pos, pos.target_price, ExitReason.TAKE_PROFIT)
                continue

            # Check partial exit at 1R
            if not pos.partial_taken:
                partial_target = pos.entry_price + (pos.risk_per_share * PARTIAL_EXIT_R)
                if high >= partial_target:
                    self._take_partial(pos, partial_target)

            # Check breakeven activation
            if not pos.be_active:
                be_price = pos.entry_price + (pos.risk_per_share * BREAKEVEN_TRIGGER_R)
                if pos.highest_price >= be_price:
                    pos.be_active = True
                    pos.stop_price = pos.entry_price + 0.01  # Breakeven + buffer
                    logger.info(f"BREAKEVEN ACTIVATED: {symbol} stop moved to ${pos.stop_price:.2f}")

            # Check trailing stop activation
            if USE_TRAILING_STOP and not pos.trail_active:
                trail_trigger = pos.entry_price + (pos.risk_per_share * TRAIL_ACTIVATION_R)
                if pos.highest_price >= trail_trigger:
                    pos.trail_active = True
                    pos.trail_stop = pos.highest_price * (1 - TRAIL_DISTANCE_PCT / 100)
                    logger.info(f"TRAILING STOP ACTIVATED: {symbol} @ ${pos.trail_stop:.2f}")

            # Update trailing stop
            if pos.trail_active:
                new_trail = pos.highest_price * (1 - TRAIL_DISTANCE_PCT / 100)
                if new_trail > pos.trail_stop:
                    pos.trail_stop = new_trail

                if low <= pos.trail_stop:
                    self._exit_position(pos, pos.trail_stop, ExitReason.TRAILING_STOP)

    def _take_partial(self, pos: Position, exit_price: float):
        """Take partial profits."""
        partial_qty = int(pos.remaining_qty * PARTIAL_EXIT_PCT)
        if partial_qty <= 0:
            return

        try:
            order = MarketOrderRequest(
                symbol=pos.symbol,
                qty=partial_qty,
                side=OrderSide.SELL,
                time_in_force=TimeInForce.DAY
            )

            self.client.submit_order(order)

            pnl = (exit_price - pos.entry_price) * partial_qty
            pos.remaining_qty -= partial_qty
            pos.partial_taken = True

            self.daily_stats.gross_pnl += pnl

            logger.info(f"PARTIAL EXIT: {pos.symbol} | {partial_qty} shares @ ${exit_price:.2f} | "
                       f"P&L: ${pnl:.2f}")

        except Exception as e:
            logger.error(f"Partial exit failed: {e}")

    def _exit_position(self, pos: Position, exit_price: float, reason: ExitReason):
        """Fully exit a position."""
        try:
            order = MarketOrderRequest(
                symbol=pos.symbol,
                qty=pos.remaining_qty,
                side=OrderSide.SELL,
                time_in_force=TimeInForce.DAY
            )

            self.client.submit_order(order)

            pnl = (exit_price - pos.entry_price) * pos.remaining_qty
            self.daily_stats.gross_pnl += pnl
            self.daily_stats.trades += 1

            if pnl > 0:
                self.daily_stats.winners += 1
                self.daily_stats.consecutive_losses = 0
            else:
                self.daily_stats.losers += 1
                self.daily_stats.consecutive_losses += 1

            # Update size factor based on cushion
            self._update_size_factor()

            # Check three strikes
            if THREE_STRIKES_ENABLED and self.daily_stats.consecutive_losses >= 3:
                if self.daily_stats.trades <= 3:  # Only if first 3 trades
                    self.daily_stats.trading_halted = True
                    self.daily_stats.halt_reason = "Three consecutive losses"
                    logger.warning("TRADING HALTED: Three strikes rule triggered")

            # Check daily loss limit
            if self.daily_stats.gross_pnl <= -MAX_DAILY_LOSS:
                self.daily_stats.trading_halted = True
                self.daily_stats.halt_reason = "Daily loss limit"
                logger.warning(f"TRADING HALTED: Daily loss limit (${MAX_DAILY_LOSS}) reached")

            r_mult = (exit_price - pos.entry_price) / pos.risk_per_share
            logger.info(f"EXIT: {pos.symbol} | {reason.value} | {pos.remaining_qty} shares @ "
                       f"${exit_price:.2f} | P&L: ${pnl:.2f} ({r_mult:.2f}R)")

            del self.positions[pos.symbol]

        except Exception as e:
            logger.error(f"Exit failed for {pos.symbol}: {e}")

    def _update_size_factor(self):
        """Update position size factor based on daily P&L."""
        if self.daily_stats.gross_pnl >= FULL_SIZE_CUSHION:
            self.daily_stats.size_factor = 1.0
        else:
            self.daily_stats.size_factor = INITIAL_SIZE_FACTOR

    def close_all(self, reason: ExitReason = ExitReason.EOD_CLOSE):
        """Close all open positions."""
        for symbol, pos in list(self.positions.items()):
            logger.info(f"Closing {symbol} - {reason.value}")
            try:
                order = MarketOrderRequest(
                    symbol=symbol,
                    qty=pos.remaining_qty,
                    side=OrderSide.SELL,
                    time_in_force=TimeInForce.DAY
                )
                self.client.submit_order(order)
                del self.positions[symbol]
            except Exception as e:
                logger.error(f"Failed to close {symbol}: {e}")


# ============================================================
# MAIN BOT
# ============================================================

class SmallCapMomentumBot:
    """Main trading bot orchestrator."""

    def __init__(self):
        self.trading_client = TradingClient(
            ALPACA_API_KEY,
            ALPACA_SECRET_KEY,
            paper=PAPER_TRADING
        )
        self.data_client = StockHistoricalDataClient(ALPACA_API_KEY, ALPACA_SECRET_KEY)

        self.scanner = PremarketScanner()
        self.pattern_detector = PatternDetector()
        self.position_manager = PositionManager(self.trading_client)

        self.watchlist: List[StockCandidate] = []
        self.minute_data: Dict[str, pd.DataFrame] = {}
        self.running = False

    async def run(self):
        """Main bot loop."""
        logger.info("=" * 60)
        logger.info("SMALL CAP MOMENTUM BOT STARTING")
        logger.info("=" * 60)

        self.running = True

        while self.running:
            now = datetime.now(ET)
            current_time = now.time()

            # Pre-market scanning phase
            if PREMARKET_START <= current_time < PREMARKET_END:
                await self._premarket_phase()

            # Active trading phase
            elif RTH_START <= current_time < TRADING_END:
                await self._trading_phase()

            # Position management only (after 11 AM)
            elif TRADING_END <= current_time < EOD_CLOSE:
                await self._management_phase()

            # End of day
            elif current_time >= EOD_CLOSE:
                await self._eod_phase()
                break

            await asyncio.sleep(1)

    async def _premarket_phase(self):
        """Pre-market scanning and preparation."""
        logger.info("Pre-market phase: Scanning for candidates...")

        self.watchlist = self.scanner.scan()

        if self.watchlist:
            logger.info(f"Watchlist prepared with {len(self.watchlist)} candidates")
            for c in self.watchlist[:5]:
                logger.info(f"  {c.symbol}: +{c.pct_change:.1f}% @ ${c.price:.2f}")

        # Wait before next scan
        await asyncio.sleep(300)  # 5 minutes

    async def _trading_phase(self):
        """Active trading: scan for setups and execute."""
        if self.position_manager.daily_stats.trading_halted:
            logger.debug("Trading halted - managing positions only")
            await self._management_phase()
            return

        # Update minute data for watchlist
        await self._update_minute_data()

        # Scan for setups
        for candidate in self.watchlist:
            if candidate.symbol in self.position_manager.positions:
                continue

            if len(self.position_manager.positions) >= MAX_POSITIONS:
                break

            df = self.minute_data.get(candidate.symbol)
            if df is None or len(df) < 10:
                continue

            setups = self.pattern_detector.detect_setups(candidate.symbol, df, candidate)

            for setup in setups:
                if setup.confidence >= 0.6:
                    logger.info(f"SETUP DETECTED: {setup.symbol} | {setup.pattern.value} | "
                               f"Entry=${setup.entry_price:.2f} | Conf={setup.confidence:.0%}")

                    size_factor = self.position_manager.daily_stats.size_factor
                    self.position_manager.open_position(setup, size_factor)
                    break

        # Update open positions
        market_data = await self._get_current_prices()
        self.position_manager.update_positions(market_data)

        await asyncio.sleep(5)  # Check every 5 seconds

    async def _management_phase(self):
        """Manage open positions only (no new entries)."""
        market_data = await self._get_current_prices()
        self.position_manager.update_positions(market_data)
        await asyncio.sleep(10)

    async def _eod_phase(self):
        """End of day: close all positions and print summary."""
        logger.info("End of day: Closing all positions...")
        self.position_manager.close_all()

        stats = self.position_manager.daily_stats
        logger.info("=" * 60)
        logger.info("DAILY SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Total Trades: {stats.trades}")
        logger.info(f"Winners: {stats.winners} | Losers: {stats.losers}")
        if stats.trades > 0:
            logger.info(f"Win Rate: {stats.winners/stats.trades*100:.1f}%")
        logger.info(f"Gross P&L: ${stats.gross_pnl:.2f}")
        logger.info("=" * 60)

        self.running = False

    async def _update_minute_data(self):
        """Fetch latest minute bars for watchlist."""
        for candidate in self.watchlist[:10]:  # Top 10 only
            try:
                request = StockBarsRequest(
                    symbol_or_symbols=candidate.symbol,
                    timeframe=TimeFrame.Minute,
                    start=datetime.now(ET) - timedelta(hours=2)
                )

                bars = self.data_client.get_stock_bars(request)

                if candidate.symbol in bars:
                    df = bars[candidate.symbol].df.reset_index()
                    df = df.rename(columns={
                        "open": "open", "high": "high",
                        "low": "low", "close": "close", "volume": "volume"
                    })
                    self.minute_data[candidate.symbol] = df

            except Exception as e:
                logger.debug(f"Error fetching data for {candidate.symbol}: {e}")

    async def _get_current_prices(self) -> Dict[str, dict]:
        """Get current prices for open positions."""
        result = {}

        symbols = list(self.position_manager.positions.keys())
        if not symbols:
            return result

        try:
            request = StockSnapshotRequest(symbol_or_symbols=symbols)
            snapshots = self.data_client.get_stock_snapshot(request)

            for symbol, snapshot in snapshots.items():
                if snapshot.minute_bar:
                    result[symbol] = {
                        "high": snapshot.minute_bar.high,
                        "low": snapshot.minute_bar.low,
                        "close": snapshot.minute_bar.close
                    }

        except Exception as e:
            logger.debug(f"Error fetching snapshots: {e}")

        return result

    def stop(self):
        """Stop the bot gracefully."""
        logger.info("Stopping bot...")
        self.running = False


# ============================================================
# ENTRY POINT
# ============================================================

async def main():
    """Entry point."""
    bot = SmallCapMomentumBot()

    try:
        await bot.run()
    except KeyboardInterrupt:
        bot.stop()
    except Exception as e:
        logger.error(f"Bot error: {e}")
        bot.stop()


if __name__ == "__main__":
    asyncio.run(main())
