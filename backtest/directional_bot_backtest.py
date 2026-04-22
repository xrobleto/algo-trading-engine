"""
Directional Bot Backtest Framework v2
======================================

Backtests both LONG and SHORT trading using regime-gated directional logic.
Based on simple_bot_backtest.py template with additions:
- SPY SMA200 regime detection (bull/bear/neutral)
- Short signal detection (inverted long logic, stricter params)
- Direction-aware position management (stops, TP, trailing)
- Direction + regime breakdown in results

v2 additions (scanner-equivalent features):
- Expanded symbol universe (high-beta stocks scanner would discover)
- SPY intraday VWAP gate (directional bias from SPY momentum)
- RVOL-weighted quality scoring (scanner prioritization)
- Toggleable A/B comparison

Strategy:
- LONG: Price > VWAP, EMA9 > EMA20, positive momentum, gap-up filter
- SHORT: Price < VWAP, EMA9 < EMA20, negative momentum, squeeze protection
- Regime: SPY vs SMA200 determines allowed directions
"""

import os
import warnings
warnings.filterwarnings("ignore", category=DeprecationWarning)
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, date
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

# ============================================================
# CONFIGURATION
# ============================================================

ET = ZoneInfo("America/New_York")
POLYGON_API_KEY = os.getenv("POLYGON_API_KEY", "eskzZ5O4QQyYpS5TsA3pe5WMeepeaxmW")

# --- Symbol Universe (core: same as simple_bot) ---
CORE_SYMBOLS = [
    # Mega-Cap Tech
    "NVDA", "AAPL", "MSFT", "GOOGL", "AMZN", "META", "TSLA",
    # Semiconductors
    "AMD", "AVGO",
    # High-Momentum Growth
    "COIN", "PLTR", "HOOD", "SQ",
    # Financials
    "JPM", "GS", "BAC", "MS",
    # Healthcare
    "UNH", "LLY",
    # Cloud/SaaS
    "NET", "DDOG", "PANW", "NOW", "CRM", "SNOW",
    # Communication / Consumer / Industrial
    "NFLX", "BA", "CAT",
    # Broad Market ETF
    "DIA",
    # Energy
    "CVX", "XOM",
]

# --- Scanner-discovered symbols (high-beta movers scanner would find) ---
SCANNER_SYMBOLS = [
    # High-beta tech / crypto-adjacent (scanner gap/RVOL picks)
    "MARA", "RIOT", "MSTR", "AFRM", "SOFI", "UPST", "RBLX",
    # High-momentum biotech / speculative
    "MRNA", "ARM", "SMCI", "CRWD", "MDB", "ZS",
    # Additional high-RVOL movers
    "SHOP", "ROKU", "SNAP", "PINS", "LYFT", "UBER",
]

# ============================================================
# SCANNER FEATURE TOGGLE (v2)
# ============================================================
USE_SCANNER_FEATURES = True           # Enable scanner-equivalent improvements
USE_EXPANDED_UNIVERSE = True          # Add SCANNER_SYMBOLS to universe
USE_SPY_VWAP_GATE = True             # SPY VWAP intraday directional bias
USE_RVOL_QUALITY_BOOST = True         # Boost score for high-RVOL candidates
SPY_VWAP_GATE_THRESHOLD = 0.001      # 0.1% from VWAP = meaningful direction
RVOL_QUALITY_BOOST_THRESHOLD = 2.0    # RVOL >= 2.0x gets a score boost
RVOL_QUALITY_BOOST_AMOUNT = 10.0      # +10 points to quality score

BACKTEST_SYMBOLS = CORE_SYMBOLS + (SCANNER_SYMBOLS if USE_SCANNER_FEATURES and USE_EXPANDED_UNIVERSE else [])

BACKTEST_DAYS = 252
INITIAL_CAPITAL = 111_000

# --- Window override (set by CLI --start/--end) ---
# When these are set, fetchers use explicit dates instead of "now - days" trailing window.
# Used by engine multi-regime backtests (backtest/engine_regime_backtest.py).
_BACKTEST_START_DATE: Optional[date] = None
_BACKTEST_END_DATE: Optional[date] = None

# ============================================================
# LONG PARAMETERS (from simple_bot v48c)
# ============================================================
LONG_MIN_RVOL = 1.5
LONG_MIN_VWAP_DISTANCE_PCT = 0.30
LONG_MIN_EMA_SEPARATION_PCT = 0.20
LONG_MIN_HIGHER_CLOSES = 3
LONG_MIN_MOMENTUM_5MIN_PCT = 0.15
LONG_ATR_STOP_MULT = 4.0
LONG_MIN_STOP_PCT = 0.70
LONG_SCALP_TP_R = 3.00
LONG_NO_TRADE_FIRST_MIN = 15
LONG_LATE_CUTOFF = (15, 0)       # 3:00 PM
LONG_TRAILING_ACTIVATION_R = 0.60
LONG_TRAILING_DISTANCE_R = 0.50
LONG_USE_GAP_UP_FILTER = True
LONG_MIN_GAP_UP_PCT = 0.50

# ============================================================
# SHORT PARAMETERS (stricter than longs)
# ============================================================
SHORT_MIN_RVOL = 1.5              # Same strictness as longs
SHORT_MIN_VWAP_DISTANCE_PCT = 0.30  # 0.3% BELOW VWAP
SHORT_MIN_EMA_SEPARATION_PCT = 0.20  # EMA9 must be 0.2% below EMA20
SHORT_MIN_LOWER_CLOSES = 3        # 3 of 5 bars must be lower closes
SHORT_MIN_MOMENTUM_5MIN_PCT = 0.15  # Negative momentum threshold (absolute)
SHORT_ATR_STOP_MULT = 6.0         # Wider stops (short squeezes are violent)
SHORT_MIN_STOP_PCT = 1.20         # Wider min stop (1.2%)
SHORT_SCALP_TP_R = 1.0            # Tighter TP (shorts reverse faster)
SHORT_NO_TRADE_FIRST_MIN = 30     # Full 30 min opening blackout
SHORT_LATE_CUTOFF = (13, 30)      # 1:30 PM (earlier cutoff)
SHORT_TRAILING_ACTIVATION_R = 0.50  # Activate earlier (take what you can get)
SHORT_TRAILING_DISTANCE_R = 0.40    # Tighter trail (shorts snap back)
SHORT_SIZE_MULT = 0.75            # 75% of equivalent long size
SHORT_MAX_DAILY_GAIN_PCT = 2.0    # Don't short stocks up > 2% (squeeze protection)
SHORT_USE_GAP_DOWN_FILTER = True
SHORT_MIN_GAP_DOWN_PCT = 0.50     # Stock must gap down >= 0.5%

# ============================================================
# SHARED PARAMETERS
# ============================================================
MIN_PRICE = 20.0
EMA_FAST = 9
EMA_SLOW = 20
ATR_PERIOD = 14
ADX_PERIOD = 14
MAX_ADX = 35.0
MIN_ADX = 10.0
SHORT_MIN_ADX = 15.0              # Shorts need clearer trend
SHORT_MAX_ADX = 35.0
MIN_SIGNAL_SCORE = 20
MAX_RISK_PER_TRADE_PCT = 0.15
POSITION_SIZE_PCT = 2.00
MAX_CAPITAL_USAGE_PCT = 4.00
MAX_HOLD_MINUTES = 390

# Dynamic position sizing
USE_DYNAMIC_SIZING = True
VOL_REGIME_LOW_THRESHOLD = 0.8
VOL_REGIME_HIGH_THRESHOLD = 1.3
SIZE_MULT_LOW_VOL = 1.25
SIZE_MULT_NORMAL_VOL = 1.00
SIZE_MULT_HIGH_VOL = 0.60

# ============================================================
# REGIME PARAMETERS
# ============================================================
SPY_SMA_PERIOD = 200              # Daily SMA for bull/bear determination
REGIME_NEUTRAL_BAND = 0.01       # +/- 1% around SMA200 = neutral zone
REGIME_ATR_HIGH = 1.3             # ATR ratio > 1.3 = high volatility

# ============================================================
# POSITION LIMITS
# ============================================================
MAX_LONG_POSITIONS = 3
MAX_SHORT_POSITIONS = 2
MAX_TOTAL_POSITIONS = 4

# ============================================================
# SESSION TIMING
# ============================================================
RTH_START = (9, 30)
RTH_END = (16, 0)
EOD_CLOSE = (15, 55)


# ============================================================
# DATA STRUCTURES
# ============================================================

class MarketRegime(Enum):
    BULL_TREND = "BULL_TREND"
    BULL_VOLATILE = "BULL_VOLATILE"
    NEUTRAL = "NEUTRAL"
    BEAR_VOLATILE = "BEAR_VOLATILE"
    BEAR_TREND = "BEAR_TREND"


# Regime -> allowed sides
REGIME_ALLOWED_SIDES = {
    MarketRegime.BULL_TREND: ["LONG"],
    MarketRegime.BULL_VOLATILE: ["LONG"],
    MarketRegime.NEUTRAL: ["LONG", "SHORT"],
    MarketRegime.BEAR_VOLATILE: ["SHORT"],
    MarketRegime.BEAR_TREND: ["SHORT"],
}

# Size multiplier per regime (reduce in volatile regimes)
REGIME_SIZE_MULT = {
    MarketRegime.BULL_TREND: 1.0,
    MarketRegime.BULL_VOLATILE: 0.75,
    MarketRegime.NEUTRAL: 0.85,
    MarketRegime.BEAR_VOLATILE: 0.75,
    MarketRegime.BEAR_TREND: 1.0,
}


class ExitReason(Enum):
    SCALP_TP = "SCALP_TP"
    STOP_LOSS = "STOP_LOSS"
    TRAILING_STOP = "TRAILING_STOP"
    EOD_CLOSE = "EOD_CLOSE"
    TIME_EXIT = "TIME_EXIT"


@dataclass
class BracketPosition:
    """Tracks a position with direction-aware stops/TP/trailing."""
    symbol: str
    side: str  # "LONG" or "SHORT"
    entry_time: datetime
    entry_price: float
    stop_price: float
    risk_per_share: float
    qty: int
    tp_price: float
    regime: MarketRegime

    # Position state
    active: bool = True
    exit_price: float = 0.0
    exit_reason: Optional[ExitReason] = None
    exit_time: Optional[datetime] = None

    # Trailing stop tracking
    trail_active: bool = False
    best_price: float = 0.0    # highest for longs, lowest for shorts
    trail_stop: float = 0.0

    pnl: float = 0.0

    @property
    def is_closed(self) -> bool:
        return not self.active

    @property
    def direction_mult(self) -> int:
        return 1 if self.side == "LONG" else -1


@dataclass
class BacktestTrade:
    """Completed trade record."""
    symbol: str
    side: str
    regime: str
    entry_time: datetime
    exit_time: datetime
    entry_price: float
    exit_price: float
    qty: int
    exit_reason: ExitReason
    pnl: float
    r_multiple: float
    hold_minutes: int


@dataclass
class BacktestResult:
    """Aggregated backtest results with direction breakdown."""
    total_trades: int = 0
    winners: int = 0
    losers: int = 0
    win_rate: float = 0.0
    gross_profit: float = 0.0
    gross_loss: float = 0.0
    net_pnl: float = 0.0
    profit_factor: float = 0.0
    avg_winner: float = 0.0
    avg_loser: float = 0.0
    avg_r: float = 0.0
    max_drawdown: float = 0.0

    # Direction breakdown
    long_trades: int = 0
    long_winners: int = 0
    long_pnl: float = 0.0
    short_trades: int = 0
    short_winners: int = 0
    short_pnl: float = 0.0

    # Regime breakdown
    regime_stats: Dict[str, Dict] = field(default_factory=dict)

    exit_reasons: Dict[str, int] = field(default_factory=dict)
    trades: List[BacktestTrade] = field(default_factory=list)


# ============================================================
# DATA FETCHING
# ============================================================

def fetch_minute_bars(symbol: str, days: int = 30) -> Optional[pd.DataFrame]:
    """Fetch minute bars from Polygon with pagination."""
    import time as _time

    # Window override: explicit start/end (via CLI) takes precedence over trailing days
    if _BACKTEST_END_DATE is not None:
        end_date = _BACKTEST_END_DATE
    else:
        end_date = datetime.now(ET).date()
    if _BACKTEST_START_DATE is not None:
        start_date = _BACKTEST_START_DATE
    else:
        start_date = end_date - timedelta(days=days + 10)

    all_results = []
    chunk_size_days = 120
    chunk_end = end_date

    while chunk_end > start_date:
        chunk_start = max(start_date, chunk_end - timedelta(days=chunk_size_days))

        url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/minute/{chunk_start}/{chunk_end}"
        params = {
            "adjusted": "true",
            "sort": "asc",
            "limit": 50000,
            "apiKey": POLYGON_API_KEY
        }

        try:
            resp = requests.get(url, params=params, timeout=30)
            if resp.status_code == 429:
                _time.sleep(12)
                resp = requests.get(url, params=params, timeout=30)
            if resp.status_code != 200:
                if not all_results:
                    print(f"  {symbol}: API error {resp.status_code}")
                    return None
                break

            data = resp.json()
            results = data.get("results", [])
            if results:
                all_results.extend(results)

        except Exception as e:
            if not all_results:
                print(f"  {symbol}: Error fetching data: {e}")
                return None
            break

        chunk_end = chunk_start - timedelta(days=1)
        if chunk_end <= start_date:
            break
        _time.sleep(0.25)

    if not all_results:
        print(f"  {symbol}: No data returned")
        return None

    df = pd.DataFrame(all_results)
    df["timestamp"] = pd.to_datetime(df["t"], unit="ms", utc=True).dt.tz_convert(ET)
    df = df.rename(columns={"o": "open", "h": "high", "l": "low", "c": "close", "v": "volume"})
    df = df[["timestamp", "open", "high", "low", "close", "volume"]]
    df = df.drop_duplicates(subset="timestamp").sort_values("timestamp").reset_index(drop=True)

    # Filter RTH only
    df["hour"] = df["timestamp"].dt.hour
    df["minute"] = df["timestamp"].dt.minute
    df = df[
        ((df["hour"] == 9) & (df["minute"] >= 30)) |
        ((df["hour"] > 9) & (df["hour"] < 16))
    ].copy()
    df = df.drop(columns=["hour", "minute"])
    df = df.reset_index(drop=True)

    return df


def fetch_daily_bars(symbol: str, days: int = 30) -> Optional[pd.DataFrame]:
    """Fetch daily bars for RVOL and SMA calculation."""
    # Window override: explicit start/end (via CLI) takes precedence over trailing days
    if _BACKTEST_END_DATE is not None:
        end_date = _BACKTEST_END_DATE
    else:
        end_date = datetime.now(ET).date()
    if _BACKTEST_START_DATE is not None:
        # Always pull +260 days of warmup before the window so SMA200 is primed
        start_date = _BACKTEST_START_DATE - timedelta(days=260)
    else:
        start_date = end_date - timedelta(days=days + 260)  # Extra for SMA200

    url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/day/{start_date}/{end_date}"
    params = {
        "adjusted": "true",
        "sort": "asc",
        "limit": 1000,
        "apiKey": POLYGON_API_KEY
    }

    try:
        resp = requests.get(url, params=params, timeout=30)
        if resp.status_code != 200:
            return None

        data = resp.json()
        results = data.get("results", [])
        if not results:
            return None

        df = pd.DataFrame(results)
        df["date"] = pd.to_datetime(df["t"], unit="ms", utc=True).dt.date
        df = df.rename(columns={"o": "open", "h": "high", "l": "low", "c": "close", "v": "volume"})
        df = df[["date", "open", "high", "low", "close", "volume"]]
        return df

    except Exception:
        return None


# ============================================================
# INDICATORS
# ============================================================

def calculate_ema(series: pd.Series, period: int) -> pd.Series:
    return series.ewm(span=period, adjust=False).mean()


def calculate_atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    high, low, close = df["high"], df["low"], df["close"]
    tr1 = high - low
    tr2 = abs(high - close.shift(1))
    tr3 = abs(low - close.shift(1))
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    return tr.rolling(window=period).mean()


def calculate_adx(df: pd.DataFrame, period: int = 14) -> pd.Series:
    high, low, close = df["high"], df["low"], df["close"]
    plus_dm = high.diff()
    minus_dm = -low.diff()
    plus_dm = plus_dm.where((plus_dm > minus_dm) & (plus_dm > 0), 0.0)
    minus_dm = minus_dm.where((minus_dm > plus_dm) & (minus_dm > 0), 0.0)
    tr1 = high - low
    tr2 = abs(high - close.shift(1))
    tr3 = abs(low - close.shift(1))
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    atr = tr.rolling(period).mean()
    plus_di = 100 * (plus_dm.rolling(period).mean() / atr)
    minus_di = 100 * (minus_dm.rolling(period).mean() / atr)
    dx = 100 * abs(plus_di - minus_di) / (plus_di + minus_di + 1e-10)
    adx = dx.rolling(period).mean()
    return adx


def calculate_atr_ratio(df: pd.DataFrame, period: int = 14, lookback: int = 20) -> pd.Series:
    atr = calculate_atr(df, period)
    atr_avg = atr.rolling(lookback).mean()
    return atr / atr_avg


def calculate_vwap(df: pd.DataFrame) -> pd.Series:
    df = df.copy()
    df["date"] = df["timestamp"].dt.date
    df["typical_price"] = (df["high"] + df["low"] + df["close"]) / 3
    df["tp_vol"] = df["typical_price"] * df["volume"]
    vwap = df.groupby("date").apply(
        lambda x: x["tp_vol"].cumsum() / x["volume"].cumsum()
    ).reset_index(level=0, drop=True)
    return vwap


def calculate_relative_volume(minute_df: pd.DataFrame, daily_df: pd.DataFrame) -> pd.Series:
    minute_df = minute_df.copy()
    minute_df["date"] = minute_df["timestamp"].dt.date
    daily_df = daily_df.copy()
    daily_df["avg_volume_20d"] = daily_df["volume"].rolling(20).mean()
    avg_vol_map = dict(zip(daily_df["date"], daily_df["avg_volume_20d"]))

    minute_df["cum_volume"] = minute_df.groupby("date")["volume"].cumsum()
    minute_df["avg_volume"] = minute_df["date"].map(avg_vol_map)
    minute_df["mins_into_session"] = (
        (minute_df["timestamp"].dt.hour - 9) * 60 +
        minute_df["timestamp"].dt.minute - 30
    )
    total_rth_minutes = 390
    minute_df["expected_volume"] = minute_df["avg_volume"] * (minute_df["mins_into_session"] / total_rth_minutes)
    minute_df["expected_volume"] = minute_df["expected_volume"].clip(lower=1)
    rvol = minute_df["cum_volume"] / minute_df["expected_volume"]
    return rvol


def calculate_sma(series: pd.Series, period: int) -> pd.Series:
    return series.rolling(window=period).mean()


# ============================================================
# REGIME DETECTION
# ============================================================

class RegimeDetector:
    """Detects market regime from SPY daily and minute data."""

    def __init__(self, spy_daily_df: pd.DataFrame, spy_minute_df: pd.DataFrame):
        self.spy_daily = spy_daily_df
        self.spy_minute = spy_minute_df

        # Pre-compute SPY SMA200 from daily bars
        if spy_daily_df is not None and len(spy_daily_df) >= SPY_SMA_PERIOD:
            self.spy_daily["sma200"] = calculate_sma(spy_daily_df["close"], SPY_SMA_PERIOD)
        else:
            self.spy_daily["sma200"] = np.nan

        # Pre-compute SPY ATR ratio from daily bars
        if spy_daily_df is not None and len(spy_daily_df) > ATR_PERIOD + 20:
            spy_daily_atr = calculate_atr(spy_daily_df, ATR_PERIOD)
            spy_daily_atr_avg = spy_daily_atr.rolling(20).mean()
            self.spy_daily["atr_ratio"] = spy_daily_atr / spy_daily_atr_avg
        else:
            self.spy_daily["atr_ratio"] = 1.0

        # Build lookups: date -> (sma200, atr_ratio)
        self.daily_lookup = {}
        for _, row in self.spy_daily.iterrows():
            d = row["date"]
            sma_val = row["sma200"] if not pd.isna(row["sma200"]) else None
            atr_r = row["atr_ratio"] if not pd.isna(row["atr_ratio"]) else 1.0
            self.daily_lookup[d] = (row["close"], sma_val, atr_r)

        # Regime cache
        self._regime_cache: Dict = {}

    def detect(self, ts: datetime) -> MarketRegime:
        """Detect regime for a given timestamp."""
        current_date = ts.date()

        if current_date in self._regime_cache:
            return self._regime_cache[current_date]

        # Get SPY daily data for this date (use most recent available)
        spy_close = None
        spy_sma200 = None
        atr_ratio = 1.0

        # Find the most recent daily data on or before this date
        for d in sorted(self.daily_lookup.keys(), reverse=True):
            if d <= current_date:
                spy_close, spy_sma200, atr_ratio = self.daily_lookup[d]
                break

        if spy_close is None or spy_sma200 is None:
            self._regime_cache[current_date] = MarketRegime.NEUTRAL
            return MarketRegime.NEUTRAL

        # Determine regime
        is_high_vol = atr_ratio > REGIME_ATR_HIGH
        upper_band = spy_sma200 * (1 + REGIME_NEUTRAL_BAND)
        lower_band = spy_sma200 * (1 - REGIME_NEUTRAL_BAND)

        if spy_close > upper_band:
            regime = MarketRegime.BULL_VOLATILE if is_high_vol else MarketRegime.BULL_TREND
        elif spy_close < lower_band:
            regime = MarketRegime.BEAR_VOLATILE if is_high_vol else MarketRegime.BEAR_TREND
        else:
            regime = MarketRegime.NEUTRAL

        self._regime_cache[current_date] = regime
        return regime


# ============================================================
# SIGNAL DETECTION
# ============================================================

def check_long_setup(df: pd.DataFrame, idx: int, rvol: float, ts: datetime) -> Tuple[bool, str, float]:
    """
    Check for A+ long setup. Returns (is_valid, reason, score).
    """
    if idx < max(EMA_SLOW, ADX_PERIOD, 5):
        return False, "insufficient_data", 0

    row = df.iloc[idx]
    price = row["close"]
    vwap = row["vwap"]
    ema_fast = row["ema_fast"]
    ema_slow = row["ema_slow"]

    # ADX filter
    if "adx" in df.columns:
        adx = row["adx"]
        if not pd.isna(adx):
            if adx > MAX_ADX:
                return False, "adx_too_high", 0
            if adx < MIN_ADX:
                return False, "adx_too_low", 0

    # Time filters
    hour, minute = ts.hour, ts.minute
    minutes_since_open = (hour - 9) * 60 + (minute - 30)
    if minutes_since_open < LONG_NO_TRADE_FIRST_MIN:
        return False, "too_early", 0
    if (hour, minute) >= LONG_LATE_CUTOFF:
        return False, "too_late", 0

    # Gap-up filter
    if LONG_USE_GAP_UP_FILTER and "gap_pct" in df.columns:
        gap = row.get("gap_pct") if "gap_pct" in row.index else 0
        if gap is not None and gap < LONG_MIN_GAP_UP_PCT:
            return False, "no_gap_up", 0

    # Price filter
    if price < MIN_PRICE:
        return False, "price_too_low", 0

    # VWAP distance (above)
    min_vwap = vwap * (1 + LONG_MIN_VWAP_DISTANCE_PCT / 100)
    if price < min_vwap:
        return False, "vwap_distance", 0

    # EMA separation (fast > slow)
    min_ema_sep = ema_slow * (1 + LONG_MIN_EMA_SEPARATION_PCT / 100)
    if ema_fast < min_ema_sep:
        return False, "ema_separation", 0

    # RVOL
    if rvol < LONG_MIN_RVOL:
        return False, "low_rvol", 0

    # Higher closes (3 of 5)
    if idx >= 5:
        closes = df["close"].iloc[idx-4:idx+1].values
        higher_count = sum(1 for i in range(1, 5) if closes[i] > closes[i-1])
        if higher_count < LONG_MIN_HIGHER_CLOSES:
            return False, "weak_trend", 0

    # Positive momentum
    if idx >= 5:
        price_5min_ago = df["close"].iloc[idx - 5]
        momentum = (price - price_5min_ago) / price_5min_ago * 100
        if momentum < LONG_MIN_MOMENTUM_5MIN_PCT:
            return False, "weak_momentum", 0

    # Score calculation
    rvol_score = min(100, max(0, (rvol - 1.2) / 1.8 * 100))
    vwap_dist = (price - vwap) / vwap * 100 if vwap > 0 else 0
    vwap_score = min(100, max(0, (vwap_dist - 0.3) / 0.7 * 100))
    if idx >= 5:
        mom = (price - df["close"].iloc[idx - 5]) / df["close"].iloc[idx - 5] * 100
        mom_score = min(100, max(0, (mom - 0.15) / 0.85 * 100))
    else:
        mom_score = 0
    ema_sep = (ema_fast - ema_slow) / ema_slow * 100 if ema_slow > 0 else 0
    ema_score = min(100, max(0, (ema_sep - 0.2) / 0.6 * 100))
    score = (rvol_score * 0.35) + (mom_score * 0.30) + (vwap_score * 0.20) + (ema_score * 0.15)

    return True, "valid", score


def check_short_setup(df: pd.DataFrame, idx: int, rvol: float, ts: datetime,
                      daily_gain_pct: float = 0.0) -> Tuple[bool, str, float]:
    """
    Check for A+ short setup (inverted long logic, stricter params).
    Returns (is_valid, reason, score).
    """
    if idx < max(EMA_SLOW, ADX_PERIOD, 5):
        return False, "insufficient_data", 0

    row = df.iloc[idx]
    price = row["close"]
    vwap = row["vwap"]
    ema_fast = row["ema_fast"]
    ema_slow = row["ema_slow"]

    # ADX filter (narrower range for shorts)
    if "adx" in df.columns:
        adx = row["adx"]
        if not pd.isna(adx):
            if adx > SHORT_MAX_ADX:
                return False, "adx_too_high", 0
            if adx < SHORT_MIN_ADX:
                return False, "adx_too_low", 0

    # Time filters (stricter for shorts)
    hour, minute = ts.hour, ts.minute
    minutes_since_open = (hour - 9) * 60 + (minute - 30)
    if minutes_since_open < SHORT_NO_TRADE_FIRST_MIN:
        return False, "too_early", 0
    if (hour, minute) >= SHORT_LATE_CUTOFF:
        return False, "too_late", 0

    # Squeeze protection: don't short stocks up > 2% on the day
    if daily_gain_pct > SHORT_MAX_DAILY_GAIN_PCT:
        return False, "squeeze_risk", 0

    # Gap-down filter
    if SHORT_USE_GAP_DOWN_FILTER and "gap_pct" in df.columns:
        gap = row.get("gap_pct") if "gap_pct" in row.index else 0
        if gap is not None and gap > -SHORT_MIN_GAP_DOWN_PCT:
            return False, "no_gap_down", 0

    # Price filter
    if price < MIN_PRICE:
        return False, "price_too_low", 0

    # VWAP distance (BELOW VWAP)
    max_vwap = vwap * (1 - SHORT_MIN_VWAP_DISTANCE_PCT / 100)
    if price > max_vwap:
        return False, "vwap_distance", 0

    # EMA separation (fast < slow = bearish)
    max_ema_sep = ema_slow * (1 - SHORT_MIN_EMA_SEPARATION_PCT / 100)
    if ema_fast > max_ema_sep:
        return False, "ema_separation", 0

    # RVOL
    if rvol < SHORT_MIN_RVOL:
        return False, "low_rvol", 0

    # Lower closes (3 of 5 bars are lower)
    if idx >= 5:
        closes = df["close"].iloc[idx-4:idx+1].values
        lower_count = sum(1 for i in range(1, 5) if closes[i] < closes[i-1])
        if lower_count < SHORT_MIN_LOWER_CLOSES:
            return False, "weak_downtrend", 0

    # Negative momentum
    if idx >= 5:
        price_5min_ago = df["close"].iloc[idx - 5]
        momentum = (price - price_5min_ago) / price_5min_ago * 100
        if momentum > -SHORT_MIN_MOMENTUM_5MIN_PCT:
            return False, "weak_down_momentum", 0

    # Score calculation (inverted metrics)
    rvol_score = min(100, max(0, (rvol - 1.2) / 1.8 * 100))
    vwap_dist = (vwap - price) / vwap * 100 if vwap > 0 else 0  # Distance BELOW vwap
    vwap_score = min(100, max(0, (vwap_dist - 0.3) / 0.7 * 100))
    if idx >= 5:
        mom = (df["close"].iloc[idx - 5] - price) / df["close"].iloc[idx - 5] * 100  # Positive = strong downmove
        mom_score = min(100, max(0, (mom - 0.15) / 0.85 * 100))
    else:
        mom_score = 0
    ema_sep = (ema_slow - ema_fast) / ema_slow * 100 if ema_slow > 0 else 0  # Inverted
    ema_score = min(100, max(0, (ema_sep - 0.2) / 0.6 * 100))
    score = (rvol_score * 0.35) + (mom_score * 0.30) + (vwap_score * 0.20) + (ema_score * 0.15)

    return True, "valid", score


def get_volatility_size_multiplier(df: pd.DataFrame, idx: int) -> float:
    if not USE_DYNAMIC_SIZING or "atr_ratio" not in df.columns:
        return 1.0
    atr_ratio = df.iloc[idx]["atr_ratio"]
    if pd.isna(atr_ratio):
        return 1.0
    if atr_ratio < VOL_REGIME_LOW_THRESHOLD:
        return SIZE_MULT_LOW_VOL
    elif atr_ratio > VOL_REGIME_HIGH_THRESHOLD:
        return SIZE_MULT_HIGH_VOL
    return SIZE_MULT_NORMAL_VOL


# ============================================================
# BACKTEST ENGINE
# ============================================================

class BacktestEngine:
    """Run directional backtest with regime-gated long/short entries."""

    def __init__(self):
        self.capital = INITIAL_CAPITAL
        self.positions: Dict[str, BracketPosition] = {}
        self.trades: List[BacktestTrade] = []
        self.equity_curve: List[float] = [INITIAL_CAPITAL]
        self.daily_pnl: Dict[str, float] = {}
        self.regime_detector: Optional[RegimeDetector] = None
        self.regime_counts: Dict[str, int] = {}
        self._spy_minute: Optional[pd.DataFrame] = None  # v2: for SPY VWAP gate

    def run_backtest(self, symbols: List[str]) -> BacktestResult:
        print(f"\n{'='*60}")
        version = "v2 (Scanner Features)" if USE_SCANNER_FEATURES else "v1 (Baseline)"
        print(f"DIRECTIONAL BOT BACKTEST {version}")
        print(f"{'='*60}")
        print(f"Symbols: {len(symbols)} ({len(CORE_SYMBOLS)} core"
              f"{f' + {len(SCANNER_SYMBOLS)} scanner' if USE_SCANNER_FEATURES and USE_EXPANDED_UNIVERSE else ''})")
        print(f"Period: Last {BACKTEST_DAYS} trading days")
        print(f"Initial Capital: ${INITIAL_CAPITAL:,.0f}")
        print(f"Max Positions: {MAX_LONG_POSITIONS}L / {MAX_SHORT_POSITIONS}S / {MAX_TOTAL_POSITIONS} total")
        if USE_SCANNER_FEATURES:
            features = []
            if USE_EXPANDED_UNIVERSE: features.append("expanded_universe")
            if USE_SPY_VWAP_GATE: features.append("spy_vwap_gate")
            if USE_RVOL_QUALITY_BOOST: features.append("rvol_boost")
            print(f"Scanner Features: {', '.join(features)}")
        print(f"{'='*60}\n")

        # Fetch data
        print("Fetching market data...")
        all_data = {}
        daily_data = {}
        import time as _time

        for symbol in symbols:
            print(f"  {symbol}...", end=" ")
            minute_df = fetch_minute_bars(symbol, BACKTEST_DAYS)
            daily_df = fetch_daily_bars(symbol, BACKTEST_DAYS)

            if minute_df is not None and len(minute_df) > 100:
                all_data[symbol] = minute_df
                daily_data[symbol] = daily_df
                print(f"OK ({len(minute_df)} bars)")
            else:
                print("SKIP (insufficient data)")

        # Always fetch SPY for regime detection
        spy_minute = None
        spy_daily = None
        if "SPY" not in all_data:
            print(f"  SPY (regime)...", end=" ")
            spy_minute = fetch_minute_bars("SPY", BACKTEST_DAYS)
            spy_daily = fetch_daily_bars("SPY", BACKTEST_DAYS)
            if spy_minute is not None:
                print(f"OK ({len(spy_minute)} bars)")
            else:
                print("FAILED")
        else:
            spy_minute = all_data.get("SPY")
            spy_daily = daily_data.get("SPY")

        print(f"\nLoaded data for {len(all_data)} symbols")

        # Calculate indicators
        print("\nCalculating indicators...")
        for symbol, df in all_data.items():
            df["ema_fast"] = calculate_ema(df["close"], EMA_FAST)
            df["ema_slow"] = calculate_ema(df["close"], EMA_SLOW)
            df["atr"] = calculate_atr(df, ATR_PERIOD)
            df["vwap"] = calculate_vwap(df)
            df["adx"] = calculate_adx(df, ADX_PERIOD)
            df["atr_ratio"] = calculate_atr_ratio(df, ATR_PERIOD, 20)

            if daily_data.get(symbol) is not None:
                df["rvol"] = calculate_relative_volume(df, daily_data[symbol])
            else:
                df["rvol"] = 1.5

            # Gap detection
            df["_date"] = df["timestamp"].dt.date
            day_groups = df.groupby("_date")
            day_open = day_groups["open"].first()
            day_close = day_groups["close"].last()
            prev_close = day_close.shift(1)
            gap_pct = ((day_open - prev_close) / prev_close * 100).fillna(0)
            gap_map = gap_pct.to_dict()
            df["gap_pct"] = df["_date"].map(gap_map).fillna(0)

            # Daily gain % (for squeeze protection)
            day_first_open = day_groups["open"].first()
            daily_gain_map = {}
            for d in day_open.index:
                o = day_first_open.get(d)
                if o and o > 0:
                    daily_gain_map[d] = 0  # Will be computed per-bar
            df["_day_open"] = df["_date"].map(day_first_open.to_dict())
            df["daily_gain_pct"] = ((df["close"] - df["_day_open"]) / df["_day_open"] * 100).fillna(0)
            df.drop(columns=["_date", "_day_open"], inplace=True)

            all_data[symbol] = df

        # Setup regime detector
        if spy_minute is not None and spy_daily is not None:
            spy_minute["vwap"] = calculate_vwap(spy_minute)
            self.regime_detector = RegimeDetector(spy_daily, spy_minute)
            self._spy_minute = spy_minute  # v2: store for VWAP gate
            print(f"  Regime detector: SPY SMA{SPY_SMA_PERIOD} with {len(spy_daily)} daily bars")
            if USE_SCANNER_FEATURES and USE_SPY_VWAP_GATE:
                print(f"  Scanner v2: SPY VWAP gate ENABLED (threshold={SPY_VWAP_GATE_THRESHOLD*100:.1f}%)")
        else:
            print("  WARNING: No SPY data - defaulting to NEUTRAL regime")

        # Run simulation
        print("\nRunning simulation...")
        self._run_simulation(all_data)

        result = self._calculate_results()
        self._print_results(result)
        return result

    def _get_regime(self, ts: datetime) -> MarketRegime:
        if self.regime_detector is not None:
            return self.regime_detector.detect(ts)
        return MarketRegime.NEUTRAL

    def _count_positions_by_side(self) -> Tuple[int, int]:
        longs = sum(1 for p in self.positions.values() if p.active and p.side == "LONG")
        shorts = sum(1 for p in self.positions.values() if p.active and p.side == "SHORT")
        return longs, shorts

    def _run_simulation(self, all_data: Dict[str, pd.DataFrame]):
        all_timestamps = set()
        for df in all_data.values():
            all_timestamps.update(df["timestamp"].tolist())
        all_timestamps = sorted(all_timestamps)

        print(f"  Processing {len(all_timestamps)} timestamps...")

        for ts in all_timestamps:
            hour, minute = ts.hour, ts.minute
            if not ((hour == 9 and minute >= 30) or (9 < hour < 16)):
                continue

            # Manage existing positions
            self._manage_positions(ts, all_data)

            # EOD close
            if (hour, minute) >= EOD_CLOSE:
                self._close_all_positions(ts, all_data, ExitReason.EOD_CLOSE)
                continue

            # Skip first 5 minutes
            if hour == 9 and minute < 35:
                continue

            # Detect regime
            regime = self._get_regime(ts)
            regime_key = regime.value
            self.regime_counts[regime_key] = self.regime_counts.get(regime_key, 0) + 1

            allowed_sides = REGIME_ALLOWED_SIDES.get(regime, ["LONG", "SHORT"])
            regime_size_mult = REGIME_SIZE_MULT.get(regime, 1.0)

            # Check position capacity
            n_longs, n_shorts = self._count_positions_by_side()
            total = n_longs + n_shorts

            if total < MAX_TOTAL_POSITIONS:
                self._scan_for_entries(ts, all_data, allowed_sides, regime,
                                       regime_size_mult, n_longs, n_shorts)

    def _manage_positions(self, ts: datetime, all_data: Dict[str, pd.DataFrame]):
        positions_to_remove = []

        for symbol, pos in self.positions.items():
            if pos.is_closed:
                positions_to_remove.append(symbol)
                continue

            df = all_data.get(symbol)
            if df is None:
                continue

            current_bars = df[df["timestamp"] == ts]
            if current_bars.empty:
                continue

            row = current_bars.iloc[0]
            high = row["high"]
            low = row["low"]
            close = row["close"]

            if pos.side == "LONG":
                self._manage_long_position(pos, ts, high, low, close)
            else:
                self._manage_short_position(pos, ts, high, low, close)

        for symbol in positions_to_remove:
            del self.positions[symbol]

    def _manage_long_position(self, pos: BracketPosition, ts: datetime,
                               high: float, low: float, close: float):
        # 1. Stop loss
        if low <= pos.stop_price:
            self._record_exit(pos, ts, pos.stop_price, ExitReason.STOP_LOSS)
            return

        # 2. Trailing stop
        activation_pct = (LONG_TRAILING_ACTIVATION_R * pos.risk_per_share / pos.entry_price) * 100
        if not pos.trail_active and ((close - pos.entry_price) / pos.entry_price * 100) >= activation_pct:
            pos.trail_active = True
            pos.best_price = close

        if pos.trail_active:
            if high > pos.best_price:
                pos.best_price = high
            trail_dist_pct = (LONG_TRAILING_DISTANCE_R * pos.risk_per_share / pos.entry_price) * 100
            new_trail = pos.best_price * (1 - trail_dist_pct / 100)
            if new_trail > pos.trail_stop:
                pos.trail_stop = new_trail
            if low <= pos.trail_stop:
                self._record_exit(pos, ts, pos.trail_stop, ExitReason.TRAILING_STOP)
                return

        # 3. Take profit
        if high >= pos.tp_price:
            self._record_exit(pos, ts, pos.tp_price, ExitReason.SCALP_TP)
            return

        # 4. Time exit
        hold_minutes = (ts - pos.entry_time).total_seconds() / 60
        if hold_minutes >= MAX_HOLD_MINUTES:
            self._record_exit(pos, ts, close, ExitReason.TIME_EXIT)

    def _manage_short_position(self, pos: BracketPosition, ts: datetime,
                                high: float, low: float, close: float):
        # 1. Stop loss (triggered when price goes UP past stop)
        if high >= pos.stop_price:
            self._record_exit(pos, ts, pos.stop_price, ExitReason.STOP_LOSS)
            return

        # 2. Trailing stop (tracks LOWEST price for shorts)
        activation_pct = (SHORT_TRAILING_ACTIVATION_R * pos.risk_per_share / pos.entry_price) * 100
        unrealized_gain_pct = (pos.entry_price - close) / pos.entry_price * 100
        if not pos.trail_active and unrealized_gain_pct >= activation_pct:
            pos.trail_active = True
            pos.best_price = close  # Lowest price seen

        if pos.trail_active:
            if low < pos.best_price:
                pos.best_price = low
            trail_dist_pct = (SHORT_TRAILING_DISTANCE_R * pos.risk_per_share / pos.entry_price) * 100
            new_trail = pos.best_price * (1 + trail_dist_pct / 100)
            # For shorts, trail_stop should only move DOWN (tighter)
            if pos.trail_stop == 0 or new_trail < pos.trail_stop:
                pos.trail_stop = new_trail
            if high >= pos.trail_stop:
                self._record_exit(pos, ts, pos.trail_stop, ExitReason.TRAILING_STOP)
                return

        # 3. Take profit (triggered when price goes DOWN past TP)
        if low <= pos.tp_price:
            self._record_exit(pos, ts, pos.tp_price, ExitReason.SCALP_TP)
            return

        # 4. Time exit
        hold_minutes = (ts - pos.entry_time).total_seconds() / 60
        if hold_minutes >= MAX_HOLD_MINUTES:
            self._record_exit(pos, ts, close, ExitReason.TIME_EXIT)

    def _get_spy_vwap_bias(self, ts: datetime, spy_data: Optional[pd.DataFrame]) -> Optional[str]:
        """Returns directional bias from SPY's intraday VWAP position.
        Returns 'LONG', 'SHORT', or None (no strong bias)."""
        if not USE_SCANNER_FEATURES or not USE_SPY_VWAP_GATE or spy_data is None:
            return None
        current = spy_data[spy_data["timestamp"] == ts]
        if current.empty:
            return None
        row = current.iloc[0]
        if "vwap" not in spy_data.columns or pd.isna(row.get("vwap", np.nan)):
            return None
        spy_price = row["close"]
        spy_vwap = row["vwap"]
        dist = (spy_price - spy_vwap) / spy_vwap
        if dist > SPY_VWAP_GATE_THRESHOLD:
            return "LONG"     # SPY above VWAP → favor longs
        elif dist < -SPY_VWAP_GATE_THRESHOLD:
            return "SHORT"    # SPY below VWAP → favor shorts
        return None

    def _scan_for_entries(self, ts: datetime, all_data: Dict[str, pd.DataFrame],
                          allowed_sides: List[str], regime: MarketRegime,
                          regime_size_mult: float, n_longs: int, n_shorts: int):
        # Pre-compute buying power
        deployed = sum(p.qty * p.entry_price for p in self.positions.values() if p.active)
        available = self.capital * MAX_CAPITAL_USAGE_PCT - deployed
        if available <= 0:
            return

        # v2: SPY VWAP directional bias
        spy_bias = self._get_spy_vwap_bias(ts, self._spy_minute)

        long_candidates = []
        short_candidates = []

        for symbol, df in all_data.items():
            if symbol in self.positions:
                continue

            current_bars = df[df["timestamp"] == ts]
            if current_bars.empty:
                continue

            idx = current_bars.index[0]
            row = df.iloc[idx]
            rvol = row["rvol"] if "rvol" in df.columns and not pd.isna(row["rvol"]) else 1.5

            # Try long setup
            if "LONG" in allowed_sides and n_longs < MAX_LONG_POSITIONS:
                # v2: SPY VWAP gate — skip longs when SPY is below VWAP (bearish intraday)
                if USE_SCANNER_FEATURES and USE_SPY_VWAP_GATE and spy_bias == "SHORT":
                    pass  # Skip long scan when SPY intraday momentum is bearish
                else:
                    is_valid, reason, score = check_long_setup(df, idx, rvol, ts)
                    if is_valid and score >= MIN_SIGNAL_SCORE:
                        # v2: RVOL quality boost
                        if USE_SCANNER_FEATURES and USE_RVOL_QUALITY_BOOST and rvol >= RVOL_QUALITY_BOOST_THRESHOLD:
                            score += RVOL_QUALITY_BOOST_AMOUNT

                        # v2: Bonus for SPY alignment (SPY above VWAP = tailwind for longs)
                        if USE_SCANNER_FEATURES and USE_SPY_VWAP_GATE and spy_bias == "LONG":
                            score += 5.0  # Tailwind bonus

                        entry_price = row["close"]
                        atr = row["atr"]
                        if not pd.isna(atr) and atr > 0:
                            stop_dist = max(atr * LONG_ATR_STOP_MULT,
                                           entry_price * (LONG_MIN_STOP_PCT / 100))
                            stop_price = entry_price - stop_dist
                            risk_per_share = stop_dist

                            vol_mult = get_volatility_size_multiplier(df, idx)
                            max_risk = self.capital * MAX_RISK_PER_TRADE_PCT * vol_mult * regime_size_mult
                            qty = int(max_risk / risk_per_share)

                            max_pos_val = self.capital * POSITION_SIZE_PCT
                            if qty * entry_price > max_pos_val:
                                qty = int(max_pos_val / entry_price)
                            if qty * entry_price > available:
                                qty = int(available / entry_price)

                            if qty > 0:
                                long_candidates.append({
                                    "symbol": symbol, "score": score, "side": "LONG",
                                    "entry_price": entry_price, "stop_price": stop_price,
                                    "risk_per_share": risk_per_share, "qty": qty,
                                })

            # Try short setup
            if "SHORT" in allowed_sides and n_shorts < MAX_SHORT_POSITIONS:
                # v2: SPY VWAP gate — skip shorts when SPY is above VWAP (bullish intraday)
                if USE_SCANNER_FEATURES and USE_SPY_VWAP_GATE and spy_bias == "LONG":
                    pass  # Skip short scan when SPY intraday momentum is bullish
                else:
                    daily_gain = row["daily_gain_pct"] if "daily_gain_pct" in df.columns else 0
                    is_valid, reason, score = check_short_setup(df, idx, rvol, ts, daily_gain)
                    if is_valid and score >= MIN_SIGNAL_SCORE:
                        # v2: RVOL quality boost
                        if USE_SCANNER_FEATURES and USE_RVOL_QUALITY_BOOST and rvol >= RVOL_QUALITY_BOOST_THRESHOLD:
                            score += RVOL_QUALITY_BOOST_AMOUNT

                        # v2: Bonus for SPY alignment (SPY below VWAP = tailwind for shorts)
                        if USE_SCANNER_FEATURES and USE_SPY_VWAP_GATE and spy_bias == "SHORT":
                            score += 5.0  # Tailwind bonus

                        entry_price = row["close"]
                        atr = row["atr"]
                        if not pd.isna(atr) and atr > 0:
                            stop_dist = max(atr * SHORT_ATR_STOP_MULT,
                                           entry_price * (SHORT_MIN_STOP_PCT / 100))
                            stop_price = entry_price + stop_dist  # Stop is ABOVE for shorts
                            risk_per_share = stop_dist

                            vol_mult = get_volatility_size_multiplier(df, idx)
                            size_mult = vol_mult * regime_size_mult * SHORT_SIZE_MULT
                            max_risk = self.capital * MAX_RISK_PER_TRADE_PCT * size_mult
                            qty = int(max_risk / risk_per_share)

                            max_pos_val = self.capital * POSITION_SIZE_PCT * SHORT_SIZE_MULT
                            if qty * entry_price > max_pos_val:
                                qty = int(max_pos_val / entry_price)
                            if qty * entry_price > available:
                                qty = int(available / entry_price)

                            if qty > 0:
                                short_candidates.append({
                                    "symbol": symbol, "score": score, "side": "SHORT",
                                    "entry_price": entry_price, "stop_price": stop_price,
                                    "risk_per_share": risk_per_share, "qty": qty,
                                })

        # Combine and rank all candidates
        all_candidates = long_candidates + short_candidates
        all_candidates.sort(key=lambda c: c["score"], reverse=True)

        # Enter best candidates up to position limits
        total = n_longs + n_shorts
        for candidate in all_candidates:
            if total >= MAX_TOTAL_POSITIONS:
                break

            side = candidate["side"]
            cur_longs, cur_shorts = self._count_positions_by_side()

            if side == "LONG" and cur_longs >= MAX_LONG_POSITIONS:
                continue
            if side == "SHORT" and cur_shorts >= MAX_SHORT_POSITIONS:
                continue

            # Recalculate available capital
            deployed = sum(p.qty * p.entry_price for p in self.positions.values() if p.active)
            remaining = self.capital * MAX_CAPITAL_USAGE_PCT - deployed
            if remaining <= 0:
                break

            qty = candidate["qty"]
            if qty * candidate["entry_price"] > remaining:
                qty = int(remaining / candidate["entry_price"])
            if qty <= 0:
                continue

            # Calculate TP
            if side == "LONG":
                tp_price = candidate["entry_price"] + (candidate["risk_per_share"] * LONG_SCALP_TP_R)
            else:
                tp_price = candidate["entry_price"] - (candidate["risk_per_share"] * SHORT_SCALP_TP_R)

            pos = BracketPosition(
                symbol=candidate["symbol"],
                side=side,
                entry_time=ts,
                entry_price=candidate["entry_price"],
                stop_price=candidate["stop_price"],
                risk_per_share=candidate["risk_per_share"],
                qty=qty,
                tp_price=tp_price,
                regime=regime,
            )
            self.positions[candidate["symbol"]] = pos
            total += 1

    def _record_exit(self, pos: BracketPosition, ts: datetime, exit_price: float,
                     reason: ExitReason):
        if not pos.active:
            return

        pos.active = False
        pos.exit_price = exit_price
        pos.exit_reason = reason
        pos.exit_time = ts

        # Direction-aware P&L
        if pos.side == "LONG":
            pnl = (exit_price - pos.entry_price) * pos.qty
            r_mult = (exit_price - pos.entry_price) / pos.risk_per_share if pos.risk_per_share > 0 else 0
        else:
            pnl = (pos.entry_price - exit_price) * pos.qty
            r_mult = (pos.entry_price - exit_price) / pos.risk_per_share if pos.risk_per_share > 0 else 0

        pos.pnl = pnl
        hold_mins = int((ts - pos.entry_time).total_seconds() / 60)

        trade = BacktestTrade(
            symbol=pos.symbol,
            side=pos.side,
            regime=pos.regime.value,
            entry_time=pos.entry_time,
            exit_time=ts,
            entry_price=pos.entry_price,
            exit_price=exit_price,
            qty=pos.qty,
            exit_reason=reason,
            pnl=pnl,
            r_multiple=r_mult,
            hold_minutes=hold_mins
        )
        self.trades.append(trade)

        self.capital += pnl
        self.equity_curve.append(self.capital)

        day_str = ts.strftime("%Y-%m-%d")
        self.daily_pnl[day_str] = self.daily_pnl.get(day_str, 0) + pnl

    def _close_all_positions(self, ts: datetime, all_data: Dict[str, pd.DataFrame],
                             reason: ExitReason):
        for symbol, pos in list(self.positions.items()):
            if not pos.active:
                continue

            df = all_data.get(symbol)
            if df is None:
                continue

            current_bars = df[df["timestamp"] == ts]
            if current_bars.empty:
                continue

            close_price = current_bars.iloc[0]["close"]
            self._record_exit(pos, ts, close_price, reason)

    def _calculate_results(self) -> BacktestResult:
        result = BacktestResult()
        result.trades = self.trades
        result.total_trades = len(self.trades)

        if result.total_trades == 0:
            return result

        pnls = [t.pnl for t in self.trades]
        winners = [p for p in pnls if p > 0]
        losers = [p for p in pnls if p <= 0]

        result.winners = len(winners)
        result.losers = len(losers)
        result.win_rate = result.winners / result.total_trades * 100
        result.gross_profit = sum(winners)
        result.gross_loss = abs(sum(losers))
        result.net_pnl = sum(pnls)
        result.profit_factor = result.gross_profit / result.gross_loss if result.gross_loss > 0 else float('inf')
        result.avg_winner = sum(winners) / len(winners) if winners else 0
        result.avg_loser = abs(sum(losers)) / len(losers) if losers else 0
        result.avg_r = sum(t.r_multiple for t in self.trades) / result.total_trades

        # Max drawdown
        peak = INITIAL_CAPITAL
        max_dd = 0
        for equity in self.equity_curve:
            if equity > peak:
                peak = equity
            dd = (peak - equity) / peak
            if dd > max_dd:
                max_dd = dd
        result.max_drawdown = max_dd * 100

        # Exit reasons
        for t in self.trades:
            reason = t.exit_reason.value
            result.exit_reasons[reason] = result.exit_reasons.get(reason, 0) + 1

        # Direction breakdown
        long_trades = [t for t in self.trades if t.side == "LONG"]
        short_trades = [t for t in self.trades if t.side == "SHORT"]

        result.long_trades = len(long_trades)
        result.long_winners = sum(1 for t in long_trades if t.pnl > 0)
        result.long_pnl = sum(t.pnl for t in long_trades)

        result.short_trades = len(short_trades)
        result.short_winners = sum(1 for t in short_trades if t.pnl > 0)
        result.short_pnl = sum(t.pnl for t in short_trades)

        # Regime breakdown
        for regime in MarketRegime:
            r_trades = [t for t in self.trades if t.regime == regime.value]
            if r_trades:
                r_winners = sum(1 for t in r_trades if t.pnl > 0)
                r_pnl = sum(t.pnl for t in r_trades)
                r_longs = sum(1 for t in r_trades if t.side == "LONG")
                r_shorts = sum(1 for t in r_trades if t.side == "SHORT")
                result.regime_stats[regime.value] = {
                    "trades": len(r_trades),
                    "winners": r_winners,
                    "win_rate": r_winners / len(r_trades) * 100,
                    "pnl": r_pnl,
                    "longs": r_longs,
                    "shorts": r_shorts,
                }

        return result

    def _print_results(self, result: BacktestResult):
        print(f"\n{'='*60}")
        print("BACKTEST RESULTS")
        print(f"{'='*60}")

        print(f"\n[OVERALL PERFORMANCE]")
        print(f"   Total Trades: {result.total_trades}")
        print(f"   Win Rate: {result.win_rate:.1f}%")
        print(f"   Profit Factor: {result.profit_factor:.2f}")
        print(f"   Net P&L: ${result.net_pnl:,.2f}")
        print(f"   Gross Profit: ${result.gross_profit:,.2f}")
        print(f"   Gross Loss: ${result.gross_loss:,.2f}")
        print(f"   Avg Winner: ${result.avg_winner:,.2f}")
        print(f"   Avg Loser: ${result.avg_loser:,.2f}")
        print(f"   Avg R-Multiple: {result.avg_r:.2f}R")
        print(f"   Max Drawdown: {result.max_drawdown:.1f}%")

        # Direction breakdown
        print(f"\n[DIRECTION BREAKDOWN]")
        if result.long_trades > 0:
            long_wr = result.long_winners / result.long_trades * 100
            long_losers_pnl = sum(t.pnl for t in result.trades if t.side == "LONG" and t.pnl <= 0)
            long_winners_pnl = sum(t.pnl for t in result.trades if t.side == "LONG" and t.pnl > 0)
            long_pf = long_winners_pnl / abs(long_losers_pnl) if long_losers_pnl != 0 else float('inf')
            print(f"   LONG:  {result.long_trades} trades | WR={long_wr:.1f}% | "
                  f"PF={long_pf:.2f} | P&L=${result.long_pnl:,.2f}")
        else:
            print(f"   LONG:  0 trades")

        if result.short_trades > 0:
            short_wr = result.short_winners / result.short_trades * 100
            short_losers_pnl = sum(t.pnl for t in result.trades if t.side == "SHORT" and t.pnl <= 0)
            short_winners_pnl = sum(t.pnl for t in result.trades if t.side == "SHORT" and t.pnl > 0)
            short_pf = short_winners_pnl / abs(short_losers_pnl) if short_losers_pnl != 0 else float('inf')
            print(f"   SHORT: {result.short_trades} trades | WR={short_wr:.1f}% | "
                  f"PF={short_pf:.2f} | P&L=${result.short_pnl:,.2f}")
        else:
            print(f"   SHORT: 0 trades")

        # Regime breakdown
        print(f"\n[REGIME BREAKDOWN]")
        for regime_name, stats in sorted(result.regime_stats.items()):
            print(f"   {regime_name}: {stats['trades']} trades "
                  f"({stats['longs']}L/{stats['shorts']}S) | "
                  f"WR={stats['win_rate']:.1f}% | P&L=${stats['pnl']:,.2f}")

        # Regime distribution
        if self.regime_counts:
            total_bars = sum(self.regime_counts.values())
            print(f"\n[REGIME DISTRIBUTION (bars)]")
            for regime_name, count in sorted(self.regime_counts.items()):
                pct = count / total_bars * 100
                print(f"   {regime_name}: {count:,} ({pct:.1f}%)")

        # Exit reasons
        print(f"\n[EXIT REASONS]")
        for reason, count in sorted(result.exit_reasons.items(), key=lambda x: -x[1]):
            pct = count / result.total_trades * 100
            reason_trades = [t for t in result.trades if t.exit_reason.value == reason]
            reason_pnl = sum(t.pnl for t in reason_trades)
            reason_wr = sum(1 for t in reason_trades if t.pnl > 0) / len(reason_trades) * 100
            # Split by direction
            r_longs = [t for t in reason_trades if t.side == "LONG"]
            r_shorts = [t for t in reason_trades if t.side == "SHORT"]
            print(f"   {reason}: {count} ({pct:.1f}%) | WR={reason_wr:.1f}% | "
                  f"P&L=${reason_pnl:,.2f} | {len(r_longs)}L/{len(r_shorts)}S")

        print(f"\n[FINAL CAPITAL]: ${self.capital:,.2f}")
        print(f"   Return: {(self.capital - INITIAL_CAPITAL) / INITIAL_CAPITAL * 100:.1f}%")
        print(f"{'='*60}\n")


# ============================================================
# MAIN
# ============================================================

def save_trades(result: BacktestResult, filename: str):
    """Save trade log CSV."""
    if result.trades:
        trades_df = pd.DataFrame([
            {
                "symbol": t.symbol,
                "side": t.side,
                "regime": t.regime,
                "entry_time": t.entry_time.isoformat(),
                "exit_time": t.exit_time.isoformat(),
                "entry_price": t.entry_price,
                "exit_price": t.exit_price,
                "qty": t.qty,
                "exit_reason": t.exit_reason.value,
                "pnl": round(t.pnl, 2),
                "r_multiple": round(t.r_multiple, 2),
                "hold_minutes": t.hold_minutes
            }
            for t in result.trades
        ])
        # If window override is set, filter trades to the requested window
        # (data fetch pulls warmup/buffer bars, so unfiltered CSV would include trades from prior days)
        if _BACKTEST_START_DATE is not None:
            # entry_time is already YYYY-MM-DDTHH:MM:SS from isoformat(); use string slicing
            # for robustness (avoids pandas to_datetime dtype surprises across versions).
            start_str = _BACKTEST_START_DATE.isoformat()
            end_str = _BACKTEST_END_DATE.isoformat() if _BACKTEST_END_DATE is not None else "9999-12-31"
            entry_dates = trades_df["entry_time"].astype(str).str[:10]
            in_window = (entry_dates >= start_str) & (entry_dates <= end_str)
            trades_df = trades_df[in_window].reset_index(drop=True)
        trades_df.to_csv(filename, index=False)
        print(f"Trade log saved to {filename} ({len(trades_df)} trades)")


def main():
    import sys
    import argparse

    parser = argparse.ArgumentParser(
        description="Backtest directional_bot.py strategy (long + short)",
        epilog=(
            "Examples:\n"
            "  python directional_bot_backtest.py\n"
            "  python directional_bot_backtest.py --start 2023-06-01 --end 2023-12-29\n"
            "  python directional_bot_backtest.py --compare\n"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--start", type=str, help="Start date (YYYY-MM-DD). Overrides BACKTEST_DAYS.")
    parser.add_argument("--end", type=str, help="End date (YYYY-MM-DD). Defaults to today when --start is given.")
    parser.add_argument("--output", type=str, default=None,
                        help="Output trades CSV path (default: directional_bot_backtest_{suffix}.csv)")
    parser.add_argument("--compare", action="store_true",
                        help="Run A/B comparison (baseline vs scanner v2)")
    args = parser.parse_args()

    # Apply window overrides
    global _BACKTEST_START_DATE, _BACKTEST_END_DATE
    if args.end:
        _BACKTEST_END_DATE = date.fromisoformat(args.end)
    elif args.start:
        _BACKTEST_END_DATE = datetime.now(ET).date()
    if args.start:
        _BACKTEST_START_DATE = date.fromisoformat(args.start)

    if _BACKTEST_START_DATE and _BACKTEST_END_DATE:
        print(f"[WINDOW OVERRIDE] {_BACKTEST_START_DATE} -> {_BACKTEST_END_DATE}")

    ab_mode = args.compare

    if ab_mode:
        # A/B comparison: run baseline, then scanner v2
        global USE_SCANNER_FEATURES, USE_EXPANDED_UNIVERSE, USE_SPY_VWAP_GATE, USE_RVOL_QUALITY_BOOST, BACKTEST_SYMBOLS

        # --- Run A: Baseline (no scanner features) ---
        USE_SCANNER_FEATURES = False
        USE_EXPANDED_UNIVERSE = False
        USE_SPY_VWAP_GATE = False
        USE_RVOL_QUALITY_BOOST = False
        BACKTEST_SYMBOLS = list(CORE_SYMBOLS)

        engine_a = BacktestEngine()
        result_a = engine_a.run_backtest(BACKTEST_SYMBOLS)
        save_trades(result_a, "directional_bot_backtest_baseline.csv")

        # --- Run B: Scanner v2 ---
        USE_SCANNER_FEATURES = True
        USE_EXPANDED_UNIVERSE = True
        USE_SPY_VWAP_GATE = True
        USE_RVOL_QUALITY_BOOST = True
        BACKTEST_SYMBOLS = list(CORE_SYMBOLS) + list(SCANNER_SYMBOLS)

        engine_b = BacktestEngine()
        result_b = engine_b.run_backtest(BACKTEST_SYMBOLS)
        save_trades(result_b, "directional_bot_backtest_scanner_v2.csv")

        # --- Print comparison ---
        print(f"\n{'='*60}")
        print("A/B COMPARISON: Baseline vs Scanner v2")
        print(f"{'='*60}")
        ret_a = (result_a.net_pnl / INITIAL_CAPITAL) * 100
        ret_b = (result_b.net_pnl / INITIAL_CAPITAL) * 100
        print(f"{'Metric':<25} {'Baseline':>12} {'Scanner v2':>12} {'Delta':>10}")
        print(f"{'-'*60}")
        print(f"{'Total Trades':<25} {result_a.total_trades:>12} {result_b.total_trades:>12} {result_b.total_trades - result_a.total_trades:>+10}")
        print(f"{'Win Rate':<25} {result_a.win_rate:>11.1f}% {result_b.win_rate:>11.1f}% {result_b.win_rate - result_a.win_rate:>+9.1f}%")
        print(f"{'Profit Factor':<25} {result_a.profit_factor:>12.2f} {result_b.profit_factor:>12.2f} {result_b.profit_factor - result_a.profit_factor:>+10.2f}")
        print(f"{'Net P&L':<25} ${result_a.net_pnl:>10,.0f} ${result_b.net_pnl:>10,.0f} ${result_b.net_pnl - result_a.net_pnl:>+9,.0f}")
        print(f"{'Return':<25} {ret_a:>11.1f}% {ret_b:>11.1f}% {ret_b - ret_a:>+9.1f}%")
        print(f"{'Max Drawdown':<25} {result_a.max_drawdown:>11.1f}% {result_b.max_drawdown:>11.1f}% {result_b.max_drawdown - result_a.max_drawdown:>+9.1f}%")
        print(f"{'Avg R-Multiple':<25} {result_a.avg_r:>11.2f}R {result_b.avg_r:>11.2f}R {result_b.avg_r - result_a.avg_r:>+9.2f}R")

        # Direction breakdown comparison
        print(f"\n{'--- Direction Breakdown ---':^60}")
        if result_a.long_trades > 0:
            lwr_a = result_a.long_winners / result_a.long_trades * 100
        else:
            lwr_a = 0
        if result_b.long_trades > 0:
            lwr_b = result_b.long_winners / result_b.long_trades * 100
        else:
            lwr_b = 0
        print(f"{'Long Trades':<25} {result_a.long_trades:>12} {result_b.long_trades:>12}")
        print(f"{'Long WR':<25} {lwr_a:>11.1f}% {lwr_b:>11.1f}%")
        print(f"{'Long P&L':<25} ${result_a.long_pnl:>10,.0f} ${result_b.long_pnl:>10,.0f}")
        if result_a.short_trades > 0:
            swr_a = result_a.short_winners / result_a.short_trades * 100
        else:
            swr_a = 0
        if result_b.short_trades > 0:
            swr_b = result_b.short_winners / result_b.short_trades * 100
        else:
            swr_b = 0
        print(f"{'Short Trades':<25} {result_a.short_trades:>12} {result_b.short_trades:>12}")
        print(f"{'Short WR':<25} {swr_a:>11.1f}% {swr_b:>11.1f}%")
        print(f"{'Short P&L':<25} ${result_a.short_pnl:>10,.0f} ${result_b.short_pnl:>10,.0f}")
        print(f"{'='*60}\n")

    else:
        # Single run with current settings
        engine = BacktestEngine()
        result = engine.run_backtest(BACKTEST_SYMBOLS)
        suffix = "scanner_v2" if USE_SCANNER_FEATURES else "baseline"
        output_path = args.output or f"directional_bot_backtest_{suffix}.csv"
        save_trades(result, output_path)


if __name__ == "__main__":
    main()
