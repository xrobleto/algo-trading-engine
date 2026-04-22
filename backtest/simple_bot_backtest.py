"""
Simple Bot Backtest Framework
=============================

Comprehensive backtesting for simple_bot.py Momentum Trading Bot.

Strategy: VWAP + EMA trend confirmation with single-bracket exits
- Entry: Price > VWAP (+0.3%), EMA9 > EMA20 (+0.2%), RVOL >= 1.2x, momentum confirmation
- Exit: Single-bracket (100% @ 1.0R) with full-position trailing stop
- ATR-based stops (5x ATR, min 1.0%)
- Trailing stop on full position (activate @ 0.40R, trail by 0.40R)
- v48: SPY VWAP regime filter (only enter when market is bullish)
"""

import os
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

# --- Backtest Settings (v44: Single-bracket + full trailing + production sync) ---
BACKTEST_SYMBOLS = [
    # v46: Expanded momentum universe (32 symbols)
    # More symbols = more entry opportunities per day

    # Mega-Cap Tech (Core - highest momentum signal quality)
    "NVDA", "AAPL", "MSFT", "GOOGL", "AMZN", "META", "TSLA",

    # Semiconductors
    "AMD", "AVGO",

    # High-Momentum Growth
    "COIN", "PLTR", "HOOD", "SQ",

    # Financials (high-beta subset)
    "JPM", "GS", "BAC", "MS",

    # Healthcare (momentum-friendly)
    "UNH", "LLY",

    # Cloud/SaaS (momentum leaders)
    "NET", "DDOG", "PANW", "NOW", "CRM", "SNOW",

    # Communication / Consumer
    "NFLX", "BA", "CAT",

    # Broad Market ETF
    "DIA",

    # Energy (high-beta movers)
    "CVX", "XOM",
]

BACKTEST_DAYS = 252  # v48: Back to 1yr for faster iteration
INITIAL_CAPITAL = 111_000  # v46: Match actual paper account balance

# --- Window override (set by CLI --start/--end) ---
# When these are set, fetchers use explicit dates instead of "now - days" trailing window.
# Used by engine multi-regime backtests (backtest/engine_regime_backtest.py).
_BACKTEST_START_DATE: Optional[date] = None
_BACKTEST_END_DATE: Optional[date] = None

# --- Entry Parameters (v44: synced with production) ---
MIN_RELATIVE_VOLUME = 1.5      # v48b: Stricter RVOL for high conviction (gap days naturally higher)
MIN_VWAP_DISTANCE_PCT = 0.30   # v46: Restored proven threshold
MIN_EMA_SEPARATION_PCT = 0.20  # v46: Restored proven threshold
MIN_HIGHER_CLOSES = 3          # v46: Restored proven threshold
MIN_MOMENTUM_5MIN_PCT = 0.15   # v46: Restored proven threshold
MIN_PRICE = 20.0
MAX_SPREAD_BPS = 5.0
EMA_FAST = 9
EMA_SLOW = 20

# --- IMPROVEMENT #1: ADX Filter (v40: wider range) ---
# ADX > threshold = strong trend, momentum more likely to reverse
USE_ADX_FILTER = True
ADX_PERIOD = 14
MAX_ADX = 35.0                 # v40: Skip if ADX > 35 (was 30)
MIN_ADX = 10.0                 # v40: Skip if ADX < 10 (was 12)

# --- IMPROVEMENT #2: Time-Based Filters (v44: synced with production) ---
# Analyzer found: 12:00-13:00 = 93.8% WR, 11:00-12:00 = 54.8% WR
USE_TIME_FILTERS = True
NO_TRADE_FIRST_MINUTES = 15    # v46: 15 mins (earlier entries to catch opening momentum)
NO_TRADE_LAST_MINUTES = 60     # Skip last 60 mins
SKIP_11AM_HOUR = False         # v45: Re-enabled 11 AM hour (was 54.8% WR but still profitable with trailing)

# --- IMPROVEMENT #3: Dynamic Position Sizing (Volatility-Based) ---
USE_DYNAMIC_SIZING = True
# High volatility = smaller position, Low volatility = larger position
VOL_REGIME_LOW_THRESHOLD = 0.8     # ATR < 80% of 20-day avg = low vol
VOL_REGIME_HIGH_THRESHOLD = 1.3    # ATR > 130% of 20-day avg = high vol
SIZE_MULT_LOW_VOL = 1.25           # 25% larger position in low vol
SIZE_MULT_NORMAL_VOL = 1.00        # Normal sizing
SIZE_MULT_HIGH_VOL = 0.60          # 40% smaller position in high vol

# --- ATR settings (v43: Analyzer-Optimized - wider stops) ---
# Analyzer found: Recommended stop 1.25-1.74%, MAE median 0.83%
ATR_PERIOD = 14
ATR_STOP_MULTIPLIER = 4.0     # v48b: Tighter stops for intraday (smaller 1R = more shares)
MIN_STOP_DISTANCE_PCT = 0.70  # v48b: 0.7% min stop (tighter for intraday gap plays)

# --- Exit Parameters (v48c: higher TP cap for gap-up runners) ---
SCALP_TP_R = 3.00             # v48c: Take profit at 3R (more room for gap runners)

# --- Signal Quality Gate (v44: only take high-quality setups from large universe) ---
MIN_SIGNAL_SCORE = 20            # v46: Moderate gate (was 30)

# --- Trailing Stop (v48c: medium trail for bigger gap-up winners) ---
USE_TRAILING_STOP = True
TRAILING_STOP_ACTIVATION_R = 0.60    # v48c: Activate at 0.6R (proven quality moves)
TRAILING_STOP_DISTANCE_R = 0.50      # v48c: Medium 0.5R trail (room for continuation)
OVERNIGHT_TRAIL_DISTANCE_R = 0.50    # v48c: N/A (intraday only)

# --- v45: Adaptive Trailing Stop Distance ---
# Adjusts trail distance based on how strong the move has been (peak R-multiple).
# Strong runners get more room; weak grinds get taken off quickly.
USE_ADAPTIVE_TRAILING = False          # Temporarily disabled for v44 baseline comparison
TRAIL_DISTANCE_STRONG_R = 0.50       # Peak >= 1.2R: strong runner
TRAIL_DISTANCE_NORMAL_R = 0.40       # Peak 0.7-1.2R: standard
TRAIL_DISTANCE_WEAK_R = 0.25         # Peak < 0.7R: just activated
TRAIL_STRONG_THRESHOLD = 1.2
TRAIL_WEAK_THRESHOLD = 0.7
# Time-based tightening (afternoon momentum fades)
TRAIL_TIME_TIGHTEN_HOUR = 13         # After 1 PM: tighten by 25%
TRAIL_TIME_TIGHTEN_MULT = 0.75

# --- v45: Enhanced Entry Filters ---
# Daily SMA20 trend context (reject stocks in daily downtrends)
USE_DAILY_SMA_FILTER = False          # v45b: Disabled - cut 59% of trades without improving WR
DAILY_SMA_PERIOD = 20
DAILY_SMA_BUFFER_PCT = 0.5           # Allow 0.5% below SMA

# 5-min higher lows (structural uptrend on higher timeframe)
USE_5MIN_HIGHER_LOWS = False          # v45b: Disabled - too restrictive, O(n²) per-bar loop
MTF_HIGHER_LOWS_COUNT = 2            # Need 2 of 3 transitions to be higher

# Volume confirmation on entry bar
USE_VOLUME_CONFIRMATION = False       # v45b: Disabled - filtered good trades equally
VOLUME_CONFIRM_MULT = 0.8            # Entry bar >= 0.8x avg recent volume

# --- Risk Management (v48b: intraday concentrated + margin) ---
MAX_RISK_PER_TRADE_PCT = 0.15     # v48b: 15% risk per trade (high conviction gap-ups)
MAX_POSITIONS = 4                 # v48b: 4 max (concentrated)
MAX_HOLD_MINUTES = 390            # v48b: Intraday only (6.5hr session)
POSITION_SIZE_PCT = 2.00          # v48b: 2x per position (margin)
MAX_CAPITAL_USAGE_PCT = 4.00      # v48b: 4x total PDT margin deployment

# --- v48: Market Filters ---
# SPY Regime: Only enter when SPY is above VWAP (bullish market context)
USE_SPY_REGIME_FILTER = True   # v48c: SPY > VWAP filter (very effective with gap-up stocks)
SPY_VWAP_BUFFER_PCT = 0.0  # SPY must be >= VWAP + 0.0%
# Gap-Up Filter: Only trade stocks that gapped up from prev close
USE_GAP_UP_FILTER = True
MIN_GAP_UP_PCT = 0.50  # v48b: Stock must gap up >= 0.5% (overnight catalyst)

# --- Session Timing (v44: earlier cutoff to reduce EOD closes) ---
RTH_START = (9, 30)    # 9:30 AM ET
RTH_END = (16, 0)      # 4:00 PM ET
LATE_CUTOFF = (15, 0)  # v46: Allow entries until 3:00 PM ET (trailing stop handles exits quickly)
EOD_CLOSE = (15, 55)   # Close all positions by 3:55 PM ET


# ============================================================
# DATA STRUCTURES
# ============================================================

class ExitReason(Enum):
    SCALP_TP = "SCALP_TP"
    RUNNER_TP = "RUNNER_TP"
    STOP_LOSS = "STOP_LOSS"
    TRAILING_STOP = "TRAILING_STOP"
    EOD_CLOSE = "EOD_CLOSE"
    TIME_EXIT = "TIME_EXIT"


@dataclass
class BracketPosition:
    """Tracks a single-bracket position with full-position trailing stop."""
    symbol: str
    entry_time: datetime
    entry_price: float
    stop_price: float
    risk_per_share: float  # 1R
    qty: int
    tp_price: float

    # Position state
    active: bool = True
    exit_price: float = 0.0
    exit_reason: Optional[ExitReason] = None
    exit_time: Optional[datetime] = None

    # Trailing stop tracking (applied to full position)
    trail_active: bool = False
    highest_price: float = 0.0
    trail_stop: float = 0.0

    # Calculated fields
    pnl: float = 0.0

    @property
    def is_closed(self) -> bool:
        return not self.active


@dataclass
class BacktestTrade:
    """Completed trade record."""
    symbol: str
    entry_time: datetime
    exit_time: datetime
    entry_price: float
    exit_price: float
    qty: int
    side: str
    exit_reason: ExitReason
    pnl: float
    r_multiple: float
    hold_minutes: int


@dataclass
class BacktestResult:
    """Aggregated backtest results."""
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

    # Exit reason breakdown
    exit_reasons: Dict[str, int] = field(default_factory=dict)

    trades: List[BacktestTrade] = field(default_factory=list)


# ============================================================
# DATA FETCHING
# ============================================================

def fetch_minute_bars(symbol: str, days: int = 30) -> Optional[pd.DataFrame]:
    """Fetch minute bars from Polygon with pagination for full coverage."""
    import time as _time

    # Window override: explicit start/end (via CLI) takes precedence over trailing days
    if _BACKTEST_END_DATE is not None:
        end_date = _BACKTEST_END_DATE
    else:
        end_date = datetime.now(ET).date()
    if _BACKTEST_START_DATE is not None:
        start_date = _BACKTEST_START_DATE
    else:
        start_date = end_date - timedelta(days=days + 10)  # Extra buffer for weekends

    # v47i: Paginate in ~120-day chunks to avoid 50K limit truncation
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
                _time.sleep(12)  # Rate limit - wait and retry
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
        _time.sleep(0.25)  # Rate limit courtesy

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
    """Fetch daily bars for RVOL calculation."""
    # Window override: explicit start/end (via CLI) takes precedence over trailing days
    if _BACKTEST_END_DATE is not None:
        end_date = _BACKTEST_END_DATE
    else:
        end_date = datetime.now(ET).date()
    if _BACKTEST_START_DATE is not None:
        # Always pull +30 days of warmup before the window so RVOL/20-day avg is primed
        start_date = _BACKTEST_START_DATE - timedelta(days=30)
    else:
        start_date = end_date - timedelta(days=days + 30)  # Extra for 20-day average

    url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/day/{start_date}/{end_date}"
    params = {
        "adjusted": "true",
        "sort": "asc",
        "limit": 500,  # v47i: Increased for longer backtests
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
        df = df.rename(columns={"v": "volume"})
        df = df[["date", "volume"]]

        return df

    except Exception as e:
        return None


# ============================================================
# INDICATORS
# ============================================================

def calculate_ema(series: pd.Series, period: int) -> pd.Series:
    """Calculate Exponential Moving Average."""
    return series.ewm(span=period, adjust=False).mean()


def calculate_atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    """Calculate Average True Range."""
    high = df["high"]
    low = df["low"]
    close = df["close"]

    tr1 = high - low
    tr2 = abs(high - close.shift(1))
    tr3 = abs(low - close.shift(1))
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

    return tr.rolling(window=period).mean()


def calculate_adx(df: pd.DataFrame, period: int = 14) -> pd.Series:
    """Calculate ADX (Average Directional Index)."""
    high = df["high"]
    low = df["low"]
    close = df["close"]

    # Calculate +DM and -DM
    plus_dm = high.diff()
    minus_dm = -low.diff()

    plus_dm = plus_dm.where((plus_dm > minus_dm) & (plus_dm > 0), 0.0)
    minus_dm = minus_dm.where((minus_dm > plus_dm) & (minus_dm > 0), 0.0)

    # Calculate True Range
    tr1 = high - low
    tr2 = abs(high - close.shift(1))
    tr3 = abs(low - close.shift(1))
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

    # Smoothed averages
    atr = tr.rolling(period).mean()
    plus_di = 100 * (plus_dm.rolling(period).mean() / atr)
    minus_di = 100 * (minus_dm.rolling(period).mean() / atr)

    # DX and ADX
    dx = 100 * abs(plus_di - minus_di) / (plus_di + minus_di + 1e-10)
    adx = dx.rolling(period).mean()
    return adx


def calculate_atr_ratio(df: pd.DataFrame, period: int = 14, lookback: int = 20) -> pd.Series:
    """Calculate ATR ratio (current ATR / 20-day avg ATR) for volatility regime."""
    atr = calculate_atr(df, period)
    atr_avg = atr.rolling(lookback).mean()
    return atr / atr_avg


def calculate_vwap(df: pd.DataFrame) -> pd.Series:
    """Calculate VWAP (resetting daily)."""
    df = df.copy()
    df["date"] = df["timestamp"].dt.date
    df["typical_price"] = (df["high"] + df["low"] + df["close"]) / 3
    df["tp_vol"] = df["typical_price"] * df["volume"]

    vwap = df.groupby("date").apply(
        lambda x: x["tp_vol"].cumsum() / x["volume"].cumsum()
    ).reset_index(level=0, drop=True)

    return vwap


def calculate_relative_volume(minute_df: pd.DataFrame, daily_df: pd.DataFrame) -> pd.Series:
    """Calculate intraday relative volume."""
    minute_df = minute_df.copy()
    minute_df["date"] = minute_df["timestamp"].dt.date

    # Calculate 20-day average volume
    daily_df = daily_df.copy()
    daily_df["avg_volume_20d"] = daily_df["volume"].rolling(20).mean()

    # Map average volume to minute bars
    avg_vol_map = dict(zip(daily_df["date"], daily_df["avg_volume_20d"]))

    # Calculate cumulative volume per day
    minute_df["cum_volume"] = minute_df.groupby("date")["volume"].cumsum()
    minute_df["avg_volume"] = minute_df["date"].map(avg_vol_map)

    # Minutes into session
    minute_df["mins_into_session"] = (
        (minute_df["timestamp"].dt.hour - 9) * 60 +
        minute_df["timestamp"].dt.minute - 30
    )

    # Expected volume at this time of day (linear approximation)
    total_rth_minutes = 390
    minute_df["expected_volume"] = minute_df["avg_volume"] * (minute_df["mins_into_session"] / total_rth_minutes)
    minute_df["expected_volume"] = minute_df["expected_volume"].clip(lower=1)

    # Relative volume
    rvol = minute_df["cum_volume"] / minute_df["expected_volume"]

    return rvol


# ============================================================
# SIGNAL DETECTION
# ============================================================

def check_long_setup(df: pd.DataFrame, idx: int, rvol: float, ts: datetime = None) -> Tuple[bool, str]:
    """
    Check if row at idx meets A+ long setup criteria.

    Includes v5 improvements:
    - ADX filter (avoid strong trends and chop)
    - Time-based filters (skip first/last 30 mins)

    Returns: (is_valid, rejection_reason)
    """
    if idx < max(EMA_SLOW, ADX_PERIOD, 5):
        return False, "insufficient_data"

    row = df.iloc[idx]
    price = row["close"]
    vwap = row["vwap"]
    ema_fast = row["ema_fast"]
    ema_slow = row["ema_slow"]

    # IMPROVEMENT #1: ADX Filter - avoid strong trends and choppy markets
    if USE_ADX_FILTER and "adx" in df.columns:
        adx = row["adx"]
        if not pd.isna(adx):
            if adx > MAX_ADX:
                return False, "adx_too_high"  # Strong trend - risky for momentum
            if adx < MIN_ADX:
                return False, "adx_too_low"   # No trend - choppy market

    # IMPROVEMENT #2: Time-based filters (v43: skip 11am hour)
    if USE_TIME_FILTERS and ts is not None:
        hour, minute = ts.hour, ts.minute
        minutes_since_open = (hour - 9) * 60 + (minute - 30)
        minutes_until_close = (16 - hour) * 60 - minute

        if minutes_since_open < NO_TRADE_FIRST_MINUTES:
            return False, "too_early"
        if minutes_until_close < NO_TRADE_LAST_MINUTES:
            return False, "too_late"

        # v43: Skip 11:00-12:00 hour (54.8% WR per analyzer)
        if SKIP_11AM_HOUR and hour == 11:
            return False, "11am_skip"

    # v48b: Gap-up filter - only trade stocks with overnight catalysts
    if USE_GAP_UP_FILTER and "gap_pct" in df.columns:
        gap = row.get("gap_pct") if "gap_pct" in row.index else 0
        if gap is not None and gap < MIN_GAP_UP_PCT:
            return False, "no_gap_up"

    # Price filter
    if price < MIN_PRICE:
        return False, "price_too_low"

    # VWAP distance check (0.5% above VWAP)
    min_vwap_distance = vwap * (1 + MIN_VWAP_DISTANCE_PCT / 100)
    if price < min_vwap_distance:
        return False, "vwap_distance"

    # EMA separation check (0.2%)
    min_ema_separation = ema_slow * (1 + MIN_EMA_SEPARATION_PCT / 100)
    if ema_fast < min_ema_separation:
        return False, "ema_separation"

    # Relative volume check
    if rvol < MIN_RELATIVE_VOLUME:
        return False, "low_rvol"

    # Higher closes check (4 of 5 bars)
    if idx >= 5:
        closes = df["close"].iloc[idx-4:idx+1].values
        higher_count = sum(1 for i in range(1, 5) if closes[i] > closes[i-1])
        if higher_count < MIN_HIGHER_CLOSES:
            return False, "weak_trend"

    # Momentum filter (0.3% in last 5 bars)
    if idx >= 5:
        price_5min_ago = df["close"].iloc[idx - 5]
        momentum = (price - price_5min_ago) / price_5min_ago * 100
        if momentum < MIN_MOMENTUM_5MIN_PCT:
            return False, "weak_momentum"

    # v45: Daily SMA20 trend context
    if USE_DAILY_SMA_FILTER and "daily_sma" in df.columns:
        daily_sma = row.get("daily_sma") if "daily_sma" in row.index else None
        if daily_sma is not None and not pd.isna(daily_sma) and daily_sma > 0:
            buffer = daily_sma * (1 - DAILY_SMA_BUFFER_PCT / 100)
            if price < buffer:
                return False, "below_daily_sma"

    # v45: 5-min higher lows structure
    if USE_5MIN_HIGHER_LOWS and "mtf_higher_lows" in df.columns:
        hl_val = row.get("mtf_higher_lows") if "mtf_higher_lows" in row.index else None
        if hl_val is not None and not hl_val:
            return False, "no_higher_lows"

    # v45: Volume confirmation on entry bar
    if USE_VOLUME_CONFIRMATION and "vol_ratio" in df.columns:
        vr = row.get("vol_ratio") if "vol_ratio" in row.index else None
        if vr is not None and not pd.isna(vr) and vr < VOLUME_CONFIRM_MULT:
            return False, "low_entry_volume"

    return True, "valid"


def get_volatility_size_multiplier(df: pd.DataFrame, idx: int) -> float:
    """
    IMPROVEMENT #3: Get position size multiplier based on volatility regime.

    Low volatility = larger position (more stable)
    High volatility = smaller position (more risk)
    """
    if not USE_DYNAMIC_SIZING or "atr_ratio" not in df.columns:
        return 1.0

    atr_ratio = df.iloc[idx]["atr_ratio"]
    if pd.isna(atr_ratio):
        return 1.0

    if atr_ratio < VOL_REGIME_LOW_THRESHOLD:
        return SIZE_MULT_LOW_VOL
    elif atr_ratio > VOL_REGIME_HIGH_THRESHOLD:
        return SIZE_MULT_HIGH_VOL
    else:
        return SIZE_MULT_NORMAL_VOL


# ============================================================
# BACKTEST ENGINE
# ============================================================

class BacktestEngine:
    """Run backtest for simple_bot strategy."""

    def __init__(self):
        self.capital = INITIAL_CAPITAL
        self.positions: Dict[str, BracketPosition] = {}
        self.trades: List[BacktestTrade] = []
        self.equity_curve: List[float] = [INITIAL_CAPITAL]
        self.daily_pnl: Dict[str, float] = {}

    def run_backtest(self, symbols: List[str]) -> BacktestResult:
        """Run backtest across all symbols."""
        print(f"\n{'='*60}")
        print("SIMPLE BOT BACKTEST - VWAP + EMA Momentum Strategy")
        print(f"{'='*60}")
        print(f"Symbols: {len(symbols)}")
        print(f"Period: Last {BACKTEST_DAYS} trading days")
        print(f"Initial Capital: ${INITIAL_CAPITAL:,.0f}")
        print(f"{'='*60}\n")

        # Fetch all data first
        print("Fetching market data...")
        all_data = {}
        daily_data = {}

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

        # v48: Always fetch SPY for regime filter (even if not in universe)
        spy_data = None
        if USE_SPY_REGIME_FILTER:
            if "SPY" not in all_data:
                print(f"  SPY (regime)...", end=" ")
                spy_minute = fetch_minute_bars("SPY", BACKTEST_DAYS)
                if spy_minute is not None and len(spy_minute) > 100:
                    print(f"OK ({len(spy_minute)} bars)")
                    spy_data = spy_minute
                else:
                    print("FAILED - regime filter disabled")
            else:
                spy_data = all_data["SPY"]

        print(f"\nLoaded data for {len(all_data)} symbols")

        # Calculate indicators for all symbols
        print("\nCalculating indicators...")
        for symbol, df in all_data.items():
            df["ema_fast"] = calculate_ema(df["close"], EMA_FAST)
            df["ema_slow"] = calculate_ema(df["close"], EMA_SLOW)
            df["atr"] = calculate_atr(df, ATR_PERIOD)
            df["vwap"] = calculate_vwap(df)

            # IMPROVEMENT #1: ADX for trend strength filter
            if USE_ADX_FILTER:
                df["adx"] = calculate_adx(df, ADX_PERIOD)

            # IMPROVEMENT #3: ATR ratio for dynamic position sizing
            if USE_DYNAMIC_SIZING:
                df["atr_ratio"] = calculate_atr_ratio(df, ATR_PERIOD, 20)

            if daily_data.get(symbol) is not None:
                df["rvol"] = calculate_relative_volume(df, daily_data[symbol])
            else:
                df["rvol"] = 1.5  # Default to minimum threshold

            # v48b: Gap-up detection (compare today's open to prev day's close)
            if USE_GAP_UP_FILTER:
                df["_date"] = df["timestamp"].dt.date
                # Get first bar of each day (open) and last bar of each day (close)
                day_groups = df.groupby("_date")
                day_open = day_groups["open"].first()
                day_close = day_groups["close"].last()
                prev_close = day_close.shift(1)
                gap_pct = ((day_open - prev_close) / prev_close * 100).fillna(0)
                # Map gap % back to minute bars
                gap_map = gap_pct.to_dict()
                df["gap_pct"] = df["_date"].map(gap_map).fillna(0)
                df.drop(columns=["_date"], inplace=True)

            # v45: Daily SMA from resampled minute bars
            if USE_DAILY_SMA_FILTER:
                try:
                    df_daily = df.set_index("timestamp").resample('D').agg({
                        'close': 'last'
                    }).dropna().reset_index()
                    if len(df_daily) >= DAILY_SMA_PERIOD:
                        df_daily["sma"] = df_daily["close"].rolling(DAILY_SMA_PERIOD).mean()
                        # Map daily SMA back to minute bars by date
                        sma_by_date = dict(zip(df_daily["timestamp"].dt.date, df_daily["sma"]))
                        df["daily_sma"] = df["timestamp"].dt.date.map(sma_by_date)
                    else:
                        df["daily_sma"] = np.nan
                except Exception:
                    df["daily_sma"] = np.nan

            # v45: 5-min resampled lows for structure check
            if USE_5MIN_HIGHER_LOWS:
                try:
                    df_5m = df.set_index("timestamp").resample('5min').agg({
                        'low': 'min'
                    }).dropna().reset_index()
                    # For each 1-min bar, check if last 4 5-min lows show higher lows
                    df["mtf_higher_lows"] = False
                    for i in range(len(df)):
                        ts_i = df["timestamp"].iloc[i]
                        prior_5m = df_5m[df_5m["timestamp"] <= ts_i].tail(4)
                        if len(prior_5m) >= 4:
                            lows = prior_5m["low"].values
                            rising = sum(1 for j in range(1, len(lows)) if lows[j] > lows[j-1])
                            df.iloc[i, df.columns.get_loc("mtf_higher_lows")] = rising >= MTF_HIGHER_LOWS_COUNT
                except Exception:
                    df["mtf_higher_lows"] = True  # Default: don't block on failure

            # v45: Entry bar volume ratio (current bar vol / recent avg)
            if USE_VOLUME_CONFIRMATION:
                avg_vol = df["volume"].rolling(20, min_periods=5).mean()
                df["vol_ratio"] = df["volume"] / avg_vol.replace(0, np.nan)

            all_data[symbol] = df

        # v48: Calculate SPY VWAP for regime filter
        if spy_data is not None:
            spy_data["vwap"] = calculate_vwap(spy_data)
            # Build a lookup: timestamp -> (spy_close, spy_vwap)
            self.spy_regime = {}
            for _, row in spy_data.iterrows():
                ts_key = row["timestamp"]
                self.spy_regime[ts_key] = (row["close"], row["vwap"])
            print(f"  SPY regime data: {len(self.spy_regime)} timestamps")
        else:
            self.spy_regime = None

        # Run simulation
        print("\nRunning simulation...")
        self._run_simulation(all_data)

        # Calculate results
        result = self._calculate_results()
        self._print_results(result)

        return result

    def _run_simulation(self, all_data: Dict[str, pd.DataFrame]):
        """Run bar-by-bar simulation."""
        # Get all timestamps across all symbols
        all_timestamps = set()
        for df in all_data.values():
            all_timestamps.update(df["timestamp"].tolist())
        all_timestamps = sorted(all_timestamps)

        print(f"  Processing {len(all_timestamps)} timestamps...")

        for ts in all_timestamps:
            # Get current day
            current_date = ts.date()

            # Check if within RTH
            hour, minute = ts.hour, ts.minute
            if not ((hour == 9 and minute >= 30) or (9 < hour < 16)):
                continue

            # Manage existing positions first
            self._manage_positions(ts, all_data)

            # EOD close check (3:55 PM) - v48b: close ALL positions (intraday only)
            if (hour, minute) >= EOD_CLOSE:
                self._close_all_positions(ts, all_data, ExitReason.EOD_CLOSE)
                continue

            # Late day cutoff for new entries (3:30 PM)
            if (hour, minute) >= LATE_CUTOFF:
                continue

            # Skip first 5 minutes (avoid open volatility)
            if hour == 9 and minute < 35:
                continue

            # Check for new entries if we have capacity
            if len(self.positions) < MAX_POSITIONS:
                self._scan_for_entries(ts, all_data)

    def _manage_positions(self, ts: datetime, all_data: Dict[str, pd.DataFrame]):
        """Manage open positions: check stops, trailing stops, TPs."""
        positions_to_remove = []

        for symbol, pos in self.positions.items():
            if pos.is_closed:
                positions_to_remove.append(symbol)
                continue

            df = all_data.get(symbol)
            if df is None:
                continue

            # Get current bar
            current_bars = df[df["timestamp"] == ts]
            if current_bars.empty:
                continue

            row = current_bars.iloc[0]
            high = row["high"]
            low = row["low"]
            close = row["close"]

            # 1. Check stop loss
            if low <= pos.stop_price:
                self._record_exit(pos, ts, pos.stop_price, ExitReason.STOP_LOSS)
                continue

            # 2. Trailing stop management (full position)
            if USE_TRAILING_STOP:
                # R-based trailing stop (convert to percentage for price calc)
                activation_pct = (TRAILING_STOP_ACTIVATION_R * pos.risk_per_share / pos.entry_price) * 100

                # Activate trailing stop when profit reaches activation threshold
                if not pos.trail_active and ((close - pos.entry_price) / pos.entry_price * 100) >= activation_pct:
                    pos.trail_active = True
                    pos.highest_price = close

                # Update trailing stop
                if pos.trail_active:
                    if high > pos.highest_price:
                        pos.highest_price = high

                    # v47e: Two-stage trail - tight intraday, wide overnight
                    is_overnight = ts.date() > pos.entry_time.date()
                    if is_overnight:
                        effective_dist_r = OVERNIGHT_TRAIL_DISTANCE_R
                    elif USE_ADAPTIVE_TRAILING:
                        peak_r = (pos.highest_price - pos.entry_price) / pos.risk_per_share if pos.risk_per_share > 0 else 0
                        if peak_r >= TRAIL_STRONG_THRESHOLD:
                            effective_dist_r = TRAIL_DISTANCE_STRONG_R
                        elif peak_r >= TRAIL_WEAK_THRESHOLD:
                            effective_dist_r = TRAIL_DISTANCE_NORMAL_R
                        else:
                            effective_dist_r = TRAIL_DISTANCE_WEAK_R
                        if ts.hour >= TRAIL_TIME_TIGHTEN_HOUR:
                            effective_dist_r *= TRAIL_TIME_TIGHTEN_MULT
                    else:
                        effective_dist_r = TRAILING_STOP_DISTANCE_R

                    trail_dist_pct = (effective_dist_r * pos.risk_per_share / pos.entry_price) * 100
                    new_trail = pos.highest_price * (1 - trail_dist_pct / 100)
                    if new_trail > pos.trail_stop:
                        pos.trail_stop = new_trail

                    # Check trailing stop hit
                    if low <= pos.trail_stop:
                        self._record_exit(pos, ts, pos.trail_stop, ExitReason.TRAILING_STOP)
                        continue

            # 3. Check take-profit (1.0R)
            if high >= pos.tp_price:
                self._record_exit(pos, ts, pos.tp_price, ExitReason.SCALP_TP)
                continue

            # 4. Time exit check
            hold_minutes = (ts - pos.entry_time).total_seconds() / 60
            if hold_minutes >= MAX_HOLD_MINUTES:
                self._record_exit(pos, ts, close, ExitReason.TIME_EXIT)

        # Clean up closed positions
        for symbol in positions_to_remove:
            del self.positions[symbol]

    def _scan_for_entries(self, ts: datetime, all_data: Dict[str, pd.DataFrame]):
        """Scan for new entry signals. Evaluates all candidates and picks the best."""
        # v48: SPY regime filter - only enter when market is bullish
        if USE_SPY_REGIME_FILTER and self.spy_regime is not None:
            spy_info = self.spy_regime.get(ts)
            if spy_info is not None:
                spy_close, spy_vwap = spy_info
                if spy_vwap > 0:
                    min_spy = spy_vwap * (1 + SPY_VWAP_BUFFER_PCT / 100)
                    if spy_close < min_spy:
                        return  # Market is bearish/neutral - skip entries

        # v44: Collect ALL valid candidates, then rank and pick the best
        candidates = []

        # Pre-compute deployed capital once (buying power guard)
        deployed = sum(p.qty * p.entry_price for p in self.positions.values() if p.active)
        available = self.capital * MAX_CAPITAL_USAGE_PCT - deployed
        if available <= 0:
            return

        for symbol, df in all_data.items():
            # Skip if already in position
            if symbol in self.positions:
                continue

            # Get current bar index
            current_bars = df[df["timestamp"] == ts]
            if current_bars.empty:
                continue

            idx = current_bars.index[0]
            row = df.iloc[idx]
            rvol = row["rvol"] if "rvol" in df.columns and not pd.isna(row["rvol"]) else 1.5

            # Check for long setup (pass timestamp for time-based filters)
            is_valid, reason = check_long_setup(df, idx, rvol, ts)
            if not is_valid:
                continue

            # Calculate position sizing and levels
            entry_price = row["close"]
            atr = row["atr"]

            # ATR-based stops
            if pd.isna(atr) or atr <= 0:
                continue

            atr_stop_distance = atr * ATR_STOP_MULTIPLIER
            min_stop_distance = entry_price * (MIN_STOP_DISTANCE_PCT / 100)
            stop_distance = max(atr_stop_distance, min_stop_distance)
            stop_price = entry_price - stop_distance
            risk_per_share = stop_distance

            # Dynamic position sizing based on volatility
            vol_size_mult = get_volatility_size_multiplier(df, idx)

            # Position sizing (with volatility adjustment)
            max_risk = self.capital * MAX_RISK_PER_TRADE_PCT * vol_size_mult
            total_qty = int(max_risk / risk_per_share)

            if total_qty <= 0:
                continue

            # Per-position cap (respect buying power)
            max_position_value = self.capital * POSITION_SIZE_PCT
            if total_qty * entry_price > max_position_value:
                total_qty = int(max_position_value / entry_price)

            # Buying power guard
            if total_qty * entry_price > available:
                total_qty = int(available / entry_price)

            if total_qty <= 0:
                continue

            # Score this candidate for ranking (higher = better setup)
            vwap = row["vwap"] if not pd.isna(row["vwap"]) else entry_price
            ema_fast = row["ema_fast"] if not pd.isna(row["ema_fast"]) else entry_price
            ema_slow = row["ema_slow"] if not pd.isna(row["ema_slow"]) else entry_price

            # Score components (0-100 each):
            # 1. RVOL strength (1.2x=0, 3.0x+=100)
            rvol_score = min(100, max(0, (rvol - 1.2) / 1.8 * 100))
            # 2. VWAP distance (0.3%=0, 1.0%+=100)
            vwap_dist = (entry_price - vwap) / vwap * 100 if vwap > 0 else 0
            vwap_score = min(100, max(0, (vwap_dist - 0.3) / 0.7 * 100))
            # 3. Momentum (use 5-bar close change)
            if idx >= 5:
                mom = (entry_price - df["close"].iloc[idx - 5]) / df["close"].iloc[idx - 5] * 100
                mom_score = min(100, max(0, (mom - 0.15) / 0.85 * 100))
            else:
                mom_score = 0
            # 4. EMA separation strength (0.2%=0, 0.8%+=100)
            ema_sep = (ema_fast - ema_slow) / ema_slow * 100 if ema_slow > 0 else 0
            ema_score = min(100, max(0, (ema_sep - 0.2) / 0.6 * 100))

            # Weighted composite: RVOL and momentum matter most
            score = (rvol_score * 0.35) + (mom_score * 0.30) + (vwap_score * 0.20) + (ema_score * 0.15)

            candidates.append({
                "symbol": symbol,
                "score": score,
                "entry_price": entry_price,
                "stop_price": stop_price,
                "risk_per_share": risk_per_share,
                "total_qty": total_qty,
            })

        # Filter by minimum score, then rank by quality
        candidates = [c for c in candidates if c["score"] >= MIN_SIGNAL_SCORE]
        if not candidates:
            return

        candidates.sort(key=lambda c: c["score"], reverse=True)

        # v46: Enter MULTIPLE candidates (fill all available position slots)
        slots_available = MAX_POSITIONS - len(self.positions)
        for candidate in candidates[:slots_available]:
            # Recalculate available capital for each entry
            deployed = sum(p.qty * p.entry_price for p in self.positions.values() if p.active)
            remaining = self.capital * MAX_CAPITAL_USAGE_PCT - deployed
            if remaining <= 0:
                break

            # Adjust qty to remaining capital
            qty = candidate["total_qty"]
            if qty * candidate["entry_price"] > remaining:
                qty = int(remaining / candidate["entry_price"])
            if qty <= 0:
                break

            tp_price = candidate["entry_price"] + (candidate["risk_per_share"] * SCALP_TP_R)

            pos = BracketPosition(
                symbol=candidate["symbol"],
                entry_time=ts,
                entry_price=candidate["entry_price"],
                stop_price=candidate["stop_price"],
                risk_per_share=candidate["risk_per_share"],
                qty=qty,
                tp_price=tp_price,
            )
            self.positions[candidate["symbol"]] = pos

    def _record_exit(self, pos: BracketPosition, ts: datetime, exit_price: float,
                     reason: ExitReason):
        """Record position exit and update capital."""
        if not pos.active:
            return

        pos.active = False
        pos.exit_price = exit_price
        pos.exit_reason = reason
        pos.exit_time = ts

        # Calculate P&L
        pnl = (exit_price - pos.entry_price) * pos.qty
        pos.pnl = pnl
        r_mult = (exit_price - pos.entry_price) / pos.risk_per_share
        hold_mins = int((ts - pos.entry_time).total_seconds() / 60)

        # Record trade
        trade = BacktestTrade(
            symbol=pos.symbol,
            entry_time=pos.entry_time,
            exit_time=ts,
            entry_price=pos.entry_price,
            exit_price=exit_price,
            qty=pos.qty,
            side="LONG",
            exit_reason=reason,
            pnl=pnl,
            r_multiple=r_mult,
            hold_minutes=hold_mins
        )
        self.trades.append(trade)

        # Update capital
        self.capital += pnl
        self.equity_curve.append(self.capital)

        # Track daily P&L
        day_str = ts.strftime("%Y-%m-%d")
        self.daily_pnl[day_str] = self.daily_pnl.get(day_str, 0) + pnl

    def _close_losing_positions(self, ts: datetime, all_data: Dict[str, pd.DataFrame], reason: ExitReason):
        """v47c: Close only LOSING positions at EOD. Hold winners overnight for swing trading."""
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
            unrealized_pnl = (close_price - pos.entry_price) * pos.qty

            # Only close if losing (unrealized P&L <= 0)
            # Winning positions carry overnight with trailing stop protection
            if unrealized_pnl <= 0:
                self._record_exit(pos, ts, close_price, reason)

    def _close_all_positions(self, ts: datetime, all_data: Dict[str, pd.DataFrame], reason: ExitReason):
        """Close all open positions at current price."""
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
        """Calculate final backtest results."""
        result = BacktestResult()
        result.trades = self.trades
        result.total_trades = len(self.trades)

        if result.total_trades == 0:
            return result

        # Basic stats
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

        return result

    def _print_results(self, result: BacktestResult):
        """Print formatted backtest results."""
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

        print(f"\n[EXIT REASONS]")
        for reason, count in sorted(result.exit_reasons.items(), key=lambda x: -x[1]):
            pct = count / result.total_trades * 100
            reason_trades = [t for t in result.trades if t.exit_reason.value == reason]
            reason_pnl = sum(t.pnl for t in reason_trades)
            reason_wr = sum(1 for t in reason_trades if t.pnl > 0) / len(reason_trades) * 100 if reason_trades else 0
            print(f"   {reason}: {count} ({pct:.1f}%) | WR={reason_wr:.1f}% | P&L=${reason_pnl:,.2f}")

        print(f"\n[FINAL CAPITAL]: ${self.capital:,.2f}")
        print(f"   Return: {(self.capital - INITIAL_CAPITAL) / INITIAL_CAPITAL * 100:.1f}%")
        print(f"{'='*60}\n")


# ============================================================
# MAIN
# ============================================================

def main():
    """Run the backtest."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Backtest simple_bot.py strategy",
        epilog=(
            "Examples:\n"
            "  python simple_bot_backtest.py\n"
            "  python simple_bot_backtest.py --start 2023-06-01 --end 2023-12-29\n"
            "  python simple_bot_backtest.py --start 2025-10-01 --end 2026-04-01 --output bt_P6.csv\n"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--start", type=str, help="Start date (YYYY-MM-DD). Overrides BACKTEST_DAYS.")
    parser.add_argument("--end", type=str, help="End date (YYYY-MM-DD). Defaults to today when --start is given.")
    parser.add_argument("--output", type=str, default="simple_bot_backtest_trades.csv",
                        help="Output trades CSV path (default: simple_bot_backtest_trades.csv)")
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

    engine = BacktestEngine()
    result = engine.run_backtest(BACKTEST_SYMBOLS)

    # Save trade log
    if result.trades:
        trades_df = pd.DataFrame([
            {
                "symbol": t.symbol,
                "entry_time": t.entry_time.isoformat(),
                "exit_time": t.exit_time.isoformat(),
                "entry_price": t.entry_price,
                "exit_price": t.exit_price,
                "qty": t.qty,
                "side": t.side,
                "exit_reason": t.exit_reason.value,
                "pnl": round(t.pnl, 2),
                "r_multiple": round(t.r_multiple, 2),
                "hold_minutes": t.hold_minutes
            }
            for t in result.trades
        ])
        # If window override is set, filter trades to the requested window
        # (data fetch includes warmup/buffer, so unfiltered CSV would include trades from prior days)
        if _BACKTEST_START_DATE is not None:
            # entry_time is YYYY-MM-DDTHH:MM:SS from isoformat(); use string slicing
            # for robustness (avoids pandas to_datetime dtype surprises across versions).
            start_str = _BACKTEST_START_DATE.isoformat()
            end_str = _BACKTEST_END_DATE.isoformat() if _BACKTEST_END_DATE is not None else "9999-12-31"
            entry_dates = trades_df["entry_time"].astype(str).str[:10]
            in_window = (entry_dates >= start_str) & (entry_dates <= end_str)
            trades_df = trades_df[in_window].reset_index(drop=True)
        trades_df.to_csv(args.output, index=False)
        print(f"Trade log saved to {args.output} ({len(trades_df)} trades)")


if __name__ == "__main__":
    main()
