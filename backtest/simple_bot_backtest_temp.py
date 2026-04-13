"""
Simple Bot Backtest Framework
=============================

Comprehensive backtesting for simple_bot.py Momentum Trading Bot.

Strategy: VWAP + EMA trend confirmation with dual-bracket exits
- Entry: Price > VWAP (+0.5%), EMA9 > EMA20 (+0.2%), RVOL >= 1.5x, momentum confirmation
- Exit: Dual-bracket (60% scalp @ 0.75R, 40% runner @ 1.75R)
- ATR-based stops (6x ATR for mega-caps, 8x for leveraged)
- Trailing stop for runner bracket (activate @ 1.0R, trail by 0.5R)
"""

import os
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

# ============================================================
# CONFIGURATION
# ============================================================

ET = ZoneInfo("America/New_York")
POLYGON_API_KEY = os.getenv("POLYGON_API_KEY", "eskzZ5O4QQyYpS5TsA3pe5WMeepeaxmW")

# --- Backtest Settings ---
BACKTEST_SYMBOLS = [
    # Mega-Cap Tech
    "NVDA", "AAPL", "MSFT", "GOOGL", "AMZN", "META", "TSLA",
    # Semiconductors
    "AMD", "AVGO",
    # High-Momentum Growth
    "COIN", "PLTR", "MSTR", "SHOP", "SQ",
    # Financials
    "JPM", "GS", "BAC",
    # Healthcare
    "UNH", "LLY",
    # Cloud/SaaS
    "NET", "DDOG", "CRWD",
    # ETFs
    "SPY", "QQQ", "IWM",
]

BACKTEST_DAYS = 30  # Days of historical data
INITIAL_CAPITAL = 100_000

# --- Entry Parameters (OPTIMIZED v5 - WITH ADX + TIME FILTERS) ---
MIN_RELATIVE_VOLUME = 1.3      # Slightly lower for more setups
MIN_VWAP_DISTANCE_PCT = 0.25   # Tighter to VWAP
MIN_EMA_SEPARATION_PCT = 0.15  # Allow earlier entries
MIN_HIGHER_CLOSES = 3          # Good balance
MIN_MOMENTUM_5MIN_PCT = 0.20   # Catch more moves
MIN_PRICE = 20.0
MAX_SPREAD_BPS = 5.0
EMA_FAST = 9
EMA_SLOW = 20

# --- IMPROVEMENT #1: ADX Filter (avoid trending markets) ---
# ADX > threshold = strong trend, momentum more likely to reverse
USE_ADX_FILTER = True
ADX_PERIOD = 14
MAX_ADX = 25.0                 # Skip if ADX > 25 (strong trend = risky for momentum)
MIN_ADX = 15.0                 # Skip if ADX < 15 (no trend = choppy)

# --- IMPROVEMENT #2: Time-Based Filters ---
USE_TIME_FILTERS = True
NO_TRADE_FIRST_MINUTES = 30    # Skip first 30 mins (open volatility)
NO_TRADE_LAST_MINUTES = 30     # Skip last 30 mins (close volatility)

# --- IMPROVEMENT #3: Dynamic Position Sizing (Volatility-Based) ---
USE_DYNAMIC_SIZING = True
# High volatility = smaller position, Low volatility = larger position
VOL_REGIME_LOW_THRESHOLD = 0.8     # ATR < 80% of 20-day avg = low vol
VOL_REGIME_HIGH_THRESHOLD = 1.3    # ATR > 130% of 20-day avg = high vol
SIZE_MULT_LOW_VOL = 1.25           # 25% larger position in low vol
SIZE_MULT_NORMAL_VOL = 1.00        # Normal sizing
SIZE_MULT_HIGH_VOL = 0.60          # 40% smaller position in high vol

# --- ATR-Based Stops (OPTIMIZED FINAL - USE ATR like v2) ---
USE_PERCENTAGE_EXITS = False   # Back to ATR-based (v2 was best)
STOP_LOSS_PCT = 0.75           # Not used when USE_PERCENTAGE_EXITS=False
SCALP_TP_PCT = 0.55
RUNNER_TP_PCT = 0.90

# ATR settings (v2 configuration)
ATR_PERIOD = 14
ATR_STOP_MULTIPLIER = 6.0     # 6x ATR for wider stops
MIN_STOP_DISTANCE_PCT = 0.70  # Minimum stop distance

# --- Exit Parameters (OPTIMIZED FINAL - v2 settings) ---
SCALP_BRACKET_PCT = 0.8
RUNNER_BRACKET_PCT = 0.20     # 20% runner

SCALP_TP_R = 0.50             # 0.50R for scalp
RUNNER_TP_R = 1.25            # 1.25R for runner

# --- Trailing Stop for Runner (OPTIMIZED FINAL) ---
USE_TRAILING_STOP = True
TRAILING_STOP_ACTIVATION_PCT = 0.40  # Used for % mode
TRAILING_STOP_DISTANCE_PCT = 0.20

# R-based trailing stop values (used when USE_PERCENTAGE_EXITS=False)
TRAILING_STOP_ACTIVATION_R = 0.40    # Activate at 0.40R
TRAILING_STOP_DISTANCE_R = 0.20      # Trail by 0.20R

# --- Risk Management ---
MAX_RISK_PER_TRADE_PCT = 0.01     # 1% risk per trade
MAX_POSITIONS = 2                 # Max concurrent positions
MAX_HOLD_MINUTES = 390            # Max hold time (RTH = 6.5 hours)
POSITION_SIZE_PCT = 0.10          # 10% of capital per position
MAX_CAPITAL_USAGE_PCT = 0.50      # 50% max capital deployed

# --- Session Timing ---
RTH_START = (9, 30)    # 9:30 AM ET
RTH_END = (16, 0)      # 4:00 PM ET
LATE_CUTOFF = (15, 30) # No new entries after 3:30 PM ET
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
    """Tracks a dual-bracket position."""
    symbol: str
    entry_time: datetime
    entry_price: float
    stop_price: float
    risk_per_share: float  # 1R

    # Bracket quantities
    scalp_qty: int
    runner_qty: int
    total_qty: int

    # Bracket targets (required fields first)
    scalp_tp_price: float
    runner_tp_price: float

    # Scalp bracket (optional/default fields)
    scalp_active: bool = True
    scalp_exit_price: float = 0.0
    scalp_exit_reason: Optional[ExitReason] = None
    scalp_exit_time: Optional[datetime] = None

    # Runner bracket
    runner_active: bool = True
    runner_exit_price: float = 0.0
    runner_exit_reason: Optional[ExitReason] = None
    runner_exit_time: Optional[datetime] = None

    # Trailing stop tracking
    trail_active: bool = False
    highest_price: float = 0.0
    trail_stop: float = 0.0

    # Calculated fields
    total_pnl: float = 0.0
    scalp_pnl: float = 0.0
    runner_pnl: float = 0.0

    @property
    def is_closed(self) -> bool:
        return not self.scalp_active and not self.runner_active

    def calculate_pnl(self):
        """Calculate total P&L from both brackets."""
        if not self.scalp_active and self.scalp_exit_price > 0:
            self.scalp_pnl = (self.scalp_exit_price - self.entry_price) * self.scalp_qty
        if not self.runner_active and self.runner_exit_price > 0:
            self.runner_pnl = (self.runner_exit_price - self.entry_price) * self.runner_qty
        self.total_pnl = self.scalp_pnl + self.runner_pnl


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
    bracket_type: str  # "scalp" or "runner"
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

    # Breakdown by bracket type
    scalp_trades: int = 0
    scalp_winners: int = 0
    scalp_win_rate: float = 0.0
    scalp_pnl: float = 0.0

    runner_trades: int = 0
    runner_winners: int = 0
    runner_win_rate: float = 0.0
    runner_pnl: float = 0.0

    # Exit reason breakdown
    exit_reasons: Dict[str, int] = field(default_factory=dict)

    trades: List[BacktestTrade] = field(default_factory=list)


# ============================================================
# DATA FETCHING
# ============================================================

def fetch_minute_bars(symbol: str, days: int = 30) -> Optional[pd.DataFrame]:
    """Fetch minute bars from Polygon."""
    end_date = datetime.now(ET).date()
    start_date = end_date - timedelta(days=days + 10)  # Extra buffer for weekends

    url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/minute/{start_date}/{end_date}"
    params = {
        "adjusted": "true",
        "sort": "asc",
        "limit": 50000,
        "apiKey": POLYGON_API_KEY
    }

    try:
        resp = requests.get(url, params=params, timeout=30)
        if resp.status_code != 200:
            print(f"  {symbol}: API error {resp.status_code}")
            return None

        data = resp.json()
        results = data.get("results", [])
        if not results:
            print(f"  {symbol}: No data returned")
            return None

        df = pd.DataFrame(results)
        df["timestamp"] = pd.to_datetime(df["t"], unit="ms", utc=True).dt.tz_convert(ET)
        df = df.rename(columns={"o": "open", "h": "high", "l": "low", "c": "close", "v": "volume"})
        df = df[["timestamp", "open", "high", "low", "close", "volume"]]
        df = df.sort_values("timestamp").reset_index(drop=True)

        # Filter RTH only
        df["hour"] = df["timestamp"].dt.hour
        df["minute"] = df["timestamp"].dt.minute
        df = df[
            ((df["hour"] == 9) & (df["minute"] >= 30)) |
            ((df["hour"] > 9) & (df["hour"] < 16))
        ].copy()
        df = df.drop(columns=["hour", "minute"])
        df = df.reset_index(drop=True)  # Reset index after RTH filter

        return df

    except Exception as e:
        print(f"  {symbol}: Error fetching data: {e}")
        return None


def fetch_daily_bars(symbol: str, days: int = 30) -> Optional[pd.DataFrame]:
    """Fetch daily bars for RVOL calculation."""
    end_date = datetime.now(ET).date()
    start_date = end_date - timedelta(days=days + 30)  # Extra for 20-day average

    url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/day/{start_date}/{end_date}"
    params = {
        "adjusted": "true",
        "sort": "asc",
        "limit": 100,
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

    # IMPROVEMENT #2: Time-based filters
    if USE_TIME_FILTERS and ts is not None:
        hour, minute = ts.hour, ts.minute
        minutes_since_open = (hour - 9) * 60 + (minute - 30)
        minutes_until_close = (16 - hour) * 60 - minute

        if minutes_since_open < NO_TRADE_FIRST_MINUTES:
            return False, "too_early"
        if minutes_until_close < NO_TRADE_LAST_MINUTES:
            return False, "too_late"

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

            all_data[symbol] = df

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

            # EOD close check (3:55 PM)
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
        """Manage open positions: check stops, TPs, trailing stops."""
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

            # Check stop loss (both brackets share same stop)
            if low <= pos.stop_price:
                # Stop hit - close both brackets
                exit_price = pos.stop_price

                if pos.scalp_active:
                    self._record_exit(pos, ts, exit_price, "scalp", ExitReason.STOP_LOSS)

                if pos.runner_active:
                    self._record_exit(pos, ts, exit_price, "runner", ExitReason.STOP_LOSS)

                continue

            # Check scalp TP
            if pos.scalp_active and high >= pos.scalp_tp_price:
                self._record_exit(pos, ts, pos.scalp_tp_price, "scalp", ExitReason.SCALP_TP)

            # Runner management with trailing stop
            if pos.runner_active:
                # Calculate current profit percentage
                profit_pct = (close - pos.entry_price) / pos.entry_price * 100

                if USE_PERCENTAGE_EXITS:
                    # Percentage-based trailing stop activation
                    activation_pct = TRAILING_STOP_ACTIVATION_PCT
                    trail_dist_pct = TRAILING_STOP_DISTANCE_PCT
                else:
                    # R-based trailing stop (convert to percentage for consistency)
                    activation_pct = (TRAILING_STOP_ACTIVATION_R * pos.risk_per_share / pos.entry_price) * 100
                    trail_dist_pct = (TRAILING_STOP_DISTANCE_R * pos.risk_per_share / pos.entry_price) * 100

                # Activate trailing stop
                if USE_TRAILING_STOP and not pos.trail_active and profit_pct >= activation_pct:
                    pos.trail_active = True
                    pos.highest_price = close
                    pos.trail_stop = close * (1 - trail_dist_pct / 100)

                # Update trailing stop
                if pos.trail_active:
                    if high > pos.highest_price:
                        pos.highest_price = high
                        new_trail = high * (1 - trail_dist_pct / 100)
                        if new_trail > pos.trail_stop:
                            pos.trail_stop = new_trail

                    # Check trailing stop hit
                    if low <= pos.trail_stop:
                        self._record_exit(pos, ts, pos.trail_stop, "runner", ExitReason.TRAILING_STOP)
                        continue

                # Check runner TP (if no trailing stop or not yet activated)
                if high >= pos.runner_tp_price:
                    self._record_exit(pos, ts, pos.runner_tp_price, "runner", ExitReason.RUNNER_TP)

            # Time exit check
            hold_minutes = (ts - pos.entry_time).total_seconds() / 60
            if hold_minutes >= MAX_HOLD_MINUTES:
                if pos.scalp_active:
                    self._record_exit(pos, ts, close, "scalp", ExitReason.TIME_EXIT)
                if pos.runner_active:
                    self._record_exit(pos, ts, close, "runner", ExitReason.TIME_EXIT)

        # Clean up closed positions
        for symbol in positions_to_remove:
            del self.positions[symbol]

    def _scan_for_entries(self, ts: datetime, all_data: Dict[str, pd.DataFrame]):
        """Scan for new entry signals."""
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

            if USE_PERCENTAGE_EXITS:
                # Percentage-based stops and targets (OPTIMIZED v3)
                stop_distance = entry_price * (STOP_LOSS_PCT / 100)
                stop_price = entry_price - stop_distance
                risk_per_share = stop_distance

                scalp_tp = entry_price * (1 + SCALP_TP_PCT / 100)
                runner_tp = entry_price * (1 + RUNNER_TP_PCT / 100)
            else:
                # ATR-based stops (original)
                if pd.isna(atr) or atr <= 0:
                    continue

                atr_stop_distance = atr * ATR_STOP_MULTIPLIER
                min_stop_distance = entry_price * (MIN_STOP_DISTANCE_PCT / 100)
                stop_distance = max(atr_stop_distance, min_stop_distance)
                stop_price = entry_price - stop_distance
                risk_per_share = stop_distance

                scalp_tp = entry_price + (risk_per_share * SCALP_TP_R)
                runner_tp = entry_price + (risk_per_share * RUNNER_TP_R)

            # IMPROVEMENT #3: Dynamic position sizing based on volatility
            vol_size_mult = get_volatility_size_multiplier(df, idx)

            # Position sizing (with volatility adjustment)
            max_risk = self.capital * MAX_RISK_PER_TRADE_PCT * vol_size_mult
            total_qty = int(max_risk / risk_per_share)

            if total_qty <= 0:
                continue

            # Check capital limits
            position_value = total_qty * entry_price
            max_position_value = self.capital * MAX_CAPITAL_USAGE_PCT
            if position_value > max_position_value:
                total_qty = int(max_position_value / entry_price)

            if total_qty <= 1:
                continue

            # Split into scalp and runner
            scalp_qty = max(1, int(total_qty * SCALP_BRACKET_PCT))
            runner_qty = total_qty - scalp_qty

            if runner_qty <= 0:
                runner_qty = 1
                scalp_qty = total_qty - 1

            # Create position
            pos = BracketPosition(
                symbol=symbol,
                entry_time=ts,
                entry_price=entry_price,
                stop_price=stop_price,
                risk_per_share=risk_per_share,
                scalp_qty=scalp_qty,
                runner_qty=runner_qty,
                total_qty=total_qty,
                scalp_tp_price=scalp_tp,
                runner_tp_price=runner_tp,
            )

            self.positions[symbol] = pos

            # Only enter one position per scan cycle
            break

    def _record_exit(self, pos: BracketPosition, ts: datetime, exit_price: float,
                     bracket_type: str, reason: ExitReason):
        """Record bracket exit and update position."""
        if bracket_type == "scalp" and pos.scalp_active:
            pos.scalp_active = False
            pos.scalp_exit_price = exit_price
            pos.scalp_exit_reason = reason
            pos.scalp_exit_time = ts
            qty = pos.scalp_qty
        elif bracket_type == "runner" and pos.runner_active:
            pos.runner_active = False
            pos.runner_exit_price = exit_price
            pos.runner_exit_reason = reason
            pos.runner_exit_time = ts
            qty = pos.runner_qty
        else:
            return

        # Calculate P&L
        pnl = (exit_price - pos.entry_price) * qty
        r_mult = (exit_price - pos.entry_price) / pos.risk_per_share
        hold_mins = int((ts - pos.entry_time).total_seconds() / 60)

        # Record trade
        trade = BacktestTrade(
            symbol=pos.symbol,
            entry_time=pos.entry_time,
            exit_time=ts,
            entry_price=pos.entry_price,
            exit_price=exit_price,
            qty=qty,
            side="LONG",
            bracket_type=bracket_type,
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

        # Clean up if position fully closed
        if pos.is_closed:
            pos.calculate_pnl()

    def _close_all_positions(self, ts: datetime, all_data: Dict[str, pd.DataFrame], reason: ExitReason):
        """Close all open positions at current price."""
        for symbol, pos in list(self.positions.items()):
            df = all_data.get(symbol)
            if df is None:
                continue

            current_bars = df[df["timestamp"] == ts]
            if current_bars.empty:
                continue

            close_price = current_bars.iloc[0]["close"]

            if pos.scalp_active:
                self._record_exit(pos, ts, close_price, "scalp", reason)
            if pos.runner_active:
                self._record_exit(pos, ts, close_price, "runner", reason)

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

        # Bracket breakdown
        scalp_trades = [t for t in self.trades if t.bracket_type == "scalp"]
        runner_trades = [t for t in self.trades if t.bracket_type == "runner"]

        result.scalp_trades = len(scalp_trades)
        result.scalp_winners = sum(1 for t in scalp_trades if t.pnl > 0)
        result.scalp_win_rate = result.scalp_winners / result.scalp_trades * 100 if scalp_trades else 0
        result.scalp_pnl = sum(t.pnl for t in scalp_trades)

        result.runner_trades = len(runner_trades)
        result.runner_winners = sum(1 for t in runner_trades if t.pnl > 0)
        result.runner_win_rate = result.runner_winners / result.runner_trades * 100 if runner_trades else 0
        result.runner_pnl = sum(t.pnl for t in runner_trades)

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

        print(f"\n[BRACKET BREAKDOWN]")
        print(f"   SCALP (60%): {result.scalp_trades} trades | {result.scalp_win_rate:.1f}% WR | ${result.scalp_pnl:,.2f}")
        print(f"   RUNNER (40%): {result.runner_trades} trades | {result.runner_win_rate:.1f}% WR | ${result.runner_pnl:,.2f}")

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
                "bracket": t.bracket_type,
                "exit_reason": t.exit_reason.value,
                "pnl": round(t.pnl, 2),
                "r_multiple": round(t.r_multiple, 2),
                "hold_minutes": t.hold_minutes
            }
            for t in result.trades
        ])
        trades_df.to_csv("simple_bot_backtest_trades.csv", index=False)
        print("Trade log saved to simple_bot_backtest_trades.csv")


if __name__ == "__main__":
    main()
