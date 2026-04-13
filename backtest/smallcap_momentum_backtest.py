"""
Small Cap Momentum Backtest Framework
=====================================

Comprehensive backtesting for smallcap_momentum_bot.py strategy.

Strategy: Ross Cameron-style small cap momentum breakout
- Pre-market % gainer scanning
- Low float, high RVOL filters
- Half/whole dollar level breakouts
- Micro pullback entries
- Progressive sizing with cushion
- Three strikes rule

Version: 1.0.0
"""

import os
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple, Set
from enum import Enum
from zoneinfo import ZoneInfo
from collections import defaultdict

# ============================================================
# CONFIGURATION
# ============================================================

ET = ZoneInfo("America/New_York")
POLYGON_API_KEY = os.getenv("POLYGON_API_KEY", "eskzZ5O4QQyYpS5TsA3pe5WMeepeaxmW")

# --- Backtest Settings ---
BACKTEST_DAYS = 180  # v2: 6 months for more data
INITIAL_CAPITAL = 35_000

# --- Universe Filters (v2: relaxed to get more signals) ---
MIN_PRICE = 2.00              # v2: Higher min (avoid penny stocks)
MAX_PRICE = 25.00             # v2: Slightly higher max
SWEET_SPOT_MIN = 4.00
SWEET_SPOT_MAX = 15.00
MIN_PCT_CHANGE = 5.0          # v2: Lower to 5% gap (more signals)
MIN_RELATIVE_VOLUME = 2.0     # v2: Lower to 2x RVOL (more signals)
MIN_ABSOLUTE_VOLUME = 200_000 # v2: Lower to 200k (more signals)

# --- Session Timing ---
TRADING_START_MINUTES = 10     # v2: Wait 10 mins for volatility to settle
TRADING_END_HOUR = 11          # Stop new entries at 11 AM
EOD_CLOSE_HOUR = 15
EOD_CLOSE_MINUTE = 55

# --- Pattern Detection (v4: BULL FLAG ONLY - proven 63% WR) ---
MIN_IMPULSE_PCT = 3.0          # v4: 3% impulse (strong signal)
MIN_IMPULSE_BARS = 3           # v4: 3 bars minimum
MICRO_PULLBACK_MAX_BARS = 4    # Not used
MICRO_PULLBACK_MAX_DEPTH = 2.5 # Not used
LEVEL_PROXIMITY_PCT = 0.3      # Not used

# --- Bull Flag Parameters (v4: STRICT - quality over quantity) ---
FLAG_POLE_MIN_PCT = 6.0        # v4: Minimum 6% pole (very strong moves only)
FLAG_MAX_RETRACE_PCT = 30.0    # v4: Max 30% retracement (very tight flag)
FLAG_MIN_BARS = 3              # Min bars in flag
FLAG_MAX_BARS = 7              # v4: Max 7 bars (fresh flags only)

# --- ATR-Based Stop System (v4: wider stops for volatility) ---
USE_ATR_STOPS = True
ATR_PERIOD = 14
ATR_STOP_MULT = 3.0            # v4: 3x ATR stop distance (wider)
MIN_STOP_DISTANCE_PCT = 2.5    # v4: At least 2.5% stop distance

# --- Risk Management (v4: single position focus) ---
MAX_RISK_PER_TRADE = 150.00    # v4: $150 risk per trade
MAX_DAILY_LOSS = 300.00        # $300 daily max loss
INITIAL_SIZE_FACTOR = 1.00     # Full size
FULL_SIZE_CUSHION = 50.00      # Not used
THREE_STRIKES_ENABLED = False  # Disabled
MAX_POSITIONS = 1              # Single position focus

# --- Exit Parameters (v4: 1.5:1 R/R - optimized for bull flags) ---
TARGET_R = 1.5                 # v4: 1.5:1 R/R (bull flags can run)
USE_PARTIAL_EXITS = False      # No partials
PARTIAL_R = 0.75               # Not used
PARTIAL_PCT = 0.50             # Not used
BREAKEVEN_R = 0.75             # v4: Move to BE at 0.75R
USE_TRAILING = True
TRAIL_ACTIVATION_R = 0.75      # v4: Activate trail at 0.75R
TRAIL_DISTANCE_PCT = 1.25      # v4: Trail 1.25% behind

# --- Pattern Weights (v4: ONLY BULL FLAGS) ---
PATTERN_WEIGHTS = {
    "MICRO_PULLBACK": 0.0,     # v4: DISABLED
    "BULL_FLAG": 2.0,          # v4: ONLY bull flags
    "LEVEL_BREAKOUT": 0.0,     # v4: DISABLED
}


# ============================================================
# DATA STRUCTURES
# ============================================================

class PatternType(Enum):
    MICRO_PULLBACK = "MICRO_PULLBACK"
    BULL_FLAG = "BULL_FLAG"
    LEVEL_BREAKOUT = "LEVEL_BREAKOUT"


class ExitReason(Enum):
    STOP_LOSS = "STOP_LOSS"
    TAKE_PROFIT = "TAKE_PROFIT"
    PARTIAL_EXIT = "PARTIAL_EXIT"
    TRAILING_STOP = "TRAILING_STOP"
    BREAKEVEN = "BREAKEVEN"
    EOD_CLOSE = "EOD_CLOSE"


@dataclass
class DailyGainer:
    """A stock that gapped up on a specific day."""
    symbol: str
    date: datetime
    gap_pct: float
    open_price: float
    prev_close: float
    volume: int
    rel_volume: float


@dataclass
class TradeSetup:
    """Detected trade setup."""
    symbol: str
    timestamp: datetime
    pattern: PatternType
    entry_price: float
    stop_price: float
    target_price: float
    risk_per_share: float
    breakout_level: float
    confidence: float


@dataclass
class Position:
    """Open position during backtest."""
    symbol: str
    entry_time: datetime
    entry_price: float
    stop_price: float
    target_price: float
    risk_per_share: float
    total_qty: int
    remaining_qty: int
    pattern: PatternType

    highest_price: float = 0.0
    trail_active: bool = False
    trail_stop: float = 0.0
    be_active: bool = False
    partial_taken: bool = False


@dataclass
class BacktestTrade:
    """Completed trade record."""
    symbol: str
    date: str
    entry_time: datetime
    exit_time: datetime
    entry_price: float
    exit_price: float
    qty: int
    pattern: str
    exit_reason: str
    pnl: float
    r_multiple: float
    hold_minutes: int
    gap_pct: float  # Original gap %


@dataclass
class DailyResult:
    """Daily backtest results."""
    date: str
    trades: int = 0
    winners: int = 0
    losers: int = 0
    gross_pnl: float = 0.0
    consecutive_losses: int = 0
    halted: bool = False
    size_factor: float = 0.25


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

    # By pattern
    pattern_stats: Dict[str, dict] = field(default_factory=dict)

    # By price bucket
    price_bucket_stats: Dict[str, dict] = field(default_factory=dict)

    # By gap size
    gap_bucket_stats: Dict[str, dict] = field(default_factory=dict)

    trades: List[BacktestTrade] = field(default_factory=list)
    daily_results: List[DailyResult] = field(default_factory=list)


# ============================================================
# DATA FETCHING
# ============================================================

def fetch_daily_gainers_historical(days: int = 90) -> Dict[str, List[DailyGainer]]:
    """
    Simulate pre-market scanning by finding stocks that gapped up each day.
    Returns dict of date -> list of gainers.
    """
    print("Building historical gainer database...")

    # Get a universe of potentially volatile small caps
    # Expanded v2: More stocks for better data coverage
    SCAN_UNIVERSE = [
        # Cannabis sector (high volatility)
        "SNDL", "TLRY", "ACB", "CGC", "CRON", "HEXO", "OGI", "VFF",
        # EVs and clean energy
        "WKHS", "GOEV", "NKLA", "LCID", "RIVN", "BLNK", "CHPT", "QS",
        "EVGO", "PTRA", "HYLN", "XPEV", "NIO", "LI",
        # Crypto/blockchain
        "MARA", "RIOT", "BITF", "HUT", "CLSK", "CIFR", "BTBT", "CAN",
        # Space
        "SPCE", "RKLB", "LUNR", "RDW", "MNTS", "ASTS", "GSAT",
        # Meme/retail favorites
        "AMC", "GME", "BB", "CLOV", "SOFI", "HOOD",
        # AI/Tech small caps
        "PLTR", "AI", "IONQ", "RGTI", "QUBT", "SOUN", "BBAI",
        # Biotech small caps
        "DNA", "CRSP", "BEAM", "EDIT", "NTLA", "ARCT", "MRNA",
        "BNTX", "NVAX", "VXRT", "INO", "OCGN",
        # High beta small caps
        "RBLX", "AFRM", "UPST", "PATH", "DOCS", "DKNG", "PENN",
        "SKLZ", "FUBO", "OPEN", "WISH", "SDC", "BARK",
        # Mining/commodities
        "VALE", "CLF", "X", "AA", "FCX", "GOLD", "NEM",
        # Recent IPOs/SPACs
        "DWAC", "PHUN", "BKKT", "GRAB", "PSFE",
        # Additional volatility plays
        "CENN", "IMPP", "BBIG", "PROG", "ATER", "GREE",
        "IRNT", "OPAD", "TMC", "LIDR", "MLGO",
        # Energy
        "TELL", "ET", "OXY", "MRO", "DVN", "FANG",
    ]

    gainers_by_date: Dict[str, List[DailyGainer]] = defaultdict(list)
    end_date = datetime.now(ET).date()
    start_date = end_date - timedelta(days=days + 10)

    for symbol in SCAN_UNIVERSE:
        print(f"  Scanning {symbol}...", end=" ")

        try:
            # Fetch daily bars
            url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/day/{start_date}/{end_date}"
            params = {"adjusted": "true", "sort": "asc", "limit": 200, "apiKey": POLYGON_API_KEY}

            resp = requests.get(url, params=params, timeout=15)
            if resp.status_code != 200:
                print("SKIP")
                continue

            results = resp.json().get("results", [])
            if len(results) < 25:
                print("SKIP (insufficient data)")
                continue

            df = pd.DataFrame(results)
            df["date"] = pd.to_datetime(df["t"], unit="ms", utc=True).dt.tz_convert(ET).dt.date
            df = df.rename(columns={"o": "open", "h": "high", "l": "low", "c": "close", "v": "volume"})

            # Calculate 20-day avg volume
            df["avg_vol_20"] = df["volume"].rolling(20).mean()

            # Calculate gap %
            df["prev_close"] = df["close"].shift(1)
            df["gap_pct"] = ((df["open"] - df["prev_close"]) / df["prev_close"]) * 100
            df["rel_vol"] = df["volume"] / df["avg_vol_20"]

            # Find gap-up days that meet criteria
            gap_days = df[
                (df["gap_pct"] >= MIN_PCT_CHANGE) &
                (df["open"] >= MIN_PRICE) &
                (df["open"] <= MAX_PRICE) &
                (df["volume"] >= MIN_ABSOLUTE_VOLUME) &
                (df["rel_vol"] >= MIN_RELATIVE_VOLUME)
            ]

            if len(gap_days) > 0:
                print(f"OK ({len(gap_days)} gap days)")

                for _, row in gap_days.iterrows():
                    gainer = DailyGainer(
                        symbol=symbol,
                        date=row["date"],
                        gap_pct=row["gap_pct"],
                        open_price=row["open"],
                        prev_close=row["prev_close"],
                        volume=int(row["volume"]),
                        rel_volume=row["rel_vol"]
                    )
                    date_str = str(row["date"])
                    gainers_by_date[date_str].append(gainer)
            else:
                print("OK (no gaps)")

        except Exception as e:
            print(f"ERROR: {e}")
            continue

    # Sort gainers by gap % on each day
    for date_str in gainers_by_date:
        gainers_by_date[date_str].sort(key=lambda x: x.gap_pct, reverse=True)

    total_gap_days = sum(len(v) for v in gainers_by_date.values())
    print(f"\nFound {total_gap_days} gap-up events across {len(gainers_by_date)} trading days")

    return dict(gainers_by_date)


def fetch_minute_bars(symbol: str, date: datetime) -> Optional[pd.DataFrame]:
    """Fetch minute bars for a specific date."""
    date_str = date.strftime("%Y-%m-%d")

    url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/minute/{date_str}/{date_str}"
    params = {"adjusted": "true", "sort": "asc", "limit": 50000, "apiKey": POLYGON_API_KEY}

    try:
        resp = requests.get(url, params=params, timeout=15)
        if resp.status_code != 200:
            return None

        results = resp.json().get("results", [])
        if not results:
            return None

        df = pd.DataFrame(results)
        df["timestamp"] = pd.to_datetime(df["t"], unit="ms", utc=True).dt.tz_convert(ET)
        df = df.rename(columns={"o": "open", "h": "high", "l": "low", "c": "close", "v": "volume"})
        df = df[["timestamp", "open", "high", "low", "close", "volume"]]

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

    except:
        return None


# ============================================================
# INDICATORS
# ============================================================

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


# ============================================================
# PATTERN DETECTION
# ============================================================

def get_price_levels(price: float) -> List[float]:
    """Get relevant half/whole dollar levels near price."""
    levels = []
    base = int(price)

    for offset in [-1.0, -0.5, 0, 0.5, 1.0, 1.5, 2.0, 2.5, 3.0]:
        level = base + offset
        if level > 0:
            levels.append(level)

    return sorted(levels)


def detect_micro_pullback(df: pd.DataFrame, idx: int, gainer: DailyGainer) -> Optional[TradeSetup]:
    """
    Detect micro pullback pattern at half/whole dollar level.

    Pattern:
    1. Impulse move up (2+ green bars, 1.5%+ move)
    2. Price near half/whole dollar level
    3. Brief pullback (1-5 bars) without breaking structure
    4. Entry on break above pullback high
    """
    if idx < 10:
        return None

    ts = df["timestamp"].iloc[idx]
    current_price = df["close"].iloc[idx]

    # Get recent price action
    lookback = df.iloc[max(0, idx-15):idx+1]

    # Find impulse move
    impulse_start_idx = None
    impulse_low = None

    for i in range(len(lookback) - 5, 0, -1):
        # Check for sequence of higher prices
        segment = lookback.iloc[i:i+5]
        if len(segment) < 3:
            continue

        # Count green bars
        green_count = sum(1 for j in range(len(segment))
                        if segment["close"].iloc[j] > segment["open"].iloc[j])

        if green_count >= MIN_IMPULSE_BARS:
            move_pct = ((segment["high"].max() - segment["low"].min()) /
                       segment["low"].min()) * 100

            if move_pct >= MIN_IMPULSE_PCT:
                impulse_start_idx = i
                impulse_low = segment["low"].min()
                break

    if impulse_start_idx is None:
        return None

    impulse_high = lookback.iloc[impulse_start_idx:]["high"].max()

    # Check for pullback
    pullback_bars = lookback.iloc[-MICRO_PULLBACK_MAX_BARS:]
    pullback_low = pullback_bars["low"].min()
    pullback_high = pullback_bars["high"].max()

    # Pullback depth check
    pullback_depth = ((impulse_high - pullback_low) / impulse_high) * 100
    if pullback_depth > MICRO_PULLBACK_MAX_DEPTH:
        return None

    # Find nearest level above current price
    levels = get_price_levels(current_price)
    target_level = None

    for level in levels:
        distance_pct = abs(level - current_price) / current_price * 100
        if level >= current_price * 0.99 and distance_pct < 2.0:
            target_level = level
            break

    if target_level is None:
        return None

    # Calculate trade levels
    entry_price = max(pullback_high + 0.02, target_level + 0.01)
    stop_price = pullback_low - 0.02
    risk_per_share = entry_price - stop_price

    if risk_per_share <= 0 or risk_per_share > entry_price * 0.05:
        return None  # Risk too small or too large

    target_price = entry_price + (risk_per_share * TARGET_R)

    # Confidence scoring
    confidence = 0.5

    # Gap size bonus
    if gainer.gap_pct > 15:
        confidence += 0.15
    elif gainer.gap_pct > 10:
        confidence += 0.10

    # RVOL bonus
    if gainer.rel_volume > 8:
        confidence += 0.15
    elif gainer.rel_volume > 5:
        confidence += 0.10

    # Price sweet spot bonus
    if SWEET_SPOT_MIN <= current_price <= SWEET_SPOT_MAX:
        confidence += 0.10

    # Tight pullback bonus
    if pullback_depth < 1.0:
        confidence += 0.10

    return TradeSetup(
        symbol=gainer.symbol,
        timestamp=ts,
        pattern=PatternType.MICRO_PULLBACK,
        entry_price=entry_price,
        stop_price=stop_price,
        target_price=target_price,
        risk_per_share=risk_per_share,
        breakout_level=target_level,
        confidence=min(confidence, 1.0)
    )


def detect_bull_flag(df: pd.DataFrame, idx: int, gainer: DailyGainer) -> Optional[TradeSetup]:
    """
    Detect bull flag pattern.

    Pattern:
    1. Strong impulse leg up (pole) - 3%+ move
    2. Consolidation (flag) - max 50% retracement
    3. 3-10 bars in flag
    4. Entry on break above flag high
    """
    if idx < 15:
        return None

    ts = df["timestamp"].iloc[idx]

    # Look for pole in last 20 bars
    lookback = df.iloc[max(0, idx-20):idx+1]

    # Find pole (biggest impulse)
    best_pole = None
    best_pole_move = 0

    for i in range(len(lookback) - 10):
        segment = lookback.iloc[i:i+8]
        pole_low = segment["low"].min()
        pole_high = segment["high"].max()
        pole_move = ((pole_high - pole_low) / pole_low) * 100

        if pole_move > best_pole_move and pole_move >= FLAG_POLE_MIN_PCT:
            best_pole = (i, pole_low, pole_high, segment["high"].idxmax())
            best_pole_move = pole_move

    if best_pole is None:
        return None

    pole_start_idx, pole_low, pole_high, pole_high_idx = best_pole

    # Find flag after pole
    pole_end_loc = lookback.index.get_loc(pole_high_idx)
    flag_bars = lookback.iloc[pole_end_loc:]

    if len(flag_bars) < FLAG_MIN_BARS or len(flag_bars) > FLAG_MAX_BARS:
        return None

    flag_high = flag_bars["high"].max()
    flag_low = flag_bars["low"].min()

    # Check retracement
    pole_range = pole_high - pole_low
    retracement = (pole_high - flag_low) / pole_range * 100

    if retracement > FLAG_MAX_RETRACE_PCT:
        return None

    # Flag should be consolidating (not making new highs)
    if flag_high > pole_high * 1.01:
        return None

    # Calculate entry price
    entry_price = flag_high + 0.02

    # v3: Use ATR-based stops for better volatility handling
    if USE_ATR_STOPS and "atr" in df.columns:
        atr = df.iloc[idx]["atr"] if idx < len(df) else df["atr"].iloc[-1]
        if pd.notna(atr) and atr > 0:
            atr_stop_distance = atr * ATR_STOP_MULT
            min_stop_distance = entry_price * (MIN_STOP_DISTANCE_PCT / 100)
            stop_distance = max(atr_stop_distance, min_stop_distance)
            stop_price = entry_price - stop_distance
        else:
            stop_price = flag_low - 0.02
    else:
        stop_price = flag_low - 0.02

    risk_per_share = entry_price - stop_price

    if risk_per_share <= 0 or risk_per_share > entry_price * 0.10:  # v3: Allow up to 10% risk
        return None

    target_price = entry_price + (risk_per_share * TARGET_R)

    # Confidence
    confidence = 0.55  # Flags are reliable

    if gainer.gap_pct > 12:
        confidence += 0.10
    if gainer.rel_volume > 6:
        confidence += 0.10
    if retracement < 30:
        confidence += 0.10
    if best_pole_move > 5:
        confidence += 0.10

    return TradeSetup(
        symbol=gainer.symbol,
        timestamp=ts,
        pattern=PatternType.BULL_FLAG,
        entry_price=entry_price,
        stop_price=stop_price,
        target_price=target_price,
        risk_per_share=risk_per_share,
        breakout_level=flag_high,
        confidence=min(confidence, 1.0)
    )


def detect_level_breakout(df: pd.DataFrame, idx: int, gainer: DailyGainer) -> Optional[TradeSetup]:
    """
    Simple breakout above a key level without specific pattern.
    Used when price consolidates at a level and breaks out.
    """
    if idx < 10:
        return None

    ts = df["timestamp"].iloc[idx]
    current_price = df["close"].iloc[idx]
    current_high = df["high"].iloc[idx]

    # Get levels
    levels = get_price_levels(current_price)

    # Check if we just broke above a level
    for level in levels:
        if level < current_price:
            continue

        distance_pct = (level - current_price) / current_price * 100
        if distance_pct > LEVEL_PROXIMITY_PCT:
            continue

        # Check if this bar broke the level
        if df["open"].iloc[idx] < level and current_high >= level:
            # Valid level break
            stop_price = df["low"].iloc[idx] - 0.02
            entry_price = level + 0.01
            risk_per_share = entry_price - stop_price

            if risk_per_share <= 0 or risk_per_share > entry_price * 0.04:
                continue

            target_price = entry_price + (risk_per_share * TARGET_R)

            confidence = 0.45
            if gainer.gap_pct > 12:
                confidence += 0.10
            if gainer.rel_volume > 6:
                confidence += 0.10

            return TradeSetup(
                symbol=gainer.symbol,
                timestamp=ts,
                pattern=PatternType.LEVEL_BREAKOUT,
                entry_price=entry_price,
                stop_price=stop_price,
                target_price=target_price,
                risk_per_share=risk_per_share,
                breakout_level=level,
                confidence=min(confidence, 1.0)
            )

    return None


# ============================================================
# BACKTEST ENGINE
# ============================================================

class BacktestEngine:
    """Run backtest for small cap momentum strategy."""

    def __init__(self):
        self.capital = INITIAL_CAPITAL
        self.positions: Dict[str, Position] = {}
        self.trades: List[BacktestTrade] = []
        self.equity_curve: List[float] = [INITIAL_CAPITAL]
        self.daily_results: List[DailyResult] = []

    def run_backtest(self, gainers_by_date: Dict[str, List[DailyGainer]]) -> BacktestResult:
        """Run backtest across all days with gap-up stocks."""
        print(f"\n{'='*60}")
        print("SMALL CAP MOMENTUM BACKTEST")
        print(f"{'='*60}")
        print(f"Period: {len(gainers_by_date)} trading days")
        print(f"Initial Capital: ${INITIAL_CAPITAL:,.0f}")
        print(f"Max Risk/Trade: ${MAX_RISK_PER_TRADE}")
        print(f"{'='*60}\n")

        sorted_dates = sorted(gainers_by_date.keys())

        for date_str in sorted_dates:
            gainers = gainers_by_date[date_str]
            if not gainers:
                continue

            # Process this day
            self._process_day(date_str, gainers)

        # Calculate results
        result = self._calculate_results()
        self._print_results(result)

        return result

    def _process_day(self, date_str: str, gainers: List[DailyGainer]):
        """Process a single trading day."""
        daily = DailyResult(date=date_str)

        # Reset positions for new day
        self.positions = {}

        # Sort gainers by gap % (top gainers first)
        gainers = sorted(gainers, key=lambda x: x.gap_pct, reverse=True)[:5]  # Top 5 only

        print(f"\n{date_str}: Processing {len(gainers)} gainers...")

        # Fetch minute data for each gainer
        minute_data: Dict[str, pd.DataFrame] = {}
        for gainer in gainers:
            df = fetch_minute_bars(gainer.symbol, gainer.date)
            if df is not None and len(df) > 30:
                # v3: Calculate ATR for stops
                if USE_ATR_STOPS:
                    df["atr"] = calculate_atr(df, ATR_PERIOD)
                minute_data[gainer.symbol] = df
                print(f"  {gainer.symbol}: +{gainer.gap_pct:.1f}% gap, RVOL={gainer.rel_volume:.1f}x")

        if not minute_data:
            return

        # Get all timestamps
        all_timestamps = set()
        for df in minute_data.values():
            all_timestamps.update(df["timestamp"].tolist())
        all_timestamps = sorted(all_timestamps)

        # Process bar by bar
        for ts in all_timestamps:
            hour, minute = ts.hour, ts.minute

            # Skip first few minutes
            if hour == 9 and minute < 30 + TRADING_START_MINUTES:
                continue

            # Manage existing positions
            self._manage_positions(ts, minute_data, daily)

            # EOD close
            if hour >= EOD_CLOSE_HOUR and minute >= EOD_CLOSE_MINUTE:
                self._close_all(ts, minute_data, daily, ExitReason.EOD_CLOSE)
                continue

            # No new entries after trading end or if halted
            if hour >= TRADING_END_HOUR or daily.halted:
                continue

            # Check 3 strikes
            if THREE_STRIKES_ENABLED and daily.consecutive_losses >= 3 and daily.trades <= 3:
                daily.halted = True
                print(f"    THREE STRIKES: Trading halted for the day")
                continue

            # Check daily loss limit
            if daily.gross_pnl <= -MAX_DAILY_LOSS:
                daily.halted = True
                print(f"    DAILY LOSS LIMIT: Trading halted")
                continue

            # Scan for new entries
            if len(self.positions) < MAX_POSITIONS:
                self._scan_for_entries(ts, minute_data, gainers, daily)

        # Close any remaining positions
        if self.positions:
            final_ts = all_timestamps[-1] if all_timestamps else datetime.now(ET)
            self._close_all(final_ts, minute_data, daily, ExitReason.EOD_CLOSE)

        self.daily_results.append(daily)
        print(f"  Day result: {daily.trades} trades, ${daily.gross_pnl:.2f} P&L")

    def _manage_positions(self, ts: datetime, minute_data: Dict[str, pd.DataFrame], daily: DailyResult):
        """Manage open positions."""
        positions_to_remove = []

        for symbol, pos in self.positions.items():
            df = minute_data.get(symbol)
            if df is None:
                continue

            current_bars = df[df["timestamp"] == ts]
            if current_bars.empty:
                continue

            row = current_bars.iloc[0]
            high = row["high"]
            low = row["low"]
            close = row["close"]

            # Update highest
            if high > pos.highest_price:
                pos.highest_price = high

            # Check stop loss
            if low <= pos.stop_price:
                exit_price = pos.stop_price
                reason = ExitReason.BREAKEVEN if pos.be_active else ExitReason.STOP_LOSS
                self._record_exit(pos, ts, exit_price, reason, daily)
                positions_to_remove.append(symbol)
                continue

            # Check full target
            if high >= pos.target_price and not pos.partial_taken:
                self._record_exit(pos, ts, pos.target_price, ExitReason.TAKE_PROFIT, daily)
                positions_to_remove.append(symbol)
                continue

            # Check partial exit
            if USE_PARTIAL_EXITS and not pos.partial_taken:
                partial_target = pos.entry_price + (pos.risk_per_share * PARTIAL_R)
                if high >= partial_target:
                    self._take_partial(pos, partial_target, ts, daily)

            # Check breakeven activation
            if not pos.be_active:
                be_price = pos.entry_price + (pos.risk_per_share * BREAKEVEN_R)
                if pos.highest_price >= be_price:
                    pos.be_active = True
                    pos.stop_price = pos.entry_price + 0.01

            # Check trailing stop
            if USE_TRAILING and not pos.trail_active:
                trail_trigger = pos.entry_price + (pos.risk_per_share * TRAIL_ACTIVATION_R)
                if pos.highest_price >= trail_trigger:
                    pos.trail_active = True
                    pos.trail_stop = pos.highest_price * (1 - TRAIL_DISTANCE_PCT / 100)

            if pos.trail_active:
                new_trail = pos.highest_price * (1 - TRAIL_DISTANCE_PCT / 100)
                if new_trail > pos.trail_stop:
                    pos.trail_stop = new_trail

                if low <= pos.trail_stop:
                    self._record_exit(pos, ts, pos.trail_stop, ExitReason.TRAILING_STOP, daily)
                    positions_to_remove.append(symbol)

        for symbol in positions_to_remove:
            if symbol in self.positions:
                del self.positions[symbol]

    def _scan_for_entries(self, ts: datetime, minute_data: Dict[str, pd.DataFrame],
                         gainers: List[DailyGainer], daily: DailyResult):
        """Scan for new entry setups."""
        for gainer in gainers:
            if gainer.symbol in self.positions:
                continue

            df = minute_data.get(gainer.symbol)
            if df is None:
                continue

            current_bars = df[df["timestamp"] == ts]
            if current_bars.empty:
                continue

            idx = current_bars.index[0]

            # Try each pattern
            setups = []

            setup = detect_micro_pullback(df, idx, gainer)
            if setup:
                setups.append(setup)

            setup = detect_bull_flag(df, idx, gainer)
            if setup:
                setups.append(setup)

            setup = detect_level_breakout(df, idx, gainer)
            if setup:
                setups.append(setup)

            # Take best setup
            if setups:
                best = max(setups, key=lambda x: x.confidence * PATTERN_WEIGHTS.get(x.pattern.value, 1.0))

                if best.confidence >= 0.55:
                    self._open_position(best, gainer, daily)
                    break  # One entry per scan

    def _open_position(self, setup: TradeSetup, gainer: DailyGainer, daily: DailyResult):
        """Open a new position."""
        # Calculate size
        size_factor = daily.size_factor
        risk_dollars = MAX_RISK_PER_TRADE * size_factor
        qty = int(risk_dollars / setup.risk_per_share)

        if qty <= 0:
            return

        # Check capital
        position_value = qty * setup.entry_price
        if position_value > self.capital * 0.5:  # Max 50% per position
            qty = int((self.capital * 0.5) / setup.entry_price)

        if qty <= 0:
            return

        pos = Position(
            symbol=setup.symbol,
            entry_time=setup.timestamp,
            entry_price=setup.entry_price,
            stop_price=setup.stop_price,
            target_price=setup.target_price,
            risk_per_share=setup.risk_per_share,
            total_qty=qty,
            remaining_qty=qty,
            pattern=setup.pattern,
            highest_price=setup.entry_price
        )

        self.positions[setup.symbol] = pos

        print(f"    ENTRY: {setup.symbol} | {setup.pattern.value} | {qty} @ ${setup.entry_price:.2f} | "
              f"Stop=${setup.stop_price:.2f} | Target=${setup.target_price:.2f}")

    def _take_partial(self, pos: Position, exit_price: float, ts: datetime, daily: DailyResult):
        """Take partial profits."""
        partial_qty = int(pos.remaining_qty * PARTIAL_PCT)
        if partial_qty <= 0:
            return

        pnl = (exit_price - pos.entry_price) * partial_qty
        r_mult = (exit_price - pos.entry_price) / pos.risk_per_share

        pos.remaining_qty -= partial_qty
        pos.partial_taken = True

        daily.gross_pnl += pnl
        self.capital += pnl
        self.equity_curve.append(self.capital)

        # Record partial trade
        trade = BacktestTrade(
            symbol=pos.symbol,
            date=ts.strftime("%Y-%m-%d"),
            entry_time=pos.entry_time,
            exit_time=ts,
            entry_price=pos.entry_price,
            exit_price=exit_price,
            qty=partial_qty,
            pattern=pos.pattern.value,
            exit_reason=ExitReason.PARTIAL_EXIT.value,
            pnl=pnl,
            r_multiple=r_mult,
            hold_minutes=int((ts - pos.entry_time).total_seconds() / 60),
            gap_pct=0  # Not tracked per-trade
        )
        self.trades.append(trade)

        print(f"    PARTIAL: {pos.symbol} | {partial_qty} @ ${exit_price:.2f} | P&L=${pnl:.2f}")

    def _record_exit(self, pos: Position, ts: datetime, exit_price: float,
                    reason: ExitReason, daily: DailyResult):
        """Record full position exit."""
        pnl = (exit_price - pos.entry_price) * pos.remaining_qty
        r_mult = (exit_price - pos.entry_price) / pos.risk_per_share

        daily.gross_pnl += pnl
        daily.trades += 1

        if pnl > 0:
            daily.winners += 1
            daily.consecutive_losses = 0
        else:
            daily.losers += 1
            daily.consecutive_losses += 1

        # Update size factor
        if daily.gross_pnl >= FULL_SIZE_CUSHION:
            daily.size_factor = 1.0
        else:
            daily.size_factor = INITIAL_SIZE_FACTOR

        self.capital += pnl
        self.equity_curve.append(self.capital)

        trade = BacktestTrade(
            symbol=pos.symbol,
            date=ts.strftime("%Y-%m-%d"),
            entry_time=pos.entry_time,
            exit_time=ts,
            entry_price=pos.entry_price,
            exit_price=exit_price,
            qty=pos.remaining_qty,
            pattern=pos.pattern.value,
            exit_reason=reason.value,
            pnl=pnl,
            r_multiple=r_mult,
            hold_minutes=int((ts - pos.entry_time).total_seconds() / 60),
            gap_pct=0
        )
        self.trades.append(trade)

        print(f"    EXIT: {pos.symbol} | {reason.value} | {pos.remaining_qty} @ ${exit_price:.2f} | "
              f"P&L=${pnl:.2f} ({r_mult:.2f}R)")

    def _close_all(self, ts: datetime, minute_data: Dict[str, pd.DataFrame],
                  daily: DailyResult, reason: ExitReason):
        """Close all open positions."""
        for symbol, pos in list(self.positions.items()):
            df = minute_data.get(symbol)
            if df is None:
                continue

            current_bars = df[df["timestamp"] <= ts].tail(1)
            if current_bars.empty:
                continue

            exit_price = current_bars.iloc[0]["close"]
            self._record_exit(pos, ts, exit_price, reason, daily)

        self.positions = {}

    def _calculate_results(self) -> BacktestResult:
        """Calculate final results."""
        result = BacktestResult()
        result.trades = self.trades
        result.daily_results = self.daily_results
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

        # Stats by pattern
        for pattern in PatternType:
            pattern_trades = [t for t in self.trades if t.pattern == pattern.value]
            if pattern_trades:
                wins = sum(1 for t in pattern_trades if t.pnl > 0)
                pnl = sum(t.pnl for t in pattern_trades)
                result.pattern_stats[pattern.value] = {
                    "trades": len(pattern_trades),
                    "wins": wins,
                    "win_rate": wins / len(pattern_trades) * 100,
                    "pnl": pnl,
                    "avg_r": sum(t.r_multiple for t in pattern_trades) / len(pattern_trades)
                }

        # Stats by exit reason
        exit_stats = {}
        for reason in ExitReason:
            reason_trades = [t for t in self.trades if t.exit_reason == reason.value]
            if reason_trades:
                wins = sum(1 for t in reason_trades if t.pnl > 0)
                pnl = sum(t.pnl for t in reason_trades)
                exit_stats[reason.value] = {
                    "trades": len(reason_trades),
                    "wins": wins,
                    "win_rate": wins / len(reason_trades) * 100,
                    "pnl": pnl
                }

        return result

    def _print_results(self, result: BacktestResult):
        """Print formatted results."""
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

        print(f"\n[PATTERN BREAKDOWN]")
        for pattern, stats in result.pattern_stats.items():
            print(f"   {pattern}: {stats['trades']} trades | "
                  f"{stats['win_rate']:.1f}% WR | ${stats['pnl']:.2f} | "
                  f"{stats['avg_r']:.2f}R avg")

        print(f"\n[EXIT REASONS]")
        exit_stats = {}
        for t in result.trades:
            reason = t.exit_reason
            if reason not in exit_stats:
                exit_stats[reason] = {"count": 0, "pnl": 0, "wins": 0}
            exit_stats[reason]["count"] += 1
            exit_stats[reason]["pnl"] += t.pnl
            if t.pnl > 0:
                exit_stats[reason]["wins"] += 1

        for reason, stats in sorted(exit_stats.items(), key=lambda x: -x[1]["count"]):
            wr = stats["wins"] / stats["count"] * 100 if stats["count"] > 0 else 0
            print(f"   {reason}: {stats['count']} ({stats['count']/result.total_trades*100:.1f}%) | "
                  f"WR={wr:.1f}% | P&L=${stats['pnl']:.2f}")

        print(f"\n[FINAL CAPITAL]: ${self.capital:,.2f}")
        print(f"   Return: {(self.capital - INITIAL_CAPITAL) / INITIAL_CAPITAL * 100:.1f}%")
        print(f"{'='*60}\n")


# ============================================================
# MAIN
# ============================================================

def main():
    """Run the backtest."""
    # Build historical gainer database
    gainers_by_date = fetch_daily_gainers_historical(BACKTEST_DAYS)

    if not gainers_by_date:
        print("No gap-up data found. Exiting.")
        return

    # Run backtest
    engine = BacktestEngine()
    result = engine.run_backtest(gainers_by_date)

    # Save trade log
    if result.trades:
        trades_df = pd.DataFrame([
            {
                "symbol": t.symbol,
                "date": t.date,
                "entry_time": t.entry_time.isoformat(),
                "exit_time": t.exit_time.isoformat(),
                "entry_price": t.entry_price,
                "exit_price": t.exit_price,
                "qty": t.qty,
                "pattern": t.pattern,
                "exit_reason": t.exit_reason,
                "pnl": round(t.pnl, 2),
                "r_multiple": round(t.r_multiple, 2),
                "hold_minutes": t.hold_minutes
            }
            for t in result.trades
        ])
        trades_df.to_csv("smallcap_momentum_backtest_trades.csv", index=False)
        print("Trade log saved to smallcap_momentum_backtest_trades.csv")


if __name__ == "__main__":
    main()
