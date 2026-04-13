"""
Small Cap Momentum Backtest Framework - V5 ENHANCED
====================================================

KEY IMPROVEMENTS IN V5 (addressing profitability issues):

1. TIERED EXITS (like humans do):
   - TP1 at 0.5R → Lock in 25% of position (quick win)
   - TP2 at 1.0R → Take another 25% (solid profit)
   - Trail remaining 50% for big runners

2. SIGNAL QUALITY GRADING (not binary):
   - A+ setup: Gap >15%, RVOL >8x, pole >8%, tight flag → Full size
   - A setup: Gap >10%, RVOL >5x, strong pattern → 75% size
   - B setup: Meets minimum criteria → 50% size
   - C setup: Weak criteria → SKIP (don't trade)

3. EARLY EXIT LOGIC (thesis invalidation):
   - If price fails to make new high within 5 bars → Consider exit
   - If volume dies after entry → Reduce position or exit
   - If price action becomes choppy → Exit early

4. CHOP DETECTION (avoid false signals):
   - Measure recent volatility vs directional movement
   - Skip setups in choppy/ranging price action

Version: 5.0.0 - Enhanced Human-Like Trading
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
BACKTEST_DAYS = 180
INITIAL_CAPITAL = 35_000

# --- Universe Filters ---
MIN_PRICE = 2.00
MAX_PRICE = 25.00
SWEET_SPOT_MIN = 4.00
SWEET_SPOT_MAX = 15.00
MIN_PCT_CHANGE = 5.0
MIN_RELATIVE_VOLUME = 2.0
MIN_ABSOLUTE_VOLUME = 200_000

# --- Session Timing ---
TRADING_START_MINUTES = 10
TRADING_END_HOUR = 11
EOD_CLOSE_HOUR = 15
EOD_CLOSE_MINUTE = 55

# --- Pattern Detection (Bull Flag Focus - V5: RELAXED for more signals) ---
MIN_IMPULSE_PCT = 2.0          # V5: Lower from 3.0
MIN_IMPULSE_BARS = 2           # V5: Lower from 3
FLAG_POLE_MIN_PCT = 3.0        # V5: Lower from 6.0 (let grading handle quality)
FLAG_MAX_RETRACE_PCT = 50.0    # V5: Allow deeper pullbacks (was 30%)
FLAG_MIN_BARS = 2              # V5: Lower from 3
FLAG_MAX_BARS = 15             # V5: Allow longer consolidations (was 7)

# --- ATR-Based Stop System ---
USE_ATR_STOPS = True
ATR_PERIOD = 14
ATR_STOP_MULT = 3.0
MIN_STOP_DISTANCE_PCT = 2.5

# --- Risk Management ---
MAX_RISK_PER_TRADE = 150.00
MAX_DAILY_LOSS = 300.00
MAX_POSITIONS = 1

# ============================================================
# V5: TIERED EXIT PARAMETERS (THE KEY CHANGE)
# ============================================================

# Tiered exits - like humans scaling out
USE_TIERED_EXITS = True

# V5.1: Let winners run MORE before taking profits
# TP1: Quick lock-in (but not too early)
TP1_R = 0.75          # V5.1: Was 0.5R - now wait for 0.75R before first take
TP1_SIZE_PCT = 0.33   # Take 33% of position (was 25%)

# TP2: Solid profit
TP2_R = 1.5           # V5.1: Was 1.0R - now wait for 1.5R
TP2_SIZE_PCT = 0.33   # Take another 33% (was 25%)

# Remaining 34% uses trailing stop for home runs
TRAIL_ACTIVATION_R = 1.5  # Activate trail after TP2
TRAIL_DISTANCE_PCT = 2.0  # V5.1: Wider trail (was 1.5%) - let it breathe

# Move to breakeven after TP1
MOVE_TO_BE_AFTER_TP1 = True

# ============================================================
# V5: SIGNAL QUALITY GRADING
# ============================================================

class SignalGrade(Enum):
    A_PLUS = "A+"  # Best setups - full size
    A = "A"        # Strong setups - 75% size
    B = "B"        # Average setups - 50% size
    C = "C"        # Weak setups - SKIP

# Grade thresholds (V5: RELAXED to get more signals, let position sizing control risk)
GRADE_A_PLUS_CRITERIA = {
    "min_gap_pct": 12.0,    # Was 15
    "min_rvol": 5.0,        # Was 8
    "min_pole_pct": 5.0,    # Was 8
    "max_retrace_pct": 35.0,  # Was 25
}

GRADE_A_CRITERIA = {
    "min_gap_pct": 8.0,     # Was 10
    "min_rvol": 3.0,        # Was 5
    "min_pole_pct": 4.0,    # Was 6
    "max_retrace_pct": 45.0,  # Was 30
}

GRADE_B_CRITERIA = {
    "min_gap_pct": 5.0,     # Was 7
    "min_rvol": 2.0,        # Was 3
    "min_pole_pct": 3.0,    # Was 5
    "max_retrace_pct": 55.0,  # Was 40
}

# Size multipliers by grade
GRADE_SIZE_MULTIPLIER = {
    SignalGrade.A_PLUS: 1.0,   # Full size
    SignalGrade.A: 0.75,       # 75% size
    SignalGrade.B: 0.50,       # 50% size
    SignalGrade.C: 0.0,        # Don't trade
}

# Minimum grade to trade
MIN_GRADE_TO_TRADE = SignalGrade.B

# ============================================================
# V5: EARLY EXIT / THESIS INVALIDATION
# ============================================================

USE_THESIS_INVALIDATION = False  # V5.1: DISABLED - it was cutting winners too early

# Bars to wait for confirmation after entry
MAX_BARS_FOR_NEW_HIGH = 8  # V5.1: Increased from 5 (give more time)

# Volume fade detection
VOLUME_FADE_THRESHOLD = 0.25  # V5.1: Lowered from 0.4 (less sensitive)

# Chop detection
CHOP_THRESHOLD = 0.3  # V5: Lowered (0.6 was too strict) - 0.3 = allow most setups

# ============================================================
# DATA STRUCTURES
# ============================================================

class PatternType(Enum):
    MICRO_PULLBACK = "MICRO_PULLBACK"
    BULL_FLAG = "BULL_FLAG"
    LEVEL_BREAKOUT = "LEVEL_BREAKOUT"


class ExitReason(Enum):
    STOP_LOSS = "STOP_LOSS"
    TP1 = "TP1"
    TP2 = "TP2"
    TRAILING_STOP = "TRAILING_STOP"
    BREAKEVEN = "BREAKEVEN"
    EOD_CLOSE = "EOD_CLOSE"
    THESIS_FAIL = "THESIS_FAIL"  # V5: New exit reason


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
    """Detected trade setup with quality grade."""
    symbol: str
    timestamp: datetime
    pattern: PatternType
    entry_price: float
    stop_price: float
    target_price: float
    risk_per_share: float
    breakout_level: float
    confidence: float

    # V5: New fields for grading
    grade: SignalGrade = SignalGrade.C
    gap_pct: float = 0.0
    rel_volume: float = 0.0
    pole_pct: float = 0.0
    retrace_pct: float = 0.0
    entry_volume: int = 0


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
    grade: SignalGrade

    highest_price: float = 0.0
    trail_active: bool = False
    trail_stop: float = 0.0
    be_active: bool = False

    # V5: Tiered exit tracking
    tp1_taken: bool = False
    tp2_taken: bool = False
    tp1_qty: int = 0
    tp2_qty: int = 0

    # V5: Thesis tracking
    entry_bar_idx: int = 0
    entry_volume: int = 0
    bars_since_entry: int = 0
    made_new_high: bool = False


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
    grade: str  # V5: Track grade


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

    # V5: Grade breakdown
    grade_stats: Dict[str, dict] = field(default_factory=dict)
    exit_stats: Dict[str, dict] = field(default_factory=dict)

    pattern_stats: Dict[str, dict] = field(default_factory=dict)
    trades: List[BacktestTrade] = field(default_factory=list)
    daily_results: List[DailyResult] = field(default_factory=list)


# ============================================================
# DATA FETCHING
# ============================================================

def fetch_daily_gainers_historical(days: int = 90) -> Dict[str, List[DailyGainer]]:
    """Simulate pre-market scanning by finding stocks that gapped up each day."""
    print("Building historical gainer database...")

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

            df["avg_vol_20"] = df["volume"].rolling(20).mean()
            df["prev_close"] = df["close"].shift(1)
            df["gap_pct"] = ((df["open"] - df["prev_close"]) / df["prev_close"]) * 100
            df["rel_vol"] = df["volume"] / df["avg_vol_20"]

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


def calculate_chop_index(df: pd.DataFrame, idx: int, lookback: int = 10) -> float:
    """
    V5: Calculate directional efficiency to detect choppy price action.
    Returns 0-1 where:
    - 1.0 = perfectly directional (trending)
    - 0.0 = pure chop (no direction)
    """
    if idx < lookback:
        return 0.5

    segment = df.iloc[idx-lookback:idx+1]

    # Total movement (sum of individual bar moves)
    total_movement = segment["high"].values - segment["low"].values
    total_movement = total_movement.sum()

    # Net movement (start to end)
    net_movement = abs(segment["close"].iloc[-1] - segment["close"].iloc[0])

    if total_movement == 0:
        return 0.5

    # Efficiency ratio
    efficiency = net_movement / total_movement

    return efficiency


# ============================================================
# V5: SIGNAL GRADING SYSTEM
# ============================================================

def grade_setup(gap_pct: float, rel_volume: float, pole_pct: float,
                retrace_pct: float, price: float) -> SignalGrade:
    """
    Grade a setup based on multiple factors.
    Human traders intuitively do this - we make it explicit.
    """
    score = 0

    # Gap size scoring (0-30 points)
    if gap_pct >= 20:
        score += 30
    elif gap_pct >= 15:
        score += 25
    elif gap_pct >= 10:
        score += 20
    elif gap_pct >= 7:
        score += 15
    elif gap_pct >= 5:
        score += 10

    # Relative volume scoring (0-30 points)
    if rel_volume >= 10:
        score += 30
    elif rel_volume >= 8:
        score += 25
    elif rel_volume >= 5:
        score += 20
    elif rel_volume >= 3:
        score += 15
    elif rel_volume >= 2:
        score += 10

    # Pole strength scoring (0-20 points)
    if pole_pct >= 10:
        score += 20
    elif pole_pct >= 8:
        score += 15
    elif pole_pct >= 6:
        score += 12
    elif pole_pct >= 5:
        score += 8

    # Retracement quality scoring (0-20 points) - tighter is better
    if retrace_pct <= 20:
        score += 20
    elif retrace_pct <= 25:
        score += 15
    elif retrace_pct <= 30:
        score += 12
    elif retrace_pct <= 40:
        score += 8

    # Price sweet spot bonus (0-10 points)
    if SWEET_SPOT_MIN <= price <= SWEET_SPOT_MAX:
        score += 10
    elif MIN_PRICE <= price <= MAX_PRICE:
        score += 5

    # Convert score to grade
    if score >= 85:
        return SignalGrade.A_PLUS
    elif score >= 65:
        return SignalGrade.A
    elif score >= 45:
        return SignalGrade.B
    else:
        return SignalGrade.C


# ============================================================
# PATTERN DETECTION
# ============================================================

def detect_bull_flag(df: pd.DataFrame, idx: int, gainer: DailyGainer) -> Optional[TradeSetup]:
    """
    Detect bull flag pattern with V5 grading.
    """
    if idx < 15:
        return None

    ts = df["timestamp"].iloc[idx]
    current_price = df["close"].iloc[idx]
    current_volume = int(df["volume"].iloc[idx])

    # V5: Check for chop first
    if USE_THESIS_INVALIDATION:
        chop = calculate_chop_index(df, idx, 10)
        if chop < CHOP_THRESHOLD:
            return None  # Too choppy, skip

    lookback = df.iloc[max(0, idx-20):idx+1]

    # Find pole
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
    if pole_range <= 0:
        return None
    retracement = (pole_high - flag_low) / pole_range * 100

    if retracement > FLAG_MAX_RETRACE_PCT:
        return None

    if flag_high > pole_high * 1.01:
        return None

    # Calculate entry/stop
    entry_price = flag_high + 0.02

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

    if risk_per_share <= 0 or risk_per_share > entry_price * 0.10:
        return None

    # V5: Grade this setup
    grade = grade_setup(
        gap_pct=gainer.gap_pct,
        rel_volume=gainer.rel_volume,
        pole_pct=best_pole_move,
        retrace_pct=retracement,
        price=current_price
    )

    # V5: Skip C-grade setups
    if GRADE_SIZE_MULTIPLIER.get(grade, 0) == 0:
        return None

    # Calculate target based on full R multiple
    target_r = 2.0  # V5: Use 2R as the "runner" target
    target_price = entry_price + (risk_per_share * target_r)

    # Confidence (still used for tiebreaking)
    confidence = 0.55
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
        confidence=min(confidence, 1.0),
        grade=grade,
        gap_pct=gainer.gap_pct,
        rel_volume=gainer.rel_volume,
        pole_pct=best_pole_move,
        retrace_pct=retracement,
        entry_volume=current_volume
    )


# ============================================================
# BACKTEST ENGINE
# ============================================================

class BacktestEngine:
    """Run backtest with V5 enhanced features."""

    def __init__(self):
        self.capital = INITIAL_CAPITAL
        self.positions: Dict[str, Position] = {}
        self.trades: List[BacktestTrade] = []
        self.equity_curve: List[float] = [INITIAL_CAPITAL]
        self.daily_results: List[DailyResult] = []

    def run_backtest(self, gainers_by_date: Dict[str, List[DailyGainer]]) -> BacktestResult:
        """Run backtest across all days."""
        print(f"\n{'='*60}")
        print("SMALL CAP MOMENTUM BACKTEST - V5 ENHANCED")
        print(f"{'='*60}")
        print(f"Period: {len(gainers_by_date)} trading days")
        print(f"Initial Capital: ${INITIAL_CAPITAL:,.0f}")
        print(f"Max Risk/Trade: ${MAX_RISK_PER_TRADE}")
        print(f"\n[V5 ENHANCEMENTS]")
        print(f"  Tiered Exits: TP1={TP1_R}R ({TP1_SIZE_PCT*100:.0f}%), TP2={TP2_R}R ({TP2_SIZE_PCT*100:.0f}%), Trail {(1-TP1_SIZE_PCT-TP2_SIZE_PCT)*100:.0f}%")
        print(f"  Signal Grading: A+/A/B (skip C-grade)")
        print(f"  Thesis Invalidation: {USE_THESIS_INVALIDATION}")
        print(f"{'='*60}\n")

        sorted_dates = sorted(gainers_by_date.keys())

        for date_str in sorted_dates:
            gainers = gainers_by_date[date_str]
            if not gainers:
                continue
            self._process_day(date_str, gainers)

        result = self._calculate_results()
        self._print_results(result)

        return result

    def _process_day(self, date_str: str, gainers: List[DailyGainer]):
        """Process a single trading day."""
        daily = DailyResult(date=date_str)
        self.positions = {}

        gainers = sorted(gainers, key=lambda x: x.gap_pct, reverse=True)[:5]

        print(f"\n{date_str}: Processing {len(gainers)} gainers...")

        minute_data: Dict[str, pd.DataFrame] = {}
        for gainer in gainers:
            df = fetch_minute_bars(gainer.symbol, gainer.date)
            if df is not None and len(df) > 30:
                if USE_ATR_STOPS:
                    df["atr"] = calculate_atr(df, ATR_PERIOD)
                minute_data[gainer.symbol] = df
                print(f"  {gainer.symbol}: +{gainer.gap_pct:.1f}% gap, RVOL={gainer.rel_volume:.1f}x")

        if not minute_data:
            return

        all_timestamps = set()
        for df in minute_data.values():
            all_timestamps.update(df["timestamp"].tolist())
        all_timestamps = sorted(all_timestamps)

        for ts in all_timestamps:
            hour, minute = ts.hour, ts.minute

            if hour == 9 and minute < 30 + TRADING_START_MINUTES:
                continue

            # Manage positions (with V5 tiered exits)
            self._manage_positions(ts, minute_data, daily)

            if hour >= EOD_CLOSE_HOUR and minute >= EOD_CLOSE_MINUTE:
                self._close_all(ts, minute_data, daily, ExitReason.EOD_CLOSE)
                continue

            if hour >= TRADING_END_HOUR or daily.halted:
                continue

            if daily.gross_pnl <= -MAX_DAILY_LOSS:
                daily.halted = True
                print(f"    DAILY LOSS LIMIT: Trading halted")
                continue

            if len(self.positions) < MAX_POSITIONS:
                self._scan_for_entries(ts, minute_data, gainers, daily)

        if self.positions:
            final_ts = all_timestamps[-1] if all_timestamps else datetime.now(ET)
            self._close_all(final_ts, minute_data, daily, ExitReason.EOD_CLOSE)

        self.daily_results.append(daily)
        print(f"  Day result: {daily.trades} trades, ${daily.gross_pnl:.2f} P&L")

    def _manage_positions(self, ts: datetime, minute_data: Dict[str, pd.DataFrame], daily: DailyResult):
        """Manage open positions with V5 tiered exits."""
        positions_to_remove = []

        for symbol, pos in self.positions.items():
            df = minute_data.get(symbol)
            if df is None:
                continue

            current_bars = df[df["timestamp"] == ts]
            if current_bars.empty:
                continue

            row = current_bars.iloc[0]
            idx = current_bars.index[0]
            high = row["high"]
            low = row["low"]
            close = row["close"]
            volume = int(row["volume"])

            # Track bars since entry
            pos.bars_since_entry += 1

            # Update highest
            if high > pos.highest_price:
                pos.highest_price = high
                pos.made_new_high = True

            # V5: Thesis invalidation check
            if USE_THESIS_INVALIDATION and not pos.tp1_taken:
                # No new high after several bars
                if pos.bars_since_entry >= MAX_BARS_FOR_NEW_HIGH and not pos.made_new_high:
                    if close < pos.entry_price:  # Only exit if underwater
                        self._record_exit(pos, ts, close, ExitReason.THESIS_FAIL, daily)
                        positions_to_remove.append(symbol)
                        continue

                # Volume fading (momentum dying)
                if pos.entry_volume > 0:
                    volume_ratio = volume / pos.entry_volume
                    if volume_ratio < VOLUME_FADE_THRESHOLD and close < pos.entry_price:
                        self._record_exit(pos, ts, close, ExitReason.THESIS_FAIL, daily)
                        positions_to_remove.append(symbol)
                        continue

            # Check stop loss first
            current_stop = pos.trail_stop if pos.trail_active else pos.stop_price
            if low <= current_stop:
                exit_price = current_stop
                if pos.be_active and current_stop >= pos.entry_price:
                    reason = ExitReason.BREAKEVEN
                elif pos.trail_active:
                    reason = ExitReason.TRAILING_STOP
                else:
                    reason = ExitReason.STOP_LOSS
                self._record_exit(pos, ts, exit_price, reason, daily)
                positions_to_remove.append(symbol)
                continue

            # V5: TIERED EXITS
            if USE_TIERED_EXITS:
                # TP1: Lock in quick profit
                if not pos.tp1_taken:
                    tp1_price = pos.entry_price + (pos.risk_per_share * TP1_R)
                    if high >= tp1_price:
                        self._take_tiered_exit(pos, tp1_price, ts, daily, "TP1")

                        # Move to breakeven after TP1
                        if MOVE_TO_BE_AFTER_TP1:
                            pos.be_active = True
                            pos.stop_price = pos.entry_price + 0.01
                        continue  # Don't check other exits this bar

                # TP2: Solid profit
                if pos.tp1_taken and not pos.tp2_taken:
                    tp2_price = pos.entry_price + (pos.risk_per_share * TP2_R)
                    if high >= tp2_price:
                        self._take_tiered_exit(pos, tp2_price, ts, daily, "TP2")

                        # Activate trailing for remainder
                        pos.trail_active = True
                        pos.trail_stop = high * (1 - TRAIL_DISTANCE_PCT / 100)
                        continue

            # Update trailing stop
            if pos.trail_active:
                new_trail = pos.highest_price * (1 - TRAIL_DISTANCE_PCT / 100)
                if new_trail > pos.trail_stop:
                    pos.trail_stop = new_trail

        for symbol in positions_to_remove:
            if symbol in self.positions:
                del self.positions[symbol]

    def _take_tiered_exit(self, pos: Position, exit_price: float, ts: datetime,
                          daily: DailyResult, tier: str):
        """Take a tiered partial exit."""
        if tier == "TP1":
            qty = pos.tp1_qty
            pos.tp1_taken = True
        else:  # TP2
            qty = pos.tp2_qty
            pos.tp2_taken = True

        if qty <= 0:
            return

        pnl = (exit_price - pos.entry_price) * qty
        r_mult = (exit_price - pos.entry_price) / pos.risk_per_share

        pos.remaining_qty -= qty

        daily.gross_pnl += pnl
        self.capital += pnl
        self.equity_curve.append(self.capital)

        exit_reason = ExitReason.TP1 if tier == "TP1" else ExitReason.TP2

        trade = BacktestTrade(
            symbol=pos.symbol,
            date=ts.strftime("%Y-%m-%d"),
            entry_time=pos.entry_time,
            exit_time=ts,
            entry_price=pos.entry_price,
            exit_price=exit_price,
            qty=qty,
            pattern=pos.pattern.value,
            exit_reason=exit_reason.value,
            pnl=pnl,
            r_multiple=r_mult,
            hold_minutes=int((ts - pos.entry_time).total_seconds() / 60),
            grade=pos.grade.value
        )
        self.trades.append(trade)

        print(f"    {tier}: {pos.symbol} [{pos.grade.value}] | {qty} @ ${exit_price:.2f} | "
              f"P&L=${pnl:.2f} ({r_mult:.2f}R)")

    def _scan_for_entries(self, ts: datetime, minute_data: Dict[str, pd.DataFrame],
                         gainers: List[DailyGainer], daily: DailyResult):
        """Scan for new entry setups with grading."""
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

            # Try bull flag (primary pattern)
            setup = detect_bull_flag(df, idx, gainer)

            if setup and setup.grade in [SignalGrade.A_PLUS, SignalGrade.A, SignalGrade.B]:
                # V5: Check minimum grade
                grade_value = list(SignalGrade).index(setup.grade)
                min_grade_value = list(SignalGrade).index(MIN_GRADE_TO_TRADE)

                if grade_value <= min_grade_value:  # Lower index = better grade
                    self._open_position(setup, gainer, daily, df, idx)
                    break

    def _open_position(self, setup: TradeSetup, gainer: DailyGainer, daily: DailyResult,
                      df: pd.DataFrame, idx: int):
        """Open a new position with V5 sizing by grade."""
        # V5: Size by grade
        size_multiplier = GRADE_SIZE_MULTIPLIER.get(setup.grade, 0)
        if size_multiplier == 0:
            return

        risk_dollars = MAX_RISK_PER_TRADE * size_multiplier
        qty = int(risk_dollars / setup.risk_per_share)

        if qty <= 0:
            return

        position_value = qty * setup.entry_price
        if position_value > self.capital * 0.5:
            qty = int((self.capital * 0.5) / setup.entry_price)

        if qty <= 0:
            return

        # V5: Calculate tiered exit quantities
        tp1_qty = int(qty * TP1_SIZE_PCT)
        tp2_qty = int(qty * TP2_SIZE_PCT)
        trail_qty = qty - tp1_qty - tp2_qty  # Remaining

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
            grade=setup.grade,
            highest_price=setup.entry_price,
            tp1_qty=tp1_qty,
            tp2_qty=tp2_qty,
            entry_bar_idx=idx,
            entry_volume=setup.entry_volume
        )

        self.positions[setup.symbol] = pos

        print(f"    ENTRY: {setup.symbol} [{setup.grade.value}] | {setup.pattern.value} | "
              f"{qty} @ ${setup.entry_price:.2f} | Stop=${setup.stop_price:.2f} | "
              f"TP1={tp1_qty} TP2={tp2_qty} Trail={trail_qty}")

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
            grade=pos.grade.value
        )
        self.trades.append(trade)

        print(f"    EXIT: {pos.symbol} [{pos.grade.value}] | {reason.value} | "
              f"{pos.remaining_qty} @ ${exit_price:.2f} | P&L=${pnl:.2f} ({r_mult:.2f}R)")

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
        """Calculate final results with V5 breakdown."""
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

        # V5: Stats by grade
        for grade in SignalGrade:
            grade_trades = [t for t in self.trades if t.grade == grade.value]
            if grade_trades:
                wins = sum(1 for t in grade_trades if t.pnl > 0)
                pnl = sum(t.pnl for t in grade_trades)
                result.grade_stats[grade.value] = {
                    "trades": len(grade_trades),
                    "wins": wins,
                    "win_rate": wins / len(grade_trades) * 100,
                    "pnl": pnl,
                    "avg_r": sum(t.r_multiple for t in grade_trades) / len(grade_trades)
                }

        # V5: Stats by exit reason
        for reason in ExitReason:
            reason_trades = [t for t in self.trades if t.exit_reason == reason.value]
            if reason_trades:
                wins = sum(1 for t in reason_trades if t.pnl > 0)
                pnl = sum(t.pnl for t in reason_trades)
                result.exit_stats[reason.value] = {
                    "trades": len(reason_trades),
                    "wins": wins,
                    "win_rate": wins / len(reason_trades) * 100 if reason_trades else 0,
                    "pnl": pnl
                }

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

        return result

    def _print_results(self, result: BacktestResult):
        """Print formatted results with V5 breakdown."""
        print(f"\n{'='*60}")
        print("BACKTEST RESULTS - V5 ENHANCED")
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

        # V5: Grade breakdown
        print(f"\n[GRADE BREAKDOWN]")
        for grade, stats in result.grade_stats.items():
            print(f"   {grade}: {stats['trades']} trades | "
                  f"{stats['win_rate']:.1f}% WR | ${stats['pnl']:.2f} | "
                  f"{stats['avg_r']:.2f}R avg")

        # V5: Exit reason breakdown
        print(f"\n[EXIT REASONS]")
        for reason, stats in sorted(result.exit_stats.items(), key=lambda x: -x[1]["trades"]):
            pct = stats["trades"] / result.total_trades * 100 if result.total_trades > 0 else 0
            print(f"   {reason}: {stats['trades']} ({pct:.1f}%) | "
                  f"WR={stats['win_rate']:.1f}% | P&L=${stats['pnl']:.2f}")

        print(f"\n[PATTERN BREAKDOWN]")
        for pattern, stats in result.pattern_stats.items():
            print(f"   {pattern}: {stats['trades']} trades | "
                  f"{stats['win_rate']:.1f}% WR | ${stats['pnl']:.2f} | "
                  f"{stats['avg_r']:.2f}R avg")

        print(f"\n[FINAL CAPITAL]: ${self.capital:,.2f}")
        print(f"   Return: {(self.capital - INITIAL_CAPITAL) / INITIAL_CAPITAL * 100:.1f}%")
        print(f"{'='*60}\n")


# ============================================================
# MAIN
# ============================================================

def main():
    """Run the V5 enhanced backtest."""
    gainers_by_date = fetch_daily_gainers_historical(BACKTEST_DAYS)

    if not gainers_by_date:
        print("No gap-up data found. Exiting.")
        return

    engine = BacktestEngine()
    result = engine.run_backtest(gainers_by_date)

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
                "grade": t.grade,
                "exit_reason": t.exit_reason,
                "pnl": round(t.pnl, 2),
                "r_multiple": round(t.r_multiple, 2),
                "hold_minutes": t.hold_minutes
            }
            for t in result.trades
        ])
        trades_df.to_csv("smallcap_momentum_backtest_v5_trades.csv", index=False)
        print("Trade log saved to smallcap_momentum_backtest_v5_trades.csv")


if __name__ == "__main__":
    main()
