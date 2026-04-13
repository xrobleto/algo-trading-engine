"""
Small Cap Momentum Backtest Framework - V6 MULTI-APPROACH TEST
===============================================================

Testing multiple approaches to fix the loser-to-winner ratio problem:

APPROACH 1: TIGHTER STOPS + WIDER TARGETS
- Tighter ATR multiplier (2.0x instead of 3.0x)
- Accept lower win rate but better R:R
- Goal: Smaller losses, let winners run

APPROACH 2: SCALE-IN ENTRY (Half size, add on confirmation)
- Enter with 50% size initially
- Add remaining 50% if price makes new high within 3 bars
- Goal: Reduce initial risk, confirm momentum before full size

APPROACH 3: TIME-BASED EXIT
- Exit if no profit after X bars (thesis failed)
- Exit at smaller loss than full stop
- Goal: Cut losers early

APPROACH 4: VOLATILITY FILTER
- Skip high-ATR days (volatile = wider stops = bigger losses)
- Only trade when ATR is below threshold
- Goal: Smaller stop distances

Version: 6.0.0 - Multi-Approach Testing
"""

import os
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple
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

# --- Pattern Detection ---
FLAG_POLE_MIN_PCT = 3.0
FLAG_MAX_RETRACE_PCT = 50.0
FLAG_MIN_BARS = 2
FLAG_MAX_BARS = 15

# ============================================================
# APPROACH SELECTION (Toggle which approach to test)
# ============================================================

# Set ONE of these to True to test that approach
# Set ALL to False to run comparison of all approaches
APPROACH_BASELINE = False      # V5 baseline (for comparison)
APPROACH_TIGHT_STOPS = False   # Approach 1: Tighter stops
APPROACH_SCALE_IN = False      # Approach 2: Scale-in entry
APPROACH_TIME_EXIT = False     # Approach 3: Time-based exit
APPROACH_VOL_FILTER = False    # Approach 4: Volatility filter
APPROACH_COMBINED = False      # Approach 5: Best combination

# ============================================================
# APPROACH 1: TIGHT STOPS PARAMETERS
# ============================================================
TIGHT_ATR_MULT = 2.0           # Tighter stop (was 3.0)
TIGHT_MIN_STOP_PCT = 1.5       # Tighter min (was 2.5%)
TIGHT_TP1_R = 1.0              # Wider TP1 (was 0.75)
TIGHT_TP2_R = 2.0              # Wider TP2 (was 1.5)

# ============================================================
# APPROACH 2: SCALE-IN PARAMETERS
# ============================================================
SCALE_INITIAL_PCT = 0.50       # Enter with 50% size
SCALE_CONFIRM_BARS = 3         # Bars to wait for confirmation
SCALE_ADD_ON_NEW_HIGH = True   # Add if new high made

# ============================================================
# APPROACH 3: TIME-BASED EXIT PARAMETERS
# ============================================================
TIME_EXIT_BARS = 10            # Exit if no profit after 10 bars
TIME_EXIT_IF_UNDERWATER = True # Only exit if position is negative
TIME_EXIT_LOSS_LIMIT_R = -0.5  # Exit early if down 0.5R

# ============================================================
# APPROACH 4: VOLATILITY FILTER PARAMETERS
# ============================================================
VOL_MAX_ATR_PCT = 4.0          # Skip if ATR > 4% of price
VOL_PREFER_LOW_ATR = True      # Prefer lower ATR setups

# ============================================================
# APPROACH 5: COMBINED (Best of all)
# ============================================================
# Uses:
# - Moderate stops (2.5x ATR)
# - Time exit (8 bars)
# - Volatility filter (skip > 5% ATR)
# - Tiered exits at 0.75R/1.5R

# ============================================================
# BASE PARAMETERS (modified by approach selection)
# ============================================================

ATR_PERIOD = 14
BASE_ATR_STOP_MULT = 3.0
BASE_MIN_STOP_PCT = 2.5
MAX_RISK_PER_TRADE = 150.00
MAX_DAILY_LOSS = 300.00
MAX_POSITIONS = 1

# Tiered exits
TP1_R = 0.75
TP1_SIZE_PCT = 0.33
TP2_R = 1.5
TP2_SIZE_PCT = 0.33
TRAIL_ACTIVATION_R = 1.5
TRAIL_DISTANCE_PCT = 2.0


# ============================================================
# DATA STRUCTURES
# ============================================================

class ExitReason(Enum):
    STOP_LOSS = "STOP_LOSS"
    TP1 = "TP1"
    TP2 = "TP2"
    TRAILING_STOP = "TRAILING_STOP"
    BREAKEVEN = "BREAKEVEN"
    EOD_CLOSE = "EOD_CLOSE"
    TIME_EXIT = "TIME_EXIT"
    EARLY_CUT = "EARLY_CUT"


@dataclass
class DailyGainer:
    symbol: str
    date: datetime
    gap_pct: float
    open_price: float
    prev_close: float
    volume: int
    rel_volume: float


@dataclass
class TradeSetup:
    symbol: str
    timestamp: datetime
    entry_price: float
    stop_price: float
    target_price: float
    risk_per_share: float
    breakout_level: float
    confidence: float
    atr_pct: float = 0.0  # ATR as % of price


@dataclass
class Position:
    symbol: str
    entry_time: datetime
    entry_price: float
    stop_price: float
    target_price: float
    risk_per_share: float
    total_qty: int
    remaining_qty: int

    highest_price: float = 0.0
    trail_active: bool = False
    trail_stop: float = 0.0
    be_active: bool = False

    tp1_taken: bool = False
    tp2_taken: bool = False
    tp1_qty: int = 0
    tp2_qty: int = 0

    # Scale-in tracking
    is_scaled_in: bool = False
    initial_qty: int = 0
    bars_since_entry: int = 0
    entry_bar_idx: int = 0
    made_new_high: bool = False


@dataclass
class BacktestTrade:
    symbol: str
    date: str
    entry_time: datetime
    exit_time: datetime
    entry_price: float
    exit_price: float
    qty: int
    exit_reason: str
    pnl: float
    r_multiple: float
    hold_minutes: int


@dataclass
class BacktestResult:
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
    approach_name: str = ""

    exit_stats: Dict[str, dict] = field(default_factory=dict)
    trades: List[BacktestTrade] = field(default_factory=list)


# ============================================================
# DATA FETCHING
# ============================================================

def fetch_daily_gainers_historical(days: int = 90) -> Dict[str, List[DailyGainer]]:
    """Fetch historical gap-up data."""
    print("Building historical gainer database...")

    SCAN_UNIVERSE = [
        "SNDL", "TLRY", "ACB", "CGC", "CRON", "HEXO", "OGI", "VFF",
        "WKHS", "GOEV", "NKLA", "LCID", "RIVN", "BLNK", "CHPT", "QS",
        "EVGO", "PTRA", "HYLN", "XPEV", "NIO", "LI",
        "MARA", "RIOT", "BITF", "HUT", "CLSK", "CIFR", "BTBT", "CAN",
        "SPCE", "RKLB", "LUNR", "RDW", "MNTS", "ASTS", "GSAT",
        "AMC", "GME", "BB", "CLOV", "SOFI", "HOOD",
        "PLTR", "AI", "IONQ", "RGTI", "QUBT", "SOUN", "BBAI",
        "DNA", "CRSP", "BEAM", "EDIT", "NTLA", "ARCT", "MRNA",
        "BNTX", "NVAX", "VXRT", "INO", "OCGN",
        "RBLX", "AFRM", "UPST", "PATH", "DOCS", "DKNG", "PENN",
        "SKLZ", "FUBO", "OPEN", "WISH", "SDC", "BARK",
        "VALE", "CLF", "X", "AA", "FCX", "GOLD", "NEM",
        "DWAC", "PHUN", "BKKT", "GRAB", "PSFE",
        "CENN", "IMPP", "BBIG", "PROG", "ATER", "GREE",
        "IRNT", "OPAD", "TMC", "LIDR", "MLGO",
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
                    gainers_by_date[str(row["date"])].append(gainer)
            else:
                print("OK (no gaps)")

        except Exception as e:
            print(f"ERROR: {e}")

    for date_str in gainers_by_date:
        gainers_by_date[date_str].sort(key=lambda x: x.gap_pct, reverse=True)

    print(f"\nFound {sum(len(v) for v in gainers_by_date.values())} gap-up events")
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
    """Calculate ATR."""
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

def detect_bull_flag(df: pd.DataFrame, idx: int, gainer: DailyGainer,
                     atr_mult: float, min_stop_pct: float) -> Optional[TradeSetup]:
    """Detect bull flag with configurable stop parameters."""
    if idx < 15:
        return None

    ts = df["timestamp"].iloc[idx]
    current_price = df["close"].iloc[idx]

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
    pole_end_loc = lookback.index.get_loc(pole_high_idx)
    flag_bars = lookback.iloc[pole_end_loc:]

    if len(flag_bars) < FLAG_MIN_BARS or len(flag_bars) > FLAG_MAX_BARS:
        return None

    flag_high = flag_bars["high"].max()
    flag_low = flag_bars["low"].min()

    pole_range = pole_high - pole_low
    if pole_range <= 0:
        return None
    retracement = (pole_high - flag_low) / pole_range * 100

    if retracement > FLAG_MAX_RETRACE_PCT:
        return None

    if flag_high > pole_high * 1.01:
        return None

    entry_price = flag_high + 0.02

    # Calculate ATR-based stop with configurable multiplier
    if "atr" in df.columns:
        atr = df.iloc[idx]["atr"] if idx < len(df) else df["atr"].iloc[-1]
        if pd.notna(atr) and atr > 0:
            atr_stop_distance = atr * atr_mult
            min_stop_distance = entry_price * (min_stop_pct / 100)
            stop_distance = max(atr_stop_distance, min_stop_distance)
            stop_price = entry_price - stop_distance
            atr_pct = (atr / current_price) * 100
        else:
            stop_price = flag_low - 0.02
            atr_pct = 0
    else:
        stop_price = flag_low - 0.02
        atr_pct = 0

    risk_per_share = entry_price - stop_price

    if risk_per_share <= 0 or risk_per_share > entry_price * 0.10:
        return None

    # Use approach-specific TP
    if APPROACH_TIGHT_STOPS:
        target_r = TIGHT_TP2_R
    else:
        target_r = 2.0

    target_price = entry_price + (risk_per_share * target_r)

    confidence = 0.55
    if gainer.gap_pct > 12:
        confidence += 0.10
    if gainer.rel_volume > 6:
        confidence += 0.10
    if retracement < 30:
        confidence += 0.10

    return TradeSetup(
        symbol=gainer.symbol,
        timestamp=ts,
        entry_price=entry_price,
        stop_price=stop_price,
        target_price=target_price,
        risk_per_share=risk_per_share,
        breakout_level=flag_high,
        confidence=min(confidence, 1.0),
        atr_pct=atr_pct
    )


# ============================================================
# BACKTEST ENGINE
# ============================================================

class BacktestEngine:
    """Run backtest with configurable approach."""

    def __init__(self, approach_name: str):
        self.approach_name = approach_name
        self.capital = INITIAL_CAPITAL
        self.positions: Dict[str, Position] = {}
        self.trades: List[BacktestTrade] = []
        self.equity_curve: List[float] = [INITIAL_CAPITAL]

    def get_stop_params(self) -> Tuple[float, float]:
        """Get ATR multiplier and min stop % based on approach."""
        if APPROACH_TIGHT_STOPS:
            return TIGHT_ATR_MULT, TIGHT_MIN_STOP_PCT
        elif APPROACH_COMBINED:
            return 2.5, 2.0  # Moderate tightening
        else:
            return BASE_ATR_STOP_MULT, BASE_MIN_STOP_PCT

    def get_tp_params(self) -> Tuple[float, float]:
        """Get TP1 and TP2 R-multiples based on approach."""
        if APPROACH_TIGHT_STOPS:
            return TIGHT_TP1_R, TIGHT_TP2_R
        else:
            return TP1_R, TP2_R

    def should_skip_for_volatility(self, atr_pct: float) -> bool:
        """Check if should skip due to high volatility."""
        if APPROACH_VOL_FILTER or APPROACH_COMBINED:
            max_atr = VOL_MAX_ATR_PCT if APPROACH_VOL_FILTER else 5.0
            return atr_pct > max_atr
        return False

    def run_backtest(self, gainers_by_date: Dict[str, List[DailyGainer]]) -> BacktestResult:
        """Run backtest."""
        print(f"\n{'='*60}")
        print(f"BACKTEST: {self.approach_name}")
        print(f"{'='*60}")

        atr_mult, min_stop = self.get_stop_params()
        tp1_r, tp2_r = self.get_tp_params()

        print(f"  ATR Mult: {atr_mult}x | Min Stop: {min_stop}%")
        print(f"  TP1: {tp1_r}R | TP2: {tp2_r}R")
        if APPROACH_VOL_FILTER or APPROACH_COMBINED:
            print(f"  Vol Filter: Skip if ATR > {VOL_MAX_ATR_PCT if APPROACH_VOL_FILTER else 5.0}%")
        if APPROACH_TIME_EXIT or APPROACH_COMBINED:
            print(f"  Time Exit: {TIME_EXIT_BARS if APPROACH_TIME_EXIT else 8} bars")
        if APPROACH_SCALE_IN:
            print(f"  Scale-In: {SCALE_INITIAL_PCT*100}% initial, add on confirm")
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
        """Process a single day."""
        self.positions = {}
        gainers = sorted(gainers, key=lambda x: x.gap_pct, reverse=True)[:5]

        minute_data: Dict[str, pd.DataFrame] = {}
        for gainer in gainers:
            df = fetch_minute_bars(gainer.symbol, gainer.date)
            if df is not None and len(df) > 30:
                df["atr"] = calculate_atr(df, ATR_PERIOD)
                minute_data[gainer.symbol] = df

        if not minute_data:
            return

        all_timestamps = set()
        for df in minute_data.values():
            all_timestamps.update(df["timestamp"].tolist())
        all_timestamps = sorted(all_timestamps)

        daily_pnl = 0.0

        for ts in all_timestamps:
            hour, minute = ts.hour, ts.minute

            if hour == 9 and minute < 30 + TRADING_START_MINUTES:
                continue

            # Manage positions
            self._manage_positions(ts, minute_data, daily_pnl)

            if hour >= EOD_CLOSE_HOUR and minute >= EOD_CLOSE_MINUTE:
                self._close_all(ts, minute_data, ExitReason.EOD_CLOSE)
                continue

            if hour >= TRADING_END_HOUR:
                continue

            if daily_pnl <= -MAX_DAILY_LOSS:
                continue

            if len(self.positions) < MAX_POSITIONS:
                self._scan_for_entries(ts, minute_data, gainers)

    def _manage_positions(self, ts: datetime, minute_data: Dict[str, pd.DataFrame],
                          daily_pnl: float):
        """Manage positions with approach-specific logic."""
        positions_to_remove = []
        tp1_r, tp2_r = self.get_tp_params()

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

            pos.bars_since_entry += 1

            if high > pos.highest_price:
                pos.highest_price = high
                pos.made_new_high = True

            # APPROACH 2: Scale-in logic
            if APPROACH_SCALE_IN and not pos.is_scaled_in:
                if pos.bars_since_entry <= SCALE_CONFIRM_BARS and pos.made_new_high:
                    # Add remaining position
                    add_qty = pos.total_qty - pos.initial_qty
                    if add_qty > 0:
                        pos.remaining_qty += add_qty
                        pos.is_scaled_in = True
                elif pos.bars_since_entry > SCALE_CONFIRM_BARS and not pos.made_new_high:
                    # No confirmation - exit early
                    self._record_exit(pos, ts, close, ExitReason.EARLY_CUT)
                    positions_to_remove.append(symbol)
                    continue

            # APPROACH 3 & COMBINED: Time-based exit
            time_exit_bars = TIME_EXIT_BARS if APPROACH_TIME_EXIT else 8
            if (APPROACH_TIME_EXIT or APPROACH_COMBINED) and not pos.tp1_taken:
                if pos.bars_since_entry >= time_exit_bars:
                    current_r = (close - pos.entry_price) / pos.risk_per_share
                    # Exit if underwater or below threshold
                    if TIME_EXIT_IF_UNDERWATER and close < pos.entry_price:
                        self._record_exit(pos, ts, close, ExitReason.TIME_EXIT)
                        positions_to_remove.append(symbol)
                        continue
                    elif current_r < TIME_EXIT_LOSS_LIMIT_R:
                        self._record_exit(pos, ts, close, ExitReason.EARLY_CUT)
                        positions_to_remove.append(symbol)
                        continue

            # Check stop loss
            current_stop = pos.trail_stop if pos.trail_active else pos.stop_price
            if low <= current_stop:
                exit_price = current_stop
                if pos.be_active and current_stop >= pos.entry_price:
                    reason = ExitReason.BREAKEVEN
                elif pos.trail_active:
                    reason = ExitReason.TRAILING_STOP
                else:
                    reason = ExitReason.STOP_LOSS
                self._record_exit(pos, ts, exit_price, reason)
                positions_to_remove.append(symbol)
                continue

            # Tiered exits
            if not pos.tp1_taken:
                tp1_price = pos.entry_price + (pos.risk_per_share * tp1_r)
                if high >= tp1_price:
                    self._take_tiered_exit(pos, tp1_price, ts, "TP1")
                    pos.be_active = True
                    pos.stop_price = pos.entry_price + 0.01
                    continue

            if pos.tp1_taken and not pos.tp2_taken:
                tp2_price = pos.entry_price + (pos.risk_per_share * tp2_r)
                if high >= tp2_price:
                    self._take_tiered_exit(pos, tp2_price, ts, "TP2")
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

    def _take_tiered_exit(self, pos: Position, exit_price: float, ts: datetime, tier: str):
        """Take tiered exit."""
        if tier == "TP1":
            qty = pos.tp1_qty
            pos.tp1_taken = True
        else:
            qty = pos.tp2_qty
            pos.tp2_taken = True

        if qty <= 0:
            return

        pnl = (exit_price - pos.entry_price) * qty
        r_mult = (exit_price - pos.entry_price) / pos.risk_per_share

        pos.remaining_qty -= qty
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
            exit_reason=exit_reason.value,
            pnl=pnl,
            r_multiple=r_mult,
            hold_minutes=int((ts - pos.entry_time).total_seconds() / 60)
        )
        self.trades.append(trade)

    def _scan_for_entries(self, ts: datetime, minute_data: Dict[str, pd.DataFrame],
                         gainers: List[DailyGainer]):
        """Scan for entries with approach-specific filtering."""
        atr_mult, min_stop = self.get_stop_params()

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

            setup = detect_bull_flag(df, idx, gainer, atr_mult, min_stop)

            if setup:
                # APPROACH 4 & COMBINED: Volatility filter
                if self.should_skip_for_volatility(setup.atr_pct):
                    continue

                self._open_position(setup, gainer, df, idx)
                break

    def _open_position(self, setup: TradeSetup, gainer: DailyGainer,
                      df: pd.DataFrame, idx: int):
        """Open position with approach-specific sizing."""
        risk_dollars = MAX_RISK_PER_TRADE
        qty = int(risk_dollars / setup.risk_per_share)

        if qty <= 0:
            return

        position_value = qty * setup.entry_price
        if position_value > self.capital * 0.5:
            qty = int((self.capital * 0.5) / setup.entry_price)

        if qty <= 0:
            return

        # APPROACH 2: Scale-in - start with partial size
        if APPROACH_SCALE_IN:
            initial_qty = int(qty * SCALE_INITIAL_PCT)
            if initial_qty <= 0:
                initial_qty = 1
        else:
            initial_qty = qty

        tp1_qty = int(qty * TP1_SIZE_PCT)
        tp2_qty = int(qty * TP2_SIZE_PCT)
        trail_qty = qty - tp1_qty - tp2_qty

        pos = Position(
            symbol=setup.symbol,
            entry_time=setup.timestamp,
            entry_price=setup.entry_price,
            stop_price=setup.stop_price,
            target_price=setup.target_price,
            risk_per_share=setup.risk_per_share,
            total_qty=qty,
            remaining_qty=initial_qty if APPROACH_SCALE_IN else qty,
            highest_price=setup.entry_price,
            tp1_qty=tp1_qty,
            tp2_qty=tp2_qty,
            entry_bar_idx=idx,
            initial_qty=initial_qty,
            is_scaled_in=not APPROACH_SCALE_IN  # Already scaled in if not using approach
        )

        self.positions[setup.symbol] = pos

    def _record_exit(self, pos: Position, ts: datetime, exit_price: float, reason: ExitReason):
        """Record exit."""
        pnl = (exit_price - pos.entry_price) * pos.remaining_qty
        r_mult = (exit_price - pos.entry_price) / pos.risk_per_share

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
            exit_reason=reason.value,
            pnl=pnl,
            r_multiple=r_mult,
            hold_minutes=int((ts - pos.entry_time).total_seconds() / 60)
        )
        self.trades.append(trade)

    def _close_all(self, ts: datetime, minute_data: Dict[str, pd.DataFrame], reason: ExitReason):
        """Close all positions."""
        for symbol, pos in list(self.positions.items()):
            df = minute_data.get(symbol)
            if df is None:
                continue

            current_bars = df[df["timestamp"] <= ts].tail(1)
            if current_bars.empty:
                continue

            exit_price = current_bars.iloc[0]["close"]
            self._record_exit(pos, ts, exit_price, reason)

        self.positions = {}

    def _calculate_results(self) -> BacktestResult:
        """Calculate results."""
        result = BacktestResult()
        result.approach_name = self.approach_name
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

        peak = INITIAL_CAPITAL
        max_dd = 0
        for equity in self.equity_curve:
            if equity > peak:
                peak = equity
            dd = (peak - equity) / peak
            if dd > max_dd:
                max_dd = dd
        result.max_drawdown = max_dd * 100

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

        return result

    def _print_results(self, result: BacktestResult):
        """Print results."""
        print(f"\n{'='*60}")
        print(f"RESULTS: {result.approach_name}")
        print(f"{'='*60}")

        print(f"\n[PERFORMANCE]")
        print(f"   Trades: {result.total_trades}")
        print(f"   Win Rate: {result.win_rate:.1f}%")
        print(f"   Profit Factor: {result.profit_factor:.2f}")
        print(f"   Net P&L: ${result.net_pnl:,.2f}")
        print(f"   Avg Winner: ${result.avg_winner:,.2f}")
        print(f"   Avg Loser: ${result.avg_loser:,.2f}")
        print(f"   Loser/Winner Ratio: {result.avg_loser/result.avg_winner:.2f}x" if result.avg_winner > 0 else "   Loser/Winner Ratio: N/A")
        print(f"   Avg R: {result.avg_r:.2f}R")
        print(f"   Max DD: {result.max_drawdown:.1f}%")

        print(f"\n[EXIT BREAKDOWN]")
        for reason, stats in sorted(result.exit_stats.items(), key=lambda x: -x[1]["trades"]):
            pct = stats["trades"] / result.total_trades * 100 if result.total_trades > 0 else 0
            print(f"   {reason}: {stats['trades']} ({pct:.1f}%) | WR={stats['win_rate']:.1f}% | P&L=${stats['pnl']:.2f}")

        print(f"\n[FINAL]: ${self.capital:,.2f} ({(self.capital - INITIAL_CAPITAL) / INITIAL_CAPITAL * 100:.1f}%)")
        print(f"{'='*60}\n")


# ============================================================
# MAIN - RUN ALL APPROACHES
# ============================================================

def run_all_approaches():
    """Run all approaches and compare."""
    # Fetch data once
    gainers_by_date = fetch_daily_gainers_historical(BACKTEST_DAYS)

    if not gainers_by_date:
        print("No data found.")
        return

    results = []

    global APPROACH_BASELINE, APPROACH_TIGHT_STOPS, APPROACH_SCALE_IN
    global APPROACH_TIME_EXIT, APPROACH_VOL_FILTER, APPROACH_COMBINED

    # Test each approach
    approaches_to_test = [
        ("BASELINE", True, False, False, False, False, False),
        ("TIGHT_STOPS", False, True, False, False, False, False),
        ("TIME_EXIT", False, False, False, True, False, False),
        ("VOL_FILTER", False, False, False, False, True, False),
        ("COMBINED", False, False, False, False, False, True),
    ]

    for name, base, tight, scale, time_ex, vol_filt, combined in approaches_to_test:
        # Reset all flags
        APPROACH_BASELINE = base
        APPROACH_TIGHT_STOPS = tight
        APPROACH_SCALE_IN = scale
        APPROACH_TIME_EXIT = time_ex
        APPROACH_VOL_FILTER = vol_filt
        APPROACH_COMBINED = combined

        engine = BacktestEngine(name)
        result = engine.run_backtest(gainers_by_date)
        results.append(result)

    # Summary comparison
    print("\n" + "="*80)
    print("APPROACH COMPARISON SUMMARY")
    print("="*80)
    print(f"{'Approach':<15} {'Trades':>7} {'WR%':>7} {'PF':>7} {'Net P&L':>12} {'Avg W':>10} {'Avg L':>10} {'L/W':>6}")
    print("-"*80)

    for r in results:
        lw_ratio = r.avg_loser / r.avg_winner if r.avg_winner > 0 else 0
        print(f"{r.approach_name:<15} {r.total_trades:>7} {r.win_rate:>6.1f}% {r.profit_factor:>7.2f} "
              f"${r.net_pnl:>10,.0f} ${r.avg_winner:>9,.0f} ${r.avg_loser:>9,.0f} {lw_ratio:>5.1f}x")

    print("="*80)

    # Find best approach
    best = max(results, key=lambda x: x.profit_factor if x.profit_factor != float('inf') else 0)
    print(f"\nBEST APPROACH: {best.approach_name} (PF: {best.profit_factor:.2f})")


def main():
    """Run single approach or all approaches."""
    # Check if running comparison or single approach
    if sum([APPROACH_BASELINE, APPROACH_TIGHT_STOPS, APPROACH_SCALE_IN,
            APPROACH_TIME_EXIT, APPROACH_VOL_FILTER, APPROACH_COMBINED]) == 0:
        # No approach selected - run all
        run_all_approaches()
    else:
        # Single approach selected
        gainers_by_date = fetch_daily_gainers_historical(BACKTEST_DAYS)
        if not gainers_by_date:
            return

        if APPROACH_BASELINE:
            name = "BASELINE"
        elif APPROACH_TIGHT_STOPS:
            name = "TIGHT_STOPS"
        elif APPROACH_SCALE_IN:
            name = "SCALE_IN"
        elif APPROACH_TIME_EXIT:
            name = "TIME_EXIT"
        elif APPROACH_VOL_FILTER:
            name = "VOL_FILTER"
        else:
            name = "COMBINED"

        engine = BacktestEngine(name)
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
                    "exit_reason": t.exit_reason,
                    "pnl": round(t.pnl, 2),
                    "r_multiple": round(t.r_multiple, 2),
                    "hold_minutes": t.hold_minutes
                }
                for t in result.trades
            ])
            trades_df.to_csv(f"smallcap_momentum_backtest_{name.lower()}_trades.csv", index=False)
            print(f"Trade log saved.")


if __name__ == "__main__":
    main()
