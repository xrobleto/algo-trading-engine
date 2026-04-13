"""
VWAP Bot Backtest Framework
============================

Comprehensive backtesting for vwap_bot.py VWAP Reclaim Strategy.

Strategy: VWAP Mean Reversion
- Entry: Price stretches below VWAP by STRETCH_ATR, then reclaims VWAP
- Exit: R-based take profit and stop loss
- Filters: ADX (trend strength), RVOL (volume confirmation)
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
    # Core symbols from vwap_bot (v11: back to original best performers)
    "SPY", "QQQ", "IWM",
    "AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOGL", "TSLA", "AMD",
    # Additional high-volume stocks
    "NFLX", "COIN", "PLTR", "JPM", "GS", "BAC",
]

BACKTEST_DAYS = 252  # Full year
INITIAL_CAPITAL = 35_000  # $35k account

# --- VWAP Reclaim Signal Parameters (v10: v8 strict + expanded universe) ---
ATR_PERIOD = 14
ADX_PERIOD = 14
TREND_ADX_MAX = 25.0          # v10: Strict ADX (quality filter)
MIN_REL_VOL = 1.2             # v10: Higher RVOL (quality)
STRETCH_ATR_MULT = 1.0        # v10: 1.0x ATR stretch required
RECLAIM_ATR_MULT = 0.20       # v10: Tight reclaim threshold

# --- Signal Quality Filters (v10: strict for quality) ---
MIN_VWAP_DISPLACEMENT_PCT = 0.30   # v10: Moderate displacement
NO_TRADE_FIRST_MINUTES = 10        # v10: Skip first 10 minutes
MIN_BARS_SINCE_STRETCH = 2         # v10: 2 bars before entry
MAX_BARS_SINCE_STRETCH = 20        # v10: Don't hold stale stretches

# --- Exit Parameters (v13: BEST - v8 optimal with no BE) ---
TP_R = 1.0                    # v13: 1:1 R/R (optimal)
SL_R = 1.0                    # Stop loss in R-multiples
MIN_STOP_DISTANCE_PCT = 0.50  # v13: Balanced stop distance

# --- Break-Even Rule ---
USE_BREAK_EVEN = False        # v13: DISABLED - let trades run to TP (optimal)
BREAK_EVEN_TRIGGER_R = 0.50   # Not used
TRAILING_DISTANCE_R = 0.40    # Not used

# --- Time-Based Filters ---
USE_TIME_FILTERS = True
NO_TRADE_LAST_MINUTES = 45    # v10: No entries in last 45 mins

# --- Risk Management (v18: YOLO) ---
MAX_RISK_PER_TRADE_PCT = 0.25     # v18: 25% risk per trade
MAX_POSITIONS = 3                 # v18: 3 max concurrent positions
TRADE_CASH_PCT = 0.80             # v18: 80% of capital per position
MAX_HOLD_MINUTES = 60             # v18: Longer hold time

# --- Session Timing ---
RTH_START = (9, 30)
RTH_END = (16, 0)
LATE_CUTOFF = (15, 45)  # No new entries after 3:45 PM
EOD_CLOSE = (15, 55)    # Close all by 3:55 PM


# ============================================================
# DATA STRUCTURES
# ============================================================

class ExitReason(Enum):
    TAKE_PROFIT = "TAKE_PROFIT"
    STOP_LOSS = "STOP_LOSS"
    BREAK_EVEN = "BREAK_EVEN"
    EOD_CLOSE = "EOD_CLOSE"
    TIME_EXIT = "TIME_EXIT"


class SignalState(Enum):
    NONE = "NONE"
    STRETCHED = "STRETCHED"    # Price stretched below VWAP
    RECLAIMED = "RECLAIMED"    # Price reclaimed VWAP - ready for entry


@dataclass
class Position:
    """Tracks an open position."""
    symbol: str
    entry_time: datetime
    entry_price: float
    stop_price: float
    tp_price: float
    qty: int
    risk_per_share: float  # 1R

    # Break-even tracking
    be_activated: bool = False
    highest_price: float = 0.0

    # Exit info
    exit_price: float = 0.0
    exit_reason: Optional[ExitReason] = None
    exit_time: Optional[datetime] = None
    pnl: float = 0.0

    @property
    def is_closed(self) -> bool:
        return self.exit_reason is not None


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

    exit_reasons: Dict[str, int] = field(default_factory=dict)
    trades: List[BacktestTrade] = field(default_factory=list)


# ============================================================
# DATA FETCHING
# ============================================================

def fetch_minute_bars(symbol: str, days: int = 30) -> Optional[pd.DataFrame]:
    """Fetch minute bars from Polygon."""
    end_date = datetime.now(ET).date()
    start_date = end_date - timedelta(days=days + 10)

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
        df = df.reset_index(drop=True)

        return df

    except Exception as e:
        print(f"  {symbol}: Error fetching data: {e}")
        return None


def fetch_daily_bars(symbol: str, days: int = 30) -> Optional[pd.DataFrame]:
    """Fetch daily bars for RVOL calculation."""
    end_date = datetime.now(ET).date()
    start_date = end_date - timedelta(days=days + 30)

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


def calculate_vwap(df: pd.DataFrame) -> pd.Series:
    """Calculate VWAP (resetting daily)."""
    df = df.copy()
    df["date"] = df["timestamp"].dt.date
    df["typical_price"] = (df["high"] + df["low"] + df["close"]) / 3
    df["tp_vol"] = df["typical_price"] * df["volume"]

    vwap = df.groupby("date").apply(
        lambda x: x["tp_vol"].cumsum() / x["volume"].cumsum(),
        include_groups=False
    ).reset_index(level=0, drop=True)

    return vwap


def calculate_relative_volume(minute_df: pd.DataFrame, daily_df: pd.DataFrame) -> pd.Series:
    """Calculate intraday relative volume."""
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


# ============================================================
# SIGNAL DETECTION
# ============================================================

@dataclass
class SymbolState:
    """Tracks VWAP reclaim signal state per symbol."""
    signal_state: SignalState = SignalState.NONE
    stretch_bar_idx: int = -1
    stretch_price: float = 0.0
    stretch_atr: float = 0.0


def check_vwap_reclaim_signal(
    df: pd.DataFrame,
    idx: int,
    state: SymbolState,
    ts: datetime = None
) -> Tuple[bool, str, SymbolState]:
    """
    Check for VWAP reclaim entry signal.

    Returns: (is_valid, rejection_reason, updated_state)
    """
    if idx < max(ATR_PERIOD, ADX_PERIOD, 5):
        return False, "insufficient_data", state

    row = df.iloc[idx]
    price = row["close"]
    low = row["low"]
    high = row["high"]
    vwap = row["vwap"]
    atr = row["atr"]
    adx = row["adx"]
    rvol = row["rvol"] if "rvol" in df.columns and not pd.isna(row["rvol"]) else 1.5

    # Time filters
    if USE_TIME_FILTERS and ts is not None:
        hour, minute = ts.hour, ts.minute
        minutes_since_open = (hour - 9) * 60 + (minute - 30)
        minutes_until_close = (16 - hour) * 60 - minute

        if minutes_since_open < NO_TRADE_FIRST_MINUTES:
            return False, "too_early", state
        if minutes_until_close < NO_TRADE_LAST_MINUTES:
            return False, "too_late", state

    # ADX filter - skip strong trends
    if not pd.isna(adx) and adx > TREND_ADX_MAX:
        return False, "adx_too_high", state

    # RVOL filter
    if rvol < MIN_REL_VOL:
        return False, "low_rvol", state

    # ATR validation
    if pd.isna(atr) or atr <= 0:
        return False, "invalid_atr", state

    # Calculate stretch and reclaim thresholds
    stretch_threshold = vwap - (atr * STRETCH_ATR_MULT)
    reclaim_threshold = vwap - (atr * RECLAIM_ATR_MULT)

    # State machine for VWAP reclaim
    new_state = SymbolState(
        signal_state=state.signal_state,
        stretch_bar_idx=state.stretch_bar_idx,
        stretch_price=state.stretch_price,
        stretch_atr=state.stretch_atr
    )

    # Check for stretch (price goes below VWAP by STRETCH_ATR)
    if low <= stretch_threshold:
        new_state.signal_state = SignalState.STRETCHED
        new_state.stretch_bar_idx = idx
        new_state.stretch_price = low
        new_state.stretch_atr = atr

    # Check if stretch is too old
    if new_state.signal_state == SignalState.STRETCHED:
        bars_since_stretch = idx - new_state.stretch_bar_idx
        if bars_since_stretch > MAX_BARS_SINCE_STRETCH:
            new_state.signal_state = SignalState.NONE
            return False, "stretch_expired", new_state

    # Check for reclaim (price crosses back above reclaim threshold)
    if new_state.signal_state == SignalState.STRETCHED:
        bars_since_stretch = idx - new_state.stretch_bar_idx

        if bars_since_stretch >= MIN_BARS_SINCE_STRETCH:
            # Check if price reclaimed VWAP
            if price >= reclaim_threshold:
                # Additional filter: minimum displacement
                displacement_pct = abs(new_state.stretch_price - vwap) / vwap * 100
                if displacement_pct >= MIN_VWAP_DISPLACEMENT_PCT:
                    new_state.signal_state = SignalState.RECLAIMED
                    return True, "valid", new_state
                else:
                    return False, "displacement_too_small", new_state

    return False, "no_signal", new_state


# ============================================================
# BACKTEST ENGINE
# ============================================================

class BacktestEngine:
    """Run backtest for VWAP reclaim strategy."""

    def __init__(self):
        self.capital = INITIAL_CAPITAL
        self.positions: Dict[str, Position] = {}
        self.trades: List[BacktestTrade] = []
        self.equity_curve: List[float] = [INITIAL_CAPITAL]
        self.daily_pnl: Dict[str, float] = {}
        self.symbol_states: Dict[str, SymbolState] = {}

    def run_backtest(self, symbols: List[str]) -> BacktestResult:
        """Run backtest across all symbols."""
        print(f"\n{'='*60}")
        print("VWAP BOT BACKTEST - VWAP Reclaim Strategy")
        print(f"{'='*60}")
        print(f"Symbols: {len(symbols)}")
        print(f"Period: Last {BACKTEST_DAYS} trading days")
        print(f"Initial Capital: ${INITIAL_CAPITAL:,.0f}")
        print(f"{'='*60}\n")

        # Fetch all data
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

        # Calculate indicators
        print("\nCalculating indicators...")
        for symbol, df in all_data.items():
            df["atr"] = calculate_atr(df, ATR_PERIOD)
            df["adx"] = calculate_adx(df, ADX_PERIOD)
            df["vwap"] = calculate_vwap(df)

            if daily_data.get(symbol) is not None:
                df["rvol"] = calculate_relative_volume(df, daily_data[symbol])
            else:
                df["rvol"] = 1.5

            all_data[symbol] = df
            self.symbol_states[symbol] = SymbolState()

        # Run simulation
        print("\nRunning simulation...")
        self._run_simulation(all_data)

        # Calculate results
        result = self._calculate_results()
        self._print_results(result)

        return result

    def _run_simulation(self, all_data: Dict[str, pd.DataFrame]):
        """Run bar-by-bar simulation."""
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

            # Late day cutoff
            if (hour, minute) >= LATE_CUTOFF:
                continue

            # Check for new entries
            if len(self.positions) < MAX_POSITIONS:
                self._scan_for_entries(ts, all_data)

    def _manage_positions(self, ts: datetime, all_data: Dict[str, pd.DataFrame]):
        """Manage open positions."""
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

            # Update highest price and trailing stop
            if high > pos.highest_price:
                pos.highest_price = high

            # Check trailing stop activation and update
            if USE_BREAK_EVEN:
                profit_r = (pos.highest_price - pos.entry_price) / pos.risk_per_share
                if profit_r >= BREAK_EVEN_TRIGGER_R:
                    if not pos.be_activated:
                        pos.be_activated = True
                    # Calculate trailing stop (trail TRAILING_DISTANCE_R behind the high)
                    trail_stop = pos.highest_price - (TRAILING_DISTANCE_R * pos.risk_per_share)
                    # Only move stop up, never down
                    if trail_stop > pos.stop_price:
                        pos.stop_price = trail_stop

            # Check stop loss
            if low <= pos.stop_price:
                exit_reason = ExitReason.BREAK_EVEN if pos.be_activated else ExitReason.STOP_LOSS
                self._record_exit(pos, ts, pos.stop_price, exit_reason)
                continue

            # Check take profit
            if high >= pos.tp_price:
                self._record_exit(pos, ts, pos.tp_price, ExitReason.TAKE_PROFIT)
                continue

            # Time exit
            hold_minutes = (ts - pos.entry_time).total_seconds() / 60
            if hold_minutes >= MAX_HOLD_MINUTES:
                self._record_exit(pos, ts, close, ExitReason.TIME_EXIT)

        for symbol in positions_to_remove:
            del self.positions[symbol]

    def _scan_for_entries(self, ts: datetime, all_data: Dict[str, pd.DataFrame]):
        """Scan for new entry signals."""
        for symbol, df in all_data.items():
            if symbol in self.positions:
                continue

            current_bars = df[df["timestamp"] == ts]
            if current_bars.empty:
                continue

            idx = current_bars.index[0]
            row = df.iloc[idx]

            # Get current state
            state = self.symbol_states.get(symbol, SymbolState())

            # Check for signal
            is_valid, reason, new_state = check_vwap_reclaim_signal(df, idx, state, ts)
            self.symbol_states[symbol] = new_state

            if not is_valid:
                continue

            # Calculate position sizing and levels
            entry_price = row["close"]
            atr = row["atr"]

            if pd.isna(atr) or atr <= 0:
                continue

            # Stop loss calculation
            stop_distance = atr * SL_R
            min_stop_distance = entry_price * (MIN_STOP_DISTANCE_PCT / 100)
            stop_distance = max(stop_distance, min_stop_distance)
            stop_price = entry_price - stop_distance
            risk_per_share = stop_distance

            # Take profit
            tp_price = entry_price + (atr * TP_R)

            # Position sizing
            max_risk = self.capital * MAX_RISK_PER_TRADE_PCT
            total_qty = int(max_risk / risk_per_share)

            if total_qty <= 0:
                continue

            # Check capital limits
            position_value = total_qty * entry_price
            max_position_value = self.capital * TRADE_CASH_PCT
            if position_value > max_position_value:
                total_qty = int(max_position_value / entry_price)

            if total_qty <= 0:
                continue

            # Create position
            pos = Position(
                symbol=symbol,
                entry_time=ts,
                entry_price=entry_price,
                stop_price=stop_price,
                tp_price=tp_price,
                qty=total_qty,
                risk_per_share=risk_per_share,
                highest_price=entry_price
            )

            self.positions[symbol] = pos

            # Reset signal state after entry
            self.symbol_states[symbol] = SymbolState()

            # One entry per scan
            break

    def _record_exit(self, pos: Position, ts: datetime, exit_price: float, reason: ExitReason):
        """Record position exit."""
        pos.exit_price = exit_price
        pos.exit_reason = reason
        pos.exit_time = ts
        pos.pnl = (exit_price - pos.entry_price) * pos.qty

        r_mult = (exit_price - pos.entry_price) / pos.risk_per_share
        hold_mins = int((ts - pos.entry_time).total_seconds() / 60)

        trade = BacktestTrade(
            symbol=pos.symbol,
            entry_time=pos.entry_time,
            exit_time=ts,
            entry_price=pos.entry_price,
            exit_price=exit_price,
            qty=pos.qty,
            side="LONG",
            exit_reason=reason,
            pnl=pos.pnl,
            r_multiple=r_mult,
            hold_minutes=hold_mins
        )
        self.trades.append(trade)

        self.capital += pos.pnl
        self.equity_curve.append(self.capital)

        day_str = ts.strftime("%Y-%m-%d")
        self.daily_pnl[day_str] = self.daily_pnl.get(day_str, 0) + pos.pnl

    def _close_all_positions(self, ts: datetime, all_data: Dict[str, pd.DataFrame], reason: ExitReason):
        """Close all open positions."""
        for symbol, pos in list(self.positions.items()):
            if pos.is_closed:
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
                "exit_reason": t.exit_reason.value,
                "pnl": round(t.pnl, 2),
                "r_multiple": round(t.r_multiple, 2),
                "hold_minutes": t.hold_minutes
            }
            for t in result.trades
        ])
        trades_df.to_csv("vwap_bot_backtest_trades.csv", index=False)
        print("Trade log saved to vwap_bot_backtest_trades.csv")


if __name__ == "__main__":
    main()
