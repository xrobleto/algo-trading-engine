"""
Master Bot Backtest - Multi-Strategy Backtesting Framework

Tests both strategies from master_bot.py:
1. Momentum Breakout (gap-up plays with volume confirmation)
2. VWAP Scalping (VWAP reclaim setups)

Usage:
    python master_bot_backtest.py --start 2024-11-01 --end 2024-12-31 --equity 100000
"""

from __future__ import annotations

import os
import math
import argparse
import datetime as dt
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple
from enum import Enum

import pandas as pd
import requests
from zoneinfo import ZoneInfo

# ============================================================
# CONFIGURATION
# ============================================================

ET = ZoneInfo("America/New_York")

# Polygon API
POLYGON_API_KEY = os.getenv("POLYGON_API_KEY", "").strip()
POLYGON_REST_BASE = "https://api.polygon.io"

# Universe (v2: removed underperformers RIOT, MARA, DDOG, NET, AMD based on backtest)
BACKTEST_SYMBOLS = [
    # High-volume (VWAP scalping) - kept proven performers
    "AAPL", "MSFT", "NVDA", "TSLA", "META", "GOOGL", "AMZN", "SPY", "QQQ",
    # Momentum candidates - removed volatile miners (RIOT, MARA)
    "COIN", "PLTR", "SNOW", "CRWD",
    # Added stable alternatives
    "JPM", "GS", "UNH",
]

# Session times
RTH_OPEN_HOUR = 9
RTH_OPEN_MIN = 30
RTH_CLOSE_HOUR = 16
RTH_CLOSE_MIN = 0

# ============================================================
# STRATEGY 1: MOMENTUM BREAKOUT PARAMETERS (v2: tightened filters)
# ============================================================
MOMENTUM_ENABLED = True
MOMENTUM_MIN_GAP_PCT = 4.0         # v2: Increased from 3.0% - higher quality gaps only
MOMENTUM_MIN_VOLUME_RATIO = 3.0    # v2: Increased from 2.0 - stronger confirmation
MOMENTUM_MIN_PRICE = 10.0          # v2: Increased from 3.0 - avoid penny stocks
MOMENTUM_MAX_PRICE = 200.0         # v2: Increased from 100 - allow higher priced stocks
MOMENTUM_ATR_STOP_MULT = 1.5       # v2: Tightened from 2.0 - reduce loss per trade
MOMENTUM_PROFIT_TARGET_R = 2.0     # Keep 2R target
MOMENTUM_ATR_LEN = 14

# v2: Time filter for momentum - skip first 30 minutes
MOMENTUM_SKIP_FIRST_MINUTES = 30

# ============================================================
# STRATEGY 2: VWAP SCALPING PARAMETERS (v2: balanced for better SL/TP ratio)
# ============================================================
VWAP_ENABLED = True
VWAP_MIN_PRICE = 30.0             # v2: Increased from 20 - better quality
VWAP_MAX_PRICE = 500.0
VWAP_MAX_DISTANCE_PCT = 0.20      # v2: Tighter from 0.25% - closer to VWAP
VWAP_PROFIT_TARGET_PCT = 0.50     # v2: Reduced from 0.65% - hit TP more often
VWAP_STOP_LOSS_PCT = 0.40         # v2: Tightened from 0.50% - cut losses quicker
VWAP_MIN_VOLUME_RATIO = 2.5       # v2: Increased from 2.0 - stronger signals
VWAP_COOLDOWN_BARS = 15           # v2: Increased from 10 - less overtrading
VWAP_MAX_ADX = 20.0               # v2: Stricter from 23 - only range-bound markets

# v2: Time filter for VWAP - skip first 30 minutes (open volatility hurts)
VWAP_SKIP_FIRST_MINUTES = 30

# ============================================================
# RISK MANAGEMENT
# ============================================================
MAX_CONCURRENT_POSITIONS = 8
POSITION_SIZE_PCT = 0.10          # 10% per position
MAX_HOLD_MINUTES = 180            # Extended to 3 hours (was 120)
USE_TRAILING_STOP = True          # Enable trailing stop
TRAILING_STOP_ACTIVATION = 0.30   # Activate after 0.30% profit
TRAILING_STOP_DISTANCE = 0.25     # Trail by 0.25%
BREAKEVEN_ACTIVATION = 0.0        # Disabled - hurts win rate

# ============================================================
# DATA CLASSES
# ============================================================

class StrategyType(Enum):
    MOMENTUM = "momentum"
    VWAP = "vwap"

@dataclass
class Position:
    sym: str
    strategy: StrategyType
    side: str
    qty: int
    entry_price: float
    entry_time: dt.datetime
    stop_price: float
    tp_price: float
    r_value: float  # Dollar risk per share
    trailing_active: bool = False
    highest_price: float = 0.0  # Track highest price for trailing stop

@dataclass
class BacktestState:
    equity: float
    cash: float
    positions: Dict[str, Position] = field(default_factory=dict)
    trades: List[Dict] = field(default_factory=list)
    signals_generated: int = 0
    signals_taken: int = 0

# ============================================================
# POLYGON CLIENT
# ============================================================

class PolygonClient:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = POLYGON_REST_BASE

    def get_agg_1m(self, symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
        """Fetch 1-minute aggregates from Polygon."""
        url = f"{self.base_url}/v2/aggs/ticker/{symbol}/range/1/minute/{start_date}/{end_date}"
        params = {"apiKey": self.api_key, "limit": 50000, "sort": "asc"}

        try:
            resp = requests.get(url, params=params, timeout=30)
            data = resp.json()

            if data.get("status") != "OK" or "results" not in data:
                return pd.DataFrame()

            df = pd.DataFrame(data["results"])
            df["timestamp"] = pd.to_datetime(df["t"], unit="ms", utc=True).dt.tz_convert(ET)
            df = df.rename(columns={"o": "open", "h": "high", "l": "low", "c": "close", "v": "volume"})
            df = df.set_index("timestamp")
            return df[["open", "high", "low", "close", "volume"]]

        except Exception as e:
            print(f"[ERROR] Failed to fetch {symbol}: {e}")
            return pd.DataFrame()

    def get_daily_bars(self, symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
        """Fetch daily bars for gap calculation."""
        url = f"{self.base_url}/v2/aggs/ticker/{symbol}/range/1/day/{start_date}/{end_date}"
        params = {"apiKey": self.api_key, "limit": 100, "sort": "asc"}

        try:
            resp = requests.get(url, params=params, timeout=30)
            data = resp.json()

            if data.get("status") != "OK" or "results" not in data:
                return pd.DataFrame()

            df = pd.DataFrame(data["results"])
            df["date"] = pd.to_datetime(df["t"], unit="ms", utc=True).dt.date
            df = df.rename(columns={"o": "open", "h": "high", "l": "low", "c": "close", "v": "volume"})
            return df

        except Exception:
            return pd.DataFrame()

# ============================================================
# TECHNICAL INDICATORS
# ============================================================

def calc_atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    """Calculate Average True Range."""
    high = df["high"]
    low = df["low"]
    close = df["close"]

    tr1 = high - low
    tr2 = abs(high - close.shift(1))
    tr3 = abs(low - close.shift(1))

    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    return tr.rolling(period).mean()

def calc_vwap(df: pd.DataFrame) -> pd.Series:
    """Calculate cumulative VWAP from start of data."""
    typical_price = (df["high"] + df["low"] + df["close"]) / 3
    cum_vol = df["volume"].cumsum()
    cum_vwap = (typical_price * df["volume"]).cumsum()
    return cum_vwap / cum_vol

def calc_volume_ratio(df: pd.DataFrame, lookback: int = 20) -> float:
    """Calculate current volume vs average."""
    if len(df) < lookback:
        return 0.0
    avg_vol = df["volume"].iloc[-lookback:].mean()
    current_vol = df["volume"].iloc[-1]
    return current_vol / avg_vol if avg_vol > 0 else 0.0

def calc_adx(df: pd.DataFrame, period: int = 14) -> pd.Series:
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

# ============================================================
# BACKTEST ENGINE
# ============================================================

class MasterBotBacktest:
    def __init__(self, start_equity: float, symbols: List[str]):
        self.state = BacktestState(equity=start_equity, cash=start_equity)
        self.symbols = symbols
        self.polygon = PolygonClient(POLYGON_API_KEY)
        self.data: Dict[str, pd.DataFrame] = {}
        self.daily_data: Dict[str, pd.DataFrame] = {}

    def load_data(self, start_date: str, end_date: str):
        """Load historical data for all symbols."""
        print(f"[DATA] Loading data from {start_date} to {end_date}...")

        # Extend start date for lookback
        start_dt = dt.datetime.strptime(start_date, "%Y-%m-%d")
        extended_start = (start_dt - dt.timedelta(days=30)).strftime("%Y-%m-%d")

        for sym in self.symbols:
            print(f"[DATA] Fetching {sym}...")

            # 1-minute data
            df = self.polygon.get_agg_1m(sym, extended_start, end_date)
            if not df.empty:
                self.data[sym] = df
                print(f"[DATA] {sym}: {len(df)} bars loaded")

            # Daily data for gap calculation
            daily = self.polygon.get_daily_bars(sym, extended_start, end_date)
            if not daily.empty:
                self.daily_data[sym] = daily

    def is_rth(self, ts: dt.datetime) -> bool:
        """Check if timestamp is during RTH."""
        if ts.weekday() >= 5:
            return False
        rth_open = ts.replace(hour=RTH_OPEN_HOUR, minute=RTH_OPEN_MIN, second=0, microsecond=0)
        rth_close = ts.replace(hour=RTH_CLOSE_HOUR, minute=RTH_CLOSE_MIN, second=0, microsecond=0)
        return rth_open <= ts < rth_close

    def get_gap_pct(self, sym: str, date: dt.date) -> Optional[float]:
        """Calculate gap percentage for a given date."""
        if sym not in self.daily_data:
            return None

        daily = self.daily_data[sym]
        date_rows = daily[daily["date"] == date]
        if date_rows.empty:
            return None

        # Find previous day's close
        prev_rows = daily[daily["date"] < date]
        if prev_rows.empty:
            return None

        prev_close = prev_rows.iloc[-1]["close"]
        today_open = date_rows.iloc[0]["open"]

        return ((today_open - prev_close) / prev_close) * 100

    def check_momentum_signal(self, sym: str, df_slice: pd.DataFrame, date: dt.date, ts: dt.datetime) -> Optional[Dict]:
        """Check for momentum breakout signal."""
        if not MOMENTUM_ENABLED:
            return None

        if len(df_slice) < MOMENTUM_ATR_LEN + 5:
            return None

        # v2: Time filter - skip first N minutes
        market_open = ts.replace(hour=RTH_OPEN_HOUR, minute=RTH_OPEN_MIN, second=0, microsecond=0)
        minutes_since_open = (ts - market_open).total_seconds() / 60
        if minutes_since_open < MOMENTUM_SKIP_FIRST_MINUTES:
            return None

        # Get current price
        px = float(df_slice["close"].iloc[-1])
        if not (MOMENTUM_MIN_PRICE <= px <= MOMENTUM_MAX_PRICE):
            return None

        # Check gap
        gap_pct = self.get_gap_pct(sym, date)
        if gap_pct is None or gap_pct < MOMENTUM_MIN_GAP_PCT:
            return None

        # Check volume ratio
        vol_ratio = calc_volume_ratio(df_slice, 20)
        if vol_ratio < MOMENTUM_MIN_VOLUME_RATIO:
            return None

        # Calculate ATR
        df_slice = df_slice.copy()
        df_slice["atr"] = calc_atr(df_slice, MOMENTUM_ATR_LEN)
        atr = float(df_slice["atr"].iloc[-1])
        if pd.isna(atr) or atr <= 0:
            return None

        # Calculate stop and target
        stop = px - (atr * MOMENTUM_ATR_STOP_MULT)
        target = px + (atr * MOMENTUM_ATR_STOP_MULT * MOMENTUM_PROFIT_TARGET_R)

        return {
            "symbol": sym,
            "strategy": StrategyType.MOMENTUM,
            "price": px,
            "gap_pct": gap_pct,
            "volume_ratio": vol_ratio,
            "atr": atr,
            "stop": stop,
            "target": target,
            "r_value": atr * MOMENTUM_ATR_STOP_MULT,
        }

    def check_vwap_signal(self, sym: str, df_slice: pd.DataFrame, anchor: dt.datetime, ts: dt.datetime) -> Optional[Dict]:
        """Check for VWAP reclaim signal."""
        if not VWAP_ENABLED:
            return None

        if len(df_slice) < 30:
            return None

        # v2: Time filter - skip first N minutes (open volatility hurts)
        minutes_since_open = (ts - anchor).total_seconds() / 60
        if minutes_since_open < VWAP_SKIP_FIRST_MINUTES:
            return None

        # Get current price
        px = float(df_slice["close"].iloc[-1])
        if not (VWAP_MIN_PRICE <= px <= VWAP_MAX_PRICE):
            return None

        # Calculate VWAP from anchor (market open)
        vwap_slice = df_slice[df_slice.index >= anchor].copy()
        if len(vwap_slice) < 10:
            return None

        vwap_slice["vwap"] = calc_vwap(vwap_slice)
        vwap = float(vwap_slice["vwap"].iloc[-1])

        if pd.isna(vwap) or vwap <= 0:
            return None

        # Check distance from VWAP
        distance_pct = ((px - vwap) / vwap) * 100

        # Look for reclaim: price slightly above VWAP (0 to max_distance)
        if distance_pct < 0 or distance_pct > VWAP_MAX_DISTANCE_PCT:
            return None

        # STRONGER RECLAIM CONFIRMATION
        # 1. Previous bar low was below VWAP (touched/undercut)
        if len(vwap_slice) >= 2:
            prev_low = float(vwap_slice["low"].iloc[-2])
            if prev_low >= vwap:
                return None  # Didn't actually touch VWAP from below

        # 2. Current bar close must be above VWAP (confirmed reclaim)
        curr_close = float(vwap_slice["close"].iloc[-1])
        if curr_close <= vwap:
            return None  # Hasn't confirmed reclaim yet

        # Check volume
        vol_ratio = calc_volume_ratio(df_slice, 20)
        if vol_ratio < VWAP_MIN_VOLUME_RATIO:
            return None

        # ADX filter - avoid trending markets (VWAP mean reversion works in ranges)
        df_slice = df_slice.copy()
        df_slice["adx"] = calc_adx(df_slice, 14)
        adx = float(df_slice["adx"].iloc[-1])
        if pd.notna(adx) and adx > VWAP_MAX_ADX:
            return None  # ADX too high = trending, skip

        # Calculate stop and target
        stop = px * (1 - VWAP_STOP_LOSS_PCT / 100)
        target = px * (1 + VWAP_PROFIT_TARGET_PCT / 100)
        r_value = px * VWAP_STOP_LOSS_PCT / 100

        return {
            "symbol": sym,
            "strategy": StrategyType.VWAP,
            "price": px,
            "vwap": vwap,
            "distance_pct": distance_pct,
            "volume_ratio": vol_ratio,
            "stop": stop,
            "target": target,
            "r_value": r_value,
        }

    def simulate_fill(self, signal: Dict, ts: dt.datetime):
        """Simulate order fill and create position."""
        sym = signal["symbol"]
        px = signal["price"]

        # Calculate position size
        risk_per_trade = self.state.equity * POSITION_SIZE_PCT
        qty = int(risk_per_trade // px)
        if qty <= 0:
            return

        position = Position(
            sym=sym,
            strategy=signal["strategy"],
            side="BUY",
            qty=qty,
            entry_price=px,
            entry_time=ts,
            stop_price=signal["stop"],
            tp_price=signal["target"],
            r_value=signal["r_value"],
        )
        self.state.positions[sym] = position
        self.state.cash -= qty * px
        self.state.signals_taken += 1

    def manage_positions(self, ts: dt.datetime, prices: Dict[str, float]):
        """Check stops, targets, trailing stops, and time exits."""
        closed = []

        for sym, pos in list(self.state.positions.items()):
            if sym not in prices:
                continue

            px = prices[sym]
            pnl = 0.0
            exit_reason = ""

            # Update trailing stop logic (for VWAP strategy)
            if USE_TRAILING_STOP and pos.strategy == StrategyType.VWAP:
                profit_pct = (px - pos.entry_price) / pos.entry_price * 100

                # Activate trailing stop once in profit
                if profit_pct >= TRAILING_STOP_ACTIVATION and not pos.trailing_active:
                    pos.trailing_active = True
                    pos.highest_price = px

                # Update trailing stop
                if pos.trailing_active:
                    if px > pos.highest_price:
                        pos.highest_price = px
                        # Move stop up (trail at TRAILING_STOP_DISTANCE % below highest)
                        new_stop = px * (1 - TRAILING_STOP_DISTANCE / 100)
                        if new_stop > pos.stop_price:
                            pos.stop_price = new_stop

            # Check stop loss (including trailing stop)
            if px <= pos.stop_price:
                pnl = (px - pos.entry_price) * pos.qty
                exit_reason = "TSL" if pos.trailing_active else "SL"

            # Check take profit
            elif px >= pos.tp_price:
                pnl = (px - pos.entry_price) * pos.qty
                exit_reason = "TP"

            # Check time stop
            elif (ts - pos.entry_time).total_seconds() / 60 > MAX_HOLD_MINUTES:
                pnl = (px - pos.entry_price) * pos.qty
                exit_reason = "TIME"

            if exit_reason:
                self.state.cash += pos.qty * px
                self.state.equity += pnl

                # Calculate R-multiple
                r_mult = (px - pos.entry_price) / pos.r_value if pos.r_value > 0 else 0

                self.state.trades.append({
                    "sym": sym,
                    "strategy": pos.strategy.value,
                    "entry_price": pos.entry_price,
                    "exit_price": px,
                    "qty": pos.qty,
                    "pnl": pnl,
                    "r_mult": r_mult,
                    "exit_reason": exit_reason,
                    "entry_time": pos.entry_time.isoformat(),
                    "exit_time": ts.isoformat(),
                })
                closed.append(sym)

        for sym in closed:
            del self.state.positions[sym]

    def run(self, start_date: str):
        """Run the backtest simulation."""
        if not self.data:
            print("[ERROR] No data loaded")
            return

        # Get all unique timestamps
        all_timestamps = set()
        for df in self.data.values():
            all_timestamps.update(df.index.tolist())
        all_timestamps = sorted(all_timestamps)

        # Filter to start date
        start_dt = dt.datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=ET)
        all_timestamps = [ts for ts in all_timestamps if ts >= start_dt]

        print(f"[BACKTEST] Running simulation over {len(all_timestamps)} bars...")
        print(f"[BACKTEST] Date range: {all_timestamps[0]} to {all_timestamps[-1]}")

        last_date = None
        momentum_signal_today = {}  # Track momentum signals per symbol per day
        vwap_last_trade = {}  # Track last VWAP trade bar per symbol {sym: bar_index}
        bar_index = 0

        for ts in all_timestamps:
            current_date = ts.date()

            # Daily reset
            if current_date != last_date:
                last_date = current_date
                momentum_signal_today = {}  # Reset daily momentum tracking
                vwap_last_trade = {}  # Reset VWAP cooldowns daily
                bar_index = 0

            # Skip non-RTH
            if not self.is_rth(ts):
                continue

            bar_index += 1  # Track bar count for cooldowns

            # Get current prices
            prices = {}
            for sym, df in self.data.items():
                if ts in df.index:
                    prices[sym] = float(df.loc[ts, "close"])

            # Manage existing positions
            self.manage_positions(ts, prices)

            # Skip if at max positions
            if len(self.state.positions) >= MAX_CONCURRENT_POSITIONS:
                continue

            # Get market open anchor for VWAP
            anchor = ts.replace(hour=RTH_OPEN_HOUR, minute=RTH_OPEN_MIN, second=0, microsecond=0)

            # Scan for signals
            for sym in self.symbols:
                if sym in self.state.positions:
                    continue

                if sym not in self.data:
                    continue

                df = self.data[sym]
                df_slice = df[df.index <= ts].tail(300)

                if df_slice.empty:
                    continue

                # Check momentum (only once per day per symbol)
                if sym not in momentum_signal_today:
                    sig = self.check_momentum_signal(sym, df_slice, current_date, ts)
                    if sig:
                        self.state.signals_generated += 1
                        momentum_signal_today[sym] = True
                        if len(self.state.positions) < MAX_CONCURRENT_POSITIONS:
                            self.simulate_fill(sig, ts)
                            continue

                # Check VWAP (with cooldown between trades)
                last_vwap_bar = vwap_last_trade.get(sym, -999)
                if bar_index - last_vwap_bar >= VWAP_COOLDOWN_BARS:
                    sig = self.check_vwap_signal(sym, df_slice, anchor, ts)
                    if sig:
                        self.state.signals_generated += 1
                        if len(self.state.positions) < MAX_CONCURRENT_POSITIONS:
                            self.simulate_fill(sig, ts)
                            vwap_last_trade[sym] = bar_index

        # Close any remaining positions at last price
        if self.state.positions:
            for sym, pos in list(self.state.positions.items()):
                if sym in prices:
                    px = prices[sym]
                    pnl = (px - pos.entry_price) * pos.qty
                    self.state.equity += pnl
                    self.state.trades.append({
                        "sym": sym,
                        "strategy": pos.strategy.value,
                        "entry_price": pos.entry_price,
                        "exit_price": px,
                        "qty": pos.qty,
                        "pnl": pnl,
                        "r_mult": (px - pos.entry_price) / pos.r_value if pos.r_value > 0 else 0,
                        "exit_reason": "EOD",
                        "entry_time": pos.entry_time.isoformat(),
                        "exit_time": str(ts),
                    })
            self.state.positions.clear()

        print("[BACKTEST] Simulation complete!")

    def report(self):
        """Generate backtest report."""
        print("\n" + "=" * 60)
        print("MASTER BOT BACKTEST RESULTS")
        print("=" * 60)

        trades = self.state.trades
        if not trades:
            print("No trades executed")
            return

        # Overall metrics
        total_pnl = sum(t["pnl"] for t in trades)
        wins = [t for t in trades if t["pnl"] > 0]
        losses = [t for t in trades if t["pnl"] < 0]

        win_rate = len(wins) / len(trades) * 100 if trades else 0
        avg_win = sum(t["pnl"] for t in wins) / len(wins) if wins else 0
        avg_loss = sum(t["pnl"] for t in losses) / len(losses) if losses else 0
        profit_factor = abs(sum(t["pnl"] for t in wins) / sum(t["pnl"] for t in losses)) if losses and sum(t["pnl"] for t in losses) != 0 else float('inf')

        print(f"\nTotal P&L: ${total_pnl:,.2f}")
        print(f"Total Trades: {len(trades)}")
        print(f"Win Rate: {win_rate:.1f}%")
        print(f"Wins: {len(wins)}, Losses: {len(losses)}")
        print(f"Avg Win: ${avg_win:.2f}")
        print(f"Avg Loss: ${avg_loss:.2f}")
        print(f"Profit Factor: {profit_factor:.2f}")

        # R-multiple analysis
        total_r = sum(t["r_mult"] for t in trades)
        avg_r = total_r / len(trades) if trades else 0
        print(f"\nTotal R: {total_r:.2f}R")
        print(f"Avg R/Trade: {avg_r:.2f}R")

        # By strategy
        print("\n--- By Strategy ---")
        for strat in [StrategyType.MOMENTUM, StrategyType.VWAP]:
            strat_trades = [t for t in trades if t["strategy"] == strat.value]
            if strat_trades:
                strat_wins = len([t for t in strat_trades if t["pnl"] > 0])
                strat_wr = strat_wins / len(strat_trades) * 100
                strat_pnl = sum(t["pnl"] for t in strat_trades)
                print(f"{strat.value.upper()}: {len(strat_trades)} trades, {strat_wr:.1f}% WR, ${strat_pnl:.2f} P&L")

        # Exit breakdown
        print("\n--- Exit Breakdown ---")
        for reason in ["TP", "SL", "TSL", "TIME", "EOD"]:
            count = len([t for t in trades if t["exit_reason"] == reason])
            if count > 0:
                pct = count / len(trades) * 100
                subset = [t for t in trades if t["exit_reason"] == reason]
                avg_pnl = sum(t["pnl"] for t in subset) / len(subset)
                print(f"  {reason}: {count} ({pct:.1f}%) avg P&L: ${avg_pnl:.2f}")

        # Save trades to CSV
        df = pd.DataFrame(trades)
        df.to_csv("master_bot_trades.csv", index=False)
        print(f"\n[REPORT] Trade log saved to master_bot_trades.csv")

# ============================================================
# MAIN
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="Master Bot Backtest")
    parser.add_argument("--start", required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", required=True, help="End date (YYYY-MM-DD)")
    parser.add_argument("--equity", type=float, default=100000, help="Starting equity")
    parser.add_argument("--api-key", help="Polygon API key")
    parser.add_argument("--symbols", help="Comma-separated symbols (overrides default)")

    args = parser.parse_args()

    global POLYGON_API_KEY
    if args.api_key:
        POLYGON_API_KEY = args.api_key

    symbols = args.symbols.split(",") if args.symbols else BACKTEST_SYMBOLS

    print("=" * 60)
    print("MASTER BOT BACKTEST")
    print("=" * 60)
    print(f"Period: {args.start} to {args.end}")
    print(f"Equity: ${args.equity:,.2f}")
    print(f"Symbols: {', '.join(symbols[:10])}{'...' if len(symbols) > 10 else ''}")
    print("=" * 60)

    engine = MasterBotBacktest(start_equity=args.equity, symbols=symbols)
    engine.load_data(args.start, args.end)
    engine.run(args.start)
    engine.report()

if __name__ == "__main__":
    main()
