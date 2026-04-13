"""
Signal Scorer Backtest - AI Phase 2 Optimization
=================================================

This script backtests and optimizes the ML signal scoring system by:
1. Using historical gainer data from smallcap_momentum_backtest_v7
2. Calculating signal scores for each historical setup
3. Testing different FEATURE_WEIGHTS combinations
4. Testing different MIN_SIGNAL_SCORE thresholds
5. Finding optimal parameters that maximize win rate / profit factor

Usage:
    python signal_scorer_backtest.py

Output:
    - Comparison of different weight configurations
    - Optimal MIN_SIGNAL_SCORE for each weight config
    - Feature importance analysis (which features predict winners)

Version: 1.0.0
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
import itertools
import json

ET = ZoneInfo("America/New_York")
POLYGON_API_KEY = os.getenv("POLYGON_API_KEY", "eskzZ5O4QQyYpS5TsA3pe5WMeepeaxmW")

# ============================================================
# BACKTEST CONFIGURATION
# ============================================================
BACKTEST_DAYS = 60
INITIAL_CAPITAL = 35_000
MIN_PRICE = 2.00
MAX_PRICE = 25.00
MIN_PCT_CHANGE = 5.0
MIN_RELATIVE_VOLUME = 2.0
MIN_ABSOLUTE_VOLUME = 200_000
TRADING_START_MINUTES = 10
TRADING_END_HOUR = 11
EOD_CLOSE_HOUR = 15
EOD_CLOSE_MINUTE = 55

# Pattern detection (from v7)
FLAG_POLE_MIN_PCT = 3.0
FLAG_MAX_RETRACE_PCT = 50.0
FLAG_MIN_BARS = 2
FLAG_MAX_BARS = 15
ATR_PERIOD = 14

# Trade execution (BEST_MIX from v7)
ATR_MULT = 4.0
MIN_STOP_PCT = 3.5
TP1_R = 1.0
TP2_R = 2.5
TRAIL_PCT = 2.0
MAX_RISK_PER_TRADE = 150.00

# ============================================================
# SIGNAL SCORING CONFIGURATION (FROM simple_bot.py)
# ============================================================

# Default weights (what we're optimizing)
DEFAULT_FEATURE_WEIGHTS = {
    "rvol": 15,
    "vwap_distance": 15,
    "ema_separation": 10,
    "adx": 15,
    "spread": 10,
    "momentum": 10,
    "time_of_day": 10,
    "gap_pct": 15,  # Added for backtest (proxy for news sentiment)
}

# Weight configurations to test
WEIGHT_CONFIGS = {
    "BASELINE": {
        "rvol": 15, "vwap_distance": 15, "ema_separation": 10,
        "adx": 15, "spread": 10, "momentum": 10, "time_of_day": 10, "gap_pct": 15,
    },
    "RVOL_HEAVY": {
        "rvol": 25, "vwap_distance": 15, "ema_separation": 10,
        "adx": 10, "spread": 5, "momentum": 10, "time_of_day": 10, "gap_pct": 15,
    },
    "MOMENTUM_HEAVY": {
        "rvol": 15, "vwap_distance": 10, "ema_separation": 15,
        "adx": 10, "spread": 5, "momentum": 20, "time_of_day": 10, "gap_pct": 15,
    },
    "TIME_HEAVY": {
        "rvol": 15, "vwap_distance": 15, "ema_separation": 10,
        "adx": 10, "spread": 5, "momentum": 10, "time_of_day": 20, "gap_pct": 15,
    },
    "GAP_HEAVY": {
        "rvol": 15, "vwap_distance": 10, "ema_separation": 10,
        "adx": 10, "spread": 5, "momentum": 10, "time_of_day": 10, "gap_pct": 30,
    },
    "VWAP_HEAVY": {
        "rvol": 15, "vwap_distance": 25, "ema_separation": 10,
        "adx": 10, "spread": 5, "momentum": 10, "time_of_day": 10, "gap_pct": 15,
    },
    "BALANCED_V2": {
        "rvol": 20, "vwap_distance": 20, "ema_separation": 10,
        "adx": 10, "spread": 5, "momentum": 15, "time_of_day": 10, "gap_pct": 10,
    },
}

# Score thresholds to test
SCORE_THRESHOLDS = [30, 40, 45, 50, 55, 60, 65, 70, 75, 80]


# ============================================================
# DATA STRUCTURES
# ============================================================

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
class SignalFeatures:
    """Features extracted for signal scoring."""
    symbol: str
    timestamp: datetime
    rvol: float
    vwap_distance_pct: float
    ema_separation_pct: float
    spread_bps: float
    adx: Optional[float]
    momentum_5min_pct: float
    minutes_since_open: int
    gap_pct: float
    score: float = 0.0


@dataclass
class TradeSetup:
    symbol: str
    timestamp: datetime
    entry_price: float
    stop_price: float
    risk_per_share: float
    features: Optional[SignalFeatures] = None
    score: float = 0.0


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
    score: float
    features: Optional[SignalFeatures] = None


@dataclass
class BacktestResult:
    config_name: str = ""
    min_score: float = 0.0
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
    avg_score_winners: float = 0.0
    avg_score_losers: float = 0.0
    trades: List[BacktestTrade] = field(default_factory=list)


# ============================================================
# DATA FETCHING (reused from v7)
# ============================================================

def fetch_daily_gainers_historical(days: int = 90) -> Dict[str, List[DailyGainer]]:
    """Fetch historical gap-up data."""
    print("Building historical gainer database...")

    SCAN_UNIVERSE = [
        "SNDL", "TLRY", "ACB", "CGC", "CRON", "VFF",
        "WKHS", "LCID", "RIVN", "BLNK", "CHPT", "QS", "EVGO", "XPEV", "NIO",
        "MARA", "RIOT", "BITF", "CLSK", "CIFR",
        "SPCE", "RKLB", "LUNR", "RDW", "MNTS",
        "AMC", "SOFI", "HOOD",
        "PLTR", "IONQ", "QUBT", "SOUN", "BBAI",
        "CRSP", "EDIT", "NTLA", "ARCT", "NVAX", "INO",
        "PATH", "DKNG", "PENN", "FUBO", "OPEN",
        "CLF", "BKKT", "IMPP", "OPAD", "TMC", "LIDR", "MLGO",
    ]

    gainers_by_date: Dict[str, List[DailyGainer]] = defaultdict(list)
    end_date = datetime.now(ET).date()
    start_date = end_date - timedelta(days=days + 10)

    for symbol in SCAN_UNIVERSE:
        print(f"  {symbol}...", end=" ")
        try:
            url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/day/{start_date}/{end_date}"
            params = {"adjusted": "true", "sort": "asc", "limit": 200, "apiKey": POLYGON_API_KEY}
            resp = requests.get(url, params=params, timeout=15)
            if resp.status_code != 200:
                print("SKIP")
                continue
            results = resp.json().get("results", [])
            if len(results) < 25:
                print("SKIP")
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
                print(f"OK ({len(gap_days)})")
                for _, row in gap_days.iterrows():
                    gainers_by_date[str(row["date"])].append(DailyGainer(
                        symbol=symbol, date=row["date"], gap_pct=row["gap_pct"],
                        open_price=row["open"], prev_close=row["prev_close"],
                        volume=int(row["volume"]), rel_volume=row["rel_vol"]
                    ))
            else:
                print("OK (0)")
        except Exception as e:
            print(f"ERR: {e}")

    for d in gainers_by_date:
        gainers_by_date[d].sort(key=lambda x: x.gap_pct, reverse=True)

    print(f"\nFound {sum(len(v) for v in gainers_by_date.values())} gap events")
    return dict(gainers_by_date)


def fetch_minute_bars(symbol: str, date: datetime) -> Optional[pd.DataFrame]:
    """Fetch minute bars."""
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
        df = df[((df["hour"] == 9) & (df["minute"] >= 30)) | ((df["hour"] > 9) & (df["hour"] < 16))].copy()
        df = df.drop(columns=["hour", "minute"]).reset_index(drop=True)
        return df
    except:
        return None


# ============================================================
# TECHNICAL INDICATORS
# ============================================================

def calculate_atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    """Calculate ATR."""
    tr1 = df["high"] - df["low"]
    tr2 = abs(df["high"] - df["close"].shift(1))
    tr3 = abs(df["low"] - df["close"].shift(1))
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    return tr.rolling(window=period).mean()


def calculate_vwap(df: pd.DataFrame) -> pd.Series:
    """Calculate VWAP."""
    pv = (df["close"] * df["volume"]).cumsum()
    v = df["volume"].cumsum().replace(0, float("nan"))
    return pv / v


def calculate_ema(series: pd.Series, period: int) -> pd.Series:
    """Calculate EMA."""
    return series.ewm(span=period, adjust=False).mean()


def calculate_adx(df: pd.DataFrame, period: int = 14) -> pd.Series:
    """Calculate ADX."""
    if len(df) < period + 1:
        return pd.Series([None] * len(df), index=df.index)

    high = df["high"]
    low = df["low"]
    close = df["close"]

    plus_dm = high.diff()
    minus_dm = low.diff().abs()

    plus_dm = plus_dm.where((plus_dm > minus_dm) & (plus_dm > 0), 0)
    minus_dm = minus_dm.where((minus_dm > plus_dm) & (minus_dm > 0), 0)

    tr1 = high - low
    tr2 = abs(high - close.shift(1))
    tr3 = abs(low - close.shift(1))
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

    atr = tr.rolling(window=period).mean()
    plus_di = 100 * (plus_dm.rolling(window=period).mean() / atr)
    minus_di = 100 * (minus_dm.rolling(window=period).mean() / atr)

    dx = 100 * abs(plus_di - minus_di) / (plus_di + minus_di).replace(0, float("nan"))
    adx = dx.rolling(window=period).mean()

    return adx


# ============================================================
# SIGNAL SCORER
# ============================================================

class SignalScorer:
    """Score setups based on weighted features."""

    def __init__(self, weights: Dict[str, float]):
        self.weights = weights

    def extract_features(self, df: pd.DataFrame, idx: int, gainer: DailyGainer) -> SignalFeatures:
        """Extract features from market data at a specific bar."""
        ts = df["timestamp"].iloc[idx]
        current_price = df["close"].iloc[idx]

        # Calculate technical indicators
        vwap = calculate_vwap(df.iloc[:idx+1]).iloc[-1]
        ema_9 = calculate_ema(df["close"].iloc[:idx+1], 9).iloc[-1]
        ema_20 = calculate_ema(df["close"].iloc[:idx+1], 20).iloc[-1]

        # VWAP distance
        vwap_distance_pct = ((current_price - vwap) / vwap) * 100 if vwap > 0 else 0

        # EMA separation
        ema_sep_pct = ((ema_9 - ema_20) / ema_20) * 100 if ema_20 > 0 else 0

        # ADX (calculate on available data)
        adx_series = calculate_adx(df.iloc[:idx+1], 14)
        adx = adx_series.iloc[-1] if len(adx_series) > 0 and not pd.isna(adx_series.iloc[-1]) else None

        # Momentum (5-bar)
        if idx >= 5:
            price_5_ago = df["close"].iloc[idx - 5]
            momentum = ((current_price - price_5_ago) / price_5_ago) * 100
        else:
            momentum = 0

        # Minutes since open
        minutes_since_open = (ts.hour - 9) * 60 + (ts.minute - 30)

        # Spread (approximation - use high/low of current bar)
        spread_bps = ((df["high"].iloc[idx] - df["low"].iloc[idx]) / current_price) * 10000

        return SignalFeatures(
            symbol=gainer.symbol,
            timestamp=ts,
            rvol=gainer.rel_volume,
            vwap_distance_pct=vwap_distance_pct,
            ema_separation_pct=ema_sep_pct,
            spread_bps=spread_bps,
            adx=adx,
            momentum_5min_pct=momentum,
            minutes_since_open=minutes_since_open,
            gap_pct=gainer.gap_pct,
        )

    def score_features(self, features: SignalFeatures) -> float:
        """Score features using weighted scoring."""
        score = 0.0

        # RVOL scoring (higher is better, sweet spot 3-10x)
        if features.rvol >= 10:
            score += self.weights.get("rvol", 0) * 1.0
        elif features.rvol >= 5:
            score += self.weights.get("rvol", 0) * 0.9
        elif features.rvol >= 3:
            score += self.weights.get("rvol", 0) * 0.7
        elif features.rvol >= 2:
            score += self.weights.get("rvol", 0) * 0.5
        else:
            score += self.weights.get("rvol", 0) * 0.2

        # VWAP distance (above VWAP is good, sweet spot 0.5-2%)
        if 0.5 <= features.vwap_distance_pct <= 2.0:
            score += self.weights.get("vwap_distance", 0) * 1.0
        elif 0 < features.vwap_distance_pct < 0.5:
            score += self.weights.get("vwap_distance", 0) * 0.6
        elif 2.0 < features.vwap_distance_pct <= 4.0:
            score += self.weights.get("vwap_distance", 0) * 0.7
        elif features.vwap_distance_pct > 4.0:
            score += self.weights.get("vwap_distance", 0) * 0.3  # Too extended
        else:
            score += 0  # Below VWAP

        # EMA separation (positive is good, sweet spot 0.2-1%)
        if 0.2 <= features.ema_separation_pct <= 1.0:
            score += self.weights.get("ema_separation", 0) * 1.0
        elif 0 < features.ema_separation_pct < 0.2:
            score += self.weights.get("ema_separation", 0) * 0.5
        elif features.ema_separation_pct > 1.0:
            score += self.weights.get("ema_separation", 0) * 0.7
        else:
            score += 0  # EMAs crossed bearish

        # ADX (trend strength, sweet spot 15-35)
        if features.adx is not None:
            if 15 <= features.adx <= 35:
                score += self.weights.get("adx", 0) * 1.0
            elif 10 <= features.adx < 15:
                score += self.weights.get("adx", 0) * 0.5  # Weak trend
            elif 35 < features.adx <= 50:
                score += self.weights.get("adx", 0) * 0.7  # Strong but maybe overextended
            elif features.adx > 50:
                score += self.weights.get("adx", 0) * 0.3  # Too strong
            else:
                score += self.weights.get("adx", 0) * 0.2  # Choppy

        # Spread (lower is better)
        if features.spread_bps < 30:
            score += self.weights.get("spread", 0) * 1.0
        elif features.spread_bps < 50:
            score += self.weights.get("spread", 0) * 0.7
        elif features.spread_bps < 100:
            score += self.weights.get("spread", 0) * 0.4
        else:
            score += self.weights.get("spread", 0) * 0.1

        # Momentum (positive is good, sweet spot 0.3-1.5%)
        if 0.3 <= features.momentum_5min_pct <= 1.5:
            score += self.weights.get("momentum", 0) * 1.0
        elif 0.15 <= features.momentum_5min_pct < 0.3:
            score += self.weights.get("momentum", 0) * 0.6
        elif 1.5 < features.momentum_5min_pct <= 3.0:
            score += self.weights.get("momentum", 0) * 0.7
        elif features.momentum_5min_pct > 3.0:
            score += self.weights.get("momentum", 0) * 0.3  # Chasing
        else:
            score += self.weights.get("momentum", 0) * 0.2

        # Time of day (best 9:35-10:30, avoid first 5 min and after 11)
        if 5 <= features.minutes_since_open <= 60:
            score += self.weights.get("time_of_day", 0) * 1.0
        elif 60 < features.minutes_since_open <= 90:
            score += self.weights.get("time_of_day", 0) * 0.7
        elif features.minutes_since_open < 5:
            score += self.weights.get("time_of_day", 0) * 0.3  # Too early
        else:
            score += self.weights.get("time_of_day", 0) * 0.4  # Late

        # Gap % (proxy for catalyst strength, higher is better)
        if features.gap_pct >= 15:
            score += self.weights.get("gap_pct", 0) * 1.0
        elif features.gap_pct >= 10:
            score += self.weights.get("gap_pct", 0) * 0.8
        elif features.gap_pct >= 7:
            score += self.weights.get("gap_pct", 0) * 0.6
        else:
            score += self.weights.get("gap_pct", 0) * 0.4

        return score


# ============================================================
# PATTERN DETECTION
# ============================================================

def detect_bull_flag(df: pd.DataFrame, idx: int, gainer: DailyGainer,
                     scorer: SignalScorer) -> Optional[TradeSetup]:
    """Detect bull flag and score it."""
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

    _, pole_low, pole_high, pole_high_idx = best_pole
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
    if retracement > FLAG_MAX_RETRACE_PCT or flag_high > pole_high * 1.01:
        return None

    entry_price = flag_high + 0.02

    # Calculate ATR-based stop
    atr = calculate_atr(df.iloc[:idx+1], ATR_PERIOD).iloc[-1]
    if pd.notna(atr) and atr > 0:
        atr_stop = atr * ATR_MULT
        min_stop = entry_price * (MIN_STOP_PCT / 100)
        stop_distance = max(atr_stop, min_stop)
        stop_price = entry_price - stop_distance
    else:
        stop_price = flag_low - 0.02

    risk = entry_price - stop_price
    if risk <= 0 or risk > entry_price * 0.10:
        return None

    # Extract features and score
    features = scorer.extract_features(df, idx, gainer)
    score = scorer.score_features(features)
    features.score = score

    return TradeSetup(
        symbol=gainer.symbol,
        timestamp=ts,
        entry_price=entry_price,
        stop_price=stop_price,
        risk_per_share=risk,
        features=features,
        score=score
    )


# ============================================================
# BACKTEST ENGINE
# ============================================================

class BacktestEngine:
    """Backtest engine with signal scoring."""

    def __init__(self, scorer: SignalScorer, min_score: float = 0.0):
        self.scorer = scorer
        self.min_score = min_score
        self.capital = INITIAL_CAPITAL
        self.trades: List[BacktestTrade] = []

    def run(self, gainers_by_date: Dict[str, List[DailyGainer]]) -> BacktestResult:
        """Run backtest."""
        for date_str in sorted(gainers_by_date.keys()):
            gainers = gainers_by_date[date_str]
            if gainers:
                self._process_day(date_str, gainers)

        return self._calc_results()

    def _process_day(self, date_str: str, gainers: List[DailyGainer]):
        """Process one trading day."""
        gainers = sorted(gainers, key=lambda x: x.gap_pct, reverse=True)[:5]

        minute_data = {}
        for g in gainers:
            df = fetch_minute_bars(g.symbol, g.date)
            if df is not None and len(df) > 30:
                minute_data[g.symbol] = df

        if not minute_data:
            return

        # Simulate intraday trading
        for g in gainers:
            if g.symbol not in minute_data:
                continue

            df = minute_data[g.symbol]
            traded = False

            for idx in range(15, len(df)):
                if traded:
                    break

                ts = df["timestamp"].iloc[idx]
                hour, minute = ts.hour, ts.minute

                if hour == 9 and minute < 30 + TRADING_START_MINUTES:
                    continue
                if hour >= TRADING_END_HOUR:
                    break

                setup = detect_bull_flag(df, idx, g, self.scorer)

                if setup and setup.score >= self.min_score:
                    # Execute trade and simulate outcome
                    trade = self._simulate_trade(df, idx, setup)
                    if trade:
                        self.trades.append(trade)
                        traded = True

    def _simulate_trade(self, df: pd.DataFrame, entry_idx: int,
                        setup: TradeSetup) -> Optional[BacktestTrade]:
        """Simulate trade execution with tiered exits."""
        entry_time = setup.timestamp
        entry_price = setup.entry_price
        stop_price = setup.stop_price
        risk = setup.risk_per_share

        tp1_price = entry_price + risk * TP1_R
        tp2_price = entry_price + risk * TP2_R

        qty = int(MAX_RISK_PER_TRADE / risk)
        if qty <= 0:
            return None

        # Simulate bar-by-bar
        highest = entry_price
        tp1_hit = False
        tp2_hit = False
        current_stop = stop_price
        trail_active = False
        pnl = 0.0
        exit_price = None
        exit_reason = None
        exit_time = None

        for i in range(entry_idx + 1, len(df)):
            ts = df["timestamp"].iloc[i]
            high = df["high"].iloc[i]
            low = df["low"].iloc[i]
            close = df["close"].iloc[i]

            if high > highest:
                highest = high

            # EOD close
            if ts.hour >= EOD_CLOSE_HOUR and ts.minute >= EOD_CLOSE_MINUTE:
                exit_price = close
                exit_reason = "EOD_CLOSE"
                exit_time = ts
                break

            # Stop check
            if low <= current_stop:
                exit_price = current_stop
                exit_reason = "TRAILING_STOP" if trail_active else ("BREAKEVEN" if tp1_hit else "STOP_LOSS")
                exit_time = ts
                break

            # TP1
            if not tp1_hit and high >= tp1_price:
                pnl += (tp1_price - entry_price) * int(qty * 0.33)
                tp1_hit = True
                current_stop = entry_price + 0.01  # Move to breakeven
                continue

            # TP2
            if tp1_hit and not tp2_hit and high >= tp2_price:
                pnl += (tp2_price - entry_price) * int(qty * 0.33)
                tp2_hit = True
                trail_active = True
                current_stop = highest * (1 - TRAIL_PCT / 100)
                continue

            # Update trail
            if trail_active:
                new_trail = highest * (1 - TRAIL_PCT / 100)
                if new_trail > current_stop:
                    current_stop = new_trail

        if exit_price is None:
            exit_price = df["close"].iloc[-1]
            exit_reason = "EOD_CLOSE"
            exit_time = df["timestamp"].iloc[-1]

        # Calculate final PnL
        remaining_qty = qty
        if tp1_hit:
            remaining_qty -= int(qty * 0.33)
        if tp2_hit:
            remaining_qty -= int(qty * 0.33)

        pnl += (exit_price - entry_price) * remaining_qty

        r_multiple = pnl / (risk * qty) if risk * qty > 0 else 0

        return BacktestTrade(
            symbol=setup.symbol,
            date=entry_time.strftime("%Y-%m-%d"),
            entry_time=entry_time,
            exit_time=exit_time,
            entry_price=entry_price,
            exit_price=exit_price,
            qty=qty,
            exit_reason=exit_reason,
            pnl=pnl,
            r_multiple=r_multiple,
            score=setup.score,
            features=setup.features
        )

    def _calc_results(self) -> BacktestResult:
        """Calculate backtest results."""
        if not self.trades:
            return BacktestResult(min_score=self.min_score)

        winners = [t for t in self.trades if t.pnl > 0]
        losers = [t for t in self.trades if t.pnl <= 0]

        gross_profit = sum(t.pnl for t in winners)
        gross_loss = abs(sum(t.pnl for t in losers))

        return BacktestResult(
            min_score=self.min_score,
            total_trades=len(self.trades),
            winners=len(winners),
            losers=len(losers),
            win_rate=len(winners) / len(self.trades) * 100 if self.trades else 0,
            gross_profit=gross_profit,
            gross_loss=gross_loss,
            net_pnl=gross_profit - gross_loss,
            profit_factor=gross_profit / gross_loss if gross_loss > 0 else float("inf"),
            avg_winner=gross_profit / len(winners) if winners else 0,
            avg_loser=gross_loss / len(losers) if losers else 0,
            avg_r=np.mean([t.r_multiple for t in self.trades]),
            avg_score_winners=np.mean([t.score for t in winners]) if winners else 0,
            avg_score_losers=np.mean([t.score for t in losers]) if losers else 0,
            trades=self.trades
        )


# ============================================================
# OPTIMIZATION
# ============================================================

def run_optimization(gainers_by_date: Dict[str, List[DailyGainer]]) -> Dict[str, List[BacktestResult]]:
    """Run optimization across all weight configs and score thresholds."""
    results = {}

    for config_name, weights in WEIGHT_CONFIGS.items():
        print(f"\n{'='*60}")
        print(f"Testing: {config_name}")
        print(f"Weights: {weights}")
        print(f"{'='*60}")

        config_results = []
        scorer = SignalScorer(weights)

        for min_score in SCORE_THRESHOLDS:
            print(f"  Min Score {min_score}...", end=" ")
            engine = BacktestEngine(scorer, min_score)
            result = engine.run(gainers_by_date)
            result.config_name = config_name
            config_results.append(result)
            print(f"Trades: {result.total_trades} | WR: {result.win_rate:.1f}% | PF: {result.profit_factor:.2f}")

        results[config_name] = config_results

    return results


def analyze_feature_importance(all_trades: List[BacktestTrade]) -> Dict[str, float]:
    """Analyze which features best predict winners."""
    if not all_trades:
        return {}

    winners = [t for t in all_trades if t.pnl > 0 and t.features]
    losers = [t for t in all_trades if t.pnl <= 0 and t.features]

    if not winners or not losers:
        return {}

    importance = {}

    # Calculate mean difference for each feature
    feature_names = ["rvol", "vwap_distance_pct", "ema_separation_pct", "momentum_5min_pct", "gap_pct"]

    for feat in feature_names:
        winner_mean = np.mean([getattr(t.features, feat) for t in winners])
        loser_mean = np.mean([getattr(t.features, feat) for t in losers])
        # Importance = how much winners differ from losers (as % of combined mean)
        combined_mean = (winner_mean + loser_mean) / 2
        if combined_mean != 0:
            importance[feat] = ((winner_mean - loser_mean) / abs(combined_mean)) * 100
        else:
            importance[feat] = 0

    return importance


def print_optimization_report(results: Dict[str, List[BacktestResult]]):
    """Print optimization report."""
    print("\n" + "="*80)
    print("OPTIMIZATION RESULTS")
    print("="*80)

    # Find best config for each metric
    best_wr = ("", 0, 0)
    best_pf = ("", 0, 0)
    best_net = ("", 0, 0)

    for config_name, config_results in results.items():
        for r in config_results:
            if r.total_trades >= 10:  # Minimum trades for significance
                if r.win_rate > best_wr[1]:
                    best_wr = (config_name, r.win_rate, r.min_score)
                if r.profit_factor > best_pf[1]:
                    best_pf = (config_name, r.profit_factor, r.min_score)
                if r.net_pnl > best_net[1]:
                    best_net = (config_name, r.net_pnl, r.min_score)

    print(f"\nBest Win Rate: {best_wr[0]} @ score>={best_wr[2]} -> {best_wr[1]:.1f}%")
    print(f"Best Profit Factor: {best_pf[0]} @ score>={best_pf[2]} -> {best_pf[1]:.2f}")
    print(f"Best Net P&L: {best_net[0]} @ score>={best_net[2]} -> ${best_net[1]:.2f}")

    # Print detailed results for each config
    print("\n" + "-"*80)
    print("DETAILED RESULTS BY CONFIGURATION")
    print("-"*80)

    for config_name, config_results in results.items():
        print(f"\n{config_name}:")
        print(f"{'Score':>6} | {'Trades':>6} | {'WR%':>6} | {'PF':>6} | {'Net P&L':>10} | {'Avg Win':>8} | {'Avg Loss':>8}")
        print("-" * 70)

        for r in config_results:
            print(f"{r.min_score:>6} | {r.total_trades:>6} | {r.win_rate:>5.1f}% | {r.profit_factor:>6.2f} | "
                  f"${r.net_pnl:>9.2f} | ${r.avg_winner:>7.2f} | ${r.avg_loser:>7.2f}")

    # Feature importance (using all trades from baseline)
    if "BASELINE" in results and results["BASELINE"]:
        all_trades = []
        for r in results["BASELINE"]:
            all_trades.extend(r.trades)

        importance = analyze_feature_importance(all_trades)
        if importance:
            print("\n" + "-"*80)
            print("FEATURE IMPORTANCE (Winner vs Loser Difference %)")
            print("-"*80)
            for feat, imp in sorted(importance.items(), key=lambda x: abs(x[1]), reverse=True):
                direction = "+" if imp > 0 else ""
                print(f"  {feat:25s}: {direction}{imp:.1f}%")


def save_results_to_json(results: Dict[str, List[BacktestResult]], filename: str = "scorer_optimization_results.json"):
    """Save results to JSON for further analysis."""
    output = {}

    for config_name, config_results in results.items():
        output[config_name] = []
        for r in config_results:
            output[config_name].append({
                "min_score": r.min_score,
                "total_trades": r.total_trades,
                "win_rate": r.win_rate,
                "profit_factor": r.profit_factor,
                "net_pnl": r.net_pnl,
                "avg_winner": r.avg_winner,
                "avg_loser": r.avg_loser,
                "avg_r": r.avg_r,
                "avg_score_winners": r.avg_score_winners,
                "avg_score_losers": r.avg_score_losers,
            })

    with open(filename, "w") as f:
        json.dump(output, f, indent=2)

    print(f"\nResults saved to {filename}")


# ============================================================
# MAIN
# ============================================================

def main():
    print("="*80)
    print("SIGNAL SCORER BACKTEST - AI Phase 2 Optimization")
    print("="*80)

    # Fetch historical data
    gainers_by_date = fetch_daily_gainers_historical(BACKTEST_DAYS)

    if not gainers_by_date:
        print("No data found.")
        return

    # Run optimization
    results = run_optimization(gainers_by_date)

    # Print report
    print_optimization_report(results)

    # Save results
    save_results_to_json(results)


if __name__ == "__main__":
    main()
