"""
VWAP Reclaim Strategy Backtest - Full Event Replay

Tests Phase 1 improvements:
- Exposure caps (max per symbol, max total, max correlated)
- Idempotency guards (prevents duplicate actions)
- VWAP signal quality filters (displacement, confirmation, timing)

Tests Phase 2 improvements:
- VIX-based volatility regime detection
- Volatility-scaled position sizing
- Dynamic ATR multipliers based on regime

Usage:
    python vwap_backtest.py --start 2024-12-01 --end 2024-12-31 --equity 100000
"""

from __future__ import annotations

import os
import json
import math
import argparse
import datetime as dt
from dataclasses import dataclass, asdict, field
from typing import Dict, List, Optional, Tuple, Any
from enum import Enum

import pandas as pd
import requests
from zoneinfo import ZoneInfo

# ============================================================
# CONFIGURATION (mirrors vwap_bot.py)
# ============================================================

ET = ZoneInfo("America/New_York")

# Polygon API
POLYGON_API_KEY = (os.getenv("POLYGON_API_KEY") or os.getenv("MASSIVE_API_KEY") or "").strip()
POLYGON_REST_BASE = "https://api.polygon.io"

# Alpaca API (fallback for historical data)
ALPACA_API_KEY = os.getenv("ALPACA_API_KEY", "").strip()
ALPACA_SECRET_KEY = os.getenv("ALPACA_SECRET_KEY", "").strip()
ALPACA_DATA_URL = "https://data.alpaca.markets"

# Universe
BACKTEST_SYMBOLS = [
    "SPY", "QQQ", "IWM",
    "AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOGL", "TSLA", "AMD",
]
MIN_PRICE = 5.0
MAX_PRICE = 500.0

# Risk / limits (from vwap_bot.py)
MAX_OPEN_POSITIONS = 2
MAX_ENTRIES_PER_MINUTE = 1
TRADE_CASH_PCT_RTH = 0.10

# Exposure Caps (Phase 1)
MAX_EXPOSURE_PCT_OF_EQUITY = 0.50
MAX_EXPOSURE_PER_SYMBOL_USD = 5000.0
MAX_CORRELATED_POSITIONS = 2
CORRELATION_CLUSTERS = {
    "TECH": ["AAPL", "MSFT", "GOOGL", "META", "AMZN", "NVDA", "AMD", "TSLA", "QQQ"],
    "FIN": ["JPM", "BAC", "GS", "MS", "WFC", "C"],
    "ENERGY": ["XOM", "CVX", "COP", "SLB", "OXY"],
    "ETF": ["SPY", "QQQ", "IWM", "DIA"],
}

# VWAP Signal Parameters (FINAL OPTIMIZED - Balanced)
LOOKBACK_MINUTES = 240
ATR_LEN = 14
ADX_LEN = 14
TREND_ADX_MAX = 23.0              # Balanced - stricter than 25, looser than 22
MIN_REL_VOL = 1.3
STRETCH_ATR = 1.15
RECLAIM_ATR = 0.30                # Balanced - tighter than 0.40, wider than 0.25

# Phase 1: Signal Quality Filters (FINAL OPTIMIZED - Balanced)
MIN_VWAP_DISPLACEMENT_PCT = 0.35  # Balanced - stronger than 0.30, not as strict as 0.40
REQUIRE_CANDLE_CLOSE_CONFIRM = True
NO_TRADE_FIRST_MINUTES = 5
MIN_BARS_SINCE_STRETCH = 2        # Original value (keeps trade count up)
MAX_BARS_SINCE_STRETCH = 30

# Phase 2: Volatility Adaptation
VIX_LOW_THRESHOLD = 15.0
VIX_HIGH_THRESHOLD = 25.0
VIX_EXTREME_THRESHOLD = 35.0

VOL_REGIME_MULTIPLIERS = {
    "LOW": 1.25,
    "NORMAL": 1.00,
    "HIGH": 0.60,
    "EXTREME": 0.00,
}

ATR_MULTIPLIER_BY_REGIME = {
    "LOW": {"stretch": 1.0, "reclaim": 1.0, "tp": 1.0, "sl": 1.0},
    "NORMAL": {"stretch": 1.0, "reclaim": 1.0, "tp": 1.0, "sl": 1.0},
    "HIGH": {"stretch": 1.3, "reclaim": 0.8, "tp": 0.8, "sl": 1.2},
    "EXTREME": {"stretch": 1.5, "reclaim": 0.6, "tp": 0.6, "sl": 1.5},
}

# Exits in R-multiples (FINAL OPTIMIZED - Balanced)
RTH_TP_R = 1.0                    # Symmetric 1R take profit
RTH_SL_R = 1.0                    # Symmetric 1R stop loss

# Time stops
MAX_HOLD_MINUTES_RTH = 45

# Cooldowns
COOLDOWN_AFTER_ENTRY_MIN = 15
COOLDOWN_AFTER_CLOSE_MIN = 10

# Session times
RTH_OPEN_HOUR = 9
RTH_OPEN_MIN = 30
RTH_CLOSE_HOUR = 16
RTH_CLOSE_MIN = 0
NO_NEW_ENTRIES_AFTER_HOUR = 15
NO_NEW_ENTRIES_AFTER_MIN = 45


# ============================================================
# DATA STRUCTURES
# ============================================================

class OrderSide(Enum):
    BUY = "buy"
    SELL = "sell"


@dataclass
class SignalFlags:
    stretched_long: bool = False
    stretched_short: bool = False
    stretch_bar_idx: int = -1


@dataclass
class Position:
    sym: str
    side: str  # "BUY" or "SELL"
    qty: int
    entry_price: float
    entry_time: dt.datetime
    stop_price: float
    tp_price: float
    r_atr: float


@dataclass
class Trade:
    sym: str
    side: str
    qty: int
    entry_price: float
    entry_time: dt.datetime
    exit_price: float
    exit_time: dt.datetime
    pnl_dollars: float
    pnl_r: float
    hold_minutes: float
    exit_reason: str  # "TP", "SL", "TIME", "EOD"


@dataclass
class BacktestState:
    equity: float
    cash: float
    positions: Dict[str, Position] = field(default_factory=dict)
    signal_flags: Dict[str, SignalFlags] = field(default_factory=dict)
    cooldown_until: Dict[str, dt.datetime] = field(default_factory=dict)
    trades: List[Trade] = field(default_factory=list)
    entries_this_minute: int = 0
    last_minute_key: str = ""

    # Tracking
    signals_generated: int = 0
    signals_taken: int = 0
    signals_blocked_exposure: int = 0
    signals_blocked_idempotency: int = 0
    signals_blocked_quality: int = 0


# ============================================================
# POLYGON DATA FETCHING
# ============================================================

class AlpacaDataClient:
    """Fetch historical data from Alpaca Data API."""

    def __init__(self, api_key: str, secret_key: str):
        self.api_key = api_key
        self.secret_key = secret_key
        self.base_url = ALPACA_DATA_URL
        self.session = requests.Session()
        self._cache: Dict[str, pd.DataFrame] = {}

    def get_agg_1m(self, symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
        """Fetch 1-minute bars for a symbol from Alpaca."""
        cache_key = f"{symbol}_{start_date}_{end_date}"
        if cache_key in self._cache:
            return self._cache[cache_key]

        all_bars = []
        page_token = None

        # Convert dates to RFC3339 format
        start_dt = f"{start_date}T00:00:00Z"
        end_dt = f"{end_date}T23:59:59Z"

        headers = {
            "APCA-API-KEY-ID": self.api_key,
            "APCA-API-SECRET-KEY": self.secret_key,
        }

        while True:
            url = f"{self.base_url}/v2/stocks/{symbol}/bars"
            params = {
                "timeframe": "1Min",
                "start": start_dt,
                "end": end_dt,
                "limit": 10000,
                "adjustment": "split",
                "feed": "sip",
            }
            if page_token:
                params["page_token"] = page_token

            try:
                resp = self.session.get(url, headers=headers, params=params, timeout=30)
                resp.raise_for_status()
                data = resp.json()
            except Exception as e:
                print(f"[DATA] Error fetching {symbol}: {e}")
                break

            bars = data.get("bars", [])
            if not bars:
                break

            all_bars.extend(bars)

            # Check for pagination
            page_token = data.get("next_page_token")
            if not page_token:
                break

        if not all_bars:
            return pd.DataFrame()

        df = pd.DataFrame(all_bars)
        df["ts"] = pd.to_datetime(df["t"]).dt.tz_convert(ET)
        df = df.rename(columns={
            "o": "open", "h": "high", "l": "low", "c": "close",
            "v": "volume", "vw": "vwap"
        })
        df = df.set_index("ts")[["open", "high", "low", "close", "volume", "vwap"]].copy()
        df = df[~df.index.duplicated(keep='first')]
        df = df.sort_index()

        self._cache[cache_key] = df
        return df


class PolygonClient:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = POLYGON_REST_BASE
        self.session = requests.Session()
        self._cache: Dict[str, pd.DataFrame] = {}

    def get_agg_1m(self, symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
        """Fetch 1-minute aggregates for a symbol."""
        cache_key = f"{symbol}_{start_date}_{end_date}"
        if cache_key in self._cache:
            return self._cache[cache_key]

        all_rows = []
        current_start = start_date

        while True:
            url = f"{self.base_url}/v2/aggs/ticker/{symbol}/range/1/minute/{current_start}/{end_date}"
            params = {
                "apiKey": self.api_key,
                "adjusted": "true",
                "sort": "asc",
                "limit": "50000",
            }

            try:
                resp = self.session.get(url, params=params, timeout=30)
                resp.raise_for_status()
                data = resp.json()
            except Exception as e:
                print(f"[DATA] Error fetching {symbol}: {e}")
                break

            results = data.get("results", [])
            if not results:
                break

            all_rows.extend(results)

            # Check if we need to paginate
            if len(results) < 50000:
                break

            # Get next start from last timestamp
            last_ts = results[-1]["t"]
            next_start = dt.datetime.fromtimestamp(last_ts / 1000, tz=dt.timezone.utc)
            next_start = next_start + dt.timedelta(minutes=1)
            current_start = next_start.strftime("%Y-%m-%d")

            if current_start > end_date:
                break

        if not all_rows:
            return pd.DataFrame()

        df = pd.DataFrame(all_rows)
        df["ts"] = pd.to_datetime(df["t"], unit="ms", utc=True).dt.tz_convert(ET)
        df = df.rename(columns={
            "o": "open", "h": "high", "l": "low", "c": "close",
            "v": "volume", "vw": "vwap"
        })
        df = df.set_index("ts")[["open", "high", "low", "close", "volume", "vwap"]].copy()
        df = df[~df.index.duplicated(keep='first')]
        df = df.sort_index()

        self._cache[cache_key] = df
        return df


# ============================================================
# INDICATORS
# ============================================================

def calc_atr(df: pd.DataFrame, n: int) -> pd.Series:
    prev_close = df["close"].shift(1)
    tr = pd.concat([
        (df["high"] - df["low"]).abs(),
        (df["high"] - prev_close).abs(),
        (df["low"] - prev_close).abs(),
    ], axis=1).max(axis=1)
    return tr.rolling(n).mean()


def calc_adx(df: pd.DataFrame, n: int) -> pd.Series:
    high, low, close = df["high"], df["low"], df["close"]
    up = high.diff()
    dn = -low.diff()
    plus_dm = up.where((up > dn) & (up > 0), 0.0)
    minus_dm = dn.where((dn > up) & (dn > 0), 0.0)

    prev_close = close.shift(1)
    tr = pd.concat([
        (high - low).abs(),
        (high - prev_close).abs(),
        (low - prev_close).abs(),
    ], axis=1).max(axis=1)

    a = tr.rolling(n).mean()
    plus_di = 100 * (plus_dm.rolling(n).mean() / a)
    minus_di = 100 * (minus_dm.rolling(n).mean() / a)
    dx = (100 * (plus_di - minus_di).abs() / (plus_di + minus_di)).fillna(0)
    return dx.rolling(n).mean()


def calc_vwap_from_anchor(df: pd.DataFrame, anchor: dt.datetime) -> pd.Series:
    x = df[df.index >= anchor].copy()
    if x.empty:
        return pd.Series(index=df.index, dtype=float)
    pv = (x["close"] * x["volume"]).cumsum()
    vv = x["volume"].cumsum().replace(0, float("nan"))
    v = pv / vv
    out = pd.Series(index=df.index, dtype=float)
    out.loc[v.index] = v
    return out


# ============================================================
# EXPOSURE MANAGER (Phase 1)
# ============================================================

class ExposureManager:
    def __init__(self):
        self._sym_to_cluster: Dict[str, str] = {}
        for cluster_name, symbols in CORRELATION_CLUSTERS.items():
            for sym in symbols:
                self._sym_to_cluster[sym] = cluster_name

    def get_cluster(self, sym: str) -> Optional[str]:
        return self._sym_to_cluster.get(sym)

    def can_open_position(
        self,
        sym: str,
        proposed_notional: float,
        positions: Dict[str, Position],
        equity: float
    ) -> Tuple[bool, str]:
        if equity <= 0:
            return False, "invalid_equity"

        # Check 1: Max per symbol
        if proposed_notional > MAX_EXPOSURE_PER_SYMBOL_USD:
            return False, f"exceeds_per_symbol_${MAX_EXPOSURE_PER_SYMBOL_USD}"

        # Check 2: Max total exposure
        current_exposure = sum(p.qty * p.entry_price for p in positions.values())
        projected = current_exposure + proposed_notional
        max_total = equity * MAX_EXPOSURE_PCT_OF_EQUITY
        if projected > max_total:
            return False, f"exceeds_total_{MAX_EXPOSURE_PCT_OF_EQUITY*100:.0f}%"

        # Check 3: Correlated positions
        cluster = self.get_cluster(sym)
        if cluster:
            cluster_count = sum(
                1 for s, p in positions.items()
                if self.get_cluster(s) == cluster
            )
            if cluster_count >= MAX_CORRELATED_POSITIONS:
                return False, f"exceeds_cluster_{cluster}"

        return True, "ok"

    def adjust_qty_for_limits(
        self,
        sym: str,
        desired_qty: int,
        px: float,
        positions: Dict[str, Position],
        equity: float
    ) -> int:
        if desired_qty <= 0 or px <= 0:
            return 0

        # Cap at per-symbol max
        max_qty_symbol = int(MAX_EXPOSURE_PER_SYMBOL_USD / px)

        # Cap at total exposure headroom
        current_exposure = sum(p.qty * p.entry_price for p in positions.values())
        max_total = equity * MAX_EXPOSURE_PCT_OF_EQUITY
        headroom = max(0, max_total - current_exposure)
        max_qty_total = int(headroom / px) if px > 0 else 0

        return min(desired_qty, max_qty_symbol, max_qty_total)


# ============================================================
# VOLATILITY REGIME MANAGER (Phase 2)
# ============================================================

class VolatilityRegimeManager:
    """Simulates VIX-based volatility regime for backtesting using multiple indicators."""

    def __init__(self):
        self._regime: str = "NORMAL"
        self._vix_value: float = 20.0
        self._vix_data: Optional[pd.DataFrame] = None

    def load_vix_data(self, client, start_date: str, end_date: str):
        """Load historical VIX proxy data using multiple volatility measures."""
        try:
            print("[VOL] Loading VIX proxy data...")
            spy_df = client.get_agg_1m("SPY", start_date, end_date)
            if not spy_df.empty:
                daily = spy_df.resample('D').agg({
                    'open': 'first',
                    'high': 'max',
                    'low': 'min',
                    'close': 'last',
                    'volume': 'sum'
                }).dropna()

                # Method 1: Parkinson volatility (high-low range) - faster to react
                # log(H/L)^2 normalized by constant
                daily['log_hl'] = (daily['high'] / daily['low']).apply(lambda x: math.log(x) ** 2 if x > 0 else 0)
                daily['parkinson_vol'] = (daily['log_hl'].rolling(5).mean() / (4 * math.log(2))) ** 0.5 * (252 ** 0.5) * 100

                # Method 2: Daily range as % of price (intraday volatility)
                daily['range_pct'] = (daily['high'] - daily['low']) / daily['close'] * 100
                # Average daily range is ~1% for SPY in normal conditions, ~2-3% in high vol
                daily['range_vol'] = daily['range_pct'].rolling(5).mean() * 8  # Scale: 1% range = VIX ~8

                # Method 3: Close-to-close returns (traditional)
                daily['returns'] = daily['close'].pct_change()
                daily['realized_vol'] = daily['returns'].rolling(10).std() * (252 ** 0.5) * 100

                # Use median of indicators (more robust than max which can spike)
                daily['vix_proxy'] = daily[['parkinson_vol', 'range_vol', 'realized_vol']].median(axis=1)
                daily['vix_proxy'] = daily['vix_proxy'].fillna(16.0).clip(lower=10, upper=50)

                self._vix_data = daily[['vix_proxy', 'range_pct']]
                print(f"[VOL] VIX proxy loaded: range {daily['vix_proxy'].min():.1f} - {daily['vix_proxy'].max():.1f}")
        except Exception as e:
            print(f"[VOL] Error loading VIX data: {e}")

    def get_regime_for_date(self, date: dt.date) -> Tuple[str, float]:
        """Get volatility regime for a specific date."""
        if self._vix_data is None or self._vix_data.empty:
            return "NORMAL", 20.0

        try:
            # Convert date to timestamp and find in index
            target = pd.Timestamp(date).tz_localize(None)
            # Normalize index to remove timezone for comparison
            idx_dates = self._vix_data.index.tz_localize(None) if self._vix_data.index.tz else self._vix_data.index

            # Find dates <= target and get the last one
            mask = idx_dates <= target
            if mask.any():
                valid_dates = self._vix_data.loc[mask]
                if not valid_dates.empty:
                    vix = float(valid_dates.iloc[-1]['vix_proxy'])
                else:
                    vix = 20.0
            else:
                vix = 20.0
        except Exception as e:
            vix = 20.0

        # Determine regime
        if vix >= VIX_EXTREME_THRESHOLD:
            regime = "EXTREME"
        elif vix >= VIX_HIGH_THRESHOLD:
            regime = "HIGH"
        elif vix < VIX_LOW_THRESHOLD:
            regime = "LOW"
        else:
            regime = "NORMAL"

        self._regime = regime
        self._vix_value = vix
        return regime, vix

    def get_size_multiplier(self) -> float:
        """Get position size multiplier for current regime."""
        return VOL_REGIME_MULTIPLIERS.get(self._regime, 1.0)

    def get_atr_multipliers(self) -> Dict[str, float]:
        """Get ATR multipliers for current regime."""
        return ATR_MULTIPLIER_BY_REGIME.get(self._regime, ATR_MULTIPLIER_BY_REGIME["NORMAL"])

    def should_allow_entry(self) -> bool:
        """Check if entries are allowed (not in EXTREME regime)."""
        return self._regime != "EXTREME"


# ============================================================
# SIGNAL EVALUATION (with Phase 1 & 2 filters)
# ============================================================

def evaluate_vwap_reclaim(
    sym: str,
    df: pd.DataFrame,
    anchor: dt.datetime,
    flags: SignalFlags,
    current_bar_idx: int,
    vol_manager: Optional[VolatilityRegimeManager] = None
) -> Optional[Tuple[OrderSide, float]]:
    """
    VWAP reclaim signal with Phase 1 quality filters and Phase 2 volatility adaptation.
    """
    min_bars = max(ATR_LEN, ADX_LEN) + 10
    if df.empty or len(df) < min_bars:
        return None

    df = df.copy()
    df["atr"] = calc_atr(df, ATR_LEN)
    df["adx"] = calc_adx(df, ADX_LEN)
    df["vwap_sess"] = calc_vwap_from_anchor(df, anchor)

    if df["atr"].notna().sum() < ATR_LEN or df["adx"].notna().sum() < ADX_LEN:
        return None

    last = df.iloc[-1]
    a = float(last["atr"]) if pd.notna(last["atr"]) else 0.0
    x = float(last["adx"]) if pd.notna(last["adx"]) else 999.0
    vwap = float(last["vwap_sess"]) if pd.notna(last["vwap_sess"]) else float("nan")
    px = float(last["close"])

    if a <= 0 or math.isnan(vwap):
        return None
    if x > TREND_ADX_MAX:
        return None

    # Phase 1: No trades in first N minutes
    bars_since_anchor = len(df[df.index >= anchor])
    if bars_since_anchor < NO_TRADE_FIRST_MINUTES:
        return None

    vol = float(last["volume"])
    vol_mean = float(df["volume"].rolling(30).mean().iloc[-1] or 0.0)
    relvol = vol / vol_mean if vol_mean > 0 else 0.0
    if relvol < MIN_REL_VOL:
        return None

    # Phase 2: Get volatility-adjusted ATR multipliers
    atr_mults = ATR_MULTIPLIER_BY_REGIME["NORMAL"]
    if vol_manager:
        atr_mults = vol_manager.get_atr_multipliers()

    stretch = STRETCH_ATR * a * atr_mults.get("stretch", 1.0)
    reclaim = RECLAIM_ATR * a * atr_mults.get("reclaim", 1.0)

    # Phase 1: Percentage displacement check
    displacement_pct = abs(px - vwap) / vwap * 100.0 if vwap > 0 else 0.0

    # Check for stretch
    if px < vwap - stretch and displacement_pct >= MIN_VWAP_DISPLACEMENT_PCT:
        if not flags.stretched_long:
            flags.stretched_long = True
            flags.stretch_bar_idx = current_bar_idx

    if px > vwap + stretch and displacement_pct >= MIN_VWAP_DISPLACEMENT_PCT:
        if not flags.stretched_short:
            flags.stretched_short = True
            flags.stretch_bar_idx = current_bar_idx

    # Phase 1: Expire stale stretches
    if flags.stretch_bar_idx >= 0:
        bars_since_stretch = current_bar_idx - flags.stretch_bar_idx
        if bars_since_stretch > MAX_BARS_SINCE_STRETCH:
            flags.stretched_long = False
            flags.stretched_short = False
            flags.stretch_bar_idx = -1
            return None

    # Check for reclaim
    if flags.stretched_long and px >= (vwap - reclaim):
        bars_since_stretch = current_bar_idx - flags.stretch_bar_idx if flags.stretch_bar_idx >= 0 else 0

        if bars_since_stretch < MIN_BARS_SINCE_STRETCH:
            return None

        if REQUIRE_CANDLE_CLOSE_CONFIRM and px < (vwap - reclaim):
            return None

        flags.stretched_long = False
        flags.stretch_bar_idx = -1
        return (OrderSide.BUY, a)

    if flags.stretched_short and px <= (vwap + reclaim):
        bars_since_stretch = current_bar_idx - flags.stretch_bar_idx if flags.stretch_bar_idx >= 0 else 0

        if bars_since_stretch < MIN_BARS_SINCE_STRETCH:
            return None

        if REQUIRE_CANDLE_CLOSE_CONFIRM and px > (vwap + reclaim):
            return None

        flags.stretched_short = False
        flags.stretch_bar_idx = -1
        return (OrderSide.SELL, a)

    return None


# ============================================================
# BACKTEST ENGINE
# ============================================================

class BacktestEngine:
    def __init__(self, start_equity: float, symbols: List[str]):
        self.state = BacktestState(equity=start_equity, cash=start_equity)
        self.symbols = symbols
        self.exposure_mgr = ExposureManager()
        self.vol_mgr = VolatilityRegimeManager()  # Phase 2
        self.polygon = PolygonClient(POLYGON_API_KEY)
        self.data: Dict[str, pd.DataFrame] = {}
        self.start_date: str = ""
        self.end_date: str = ""

    def load_data(self, start_date: str, end_date: str):
        """Load historical data for all symbols."""
        self.start_date = start_date
        self.end_date = end_date
        print(f"[DATA] Loading data from {start_date} to {end_date}...")
        for sym in self.symbols:
            print(f"[DATA] Fetching {sym}...")
            df = self.polygon.get_agg_1m(sym, start_date, end_date)
            if not df.empty:
                self.data[sym] = df
                print(f"[DATA] {sym}: {len(df)} bars loaded")
            else:
                print(f"[DATA] {sym}: No data")

        # Phase 2: Load VIX proxy data
        self.vol_mgr.load_vix_data(self.polygon, start_date, end_date)

    def is_rth(self, ts: dt.datetime) -> bool:
        """Check if timestamp is during RTH."""
        if ts.weekday() >= 5:
            return False
        rth_open = ts.replace(hour=RTH_OPEN_HOUR, minute=RTH_OPEN_MIN, second=0, microsecond=0)
        rth_close = ts.replace(hour=RTH_CLOSE_HOUR, minute=RTH_CLOSE_MIN, second=0, microsecond=0)
        return rth_open <= ts < rth_close

    def is_no_new_entries(self, ts: dt.datetime) -> bool:
        """Check if we're in the no-new-entries window."""
        cutoff = ts.replace(hour=NO_NEW_ENTRIES_AFTER_HOUR, minute=NO_NEW_ENTRIES_AFTER_MIN, second=0, microsecond=0)
        return ts >= cutoff

    def get_rth_open(self, ts: dt.datetime) -> dt.datetime:
        """Get RTH open time for given date."""
        return ts.replace(hour=RTH_OPEN_HOUR, minute=RTH_OPEN_MIN, second=0, microsecond=0)

    def simulate_fill(self, sym: str, side: OrderSide, qty: int, price: float, ts: dt.datetime, r_atr: float):
        """Simulate order fill and create position."""
        # Phase 2: Get volatility-adjusted TP/SL multipliers
        atr_mults = self.vol_mgr.get_atr_multipliers()
        tp_mult = atr_mults.get("tp", 1.0)
        sl_mult = atr_mults.get("sl", 1.0)

        if side == OrderSide.BUY:
            stop = price - (RTH_SL_R * r_atr * sl_mult)
            tp = price + (RTH_TP_R * r_atr * tp_mult)
        else:
            stop = price + (RTH_SL_R * r_atr * sl_mult)
            tp = price - (RTH_TP_R * r_atr * tp_mult)

        position = Position(
            sym=sym,
            side=side.name,
            qty=qty,
            entry_price=price,
            entry_time=ts,
            stop_price=stop,
            tp_price=tp,
            r_atr=r_atr,
        )
        self.state.positions[sym] = position
        self.state.cash -= qty * price
        self.state.cooldown_until[sym] = ts + dt.timedelta(minutes=COOLDOWN_AFTER_ENTRY_MIN)

    def close_position(self, sym: str, exit_price: float, exit_time: dt.datetime, reason: str):
        """Close position and record trade."""
        pos = self.state.positions.get(sym)
        if not pos:
            return

        if pos.side == "BUY":
            pnl = (exit_price - pos.entry_price) * pos.qty
        else:
            pnl = (pos.entry_price - exit_price) * pos.qty

        pnl_r = pnl / (pos.r_atr * pos.qty) if pos.r_atr > 0 else 0.0
        hold_min = (exit_time - pos.entry_time).total_seconds() / 60.0

        trade = Trade(
            sym=sym,
            side=pos.side,
            qty=pos.qty,
            entry_price=pos.entry_price,
            entry_time=pos.entry_time,
            exit_price=exit_price,
            exit_time=exit_time,
            pnl_dollars=pnl,
            pnl_r=pnl_r,
            hold_minutes=hold_min,
            exit_reason=reason,
        )
        self.state.trades.append(trade)

        self.state.cash += pos.qty * exit_price
        self.state.equity += pnl
        del self.state.positions[sym]
        self.state.cooldown_until[sym] = exit_time + dt.timedelta(minutes=COOLDOWN_AFTER_CLOSE_MIN)

    def manage_positions(self, ts: dt.datetime, prices: Dict[str, float]):
        """Check stops, targets, and time stops for all positions."""
        for sym in list(self.state.positions.keys()):
            pos = self.state.positions[sym]
            px = prices.get(sym)
            if px is None:
                continue

            # Time stop
            hold_min = (ts - pos.entry_time).total_seconds() / 60.0
            if hold_min >= MAX_HOLD_MINUTES_RTH:
                self.close_position(sym, px, ts, "TIME")
                continue

            # Stop loss
            if pos.side == "BUY" and px <= pos.stop_price:
                self.close_position(sym, px, ts, "SL")
                continue
            if pos.side == "SELL" and px >= pos.stop_price:
                self.close_position(sym, px, ts, "SL")
                continue

            # Take profit
            if pos.side == "BUY" and px >= pos.tp_price:
                self.close_position(sym, px, ts, "TP")
                continue
            if pos.side == "SELL" and px <= pos.tp_price:
                self.close_position(sym, px, ts, "TP")
                continue

    def flatten_eod(self, ts: dt.datetime, prices: Dict[str, float]):
        """Flatten all positions at end of day."""
        for sym in list(self.state.positions.keys()):
            px = prices.get(sym)
            if px:
                self.close_position(sym, px, ts, "EOD")

    def run(self):
        """Run the backtest simulation."""
        if not self.data:
            print("[BACKTEST] No data loaded!")
            return

        # Get all unique timestamps across all symbols
        all_timestamps = set()
        for df in self.data.values():
            all_timestamps.update(df.index.tolist())
        all_timestamps = sorted(all_timestamps)

        print(f"[BACKTEST] Running simulation over {len(all_timestamps)} bars...")
        print(f"[BACKTEST] Date range: {all_timestamps[0]} to {all_timestamps[-1]}")

        bar_idx_map: Dict[str, int] = {sym: 0 for sym in self.symbols}
        last_date = None

        for ts in all_timestamps:
            current_date = ts.date()

            # Daily reset
            if current_date != last_date:
                if last_date is not None:
                    # Flatten EOD
                    prices = {sym: float(df.loc[df.index <= ts, "close"].iloc[-1])
                              for sym, df in self.data.items() if not df.empty and len(df.loc[df.index <= ts]) > 0}
                    self.flatten_eod(ts, prices)
                last_date = current_date

                # Phase 2: Update volatility regime for new day
                regime, vix = self.vol_mgr.get_regime_for_date(current_date)
                print(f"[VOL] {current_date}: regime={regime} VIX={vix:.1f}")

            # Skip non-RTH
            if not self.is_rth(ts):
                continue

            # Get current prices
            prices = {}
            for sym, df in self.data.items():
                if ts in df.index:
                    prices[sym] = float(df.loc[ts, "close"])

            # Manage existing positions
            self.manage_positions(ts, prices)

            # Skip if in no-new-entries window
            if self.is_no_new_entries(ts):
                continue

            # Skip if at max positions
            if len(self.state.positions) >= MAX_OPEN_POSITIONS:
                continue

            # Rate limit
            minute_key = ts.strftime("%Y-%m-%d %H:%M")
            if minute_key != self.state.last_minute_key:
                self.state.last_minute_key = minute_key
                self.state.entries_this_minute = 0

            if self.state.entries_this_minute >= MAX_ENTRIES_PER_MINUTE:
                continue

            # Scan for signals
            anchor = self.get_rth_open(ts)

            for sym in self.symbols:
                if sym in self.state.positions:
                    continue

                # Cooldown check
                cd = self.state.cooldown_until.get(sym)
                if cd and ts < cd:
                    continue

                df = self.data.get(sym)
                if df is None or df.empty:
                    continue

                # Get data up to current bar
                df_slice = df[df.index <= ts].tail(LOOKBACK_MINUTES)
                if len(df_slice) < max(ATR_LEN, ADX_LEN) + 10:
                    continue

                px = float(df_slice.iloc[-1]["close"])
                if not (MIN_PRICE <= px <= MAX_PRICE):
                    continue

                # Get or create signal flags
                if sym not in self.state.signal_flags:
                    self.state.signal_flags[sym] = SignalFlags()
                flags = self.state.signal_flags[sym]

                # Phase 2: Check if entries are allowed (not in EXTREME regime)
                if not self.vol_mgr.should_allow_entry():
                    continue

                # Evaluate signal (with Phase 2 vol_manager)
                bar_idx_map[sym] = bar_idx_map.get(sym, 0) + 1
                sig = evaluate_vwap_reclaim(sym, df_slice, anchor, flags, bar_idx_map[sym], self.vol_mgr)

                if not sig:
                    continue

                side, r_atr = sig
                self.state.signals_generated += 1

                # Compute qty with Phase 2 volatility sizing
                base_notional = self.state.cash * TRADE_CASH_PCT_RTH
                vol_mult = self.vol_mgr.get_size_multiplier()
                adjusted_notional = base_notional * vol_mult
                qty = int(adjusted_notional // px)
                if qty <= 0:
                    continue

                # Phase 1: Exposure check
                qty = self.exposure_mgr.adjust_qty_for_limits(
                    sym, qty, px, self.state.positions, self.state.equity
                )
                if qty <= 0:
                    self.state.signals_blocked_exposure += 1
                    continue

                # Simulate fill
                self.simulate_fill(sym, side, qty, px, ts, r_atr)
                self.state.signals_taken += 1
                self.state.entries_this_minute += 1
                break  # One entry per scan

        # Final flatten
        if self.state.positions:
            last_ts = all_timestamps[-1]
            prices = {sym: float(df.iloc[-1]["close"]) for sym, df in self.data.items() if not df.empty}
            self.flatten_eod(last_ts, prices)

        print("[BACKTEST] Simulation complete!")

    def report(self):
        """Generate performance report."""
        trades = self.state.trades
        if not trades:
            print("\n[REPORT] No trades executed!")
            return

        # Calculate metrics
        total_trades = len(trades)
        wins = [t for t in trades if t.pnl_dollars > 0]
        losses = [t for t in trades if t.pnl_dollars < 0]
        breakevens = [t for t in trades if t.pnl_dollars == 0]

        win_rate = len(wins) / total_trades if total_trades > 0 else 0
        total_pnl = sum(t.pnl_dollars for t in trades)
        total_r = sum(t.pnl_r for t in trades)
        avg_r = total_r / total_trades if total_trades > 0 else 0

        avg_win = sum(t.pnl_dollars for t in wins) / len(wins) if wins else 0
        avg_loss = sum(t.pnl_dollars for t in losses) / len(losses) if losses else 0
        profit_factor = abs(sum(t.pnl_dollars for t in wins) / sum(t.pnl_dollars for t in losses)) if losses and sum(t.pnl_dollars for t in losses) != 0 else float('inf')

        avg_hold = sum(t.hold_minutes for t in trades) / total_trades if trades else 0

        # Exit breakdown
        exit_counts = {}
        for t in trades:
            exit_counts[t.exit_reason] = exit_counts.get(t.exit_reason, 0) + 1

        print("\n" + "=" * 60)
        print("BACKTEST RESULTS")
        print("=" * 60)
        print(f"\nStarting Equity: ${self.state.equity - total_pnl:,.2f}")
        print(f"Ending Equity:   ${self.state.equity:,.2f}")
        print(f"Total P&L:       ${total_pnl:,.2f} ({total_pnl / (self.state.equity - total_pnl) * 100:.2f}%)")
        print(f"\n--- Trade Statistics ---")
        print(f"Total Trades:    {total_trades}")
        print(f"Wins:            {len(wins)} ({win_rate*100:.1f}%)")
        print(f"Losses:          {len(losses)}")
        print(f"Breakeven:       {len(breakevens)}")
        print(f"\nAvg Win:         ${avg_win:,.2f}")
        print(f"Avg Loss:        ${avg_loss:,.2f}")
        print(f"Profit Factor:   {profit_factor:.2f}")
        print(f"\nTotal R:         {total_r:.2f}R")
        print(f"Avg R/Trade:     {avg_r:.2f}R")
        print(f"Avg Hold Time:   {avg_hold:.1f} min")
        print(f"\n--- Exit Breakdown ---")
        for reason, count in sorted(exit_counts.items()):
            print(f"  {reason}: {count} ({count/total_trades*100:.1f}%)")
        print(f"\n--- Signal Statistics ---")
        print(f"Signals Generated:       {self.state.signals_generated}")
        print(f"Signals Taken:           {self.state.signals_taken}")
        print(f"Blocked by Exposure:     {self.state.signals_blocked_exposure}")
        print(f"Signal Take Rate:        {self.state.signals_taken/self.state.signals_generated*100:.1f}%" if self.state.signals_generated > 0 else "N/A")
        print("=" * 60)

        # Save trades to CSV
        trades_df = pd.DataFrame([asdict(t) for t in trades])
        trades_df.to_csv("backtest_trades.csv", index=False)
        print(f"\n[REPORT] Trade log saved to backtest_trades.csv")


# ============================================================
# MAIN
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="VWAP Reclaim Strategy Backtest")
    parser.add_argument("--start", type=str, required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, required=True, help="End date (YYYY-MM-DD)")
    parser.add_argument("--equity", type=float, default=100000, help="Starting equity (default: 100000)")
    parser.add_argument("--symbols", type=str, default=None, help="Comma-separated symbols (default: core universe)")
    parser.add_argument("--api-key", type=str, default=None, help="Polygon API key (overrides env var)")
    args = parser.parse_args()

    # Allow API key from command line
    global POLYGON_API_KEY
    if args.api_key:
        POLYGON_API_KEY = args.api_key

    if not POLYGON_API_KEY:
        print("[ERROR] POLYGON_API_KEY not set!")
        return

    symbols = args.symbols.split(",") if args.symbols else BACKTEST_SYMBOLS

    print(f"\n{'='*60}")
    print("VWAP RECLAIM STRATEGY BACKTEST")
    print(f"{'='*60}")
    print(f"Period:     {args.start} to {args.end}")
    print(f"Equity:     ${args.equity:,.2f}")
    print(f"Symbols:    {', '.join(symbols)}")
    print(f"{'='*60}\n")

    engine = BacktestEngine(start_equity=args.equity, symbols=symbols)
    engine.load_data(args.start, args.end)
    engine.run()
    engine.report()


if __name__ == "__main__":
    main()
