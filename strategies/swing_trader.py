"""
AI Swing Trader v1.0 - LLM-Powered Swing Trading Bot
=====================================================

A sophisticated swing trading bot that uses Claude AI to analyze stocks
and make intelligent trading decisions with 5-15 day holding periods.

Key Features:
- Scans entire US stock universe for opportunities
- LLM-powered analysis (technical, fundamental, news/catalyst)
- 10-20 position portfolio with intelligent sizing
- Continuous monitoring with WebSocket streaming
- Comprehensive risk management

Architecture:
1. DISCOVERY LAYER: Scans for candidates using technical filters
2. ANALYSIS LAYER: LLM scores and ranks candidates
3. PORTFOLIO LAYER: Position sizing and risk management
4. MONITOR LAYER: Real-time price/news monitoring

Author: AI-Enhanced Trading System
Version: 1.0.0
"""

from __future__ import annotations

import os
import sys
import json
import time
import math
import csv
import traceback
import logging
import threading
import signal
import asyncio
import hashlib
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta, date, time as dt_time
from typing import Dict, List, Tuple, Optional, Any, Set
from logging.handlers import RotatingFileHandler
from enum import Enum
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import numpy as np
import pandas as pd
import pytz
import requests

# Alpaca SDK
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import (
    MarketOrderRequest,
    LimitOrderRequest,
    StopLossRequest,
    TakeProfitRequest,
    TrailingStopOrderRequest
)
from alpaca.trading.enums import OrderSide, TimeInForce, OrderType, OrderClass
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest, StockSnapshotRequest
from alpaca.data.timeframe import TimeFrame
from alpaca.data.live import StockDataStream
from alpaca.trading.stream import TradingStream

# Load .env if available
try:
    from dotenv import load_dotenv
    _env_path = Path(__file__).parent.parent / "config" / "swing_trader.env"
    if _env_path.exists():
        load_dotenv(_env_path)
    else:
        # Try parent .env
        _env_path = Path(__file__).parent.parent / "config" / ".env"
        if _env_path.exists():
            load_dotenv(_env_path)
except ImportError:
    pass

# Windows ANSI support
if sys.platform == "win32":
    try:
        import ctypes
        kernel32 = ctypes.windll.kernel32
        kernel32.SetConsoleMode(kernel32.GetStdHandle(-11), 7)
    except Exception:
        pass


# ============================================================
# CONFIGURATION
# ============================================================

ET = pytz.timezone("America/New_York")

# --- Directory Structure ---
ALGO_ROOT = Path(__file__).parent.parent  # Algo_Trading root
# Allow output dir override (useful when source lives on Google Drive)
_output_root = Path(os.getenv("ALGO_OUTPUT_DIR", "")) if os.getenv("ALGO_OUTPUT_DIR") else ALGO_ROOT
DATA_DIR = _output_root / "data"
LOGS_DIR = _output_root / "logs"
CONFIG_DIR = ALGO_ROOT / "config"
DATA_DIR.mkdir(exist_ok=True)
LOGS_DIR.mkdir(exist_ok=True)

# --- API Keys ---
ALPACA_API_KEY = os.getenv("ALPACA_API_KEY", "")
ALPACA_SECRET_KEY = os.getenv("ALPACA_SECRET_KEY", "")
ALPACA_PAPER = os.getenv("ALPACA_PAPER", "true").lower() in ("1", "true", "yes")
POLYGON_API_KEY = os.getenv("POLYGON_API_KEY", "")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")

# --- Trading Parameters ---
MAX_POSITIONS = 20                    # Maximum simultaneous positions
MIN_POSITIONS = 10                    # Target minimum positions
POSITION_SIZE_PCT = 0.05              # Base position size (5% of portfolio)
MAX_POSITION_SIZE_PCT = 0.08          # Maximum single position (8%)
MIN_POSITION_SIZE_PCT = 0.02          # Minimum position size (2%)

# --- Swing Trade Parameters ---
TARGET_HOLD_DAYS = 10                 # Target holding period (5-15 day range)
MAX_HOLD_DAYS = 15                    # Maximum holding period before forced review
MIN_HOLD_DAYS = 2                     # Minimum hold before profit-taking allowed

# --- Entry Criteria ---
MIN_CONVICTION_SCORE = 70             # Minimum LLM score to enter (0-100)
MIN_PRICE = 10.0                      # Minimum stock price
MAX_PRICE = 500.0                     # Maximum stock price
MIN_RVOL = 1.5                        # Minimum relative volume
MIN_AVG_VOLUME = 500_000              # Minimum average daily volume (shares)
MIN_DOLLAR_VOLUME = 10_000_000        # Minimum daily dollar volume

# --- Exit Strategy ---
DEFAULT_STOP_LOSS_PCT = 0.07          # Default 7% stop loss
DEFAULT_TARGET_PCT = 0.15             # Default 15% profit target
TRAILING_STOP_ACTIVATION_PCT = 0.05   # Activate trailing stop after 5% gain
TRAILING_STOP_DISTANCE_PCT = 0.03     # Trail by 3%

# --- Risk Management ---
MAX_SECTOR_EXPOSURE_PCT = 0.30        # Max 30% in any single sector
MAX_DAILY_LOSS_PCT = 0.03             # Halt new entries if down 3% on day
MAX_PORTFOLIO_DRAWDOWN_PCT = 0.10     # Reduce exposure at 10% drawdown
REGIME_VIX_CAUTION = 25.0             # Reduce size when VIX > 25
REGIME_VIX_PAUSE = 35.0               # Pause new entries when VIX > 35

# --- Scanning Schedule ---
SCAN_TIMES = [
    dt_time(6, 30),   # Pre-market scan
    dt_time(10, 0),   # Morning scan (after open volatility settles)
    dt_time(13, 0),   # Midday scan
    dt_time(15, 0),   # Afternoon scan
]
POSITION_REVIEW_TIME = dt_time(8, 0)  # Daily position review (pre-market)

# --- LLM Configuration ---
LLM_MODEL = "claude-sonnet-4-20250514"  # Fast and capable
LLM_MAX_TOKENS = 1024
LLM_TEMPERATURE = 0.3                 # Lower = more consistent
LLM_BATCH_SIZE = 10                   # Analyze 10 stocks per batch
LLM_RATE_LIMIT_DELAY = 1.0            # Seconds between API calls

# --- Data Refresh ---
SCANNER_REFRESH_INTERVAL = 60 * 60 * 2  # Rescan every 2 hours
POSITION_MONITOR_INTERVAL = 30          # Check positions every 30 seconds
NEWS_CHECK_INTERVAL = 60 * 15           # Check news every 15 minutes

# --- Paths ---
STATE_PATH = str(DATA_DIR / "swing_trader_state.json")
TRADES_LOG_PATH = str(DATA_DIR / "swing_trader_trades.csv")
ANALYSIS_LOG_PATH = str(DATA_DIR / "swing_trader_analysis.jsonl")
CANDIDATES_CACHE_PATH = str(DATA_DIR / "swing_trader_candidates.json")

# --- Logging ---
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
MAX_LOG_SIZE_MB = 50
MAX_LOG_BACKUPS = 5

# --- Paper/Live Mode ---
DRY_RUN = os.getenv("DRY_RUN", "false").lower() in ("1", "true", "yes")


# ============================================================
# ENUMS & DATA CLASSES
# ============================================================

class TradeDirection(Enum):
    LONG = "LONG"
    SHORT = "SHORT"  # Future: if you want to short
    AVOID = "AVOID"


class PositionStatus(Enum):
    PENDING = "PENDING"           # Order submitted, awaiting fill
    ACTIVE = "ACTIVE"             # Position is open
    PARTIAL_EXIT = "PARTIAL_EXIT" # Partial profit taken
    EXITING = "EXITING"           # Exit order submitted
    CLOSED = "CLOSED"             # Position fully closed


class ExitReason(Enum):
    STOP_LOSS = "STOP_LOSS"
    TARGET_HIT = "TARGET_HIT"
    TRAILING_STOP = "TRAILING_STOP"
    TIME_EXIT = "TIME_EXIT"
    SCORE_DEGRADED = "SCORE_DEGRADED"
    MANUAL = "MANUAL"
    REGIME_CHANGE = "REGIME_CHANGE"
    NEWS_NEGATIVE = "NEWS_NEGATIVE"


@dataclass
class SwingCandidate:
    """A stock candidate identified by the scanner."""
    symbol: str
    price: float
    rvol: float
    avg_volume: float
    dollar_volume: float
    gap_pct: float = 0.0
    sector: str = "Unknown"

    # Technical indicators
    rsi: float = 50.0
    sma_20: float = 0.0
    sma_50: float = 0.0
    atr: float = 0.0
    support: float = 0.0
    resistance: float = 0.0

    # Fundamental
    float_shares: float = 0.0
    short_interest_pct: float = 0.0
    inst_ownership_pct: float = 0.0
    market_cap: float = 0.0

    # News
    recent_headlines: List[str] = field(default_factory=list)

    # LLM Analysis Results
    conviction_score: float = 0.0
    direction: TradeDirection = TradeDirection.AVOID
    suggested_entry: float = 0.0
    suggested_stop: float = 0.0
    suggested_target: float = 0.0
    expected_hold_days: int = 10
    thesis: str = ""
    risks: List[str] = field(default_factory=list)
    analyzed_at: Optional[str] = None

    def to_dict(self) -> dict:
        d = asdict(self)
        d['direction'] = self.direction.value
        return d

    @classmethod
    def from_dict(cls, d: dict) -> 'SwingCandidate':
        d = d.copy()
        if 'direction' in d:
            d['direction'] = TradeDirection(d['direction'])
        return cls(**d)


@dataclass
class Position:
    """An active or closed position."""
    symbol: str
    entry_price: float
    entry_date: str
    shares: int
    direction: TradeDirection
    status: PositionStatus

    # Risk levels
    stop_loss: float
    target_price: float
    trailing_stop_price: Optional[float] = None
    trailing_stop_active: bool = False

    # Analysis
    conviction_score: float = 0.0
    thesis: str = ""
    sector: str = "Unknown"

    # Exit tracking
    exit_price: Optional[float] = None
    exit_date: Optional[str] = None
    exit_reason: Optional[ExitReason] = None

    # Order tracking
    entry_order_id: Optional[str] = None
    exit_order_id: Optional[str] = None

    # Performance
    unrealized_pnl: float = 0.0
    unrealized_pnl_pct: float = 0.0
    high_water_mark: float = 0.0  # Highest price since entry

    def days_held(self) -> int:
        entry = datetime.fromisoformat(self.entry_date).date()
        return (datetime.now(ET).date() - entry).days

    def to_dict(self) -> dict:
        d = asdict(self)
        d['direction'] = self.direction.value
        d['status'] = self.status.value
        if self.exit_reason:
            d['exit_reason'] = self.exit_reason.value
        return d

    @classmethod
    def from_dict(cls, d: dict) -> 'Position':
        d = d.copy()
        d['direction'] = TradeDirection(d['direction'])
        d['status'] = PositionStatus(d['status'])
        if d.get('exit_reason'):
            d['exit_reason'] = ExitReason(d['exit_reason'])
        return cls(**d)


@dataclass
class BotState:
    """Persistent bot state."""
    # Portfolio
    positions: Dict[str, dict] = field(default_factory=dict)  # symbol -> Position.to_dict()
    closed_positions: List[dict] = field(default_factory=list)

    # Tracking
    starting_equity: float = 0.0
    equity_high_water_mark: float = 0.0
    daily_starting_equity: float = 0.0
    last_scan_timestamp: Optional[float] = None
    last_position_review_date: Optional[str] = None

    # Candidates cache
    candidates: List[dict] = field(default_factory=list)  # SwingCandidate.to_dict()

    # Risk state
    in_drawdown_mode: bool = False
    trading_halted: bool = False
    halt_reason: Optional[str] = None

    # Sector exposure tracking
    sector_exposure: Dict[str, float] = field(default_factory=dict)

    # Statistics
    total_trades: int = 0
    winning_trades: int = 0
    losing_trades: int = 0
    total_pnl: float = 0.0


# ============================================================
# LOGGING SETUP
# ============================================================

def setup_logging() -> logging.Logger:
    """Configure logging with rotation."""
    logger = logging.getLogger("SwingTrader")
    logger.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))

    # Prevent duplicate handlers
    if logger.handlers:
        return logger

    # Console handler
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(logging.Formatter(
        fmt='[%(asctime)s] %(levelname)s: %(message)s',
        datefmt='%H:%M:%S'
    ))

    # File handler with rotation
    file_handler = RotatingFileHandler(
        str(LOGS_DIR / "swing_trader.log"),
        maxBytes=MAX_LOG_SIZE_MB * 1024 * 1024,
        backupCount=MAX_LOG_BACKUPS
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(logging.Formatter(
        fmt='[%(asctime)s] %(levelname)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    ))

    logger.addHandler(console)
    logger.addHandler(file_handler)
    return logger


logger = setup_logging()


# ============================================================
# STATE MANAGEMENT
# ============================================================

def load_state() -> BotState:
    """Load bot state from disk."""
    if not os.path.exists(STATE_PATH):
        logger.info("[STATE] No existing state file, starting fresh")
        return BotState()

    try:
        with open(STATE_PATH, 'r') as f:
            data = json.load(f)

        state = BotState()
        for key, value in data.items():
            if hasattr(state, key):
                setattr(state, key, value)

        logger.info(f"[STATE] Loaded state: {len(state.positions)} active positions, "
                   f"{len(state.candidates)} candidates cached")
        return state
    except Exception as e:
        logger.error(f"[STATE] Failed to load state: {e}")
        return BotState()


def save_state(state: BotState) -> None:
    """Save bot state to disk atomically."""
    temp_path = STATE_PATH + ".tmp"
    try:
        with open(temp_path, 'w') as f:
            json.dump(asdict(state), f, indent=2, default=str)

        if os.path.exists(STATE_PATH):
            os.remove(STATE_PATH)
        os.rename(temp_path, STATE_PATH)
        logger.debug("[STATE] State saved successfully")
    except Exception as e:
        logger.error(f"[STATE] Failed to save state: {e}")


# ============================================================
# ALPACA CLIENT SETUP
# ============================================================

class AlpacaClients:
    """Manages Alpaca API clients."""

    def __init__(self):
        if not ALPACA_API_KEY or not ALPACA_SECRET_KEY:
            raise RuntimeError("Missing ALPACA_API_KEY or ALPACA_SECRET_KEY")

        self.trading = TradingClient(
            api_key=ALPACA_API_KEY,
            secret_key=ALPACA_SECRET_KEY,
            paper=ALPACA_PAPER
        )

        self.data = StockHistoricalDataClient(
            api_key=ALPACA_API_KEY,
            secret_key=ALPACA_SECRET_KEY
        )

        logger.info(f"[ALPACA] Clients initialized (paper={ALPACA_PAPER})")

    def get_account(self):
        """Get account information."""
        return self.trading.get_account()

    def get_positions(self) -> Dict[str, Any]:
        """Get all current positions."""
        positions = self.trading.get_all_positions()
        return {p.symbol: p for p in positions}

    def get_equity(self) -> float:
        """Get current portfolio equity."""
        account = self.get_account()
        return float(account.equity)

    def get_buying_power(self) -> float:
        """Get available buying power."""
        account = self.get_account()
        return float(account.buying_power)


# ============================================================
# POLYGON DATA CLIENT
# ============================================================

class PolygonClient:
    """Handles Polygon.io API requests."""

    BASE_URL = "https://api.polygon.io"

    def __init__(self):
        if not POLYGON_API_KEY:
            raise RuntimeError("Missing POLYGON_API_KEY")
        self.api_key = POLYGON_API_KEY
        self.session = requests.Session()

    def _request(self, endpoint: str, params: dict = None) -> dict:
        """Make API request with error handling."""
        params = params or {}
        params['apiKey'] = self.api_key

        url = f"{self.BASE_URL}{endpoint}"

        try:
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"[POLYGON] Request failed: {e}")
            raise

    def get_all_tickers(self, min_price: float = 10.0, max_price: float = 500.0) -> List[dict]:
        """
        Get all active US stock tickers.
        Returns list of ticker info dicts.
        """
        tickers = []
        next_url = None

        endpoint = "/v3/reference/tickers"
        params = {
            "market": "stocks",
            "active": "true",
            "limit": 1000,
            "sort": "ticker",
            "order": "asc"
        }

        while True:
            if next_url:
                # Parse the cursor from next_url
                response = self.session.get(next_url + f"&apiKey={self.api_key}", timeout=30)
                data = response.json()
            else:
                data = self._request(endpoint, params)

            results = data.get("results", [])
            if not results:
                break

            for ticker in results:
                # Filter to common stocks only (no warrants, units, etc.)
                ticker_type = ticker.get("type", "")
                if ticker_type not in ["CS", "ADRC"]:  # Common Stock or ADR
                    continue

                tickers.append({
                    "symbol": ticker.get("ticker"),
                    "name": ticker.get("name", ""),
                    "market_cap": ticker.get("market_cap", 0),
                    "primary_exchange": ticker.get("primary_exchange", ""),
                    "type": ticker_type
                })

            # Check for pagination
            next_url = data.get("next_url")
            if not next_url:
                break

            time.sleep(0.1)  # Rate limiting

        logger.info(f"[POLYGON] Retrieved {len(tickers)} US stock tickers")
        return tickers

    def get_snapshot(self, symbol: str) -> Optional[dict]:
        """Get real-time snapshot for a symbol."""
        try:
            data = self._request(f"/v2/snapshot/locale/us/markets/stocks/tickers/{symbol}")
            return data.get("ticker")
        except Exception as e:
            logger.debug(f"[POLYGON] Snapshot failed for {symbol}: {e}")
            return None

    def get_snapshots_batch(self, symbols: List[str]) -> Dict[str, dict]:
        """Get snapshots for multiple symbols."""
        results = {}

        # Polygon allows up to 50 symbols per request
        for i in range(0, len(symbols), 50):
            batch = symbols[i:i+50]
            tickers_str = ",".join(batch)

            try:
                data = self._request(f"/v2/snapshot/locale/us/markets/stocks/tickers",
                                    {"tickers": tickers_str})
                for ticker in data.get("tickers", []):
                    results[ticker.get("ticker")] = ticker
            except Exception as e:
                logger.warning(f"[POLYGON] Batch snapshot failed: {e}")

            time.sleep(0.1)

        return results

    def get_gainers_losers(self, direction: str = "gainers") -> List[dict]:
        """Get top gainers or losers."""
        try:
            data = self._request(f"/v2/snapshot/locale/us/markets/stocks/{direction}")
            return data.get("tickers", [])
        except Exception as e:
            logger.warning(f"[POLYGON] Failed to get {direction}: {e}")
            return []

    def get_bars(self, symbol: str, timeframe: str = "day",
                 from_date: str = None, to_date: str = None, limit: int = 100) -> pd.DataFrame:
        """Get historical bars for a symbol."""
        if not from_date:
            from_date = (datetime.now() - timedelta(days=limit * 2)).strftime("%Y-%m-%d")
        if not to_date:
            to_date = datetime.now().strftime("%Y-%m-%d")

        endpoint = f"/v2/aggs/ticker/{symbol}/range/1/{timeframe}/{from_date}/{to_date}"
        params = {"adjusted": "true", "sort": "asc", "limit": limit}

        try:
            data = self._request(endpoint, params)
            results = data.get("results", [])

            if not results:
                return pd.DataFrame()

            df = pd.DataFrame(results)
            df['timestamp'] = pd.to_datetime(df['t'], unit='ms')
            df = df.rename(columns={
                'o': 'open', 'h': 'high', 'l': 'low', 'c': 'close',
                'v': 'volume', 'vw': 'vwap', 'n': 'trades'
            })
            return df[['timestamp', 'open', 'high', 'low', 'close', 'volume', 'vwap']]
        except Exception as e:
            logger.warning(f"[POLYGON] Failed to get bars for {symbol}: {e}")
            return pd.DataFrame()

    def get_ticker_details(self, symbol: str) -> Optional[dict]:
        """Get detailed ticker information."""
        try:
            data = self._request(f"/v3/reference/tickers/{symbol}")
            return data.get("results")
        except Exception as e:
            logger.debug(f"[POLYGON] Ticker details failed for {symbol}: {e}")
            return None

    def get_news(self, symbol: str = None, limit: int = 10) -> List[dict]:
        """Get recent news articles."""
        params = {"limit": limit, "sort": "published_utc", "order": "desc"}
        if symbol:
            params["ticker"] = symbol

        try:
            data = self._request("/v2/reference/news", params)
            return data.get("results", [])
        except Exception as e:
            logger.warning(f"[POLYGON] Failed to get news: {e}")
            return []

    def calculate_rvol(self, symbol: str, lookback_days: int = 20) -> Optional[float]:
        """Calculate relative volume (today's volume vs average)."""
        try:
            # Get historical bars
            bars = self.get_bars(symbol, "day", limit=lookback_days + 5)
            if bars.empty or len(bars) < lookback_days:
                return None

            # Average volume over lookback period (excluding today)
            avg_vol = bars['volume'].iloc[:-1].mean()

            # Get today's volume from snapshot
            snapshot = self.get_snapshot(symbol)
            if not snapshot:
                return None

            today_vol = snapshot.get("day", {}).get("v", 0)

            if avg_vol > 0:
                return today_vol / avg_vol
            return None
        except Exception as e:
            logger.debug(f"[POLYGON] RVOL calculation failed for {symbol}: {e}")
            return None


# ============================================================
# TECHNICAL ANALYSIS
# ============================================================

class TechnicalAnalyzer:
    """Calculate technical indicators for stocks."""

    @staticmethod
    def calculate_rsi(prices: pd.Series, period: int = 14) -> float:
        """Calculate RSI."""
        if len(prices) < period + 1:
            return 50.0

        delta = prices.diff()
        gain = delta.where(delta > 0, 0).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()

        last_gain = gain.iloc[-1]
        last_loss = loss.iloc[-1]

        if last_loss == 0:
            return 100.0 if last_gain > 0 else 50.0
        if last_gain == 0:
            return 0.0

        rs = last_gain / last_loss
        rsi = 100 - (100 / (1 + rs))

        return float(rsi) if not np.isnan(rsi) else 50.0

    @staticmethod
    def calculate_sma(prices: pd.Series, period: int) -> float:
        """Calculate Simple Moving Average."""
        if len(prices) < period:
            return float(prices.iloc[-1]) if len(prices) > 0 else 0.0
        return float(prices.iloc[-period:].mean())

    @staticmethod
    def calculate_ema(prices: pd.Series, period: int) -> float:
        """Calculate Exponential Moving Average."""
        if len(prices) < period:
            return float(prices.iloc[-1]) if len(prices) > 0 else 0.0
        return float(prices.ewm(span=period, adjust=False).mean().iloc[-1])

    @staticmethod
    def calculate_atr(high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14) -> float:
        """Calculate Average True Range."""
        if len(high) < period + 1:
            return float(high.iloc[-1] - low.iloc[-1]) if len(high) > 0 else 0.0

        tr1 = high - low
        tr2 = abs(high - close.shift(1))
        tr3 = abs(low - close.shift(1))

        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        atr = tr.rolling(window=period).mean()

        return float(atr.iloc[-1]) if not np.isnan(atr.iloc[-1]) else 0.0

    @staticmethod
    def find_support_resistance(high: pd.Series, low: pd.Series, close: pd.Series,
                                lookback: int = 20) -> Tuple[float, float]:
        """Find recent support and resistance levels."""
        if len(high) < lookback:
            return float(low.min()), float(high.max())

        recent_high = high.iloc[-lookback:]
        recent_low = low.iloc[-lookback:]
        current_price = float(close.iloc[-1])

        # Support: recent swing lows below current price
        support_candidates = recent_low[recent_low < current_price]
        support = float(support_candidates.max()) if len(support_candidates) > 0 else float(recent_low.min())

        # Resistance: recent swing highs above current price
        resistance_candidates = recent_high[recent_high > current_price]
        resistance = float(resistance_candidates.min()) if len(resistance_candidates) > 0 else float(recent_high.max())

        return support, resistance

    @staticmethod
    def analyze(bars: pd.DataFrame) -> dict:
        """Run full technical analysis on price bars."""
        if bars.empty or len(bars) < 20:
            return {}

        close = bars['close']
        high = bars['high']
        low = bars['low']

        support, resistance = TechnicalAnalyzer.find_support_resistance(high, low, close)

        return {
            'rsi': TechnicalAnalyzer.calculate_rsi(close),
            'sma_20': TechnicalAnalyzer.calculate_sma(close, 20),
            'sma_50': TechnicalAnalyzer.calculate_sma(close, 50),
            'ema_9': TechnicalAnalyzer.calculate_ema(close, 9),
            'ema_21': TechnicalAnalyzer.calculate_ema(close, 21),
            'atr': TechnicalAnalyzer.calculate_atr(high, low, close),
            'support': support,
            'resistance': resistance,
            'current_price': float(close.iloc[-1]),
            '52w_high': float(high.max()),
            '52w_low': float(low.min()),
        }


# ============================================================
# UNIVERSE SCANNER
# ============================================================

class UniverseScanner:
    """
    Scans the entire US stock universe for swing trade candidates.
    Uses multiple filters to narrow down from ~8000 stocks to ~100 candidates.
    """

    def __init__(self, polygon: PolygonClient):
        self.polygon = polygon
        self.technical = TechnicalAnalyzer()
        self._all_tickers_cache: List[dict] = []
        self._cache_timestamp: float = 0
        self._cache_duration = 60 * 60 * 24  # Refresh ticker list daily

    def _get_all_tickers(self) -> List[dict]:
        """Get all tickers with caching."""
        now = time.time()
        if self._all_tickers_cache and (now - self._cache_timestamp) < self._cache_duration:
            return self._all_tickers_cache

        logger.info("[SCANNER] Refreshing full ticker universe...")
        self._all_tickers_cache = self.polygon.get_all_tickers(MIN_PRICE, MAX_PRICE)
        self._cache_timestamp = now
        return self._all_tickers_cache

    def scan_for_candidates(self, max_candidates: int = 100) -> List[SwingCandidate]:
        """
        Run full universe scan to find swing trade candidates.

        Strategy:
        1. Start with gainers/high RVOL stocks (fast pre-filter)
        2. Apply technical filters
        3. Return top candidates for LLM analysis
        """
        logger.info("[SCANNER] Starting universe scan...")
        candidates = []

        # Get pre-filtered lists from Polygon
        gainers = self.polygon.get_gainers_losers("gainers")
        logger.info(f"[SCANNER] Got {len(gainers)} gainers from Polygon")

        # Also check for high volume movers
        # We'll check snapshots of recent movers

        symbols_to_check = set()

        # Add gainers (already showing momentum)
        for g in gainers[:50]:  # Top 50 gainers
            symbol = g.get("ticker", "")
            if symbol:
                symbols_to_check.add(symbol)

        # Add some from our full universe based on sector diversity
        # This ensures we're not just chasing today's movers
        all_tickers = self._get_all_tickers()

        # Sample from different market cap tiers
        large_caps = [t for t in all_tickers if t.get("market_cap", 0) > 10_000_000_000]
        mid_caps = [t for t in all_tickers if 2_000_000_000 < t.get("market_cap", 0) <= 10_000_000_000]
        small_caps = [t for t in all_tickers if 500_000_000 < t.get("market_cap", 0) <= 2_000_000_000]

        # Add samples from each tier
        import random
        for tier in [large_caps[:100], mid_caps[:200], small_caps[:200]]:
            if tier:
                sampled = random.sample(tier, min(30, len(tier)))
                for t in sampled:
                    symbols_to_check.add(t.get("symbol"))

        logger.info(f"[SCANNER] Checking {len(symbols_to_check)} symbols...")

        # Get snapshots in batches
        symbols_list = list(symbols_to_check)
        snapshots = self.polygon.get_snapshots_batch(symbols_list)

        logger.info(f"[SCANNER] Got {len(snapshots)} snapshots")

        # Filter and build candidates
        for symbol, snapshot in snapshots.items():
            try:
                candidate = self._evaluate_snapshot(symbol, snapshot)
                if candidate:
                    candidates.append(candidate)
            except Exception as e:
                logger.debug(f"[SCANNER] Error evaluating {symbol}: {e}")
                continue

        # Sort by initial score (RVOL * gap momentum)
        candidates.sort(key=lambda c: c.rvol * abs(c.gap_pct + 1), reverse=True)

        # Return top candidates
        final_candidates = candidates[:max_candidates]
        logger.info(f"[SCANNER] Found {len(final_candidates)} candidates for LLM analysis")

        return final_candidates

    def _evaluate_snapshot(self, symbol: str, snapshot: dict) -> Optional[SwingCandidate]:
        """Evaluate a snapshot to see if it's a valid candidate."""
        if not snapshot:
            return None

        day_data = snapshot.get("day", {})
        prev_day = snapshot.get("prevDay", {})

        # Current price
        price = day_data.get("vw", 0) or day_data.get("c", 0)
        if not price or price < MIN_PRICE or price > MAX_PRICE:
            return None

        # Volume checks
        volume = day_data.get("v", 0)
        prev_volume = prev_day.get("v", 1)

        if prev_volume == 0:
            prev_volume = 1

        rvol = volume / prev_volume if prev_volume > 0 else 0

        # Dollar volume
        dollar_volume = volume * price
        if dollar_volume < MIN_DOLLAR_VOLUME:
            return None

        # RVOL filter
        if rvol < MIN_RVOL:
            return None

        # Gap calculation
        prev_close = prev_day.get("c", price)
        open_price = day_data.get("o", price)
        gap_pct = ((open_price - prev_close) / prev_close) if prev_close > 0 else 0

        # Create candidate
        candidate = SwingCandidate(
            symbol=symbol,
            price=price,
            rvol=rvol,
            avg_volume=prev_volume,
            dollar_volume=dollar_volume,
            gap_pct=gap_pct * 100,  # Convert to percentage
        )

        return candidate

    def enrich_candidate(self, candidate: SwingCandidate) -> SwingCandidate:
        """Add technical and fundamental data to a candidate."""
        symbol = candidate.symbol

        # Get historical bars for technical analysis
        bars = self.polygon.get_bars(symbol, "day", limit=60)

        if not bars.empty:
            technicals = TechnicalAnalyzer.analyze(bars)
            candidate.rsi = technicals.get('rsi', 50)
            candidate.sma_20 = technicals.get('sma_20', 0)
            candidate.sma_50 = technicals.get('sma_50', 0)
            candidate.atr = technicals.get('atr', 0)
            candidate.support = technicals.get('support', 0)
            candidate.resistance = technicals.get('resistance', 0)

        # Get ticker details for fundamentals
        details = self.polygon.get_ticker_details(symbol)
        if details:
            candidate.sector = details.get("sic_description", "Unknown")[:50]
            candidate.market_cap = details.get("market_cap", 0)
            candidate.float_shares = details.get("share_class_shares_outstanding", 0) / 1_000_000

        # Get recent news
        news = self.polygon.get_news(symbol, limit=5)
        candidate.recent_headlines = [
            n.get("title", "")[:100] for n in news if n.get("title")
        ][:5]

        return candidate


# ============================================================
# LLM ANALYZER (Claude Integration)
# ============================================================

class LLMAnalyzer:
    """
    Uses Claude AI to analyze swing trade candidates.
    Provides conviction scores, entry/exit levels, and trade thesis.
    """

    ANALYSIS_PROMPT = """You are an expert swing trader analyzing stocks for 5-15 day holding periods.

Analyze {symbol} for a potential swing trade:

CURRENT PRICE: ${price:.2f}

TECHNICAL DATA:
- RSI(14): {rsi:.1f}
- 20-day SMA: ${sma_20:.2f} | 50-day SMA: ${sma_50:.2f}
- ATR(14): ${atr:.2f} ({atr_pct:.1f}% of price)
- Support: ${support:.2f} | Resistance: ${resistance:.2f}
- RVOL: {rvol:.1f}x average volume
- Gap: {gap_pct:+.1f}%

FUNDAMENTAL DATA:
- Sector: {sector}
- Market Cap: ${market_cap_str}
- Float: {float_shares:.1f}M shares

RECENT NEWS:
{news_section}

YOUR TASK:
Analyze this stock for a 5-15 day swing trade. Consider:
1. Technical setup quality (trend, momentum, support/resistance)
2. Risk/reward ratio
3. Catalysts or news that could move the stock
4. Current market conditions

Return your analysis as JSON with this exact structure:
{{
    "conviction_score": <0-100 integer>,
    "direction": "LONG" or "AVOID",
    "entry_price": <suggested entry price as float>,
    "stop_loss": <stop loss price as float>,
    "target_price": <profit target price as float>,
    "expected_hold_days": <5-15 integer>,
    "thesis": "<1-2 sentence trade rationale>",
    "risks": ["<risk 1>", "<risk 2>"]
}}

SCORING GUIDE:
- 80-100: Exceptional setup, high confidence
- 70-79: Good setup, worth taking
- 60-69: Marginal, only in favorable conditions
- Below 60: AVOID - insufficient edge

Be conservative. Only score 70+ if you see a genuine edge."""

    def __init__(self):
        if not ANTHROPIC_API_KEY:
            logger.warning("[LLM] No ANTHROPIC_API_KEY set - LLM analysis disabled")
            self.enabled = False
            return

        self.enabled = True
        self.api_key = ANTHROPIC_API_KEY
        self.session = requests.Session()
        self.session.headers.update({
            "x-api-key": self.api_key,
            "content-type": "application/json",
            "anthropic-version": "2023-06-01"
        })

    def analyze_candidate(self, candidate: SwingCandidate) -> SwingCandidate:
        """Analyze a single candidate using Claude."""
        if not self.enabled:
            logger.warning("[LLM] Analysis skipped - API key not configured")
            return candidate

        # Format news section
        news_section = "\n".join([f"- {h}" for h in candidate.recent_headlines]) or "No recent news"

        # Format market cap
        mc = candidate.market_cap
        if mc >= 1_000_000_000:
            market_cap_str = f"{mc/1_000_000_000:.1f}B"
        elif mc >= 1_000_000:
            market_cap_str = f"{mc/1_000_000:.1f}M"
        else:
            market_cap_str = "N/A"

        # ATR as percentage
        atr_pct = (candidate.atr / candidate.price * 100) if candidate.price > 0 else 0

        # Build prompt
        prompt = self.ANALYSIS_PROMPT.format(
            symbol=candidate.symbol,
            price=candidate.price,
            rsi=candidate.rsi,
            sma_20=candidate.sma_20,
            sma_50=candidate.sma_50,
            atr=candidate.atr,
            atr_pct=atr_pct,
            support=candidate.support,
            resistance=candidate.resistance,
            rvol=candidate.rvol,
            gap_pct=candidate.gap_pct,
            sector=candidate.sector,
            market_cap_str=market_cap_str,
            float_shares=candidate.float_shares,
            news_section=news_section
        )

        try:
            response = self._call_claude(prompt)
            result = self._parse_response(response)

            if result:
                # Validate the LLM output before accepting it
                is_valid, validation_reason = self._validate_analysis(result, candidate.price)

                if not is_valid:
                    logger.warning(f"[LLM] {candidate.symbol}: Invalid analysis - {validation_reason}")
                    # Mark as AVOID if validation fails
                    candidate.conviction_score = 0
                    candidate.direction = TradeDirection.AVOID
                    candidate.thesis = f"Analysis failed validation: {validation_reason}"
                    candidate.analyzed_at = datetime.now(ET).isoformat()
                else:
                    candidate.conviction_score = result.get("conviction_score", 0)
                    candidate.direction = TradeDirection(result.get("direction", "AVOID"))
                    candidate.suggested_entry = result.get("entry_price", candidate.price)
                    candidate.suggested_stop = result.get("stop_loss", candidate.price * 0.93)
                    candidate.suggested_target = result.get("target_price", candidate.price * 1.15)
                    candidate.expected_hold_days = result.get("expected_hold_days", 10)
                    candidate.thesis = result.get("thesis", "")
                    candidate.risks = result.get("risks", [])
                    candidate.analyzed_at = datetime.now(ET).isoformat()

                    logger.info(f"[LLM] {candidate.symbol}: Score={candidate.conviction_score}, "
                               f"Direction={candidate.direction.value}, Thesis: {candidate.thesis[:50]}...")
        except Exception as e:
            logger.error(f"[LLM] Analysis failed for {candidate.symbol}: {e}")

        return candidate

    def analyze_batch(self, candidates: List[SwingCandidate],
                      max_concurrent: int = 5) -> List[SwingCandidate]:
        """Analyze multiple candidates with rate limiting."""
        logger.info(f"[LLM] Analyzing {len(candidates)} candidates...")

        analyzed = []

        for i, candidate in enumerate(candidates):
            try:
                analyzed_candidate = self.analyze_candidate(candidate)
                analyzed.append(analyzed_candidate)

                # Rate limiting
                if i < len(candidates) - 1:
                    time.sleep(LLM_RATE_LIMIT_DELAY)

                # Progress logging
                if (i + 1) % 10 == 0:
                    logger.info(f"[LLM] Analyzed {i + 1}/{len(candidates)} candidates")

            except Exception as e:
                logger.error(f"[LLM] Error analyzing {candidate.symbol}: {e}")
                analyzed.append(candidate)

        return analyzed

    def _call_claude(self, prompt: str) -> str:
        """Make API call to Claude."""
        payload = {
            "model": LLM_MODEL,
            "max_tokens": LLM_MAX_TOKENS,
            "temperature": LLM_TEMPERATURE,
            "messages": [
                {"role": "user", "content": prompt}
            ]
        }

        response = self.session.post(
            "https://api.anthropic.com/v1/messages",
            json=payload,
            timeout=60
        )
        response.raise_for_status()

        data = response.json()
        return data.get("content", [{}])[0].get("text", "")

    def _parse_response(self, response: str) -> Optional[dict]:
        """Parse JSON response from Claude."""
        try:
            # Try to extract JSON from response
            # Claude sometimes wraps JSON in markdown code blocks
            if "```json" in response:
                start = response.find("```json") + 7
                end = response.find("```", start)
                response = response[start:end]
            elif "```" in response:
                start = response.find("```") + 3
                end = response.find("```", start)
                response = response[start:end]

            return json.loads(response.strip())
        except json.JSONDecodeError as e:
            logger.warning(f"[LLM] Failed to parse response: {e}")
            logger.debug(f"[LLM] Raw response: {response[:500]}")
            return None

    def _validate_analysis(self, result: dict, current_price: float) -> Tuple[bool, str]:
        """
        Validate LLM analysis output for sanity.

        Returns (is_valid, reason)
        """
        MIN_RR_RATIO = 1.5  # Minimum reward-to-risk ratio

        entry = result.get("entry_price", 0)
        stop = result.get("stop_loss", 0)
        target = result.get("target_price", 0)
        direction = result.get("direction", "AVOID")

        # Skip validation for AVOID recommendations
        if direction == "AVOID":
            return True, "AVOID - no validation needed"

        # Validate prices are positive
        if entry <= 0 or stop <= 0 or target <= 0:
            return False, f"Invalid prices: entry={entry}, stop={stop}, target={target}"

        # For LONG: stop must be below entry, target must be above entry
        if direction == "LONG":
            if stop >= entry:
                return False, f"Stop ({stop}) must be below entry ({entry}) for LONG"

            if target <= entry:
                return False, f"Target ({target}) must be above entry ({entry}) for LONG"

            # Check minimum R:R ratio
            risk = entry - stop
            reward = target - entry
            if risk > 0:
                rr_ratio = reward / risk
                if rr_ratio < MIN_RR_RATIO:
                    return False, f"R:R ratio {rr_ratio:.2f} below minimum {MIN_RR_RATIO}"

            # Check stop isn't too far from entry (max 15% for swing trades)
            stop_pct = (entry - stop) / entry
            if stop_pct > 0.15:
                return False, f"Stop too wide ({stop_pct*100:.1f}% from entry)"

            # Check entry is reasonable vs current price (within 5%)
            entry_diff_pct = abs(entry - current_price) / current_price
            if entry_diff_pct > 0.05:
                return False, f"Entry ({entry}) too far from current price ({current_price})"

        return True, "OK"


# ============================================================
# PORTFOLIO MANAGER
# ============================================================

class PortfolioManager:
    """
    Manages position sizing, entry/exit execution, and risk management.
    """

    def __init__(self, alpaca: AlpacaClients, state: BotState):
        self.alpaca = alpaca
        self.state = state

    def calculate_position_size(self, candidate: SwingCandidate, equity: float) -> int:
        """
        Calculate position size based on:
        - Conviction score
        - Volatility (ATR)
        - Portfolio constraints

        Returns 0 if position cannot be taken (sector cap, insufficient equity, etc.)
        """
        if equity <= 0 or candidate.price <= 0:
            return 0

        # Base size from conviction score
        score = candidate.conviction_score
        if score >= 85:
            size_pct = MAX_POSITION_SIZE_PCT
        elif score >= 75:
            size_pct = POSITION_SIZE_PCT * 1.2
        elif score >= 70:
            size_pct = POSITION_SIZE_PCT
        else:
            size_pct = MIN_POSITION_SIZE_PCT

        # Adjust for volatility (reduce size for high ATR stocks)
        if candidate.atr > 0:
            atr_pct = candidate.atr / candidate.price
            if atr_pct > 0.05:  # >5% daily ATR
                size_pct *= 0.7
            elif atr_pct > 0.03:  # >3% daily ATR
                size_pct *= 0.85

        # Check sector exposure - if sector is maxed out, return 0
        sector = candidate.sector
        current_sector_exposure = self.state.sector_exposure.get(sector, 0)
        if current_sector_exposure >= MAX_SECTOR_EXPOSURE_PCT:
            logger.debug(f"[PORTFOLIO] {candidate.symbol}: Sector {sector} at max exposure ({current_sector_exposure*100:.1f}%)")
            return 0

        # Cap size_pct to remaining sector capacity
        remaining_sector_capacity = MAX_SECTOR_EXPOSURE_PCT - current_sector_exposure
        size_pct = min(size_pct, remaining_sector_capacity)

        # Calculate dollar amount and shares
        dollar_amount = equity * size_pct
        shares = int(dollar_amount / candidate.price)

        # If calculated shares is 0 after all adjustments, don't force to 1
        # This respects the caps and prevents tiny/unauthorized positions
        return shares

    def should_enter_position(self, candidate: SwingCandidate) -> Tuple[bool, str]:
        """
        Check if we should enter a position.
        Returns (should_enter, reason)
        """
        # Check daily loss circuit breaker
        daily_loss_exceeded, daily_pnl_pct = self._check_daily_loss_circuit_breaker()
        if daily_loss_exceeded:
            return False, f"Daily loss circuit breaker triggered ({daily_pnl_pct*100:.1f}% loss)"

        # Check score threshold
        if candidate.conviction_score < MIN_CONVICTION_SCORE:
            return False, f"Score {candidate.conviction_score} below threshold {MIN_CONVICTION_SCORE}"

        # Check direction
        if candidate.direction != TradeDirection.LONG:
            return False, f"Direction is {candidate.direction.value}"

        # Check max positions
        active_positions = len([p for p in self.state.positions.values()
                               if p.get('status') == PositionStatus.ACTIVE.value])
        if active_positions >= MAX_POSITIONS:
            return False, f"Max positions reached ({MAX_POSITIONS})"

        # Check if already holding
        if candidate.symbol in self.state.positions:
            return False, "Already holding position"

        # Check trading halt
        if self.state.trading_halted:
            return False, f"Trading halted: {self.state.halt_reason}"

        return True, "OK"

    def _check_daily_loss_circuit_breaker(self) -> Tuple[bool, float]:
        """
        Check if daily loss exceeds the circuit breaker threshold.

        Returns (is_exceeded, daily_pnl_pct)
        """
        try:
            current_equity = self.alpaca.get_equity()

            # Update daily starting equity if it's a new day
            today = datetime.now(ET).date().isoformat()
            if not hasattr(self, '_last_daily_check_date') or self._last_daily_check_date != today:
                self._last_daily_check_date = today
                # Reset daily starting equity at start of new day
                if self.state.daily_starting_equity == 0:
                    self.state.daily_starting_equity = current_equity
                    save_state(self.state)

            if self.state.daily_starting_equity > 0:
                daily_pnl_pct = (current_equity - self.state.daily_starting_equity) / self.state.daily_starting_equity

                if daily_pnl_pct <= -MAX_DAILY_LOSS_PCT:
                    if not self.state.trading_halted:
                        self.state.trading_halted = True
                        self.state.halt_reason = f"Daily loss exceeded {MAX_DAILY_LOSS_PCT*100:.1f}%"
                        save_state(self.state)
                        logger.warning(f"[RISK] CIRCUIT BREAKER: Daily loss {daily_pnl_pct*100:.1f}% "
                                      f"exceeds limit {MAX_DAILY_LOSS_PCT*100:.1f}%")
                    return True, daily_pnl_pct

                # Reset halt if we've recovered
                if self.state.trading_halted and "Daily loss" in (self.state.halt_reason or ""):
                    if daily_pnl_pct > -MAX_DAILY_LOSS_PCT * 0.5:  # Recovered to half the threshold
                        self.state.trading_halted = False
                        self.state.halt_reason = None
                        save_state(self.state)
                        logger.info("[RISK] Circuit breaker reset - daily P&L recovered")

                return False, daily_pnl_pct

        except Exception as e:
            logger.error(f"[RISK] Failed to check daily loss: {e}")

        return False, 0.0

    def enter_position(self, candidate: SwingCandidate, shares: int) -> Optional[Position]:
        """Submit entry order for a candidate."""
        symbol = candidate.symbol

        logger.info(f"[PORTFOLIO] Entering {symbol}: {shares} shares @ ~${candidate.price:.2f}")

        if DRY_RUN:
            logger.info(f"[DRY-RUN] Would buy {shares} {symbol}")
            return None

        try:
            # Use limit order at suggested entry (or slightly above current price)
            limit_price = min(candidate.suggested_entry, candidate.price * 1.005)

            order_request = LimitOrderRequest(
                symbol=symbol,
                qty=shares,
                side=OrderSide.BUY,
                time_in_force=TimeInForce.DAY,
                limit_price=round(limit_price, 2)
            )

            order = self.alpaca.trading.submit_order(order_request)

            # Create position record
            position = Position(
                symbol=symbol,
                entry_price=limit_price,
                entry_date=datetime.now(ET).isoformat(),
                shares=shares,
                direction=TradeDirection.LONG,
                status=PositionStatus.PENDING,
                stop_loss=candidate.suggested_stop,
                target_price=candidate.suggested_target,
                conviction_score=candidate.conviction_score,
                thesis=candidate.thesis,
                sector=candidate.sector,
                entry_order_id=str(order.id)
            )

            # Save to state
            self.state.positions[symbol] = position.to_dict()

            # Update sector exposure
            equity = self.alpaca.get_equity()
            position_value = shares * limit_price
            sector_pct = position_value / equity if equity > 0 else 0
            self.state.sector_exposure[candidate.sector] = \
                self.state.sector_exposure.get(candidate.sector, 0) + sector_pct

            save_state(self.state)

            logger.info(f"[PORTFOLIO] Order submitted: {order.id} | "
                       f"Buy {shares} {symbol} @ ${limit_price:.2f}")

            return position

        except Exception as e:
            logger.error(f"[PORTFOLIO] Failed to enter {symbol}: {e}")
            return None

    def exit_position(self, symbol: str, reason: ExitReason,
                      current_price: Optional[float] = None) -> bool:
        """
        Exit a position.

        Args:
            symbol: Stock symbol to exit
            reason: Why we're exiting
            current_price: Optional current price for P&L calculation in dry run
        """
        if symbol not in self.state.positions:
            logger.warning(f"[PORTFOLIO] No position found for {symbol}")
            return False

        position_dict = self.state.positions[symbol]
        position = Position.from_dict(position_dict)

        logger.info(f"[PORTFOLIO] Exiting {symbol}: {reason.value}")

        if DRY_RUN:
            logger.info(f"[DRY-RUN] Would sell {position.shares} {symbol}")

            # In dry run, simulate the exit completion
            exit_price = current_price or position.entry_price
            pnl = (exit_price - position.entry_price) * position.shares

            # Reduce sector exposure
            equity = self.alpaca.get_equity()
            if equity > 0:
                position_value = position.shares * position.entry_price
                sector_pct = position_value / equity
                current_exposure = self.state.sector_exposure.get(position.sector, 0)
                self.state.sector_exposure[position.sector] = max(0, current_exposure - sector_pct)

            # Update stats
            self.state.total_pnl += pnl
            if pnl > 0:
                self.state.winning_trades += 1
            else:
                self.state.losing_trades += 1

            # Move to closed
            position.status = PositionStatus.CLOSED
            position.exit_price = exit_price
            position.exit_date = datetime.now(ET).isoformat()
            position.exit_reason = reason
            self.state.closed_positions.append(position.to_dict())
            del self.state.positions[symbol]
            save_state(self.state)

            return True

        try:
            order_request = MarketOrderRequest(
                symbol=symbol,
                qty=position.shares,
                side=OrderSide.SELL,
                time_in_force=TimeInForce.DAY
            )

            order = self.alpaca.trading.submit_order(order_request)

            # Update position to EXITING (actual close happens in _on_order_update)
            position.status = PositionStatus.EXITING
            position.exit_order_id = str(order.id)
            position.exit_reason = reason
            self.state.positions[symbol] = position.to_dict()

            save_state(self.state)

            logger.info(f"[PORTFOLIO] Exit order submitted: {order.id} | "
                       f"Sell {position.shares} {symbol}")

            return True

        except Exception as e:
            logger.error(f"[PORTFOLIO] Failed to exit {symbol}: {e}")
            return False

    def update_position_status(self, symbol: str, current_price: float) -> None:
        """Update position P&L and check exit conditions."""
        if symbol not in self.state.positions:
            return

        position_dict = self.state.positions[symbol]
        position = Position.from_dict(position_dict)

        if position.status != PositionStatus.ACTIVE:
            return

        # Update unrealized P&L
        position.unrealized_pnl = (current_price - position.entry_price) * position.shares
        position.unrealized_pnl_pct = (current_price - position.entry_price) / position.entry_price

        # Update high water mark
        position.high_water_mark = max(position.high_water_mark, current_price)

        # Check trailing stop activation
        if not position.trailing_stop_active:
            gain_pct = (current_price - position.entry_price) / position.entry_price
            if gain_pct >= TRAILING_STOP_ACTIVATION_PCT:
                position.trailing_stop_active = True
                position.trailing_stop_price = current_price * (1 - TRAILING_STOP_DISTANCE_PCT)
                logger.info(f"[PORTFOLIO] {symbol}: Trailing stop activated @ ${position.trailing_stop_price:.2f}")

        # Update trailing stop price
        if position.trailing_stop_active:
            new_trail = current_price * (1 - TRAILING_STOP_DISTANCE_PCT)
            if new_trail > position.trailing_stop_price:
                position.trailing_stop_price = new_trail

        # Save updates
        self.state.positions[symbol] = position.to_dict()

        # Check exit conditions
        exit_reason = self._check_exit_conditions(position, current_price)
        if exit_reason:
            self.exit_position(symbol, exit_reason)

    def _check_exit_conditions(self, position: Position, current_price: float) -> Optional[ExitReason]:
        """Check if position should be exited."""
        # Stop loss
        if current_price <= position.stop_loss:
            return ExitReason.STOP_LOSS

        # Profit target
        if current_price >= position.target_price:
            return ExitReason.TARGET_HIT

        # Trailing stop
        if position.trailing_stop_active and position.trailing_stop_price:
            if current_price <= position.trailing_stop_price:
                return ExitReason.TRAILING_STOP

        # Time-based exit
        if position.days_held() >= MAX_HOLD_DAYS:
            return ExitReason.TIME_EXIT

        return None


# ============================================================
# ALERT MANAGER
# ============================================================

class AlertManager:
    """
    Manages alerts via various channels (console, email, SMS placeholder).
    """

    def __init__(self):
        self._alert_log: List[Dict] = []
        self._last_alerts: Dict[str, float] = {}  # Prevent alert spam
        self._min_alert_interval = 300  # 5 minutes between same alerts

    def send_alert(self, alert_type: str, symbol: str, message: str,
                   priority: str = "normal", data: Dict = None):
        """Send an alert through configured channels."""
        alert_key = f"{alert_type}:{symbol}"
        now = time.time()

        # Prevent spam
        if alert_key in self._last_alerts:
            if now - self._last_alerts[alert_key] < self._min_alert_interval:
                return

        self._last_alerts[alert_key] = now

        # Build alert
        alert = {
            "timestamp": datetime.now(ET).isoformat(),
            "type": alert_type,
            "symbol": symbol,
            "message": message,
            "priority": priority,
            "data": data or {}
        }

        self._alert_log.append(alert)

        # Log to console with color coding
        priority_colors = {
            "critical": "\033[91m",  # Red
            "high": "\033[93m",      # Yellow
            "normal": "\033[94m",    # Blue
            "low": "\033[90m"        # Gray
        }
        color = priority_colors.get(priority, "")
        reset = "\033[0m"

        logger.info(f"{color}[ALERT:{priority.upper()}] {symbol} - {message}{reset}")

        # Future: Add email/SMS/webhook integrations here
        # self._send_email(alert)
        # self._send_sms(alert)
        # self._send_webhook(alert)

    def get_recent_alerts(self, count: int = 10) -> List[Dict]:
        """Get most recent alerts."""
        return self._alert_log[-count:]


# ============================================================
# REAL-TIME WEBSOCKET MONITOR
# ============================================================

class RealTimeMonitor:
    """
    Real-time price and order monitoring using Alpaca WebSocket streams.

    Features:
    - Real-time price updates for held positions
    - Instant stop-loss and take-profit triggers
    - Trade/order status updates
    - Volume spike detection
    """

    def __init__(self, alpaca: 'AlpacaClients', state: 'BotState',
                 portfolio: 'PortfolioManager', alert_manager: 'AlertManager'):
        self.alpaca = alpaca
        self.state = state
        self.portfolio = portfolio
        self.alerts = alert_manager

        # Current prices (updated by WebSocket)
        self._prices: Dict[str, float] = {}
        self._last_update: Dict[str, float] = {}

        # WebSocket streams
        self._data_stream: Optional[StockDataStream] = None
        self._trade_stream: Optional[TradingStream] = None
        self._stream_thread: Optional[threading.Thread] = None
        self._running = False

        # Price thresholds for alerts
        self._price_alert_thresholds: Dict[str, Dict] = {}

    def start(self):
        """Start the WebSocket streams in background threads."""
        if self._running:
            logger.warning("[MONITOR] Already running")
            return

        self._running = True
        logger.info("[MONITOR] Starting real-time monitoring...")

        # Start data stream in thread
        self._stream_thread = threading.Thread(target=self._run_streams, daemon=True)
        self._stream_thread.start()

    def stop(self):
        """Stop the WebSocket streams."""
        self._running = False

        if self._data_stream:
            try:
                self._data_stream.stop()
            except Exception:
                pass

        if self._trade_stream:
            try:
                self._trade_stream.stop()
            except Exception:
                pass

        logger.info("[MONITOR] Stopped real-time monitoring")

    def _run_streams(self):
        """Run WebSocket streams (blocking - runs in thread)."""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        try:
            loop.run_until_complete(self._async_run_streams())
        except Exception as e:
            logger.error(f"[MONITOR] Stream error: {e}")
        finally:
            loop.close()

    async def _async_run_streams(self):
        """Async stream runner."""
        # Initialize data stream for price updates
        self._data_stream = StockDataStream(
            ALPACA_API_KEY,
            ALPACA_SECRET_KEY,
            feed="iex"  # or "sip" for premium
        )

        # Initialize trading stream for order updates
        self._trade_stream = TradingStream(
            ALPACA_API_KEY,
            ALPACA_SECRET_KEY,
            paper=ALPACA_PAPER
        )

        # Register handlers
        @self._data_stream.on_bar
        async def handle_bar(bar):
            await self._on_bar(bar)

        @self._data_stream.on_trade
        async def handle_trade(trade):
            await self._on_trade(trade)

        @self._trade_stream.on_trade_update
        async def handle_trade_update(update):
            await self._on_order_update(update)

        # Subscribe to symbols we're tracking
        await self._update_subscriptions()

        # Run both streams
        try:
            # Start trade stream in background
            asyncio.create_task(self._trade_stream._run())

            # Run data stream (blocking)
            await self._data_stream._run()
        except asyncio.CancelledError:
            pass

    async def _update_subscriptions(self):
        """Update WebSocket subscriptions based on current positions."""
        symbols = list(self.state.positions.keys())

        if not symbols:
            logger.debug("[MONITOR] No positions to monitor")
            return

        try:
            # Subscribe to bars and trades for held positions
            self._data_stream.subscribe_bars(*symbols)
            self._data_stream.subscribe_trades(*symbols)
            logger.info(f"[MONITOR] Subscribed to {len(symbols)} symbols: {', '.join(symbols)}")

            # Set up alert thresholds
            for symbol in symbols:
                pos_dict = self.state.positions.get(symbol)
                if pos_dict:
                    position = Position.from_dict(pos_dict)
                    self._price_alert_thresholds[symbol] = {
                        "stop_loss": position.stop_loss,
                        "take_profit": position.target_price,
                        "trailing_stop": position.trailing_stop_price,
                        "entry": position.entry_price
                    }
        except Exception as e:
            logger.error(f"[MONITOR] Subscription error: {e}")

    async def _on_bar(self, bar):
        """Handle incoming bar data."""
        symbol = bar.symbol
        price = bar.close

        self._prices[symbol] = price
        self._last_update[symbol] = time.time()

        # Check for exit triggers
        await self._check_exit_triggers(symbol, price)

    async def _on_trade(self, trade):
        """Handle incoming trade data (more granular than bars)."""
        symbol = trade.symbol
        price = trade.price

        self._prices[symbol] = price
        self._last_update[symbol] = time.time()

        # For high-priority monitoring (stops), check on every trade
        thresholds = self._price_alert_thresholds.get(symbol)
        if thresholds:
            stop_loss = thresholds.get("stop_loss")
            if stop_loss and price <= stop_loss:
                await self._trigger_stop_loss(symbol, price)

    async def _check_exit_triggers(self, symbol: str, current_price: float):
        """Check if any exit conditions are met."""
        pos_dict = self.state.positions.get(symbol)
        if not pos_dict:
            return

        position = Position.from_dict(pos_dict)

        if position.status != PositionStatus.ACTIVE:
            return

        # Check exit conditions
        exit_reason = self.portfolio._check_exit_conditions(position, current_price)

        if exit_reason:
            # Send alert
            pnl_pct = (current_price - position.entry_price) / position.entry_price * 100
            self.alerts.send_alert(
                alert_type="EXIT_TRIGGER",
                symbol=symbol,
                message=f"{exit_reason.value} triggered at ${current_price:.2f} (P&L: {pnl_pct:+.1f}%)",
                priority="high",
                data={
                    "reason": exit_reason.value,
                    "price": current_price,
                    "entry_price": position.entry_price,
                    "pnl_pct": pnl_pct
                }
            )

            # Execute the exit
            try:
                self.portfolio.exit_position(symbol, exit_reason)
            except Exception as e:
                logger.error(f"[MONITOR] Failed to exit {symbol}: {e}")

    async def _trigger_stop_loss(self, symbol: str, price: float):
        """Immediately trigger stop loss (time-sensitive)."""
        pos_dict = self.state.positions.get(symbol)
        if not pos_dict:
            return

        position = Position.from_dict(pos_dict)

        if position.status != PositionStatus.ACTIVE:
            return

        pnl_pct = (price - position.entry_price) / position.entry_price * 100

        self.alerts.send_alert(
            alert_type="STOP_LOSS",
            symbol=symbol,
            message=f"STOP LOSS HIT at ${price:.2f} (Loss: {pnl_pct:.1f}%)",
            priority="critical",
            data={"price": price, "stop": position.stop_loss}
        )

        # Execute exit immediately
        try:
            self.portfolio.exit_position(symbol, ExitReason.STOP_LOSS)
        except Exception as e:
            logger.error(f"[MONITOR] Stop loss exit failed for {symbol}: {e}")

    async def _on_order_update(self, update):
        """Handle order status updates from trading stream."""
        event = update.event
        order = update.order

        symbol = order.symbol
        order_id = str(order.id)
        side = str(order.side).upper()

        logger.info(f"[MONITOR] Order update: {symbol} - {event} (side={side})")

        if event == "fill":
            fill_price = float(order.filled_avg_price) if order.filled_avg_price else 0
            fill_qty = float(order.filled_qty) if order.filled_qty else float(order.qty)

            self.alerts.send_alert(
                alert_type="ORDER_FILL",
                symbol=symbol,
                message=f"Order filled: {fill_qty} shares at ${fill_price:.2f}",
                priority="normal",
                data={
                    "order_id": order_id,
                    "side": side,
                    "qty": fill_qty,
                    "price": fill_price
                }
            )

            # Update position state based on order side
            pos_dict = self.state.positions.get(symbol)
            if pos_dict:
                position = Position.from_dict(pos_dict)

                if side == "BUY" and position.status == PositionStatus.PENDING:
                    # Entry order filled - transition to ACTIVE
                    position.status = PositionStatus.ACTIVE
                    position.entry_price = fill_price  # Update with actual fill price
                    position.high_water_mark = fill_price
                    self.state.positions[symbol] = position.to_dict()
                    save_state(self.state)
                    logger.info(f"[MONITOR] {symbol}: Position now ACTIVE at ${fill_price:.2f}")

                elif side == "SELL" and position.status == PositionStatus.EXITING:
                    # Exit order filled - close position
                    position.status = PositionStatus.CLOSED
                    position.exit_price = fill_price
                    position.exit_date = datetime.now(ET).isoformat()

                    # Calculate P&L
                    pnl = (fill_price - position.entry_price) * position.shares
                    pnl_pct = (fill_price - position.entry_price) / position.entry_price

                    # Update statistics
                    self.state.total_pnl += pnl
                    if pnl > 0:
                        self.state.winning_trades += 1
                    else:
                        self.state.losing_trades += 1

                    # Reduce sector exposure
                    equity = self.alpaca.get_equity()
                    if equity > 0:
                        position_value = position.shares * position.entry_price
                        sector_pct = position_value / equity
                        current_exposure = self.state.sector_exposure.get(position.sector, 0)
                        self.state.sector_exposure[position.sector] = max(0, current_exposure - sector_pct)

                    # Move to closed positions
                    self.state.closed_positions.append(position.to_dict())
                    del self.state.positions[symbol]
                    save_state(self.state)

                    # Remove from monitoring
                    self.remove_symbol(symbol)

                    logger.info(f"[MONITOR] {symbol}: Position CLOSED at ${fill_price:.2f} "
                               f"(P&L: ${pnl:+.2f} / {pnl_pct*100:+.1f}%)")

        elif event == "canceled":
            self.alerts.send_alert(
                alert_type="ORDER_CANCELED",
                symbol=symbol,
                message=f"Order canceled",
                priority="low"
            )

            # If entry order canceled, remove pending position
            pos_dict = self.state.positions.get(symbol)
            if pos_dict:
                position = Position.from_dict(pos_dict)
                if position.status == PositionStatus.PENDING and position.entry_order_id == order_id:
                    del self.state.positions[symbol]
                    save_state(self.state)
                    logger.info(f"[MONITOR] {symbol}: Pending position removed (entry canceled)")

        elif event == "rejected":
            self.alerts.send_alert(
                alert_type="ORDER_REJECTED",
                symbol=symbol,
                message=f"Order REJECTED: {order.reject_reason}",
                priority="high",
                data={"reason": order.reject_reason}
            )

            # If entry order rejected, remove pending position
            pos_dict = self.state.positions.get(symbol)
            if pos_dict:
                position = Position.from_dict(pos_dict)
                if position.status == PositionStatus.PENDING and position.entry_order_id == order_id:
                    del self.state.positions[symbol]
                    save_state(self.state)
                    logger.warning(f"[MONITOR] {symbol}: Pending position removed (entry rejected)")

    def get_current_price(self, symbol: str) -> Optional[float]:
        """Get the latest price from WebSocket (or None if stale)."""
        price = self._prices.get(symbol)
        last_update = self._last_update.get(symbol, 0)

        # Consider stale if > 60 seconds old
        if time.time() - last_update > 60:
            return None

        return price

    def add_symbol(self, symbol: str, position: Position):
        """Add a new symbol to monitor (called when entering position)."""
        self._price_alert_thresholds[symbol] = {
            "stop_loss": position.stop_loss,
            "take_profit": position.target_price,
            "trailing_stop": position.trailing_stop_price,
            "entry": position.entry_price
        }

        # Subscribe to the new symbol
        if self._data_stream and self._running:
            try:
                self._data_stream.subscribe_bars(symbol)
                self._data_stream.subscribe_trades(symbol)
                logger.info(f"[MONITOR] Added {symbol} to real-time monitoring")
            except Exception as e:
                logger.error(f"[MONITOR] Failed to subscribe to {symbol}: {e}")

    def remove_symbol(self, symbol: str):
        """Remove a symbol from monitoring (called when exiting position)."""
        self._price_alert_thresholds.pop(symbol, None)
        self._prices.pop(symbol, None)
        self._last_update.pop(symbol, None)

        # Unsubscribe (if supported)
        if self._data_stream and self._running:
            try:
                self._data_stream.unsubscribe_bars(symbol)
                self._data_stream.unsubscribe_trades(symbol)
                logger.info(f"[MONITOR] Removed {symbol} from monitoring")
            except Exception as e:
                logger.debug(f"[MONITOR] Unsubscribe failed for {symbol}: {e}")

    def update_trailing_stop(self, symbol: str, new_stop: float):
        """Update trailing stop price for a symbol."""
        if symbol in self._price_alert_thresholds:
            self._price_alert_thresholds[symbol]["trailing_stop"] = new_stop


# ============================================================
# MAIN BOT CLASS
# ============================================================

class SwingTraderBot:
    """
    Main bot orchestrator that coordinates all components.
    """

    def __init__(self):
        logger.info("=" * 60)
        logger.info("AI SWING TRADER v1.0 STARTING")
        logger.info("=" * 60)

        # Initialize components
        self.alpaca = AlpacaClients()
        self.polygon = PolygonClient()
        self.scanner = UniverseScanner(self.polygon)
        self.llm = LLMAnalyzer()
        self.state = load_state()
        self.portfolio = PortfolioManager(self.alpaca, self.state)

        # Alert manager
        self.alerts = AlertManager()

        # Real-time monitor (initialized after portfolio)
        self.monitor = RealTimeMonitor(
            self.alpaca, self.state, self.portfolio, self.alerts
        )

        # Threading
        self._running = True
        self._lock = threading.Lock()

        # Verify account
        self._verify_account()

        # Initialize state
        if self.state.starting_equity == 0:
            self.state.starting_equity = self.alpaca.get_equity()
            self.state.equity_high_water_mark = self.state.starting_equity
            self.state.daily_starting_equity = self.state.starting_equity
            save_state(self.state)

        # Reconcile positions with Alpaca on startup
        self._reconcile_positions()

    def _verify_account(self):
        """Verify Alpaca account status."""
        try:
            account = self.alpaca.get_account()

            logger.info("=" * 60)
            logger.info("[ACCOUNT] Verification")
            logger.info(f"  Account ID: {account.id}")
            logger.info(f"  Status: {account.status}")
            logger.info(f"  Equity: ${float(account.equity):,.2f}")
            logger.info(f"  Buying Power: ${float(account.buying_power):,.2f}")
            logger.info(f"  Day Trade Count: {account.daytrade_count}")
            logger.info(f"  Pattern Day Trader: {account.pattern_day_trader}")
            logger.info(f"  Paper Trading: {ALPACA_PAPER}")
            logger.info("=" * 60)

            if account.trading_blocked:
                raise RuntimeError("Trading is blocked on this account")

        except Exception as e:
            logger.error(f"[ACCOUNT] Verification failed: {e}")
            raise

    def _reconcile_positions(self):
        """
        Reconcile internal state with Alpaca's actual positions.

        This handles:
        1. Positions in Alpaca but not in our state (orphaned positions)
        2. Positions in our state but not in Alpaca (ghost positions)
        3. Positions in PENDING status that have actually filled
        """
        logger.info("[RECONCILE] Starting position reconciliation...")

        # Get actual positions from Alpaca
        try:
            alpaca_positions = self.alpaca.get_positions()
        except Exception as e:
            logger.error(f"[RECONCILE] Failed to fetch Alpaca positions: {e}")
            return

        alpaca_symbols = set(alpaca_positions.keys())
        our_symbols = set(self.state.positions.keys())

        # 1. Find orphaned positions (in Alpaca but not our state)
        orphaned = alpaca_symbols - our_symbols
        for symbol in orphaned:
            alpaca_pos = alpaca_positions[symbol]
            logger.warning(f"[RECONCILE] Found orphaned position: {symbol} "
                          f"({alpaca_pos.qty} shares @ ${float(alpaca_pos.avg_entry_price):.2f})")

            # Create a minimal position record for tracking
            # (We don't have the original entry info, so use current data)
            position = Position(
                symbol=symbol,
                entry_price=float(alpaca_pos.avg_entry_price),
                entry_date=datetime.now(ET).isoformat(),
                shares=int(float(alpaca_pos.qty)),
                direction=TradeDirection.LONG,
                status=PositionStatus.ACTIVE,
                stop_loss=float(alpaca_pos.avg_entry_price) * (1 - DEFAULT_STOP_LOSS_PCT),
                target_price=float(alpaca_pos.avg_entry_price) * (1 + DEFAULT_TARGET_PCT),
                conviction_score=50,  # Unknown
                thesis="Position reconciled from Alpaca (pre-existing)",
                sector="Unknown"
            )
            self.state.positions[symbol] = position.to_dict()
            logger.info(f"[RECONCILE] Added {symbol} to tracking")

        # 2. Find ghost positions (in our state but not in Alpaca)
        ghost = our_symbols - alpaca_symbols
        for symbol in ghost:
            pos_dict = self.state.positions[symbol]
            position = Position.from_dict(pos_dict)

            # If position is PENDING or EXITING, it might still be in flight
            # Only remove if it's supposed to be ACTIVE
            if position.status == PositionStatus.ACTIVE:
                logger.warning(f"[RECONCILE] Ghost position detected: {symbol} "
                              f"(marked ACTIVE but not in Alpaca)")

                # Assume it was closed externally - move to closed positions
                position.status = PositionStatus.CLOSED
                position.exit_date = datetime.now(ET).isoformat()
                position.exit_reason = ExitReason.MANUAL
                self.state.closed_positions.append(position.to_dict())
                del self.state.positions[symbol]
                logger.info(f"[RECONCILE] Moved {symbol} to closed positions (external close)")

            elif position.status == PositionStatus.PENDING:
                # Entry never filled - just remove
                logger.info(f"[RECONCILE] Removing unfilled PENDING position: {symbol}")
                del self.state.positions[symbol]

        # 3. Update PENDING positions that have filled
        for symbol in (our_symbols & alpaca_symbols):
            pos_dict = self.state.positions[symbol]
            position = Position.from_dict(pos_dict)
            alpaca_pos = alpaca_positions[symbol]

            if position.status == PositionStatus.PENDING:
                # Position has filled but we missed the WebSocket update
                logger.info(f"[RECONCILE] Promoting PENDING to ACTIVE: {symbol}")
                position.status = PositionStatus.ACTIVE
                position.entry_price = float(alpaca_pos.avg_entry_price)
                position.shares = int(float(alpaca_pos.qty))
                position.high_water_mark = float(alpaca_pos.current_price)
                self.state.positions[symbol] = position.to_dict()

        # Rebuild sector exposure from reconciled positions
        self._rebuild_sector_exposure()

        save_state(self.state)
        logger.info(f"[RECONCILE] Complete. Tracking {len(self.state.positions)} positions")

    def _rebuild_sector_exposure(self):
        """Rebuild sector exposure tracking from current positions."""
        equity = self.alpaca.get_equity()
        if equity <= 0:
            return

        self.state.sector_exposure = {}

        for symbol, pos_dict in self.state.positions.items():
            position = Position.from_dict(pos_dict)
            if position.status in (PositionStatus.ACTIVE, PositionStatus.PENDING):
                position_value = position.shares * position.entry_price
                sector_pct = position_value / equity
                current = self.state.sector_exposure.get(position.sector, 0)
                self.state.sector_exposure[position.sector] = current + sector_pct

        logger.debug(f"[RECONCILE] Sector exposure: {self.state.sector_exposure}")

    def run_scan(self) -> List[SwingCandidate]:
        """Run full universe scan and LLM analysis."""
        logger.info("[BOT] Starting full scan cycle...")

        # Phase 1: Universe scan
        candidates = self.scanner.scan_for_candidates(max_candidates=100)

        if not candidates:
            logger.warning("[BOT] No candidates found in scan")
            return []

        # Phase 2: Enrich with technical/fundamental data
        logger.info(f"[BOT] Enriching {len(candidates)} candidates with data...")
        enriched = []
        for candidate in candidates[:50]:  # Limit to top 50 for enrichment
            try:
                enriched.append(self.scanner.enrich_candidate(candidate))
            except Exception as e:
                logger.debug(f"[BOT] Failed to enrich {candidate.symbol}: {e}")

        # Phase 3: LLM analysis
        if self.llm.enabled:
            analyzed = self.llm.analyze_batch(enriched[:30])  # Top 30 for LLM
        else:
            analyzed = enriched

        # Filter to tradeable candidates
        tradeable = [c for c in analyzed
                    if c.conviction_score >= MIN_CONVICTION_SCORE
                    and c.direction == TradeDirection.LONG]

        # Sort by conviction
        tradeable.sort(key=lambda c: c.conviction_score, reverse=True)

        # Cache results
        self.state.candidates = [c.to_dict() for c in tradeable]
        self.state.last_scan_timestamp = time.time()
        save_state(self.state)

        logger.info(f"[BOT] Scan complete: {len(tradeable)} tradeable candidates")
        for c in tradeable[:5]:
            logger.info(f"  {c.symbol}: Score={c.conviction_score}, "
                       f"Entry=${c.suggested_entry:.2f}, Target=${c.suggested_target:.2f}")

        return tradeable

    def process_candidates(self, candidates: List[SwingCandidate]) -> int:
        """Process candidates and enter positions."""
        entries = 0
        equity = self.alpaca.get_equity()

        for candidate in candidates:
            should_enter, reason = self.portfolio.should_enter_position(candidate)

            if not should_enter:
                logger.debug(f"[BOT] Skipping {candidate.symbol}: {reason}")
                continue

            # Calculate position size
            shares = self.portfolio.calculate_position_size(candidate, equity)

            if shares < 1:
                logger.debug(f"[BOT] Skipping {candidate.symbol}: Position size too small")
                continue

            # Enter position
            position = self.portfolio.enter_position(candidate, shares)
            if position:
                entries += 1

                # Update state
                self.state.total_trades += 1
                save_state(self.state)

                # Stop if we've reached max new entries per scan
                if entries >= 3:
                    logger.info(f"[BOT] Reached max entries per scan ({entries})")
                    break

        return entries

    def monitor_positions(self):
        """Monitor all active positions and update status."""
        if not self.state.positions:
            return

        symbols = list(self.state.positions.keys())

        # Get current prices
        try:
            snapshots = self.polygon.get_snapshots_batch(symbols)
        except Exception as e:
            logger.error(f"[BOT] Failed to get position snapshots: {e}")
            return

        for symbol in symbols:
            snapshot = snapshots.get(symbol)
            if not snapshot:
                continue

            current_price = snapshot.get("day", {}).get("c", 0) or \
                           snapshot.get("prevDay", {}).get("c", 0)

            if current_price > 0:
                self.portfolio.update_position_status(symbol, current_price)

    def daily_review(self):
        """Run daily position review (re-score existing positions)."""
        today = datetime.now(ET).date().isoformat()

        if self.state.last_position_review_date == today:
            return

        logger.info("[BOT] Running daily position review...")

        # Reset daily starting equity for new day
        current_equity = self.alpaca.get_equity()
        self.state.daily_starting_equity = current_equity
        logger.info(f"[BOT] Daily starting equity reset to ${current_equity:,.2f}")

        # Reset daily circuit breaker if it was triggered
        if self.state.trading_halted and "Daily loss" in (self.state.halt_reason or ""):
            self.state.trading_halted = False
            self.state.halt_reason = None
            logger.info("[BOT] Daily circuit breaker reset for new trading day")

        # Update equity high water mark
        if current_equity > self.state.equity_high_water_mark:
            self.state.equity_high_water_mark = current_equity

        # Log positions
        for symbol, pos_dict in self.state.positions.items():
            position = Position.from_dict(pos_dict)
            if position.status == PositionStatus.ACTIVE:
                logger.info(f"  {symbol}: Days held={position.days_held()}, "
                           f"P&L={position.unrealized_pnl_pct*100:.1f}%")

        self.state.last_position_review_date = today
        save_state(self.state)

    def run(self):
        """Main bot loop."""
        logger.info("[BOT] Starting main loop...")

        # Setup signal handlers
        def shutdown_handler(sig, frame):
            logger.info("[BOT] Shutdown signal received")
            self._running = False

        signal.signal(signal.SIGINT, shutdown_handler)
        signal.signal(signal.SIGTERM, shutdown_handler)

        last_scan_time = 0
        last_monitor_time = 0
        websocket_started = False

        while self._running:
            try:
                now = time.time()
                current_time = datetime.now(ET).time()

                # Check if market hours (simplified check)
                market_open = dt_time(9, 30) <= current_time <= dt_time(16, 0)
                pre_market = dt_time(6, 0) <= current_time < dt_time(9, 30)
                after_hours = current_time > dt_time(16, 0)

                # Start WebSocket monitoring during market hours
                if market_open and not websocket_started and self.state.positions:
                    logger.info("[BOT] Starting real-time WebSocket monitoring...")
                    self.monitor.start()
                    websocket_started = True

                # Stop WebSocket after market close
                if after_hours and websocket_started:
                    logger.info("[BOT] Market closed - stopping WebSocket monitoring")
                    self.monitor.stop()
                    websocket_started = False

                # Run scan at scheduled times or if enough time has passed
                should_scan = False
                for scan_time in SCAN_TIMES:
                    # Check if within 5 minutes of scheduled scan time
                    scan_dt = datetime.combine(datetime.now(ET).date(), scan_time)
                    now_dt = datetime.now(ET)
                    if abs((now_dt - scan_dt.replace(tzinfo=ET)).total_seconds()) < 300:
                        if now - last_scan_time > 3600:  # Don't re-scan within an hour
                            should_scan = True
                            break

                if should_scan or (now - last_scan_time > SCANNER_REFRESH_INTERVAL):
                    candidates = self.run_scan()
                    if candidates and (market_open or pre_market):
                        entries = self.process_candidates(candidates)
                        if entries > 0:
                            logger.info(f"[BOT] Entered {entries} new positions")
                            self.alerts.send_alert(
                                "NEW_ENTRIES",
                                "PORTFOLIO",
                                f"Entered {entries} new position(s)",
                                priority="normal"
                            )
                    last_scan_time = now

                # Fallback position monitoring (in case WebSocket is not running)
                if market_open and (now - last_monitor_time > POSITION_MONITOR_INTERVAL):
                    if not websocket_started:
                        self.monitor_positions()
                    last_monitor_time = now

                # Daily review in pre-market
                if pre_market:
                    self.daily_review()

                # Status log (every 5 minutes)
                if int(now) % 300 < 30:  # Within first 30 seconds of each 5-min block
                    active_positions = len([p for p in self.state.positions.values()
                                           if p.get('status') == PositionStatus.ACTIVE.value])
                    logger.info(f"[BOT] Status: {active_positions} active positions, "
                               f"WebSocket={'ON' if websocket_started else 'OFF'}, "
                               f"last_scan={int((now - last_scan_time)/60)}m ago")

                # Sleep
                time.sleep(30)

            except Exception as e:
                logger.error(f"[BOT] Error in main loop: {e}")
                traceback.print_exc()
                self.alerts.send_alert(
                    "BOT_ERROR",
                    "SYSTEM",
                    f"Main loop error: {str(e)[:100]}",
                    priority="high"
                )
                time.sleep(60)

        # Cleanup
        logger.info("[BOT] Shutting down...")
        self.monitor.stop()
        save_state(self.state)
        logger.info("[BOT] Shutdown complete")


# ============================================================
# ENTRY POINT
# ============================================================

def main():
    """Entry point."""
    # Validate configuration
    if not ALPACA_API_KEY or not ALPACA_SECRET_KEY:
        raise RuntimeError("Missing Alpaca API credentials. Set ALPACA_API_KEY and ALPACA_SECRET_KEY")

    if not POLYGON_API_KEY:
        raise RuntimeError("Missing Polygon API key. Set POLYGON_API_KEY")

    if not ANTHROPIC_API_KEY:
        logger.warning("Missing ANTHROPIC_API_KEY - LLM analysis will be disabled")

    if DRY_RUN:
        logger.warning("=" * 60)
        logger.warning("DRY RUN MODE - No real orders will be placed")
        logger.warning("=" * 60)

    # Create and run bot
    bot = SwingTraderBot()
    bot.run()


if __name__ == "__main__":
    main()
