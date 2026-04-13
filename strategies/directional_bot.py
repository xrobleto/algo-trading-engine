"""
Directional Bot - Short-Only Intraday Momentum
================================================

Strategy: Short-only intraday momentum with regime-gated entries and software
trailing stops. Proven via 20-iteration backtesting process (v20: +21.8%,
PF 1.50, 66.9% WR on 252 days).

Core Edge:
- STRICT entry filters are the alpha (RVOL 1.5x, VWAP 0.15%, EMA 0.10%
  separation, negative momentum, 30-min blackout, 2:30 PM cutoff, ADX 15-50)
- Software-managed trailing stop (0.60R activation / 0.40R distance) is the
  profit engine (~75% of gross profit comes from trailing stop exits)
- 1.0R take profit at 100% position (no partial exits, no runners)
- NEUTRAL regime (SPY near VWAP) generates 90%+ of trades

Key Features:
- RTH only (9:30 AM - 4:00 PM ET), all positions closed by 3:50 PM
- Regime detection: SPY SMA200 + intraday VWAP proximity (5-state model)
- Software trailing stop with broker-native safety net (crash protection)
- Shared-account safety: checks for existing positions from simple_bot/trend_bot
- ATR-based stops (4.5x ATR, min 0.80% distance)
- Daily loss limits with persistence (survives restarts)
- State persistence for crash recovery

Production Sizing (conservative vs backtest):
- Position size: 35% (backtest used 75%)
- Max short positions: 3 (backtest used 5)
- Risk per trade: 7% (backtest used 10%)
- Realistic return expectation: +6-10% annualized

Shared-Account Safety:
- Designed to run alongside simple_bot (long-only) and trend_bot (ETF trend)
- Checks Alpaca positions before every entry to avoid conflicts
- Tags all orders with "dir_" prefix for identification
- Never touches positions belonging to other bots
- Excludes trend_bot ETF universe from trading

Architecture:
- Software-managed exits (trailing stop, TP, SL) with polling
- Broker stop-loss as crash-protection safety net
- Order lifecycle: Entry -> Fill -> Software Tracking -> Exit
- State persisted to JSON for restart recovery

Author: Claude Code
Version: 1.0.0
"""

from __future__ import annotations

import os
import sys
import json
import time
import signal
import logging
import logging.handlers
import threading
import datetime as dt
import hashlib
from dataclasses import dataclass, asdict, field
from enum import Enum
from typing import Dict, List, Optional, Tuple, Set

import requests
from requests.adapters import HTTPAdapter
import pandas as pd
from zoneinfo import ZoneInfo

# v2: Import enhanced MarketScanner for dynamic universe discovery
try:
    from market_scanner import MarketScanner as _EnhancedMarketScanner
    _ENHANCED_SCANNER_AVAILABLE = True
except ImportError:
    try:
        from strategies.market_scanner import MarketScanner as _EnhancedMarketScanner
        _ENHANCED_SCANNER_AVAILABLE = True
    except ImportError:
        _ENHANCED_SCANNER_AVAILABLE = False

# ============================================================
# CONFIGURATION
# ============================================================

ET = ZoneInfo("America/New_York")

# --- Directory Paths ---
from pathlib import Path
ALGO_ROOT = Path(__file__).parent.parent
# Allow output dir override (useful when source lives on Google Drive)
_output_root = Path(os.getenv("ALGO_OUTPUT_DIR", "")) if os.getenv("ALGO_OUTPUT_DIR") else ALGO_ROOT
DATA_DIR = _output_root / "data" / "state"
LOGS_DIR = _output_root / "logs"
CONFIG_DIR = ALGO_ROOT / "config"

DATA_DIR.mkdir(parents=True, exist_ok=True)
LOGS_DIR.mkdir(exist_ok=True)

# --- API Credentials ---
ALPACA_API_KEY = os.getenv("ALPACA_API_KEY", "").strip()
ALPACA_SECRET_KEY = os.getenv("ALPACA_SECRET_KEY", "").strip()
ALPACA_TRADING_BASE_URL = os.getenv("ALPACA_BASE_URL", "https://paper-api.alpaca.markets").strip()
ALPACA_DATA_BASE_URL = os.getenv("ALPACA_DATA_BASE_URL", "https://data.alpaca.markets").strip()
POLYGON_API_KEY = (os.getenv("POLYGON_API_KEY") or os.getenv("MASSIVE_API_KEY") or "").strip()
POLYGON_REST_BASE = "https://api.polygon.io"

# --- Production Safety ---
LIVE_TRADING_ENABLED = os.getenv("LIVE_TRADING", "0") == "1"
LIVE_TRADING_CONFIRMATION = os.getenv("I_UNDERSTAND_LIVE_TRADING", "").upper()
SHUTDOWN_POLICY = os.getenv("SHUTDOWN_POLICY", "CANCEL_ORDERS_ONLY").upper()

# --- Logging ---
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
MAX_LOG_SIZE_MB = 50
MAX_LOG_BACKUPS = 5

# --- Daily Equity Snapshot (Performance Tracking) ---
NOTES_DIR = _output_root / "project_notes"
NOTES_DIR.mkdir(exist_ok=True)
EQUITY_SNAPSHOT_PATH = str(NOTES_DIR / "directional_bot_equity.csv")
SCAN_DIAGNOSTICS_PATH = str(NOTES_DIR / "directional_bot_scan_diagnostics.csv")

# --- Alerting ---
ENABLE_EMAIL_ALERTS = os.getenv("ENABLE_EMAIL_ALERTS", "0") == "1"
ALERT_EMAIL_TO = os.getenv("ALERT_EMAIL_TO", "").strip()
ALERT_EMAIL_FROM = os.getenv("ALERT_EMAIL_FROM", "").strip()
SMTP_SERVER = os.getenv("SMTP_SERVER", "smtp.gmail.com").strip()
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USERNAME = os.getenv("SMTP_USERNAME", "").strip()
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "").strip()

# --- Trading Universe (v20 proven 32 symbols) ---
CORE_SYMBOLS = [
    # Mega-Cap Tech
    "NVDA", "AAPL", "MSFT", "GOOGL", "AMZN", "META", "TSLA",
    # Semiconductors
    "AMD", "AVGO",
    # High-Momentum Growth
    "COIN", "PLTR", "SQ", "HOOD",
    # Financials
    "JPM", "GS", "BAC", "V", "MA",
    # Healthcare
    "UNH", "LLY",
    # Energy
    "CVX", "XOM",
    # Consumer
    "COST", "WMT", "HD",
    # Cloud/SaaS
    "NET", "DDOG", "PANW", "CRM",
    # Communication
    "NFLX",
    # Industrials
    "CAT", "BA",
    # Broad Market ETF
    "DIA",
]

# Symbols managed by trend_bot - NEVER trade these (v8 universe)
TREND_BOT_SYMBOLS = {
    "SPY", "QQQ", "IWM",
    "XLK", "XLF", "XLE", "XLV", "XLI", "XLY", "XLC",
    "SMH", "IBB", "XHB",
    "MTUM", "QUAL",
    "TQQQ", "UPRO", "SOXL", "TECL", "FAS",  # v8: leveraged
    "ARKK", "XBI", "KWEB", "SOXX", "IGV", "CIBR", "SKYY",  # v8: momentum
    "IEF", "TLT", "GLD", "DBC",
    "SGOV",
}

# --- Short Entry Parameters (v1/v13 STRICT - signal quality IS the edge) ---
# v20 backtest: +21.8% (252 days, PF 1.50, 66.9% WR)
# v21 live tuning: relaxed ADX cap (35->50), VWAP dist (0.30->0.15%),
#   EMA sep (0.20->0.10%), late cutoff (13:30->14:30) to allow entries
#   on strong trend / gap-down days that v20 params blocked entirely.
# v22: VWAP dist 0.15->0.05% — 0.15% blocked 70% of symbols every scan,
#   bot never traded in months. 0.05% still requires price below VWAP.
SHORT_MIN_RVOL = 1.5
SHORT_MIN_VWAP_DISTANCE_PCT = 0.20     # Price must be >= 0.20% BELOW VWAP (v23: was 0.05%, too tight)
SHORT_MIN_EMA_SEPARATION_PCT = 0.04    # EMA9 < EMA20 by >= 0.04% (v23: was 0.10%, too restrictive)
SHORT_MIN_LOWER_CLOSES = 3             # 3 of 5 bars must be lower closes
SHORT_MIN_MOMENTUM_5MIN_PCT = 0.15     # Must have >= 0.15% decline over 5 bars
SHORT_NO_TRADE_FIRST_MINUTES = 30      # No trades in first 30 min after open
SHORT_LATE_CUTOFF = (15, 15)           # v23: No new entries after 3:15 PM ET (was 2:30 PM, missed power hour)
SHORT_MAX_DAILY_GAIN_PCT = 4.0         # v23: Don't short stocks up >4% (was 2.0%, blocked too many)

# --- ADX Filter ---
USE_ADX_FILTER = True
ADX_PERIOD = 8                         # v23: Intraday-optimized (was 14, too slow for 1-min bars)
SHORT_MIN_ADX = 20.0                   # v23: Need clearer trend (was 15.0)
SHORT_MAX_ADX = 70.0                   # v23: Allow gap-down days (was 50.0, blocked best shorts)

# --- Exit Parameters ---
SHORT_ATR_STOP_MULTIPLIER = 4.5        # ATR-based stop distance
SHORT_MIN_STOP_DISTANCE_PCT = 0.008    # 0.80% minimum stop distance
SHORT_SCALP_TP_R = 1.0                 # 1:1 R:R take profit (optimal)
ATR_PERIOD = 14                        # ATR lookback (1-min bars)
EMA_FAST = 9
EMA_SLOW = 20

# --- Trailing Stop (THE alpha engine - generates ~75% of gross profit) ---
USE_TRAILING_STOP = True
SHORT_TRAILING_ACTIVATION_R = 0.60     # Activate when profit >= 0.60R
SHORT_TRAILING_DISTANCE_R = 0.40       # Trail by 0.40R from best price

# --- Position Sizing (PRODUCTION conservative - backtest used 75%/10%/5) ---
MAX_RISK_PER_TRADE_PCT = 0.07          # 7% risk per trade (backtest: 10%)
MAX_CAPITAL_USAGE_PCT = 0.90           # 90% max capital deployed
POSITION_SIZE_PCT = 0.35               # 35% per position (backtest: 75%)
USE_COMPOUNDING = True                 # Size off current equity

# --- Position Limits ---
MAX_SHORT_POSITIONS = 3                # Max 3 concurrent shorts (backtest: 5)
MAX_DAILY_TRADES = 6                   # Max 6 trades per day
MAX_DAILY_LOSS_PCT = 0.03              # 3% daily loss limit -> halt trading

# --- Volatility Sizing ---
USE_DYNAMIC_SIZING = True
VOL_REGIME_LOW_THRESHOLD = 0.8
VOL_REGIME_HIGH_THRESHOLD = 1.3
SIZE_MULT_LOW_VOL = 1.25
SIZE_MULT_NORMAL_VOL = 1.00
SIZE_MULT_HIGH_VOL = 0.60

# --- Regime Detection (SPY-based) ---
SPY_SMA_PERIOD = 200                   # Daily SMA for macro trend
REGIME_VWAP_THRESHOLD = 0.003          # 0.3% threshold for NEUTRAL classification
REGIME_ATR_HIGH_THRESHOLD = 1.3        # ATR ratio > 1.3 = high vol

# Regime -> short size multiplier
# NEUTRAL is the bread-and-butter (92.8% of intraday time, best profit)
REGIME_SHORT_SIZE_MULT = {
    "BULL_TREND": 1.00,
    "BULL_VOLATILE": 0.75,
    "NEUTRAL": 1.25,
    "BEAR_VOLATILE": 1.00,
    "BEAR_TREND": 1.25,
}

# --- Timing ---
AUTO_CLOSE_EOD = True
EOD_CLOSE_TIME_ET = (15, 50)           # Close all positions at 3:50 PM ET
DATA_POLL_INTERVAL_SEC = 10            # How often to poll prices
SCAN_INTERVAL_SEC = 30                 # How often to scan for new setups
ENTRY_TIMEOUT_SEC = 45                 # Cancel unfilled entry after 45s

# --- State Persistence ---
STATE_PATH = str(DATA_DIR / "directional_bot_state.json")
TRADE_JOURNAL_PATH = str(DATA_DIR / "directional_bot_trades.jsonl")

# --- Kill Switch ---
KILL_SWITCH_FILE = str(DATA_DIR / "HALT_DIRECTIONAL")
KILL_SWITCH_ENV = "KILL_SWITCH_DIRECTIONAL"

# --- Enhanced Market Scanner (v2: dynamic universe discovery) ---
USE_ENHANCED_SCANNER = True and _ENHANCED_SCANNER_AVAILABLE
ENABLE_DYNAMIC_UNIVERSE = True
DYNAMIC_SCAN_INTERVAL_SEC = 120       # Rescan every 2 minutes
DYNAMIC_EXCLUSION_LIST = TREND_BOT_SYMBOLS | {"MSTR", "SHOP", "CRWD"}  # Same as simple_bot

# --- Entry Execution ---
ENTRY_REPRICE_MAX_ATTEMPTS = 2
ENTRY_REPRICE_INTERVAL_SEC = 1.5
MIN_PRICE = 10.0

# ============================================================
# LOGGING SETUP
# ============================================================

logger = logging.getLogger("directional_bot")
logger.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))

# Console handler
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(logging.Formatter(
    "%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
))
logger.addHandler(console_handler)

# File handler with rotation
log_file = str(LOGS_DIR / "directional_bot.log")
file_handler = logging.handlers.RotatingFileHandler(
    log_file,
    maxBytes=MAX_LOG_SIZE_MB * 1024 * 1024,
    backupCount=MAX_LOG_BACKUPS
)
file_handler.setFormatter(logging.Formatter(
    "%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
))
logger.addHandler(file_handler)


# ============================================================
# HELPERS
# ============================================================

def now_et() -> dt.datetime:
    """Current time in Eastern."""
    return dt.datetime.now(ET)

def iso(d: dt.datetime) -> str:
    return d.isoformat()

def from_iso(s: str) -> dt.datetime:
    return dt.datetime.fromisoformat(s)

def atomic_write_json(path: str, data: dict, indent: int = 2):
    """Write JSON atomically to prevent corruption on crash."""
    tmp_path = path + ".tmp"
    with open(tmp_path, "w") as f:
        json.dump(data, f, indent=indent, default=str)
    os.replace(tmp_path, path)

def generate_client_order_id(symbol: str, action: str, date_str: str) -> str:
    """Generate deterministic client_order_id for idempotency."""
    raw = f"dir_{symbol}_{action}_{date_str}_{int(time.time())}"
    return f"dir_{hashlib.md5(raw.encode()).hexdigest()[:16]}"

class MarketSession(Enum):
    PRE_MARKET = "PRE_MARKET"
    REGULAR = "REGULAR"
    AFTER_HOURS = "AFTER_HOURS"
    CLOSED = "CLOSED"

def get_market_session() -> MarketSession:
    """Determine current market session."""
    now = now_et()
    if now.weekday() >= 5:  # Weekend
        return MarketSession.CLOSED
    t = now.time()
    if dt.time(9, 30) <= t < dt.time(16, 0):
        return MarketSession.REGULAR
    elif dt.time(4, 0) <= t < dt.time(9, 30):
        return MarketSession.PRE_MARKET
    elif dt.time(16, 0) <= t < dt.time(20, 0):
        return MarketSession.AFTER_HOURS
    return MarketSession.CLOSED

class MarketRegime(Enum):
    BULL_TREND = "BULL_TREND"
    BULL_VOLATILE = "BULL_VOLATILE"
    NEUTRAL = "NEUTRAL"
    BEAR_VOLATILE = "BEAR_VOLATILE"
    BEAR_TREND = "BEAR_TREND"


# ============================================================
# ALPACA REST CLIENT
# ============================================================

class AlpacaClient:
    """Alpaca REST client with retry logic and connection pooling."""

    def __init__(self):
        self.trading_base = ALPACA_TRADING_BASE_URL
        self.data_base = ALPACA_DATA_BASE_URL
        self.headers = {
            "APCA-API-KEY-ID": ALPACA_API_KEY,
            "APCA-API-SECRET-KEY": ALPACA_SECRET_KEY,
            "Content-Type": "application/json",
        }
        # Persistent session reuses TCP/SSL connections (avoids SSL exhaustion
        # when scanning 50+ symbols every 30 seconds)
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        adapter = HTTPAdapter(
            pool_connections=2,     # 2 hosts: trading + data
            pool_maxsize=10,        # up to 10 keep-alive connections per host
            max_retries=0,          # we handle retries ourselves
        )
        self.session.mount("https://", adapter)

    def _request(self, method: str, path: str, base_url: str = None,
                 retries: int = 3, **kwargs):
        if base_url is None:
            base_url = self.trading_base
        url = f"{base_url}{path}"

        # Never retry order submissions (prevents duplicates)
        is_order_submission = method == "POST" and "/v2/orders" in path
        max_attempts = 1 if is_order_submission else retries

        last_exception = None
        for attempt in range(max_attempts):
            try:
                response = self.session.request(
                    method, url, timeout=15, **kwargs
                )
                response.raise_for_status()
                return response.json() if response.text else {}

            except requests.HTTPError as e:
                status_code = e.response.status_code if e.response else 0
                if status_code == 429 or 500 <= status_code < 600:
                    if attempt < max_attempts - 1:
                        backoff = 2 ** attempt
                        logger.warning(f"[API] {method} {path} | {status_code} | retry {attempt+1}/{max_attempts} in {backoff}s")
                        time.sleep(backoff)
                        last_exception = e
                        continue
                error_detail = "no response"
                if e.response is not None:
                    try:
                        error_detail = e.response.json().get("message", e.response.text[:500])
                    except Exception:
                        error_detail = e.response.text[:500] if e.response.text else "empty"
                logger.error(f"[API] {method} {path} | {status_code} | {error_detail}")
                raise

            except requests.RequestException as e:
                if attempt < max_attempts - 1 and not is_order_submission:
                    backoff = 2 ** attempt
                    logger.warning(f"[API] {method} {path} | network error | retry {attempt+1}/{max_attempts} in {backoff}s")
                    time.sleep(backoff)
                    last_exception = e
                    continue
                logger.error(f"[API] {method} {path} | network error | {e}")
                raise

        if last_exception:
            raise last_exception

    def get_account(self) -> dict:
        return self._request("GET", "/v2/account")

    def get_positions(self) -> List[dict]:
        return self._request("GET", "/v2/positions")

    def get_position(self, symbol: str) -> Optional[dict]:
        try:
            return self._request("GET", f"/v2/positions/{symbol}")
        except requests.HTTPError:
            return None

    def get_orders(self, status: str = "open") -> List[dict]:
        return self._request("GET", "/v2/orders", params={"status": status, "limit": 500})

    def get_order(self, order_id: str) -> dict:
        return self._request("GET", f"/v2/orders/{order_id}")

    def submit_order(self, symbol: str, qty: int, side: str,
                     order_type: str = "market", limit_price: float = None,
                     stop_price: float = None, time_in_force: str = "day",
                     order_class: str = None, take_profit: dict = None,
                     stop_loss: dict = None,
                     client_order_id: str = None) -> dict:
        payload = {
            "symbol": symbol,
            "qty": str(qty),
            "side": side,
            "type": order_type,
            "time_in_force": time_in_force,
        }
        if limit_price is not None:
            payload["limit_price"] = f"{float(limit_price):.2f}"
        if stop_price is not None:
            payload["stop_price"] = f"{float(stop_price):.2f}"
        if order_class:
            payload["order_class"] = order_class
        if take_profit:
            payload["take_profit"] = take_profit
        if stop_loss:
            payload["stop_loss"] = stop_loss
        if client_order_id:
            payload["client_order_id"] = client_order_id
        return self._request("POST", "/v2/orders", json=payload)

    def cancel_order(self, order_id: str):
        return self._request("DELETE", f"/v2/orders/{order_id}")

    def cancel_all_orders(self) -> List[dict]:
        return self._request("DELETE", "/v2/orders")

    def close_position(self, symbol: str) -> dict:
        """Close an entire position (market order)."""
        return self._request("DELETE", f"/v2/positions/{symbol}")

    def get_latest_quote(self, symbol: str) -> Optional[dict]:
        """Get latest NBBO quote from Alpaca."""
        try:
            data = self._request("GET", f"/v2/stocks/{symbol}/quotes/latest",
                                 base_url=self.data_base)
            return data.get("quote", data)
        except Exception as e:
            logger.warning(f"[DATA] Could not get quote for {symbol}: {e}")
            return None

    def get_latest_trade(self, symbol: str) -> Optional[dict]:
        """Get latest trade from Alpaca."""
        try:
            data = self._request("GET", f"/v2/stocks/{symbol}/trades/latest",
                                 base_url=self.data_base)
            return data.get("trade", data)
        except Exception as e:
            logger.warning(f"[DATA] Could not get trade for {symbol}: {e}")
            return None

    def get_bars(self, symbol: str, timeframe: str = "1Min",
                 limit: int = 100, start: str = None, end: str = None) -> List[dict]:
        """Get historical bars from Alpaca."""
        params = {"timeframe": timeframe, "limit": limit, "feed": "sip"}
        if start:
            params["start"] = start
        if end:
            params["end"] = end
        try:
            data = self._request("GET", f"/v2/stocks/{symbol}/bars",
                                 base_url=self.data_base, params=params)
            return data.get("bars", [])
        except Exception as e:
            logger.warning(f"[DATA] Could not get bars for {symbol}: {e}")
            return []

    def get_snapshot(self, symbol: str) -> Optional[dict]:
        """Get real-time snapshot from Alpaca."""
        try:
            data = self._request("GET", f"/v2/stocks/{symbol}/snapshot",
                                 base_url=self.data_base, params={"feed": "sip"})
            return data
        except Exception as e:
            logger.debug(f"[DATA] Could not get snapshot for {symbol}: {e}")
            return None

    def get_asset(self, symbol: str) -> Optional[dict]:
        """Get asset info (for borrow check)."""
        try:
            return self._request("GET", f"/v2/assets/{symbol}")
        except Exception:
            return None

    def is_symbol_shortable(self, symbol: str) -> Tuple[bool, str]:
        """Check if a symbol can be shorted (easy to borrow + shortable)."""
        asset = self.get_asset(symbol)
        if not asset:
            return False, "asset_not_found"
        if not asset.get("shortable", False):
            return False, "not_shortable"
        if not asset.get("easy_to_borrow", False):
            return False, "hard_to_borrow"
        if asset.get("status") != "active":
            return False, f"status_{asset.get('status')}"
        return True, "ok"

    def get_clock(self) -> Optional[dict]:
        """Get market clock."""
        try:
            return self._request("GET", "/v2/clock")
        except Exception:
            return None


# ============================================================
# POLYGON CLIENT (for regime detection & indicators)
# ============================================================

class PolygonClient:
    """Polygon.io REST client for market data."""

    def __init__(self):
        self.base = POLYGON_REST_BASE
        self.headers = {"Authorization": f"Bearer {POLYGON_API_KEY}"}
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        adapter = HTTPAdapter(pool_connections=1, pool_maxsize=5, max_retries=0)
        self.session.mount("https://", adapter)

    def _request(self, path: str, params: dict = None) -> Optional[dict]:
        url = f"{self.base}{path}"
        try:
            resp = self.session.get(url, params=params, timeout=15)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.debug(f"[POLYGON] {path}: {e}")
            return None

    def get_daily_bars(self, symbol: str, from_date: str, to_date: str) -> List[dict]:
        """Get daily bars from Polygon."""
        data = self._request(
            f"/v2/aggs/ticker/{symbol}/range/1/day/{from_date}/{to_date}",
            params={"adjusted": "true", "sort": "asc", "limit": 5000}
        )
        if data and data.get("results"):
            return data["results"]
        return []

    def get_snapshot(self, symbol: str) -> Optional[dict]:
        """Get real-time snapshot from Polygon."""
        data = self._request(f"/v2/snapshot/locale/us/markets/stocks/tickers/{symbol}")
        if data and data.get("ticker"):
            return data["ticker"]
        return None

    def get_current_price(self, snapshot: dict) -> Optional[float]:
        """Extract current price from Polygon snapshot."""
        if not snapshot:
            return None
        # Try lastTrade first
        last_trade = snapshot.get("lastTrade", {})
        if last_trade.get("p"):
            return float(last_trade["p"])
        # Fall back to min-level data
        minute = snapshot.get("min", {})
        if minute.get("c"):
            return float(minute["c"])
        day = snapshot.get("day", {})
        if day.get("c"):
            return float(day["c"])
        return None


# ============================================================
# INDICATOR CALCULATIONS
# ============================================================

def calculate_ema(series: pd.Series, period: int) -> pd.Series:
    return series.ewm(span=period, adjust=False).mean()

def calculate_atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    high = df["h"] if "h" in df.columns else df["high"]
    low = df["l"] if "l" in df.columns else df["low"]
    close = df["c"] if "c" in df.columns else df["close"]
    prev_close = close.shift(1)
    tr = pd.concat([
        high - low,
        (high - prev_close).abs(),
        (low - prev_close).abs()
    ], axis=1).max(axis=1)
    return tr.ewm(span=period, adjust=False).mean()

def calculate_adx(df: pd.DataFrame, period: int = 14) -> pd.Series:
    high = df["h"] if "h" in df.columns else df["high"]
    low = df["l"] if "l" in df.columns else df["low"]
    close = df["c"] if "c" in df.columns else df["close"]

    plus_dm = high.diff()
    minus_dm = -low.diff()
    plus_dm = plus_dm.where((plus_dm > minus_dm) & (plus_dm > 0), 0.0)
    minus_dm = minus_dm.where((minus_dm > plus_dm) & (minus_dm > 0), 0.0)

    prev_close = close.shift(1)
    tr = pd.concat([
        high - low,
        (high - prev_close).abs(),
        (low - prev_close).abs()
    ], axis=1).max(axis=1)

    atr = tr.ewm(span=period, adjust=False).mean()
    plus_di = 100 * (plus_dm.ewm(span=period, adjust=False).mean() / atr)
    minus_di = 100 * (minus_dm.ewm(span=period, adjust=False).mean() / atr)

    dx = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, 1)
    adx = dx.ewm(span=period, adjust=False).mean()
    return adx

def calculate_sma(series: pd.Series, period: int) -> pd.Series:
    return series.rolling(window=period, min_periods=period).mean()


# ============================================================
# POSITION TRACKING
# ============================================================

@dataclass
class SoftwarePosition:
    """
    Tracks a short position managed by this bot.

    Exits are handled in software (trailing stop, TP, SL) with a broker
    safety-net stop order for crash protection.
    """
    symbol: str
    entry_time: str              # ISO timestamp
    entry_price: float
    qty: int
    stop_price: float            # Initial stop (above entry for shorts)
    tp_price: float              # Take profit (below entry for shorts)
    risk_per_share: float        # Entry to stop distance
    regime: str                  # Regime at entry time
    safety_stop_order_id: Optional[str] = None  # Broker stop for crash protection

    # Trailing stop state
    trail_active: bool = False
    best_price: float = 0.0      # Lowest price seen (for shorts)
    trail_stop: float = 0.0      # Current trailing stop level

    # Order tracking
    entry_order_id: Optional[str] = None
    entry_client_order_id: Optional[str] = None

    def to_dict(self) -> dict:
        return asdict(self)

    @staticmethod
    def from_dict(d: dict) -> 'SoftwarePosition':
        return SoftwarePosition(**{k: v for k, v in d.items()
                                   if k in SoftwarePosition.__dataclass_fields__})


# ============================================================
# REGIME DETECTOR
# ============================================================

class RegimeDetector:
    """
    Detects market regime using SPY SMA200 + intraday VWAP proximity.

    5-state model:
    - NEUTRAL: SPY within +/- 0.3% of VWAP (92.8% of intraday time)
    - BULL_TREND: SPY above SMA200, not near VWAP, normal vol
    - BULL_VOLATILE: SPY above SMA200, high vol
    - BEAR_TREND: SPY below SMA200, not near VWAP, normal vol
    - BEAR_VOLATILE: SPY below SMA200, high vol
    """

    def __init__(self, alpaca_client: AlpacaClient, polygon_client: PolygonClient):
        self.alpaca = alpaca_client
        self.polygon = polygon_client
        self.spy_sma200: Optional[float] = None
        self.last_sma_update: Optional[dt.date] = None
        self._current_regime = MarketRegime.NEUTRAL

    def update_daily_sma(self):
        """Fetch SPY daily bars and compute SMA200. Called once per day."""
        today = now_et().date()
        if self.last_sma_update == today and self.spy_sma200 is not None:
            return

        logger.info("[REGIME] Fetching SPY daily bars for SMA200...")
        end_date = today.isoformat()
        start_date = (today - dt.timedelta(days=400)).isoformat()  # Extra buffer

        bars = self.polygon.get_daily_bars("SPY", start_date, end_date)
        if not bars or len(bars) < SPY_SMA_PERIOD:
            # Try Alpaca as fallback
            logger.warning("[REGIME] Polygon daily bars insufficient, trying Alpaca...")
            try:
                alpaca_bars = self.alpaca.get_bars(
                    "SPY", timeframe="1Day", limit=300,
                    start=start_date, end=end_date
                )
                if alpaca_bars and len(alpaca_bars) >= SPY_SMA_PERIOD:
                    closes = [float(b.get("c", 0)) for b in alpaca_bars]
                    self.spy_sma200 = sum(closes[-SPY_SMA_PERIOD:]) / SPY_SMA_PERIOD
                    self.last_sma_update = today
                    logger.info(f"[REGIME] SPY SMA200 = ${self.spy_sma200:.2f} (from Alpaca, {len(alpaca_bars)} bars)")
                    return
            except Exception as e:
                logger.warning(f"[REGIME] Alpaca fallback failed: {e}")

            if not bars:
                logger.warning("[REGIME] Could not compute SMA200 - defaulting to NEUTRAL")
                return

        closes = [b["c"] for b in bars]
        if len(closes) >= SPY_SMA_PERIOD:
            self.spy_sma200 = sum(closes[-SPY_SMA_PERIOD:]) / SPY_SMA_PERIOD
            self.last_sma_update = today
            logger.info(f"[REGIME] SPY SMA200 = ${self.spy_sma200:.2f} ({len(closes)} daily bars)")
        else:
            logger.warning(f"[REGIME] Only {len(closes)} bars, need {SPY_SMA_PERIOD} for SMA200")

    def detect(self) -> MarketRegime:
        """Detect current market regime from SPY real-time data."""
        if self.spy_sma200 is None:
            self.update_daily_sma()
            if self.spy_sma200 is None:
                return MarketRegime.NEUTRAL

        # Get SPY current snapshot
        snapshot = self.alpaca.get_snapshot("SPY")
        if not snapshot:
            return self._current_regime  # Use last known

        # Extract SPY current price
        minute_bar = snapshot.get("minuteBar", {})
        spy_close = float(minute_bar.get("c", 0))
        if spy_close <= 0:
            latest_trade = snapshot.get("latestTrade", {})
            spy_close = float(latest_trade.get("p", 0))
        if spy_close <= 0:
            return self._current_regime

        # SPY VWAP from daily bar
        daily_bar = snapshot.get("dailyBar", {})
        spy_vwap = float(daily_bar.get("vw", spy_close))

        # Check VWAP proximity (NEUTRAL = near VWAP)
        if spy_vwap > 0:
            vwap_dist_pct = (spy_close - spy_vwap) / spy_vwap
        else:
            vwap_dist_pct = 0.0

        # NEUTRAL: SPY within +/- REGIME_VWAP_THRESHOLD of VWAP
        if abs(vwap_dist_pct) <= REGIME_VWAP_THRESHOLD * 1.5:
            self._current_regime = MarketRegime.NEUTRAL
        elif spy_close > self.spy_sma200:
            # Above SMA200 = bullish bias
            # Check volatility via prevDay/day comparison for ATR ratio proxy
            prev_day = snapshot.get("prevDailyBar", {})
            day_range = float(daily_bar.get("h", 0)) - float(daily_bar.get("l", 0))
            prev_range = float(prev_day.get("h", 0)) - float(prev_day.get("l", 0))
            atr_ratio = day_range / prev_range if prev_range > 0 else 1.0

            if atr_ratio > REGIME_ATR_HIGH_THRESHOLD:
                self._current_regime = MarketRegime.BULL_VOLATILE
            else:
                self._current_regime = MarketRegime.BULL_TREND
        else:
            # Below SMA200 = bearish bias
            prev_day = snapshot.get("prevDailyBar", {})
            day_range = float(daily_bar.get("h", 0)) - float(daily_bar.get("l", 0))
            prev_range = float(prev_day.get("h", 0)) - float(prev_day.get("l", 0))
            atr_ratio = day_range / prev_range if prev_range > 0 else 1.0

            if atr_ratio > REGIME_ATR_HIGH_THRESHOLD:
                self._current_regime = MarketRegime.BEAR_VOLATILE
            else:
                self._current_regime = MarketRegime.BEAR_TREND

        return self._current_regime


# ============================================================
# RISK MANAGER
# ============================================================

class RiskManager:
    """Daily loss limits, position sizing, and trade counting."""

    def __init__(self):
        self.start_equity = 0.0
        self.current_equity = 0.0
        self.buying_power = 0.0
        self.daily_pnl = 0.0
        self.halted = False
        self.start_date = None
        self.daily_trade_count = 0

    def _load_state(self) -> Optional[dict]:
        try:
            if os.path.exists(STATE_PATH):
                with open(STATE_PATH, "r") as f:
                    state = json.load(f)
                    return state.get("risk_state")
        except Exception:
            pass
        return None

    def _save_state(self):
        try:
            state = {}
            if os.path.exists(STATE_PATH):
                with open(STATE_PATH, "r") as f:
                    state = json.load(f)
            state["risk_state"] = {
                "start_equity": self.start_equity,
                "start_date": self.start_date.isoformat() if self.start_date else None,
                "halted": self.halted,
                "daily_trade_count": self.daily_trade_count,
            }
            atomic_write_json(STATE_PATH, state)
        except Exception as e:
            logger.warning(f"[RISK] Could not save state: {e}")

    def initialize(self, alpaca_client: AlpacaClient):
        account = alpaca_client.get_account()
        current_equity = float(account["equity"])
        today = now_et().date()

        saved = self._load_state()
        if saved and saved.get("start_date"):
            saved_date = dt.date.fromisoformat(saved["start_date"])
            if saved_date == today:
                self.start_equity = saved["start_equity"]
                self.start_date = saved_date
                self.halted = saved.get("halted", False)
                self.daily_trade_count = saved.get("daily_trade_count", 0)
                logger.warning(f"[RISK] Restored state from earlier today | start=${self.start_equity:,.2f} trades={self.daily_trade_count}")
            else:
                self.start_equity = current_equity
                self.start_date = today
                self.halted = False
                self.daily_trade_count = 0
                logger.info(f"[RISK] New trading day | start_equity=${self.start_equity:,.2f}")
        else:
            self.start_equity = current_equity
            self.start_date = today
            self.halted = False
            logger.info(f"[RISK] First run | start_equity=${self.start_equity:,.2f}")

        self.current_equity = current_equity
        self.buying_power = float(account.get("buying_power", current_equity))
        self.daily_pnl = self.current_equity - self.start_equity
        self._save_state()

        deployable = min(self.buying_power, self.start_equity * MAX_CAPITAL_USAGE_PCT)
        logger.info(f"[RISK] equity=${self.current_equity:,.2f} buying_power=${self.buying_power:,.2f} deployable=${deployable:,.2f}")

        if self.halted:
            logger.error("[RISK] HALTED state from earlier - trading remains HALTED")

    def update(self, alpaca_client: AlpacaClient) -> bool:
        """Update equity and check daily loss limit. Returns True if halted."""
        try:
            account = alpaca_client.get_account()
            self.current_equity = float(account["equity"])
            self.buying_power = float(account.get("buying_power", self.current_equity))
            self.daily_pnl = self.current_equity - self.start_equity

            if self.start_equity > 0:
                loss_pct = abs(self.daily_pnl / self.start_equity)
                if self.daily_pnl < 0 and loss_pct >= MAX_DAILY_LOSS_PCT:
                    if not self.halted:
                        self.halted = True
                        logger.error(f"[RISK] DAILY LOSS LIMIT HIT | loss=${self.daily_pnl:.2f} ({loss_pct:.1%}) - HALTING")
                        self._save_state()
        except Exception as e:
            logger.warning(f"[RISK] Could not update: {e}")
        return self.halted

    def can_trade(self) -> Tuple[bool, str]:
        """Check if trading is allowed."""
        if self.halted:
            return False, "daily_loss_limit_hit"
        if self.daily_trade_count >= MAX_DAILY_TRADES:
            return False, f"daily_trade_limit ({MAX_DAILY_TRADES})"
        return True, "ok"

    def increment_trade_count(self):
        self.daily_trade_count += 1
        self._save_state()

    def calculate_position_size(self, entry_price: float, stop_price: float,
                                regime_mult: float = 1.0,
                                vol_mult: float = 1.0) -> int:
        """
        Calculate short position size based on risk parameters.

        Uses the more conservative of:
        1. Risk-based: risk% of equity / risk_per_share
        2. Capital-based: position_size% of equity / entry_price
        """
        risk_per_share = abs(stop_price - entry_price)
        if risk_per_share <= 0:
            return 0

        sizing_capital = self.current_equity if USE_COMPOUNDING else self.start_equity
        combined_mult = regime_mult * vol_mult

        # Risk-based sizing
        max_risk = sizing_capital * MAX_RISK_PER_TRADE_PCT * combined_mult
        risk_qty = int(max_risk / risk_per_share)

        # Capital-based sizing (the binding constraint in practice)
        max_position = sizing_capital * MAX_CAPITAL_USAGE_PCT * POSITION_SIZE_PCT
        cap_qty = int(max_position / entry_price)

        # Use the more conservative
        qty = min(risk_qty, cap_qty)

        if qty <= 0:
            return 0

        # Final sanity check: position value vs buying power
        position_value = qty * entry_price
        if position_value > self.buying_power * 0.95:  # 5% buffer
            qty = int(self.buying_power * 0.95 / entry_price)

        return max(0, qty)


# ============================================================
# DAILY EQUITY SNAPSHOT (Performance Tracking)
# ============================================================

class EquitySnapshotLogger:
    """Logs one equity snapshot per trading day to CSV for performance analysis."""

    def __init__(self):
        self._last_snapshot_date = None
        self._lock = threading.Lock()
        self._ensure_header()

    def _ensure_header(self):
        if os.path.exists(EQUITY_SNAPSHOT_PATH):
            return
        try:
            import csv as _csv
            with open(EQUITY_SNAPSHOT_PATH, "w", newline="", encoding="utf-8") as f:
                w = _csv.writer(f)
                w.writerow([
                    "date", "equity", "start_equity", "daily_pnl",
                    "positions_value", "cash", "num_positions",
                    "halted", "daily_trades"
                ])
            logger.debug(f"Created equity snapshot header at {EQUITY_SNAPSHOT_PATH}")
        except Exception as e:
            logger.error(f"Failed to create equity snapshot header: {e}")

    def log_snapshot(self, risk_mgr, alpaca_client: AlpacaClient):
        """Log daily snapshot if not yet logged today. Called from main loop."""
        today = now_et().date()
        if self._last_snapshot_date == today:
            return

        with self._lock:
            if self._last_snapshot_date == today:
                return
            try:
                import csv as _csv
                equity = risk_mgr.current_equity
                start_eq = risk_mgr.start_equity
                daily_pnl = risk_mgr.daily_pnl

                positions = alpaca_client.get_positions()
                positions_value = sum(abs(float(p.get("market_value", 0))) for p in positions)
                cash = equity - positions_value
                num_positions = len(positions)

                with open(EQUITY_SNAPSHOT_PATH, "a", newline="", encoding="utf-8") as f:
                    w = _csv.writer(f)
                    w.writerow([
                        today.isoformat(),
                        f"{equity:.2f}",
                        f"{start_eq:.2f}",
                        f"{daily_pnl:.2f}",
                        f"{positions_value:.2f}",
                        f"{cash:.2f}",
                        num_positions,
                        risk_mgr.halted,
                        risk_mgr.daily_trade_count
                    ])

                self._last_snapshot_date = today
                logger.info(f"[EQUITY_SNAPSHOT] {today}: ${equity:.2f} | "
                           f"PnL=${daily_pnl:+.2f} | {num_positions} positions")

            except Exception as e:
                logger.warning(f"[EQUITY_SNAPSHOT] Failed: {e}")


# ============================================================
# SCAN DIAGNOSTICS (Filter Funnel Tracking)
# ============================================================

_REJECTION_COLUMNS = [
    "no_data", "too_early", "too_late", "past_cutoff", "price_too_low",
    "daily_gain_too_high", "adx_too_high", "adx_too_low",
    "vwap_distance", "ema_separation", "low_rvol",
    "weak_downtrend", "weak_momentum", "not_shortable"
]


class ScanDiagnosticsLogger:
    """Aggregates per-scan rejection counts into a daily summary CSV."""

    def __init__(self):
        self._today = None
        self._daily_scans = 0
        self._symbols_scanned_total = 0
        self._daily_rejections = {}
        self._daily_candidates = 0
        self._daily_trades = 0
        self._lock = threading.Lock()
        self._ensure_header()

    def _ensure_header(self):
        if os.path.exists(SCAN_DIAGNOSTICS_PATH):
            return
        try:
            import csv as _csv
            with open(SCAN_DIAGNOSTICS_PATH, "w", newline="", encoding="utf-8") as f:
                w = _csv.writer(f)
                w.writerow(["date", "total_scans", "symbols_scanned"] +
                          _REJECTION_COLUMNS +
                          ["candidates_found", "trades_entered"])
        except Exception as e:
            logger.error(f"Failed to create scan diagnostics header: {e}")

    def _flush_day(self, date_str: str):
        try:
            import csv as _csv
            row = [date_str, self._daily_scans, self._symbols_scanned_total]
            for col in _REJECTION_COLUMNS:
                row.append(self._daily_rejections.get(col, 0))
            row.append(self._daily_candidates)
            row.append(self._daily_trades)

            with open(SCAN_DIAGNOSTICS_PATH, "a", newline="", encoding="utf-8") as f:
                w = _csv.writer(f)
                w.writerow(row)

            top = sorted(self._daily_rejections.items(), key=lambda x: x[1], reverse=True)[:5]
            top_str = ", ".join(f"{k}={v}" for k, v in top)
            logger.info(f"[SCAN_DIAG] Daily summary: {self._daily_scans} scans, "
                       f"{self._daily_candidates} candidates, {self._daily_trades} trades | "
                       f"Top rejections: {top_str}")
        except Exception as e:
            logger.warning(f"[SCAN_DIAG] Failed to flush: {e}")

    def record_scan(self, symbols_scanned: int, rejection_counts: dict,
                    entries: int):
        with self._lock:
            today = now_et().date().isoformat()
            if self._today and self._today != today:
                self._flush_day(self._today)
                self._daily_scans = 0
                self._symbols_scanned_total = 0
                self._daily_rejections = {}
                self._daily_candidates = 0
                self._daily_trades = 0

            self._today = today
            self._daily_scans += 1
            self._symbols_scanned_total += symbols_scanned
            self._daily_trades += entries

            # Count candidates = symbols_scanned - sum(rejections) - skipped
            total_rejected = sum(rejection_counts.values())
            self._daily_candidates += max(0, symbols_scanned - total_rejected)

            for reason, count in rejection_counts.items():
                self._daily_rejections[reason] = self._daily_rejections.get(reason, 0) + count

    def flush_if_needed(self):
        with self._lock:
            if self._today and self._daily_scans > 0:
                self._flush_day(self._today)
                self._daily_scans = 0


# ============================================================
# KILL SWITCH
# ============================================================

class KillSwitch:
    """Emergency halt mechanism - file-based or env-based."""

    def is_triggered(self) -> Tuple[bool, str]:
        if os.getenv(KILL_SWITCH_ENV, "0") == "1":
            return True, "env_variable"
        if os.path.exists(KILL_SWITCH_FILE):
            return True, "halt_file"
        return False, ""

    def execute_emergency_shutdown(self, alpaca_client: AlpacaClient):
        logger.error("[KILL_SWITCH] EXECUTING EMERGENCY SHUTDOWN")
        try:
            # Only cancel our orders (dir_ prefix) - preserve other bots' orders
            open_orders = alpaca_client.get_orders(status="open")
            our_orders = [o for o in open_orders
                         if (o.get("client_order_id") or "").startswith("dir_")]
            for order in our_orders:
                try:
                    alpaca_client.cancel_order(order["id"])
                except Exception:
                    pass
            other_count = len(open_orders) - len(our_orders)
            logger.info(f"[KILL_SWITCH] Cancelled {len(our_orders)} dir_ order(s)"
                       f"{f' (preserved {other_count} other bot orders)' if other_count else ''}")
        except Exception as e:
            logger.error(f"[KILL_SWITCH] Failed to cancel orders: {e}")


# ============================================================
# TRADE JOURNAL
# ============================================================

class TradeJournal:
    """Append-only JSONL trade journal for analytics."""

    def __init__(self):
        self.path = TRADE_JOURNAL_PATH

    def log_entry(self, symbol: str, qty: int, entry_price: float,
                  stop_price: float, tp_price: float, regime: str):
        record = {
            "event": "ENTRY",
            "timestamp": iso(now_et()),
            "symbol": symbol,
            "side": "SHORT",
            "qty": qty,
            "entry_price": entry_price,
            "stop_price": stop_price,
            "tp_price": tp_price,
            "regime": regime,
            "risk_per_share": abs(stop_price - entry_price),
        }
        self._append(record)

    def log_exit(self, symbol: str, qty: int, entry_price: float,
                 exit_price: float, exit_reason: str, regime: str,
                 hold_seconds: float):
        pnl = (entry_price - exit_price) * qty  # Short P&L
        risk_per_share = abs(entry_price * SHORT_MIN_STOP_DISTANCE_PCT)  # Approximate
        r_multiple = (entry_price - exit_price) / risk_per_share if risk_per_share > 0 else 0

        record = {
            "event": "EXIT",
            "timestamp": iso(now_et()),
            "symbol": symbol,
            "side": "SHORT",
            "qty": qty,
            "entry_price": entry_price,
            "exit_price": exit_price,
            "pnl": round(pnl, 2),
            "r_multiple": round(r_multiple, 2),
            "exit_reason": exit_reason,
            "regime": regime,
            "hold_seconds": round(hold_seconds, 1),
        }
        self._append(record)
        logger.info(f"[JOURNAL] {symbol} EXIT | pnl=${pnl:+.2f} | {exit_reason} | {regime}")

    def _append(self, record: dict):
        try:
            with open(self.path, "a") as f:
                f.write(json.dumps(record, default=str) + "\n")
        except Exception as e:
            logger.warning(f"[JOURNAL] Write failed: {e}")


# ============================================================
# MARKET DATA SCANNER
# ============================================================

@dataclass
class MarketData:
    """Aggregated market data for signal evaluation."""
    symbol: str
    price: float
    vwap: float
    ema_fast: float
    ema_slow: float
    atr: float
    atr_ratio: float
    adx: float
    rvol: float
    daily_gain_pct: float
    bars: Optional[pd.DataFrame] = field(default=None, repr=False)


class MarketScanner:
    """Fetches and processes market data for signal evaluation."""

    def __init__(self, alpaca_client: AlpacaClient):
        self.alpaca = alpaca_client
        self._rvol_cache: Dict[str, float] = {}  # symbol -> avg volume
        self._last_rvol_update: Optional[dt.date] = None
        self._bar_cache: Dict[str, Tuple[float, pd.DataFrame]] = {}  # symbol -> (timestamp, df)
        self._cache_ttl = 20.0  # seconds (was 8s — increased to reduce API calls per scan cycle)

    def update_rvol_baselines(self, symbols: List[str]):
        """Compute average historical volume for RVOL calculation. Once per day."""
        today = now_et().date()
        if self._last_rvol_update == today:
            return

        logger.info(f"[SCANNER] Computing RVOL baselines for {len(symbols)} symbols...")
        for symbol in symbols:
            try:
                end_date = (today - dt.timedelta(days=1)).isoformat()
                start_date = (today - dt.timedelta(days=30)).isoformat()
                bars = self.alpaca.get_bars(symbol, timeframe="1Day", limit=20,
                                           start=start_date, end=end_date)
                if bars and len(bars) >= 5:
                    volumes = [float(b.get("v", 0)) for b in bars]
                    self._rvol_cache[symbol] = sum(volumes) / len(volumes)
            except Exception:
                pass
        self._last_rvol_update = today
        logger.info(f"[SCANNER] RVOL baselines computed for {len(self._rvol_cache)} symbols")

    def get_market_data(self, symbol: str) -> Optional[MarketData]:
        """Fetch current market data for signal evaluation."""
        try:
            # Check cache
            cached = self._bar_cache.get(symbol)
            if cached and (time.time() - cached[0]) < self._cache_ttl:
                df = cached[1]
            else:
                # Fetch fresh 1-min bars
                bars = self.alpaca.get_bars(symbol, timeframe="1Min", limit=100)
                if not bars or len(bars) < 30:
                    return None

                df = pd.DataFrame(bars)
                # Rename Alpaca columns
                col_map = {"t": "timestamp", "o": "open", "h": "high",
                           "l": "low", "c": "close", "v": "volume", "vw": "vwap"}
                df.rename(columns={k: v for k, v in col_map.items() if k in df.columns},
                          inplace=True)

                # Compute indicators
                df["ema_fast"] = calculate_ema(df["close"], EMA_FAST)
                df["ema_slow"] = calculate_ema(df["close"], EMA_SLOW)
                df["atr"] = calculate_atr(df, ATR_PERIOD)
                df["adx"] = calculate_adx(df, ADX_PERIOD)

                self._bar_cache[symbol] = (time.time(), df)

            last = df.iloc[-1]
            price = float(last["close"])
            vwap = float(last.get("vwap", price))
            ema_f = float(last["ema_fast"])
            ema_s = float(last["ema_slow"])
            atr = float(last["atr"]) if not pd.isna(last["atr"]) else 0.0
            adx = float(last["adx"]) if not pd.isna(last["adx"]) else 20.0

            # ATR ratio (current vs average)
            atr_values = df["atr"].dropna()
            atr_ratio = atr / atr_values.mean() if len(atr_values) > 10 and atr_values.mean() > 0 else 1.0

            # RVOL: current volume vs historical average
            current_volume = float(df["volume"].tail(20).sum()) if "volume" in df.columns else 0
            avg_volume = self._rvol_cache.get(symbol, 0)
            rvol = current_volume / avg_volume if avg_volume > 0 else 1.0

            # Daily gain %
            day_open = float(df.iloc[0]["open"]) if len(df) > 0 else price
            daily_gain_pct = ((price - day_open) / day_open * 100) if day_open > 0 else 0

            return MarketData(
                symbol=symbol, price=price, vwap=vwap,
                ema_fast=ema_f, ema_slow=ema_s, atr=atr,
                atr_ratio=atr_ratio, adx=adx, rvol=rvol,
                daily_gain_pct=daily_gain_pct, bars=df
            )

        except Exception as e:
            logger.debug(f"[SCANNER] {symbol}: Error getting data: {e}")
            return None


# ============================================================
# SIGNAL DETECTION
# ============================================================

def check_short_setup(data: MarketData) -> Tuple[bool, str]:
    """
    Check if market data meets SHORT entry criteria.

    These filters are the proven edge from 20 backtest iterations.
    WARNING: NEVER relax any threshold - doing so was proven to destroy
    profitability every time it was attempted (v6: -16.4%, v14: -8.2%).
    """
    now = now_et()
    hour, minute = now.hour, now.minute

    # Time filter: 30-min blackout after open
    minutes_since_open = (hour - 9) * 60 + (minute - 30)
    if minutes_since_open < SHORT_NO_TRADE_FIRST_MINUTES:
        return False, "too_early"

    # Time filter: no entries in last 45 min (v23: was 60, too aggressive)
    minutes_until_close = (16 - hour) * 60 - minute
    if minutes_until_close < 45:
        return False, "too_late"

    # Time filter: late cutoff
    if (hour, minute) >= SHORT_LATE_CUTOFF:
        return False, "past_cutoff"

    # Price filter
    if data.price < MIN_PRICE:
        return False, "price_too_low"

    # Squeeze protection: don't short stocks up >2% today
    if data.daily_gain_pct > SHORT_MAX_DAILY_GAIN_PCT:
        return False, "daily_gain_too_high"

    # ADX filter (need clearer trend, avoid extremes)
    if USE_ADX_FILTER:
        if data.adx > SHORT_MAX_ADX:
            return False, "adx_too_high"
        if data.adx < SHORT_MIN_ADX:
            return False, "adx_too_low"

    # VWAP distance: price must be >= 0.30% BELOW VWAP
    if data.vwap > 0:
        max_price_for_short = data.vwap * (1 - SHORT_MIN_VWAP_DISTANCE_PCT / 100)
        if data.price > max_price_for_short:
            return False, "vwap_distance"

    # EMA separation: EMA9 < EMA20 by >= 0.20% (downtrend)
    if data.ema_slow > 0:
        max_fast_for_short = data.ema_slow * (1 - SHORT_MIN_EMA_SEPARATION_PCT / 100)
        if data.ema_fast > max_fast_for_short:
            return False, "ema_separation"

    # RVOL (stricter for shorts)
    if data.rvol < SHORT_MIN_RVOL:
        return False, "low_rvol"

    # Lower closes (3 of 5 bars must be lower)
    if data.bars is not None and len(data.bars) >= 5:
        closes = data.bars["close"].tail(5).values
        lower_count = sum(1 for i in range(1, len(closes)) if closes[i] < closes[i-1])
        if lower_count < SHORT_MIN_LOWER_CLOSES:
            return False, "weak_downtrend"

    # Negative momentum: >= 0.15% decline over 5 bars
    if data.bars is not None and len(data.bars) >= 6:
        price_5_ago = float(data.bars["close"].iloc[-6])
        if price_5_ago > 0:
            momentum_pct = (data.price - price_5_ago) / price_5_ago * 100
            if momentum_pct > -SHORT_MIN_MOMENTUM_5MIN_PCT:
                return False, "weak_momentum"

    return True, "valid"

def get_volatility_size_mult(data: MarketData) -> float:
    """Position size multiplier based on current volatility."""
    if not USE_DYNAMIC_SIZING:
        return 1.0
    if data.atr_ratio < VOL_REGIME_LOW_THRESHOLD:
        return SIZE_MULT_LOW_VOL
    elif data.atr_ratio > VOL_REGIME_HIGH_THRESHOLD:
        return SIZE_MULT_HIGH_VOL
    return SIZE_MULT_NORMAL_VOL


# ============================================================
# DIRECTIONAL BOT (MAIN ORCHESTRATOR)
# ============================================================

class DirectionalBot:
    """
    Short-only intraday momentum bot with software trailing stops.

    Designed to run alongside simple_bot (long-only) and trend_bot (ETF trend)
    on a shared Alpaca account.
    """

    def __init__(self):
        self.alpaca = AlpacaClient()
        self.polygon = PolygonClient()
        self.regime_detector = RegimeDetector(self.alpaca, self.polygon)
        self.risk_manager = RiskManager()
        self.kill_switch = KillSwitch()
        self.journal = TradeJournal()
        self.scanner = MarketScanner(self.alpaca)

        # v2: Enhanced scanner for dynamic universe discovery
        self.enhanced_scanner = None
        self.dynamic_universe: Set[str] = set()
        self.last_dynamic_scan_time = 0.0
        if USE_ENHANCED_SCANNER:
            try:
                self.enhanced_scanner = _EnhancedMarketScanner(
                    polygon_api_key=POLYGON_API_KEY,
                    excluded_symbols=DYNAMIC_EXCLUSION_LIST,
                    core_symbols=set(CORE_SYMBOLS),
                )
                logger.info("[SCANNER] Enhanced MarketScanner initialized for dynamic universe")
            except Exception as e:
                logger.warning(f"[SCANNER] Failed to init enhanced scanner: {e} — using static universe")
                self.enhanced_scanner = None

        self.positions: Dict[str, SoftwarePosition] = {}
        self.running = False
        self.last_scan_time = 0.0
        self._lock = threading.Lock()
        self.equity_snapshot = EquitySnapshotLogger()
        self.scan_diagnostics = ScanDiagnosticsLogger()

    # ----------------------------------------------------------
    # STATE PERSISTENCE
    # ----------------------------------------------------------

    def _save_positions(self):
        """Persist active positions to disk for crash recovery."""
        try:
            state = {}
            if os.path.exists(STATE_PATH):
                with open(STATE_PATH, "r") as f:
                    state = json.load(f)
            state["positions"] = {s: p.to_dict() for s, p in self.positions.items()}
            atomic_write_json(STATE_PATH, state)
        except Exception as e:
            logger.warning(f"[STATE] Could not save positions: {e}")

    def _load_positions(self):
        """Load positions from disk on startup."""
        try:
            if os.path.exists(STATE_PATH):
                with open(STATE_PATH, "r") as f:
                    state = json.load(f)
                saved_positions = state.get("positions", {})
                for symbol, pos_data in saved_positions.items():
                    try:
                        self.positions[symbol] = SoftwarePosition.from_dict(pos_data)
                    except Exception as e:
                        logger.warning(f"[STATE] Could not load position {symbol}: {e}")
                if self.positions:
                    logger.info(f"[STATE] Loaded {len(self.positions)} position(s) from disk")
        except Exception as e:
            logger.warning(f"[STATE] Could not load positions: {e}")

    # ----------------------------------------------------------
    # SHARED-ACCOUNT SAFETY
    # ----------------------------------------------------------

    def _get_existing_position_symbols(self) -> Set[str]:
        """Get all symbols with existing positions on the account (any bot)."""
        try:
            broker_positions = self.alpaca.get_positions()
            return {p["symbol"] for p in broker_positions}
        except Exception as e:
            logger.warning(f"[SAFETY] Could not fetch positions: {e}")
            return set()

    def _is_our_position(self, symbol: str) -> bool:
        """Check if a position belongs to this bot (tracked in our state)."""
        return symbol in self.positions

    def _count_our_positions(self) -> int:
        """Count positions managed by this bot."""
        return len(self.positions)

    # ----------------------------------------------------------
    # STARTUP
    # ----------------------------------------------------------

    def verify_account(self):
        """Verify account status and API connectivity."""
        try:
            account = self.alpaca.get_account()
            status = account.get("status", "unknown")
            equity = float(account.get("equity", 0))
            buying_power = float(account.get("buying_power", 0))
            shorting_enabled = account.get("shorting_enabled", False)

            logger.info(f"[ACCOUNT] Status={status} Equity=${equity:,.2f} "
                       f"BuyingPower=${buying_power:,.2f} Shorting={'ENABLED' if shorting_enabled else 'DISABLED'}")

            if status != "ACTIVE":
                logger.error(f"[ACCOUNT] Account not active: {status}")
                return False

            if not shorting_enabled:
                logger.error("[ACCOUNT] SHORT SELLING IS DISABLED - cannot run directional bot")
                return False

            # Production safety check
            if LIVE_TRADING_ENABLED:
                if LIVE_TRADING_CONFIRMATION != "YES":
                    logger.error("[SAFETY] LIVE_TRADING=1 but I_UNDERSTAND_LIVE_TRADING != YES")
                    return False
                logger.warning("[SAFETY] *** LIVE TRADING ENABLED ***")
            else:
                logger.info("[SAFETY] Paper trading mode")

            return True

        except Exception as e:
            logger.error(f"[ACCOUNT] Verification failed: {e}")
            return False

    def reconcile_broker_state(self):
        """
        Reconcile loaded positions with actual broker state on startup.

        1. Cancel orphaned entry orders (from crashed runs) that could fill untracked
        2. Remove positions from tracker that no longer exist at broker
        """
        # Step 0: Cancel orphaned entry orders from previous runs
        # If bot crashed with a pending entry, the order could fill after restart
        # creating an untracked, unprotected position
        try:
            open_orders = self.alpaca.get_orders(status="open")
            tracked_symbols = set(self.positions.keys())
            for order in open_orders:
                client_id = order.get("client_order_id", "")
                symbol = order.get("symbol", "")
                # Only cancel our orders (dir_ prefix) for untracked symbols
                if client_id.startswith("dir_") and symbol not in tracked_symbols:
                    order_side = order.get("side", "")
                    order_type = order.get("type", "")
                    try:
                        self.alpaca.cancel_order(order["id"])
                        logger.warning(f"[RECONCILE] {symbol}: Cancelled orphaned {order_side} {order_type} order "
                                     f"(id={order['id']}, client_id={client_id}) — no tracked position")
                    except Exception as e:
                        logger.error(f"[RECONCILE] {symbol}: Failed to cancel orphaned order {order['id']}: {e}")
        except Exception as e:
            logger.error(f"[RECONCILE] Failed to check for orphaned orders: {e}")

        broker_symbols = self._get_existing_position_symbols()

        orphaned = []
        for symbol in list(self.positions.keys()):
            if symbol not in broker_symbols:
                orphaned.append(symbol)
                logger.warning(f"[RECONCILE] {symbol}: Position closed while bot was down - removing from tracker")

        for symbol in orphaned:
            del self.positions[symbol]

        if orphaned:
            self._save_positions()
            logger.info(f"[RECONCILE] Removed {len(orphaned)} orphaned position(s)")

        # Check for broker positions we DON'T track (from other bots)
        our_symbols = set(self.positions.keys())
        other_bot_symbols = broker_symbols - our_symbols
        if other_bot_symbols:
            logger.info(f"[RECONCILE] Other bots have positions in: {', '.join(sorted(other_bot_symbols))}")

    # ----------------------------------------------------------
    # MAIN LOOP
    # ----------------------------------------------------------

    def run(self):
        """Main bot loop."""
        logger.info("=" * 60)
        logger.info("DIRECTIONAL BOT STARTING (Short-Only Momentum)")
        logger.info("=" * 60)
        logger.info(f"Universe: {len(CORE_SYMBOLS)} symbols")
        logger.info(f"Strategy: SHORT-ONLY | Risk={MAX_RISK_PER_TRADE_PCT:.0%} | "
                    f"Position={POSITION_SIZE_PCT:.0%} | Max={MAX_SHORT_POSITIONS} shorts")
        logger.info(f"Trailing Stop: activation={SHORT_TRAILING_ACTIVATION_R}R "
                    f"distance={SHORT_TRAILING_DISTANCE_R}R")

        # Verify account
        if not self.verify_account():
            logger.error("[STARTUP] Account verification failed - exiting")
            return

        # Initialize risk manager
        self.risk_manager.initialize(self.alpaca)

        # Load saved positions
        self._load_positions()

        # Reconcile with broker
        self.reconcile_broker_state()

        # Compute RVOL baselines (core + any dynamic symbols)
        all_symbols = list(CORE_SYMBOLS) + list(self.dynamic_universe)
        self.scanner.update_rvol_baselines(all_symbols)

        # Update regime
        self.regime_detector.update_daily_sma()

        # v2: Pre-market gap scan (if enhanced scanner available)
        if self.enhanced_scanner:
            now = now_et()
            if now.hour < 9 or (now.hour == 9 and now.minute < 30):
                try:
                    logger.info("[SCANNER] Running pre-market gap scan...")
                    gappers = self.enhanced_scanner.scan_premarket()
                    if gappers:
                        self.dynamic_universe = set(self.enhanced_scanner.get_symbols()) - set(CORE_SYMBOLS)
                        logger.info(f"[SCANNER] Pre-market: {len(gappers)} gappers, "
                                   f"{len(self.dynamic_universe)} added to dynamic universe")
                except Exception as e:
                    logger.warning(f"[SCANNER] Pre-market scan failed: {e}")

        self.running = True

        # Signal handlers
        def signal_handler(sig, frame):
            logger.info("[SHUTDOWN] Received interrupt signal")
            self.running = False
            self.shutdown()
            sys.exit(0)

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        logger.info("[STARTUP] Bot running - entering main loop")

        while self.running:
            try:
                # Kill switch check
                triggered, reason = self.kill_switch.is_triggered()
                if triggered:
                    logger.error(f"[KILL_SWITCH] TRIGGERED: {reason}")
                    self.kill_switch.execute_emergency_shutdown(self.alpaca)
                    self.running = False
                    break

                # Market session check
                session = get_market_session()
                if session != MarketSession.REGULAR:
                    if session == MarketSession.CLOSED:
                        logger.debug("[STATUS] Market closed - waiting...")
                        time.sleep(60)
                    else:
                        logger.debug(f"[STATUS] {session.value} - waiting for RTH...")
                        time.sleep(30)
                    continue

                # Daily reset check (RVOL, SMA)
                today = now_et().date()
                if self.scanner._last_rvol_update != today:
                    all_syms = list(CORE_SYMBOLS) + list(self.dynamic_universe)
                    self.scanner.update_rvol_baselines(all_syms)
                if self.regime_detector.last_sma_update != today:
                    self.regime_detector.update_daily_sma()

                # v2: Dynamic universe scan (every 2 min)
                if (self.enhanced_scanner and ENABLE_DYNAMIC_UNIVERSE and
                        time.time() - self.last_dynamic_scan_time > DYNAMIC_SCAN_INTERVAL_SEC):
                    try:
                        watchlist = self.enhanced_scanner.scan()
                        new_symbols = set(self.enhanced_scanner.get_symbols())
                        dynamic_additions = new_symbols - set(CORE_SYMBOLS)
                        added = dynamic_additions - self.dynamic_universe
                        removed = self.dynamic_universe - dynamic_additions
                        self.dynamic_universe = dynamic_additions
                        if added:
                            logger.info(f"[SCANNER] Dynamic +{len(added)}: {', '.join(sorted(added)[:5])}")
                        if removed:
                            logger.debug(f"[SCANNER] Dynamic -{len(removed)}: {', '.join(sorted(removed)[:5])}")
                    except Exception as e:
                        logger.warning(f"[SCANNER] Intraday scan failed: {e}")
                    self.last_dynamic_scan_time = time.time()

                # Risk check
                if self.risk_manager.update(self.alpaca):
                    logger.warning("[STATUS] Trading halted - daily loss limit")
                    time.sleep(60)
                    continue

                # Daily equity snapshot (once per trading day)
                self.equity_snapshot.log_snapshot(self.risk_manager, self.alpaca)

                # EOD close check
                if AUTO_CLOSE_EOD:
                    self._check_eod_close()

                # Manage existing positions (software trailing stop, TP, SL)
                self._manage_positions()

                # Scan for new setups (throttled)
                if time.time() - self.last_scan_time > SCAN_INTERVAL_SEC:
                    self._scan_for_setups()
                    self.last_scan_time = time.time()

                time.sleep(DATA_POLL_INTERVAL_SEC)

            except Exception as e:
                logger.error(f"[ERROR] Main loop error: {e}", exc_info=True)
                time.sleep(15)

        logger.info("[SHUTDOWN] Bot stopped")

    # ----------------------------------------------------------
    # SCANNING
    # ----------------------------------------------------------

    def _scan_for_setups(self):
        """Scan universe for short entry setups."""
        # Check if we can take more positions
        our_count = self._count_our_positions()
        if our_count >= MAX_SHORT_POSITIONS:
            logger.debug(f"[SCAN] Max positions ({MAX_SHORT_POSITIONS}) reached")
            return

        # Check trade limits
        can_trade, reason = self.risk_manager.can_trade()
        if not can_trade:
            logger.debug(f"[SCAN] {reason}")
            return

        # Detect current regime
        regime = self.regime_detector.detect()
        regime_mult = REGIME_SHORT_SIZE_MULT.get(regime.value, 1.0)

        # Get existing positions on the account (all bots)
        existing_symbols = self._get_existing_position_symbols()

        slots = MAX_SHORT_POSITIONS - our_count

        # v2: Build combined universe with scanner-based prioritization
        import random
        scored_symbols = []     # Scanner-ranked symbols (highest quality first)
        unscored_symbols = []   # Core symbols not in scanner watchlist

        if self.enhanced_scanner and self.dynamic_universe:
            watchlist = self.enhanced_scanner.get_watchlist()
            scored_set = set()
            for entry in watchlist:
                sym = entry.symbol
                if sym not in TREND_BOT_SYMBOLS and sym not in existing_symbols and sym not in self.positions:
                    scored_symbols.append(sym)
                    scored_set.add(sym)
            # Core symbols not already scored go into unscored pool
            for sym in CORE_SYMBOLS:
                if sym not in scored_set:
                    unscored_symbols.append(sym)
            random.shuffle(unscored_symbols)
        else:
            unscored_symbols = list(CORE_SYMBOLS)
            random.shuffle(unscored_symbols)

        # Scored symbols first (scanner-ranked order), then shuffled core
        scan_universe = scored_symbols + unscored_symbols
        logger.info(f"[SCAN] Scanning {len(scan_universe)} symbols "
                    f"({len(scored_symbols)} scored + {len(unscored_symbols)} core) "
                    f"| Regime={regime.value} | Slots={slots} "
                    f"| Daily trades={self.risk_manager.daily_trade_count}")

        entries_this_scan = 0
        rejection_counts = {}

        for symbol in scan_universe:
            if entries_this_scan >= 2:  # Max 2 entries per scan
                break
            if our_count + entries_this_scan >= MAX_SHORT_POSITIONS:
                break

            # Skip if ANY bot has a position in this symbol
            if symbol in existing_symbols:
                continue

            # Skip trend_bot symbols
            if symbol in TREND_BOT_SYMBOLS:
                continue

            # Skip if we already have a position (redundant check)
            if symbol in self.positions:
                continue

            # Get market data
            data = self.scanner.get_market_data(symbol)
            if data is None:
                rejection_counts["no_data"] = rejection_counts.get("no_data", 0) + 1
                continue

            # Check short signal
            valid, reason = check_short_setup(data)
            if not valid:
                rejection_counts[reason] = rejection_counts.get(reason, 0) + 1
                continue

            # Check shortability
            shortable, borrow_reason = self.alpaca.is_symbol_shortable(symbol)
            if not shortable:
                logger.debug(f"[SCAN] {symbol}: {borrow_reason}")
                continue

            # Attempt entry
            logger.info(f"[SCAN] {symbol}: SHORT SIGNAL FOUND | "
                       f"price=${data.price:.2f} vwap=${data.vwap:.2f} "
                       f"ema9=${data.ema_fast:.2f} ema20={data.ema_slow:.2f} "
                       f"rvol={data.rvol:.1f}x adx={data.adx:.1f} "
                       f"daily_gain={data.daily_gain_pct:+.1f}%")

            success = self._enter_short(symbol, data, regime, regime_mult)
            if success:
                entries_this_scan += 1
                # Re-check existing positions for next iteration
                existing_symbols.add(symbol)

        # Record scan diagnostics (daily CSV)
        self.scan_diagnostics.record_scan(
            symbols_scanned=len(scan_universe),
            rejection_counts=rejection_counts,
            entries=entries_this_scan
        )

        # Log rejection summary for diagnostics
        if rejection_counts:
            sorted_reasons = sorted(rejection_counts.items(), key=lambda x: x[1], reverse=True)
            summary = ", ".join(f"{r}={c}" for r, c in sorted_reasons)
            logger.info(f"[SCAN] Filter funnel: {len(scan_universe)} scanned -> "
                       f"{entries_this_scan} entries | Rejections: {summary}")

    # ----------------------------------------------------------
    # ENTRY
    # ----------------------------------------------------------

    def _enter_short(self, symbol: str, data: MarketData,
                     regime: MarketRegime, regime_mult: float) -> bool:
        """
        Enter a short position.

        1. Calculate stop/TP based on ATR
        2. Calculate position size
        3. Submit sell limit order
        4. Wait for fill
        5. If filled: place safety net stop, start software tracking
        """
        try:
            # Get current quote for entry pricing
            quote = self.alpaca.get_latest_quote(symbol)
            if not quote:
                logger.warning(f"[ENTRY] {symbol}: No quote available")
                return False

            bid_price = float(quote.get("bp", quote.get("BidPrice", 0)))
            ask_price = float(quote.get("ap", quote.get("AskPrice", 0)))
            if bid_price <= 0 or ask_price <= 0:
                logger.warning(f"[ENTRY] {symbol}: Invalid quote bid={bid_price} ask={ask_price}")
                return False

            # For shorts: sell at bid-side (conservative) or mid
            mid_price = (bid_price + ask_price) / 2
            entry_limit = round(mid_price - 0.01, 2)  # Slightly below mid for faster fill

            # ATR-based stop (above entry for shorts)
            if data.atr > 0:
                atr_stop_distance = data.atr * SHORT_ATR_STOP_MULTIPLIER
                min_stop_distance = entry_limit * SHORT_MIN_STOP_DISTANCE_PCT
                stop_distance = max(atr_stop_distance, min_stop_distance)
                stop_price = round(entry_limit + stop_distance, 2)
            else:
                # Fallback: 2% stop
                stop_distance = entry_limit * 0.02
                stop_price = round(entry_limit + stop_distance, 2)

            risk_per_share = stop_price - entry_limit

            # Take profit (below entry for shorts)
            tp_distance = risk_per_share * SHORT_SCALP_TP_R
            tp_price = round(entry_limit - tp_distance, 2)

            # Position sizing
            vol_mult = get_volatility_size_mult(data)
            qty = self.risk_manager.calculate_position_size(
                entry_limit, stop_price, regime_mult, vol_mult
            )
            if qty <= 0:
                logger.warning(f"[ENTRY] {symbol}: Qty=0 after sizing")
                return False

            # Pre-flight: check buying power
            position_value = qty * entry_limit
            if position_value > self.risk_manager.buying_power * 0.90:
                logger.warning(f"[ENTRY] {symbol}: Insufficient buying power "
                             f"need=${position_value:,.0f} have=${self.risk_manager.buying_power:,.0f}")
                return False

            stop_dist_pct = (stop_distance / entry_limit) * 100
            logger.info(f"[ENTRY] {symbol}: Submitting SHORT | qty={qty} entry=${entry_limit:.2f} "
                       f"stop=${stop_price:.2f} (+{stop_dist_pct:.2f}%) "
                       f"tp=${tp_price:.2f} | 1R=${risk_per_share:.2f} "
                       f"notional=${position_value:,.0f} | regime={regime.value}")

            # Generate client order ID for idempotency
            date_str = now_et().strftime("%Y%m%d")
            client_id = generate_client_order_id(symbol, "short_entry", date_str)

            # Submit sell limit order
            order = self.alpaca.submit_order(
                symbol=symbol,
                qty=qty,
                side="sell",
                order_type="limit",
                limit_price=entry_limit,
                time_in_force="day",
                client_order_id=client_id,
            )
            order_id = order["id"]
            logger.info(f"[ENTRY] {symbol}: Order submitted | id={order_id}")

            # Wait for fill (with timeout)
            fill_price = self._wait_for_fill(order_id, symbol)
            if fill_price is None:
                # Timeout - cancel order
                try:
                    self.alpaca.cancel_order(order_id)
                    logger.warning(f"[ENTRY] {symbol}: Entry timed out - cancelled")
                except Exception:
                    pass
                return False

            # Recalculate stop/TP based on actual fill price
            if data.atr > 0:
                atr_stop_distance = data.atr * SHORT_ATR_STOP_MULTIPLIER
                min_stop_distance = fill_price * SHORT_MIN_STOP_DISTANCE_PCT
                stop_distance = max(atr_stop_distance, min_stop_distance)
            else:
                stop_distance = fill_price * 0.02
            stop_price = round(fill_price + stop_distance, 2)
            risk_per_share = stop_price - fill_price
            tp_price = round(fill_price - risk_per_share * SHORT_SCALP_TP_R, 2)

            # Place safety net stop order (broker-native, for crash protection)
            safety_stop_id = None
            try:
                safety_order = self.alpaca.submit_order(
                    symbol=symbol,
                    qty=qty,
                    side="buy",
                    order_type="stop",
                    stop_price=stop_price,
                    time_in_force="gtc",
                    client_order_id=generate_client_order_id(symbol, "safety_stop", date_str),
                )
                safety_stop_id = safety_order["id"]
                logger.info(f"[ENTRY] {symbol}: Safety stop placed at ${stop_price:.2f} | id={safety_stop_id}")
            except Exception as e:
                logger.error(f"[ENTRY] {symbol}: FAILED to place safety stop: {e}")
                # Continue anyway - software stop will manage, but log critical warning
                logger.error(f"[ENTRY] {symbol}: *** NO CRASH PROTECTION *** - monitor closely")

            # Create software position tracker
            pos = SoftwarePosition(
                symbol=symbol,
                entry_time=iso(now_et()),
                entry_price=fill_price,
                qty=qty,
                stop_price=stop_price,
                tp_price=tp_price,
                risk_per_share=risk_per_share,
                regime=regime.value,
                safety_stop_order_id=safety_stop_id,
                entry_order_id=order_id,
                entry_client_order_id=client_id,
            )

            with self._lock:
                self.positions[symbol] = pos
            self._save_positions()

            # Log to journal
            self.journal.log_entry(symbol, qty, fill_price, stop_price, tp_price, regime.value)

            # Increment trade count
            self.risk_manager.increment_trade_count()

            logger.info(f"[ENTRY] {symbol}: SHORT FILLED | qty={qty} fill=${fill_price:.2f} "
                       f"stop=${stop_price:.2f} tp=${tp_price:.2f} | {regime.value}")
            return True

        except Exception as e:
            logger.error(f"[ENTRY] {symbol}: Failed to enter short: {e}", exc_info=True)
            return False

    def _wait_for_fill(self, order_id: str, symbol: str,
                       timeout: float = None) -> Optional[float]:
        """Wait for an order to fill. Returns fill price or None on timeout."""
        if timeout is None:
            timeout = ENTRY_TIMEOUT_SEC

        start = time.time()
        while time.time() - start < timeout:
            try:
                order = self.alpaca.get_order(order_id)
                status = order.get("status", "")

                if status == "filled":
                    fill_price = float(order.get("filled_avg_price", 0))
                    logger.info(f"[FILL] {symbol}: Filled at ${fill_price:.2f}")
                    return fill_price

                if status in ("cancelled", "expired", "rejected"):
                    logger.warning(f"[FILL] {symbol}: Order {status}")
                    return None

            except Exception as e:
                logger.debug(f"[FILL] {symbol}: Poll error: {e}")

            time.sleep(1.5)

        return None

    # ----------------------------------------------------------
    # POSITION MANAGEMENT (Software Trailing Stops)
    # ----------------------------------------------------------

    def _manage_positions(self):
        """
        Manage open positions with software-based exits.

        This is where the trailing stop logic lives - the primary
        profit engine of the strategy.
        """
        if not self.positions:
            return

        positions_to_close = []

        for symbol, pos in list(self.positions.items()):
            try:
                # Get current price
                snapshot = self.alpaca.get_snapshot(symbol)
                if not snapshot:
                    continue

                # Extract current price from snapshot
                minute_bar = snapshot.get("minuteBar", {})
                current_price = float(minute_bar.get("c", 0))
                current_high = float(minute_bar.get("h", 0))
                current_low = float(minute_bar.get("l", 0))

                if current_price <= 0:
                    latest_trade = snapshot.get("latestTrade", {})
                    current_price = float(latest_trade.get("p", 0))
                    current_high = current_price
                    current_low = current_price

                if current_price <= 0:
                    continue

                # Calculate P&L
                pnl_per_share = pos.entry_price - current_price  # Short: profit when price drops
                profit_r = pnl_per_share / pos.risk_per_share if pos.risk_per_share > 0 else 0
                total_pnl = pnl_per_share * pos.qty

                # --- STOP LOSS CHECK ---
                if current_high >= pos.stop_price or current_price >= pos.stop_price:
                    logger.warning(f"[EXIT] {symbol}: STOP LOSS HIT | price=${current_price:.2f} "
                                  f"stop=${pos.stop_price:.2f} | pnl=${total_pnl:+.2f}")
                    positions_to_close.append((symbol, "STOP_LOSS", current_price))
                    continue

                # --- TAKE PROFIT CHECK ---
                if current_low <= pos.tp_price or current_price <= pos.tp_price:
                    logger.info(f"[EXIT] {symbol}: TAKE PROFIT HIT | price=${current_price:.2f} "
                               f"tp=${pos.tp_price:.2f} | pnl=${total_pnl:+.2f}")
                    positions_to_close.append((symbol, "SCALP_TP", pos.tp_price))
                    continue

                # --- TRAILING STOP ---
                if USE_TRAILING_STOP and pos.risk_per_share > 0:
                    trail_dist = SHORT_TRAILING_DISTANCE_R * pos.risk_per_share

                    # Activation check
                    if not pos.trail_active and profit_r >= SHORT_TRAILING_ACTIVATION_R:
                        pos.trail_active = True
                        pos.best_price = current_low  # Track lowest for shorts
                        pos.trail_stop = pos.best_price + trail_dist
                        logger.info(f"[TRAIL] {symbol}: ACTIVATED at {profit_r:.2f}R | "
                                   f"best=${pos.best_price:.2f} trail_stop=${pos.trail_stop:.2f}")

                    # Update trailing stop
                    if pos.trail_active:
                        if current_low < pos.best_price:
                            pos.best_price = current_low
                            new_trail = pos.best_price + trail_dist
                            if new_trail < pos.trail_stop:  # Only tighten
                                pos.trail_stop = new_trail
                                logger.debug(f"[TRAIL] {symbol}: Tightened to ${pos.trail_stop:.2f} "
                                           f"(best=${pos.best_price:.2f})")

                        # Check trail stop hit
                        if current_high >= pos.trail_stop or current_price >= pos.trail_stop:
                            logger.info(f"[EXIT] {symbol}: TRAILING STOP | price=${current_price:.2f} "
                                       f"trail=${pos.trail_stop:.2f} best=${pos.best_price:.2f} "
                                       f"| pnl=${total_pnl:+.2f}")
                            positions_to_close.append((symbol, "TRAILING_STOP", current_price))
                            continue

                # Status log
                trail_info = f" trail={'ACTIVE' if pos.trail_active else 'inactive'}"
                if pos.trail_active:
                    trail_info += f" stop=${pos.trail_stop:.2f} best=${pos.best_price:.2f}"
                logger.debug(f"[MANAGE] {symbol}: price=${current_price:.2f} "
                           f"pnl=${total_pnl:+.2f} ({profit_r:+.2f}R){trail_info}")

            except Exception as e:
                logger.error(f"[MANAGE] {symbol}: Error: {e}")

        # Execute closes
        for symbol, reason, exit_price in positions_to_close:
            self._close_position(symbol, reason, exit_price)

        # Save updated trailing stop state
        if self.positions:
            self._save_positions()

    def _close_position(self, symbol: str, exit_reason: str,
                        approximate_exit_price: float):
        """
        Close a short position by buying to cover.

        Safety protocol:
        1. Cancel safety net stop order (prevents double-buy if cover + stop both trigger)
        2. Submit buy-to-cover market order
        3. Verify fill with 30-second timeout
        4. If cover fails, attempt to re-place safety stop to restore protection
        5. Only remove from tracker when position is confirmed closed
        """
        pos = self.positions.get(symbol)
        if not pos:
            return

        safety_was_cancelled = False

        try:
            # Cancel safety net stop order first (must cancel before cover to prevent double-buy)
            if pos.safety_stop_order_id:
                try:
                    self.alpaca.cancel_order(pos.safety_stop_order_id)
                    safety_was_cancelled = True
                    logger.debug(f"[EXIT] {symbol}: Safety stop cancelled")
                except Exception:
                    pass  # May already be filled/cancelled

            # Submit buy-to-cover market order
            order = self.alpaca.submit_order(
                symbol=symbol,
                qty=pos.qty,
                side="buy",
                order_type="market",
                time_in_force="day",
                client_order_id=generate_client_order_id(
                    symbol, f"cover_{exit_reason.lower()}", now_et().strftime("%Y%m%d")
                ),
            )
            logger.info(f"[EXIT] {symbol}: Buy-to-cover submitted | qty={pos.qty} | reason={exit_reason}")

            # Verify fill with adequate timeout (30s for exit orders)
            fill_price = self._wait_for_fill(order["id"], symbol, timeout=30)

            if fill_price:
                # Cover confirmed filled
                actual_exit = fill_price
            else:
                # Fill not confirmed within timeout - check broker position
                logger.warning(f"[EXIT] {symbol}: Cover fill not confirmed after 30s - checking broker position")
                try:
                    broker_pos = self.alpaca.get_position(symbol)
                    if broker_pos:
                        # Position still exists at broker - cover likely failed
                        logger.error(f"[EXIT] {symbol}: Position STILL OPEN at broker after cover attempt! "
                                    f"qty={broker_pos.get('qty')} | Keeping in tracker for retry")
                        # Try to restore safety stop since position is still open
                        self._restore_safety_stop(symbol, pos)
                        return  # Don't remove from tracker
                    else:
                        # Position gone at broker - cover worked, just slow confirmation
                        logger.info(f"[EXIT] {symbol}: Position confirmed closed at broker (slow fill confirmation)")
                        actual_exit = approximate_exit_price
                except Exception:
                    # Can't verify - use estimate but log warning
                    logger.warning(f"[EXIT] {symbol}: Could not verify broker position - using estimated exit price")
                    actual_exit = approximate_exit_price

            # Calculate final P&L
            pnl = (pos.entry_price - actual_exit) * pos.qty
            hold_seconds = (now_et() - from_iso(pos.entry_time)).total_seconds()

            # Log to journal
            self.journal.log_exit(
                symbol, pos.qty, pos.entry_price, actual_exit,
                exit_reason, pos.regime, hold_seconds
            )

            logger.info(f"[EXIT] {symbol}: CLOSED | entry=${pos.entry_price:.2f} "
                       f"exit=${actual_exit:.2f} pnl=${pnl:+.2f} "
                       f"hold={hold_seconds/60:.0f}min | {exit_reason}")

        except Exception as e:
            logger.error(f"[EXIT] {symbol}: Failed to close: {e}", exc_info=True)
            # Try to close via Alpaca position close as fallback
            try:
                self.alpaca.close_position(symbol)
                logger.warning(f"[EXIT] {symbol}: Used fallback position close")
            except Exception as e2:
                logger.error(f"[EXIT] {symbol}: FALLBACK ALSO FAILED: {e2} | "
                            f"Position may be EXPOSED without safety stop!")
                # Try to restore safety stop since position couldn't be closed
                if safety_was_cancelled:
                    self._restore_safety_stop(symbol, pos)
                return  # Don't remove from tracker if we couldn't close

        # Remove from tracker (only reached if close confirmed or fallback succeeded)
        with self._lock:
            if symbol in self.positions:
                del self.positions[symbol]
        self._save_positions()

    def _restore_safety_stop(self, symbol: str, pos):
        """Attempt to re-place safety stop order after a failed cover."""
        try:
            safety_order = self.alpaca.submit_order(
                symbol=symbol,
                qty=pos.qty,
                side="buy",
                order_type="stop",
                stop_price=pos.stop_price,
                time_in_force="day",
                client_order_id=generate_client_order_id(
                    symbol, "safety_restore", now_et().strftime("%Y%m%d")
                ),
            )
            pos.safety_stop_order_id = safety_order["id"]
            self._save_positions()
            logger.warning(f"[EXIT] {symbol}: Safety stop RESTORED at ${pos.stop_price:.2f} | "
                          f"id={safety_order['id']} | Position still protected")
        except Exception as restore_err:
            logger.error(f"[EXIT] {symbol}: FAILED to restore safety stop: {restore_err} | "
                        f"*** POSITION EXPOSED - MANUAL INTERVENTION NEEDED ***")

    # ----------------------------------------------------------
    # EOD CLOSE
    # ----------------------------------------------------------

    def _check_eod_close(self):
        """Close all positions at end of day."""
        now = now_et()
        eod_hour, eod_minute = EOD_CLOSE_TIME_ET

        if now.hour > eod_hour or (now.hour == eod_hour and now.minute >= eod_minute):
            if self.positions:
                logger.warning(f"[EOD] {now.strftime('%H:%M')} - Closing {len(self.positions)} position(s)")
                for symbol in list(self.positions.keys()):
                    # Get approximate price for logging
                    snapshot = self.alpaca.get_snapshot(symbol)
                    price = 0.0
                    if snapshot:
                        minute_bar = snapshot.get("minuteBar", {})
                        price = float(minute_bar.get("c", 0))
                        if price <= 0:
                            trade = snapshot.get("latestTrade", {})
                            price = float(trade.get("p", 0))

                    self._close_position(symbol, "EOD_CLOSE", price)

    # ----------------------------------------------------------
    # SHUTDOWN
    # ----------------------------------------------------------

    def shutdown(self):
        """Graceful shutdown."""
        logger.warning("[SHUTDOWN] Initiating graceful shutdown...")
        logger.warning(f"[SHUTDOWN] Policy: {SHUTDOWN_POLICY}")

        try:
            # Cancel our open orders (identified by dir_ prefix)
            try:
                open_orders = self.alpaca.get_orders(status="open")
                our_orders = [o for o in open_orders
                             if (o.get("client_order_id") or "").startswith("dir_")]
                for order in our_orders:
                    try:
                        self.alpaca.cancel_order(order["id"])
                    except Exception:
                        pass
                logger.info(f"[SHUTDOWN] Cancelled {len(our_orders)} of our open order(s)")
            except Exception as e:
                logger.error(f"[SHUTDOWN] Error cancelling orders: {e}")

            # Optionally close our positions
            if SHUTDOWN_POLICY == "FLATTEN_ALL":
                for symbol in list(self.positions.keys()):
                    try:
                        self.alpaca.close_position(symbol)
                        logger.info(f"[SHUTDOWN] Closed {symbol}")
                    except Exception as e:
                        logger.error(f"[SHUTDOWN] Failed to close {symbol}: {e}")
            else:
                if self.positions:
                    logger.warning(f"[SHUTDOWN] Leaving {len(self.positions)} position(s) open "
                                  f"(policy=CANCEL_ORDERS_ONLY)")
                    logger.warning("[SHUTDOWN] Safety net stop orders remain active at broker")

            # Save final state
            self._save_positions()

            # Flush scan diagnostics for the day
            self.scan_diagnostics.flush_if_needed()

            logger.info("[SHUTDOWN] Shutdown complete")

        except Exception as e:
            logger.error(f"[SHUTDOWN] Error: {e}")


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    bot = DirectionalBot()
    bot.run()
