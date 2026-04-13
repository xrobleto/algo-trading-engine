"""
Cross-Asset Trend-Following Bot (Managed Futures Style)
=======================================================

What it does
------------
- Trades a diversified cross-asset ETF universe (bonds, commodities, FX)
  using dual-momentum channel breakout signals
- Sizes positions using inverse-volatility risk parity
- Supports both LONG and SHORT exposure via inverse ETFs
- Rebalances weekly (Wednesdays at 10:30 AM ET)
- Provides genuine portfolio diversification against equity momentum strategies

Signal construction
-------------------
For each asset:
  1. Absolute momentum: 12-1 month return (skip most recent month)
  2. Donchian channel: 100-day high/low breakout
  3. Trend filter: 200-day SMA

  LONG:  momentum > 0 AND price > SMA200 AND (above channel high OR midpoint)
  SHORT: momentum < 0 AND price < SMA200 AND (below channel low OR midpoint)
         --> implemented by buying the inverse ETF (e.g., TLT -> TBT)
  FLAT:  otherwise

Position sizing (risk parity)
-----------------------------
  weight_i = (target_vol / realized_vol_i) / sum(target_vol / realized_vol_j)
  - Normalized to max gross exposure of 100% (no leverage)
  - Individual position capped at 25%
  - Vol floor at 12% annualized to prevent over-concentration

Data/Execution
--------------
- Uses Alpaca for orders, Polygon or Alpaca for daily bars
- Weekly rebalance: Wednesday 10:30-11:00 AM ET (avoids Friday trend_bot rebalance)

IMPORTANT SECURITY NOTE
-----------------------
Do NOT paste API keys into this file. Use environment variables.

Dependencies
------------
pip install alpaca-py pandas numpy requests pytz python-dotenv

Run
---
python cross_asset_bot.py
"""

from __future__ import annotations

import os
import json
import time
import math
import csv
import traceback
import logging
import hashlib
from dataclasses import dataclass, asdict, fields
from datetime import datetime, timedelta, date, time as dt_time
from typing import Any, Dict, List, Tuple, Optional, Set
from logging.handlers import RotatingFileHandler

import numpy as np
import pandas as pd
import pytz
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Alpaca (alpaca-py)
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import MarketOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame

# Load config from .env file if present
from pathlib import Path
try:
    from dotenv import load_dotenv
    _env_path = Path(__file__).parent.parent / "config" / "cross_asset_bot.env"
    if not _env_path.exists():
        _env_path = Path(__file__).parent / "cross_asset_bot.env"
    if _env_path.exists():
        load_dotenv(_env_path)
except ImportError:
    pass


# =============================================================================
# TIMEZONE & DIRECTORY PATHS
# =============================================================================

ET = pytz.timezone("America/New_York")

ALGO_ROOT = Path(__file__).parent.parent
_output_root = Path(os.getenv("ALGO_OUTPUT_DIR", "")) if os.getenv("ALGO_OUTPUT_DIR") else ALGO_ROOT
DATA_DIR = _output_root / "data" / "state"
LOGS_DIR = _output_root / "logs"
DATA_DIR.mkdir(parents=True, exist_ok=True)
LOGS_DIR.mkdir(exist_ok=True)


# =============================================================================
# CROSS-ASSET UNIVERSE
# =============================================================================

# LONG instruments (buy when trend is UP)
RATES_LONG = ["TLT", "IEF", "SHY"]
COMMODITIES_LONG = ["GLD", "SLV", "DBC", "USO", "UNG", "DBA"]
FX_LONG = ["UUP", "FXE", "FXY"]

# INVERSE instruments (buy when underlying trend is DOWN)
INVERSE_MAP: Dict[str, str] = {
    "TLT": "TBT",    # short 20yr bonds
    "GLD": "GLL",    # short gold
}

LONG_UNIVERSE = sorted(set(RATES_LONG + COMMODITIES_LONG + FX_LONG))
INVERSE_UNIVERSE = sorted(set(INVERSE_MAP.values()))
ALL_SYMBOLS = sorted(set(LONG_UNIVERSE + INVERSE_UNIVERSE))
KNOWN_SYMBOLS: Set[str] = set(ALL_SYMBOLS)

# Reverse map: inverse ETF -> underlying (for logging/diagnostics)
INVERSE_REVERSE_MAP: Dict[str, str] = {v: k for k, v in INVERSE_MAP.items()}


# =============================================================================
# SIGNAL PARAMETERS
# =============================================================================

MOMENTUM_LOOKBACK_DAYS = 252      # 12-month absolute momentum
MOMENTUM_SKIP_DAYS = 21           # skip most recent month (reversal avoidance)
DONCHIAN_CHANNEL_DAYS = 100       # N-day high/low channel breakout
TREND_SMA_DAYS = 200              # 200-day SMA trend filter


# =============================================================================
# POSITION SIZING (Inverse-Volatility Risk Parity)
# =============================================================================

TARGET_VOL_PER_POSITION = 0.10    # 10% annualized vol target per position
VOL_LOOKBACK_DAYS = 60            # realized vol lookback
VOL_FLOOR_ANNUAL = 0.12           # minimum vol to prevent over-leverage
MAX_WEIGHT_PER_ASSET = 0.25       # 25% max per position
MAX_GROSS_EXPOSURE = 1.00         # no leverage — max 100% of sleeve


# =============================================================================
# REBALANCE SCHEDULE
# =============================================================================

REBALANCE_WEEKDAY = 2             # Wednesday (0=Mon, 2=Wed, 4=Fri)
REBALANCE_TIME_ET = (10, 30)      # 10:30 AM ET (30 min post-open for clean fills)
REBALANCE_DEADLINE_ET = (11, 0)   # 11:00 AM ET (30 min execution window)


# =============================================================================
# RISK CONTROLS
# =============================================================================

PORTFOLIO_TRAILING_STOP_PCT = 0.15    # -15% from equity peak => flatten all
POSITION_STOP_LOSS_PCT = 0.20        # -20% from entry => exit position
CIRCUIT_BREAKER_MAX_FAILURES = 3     # 3 consecutive API failures => pause
CIRCUIT_BREAKER_PAUSE_SEC = 300      # 5 minute pause


# =============================================================================
# OPERATIONAL CONFIG
# =============================================================================

DRY_RUN = os.getenv("DRY_RUN", "false").lower() in ("1", "true", "yes", "y")
DATA_SOURCE = os.getenv("DATA_SOURCE", "polygon").lower()

# Paths
_state_raw = os.getenv("XASSET_STATE_PATH", "cross_asset_state.json")
_log_raw = os.getenv("XASSET_LOG_PATH", "cross_asset_trades.csv")
STATE_PATH = _state_raw if os.path.isabs(_state_raw) else str(DATA_DIR / _state_raw)
LOG_PATH = _log_raw if os.path.isabs(_log_raw) else str(DATA_DIR / _log_raw)

_rebalance_log_name = os.getenv("XASSET_REBALANCE_LOG_PATH", "cross_asset_rebalances.csv")
REBALANCE_LOG_PATH = str((_output_root / "project_notes" / _rebalance_log_name))
(_output_root / "project_notes").mkdir(exist_ok=True)

# Logging
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
_log_file_name = os.getenv("XASSET_LOG_FILE", "cross_asset_bot.log")
MAX_LOG_SIZE_MB = 50
MAX_LOG_BACKUPS = 5

# Live trading safety
LIVE_TRADING_ENABLED = os.getenv("LIVE_TRADING", "0") == "1"

# Kill switch
KILL_SWITCH_FILE = str(DATA_DIR / "XASSET_KILL_SWITCH")
KILL_SWITCH_ENV = os.getenv("XASSET_KILL_SWITCH", "0") == "1"
SHUTDOWN_POLICY = os.getenv("XASSET_SHUTDOWN_POLICY", "CANCEL_ORDERS_ONLY").upper()

# Trade sizing minimums
MIN_TRADE_NOTIONAL_USD = 50.0
MIN_TRADE_SHARES = 0.01
ALLOW_FRACTIONAL = True

# Network
HTTP_TIMEOUT_DEFAULT = 15
HTTP_TIMEOUT_POLYGON = 30
POLYGON_RATE_LIMIT_BATCH = 10
POLYGON_RATE_LIMIT_SLEEP = 1


# =============================================================================
# LOGGING SETUP
# =============================================================================

def setup_logging() -> logging.Logger:
    """Setup logging with console + rotating file output."""
    import sys
    logger = logging.getLogger("CrossAssetBot")
    logger.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))

    console_handler = logging.StreamHandler(stream=sys.stdout)
    console_handler.setLevel(logging.INFO)
    if sys.platform == "win32":
        try:
            sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        except AttributeError:
            pass
    console_formatter = logging.Formatter(
        fmt="[%(asctime)s] %(levelname)s - %(message)s", datefmt="%H:%M:%S"
    )
    console_handler.setFormatter(console_formatter)

    file_handler = RotatingFileHandler(
        str(LOGS_DIR / _log_file_name),
        maxBytes=MAX_LOG_SIZE_MB * 1024 * 1024,
        backupCount=MAX_LOG_BACKUPS,
    )
    file_handler.setLevel(logging.DEBUG)
    file_formatter = logging.Formatter(
        fmt="[%(asctime)s] %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    )
    file_handler.setFormatter(file_formatter)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    return logger


log = setup_logging()


# =============================================================================
# NETWORK HARDENING
# =============================================================================

def create_retry_session(
    retries: int = 3,
    backoff_factor: float = 1.0,
    status_forcelist: tuple = (429, 500, 502, 503, 504),
) -> requests.Session:
    """Create a requests Session with retry/backoff for transient errors."""
    session = requests.Session()
    retry_strategy = Retry(
        total=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        allowed_methods=["GET", "POST"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


_http_session: Optional[requests.Session] = None


def get_http_session() -> requests.Session:
    """Get or create the global retry-enabled HTTP session."""
    global _http_session
    if _http_session is None:
        _http_session = create_retry_session()
    return _http_session


# =============================================================================
# TIME UTILITIES
# =============================================================================

def now_et() -> datetime:
    return datetime.now(tz=ET)


def today_iso_et() -> str:
    return now_et().date().isoformat()


# =============================================================================
# STATE MANAGEMENT
# =============================================================================

@dataclass
class CrossAssetState:
    """Persisted bot state (JSON serializable)."""
    last_rebalance_date_iso: Optional[str] = None
    equity_peak: Optional[float] = None
    last_equity: Optional[float] = None
    rebalance_in_progress: bool = False
    rebalance_started_at_iso: Optional[str] = None
    last_target_weights: Optional[Dict[str, float]] = None

    # Signal state (persisted for drift checks between rebalances)
    last_signals: Optional[Dict[str, str]] = None      # symbol -> "LONG"|"SHORT"|"FLAT"
    last_entry_prices: Optional[Dict[str, float]] = None  # symbol -> entry price (stop loss)

    # Portfolio trailing stop
    portfolio_halted: bool = False
    portfolio_halt_reason: Optional[str] = None

    # Circuit breaker
    consecutive_api_failures: int = 0
    circuit_breaker_until_iso: Optional[str] = None


def load_state(path: str) -> CrossAssetState:
    """Load state from JSON, filtering unknown keys for forward compatibility."""
    if not os.path.exists(path):
        log.info("No existing state file found. Starting fresh.")
        return CrossAssetState()
    try:
        with open(path, "r", encoding="utf-8") as f:
            raw = json.load(f)
        allowed = {fld.name for fld in fields(CrossAssetState)}
        filtered = {k: v for k, v in raw.items() if k in allowed}
        dropped = sorted(set(raw.keys()) - allowed)
        if dropped:
            log.warning(f"[STATE] Dropped unknown keys: {dropped}")
        log.info(f"Loaded state from {path}")
        return CrossAssetState(**filtered)
    except Exception as e:
        log.error(f"Failed to load state from {path}: {e}")
        raise


def save_state(path: str, state: CrossAssetState) -> None:
    """Atomically save state via temp file + rename."""
    temp_path = path + ".tmp"
    try:
        with open(temp_path, "w", encoding="utf-8") as f:
            json.dump(asdict(state), f, indent=2, sort_keys=True)
        os.replace(temp_path, path)
        log.debug(f"State saved to {path}")
    except Exception as e:
        log.error(f"Failed to save state to {path}: {e}")
        raise


STALE_REBALANCE_THRESHOLD_MIN = 30


def clear_stale_rebalance_flag(state: CrossAssetState, state_path: str) -> None:
    """Clear rebalance_in_progress if stale (from crash/hard-kill)."""
    if not state.rebalance_in_progress:
        return
    if not state.rebalance_started_at_iso:
        log.warning("[STATE] rebalance_in_progress=True but no timestamp; clearing")
        state.rebalance_in_progress = False
        save_state(state_path, state)
        return
    try:
        started_at = datetime.fromisoformat(state.rebalance_started_at_iso)
        if started_at.tzinfo is None:
            started_at = ET.localize(started_at)
        elapsed_min = (now_et() - started_at).total_seconds() / 60.0
        is_stale = elapsed_min > STALE_REBALANCE_THRESHOLD_MIN
        is_different_day = started_at.date() != now_et().date()
        if is_stale or is_different_day:
            reason = "older than threshold" if is_stale else "from different day"
            log.warning(f"[STATE] Clearing stale rebalance flag ({reason}, {elapsed_min:.1f}min ago)")
            state.rebalance_in_progress = False
            state.rebalance_started_at_iso = None
            save_state(state_path, state)
    except Exception as e:
        log.warning(f"[STATE] Failed to parse rebalance timestamp: {e}; clearing flag")
        state.rebalance_in_progress = False
        state.rebalance_started_at_iso = None
        save_state(state_path, state)


# =============================================================================
# KILL SWITCH
# =============================================================================

class KillSwitch:
    """Emergency halt mechanism (file + env var)."""

    def is_triggered(self) -> Tuple[bool, Optional[str]]:
        if KILL_SWITCH_ENV:
            return True, "XASSET_KILL_SWITCH env var set"
        if os.path.exists(KILL_SWITCH_FILE):
            return True, f"{KILL_SWITCH_FILE} file detected"
        return False, None

    def execute_emergency_shutdown(self, trading_client: TradingClient) -> None:
        """Cancel XABOT_ orders and optionally close cross-asset positions."""
        log.error("[KILL_SWITCH] EMERGENCY SHUTDOWN INITIATED")
        if DRY_RUN:
            log.warning("[KILL_SWITCH] DRY_RUN mode — skipping actual cancels/closes")
            return

        # 1. Cancel our orders
        try:
            all_orders = trading_client.get_orders()
            our_orders = [
                o for o in all_orders
                if (o.client_order_id or "").startswith("XABOT_")
            ]
            for order in our_orders:
                try:
                    trading_client.cancel_order_by_id(order.id)
                except Exception:
                    pass
            other_count = len(all_orders) - len(our_orders)
            log.warning(
                f"[KILL_SWITCH] Cancelled {len(our_orders)} XABOT_ orders"
                f"{f' (preserved {other_count} other orders)' if other_count else ''}"
            )
        except Exception as e:
            log.error(f"[KILL_SWITCH] Error cancelling orders: {e}")

        # 2. Flatten if policy demands it
        if SHUTDOWN_POLICY == "FLATTEN_ALL":
            try:
                positions = trading_client.get_all_positions()
                our_positions = [p for p in positions if p.symbol in ALL_SYMBOLS]
                for pos in our_positions:
                    try:
                        trading_client.close_position(pos.symbol)
                    except Exception as e:
                        log.error(f"[KILL_SWITCH] Failed to close {pos.symbol}: {e}")
                other_count = len(positions) - len(our_positions)
                log.warning(
                    f"[KILL_SWITCH] Closed {len(our_positions)} positions"
                    f"{f' (preserved {other_count} others)' if other_count else ''}"
                )
            except Exception as e:
                log.error(f"[KILL_SWITCH] Error closing positions: {e}")


kill_switch = KillSwitch()


# =============================================================================
# CIRCUIT BREAKER
# =============================================================================

class CircuitBreaker:
    """Pauses trading after consecutive API failures."""

    def __init__(self):
        self._failure_count = 0
        self._halted_until: Optional[datetime] = None

    def record_api_failure(self, context: str) -> None:
        self._failure_count += 1
        if self._failure_count >= CIRCUIT_BREAKER_MAX_FAILURES:
            self._halted_until = now_et() + timedelta(seconds=CIRCUIT_BREAKER_PAUSE_SEC)
            log.error(
                f"[CIRCUIT_BREAKER] {self._failure_count} consecutive failures "
                f"({context}) — pausing until {self._halted_until.strftime('%H:%M:%S')}"
            )

    def record_success(self) -> None:
        self._failure_count = 0

    def is_halted(self) -> bool:
        if self._halted_until is None:
            return False
        if now_et() >= self._halted_until:
            log.info("[CIRCUIT_BREAKER] Pause expired, resuming")
            self._halted_until = None
            self._failure_count = 0
            return False
        return True


circuit_breaker = CircuitBreaker()


# =============================================================================
# DATA FETCHING
# =============================================================================

class DataCache:
    """Cache for daily bars to avoid redundant fetches within the same day."""
    bars: Optional[pd.DataFrame] = None
    cache_date: Optional[str] = None
    cached_tickers: Optional[set] = None
    cached_lookback: int = 0

    def get_bars(
        self,
        data_client: StockHistoricalDataClient,
        tickers: List[str],
        lookback_days: int,
    ) -> pd.DataFrame:
        """Get cached bars or fetch if stale/insufficient."""
        today = today_iso_et()
        requested = set(tickers)

        if (
            self.bars is not None
            and self.cache_date == today
            and self.cached_tickers is not None
            and requested.issubset(self.cached_tickers)
            and lookback_days <= self.cached_lookback
        ):
            log.debug("Data cache hit")
            return self.bars

        fetch_tickers = sorted(
            requested | (self.cached_tickers or set())
        ) if self.cache_date == today else sorted(requested)
        fetch_lookback = max(lookback_days, self.cached_lookback) if self.cache_date == today else lookback_days

        log.info(f"Fetching {fetch_lookback}d bars for {len(fetch_tickers)} tickers...")

        end_dt_utc = now_et().astimezone(pytz.UTC)
        start_dt_utc = (now_et() - timedelta(days=fetch_lookback)).astimezone(pytz.UTC)

        if DATA_SOURCE == "polygon":
            bars = fetch_daily_bars_polygon(
                tickers=fetch_tickers,
                start_date=(now_et().date() - timedelta(days=fetch_lookback)),
                end_date=now_et().date(),
            )
        else:
            bars = fetch_daily_bars_alpaca(
                data_client=data_client,
                tickers=fetch_tickers,
                start=start_dt_utc,
                end=end_dt_utc,
            )

        self.bars = bars
        self.cache_date = today
        self.cached_tickers = set(fetch_tickers)
        self.cached_lookback = fetch_lookback
        log.info(f"Cached {len(bars)} bars for {len(fetch_tickers)} tickers")
        return bars


data_cache = DataCache()


def fetch_daily_bars_alpaca(
    data_client: StockHistoricalDataClient,
    tickers: List[str],
    start: datetime,
    end: datetime,
) -> pd.DataFrame:
    """Fetch daily OHLC bars from Alpaca."""
    try:
        req = StockBarsRequest(
            symbol_or_symbols=tickers,
            timeframe=TimeFrame.Day,
            start=start,
            end=end,
            adjustment="all",
        )
        bars = data_client.get_stock_bars(req).df
        if bars.empty:
            raise RuntimeError("No bars returned from Alpaca.")
        if isinstance(bars.index, pd.MultiIndex):
            bars = bars.reset_index()
        bars["timestamp"] = pd.to_datetime(bars["timestamp"], utc=True).dt.tz_convert(ET).dt.date
        bars = bars.sort_values(["symbol", "timestamp"])
        # Keep OHLC columns (needed for Donchian channel)
        keep_cols = ["timestamp", "symbol"]
        for col in ("open", "high", "low", "close"):
            if col in bars.columns:
                keep_cols.append(col)
        bars = bars[keep_cols]
        log.debug(f"Fetched {len(bars)} bars from Alpaca")
        return bars
    except Exception as e:
        log.error(f"Failed to fetch daily bars from Alpaca: {e}")
        raise


def fetch_daily_bars_polygon(
    tickers: List[str],
    start_date: date,
    end_date: date,
) -> pd.DataFrame:
    """Fetch daily OHLC bars from Polygon with rate limit pacing."""
    api_key = os.getenv("POLYGON_API_KEY", "")
    if not api_key:
        raise RuntimeError("POLYGON_API_KEY not set but DATA_SOURCE='polygon'.")
    rows: List[dict] = []
    failed_symbols: List[str] = []
    session = get_http_session()

    for i, sym in enumerate(tickers):
        if i > 0 and i % POLYGON_RATE_LIMIT_BATCH == 0:
            time.sleep(POLYGON_RATE_LIMIT_SLEEP)
        try:
            url = (
                f"https://api.polygon.io/v2/aggs/ticker/{sym}/range/1/day/"
                f"{start_date.isoformat()}/{end_date.isoformat()}"
            )
            params = {
                "adjusted": "true",
                "sort": "asc",
                "limit": 50000,
                "apiKey": api_key,
            }
            r = session.get(url, params=params, timeout=HTTP_TIMEOUT_POLYGON)
            r.raise_for_status()
            data = r.json()
            results = data.get("results", []) or []
            for item in results:
                ts = datetime.fromtimestamp(item["t"] / 1000, tz=pytz.UTC).astimezone(ET).date()
                rows.append({
                    "timestamp": ts,
                    "symbol": sym,
                    "open": float(item.get("o", 0)),
                    "high": float(item.get("h", 0)),
                    "low": float(item.get("l", 0)),
                    "close": float(item["c"]),
                })
            if not results:
                failed_symbols.append(sym)
                log.warning(f"[POLYGON] No bars for {sym}")
        except Exception as e:
            failed_symbols.append(sym)
            log.error(f"Failed to fetch Polygon data for {sym}: {e}")
            continue

    if failed_symbols:
        log.warning(
            f"[POLYGON] Failed/empty for {len(failed_symbols)}/{len(tickers)}: "
            f"{failed_symbols[:10]}{'...' if len(failed_symbols) > 10 else ''}"
        )

    df = pd.DataFrame(rows)
    if df.empty:
        raise RuntimeError("No bars returned from Polygon.")
    df = df.sort_values(["symbol", "timestamp"])
    log.info(
        f"Fetched {len(df)} bars from Polygon for "
        f"{len(df['symbol'].unique())}/{len(tickers)} symbols"
    )
    return df


# =============================================================================
# DATA EXTRACTION
# =============================================================================

def get_close_series(bars: pd.DataFrame, symbol: str) -> Optional[pd.Series]:
    """Extract close price series for a symbol. Returns None if missing."""
    sub = bars[bars["symbol"] == symbol]
    if sub.empty:
        return None
    s = pd.Series(sub["close"].values, index=pd.to_datetime(sub["timestamp"]))
    return s.sort_index()


def get_high_series(bars: pd.DataFrame, symbol: str) -> Optional[pd.Series]:
    """Extract high price series for a symbol. Returns None if missing."""
    if "high" not in bars.columns:
        return get_close_series(bars, symbol)  # fallback
    sub = bars[bars["symbol"] == symbol]
    if sub.empty:
        return None
    s = pd.Series(sub["high"].values, index=pd.to_datetime(sub["timestamp"]))
    return s.sort_index()


def get_low_series(bars: pd.DataFrame, symbol: str) -> Optional[pd.Series]:
    """Extract low price series for a symbol. Returns None if missing."""
    if "low" not in bars.columns:
        return get_close_series(bars, symbol)  # fallback
    sub = bars[bars["symbol"] == symbol]
    if sub.empty:
        return None
    s = pd.Series(sub["low"].values, index=pd.to_datetime(sub["timestamp"]))
    return s.sort_index()


# =============================================================================
# VOLATILITY
# =============================================================================

def annualized_realized_vol(close: pd.Series, lookback: int) -> float:
    """Calculate annualized realized volatility with vol floor."""
    if len(close) < lookback + 1:
        raise RuntimeError(
            f"Not enough data for vol (need {lookback + 1}, got {len(close)})."
        )
    rets = close.pct_change().dropna()
    window = rets.iloc[-lookback:]

    if window.isna().any() or np.isinf(window).any():
        raise RuntimeError("NaN/inf in returns for volatility calculation.")

    vol_daily = float(window.std(ddof=1))
    if vol_daily <= 0 or np.isnan(vol_daily) or np.isinf(vol_daily):
        raise RuntimeError(f"Invalid volatility: {vol_daily}")

    vol_annual = vol_daily * math.sqrt(252.0)
    return max(vol_annual, VOL_FLOOR_ANNUAL)


# =============================================================================
# SIGNAL CONSTRUCTION
# =============================================================================

def compute_signals(bars: pd.DataFrame) -> Dict[str, str]:
    """
    Compute signal for each asset in the LONG universe.

    Returns:
        Dict mapping symbol -> "LONG" | "SHORT" | "FLAT"
        "SHORT" means buy the inverse ETF, not actual short selling.
    """
    signals: Dict[str, str] = {}
    min_bars_needed = MOMENTUM_LOOKBACK_DAYS + MOMENTUM_SKIP_DAYS + 1

    for symbol in LONG_UNIVERSE:
        close = get_close_series(bars, symbol)
        high = get_high_series(bars, symbol)
        low = get_low_series(bars, symbol)

        if close is None or len(close) < min_bars_needed:
            log.debug(f"[SIGNAL] {symbol}: insufficient data ({len(close) if close is not None else 0} bars)")
            signals[symbol] = "FLAT"
            continue

        # --- SIGNAL 1: Absolute Momentum (12-1 month) ---
        if MOMENTUM_SKIP_DAYS > 0:
            mom_close = close.iloc[:-MOMENTUM_SKIP_DAYS]
        else:
            mom_close = close

        if len(mom_close) > MOMENTUM_LOOKBACK_DAYS:
            start_price = float(mom_close.iloc[-MOMENTUM_LOOKBACK_DAYS])
            end_price = float(mom_close.iloc[-1])
            abs_momentum = (end_price / start_price - 1.0) if start_price > 0 else 0.0
        else:
            abs_momentum = 0.0

        # --- SIGNAL 2: Donchian Channel Breakout ---
        current_price = float(close.iloc[-1])

        if high is not None and low is not None and len(high) >= DONCHIAN_CHANNEL_DAYS:
            channel_high = float(high.iloc[-DONCHIAN_CHANNEL_DAYS:].max())
            channel_low = float(low.iloc[-DONCHIAN_CHANNEL_DAYS:].min())
            channel_mid = (channel_high + channel_low) / 2.0

            price_above_channel = current_price >= channel_high
            price_below_channel = current_price <= channel_low
            price_above_mid = current_price >= channel_mid
            price_below_mid = current_price < channel_mid
        else:
            # Fallback if no OHLC data
            price_above_channel = False
            price_below_channel = False
            price_above_mid = True
            price_below_mid = False

        # --- SIGNAL 3: Trend Filter (200-day SMA) ---
        if len(close) >= TREND_SMA_DAYS:
            sma_200 = float(close.iloc[-TREND_SMA_DAYS:].mean())
            above_sma = current_price > sma_200
            below_sma = current_price < sma_200
        else:
            above_sma = True
            below_sma = False

        # --- COMBINED SIGNAL ---
        # LONG: momentum positive AND above SMA AND (above channel or midpoint)
        # SHORT: momentum negative AND below SMA AND (below channel or midpoint) AND inverse exists
        # FLAT: otherwise
        if abs_momentum > 0 and above_sma and (price_above_channel or price_above_mid):
            signals[symbol] = "LONG"
        elif abs_momentum < 0 and below_sma and (price_below_channel or price_below_mid):
            if symbol in INVERSE_MAP:
                signals[symbol] = "SHORT"
            else:
                signals[symbol] = "FLAT"
        else:
            signals[symbol] = "FLAT"

        log.debug(
            f"[SIGNAL] {symbol}: mom={abs_momentum:+.3f} sma200={'above' if above_sma else 'below'} "
            f"channel={'above' if price_above_channel else 'below' if price_below_channel else 'mid'} "
            f"=> {signals[symbol]}"
        )

    return signals


# =============================================================================
# POSITION SIZING (Inverse-Volatility Risk Parity)
# =============================================================================

def compute_target_weights(
    bars: pd.DataFrame,
    signals: Dict[str, str],
) -> Tuple[Dict[str, float], Dict[str, Any]]:
    """
    Compute target portfolio weights using inverse-volatility risk parity.

    For SHORT signals, the weight maps to the inverse ETF symbol.

    Returns:
        (target_weights: {trade_symbol: weight}, diagnostics: dict)
    """
    raw_weights: Dict[str, float] = {}
    diag: Dict[str, Any] = {"signals": signals, "vols": {}, "raw_weights": {}}

    # Step 1: Compute inverse-vol weight for each active signal
    for symbol, signal in signals.items():
        if signal == "FLAT":
            continue

        close = get_close_series(bars, symbol)
        if close is None or len(close) < VOL_LOOKBACK_DAYS + 1:
            log.warning(f"[SIZING] {symbol}: insufficient data for vol calc")
            continue

        try:
            vol = annualized_realized_vol(close, VOL_LOOKBACK_DAYS)
        except RuntimeError as e:
            log.warning(f"[SIZING] {symbol}: vol calc failed: {e}")
            continue

        diag["vols"][symbol] = vol
        inv_vol = TARGET_VOL_PER_POSITION / vol

        # Determine actual symbol to trade
        if signal == "SHORT":
            trade_symbol = INVERSE_MAP.get(symbol)
            if not trade_symbol:
                continue
        else:
            trade_symbol = symbol

        raw_weights[trade_symbol] = inv_vol

    # Step 2: Normalize to MAX_GROSS_EXPOSURE
    total_raw = sum(raw_weights.values())
    if total_raw <= 0:
        return {}, diag

    if total_raw > MAX_GROSS_EXPOSURE:
        scale = MAX_GROSS_EXPOSURE / total_raw
    else:
        scale = 1.0

    target_weights: Dict[str, float] = {}
    for sym, raw_w in raw_weights.items():
        w = raw_w * scale
        w = min(w, MAX_WEIGHT_PER_ASSET)
        target_weights[sym] = w

    # Step 3: Iterative re-normalization after capping (max 5 rounds)
    for _ in range(5):
        total_w = sum(target_weights.values())
        if total_w <= MAX_GROSS_EXPOSURE + 0.001:
            break
        scale_down = MAX_GROSS_EXPOSURE / total_w
        target_weights = {
            s: min(w * scale_down, MAX_WEIGHT_PER_ASSET)
            for s, w in target_weights.items()
        }

    diag["raw_weights"] = raw_weights
    diag["final_weights"] = target_weights
    return target_weights, diag


# =============================================================================
# RISK CONTROLS
# =============================================================================

def check_portfolio_trailing_stop(state: CrossAssetState, current_equity: float) -> bool:
    """Check portfolio trailing stop. Returns True if halt triggered."""
    if state.equity_peak is None:
        state.equity_peak = current_equity
        return False

    state.equity_peak = max(state.equity_peak, current_equity)
    drawdown = (state.equity_peak - current_equity) / state.equity_peak

    if drawdown >= PORTFOLIO_TRAILING_STOP_PCT:
        state.portfolio_halted = True
        state.portfolio_halt_reason = (
            f"Portfolio trailing stop: {drawdown:.1%} drawdown "
            f"(peak ${state.equity_peak:,.2f}, current ${current_equity:,.2f})"
        )
        return True
    return False


def check_position_stop_loss(
    symbol: str,
    current_price: float,
    entry_price: float,
) -> bool:
    """Check if individual position stop is hit. Returns True if should exit."""
    if entry_price <= 0:
        return False
    loss_pct = (entry_price - current_price) / entry_price
    return loss_pct >= POSITION_STOP_LOSS_PCT


# =============================================================================
# CLIENT/BROKER HELPERS
# =============================================================================

def get_trading_client() -> TradingClient:
    """Initialize Alpaca TradingClient."""
    key = os.getenv("ALPACA_API_KEY", "")
    secret = os.getenv("ALPACA_SECRET_KEY", "")
    if not key or not secret:
        raise RuntimeError("Missing ALPACA_API_KEY / ALPACA_SECRET_KEY env vars.")
    paper = not LIVE_TRADING_ENABLED
    if not paper:
        log.warning("*** LIVE TRADING MODE — REAL MONEY ***")
    else:
        log.info("Paper trading mode")
    return TradingClient(api_key=key, secret_key=secret, paper=paper)


def get_data_client() -> StockHistoricalDataClient:
    """Initialize Alpaca DataClient."""
    key = os.getenv("ALPACA_API_KEY", "")
    secret = os.getenv("ALPACA_SECRET_KEY", "")
    if not key or not secret:
        raise RuntimeError("Missing ALPACA_API_KEY / ALPACA_SECRET_KEY env vars.")
    return StockHistoricalDataClient(api_key=key, secret_key=secret)


def get_portfolio_equity(trading: TradingClient) -> float:
    """Get total portfolio equity."""
    try:
        acct = trading.get_account()
        equity = float(acct.equity)
        log.debug(f"Portfolio equity: ${equity:,.2f}")
        return equity
    except Exception as e:
        log.error(f"Failed to get portfolio equity: {e}")
        raise


def get_positions(trading: TradingClient) -> Dict[str, Dict[str, float]]:
    """Get all current positions as {symbol: {qty, market_value, avg_entry_price, current_price}}."""
    try:
        pos = trading.get_all_positions()
        out: Dict[str, Dict[str, float]] = {}
        for p in pos:
            out[p.symbol] = {
                "qty": float(p.qty),
                "market_value": float(p.market_value),
                "avg_entry_price": float(p.avg_entry_price) if p.avg_entry_price else 0.0,
                "current_price": float(p.current_price) if p.current_price else 0.0,
            }
        log.debug(f"Retrieved {len(out)} positions")
        return out
    except Exception as e:
        log.error(f"Failed to get positions: {e}")
        raise


def is_market_open(trading_client: TradingClient) -> bool:
    """Check if market is currently open via Alpaca clock API."""
    try:
        clock = trading_client.get_clock()
        return clock.is_open
    except Exception as e:
        log.warning(f"Failed to check market hours: {e}")
        dt = now_et()
        if dt.weekday() >= 5:
            return False
        return (9, 30) <= (dt.hour, dt.minute) < (16, 0)


def get_latest_price(
    data_client: StockHistoricalDataClient, symbol: str
) -> float:
    """Get latest daily close price for sizing estimates."""
    try:
        end = now_et().astimezone(pytz.UTC)
        start = (now_et() - timedelta(days=10)).astimezone(pytz.UTC)
        bars = fetch_daily_bars_alpaca(data_client, [symbol], start, end)
        s = get_close_series(bars, symbol)
        if s is None or s.empty:
            return 0.0
        return float(s.iloc[-1])
    except Exception as e:
        log.warning(f"Failed to get latest price for {symbol}: {e}")
        return 0.0


# =============================================================================
# ORDER ID GENERATION
# =============================================================================

def generate_client_order_id(reason: str, symbol: str, side: str) -> str:
    """Generate XABOT_ prefixed order ID."""
    date_str = now_et().date().isoformat()
    time_suffix = now_et().strftime("%H%M%S")
    reason_abbrev = reason[:4]
    base = f"XABOT_{date_str}_{time_suffix}_{reason_abbrev}_{symbol}_{side}"
    if len(base) <= 48:
        return base
    h = hashlib.md5(base.encode()).hexdigest()[:8]
    return f"XABOT_{date_str}_{time_suffix}_{symbol}_{h}"


# =============================================================================
# TRADE LOGGING
# =============================================================================

def log_trade(
    side: str,
    symbol: str,
    qty: float,
    price: float,
    reason: str = "",
    client_order_id: str = "",
) -> None:
    """Append a trade record to the CSV log."""
    try:
        write_header = not os.path.exists(LOG_PATH) or os.path.getsize(LOG_PATH) == 0
        with open(LOG_PATH, "a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            if write_header:
                writer.writerow([
                    "timestamp", "side", "symbol", "qty", "price",
                    "notional", "reason", "client_order_id",
                ])
            writer.writerow([
                now_et().isoformat(),
                side,
                symbol,
                f"{qty:.4f}",
                f"{price:.2f}",
                f"{qty * price:.2f}",
                reason,
                client_order_id,
            ])
    except Exception as e:
        log.warning(f"Failed to log trade: {e}")


def log_rebalance_summary(
    target_weights: Dict[str, float],
    signals: Dict[str, str],
    equity: float,
    orders_placed: int,
) -> None:
    """Append rebalance summary to CSV."""
    try:
        write_header = not os.path.exists(REBALANCE_LOG_PATH) or os.path.getsize(REBALANCE_LOG_PATH) == 0
        with open(REBALANCE_LOG_PATH, "a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            if write_header:
                writer.writerow([
                    "timestamp", "equity", "orders_placed",
                    "long_count", "short_count", "flat_count",
                    "gross_exposure", "target_weights_json",
                ])
            long_count = sum(1 for s in signals.values() if s == "LONG")
            short_count = sum(1 for s in signals.values() if s == "SHORT")
            flat_count = sum(1 for s in signals.values() if s == "FLAT")
            gross = sum(target_weights.values())
            writer.writerow([
                now_et().isoformat(),
                f"{equity:.2f}",
                orders_placed,
                long_count,
                short_count,
                flat_count,
                f"{gross:.4f}",
                json.dumps(target_weights),
            ])
    except Exception as e:
        log.warning(f"Failed to log rebalance summary: {e}")


# =============================================================================
# REBALANCE WINDOW CHECK
# =============================================================================

def is_rebalance_window(dt: datetime) -> bool:
    """Check if current time is within rebalance window (static fallback)."""
    if dt.weekday() != REBALANCE_WEEKDAY:
        return False
    start_h, start_m = REBALANCE_TIME_ET
    end_h, end_m = REBALANCE_DEADLINE_ET
    return (dt.hour, dt.minute) >= (start_h, start_m) and (dt.hour, dt.minute) <= (end_h, end_m)


def is_rebalance_window_dynamic(trading_client: TradingClient) -> bool:
    """Check rebalance window using Alpaca clock (handles early-close days)."""
    now_dt = now_et()
    if now_dt.weekday() != REBALANCE_WEEKDAY:
        return False
    try:
        clock = trading_client.get_clock()
        if not clock.is_open:
            return False
        start_h, start_m = REBALANCE_TIME_ET
        end_h, end_m = REBALANCE_DEADLINE_ET
        current_time = now_dt.time()
        return dt_time(start_h, start_m) <= current_time <= dt_time(end_h, end_m)
    except Exception as e:
        log.warning(f"Clock API failed, using static window: {e}")
        return is_rebalance_window(now_dt)


# =============================================================================
# REBALANCE EXECUTION
# =============================================================================

def rebalance(
    trading: TradingClient,
    data_client: StockHistoricalDataClient,
    state: CrossAssetState,
) -> None:
    """Execute weekly portfolio rebalance."""
    if state.rebalance_in_progress:
        log.warning("[REBALANCE] Already in progress. Skipping.")
        return

    state.rebalance_in_progress = True
    state.rebalance_started_at_iso = now_et().isoformat()
    save_state(STATE_PATH, state)

    try:
        # 1. Check market is open
        if not is_market_open(trading):
            log.info("[REBALANCE] Market closed. Skipping.")
            return

        # 2. Get equity + positions
        total_equity = get_portfolio_equity(trading)
        positions = get_positions(trading)
        log.info(f"[REBALANCE] Equity: ${total_equity:,.2f} | Positions: {len(positions)}")

        # 3. Portfolio trailing stop check
        if check_portfolio_trailing_stop(state, total_equity):
            log.critical(f"[REBALANCE] {state.portfolio_halt_reason}")
            _flatten_all(trading, positions, state)
            return

        # 4. Fetch bars (need ~400+ calendar days)
        lookback_cal = int((MOMENTUM_LOOKBACK_DAYS + MOMENTUM_SKIP_DAYS + 10) * 1.43) + 30
        bars = data_cache.get_bars(data_client, ALL_SYMBOLS, lookback_cal)

        # 5. Position-level stop-loss check (before computing new signals)
        stopped_symbols: Set[str] = set()
        if state.last_entry_prices:
            for symbol, entry_price in state.last_entry_prices.items():
                pos_info = positions.get(symbol)
                if not pos_info:
                    continue
                current_price = pos_info.get("current_price", 0.0)
                if current_price <= 0:
                    current_price = pos_info["market_value"] / max(pos_info["qty"], 0.001)
                if check_position_stop_loss(symbol, current_price, entry_price):
                    log.warning(
                        f"[REBALANCE] Stop-loss hit for {symbol}: "
                        f"entry=${entry_price:.2f}, current=${current_price:.2f} "
                        f"({(entry_price - current_price) / entry_price:.1%} loss)"
                    )
                    stopped_symbols.add(symbol)

        # 6. Compute signals
        signals = compute_signals(bars)
        # Override stopped symbols to FLAT
        for sym in stopped_symbols:
            if sym in signals:
                signals[sym] = "FLAT"

        long_count = sum(1 for s in signals.values() if s == "LONG")
        short_count = sum(1 for s in signals.values() if s == "SHORT")
        log.info(f"[REBALANCE] Signals: {long_count} LONG, {short_count} SHORT, "
                 f"{len(signals) - long_count - short_count} FLAT")

        # 7. Compute target weights
        target_weights, diag = compute_target_weights(bars, signals)
        gross_exposure = sum(target_weights.values())
        log.info(f"[REBALANCE] Target weights: {len(target_weights)} positions, "
                 f"gross exposure: {gross_exposure:.1%}")
        for sym, w in sorted(target_weights.items(), key=lambda x: -x[1]):
            underlying = INVERSE_REVERSE_MAP.get(sym, sym)
            signal = signals.get(underlying, "?")
            vol = diag.get("vols", {}).get(underlying, 0)
            log.info(f"  {sym}: {w:.1%} (signal={signal}, vol={vol:.1%})")

        # 8. Compute current weights
        current_weights: Dict[str, float] = {}
        for sym in ALL_SYMBOLS:
            mv = positions.get(sym, {}).get("market_value", 0.0)
            current_weights[sym] = mv / total_equity if total_equity > 0 else 0.0

        # 9. Build order plan (sells first, then buys)
        sell_orders: List[dict] = []
        buy_orders: List[dict] = []

        for sym in ALL_SYMBOLS:
            tw = target_weights.get(sym, 0.0)
            cw = current_weights.get(sym, 0.0)
            delta_w = tw - cw
            delta_notional = delta_w * total_equity

            if abs(delta_notional) < MIN_TRADE_NOTIONAL_USD:
                continue

            # Estimate price for qty calculation
            close = get_close_series(bars, sym)
            if close is not None and len(close) > 0:
                est_price = float(close.iloc[-1])
            else:
                est_price = get_latest_price(data_client, sym)
            if est_price <= 0:
                log.warning(f"[REBALANCE] Cannot price {sym}, skipping")
                continue

            qty = abs(delta_notional) / est_price
            if qty < MIN_TRADE_SHARES:
                continue

            side = OrderSide.BUY if delta_notional > 0 else OrderSide.SELL

            # Clamp sell qty to available shares
            if side == OrderSide.SELL:
                available = positions.get(sym, {}).get("qty", 0.0)
                qty = min(qty, available)
                if qty <= 0:
                    continue

            order_info = {
                "symbol": sym,
                "qty": qty,
                "side": side,
                "est_price": est_price,
                "target_weight": tw,
                "current_weight": cw,
            }
            if side == OrderSide.SELL:
                sell_orders.append(order_info)
            else:
                buy_orders.append(order_info)

        # 10. Execute: sells first (free buying power), then buys
        orders_placed = 0
        for order in sell_orders:
            if _submit_rebalance_order(trading, order, state):
                orders_placed += 1
        for order in buy_orders:
            if _submit_rebalance_order(trading, order, state):
                orders_placed += 1

        # 11. Update state
        state.last_target_weights = target_weights
        state.last_signals = signals
        state.last_rebalance_date_iso = now_et().date().isoformat()
        state.last_equity = total_equity

        log.info(f"[REBALANCE] Complete: {orders_placed} orders placed "
                 f"({len(sell_orders)} sells, {len(buy_orders)} buys)")
        log_rebalance_summary(target_weights, signals, total_equity, orders_placed)

    except Exception as e:
        log.error(f"[REBALANCE] Error: {e}")
        traceback.print_exc()
        circuit_breaker.record_api_failure("rebalance")
        raise
    finally:
        state.rebalance_in_progress = False
        state.rebalance_started_at_iso = None
        save_state(STATE_PATH, state)


def _submit_rebalance_order(
    trading: TradingClient,
    order_info: dict,
    state: CrossAssetState,
) -> bool:
    """Submit a single rebalance order. Returns True on success."""
    symbol = order_info["symbol"]
    qty = order_info["qty"]
    side = order_info["side"]
    est_price = order_info["est_price"]
    side_str = "BUY" if side == OrderSide.BUY else "SELL"

    if DRY_RUN:
        log.info(f"[DRY_RUN] {side_str} {qty:.4f} {symbol} @ ~${est_price:.2f}")
        log_trade(f"DRY_RUN_{side_str}", symbol, qty, est_price, "rebalance")
        return True

    client_oid = generate_client_order_id("reb", symbol, side_str)

    order_req = MarketOrderRequest(
        symbol=symbol,
        qty=round(qty, 4) if ALLOW_FRACTIONAL else int(qty),
        side=side,
        time_in_force=TimeInForce.DAY,
        client_order_id=client_oid,
    )

    try:
        result = trading.submit_order(order_req)
        log.info(f"[ORDER] {side_str} {qty:.4f} {symbol} @ ~${est_price:.2f} | oid={client_oid}")

        # Track entry price for stop-loss
        if side == OrderSide.BUY:
            if state.last_entry_prices is None:
                state.last_entry_prices = {}
            state.last_entry_prices[symbol] = est_price
        elif side == OrderSide.SELL:
            if state.last_entry_prices and symbol in state.last_entry_prices:
                del state.last_entry_prices[symbol]

        log_trade(side_str, symbol, qty, est_price, "rebalance", client_oid)
        circuit_breaker.record_success()
        return True
    except Exception as e:
        log.error(f"[ORDER] Failed: {side_str} {qty:.4f} {symbol}: {e}")
        circuit_breaker.record_api_failure(f"order_{symbol}")
        return False


def _flatten_all(
    trading: TradingClient,
    positions: Dict[str, Dict[str, float]],
    state: CrossAssetState,
) -> None:
    """Emergency flatten: close all cross-asset positions."""
    our_symbols = [s for s in positions if s in KNOWN_SYMBOLS]
    if not our_symbols:
        log.info("[FLATTEN] No cross-asset positions to close")
        return

    log.warning(f"[FLATTEN] Closing {len(our_symbols)} positions: {our_symbols}")
    for sym in our_symbols:
        try:
            if not DRY_RUN:
                trading.close_position(sym)
            log.info(f"[FLATTEN] Closed {sym}")
            if state.last_entry_prices and sym in state.last_entry_prices:
                del state.last_entry_prices[sym]
        except Exception as e:
            log.error(f"[FLATTEN] Failed to close {sym}: {e}")


# =============================================================================
# CONFIGURATION VALIDATION
# =============================================================================

def validate_configuration() -> None:
    """Validate all configuration parameters before starting."""
    log.info("=== Validating Configuration ===")
    errors: List[str] = []

    if not os.getenv("ALPACA_API_KEY"):
        errors.append("Missing ALPACA_API_KEY")
    if not os.getenv("ALPACA_SECRET_KEY"):
        errors.append("Missing ALPACA_SECRET_KEY")
    if DATA_SOURCE == "polygon" and not os.getenv("POLYGON_API_KEY"):
        errors.append("DATA_SOURCE=polygon but POLYGON_API_KEY not set")

    if MOMENTUM_LOOKBACK_DAYS <= 0:
        errors.append(f"Invalid MOMENTUM_LOOKBACK_DAYS: {MOMENTUM_LOOKBACK_DAYS}")
    if VOL_LOOKBACK_DAYS <= 0:
        errors.append(f"Invalid VOL_LOOKBACK_DAYS: {VOL_LOOKBACK_DAYS}")
    if not (0 < MAX_WEIGHT_PER_ASSET <= 1.0):
        errors.append(f"Invalid MAX_WEIGHT_PER_ASSET: {MAX_WEIGHT_PER_ASSET}")
    if not (0 < MAX_GROSS_EXPOSURE <= 1.0):
        errors.append(f"Invalid MAX_GROSS_EXPOSURE: {MAX_GROSS_EXPOSURE}")
    if not (0 < PORTFOLIO_TRAILING_STOP_PCT <= 1.0):
        errors.append(f"Invalid PORTFOLIO_TRAILING_STOP_PCT: {PORTFOLIO_TRAILING_STOP_PCT}")
    if not ALL_SYMBOLS:
        errors.append("Empty universe")
    if not (0 <= REBALANCE_WEEKDAY <= 4):
        errors.append(f"Invalid REBALANCE_WEEKDAY: {REBALANCE_WEEKDAY}")

    if errors:
        for err in errors:
            log.error(f"Config error: {err}")
        raise RuntimeError(f"Configuration validation failed: {len(errors)} error(s)")

    log.info("Configuration validation passed")
    log.info(f"Universe: {len(LONG_UNIVERSE)} long + {len(INVERSE_UNIVERSE)} inverse ETFs")
    log.info(f"Data source: {DATA_SOURCE}")
    log.info(f"DRY_RUN: {DRY_RUN}")
    log.info(f"Rebalance: {['Mon','Tue','Wed','Thu','Fri'][REBALANCE_WEEKDAY]} @ "
             f"{REBALANCE_TIME_ET[0]:02d}:{REBALANCE_TIME_ET[1]:02d} ET")


# =============================================================================
# MAIN ENTRY POINT (STANDALONE MODE)
# =============================================================================

def main() -> None:
    """Main loop for standalone operation (outside unified engine)."""
    validate_configuration()

    trading = get_trading_client()
    data_client = get_data_client()

    # Validate API credentials
    account = trading.get_account()
    log.info(f"Account: {account.id} | Equity: ${float(account.equity):,.2f}")

    state = load_state(STATE_PATH)
    clear_stale_rebalance_flag(state, STATE_PATH)

    log.info("=" * 60)
    log.info("CROSS-ASSET TREND-FOLLOWING BOT")
    log.info("=" * 60)
    log.info(f"Universe: {LONG_UNIVERSE} + inverse: {INVERSE_UNIVERSE}")
    log.info(f"Rebalance: {['Mon','Tue','Wed','Thu','Fri'][REBALANCE_WEEKDAY]} "
             f"@ {REBALANCE_TIME_ET[0]:02d}:{REBALANCE_TIME_ET[1]:02d} ET")

    while True:
        try:
            # Kill switch
            ks_triggered, ks_reason = kill_switch.is_triggered()
            if ks_triggered:
                log.error(f"[XASSET] Kill switch: {ks_reason}")
                kill_switch.execute_emergency_shutdown(trading)
                break

            # Circuit breaker
            if circuit_breaker.is_halted():
                time.sleep(60)
                continue

            # Portfolio halt
            if state.portfolio_halted:
                log.warning(f"[XASSET] Portfolio halted: {state.portfolio_halt_reason}")
                time.sleep(300)
                continue

            # Rebalance window check
            already_done = state.last_rebalance_date_iso == now_et().date().isoformat()
            in_window = is_rebalance_window_dynamic(trading)

            if in_window and not already_done and not state.rebalance_in_progress:
                rebalance(trading, data_client, state)

            time.sleep(60)

        except KeyboardInterrupt:
            log.info("[XASSET] Keyboard interrupt")
            break
        except Exception as e:
            log.error(f"[XASSET] Main loop error: {e}")
            traceback.print_exc()
            time.sleep(60)

    save_state(STATE_PATH, state)
    log.info("[XASSET] Bot stopped")


if __name__ == "__main__":
    main()
