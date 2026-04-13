"""
Small Cap Momentum Executor
===========================

A trade execution script that takes a ticker and executes the entry
with automated exit management (tiered exits with trailing stop).

This script is designed to work alongside smallcap_scanner.py:
1. Run the scanner to find setups
2. When you see a good setup, run this executor with the ticker
3. The executor validates the setup, shows risk/reward, and executes

Features:
1. Quick setup validation
2. Position sizing based on risk
3. Automated tiered exits (TP1 at 1.0R, TP2 at 2.5R, Trail 34%)
4. Real-time position monitoring
5. Automatic stop management
6. Trade journaling

Usage:
    python smallcap_executor.py              # Interactive mode
    python smallcap_executor.py GLUE         # Direct ticker entry
    python smallcap_executor.py --monitor    # Monitor existing positions

Version: 1.0.0
"""

import os
import sys
import time
import json
import argparse
import requests
import pandas as pd
import numpy as np
import threading
import msvcrt  # Windows keyboard input (non-blocking)
from datetime import datetime, timedelta, time as dt_time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple
from enum import Enum
from zoneinfo import ZoneInfo
from pathlib import Path

# Alpaca imports
try:
    from alpaca.trading.client import TradingClient
    from alpaca.trading.requests import MarketOrderRequest, LimitOrderRequest
    from alpaca.trading.enums import OrderSide, TimeInForce, OrderType, OrderStatus
    from alpaca.data.historical import StockHistoricalDataClient
    from alpaca.data.requests import StockBarsRequest, StockLatestQuoteRequest
    from alpaca.data.timeframe import TimeFrame
    ALPACA_AVAILABLE = True
except ImportError:
    ALPACA_AVAILABLE = False
    print("Warning: Alpaca SDK not installed. Install with: pip install alpaca-py")

# ============================================================
# CONFIGURATION
# ============================================================

ET = ZoneInfo("America/New_York")

# --- Directory Paths (for organized folder structure) ---
ALGO_ROOT = Path(__file__).parent.parent  # Algo_Trading root
_output_root = Path(os.getenv("ALGO_OUTPUT_DIR", "")) if os.getenv("ALGO_OUTPUT_DIR") else ALGO_ROOT
DATA_DIR = _output_root / "data"
LOGS_DIR = _output_root / "logs"
DATA_DIR.mkdir(exist_ok=True)
LOGS_DIR.mkdir(exist_ok=True)

# API Keys
ALPACA_API_KEY = os.getenv("ALPACA_API_KEY", "")
ALPACA_SECRET_KEY = os.getenv("ALPACA_SECRET_KEY", "")
POLYGON_API_KEY = (os.getenv("POLYGON_API_KEY") or os.getenv("MASSIVE_API_KEY") or "").strip()
PAPER_TRADING = True  # Set to False for live trading (CAREFUL!)

# --- Risk Management (Percentage-Based on Available Cash) ---
RISK_PCT_PER_TRADE = 0.50       # Risk 0.5% of cash per trade (1R)
MAX_DAILY_LOSS_PCT = 1.50       # Stop trading if daily loss exceeds 1.5% of cash
INITIAL_SIZE_FACTOR = 0.50      # Start at 50% of full risk
FULL_SIZE_CUSHION_PCT = 0.50    # Need 0.5% profit (of starting cash) before full size
MAX_POSITIONS = 3               # Maximum concurrent positions
MAX_POSITION_PCT = 15.0         # Maximum 15% of cash in single position
MAX_CONSECUTIVE_LOSSES = 3      # After 3 losses, reduce to minimum size

# Fallback minimums (in case API fails to fetch cash)
MIN_RISK_DOLLARS = 50.00        # Minimum risk $ if cash fetch fails
MAX_RISK_DOLLARS = 1000.00      # Maximum risk $ cap for safety

# --- Execution Quality Gates ---
MAX_SPREAD_BPS = 80             # Maximum spread in basis points (0.8%)
MIN_BID_SIZE = 100              # Minimum bid size to ensure liquidity
MIN_ASK_SIZE = 100              # Minimum ask size to ensure liquidity
MIN_DOLLAR_VOLUME = 100_000     # Minimum $ volume in session
PANIC_EXIT_OFFSET = 0.02        # $ below bid for aggressive limit exits

# --- ATR-Based Stops (V7 BEST_MIX) ---
ATR_PERIOD = 14
ATR_STOP_MULT = 4.0             # Stop = 4x ATR (sanity cap, not primary)
MIN_STOP_DISTANCE_PCT = 1.5     # Minimum stop distance % (tighter for structure stops)
MAX_STOP_DISTANCE_PCT = 8.0     # Maximum stop distance % (sanity cap)

# --- Exit Parameters (V7 BEST_MIX: TIERED EXITS) ---
USE_TIERED_EXITS = True
TP1_R_MULTIPLE = 1.0            # First take profit at 1.0R
TP1_SIZE_PCT = 0.33             # Take 33% at TP1
TP2_R_MULTIPLE = 2.5            # Second take profit at 2.5R
TP2_SIZE_PCT = 0.33             # Take 33% at TP2
# Remaining 34% trails

BREAKEVEN_TRIGGER_R = 1.0       # Move to breakeven after TP1
USE_TRAILING_STOP = True
TRAIL_ACTIVATION_R = 2.5        # Activate trail after TP2
TRAIL_DISTANCE_PCT = 2.0        # Trail 2.0% behind high

# --- Timing ---
POSITION_CHECK_INTERVAL = 2     # Check positions every 2 seconds
EOD_CLOSE_TIME = dt_time(15, 55) # Close all by 3:55 PM ET
PREMARKET_START = dt_time(4, 0)  # Pre-market starts 4:00 AM ET
RTH_START = dt_time(9, 30)       # Regular trading hours start
RTH_END = dt_time(16, 0)         # Regular trading hours end
AFTERHOURS_END = dt_time(20, 0)  # After-hours end

# --- Persistence ---
STATE_FILE = str(DATA_DIR / "smallcap_executor_state.json")
TRADE_JOURNAL = str(DATA_DIR / "smallcap_executor_trades.jsonl")


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
    MANUAL = "MANUAL"


class MarketSession(Enum):
    """Current market session."""
    PREMARKET = "PREMARKET"
    RTH = "RTH"           # Regular Trading Hours
    AFTERHOURS = "AFTERHOURS"
    CLOSED = "CLOSED"


class EntryType(Enum):
    """Type of entry setup."""
    BREAKOUT = "BREAKOUT"       # Stop-limit above level
    PULLBACK = "PULLBACK"       # Limit at support
    MOMENTUM = "MOMENTUM"       # Market/aggressive limit at ask


class CatalystType(Enum):
    """Type of news catalyst."""
    FDA = "FDA"
    EARNINGS = "EARNINGS"
    CONTRACT = "CONTRACT"
    MERGER = "MERGER"
    OFFERING = "OFFERING"       # WARNING - dilution risk
    COMPLIANCE = "COMPLIANCE"
    LEGAL = "LEGAL"
    PRODUCT = "PRODUCT"
    UNKNOWN = "UNKNOWN"


def get_market_session() -> MarketSession:
    """Determine current market session."""
    now = datetime.now(ET)
    current_time = now.time()

    # Check day of week (0=Monday, 6=Sunday)
    if now.weekday() >= 5:  # Weekend
        return MarketSession.CLOSED

    if current_time < PREMARKET_START:
        return MarketSession.CLOSED
    elif current_time < RTH_START:
        return MarketSession.PREMARKET
    elif current_time < RTH_END:
        return MarketSession.RTH
    elif current_time < AFTERHOURS_END:
        return MarketSession.AFTERHOURS
    else:
        return MarketSession.CLOSED


@dataclass
class ActivePosition:
    """An active position being managed."""
    symbol: str
    entry_time: datetime
    entry_price: float
    stop_price: float
    risk_per_share: float

    # Quantities
    total_qty: int
    remaining_qty: int

    # Tiered exit state
    tp1_price: float
    tp2_price: float
    tp1_hit: bool = False
    tp2_hit: bool = False

    # State tracking
    highest_price: float = 0.0
    lowest_price_since_entry: float = float('inf')  # Track for stop trigger
    trail_active: bool = False
    trail_stop: float = 0.0
    be_active: bool = False

    # Structure-based stop (from scanner pattern detection)
    structure_stop: float = 0.0     # Pattern-based stop level
    atr_stop: float = 0.0           # ATR-based stop for reference

    # Order IDs
    entry_order_id: str = ""
    stop_order_id: str = ""

    # Session tracking
    entry_session: MarketSession = MarketSession.RTH

    # Exit state tracking
    exit_pending: bool = False  # Prevents repeated exit attempts


@dataclass
class DailyStats:
    """Daily trading statistics."""
    date: str
    trades: int = 0
    winners: int = 0
    losers: int = 0
    gross_pnl: float = 0.0
    consecutive_losses: int = 0
    size_factor: float = 0.25
    trading_halted: bool = False


# ============================================================
# HELPER FUNCTIONS
# ============================================================

def clear_screen():
    """Clear terminal screen."""
    os.system('cls' if os.name == 'nt' else 'clear')


def get_color(code: str) -> str:
    """Get ANSI color code."""
    colors = {
        "green": "\033[92m",
        "red": "\033[91m",
        "yellow": "\033[93m",
        "cyan": "\033[96m",
        "reset": "\033[0m",
        "bold": "\033[1m",
    }
    return colors.get(code, "")


def print_colored(text: str, color: str):
    """Print colored text."""
    print(f"{get_color(color)}{text}{get_color('reset')}")


def format_money(value: float) -> str:
    """Format as money."""
    return f"${value:,.2f}"


def format_pct(value: float) -> str:
    """Format as percentage."""
    return f"{value:.2f}%"


# ============================================================
# MARKET DATA
# ============================================================

class MarketDataFetcher:
    """Fetches market data from Polygon with Alpaca backup."""

    def __init__(self, alpaca_data_client=None):
        self.polygon_key = POLYGON_API_KEY
        self.alpaca_client = alpaca_data_client

    def get_alpaca_quote(self, symbol: str) -> Optional[dict]:
        """
        Get real-time quote from Alpaca (backup/comparison source).

        Alpaca provides real-time data through IEX feed (free) or SIP (paid).
        This serves as a backup when Polygon data seems delayed.
        """
        if not self.alpaca_client:
            return None
        try:
            from alpaca.data.requests import StockLatestQuoteRequest
            request = StockLatestQuoteRequest(symbol_or_symbols=symbol)
            quotes = self.alpaca_client.get_stock_latest_quote(request)
            if symbol in quotes:
                q = quotes[symbol]
                return {
                    "bid": float(q.bid_price) if q.bid_price else 0,
                    "ask": float(q.ask_price) if q.ask_price else 0,
                    "bid_size": int(q.bid_size) if q.bid_size else 0,
                    "ask_size": int(q.ask_size) if q.ask_size else 0,
                    "source": "alpaca"
                }
        except Exception:
            pass
        return None

    def get_latest_quote(self, symbol: str) -> Optional[dict]:
        """Get latest quote from Polygon."""
        url = f"https://api.polygon.io/v3/quotes/{symbol}"
        params = {"limit": 1, "apiKey": self.polygon_key}

        try:
            resp = requests.get(url, params=params, timeout=5)
            if resp.status_code != 200:
                return None
            results = resp.json().get("results", [])
            if not results:
                return None
            q = results[0]
            return {
                "bid": q.get("bid_price", 0),
                "ask": q.get("ask_price", 0),
                "bid_size": q.get("bid_size", 0),
                "ask_size": q.get("ask_size", 0),
            }
        except Exception:
            return None

    def get_snapshot(self, symbol: str) -> Optional[dict]:
        """
        Get snapshot with current price and volume.

        Uses Polygon for OHLCV data but cross-checks bid/ask with Alpaca
        for more accurate real-time quotes (especially for order decisions).
        """
        url = f"https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers/{symbol}"
        params = {"apiKey": self.polygon_key}

        try:
            resp = requests.get(url, params=params, timeout=5)
            if resp.status_code != 200:
                print_colored(f"  Snapshot API returned {resp.status_code}", "yellow")
                return self._get_fallback_snapshot(symbol)

            data = resp.json()
            ticker = data.get("ticker", {})

            # Check if we got valid data
            if not ticker:
                print_colored(f"  No ticker data in snapshot response", "yellow")
                return self._get_fallback_snapshot(symbol)

            day = ticker.get("day", {})
            prev = ticker.get("prevDay", {})
            last_trade = ticker.get("lastTrade", {})
            last_quote = ticker.get("lastQuote", {})

            # Use prev day close if current day has no data (market closed/premarket)
            price = day.get("c", 0) or prev.get("c", 0)
            if not price:
                print_colored(f"  No price data available", "yellow")
                return self._get_fallback_snapshot(symbol)

            # Extract Polygon quote data
            polygon_bid = last_quote.get("p", 0) or last_quote.get("P", 0) or price * 0.999
            polygon_ask = last_quote.get("P", 0) or last_quote.get("p", 0) or price * 1.001
            polygon_bid_size = last_quote.get("s", 0) or last_quote.get("S", 0) or 100
            polygon_ask_size = last_quote.get("S", 0) or last_quote.get("s", 0) or 100

            # Try to get Alpaca quote for more accurate bid/ask (real-time backup)
            alpaca_quote = self.get_alpaca_quote(symbol)
            if alpaca_quote and alpaca_quote.get("bid", 0) > 0:
                # Use Alpaca quote for bid/ask (more reliable for order execution)
                bid = alpaca_quote["bid"]
                ask = alpaca_quote["ask"]
                bid_size = alpaca_quote["bid_size"]
                ask_size = alpaca_quote["ask_size"]
            else:
                # Fall back to Polygon quote
                bid = polygon_bid
                ask = polygon_ask
                bid_size = polygon_bid_size
                ask_size = polygon_ask_size

            return {
                "price": price,
                "open": day.get("o", 0) or prev.get("o", 0),
                "high": day.get("h", 0) or prev.get("h", 0),
                "low": day.get("l", 0) or prev.get("l", 0),
                "volume": day.get("v", 0) or prev.get("v", 0),
                "prev_close": prev.get("c", 0),
                "last_trade_price": last_trade.get("p", 0) or price,
                "last_trade_size": last_trade.get("s", 0),
                "bid": bid,
                "ask": ask,
                "bid_size": bid_size,
                "ask_size": ask_size,
            }
        except Exception as e:
            print_colored(f"  Snapshot error: {e}", "yellow")
            return self._get_fallback_snapshot(symbol)

    def _get_fallback_snapshot(self, symbol: str) -> Optional[dict]:
        """Fallback: Get previous day's data from daily bars endpoint."""
        print_colored(f"  Trying fallback (daily bars)...", "yellow")
        try:
            from datetime import timedelta
            end_date = datetime.now(ET).date()
            start_date = end_date - timedelta(days=7)  # Look back a week
            url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/day/{start_date}/{end_date}"
            params = {"adjusted": "true", "sort": "desc", "limit": 1, "apiKey": self.polygon_key}

            resp = requests.get(url, params=params, timeout=5)
            if resp.status_code != 200:
                print_colored(f"  Fallback API returned {resp.status_code}", "red")
                return None

            results = resp.json().get("results", [])
            if not results:
                print_colored(f"  No historical data found for {symbol} - ticker may be invalid", "red")
                return None

            bar = results[0]
            price = bar.get("c", 0)
            print_colored(f"  Using fallback data (last close: ${price:.2f})", "green")

            return {
                "price": price,
                "open": bar.get("o", 0),
                "high": bar.get("h", 0),
                "low": bar.get("l", 0),
                "volume": bar.get("v", 0),
                "prev_close": price,
                "last_trade_price": price,
                "last_trade_size": 0,
                "bid": price * 0.999,
                "ask": price * 1.001,
                "bid_size": 100,
                "ask_size": 100,
            }
        except Exception as e:
            print_colored(f"  Fallback error: {e}", "red")
            return None

    def get_recent_bars(self, symbol: str, since_time: datetime, limit: int = 30) -> Optional[pd.DataFrame]:
        """
        Get 1-minute bars since a specific time.

        CRITICAL: Used for trigger detection - only consider price action AFTER entry.
        """
        today = datetime.now(ET).date()
        url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/minute/{today}/{today}"
        params = {"adjusted": "true", "sort": "asc", "limit": limit, "apiKey": self.polygon_key}

        try:
            resp = requests.get(url, params=params, timeout=10)
            if resp.status_code != 200:
                return None
            results = resp.json().get("results", [])
            if not results:
                return None

            df = pd.DataFrame(results)
            df.columns = [c.lower() for c in df.columns]
            df = df.rename(columns={"o": "open", "h": "high", "l": "low", "c": "close", "v": "volume", "t": "timestamp"})

            # Convert timestamp to datetime
            df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
            df["timestamp"] = df["timestamp"].dt.tz_convert(ET)

            # Filter to only bars since entry time
            since_time_aware = since_time if since_time.tzinfo else since_time.replace(tzinfo=ET)
            df = df[df["timestamp"] >= since_time_aware]

            return df if len(df) > 0 else None
        except Exception:
            return None

    def get_intraday_bars(self, symbol: str, limit: int = 50) -> Optional[pd.DataFrame]:
        """Get 1-minute intraday bars."""
        today = datetime.now(ET).date()
        url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/minute/{today}/{today}"
        params = {"adjusted": "true", "sort": "asc", "limit": limit, "apiKey": self.polygon_key}

        try:
            resp = requests.get(url, params=params, timeout=10)
            if resp.status_code != 200:
                return None
            results = resp.json().get("results", [])
            if not results:
                return None

            df = pd.DataFrame(results)
            df.columns = [c.lower() for c in df.columns]
            df = df.rename(columns={"o": "open", "h": "high", "l": "low", "c": "close", "v": "volume"})
            return df
        except Exception:
            return None

    def calculate_atr(self, df: pd.DataFrame, period: int = 14) -> float:
        """Calculate ATR from dataframe."""
        if len(df) < period + 1:
            return (df["high"] - df["low"]).mean()

        high = df["high"]
        low = df["low"]
        close = df["close"]

        tr1 = high - low
        tr2 = abs(high - close.shift(1))
        tr3 = abs(low - close.shift(1))

        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        atr = tr.rolling(window=period).mean()

        return atr.iloc[-1] if not pd.isna(atr.iloc[-1]) else (high - low).mean()


# ============================================================
# TRADE EXECUTOR
# ============================================================

class TradeExecutor:
    """Executes trades and manages positions."""

    def __init__(self):
        self.positions: Dict[str, ActivePosition] = {}
        self.daily_stats = DailyStats(date=datetime.now(ET).strftime("%Y-%m-%d"))

        # Initialize Alpaca clients
        if ALPACA_AVAILABLE and ALPACA_API_KEY:
            self.trading_client = TradingClient(
                ALPACA_API_KEY,
                ALPACA_SECRET_KEY,
                paper=PAPER_TRADING
            )
            self.data_client = StockHistoricalDataClient(ALPACA_API_KEY, ALPACA_SECRET_KEY)
        else:
            self.trading_client = None
            self.data_client = None

        # Initialize market data fetcher with Alpaca backup
        self.market_data = MarketDataFetcher(alpaca_data_client=self.data_client)

        # Cache available cash at startup (refresh periodically)
        self._cached_cash: float = 0.0
        self._cash_fetched_at: Optional[datetime] = None
        self._starting_cash: float = 0.0  # For daily P&L tracking
        self._refresh_cash()

        # Load state
        self._load_state()

    def _load_state(self):
        """Load persisted state."""
        if Path(STATE_FILE).exists():
            try:
                with open(STATE_FILE, "r") as f:
                    state = json.load(f)
                    # Load daily stats if same day
                    if state.get("date") == datetime.now(ET).strftime("%Y-%m-%d"):
                        self.daily_stats.trades = state.get("trades", 0)
                        self.daily_stats.winners = state.get("winners", 0)
                        self.daily_stats.losers = state.get("losers", 0)
                        self.daily_stats.gross_pnl = state.get("gross_pnl", 0.0)
                        self.daily_stats.size_factor = state.get("size_factor", INITIAL_SIZE_FACTOR)
            except Exception:
                pass

    def _save_state(self):
        """Save state to file."""
        state = {
            "date": self.daily_stats.date,
            "trades": self.daily_stats.trades,
            "winners": self.daily_stats.winners,
            "losers": self.daily_stats.losers,
            "gross_pnl": self.daily_stats.gross_pnl,
            "size_factor": self.daily_stats.size_factor,
        }
        try:
            with open(STATE_FILE, "w") as f:
                json.dump(state, f)
        except Exception:
            pass

    def _log_trade(self, trade_data: dict):
        """Log trade to journal."""
        try:
            with open(TRADE_JOURNAL, "a") as f:
                f.write(json.dumps(trade_data) + "\n")
        except Exception:
            pass

    def _refresh_cash(self) -> float:
        """
        Fetch available cash from Alpaca (not buying power to avoid margin).

        Returns:
            Available cash balance
        """
        if not self.trading_client:
            print_colored("  WARNING: No trading client - using fallback cash estimate", "yellow")
            self._cached_cash = 10000.0  # Fallback
            return self._cached_cash

        try:
            account = self.trading_client.get_account()
            # Use 'cash' not 'buying_power' to avoid margin
            self._cached_cash = float(account.cash)
            self._cash_fetched_at = datetime.now(ET)

            # Set starting cash for daily P&L tracking (only once per day)
            if self._starting_cash == 0:
                self._starting_cash = self._cached_cash

            print_colored(f"  Account cash: ${self._cached_cash:,.2f}", "cyan")
            return self._cached_cash

        except Exception as e:
            print_colored(f"  WARNING: Failed to fetch cash: {e}", "yellow")
            if self._cached_cash > 0:
                return self._cached_cash
            self._cached_cash = 10000.0  # Fallback
            return self._cached_cash

    def _get_available_cash(self) -> float:
        """
        Get available cash, refreshing if stale (>5 minutes old).

        Returns:
            Available cash balance
        """
        # Refresh if never fetched or older than 5 minutes
        if (self._cash_fetched_at is None or
            (datetime.now(ET) - self._cash_fetched_at).total_seconds() > 300):
            return self._refresh_cash()
        return self._cached_cash

    def _calculate_risk_dollars(self) -> Tuple[float, float, float]:
        """
        Calculate risk dollars and limits based on available cash.

        Returns:
            Tuple of (risk_dollars, max_position_value, max_daily_loss)
        """
        cash = self._get_available_cash()

        # Calculate percentage-based values
        risk_dollars = cash * (RISK_PCT_PER_TRADE / 100) * self.daily_stats.size_factor
        max_position_value = cash * (MAX_POSITION_PCT / 100)
        max_daily_loss = cash * (MAX_DAILY_LOSS_PCT / 100)
        full_size_cushion = cash * (FULL_SIZE_CUSHION_PCT / 100)

        # Apply safety bounds
        risk_dollars = max(MIN_RISK_DOLLARS, min(risk_dollars, MAX_RISK_DOLLARS))

        return risk_dollars, max_position_value, max_daily_loss, full_size_cushion

    def validate_setup(self, symbol: str, scanner_setup: dict = None) -> Optional[dict]:
        """
        Validate and analyze a ticker for entry.

        ENHANCED: Now includes execution quality gates and structure-based stops.

        Args:
            symbol: Ticker symbol
            scanner_setup: Optional setup data from smallcap_scanner with structure-based stop
        """
        # Normalize symbol to uppercase (Polygon API requires uppercase)
        symbol = symbol.upper().strip()
        print(f"\nValidating {symbol}...")

        # === ENTRY GATES (check before anything else) ===
        gates_passed, gate_reason = self._check_entry_gates(symbol)
        if not gates_passed:
            print_colored(f"  BLOCKED: {gate_reason}", "red")
            return None

        # Get snapshot
        snapshot = self.market_data.get_snapshot(symbol)
        if not snapshot:
            print_colored("  ERROR: Could not fetch snapshot data", "red")
            return None

        # Get quote
        quote = self.market_data.get_latest_quote(symbol)
        if not quote:
            print_colored("  WARNING: Could not fetch quote data", "yellow")
            quote = {"bid": snapshot["price"] * 0.999, "ask": snapshot["price"] * 1.001,
                    "bid_size": 100, "ask_size": 100}

        # === EXECUTION QUALITY GATES ===
        spread = quote["ask"] - quote["bid"]
        spread_bps = (spread / ((quote["bid"] + quote["ask"]) / 2)) * 10000 if quote["bid"] > 0 else 9999

        # GATE 1: Spread check
        spread_pass = spread_bps <= MAX_SPREAD_BPS
        if not spread_pass:
            print_colored(f"  FAIL: Spread {spread_bps:.0f} bps > max {MAX_SPREAD_BPS} bps", "red")

        # GATE 2: Bid/ask size check
        bid_size = quote.get("bid_size", 0)
        ask_size = quote.get("ask_size", 0)
        liquidity_pass = bid_size >= MIN_BID_SIZE and ask_size >= MIN_ASK_SIZE
        if not liquidity_pass:
            print_colored(f"  FAIL: Thin liquidity (bid_size={bid_size}, ask_size={ask_size})", "red")

        # GATE 3: Dollar volume check
        dollar_volume = snapshot.get("volume", 0) * snapshot.get("price", 0)
        volume_pass = dollar_volume >= MIN_DOLLAR_VOLUME
        if not volume_pass:
            print_colored(f"  FAIL: Dollar volume ${dollar_volume:,.0f} < ${MIN_DOLLAR_VOLUME:,.0f}", "red")

        # Combined gate result
        execution_gates_pass = spread_pass and liquidity_pass and volume_pass

        # Get intraday bars for ATR
        bars = self.market_data.get_intraday_bars(symbol)
        if bars is None or len(bars) < 10:
            print_colored("  WARNING: Limited bar data, using estimate for ATR", "yellow")
            atr = snapshot["price"] * 0.02  # Estimate 2% ATR
        else:
            atr = self.market_data.calculate_atr(bars)

        # Calculate levels
        price = snapshot["price"]
        prev_close = snapshot["prev_close"]
        gap_pct = ((price - prev_close) / prev_close * 100) if prev_close > 0 else 0

        # Entry at ask
        entry_price = quote["ask"]

        # === STRUCTURE-BASED STOP (primary) vs ATR STOP (cap) ===
        if scanner_setup and scanner_setup.get("structure_stop"):
            # Use scanner's structure-based stop (pattern invalidation)
            structure_stop = scanner_setup["structure_stop"]
            structure_risk_pct = (entry_price - structure_stop) / entry_price * 100

            # Sanity check: if structure stop is too wide, use ATR cap
            if structure_risk_pct > MAX_STOP_DISTANCE_PCT:
                atr_stop = entry_price - (atr * ATR_STOP_MULT)
                stop_price = atr_stop
                stop_pct = (entry_price - atr_stop) / entry_price * 100
                print_colored(f"  NOTE: Structure stop too wide ({structure_risk_pct:.1f}%), using ATR cap", "yellow")
            else:
                stop_price = structure_stop
                stop_pct = structure_risk_pct
        else:
            # Fallback to ATR-based stop
            atr_stop_distance = atr * ATR_STOP_MULT
            min_stop_distance = entry_price * (MIN_STOP_DISTANCE_PCT / 100)
            stop_distance = max(atr_stop_distance, min_stop_distance)
            stop_price = entry_price - stop_distance
            stop_pct = (stop_distance / entry_price) * 100

        # Cap max stop distance
        max_stop_distance = entry_price * (MAX_STOP_DISTANCE_PCT / 100)
        if entry_price - stop_price > max_stop_distance:
            stop_price = entry_price - max_stop_distance
            stop_pct = MAX_STOP_DISTANCE_PCT

        risk_per_share = entry_price - stop_price

        # Tiered targets
        tp1_price = entry_price + (risk_per_share * TP1_R_MULTIPLE)
        tp2_price = entry_price + (risk_per_share * TP2_R_MULTIPLE)

        # Position sizing (percentage-based on available cash)
        risk_dollars, max_position_value, max_daily_loss, _ = self._calculate_risk_dollars()
        qty = int(risk_dollars / risk_per_share)
        position_value = qty * entry_price

        # Check max position value (percentage of cash)
        if position_value > max_position_value:
            qty = int(max_position_value / entry_price)
            position_value = qty * entry_price

        if qty <= 0:
            print_colored("  ERROR: Position size too small", "red")
            return None

        # === KEY LEVELS (from scanner or calculate) ===
        if scanner_setup:
            nearest_key_level = scanner_setup.get("nearest_key_level", 0)
            key_level_state = scanner_setup.get("key_level_state", "UNKNOWN")
            hod = scanner_setup.get("hod", snapshot.get("high", 0))
            pmh = scanner_setup.get("pmh", 0)
            catalyst_type = scanner_setup.get("catalyst_type", "UNKNOWN")
            news_sentiment = scanner_setup.get("news_sentiment", "NEUTRAL")
            trigger_desc = scanner_setup.get("trigger_desc", "")
            entry_type = scanner_setup.get("entry_type", EntryType.MOMENTUM.value)
        else:
            # Calculate key levels ourselves
            nearest_key_level = self._get_nearest_key_level(price)
            key_level_state = "UNKNOWN"
            hod = snapshot.get("high", 0)
            pmh = 0
            catalyst_type = "UNKNOWN"
            news_sentiment = "NEUTRAL"
            trigger_desc = "Manual entry"
            entry_type = EntryType.MOMENTUM.value

        # Current session
        session = get_market_session()

        return {
            "symbol": symbol,
            "price": price,
            "bid": quote["bid"],
            "ask": quote["ask"],
            "bid_size": bid_size,
            "ask_size": ask_size,
            "spread_bps": spread_bps,
            "gap_pct": gap_pct,
            "entry_price": entry_price,
            "stop_price": stop_price,
            "stop_pct": stop_pct,
            "tp1_price": tp1_price,
            "tp2_price": tp2_price,
            "risk_per_share": risk_per_share,
            "atr": atr,
            "qty": qty,
            "position_value": position_value,
            "risk_dollars": qty * risk_per_share,
            # Execution quality gates
            "execution_gates_pass": execution_gates_pass,
            "spread_pass": spread_pass,
            "liquidity_pass": liquidity_pass,
            "volume_pass": volume_pass,
            "dollar_volume": dollar_volume,
            # Key levels
            "nearest_key_level": nearest_key_level,
            "key_level_state": key_level_state,
            "hod": hod,
            "pmh": pmh,
            # Catalyst info
            "catalyst_type": catalyst_type,
            "news_sentiment": news_sentiment,
            "trigger_desc": trigger_desc,
            "entry_type": entry_type,
            # Session
            "session": session.value,
        }

    def _check_entry_gates(self, symbol: str) -> Tuple[bool, str]:
        """
        Check all entry gates before validating a setup.

        Returns (passed, reason) tuple.
        """
        # Gate 1: Trading halted?
        if self.daily_stats.trading_halted:
            return False, "Trading halted - daily loss limit reached"

        # Gate 2: Max positions?
        if len(self.positions) >= MAX_POSITIONS:
            return False, f"Max positions ({MAX_POSITIONS}) reached"

        # Gate 3: Already in this symbol?
        if symbol in self.positions:
            return False, f"Already have position in {symbol}"

        # Gate 4: Consecutive losses check (3 strikes)
        if self.daily_stats.consecutive_losses >= MAX_CONSECUTIVE_LOSSES:
            # Not blocking, but size is reduced
            print_colored(f"  WARNING: 3 strikes active - size reduced to {self.daily_stats.size_factor:.0%}", "yellow")

        return True, ""

    @staticmethod
    def _get_nearest_key_level(price: float) -> float:
        """Calculate nearest psychological key level ($0.50 or $1.00 intervals)."""
        if price < 10:
            interval = 0.50
        else:
            interval = 1.00

        level_below = (price // interval) * interval
        level_above = level_below + interval

        if price - level_below <= level_above - price:
            return level_below
        return level_above

    def display_setup(self, setup: dict):
        """Display setup details for confirmation with enhanced UI."""
        print()
        print("=" * 70)

        # Header with session badge
        session_badge = f"[{setup.get('session', 'RTH')}]"
        print(f" {setup['symbol']} @ ${setup['price']:.2f} | Gap: +{setup['gap_pct']:.1f}% {session_badge}")
        print("=" * 70)

        # === EXECUTION QUALITY GATES ===
        print()
        print(" EXECUTION GATES:")
        gates_pass = setup.get("execution_gates_pass", True)

        # Spread gate
        spread_status = get_color("green") + "PASS" if setup.get("spread_pass", True) else get_color("red") + "FAIL"
        print(f"   Spread:    {setup['spread_bps']:.0f} bps [{spread_status}{get_color('reset')}] (max: {MAX_SPREAD_BPS})")

        # Liquidity gate
        liq_status = get_color("green") + "PASS" if setup.get("liquidity_pass", True) else get_color("red") + "FAIL"
        print(f"   Liquidity: bid {setup['bid_size']} x ask {setup['ask_size']} [{liq_status}{get_color('reset')}]")

        # Volume gate
        vol_status = get_color("green") + "PASS" if setup.get("volume_pass", True) else get_color("red") + "FAIL"
        dollar_vol = setup.get("dollar_volume", 0)
        if dollar_vol >= 1_000_000:
            vol_str = f"${dollar_vol/1_000_000:.1f}M"
        else:
            vol_str = f"${dollar_vol/1_000:.0f}K"
        print(f"   $Volume:   {vol_str} [{vol_status}{get_color('reset')}]")

        if not gates_pass:
            print()
            print_colored("   ⚠️  EXECUTION GATES FAILED - Trade at your own risk!", "red")

        # === CATALYST & NEWS ===
        print()
        print(" CATALYST:")
        catalyst_type = setup.get("catalyst_type", "UNKNOWN")
        news_sentiment = setup.get("news_sentiment", "NEUTRAL")

        # Color-code catalyst type
        catalyst_colors = {
            "FDA": "cyan",
            "EARNINGS": "cyan",
            "CONTRACT": "green",
            "MERGER": "cyan",
            "OFFERING": "red",      # WARNING
            "COMPLIANCE": "yellow",
            "LEGAL": "red",
            "PRODUCT": "green",
        }
        cat_color = catalyst_colors.get(catalyst_type, "reset")
        print(f"   Type:      {get_color(cat_color)}{catalyst_type}{get_color('reset')}")

        # Sentiment with color
        sent_colors = {"GREAT": "cyan", "GOOD": "green", "NEUTRAL": "reset", "BAD": "red"}
        sent_color = sent_colors.get(news_sentiment, "reset")
        print(f"   Sentiment: {get_color(sent_color)}{news_sentiment}{get_color('reset')}")

        # Offering warning
        if catalyst_type == "OFFERING":
            print_colored("   ⚠️  DILUTION RISK - Offering detected!", "red")

        # === KEY LEVELS ===
        print()
        print(" KEY LEVELS:")
        key_level = setup.get("nearest_key_level", 0)
        key_state = setup.get("key_level_state", "UNKNOWN")
        hod = setup.get("hod", 0)
        pmh = setup.get("pmh", 0)

        if key_level > 0:
            dist_to_level = setup['price'] - key_level
            dist_pct = (dist_to_level / key_level) * 100 if key_level > 0 else 0
            level_color = "green" if dist_to_level > 0 else "red"
            print(f"   Key Level: ${key_level:.2f} ({get_color(level_color)}{dist_pct:+.1f}%{get_color('reset')}) [{key_state}]")

        if hod > 0:
            dist_to_hod = (hod - setup['price']) / hod * 100
            print(f"   HOD:       ${hod:.2f} ({dist_to_hod:.1f}% away)")
        if pmh > 0:
            print(f"   PMH:       ${pmh:.2f}")

        # === ENTRY TYPE & TRIGGER ===
        print()
        print(" ENTRY:")
        entry_type = setup.get("entry_type", "MOMENTUM")
        trigger_desc = setup.get("trigger_desc", "")
        print(f"   Type:      {entry_type}")
        if trigger_desc:
            print(f"   Trigger:   {trigger_desc}")
        print(f"   Bid/Ask:   ${setup['bid']:.2f} / ${setup['ask']:.2f}")

        # === LEVELS ===
        print()
        print("-" * 70)
        print(f" Entry:  ${setup['entry_price']:.2f} (limit at ask)")
        print(f" Stop:   ${setup['stop_price']:.2f} (-{setup['stop_pct']:.1f}%)")

        tp1_pct = ((setup['tp1_price'] - setup['entry_price']) / setup['entry_price']) * 100
        tp2_pct = ((setup['tp2_price'] - setup['entry_price']) / setup['entry_price']) * 100

        print(f" TP1:    ${setup['tp1_price']:.2f} (+{tp1_pct:.1f}%, 1.0R) --> Sell 33%")
        print(f" TP2:    ${setup['tp2_price']:.2f} (+{tp2_pct:.1f}%, 2.5R) --> Sell 33%")
        print(f" Trail:  {TRAIL_DISTANCE_PCT:.1f}% behind high --> Remaining 34%")
        print("-" * 70)

        # === POSITION SIZING ===
        print()
        cash = self._get_available_cash()
        position_pct = (setup['position_value'] / cash * 100) if cash > 0 else 0
        risk_pct = (setup['risk_dollars'] / cash * 100) if cash > 0 else 0
        print(f" Shares:     {setup['qty']}")
        print(f" Position:   {format_money(setup['position_value'])} ({position_pct:.1f}% of cash)")
        print(f" Risk:       {format_money(setup['risk_dollars'])} ({risk_pct:.2f}% of cash, "
              f"${setup['risk_per_share']:.2f}/share)")

        # === DAILY STATS ===
        print()
        size_color = "green" if self.daily_stats.size_factor >= 1.0 else "yellow"
        consec_str = f" | {self.daily_stats.consecutive_losses} consecutive L" if self.daily_stats.consecutive_losses > 0 else ""
        print(f" Cash: {format_money(cash)} | "
              f"Today: {self.daily_stats.trades} trades | "
              f"W:{self.daily_stats.winners} L:{self.daily_stats.losers} | "
              f"P&L: {format_money(self.daily_stats.gross_pnl)}")
        print(f" Size: {get_color(size_color)}{self.daily_stats.size_factor:.0%}{get_color('reset')}{consec_str}")

        print("=" * 70)

    def execute_entry(self, setup: dict) -> Optional[ActivePosition]:
        """Execute the entry order."""
        if not self.trading_client:
            print_colored("ERROR: Alpaca client not available", "red")
            return None

        symbol = setup["symbol"]
        qty = setup["qty"]

        # Re-fetch current ask price right before order (price may have moved since validation)
        fresh_quote = self.market_data.get_latest_quote(symbol)
        if fresh_quote and fresh_quote.get("ask", 0) > 0:
            entry_price = fresh_quote["ask"]
            # Add small buffer for fast movers (0.5% above ask)
            entry_price = entry_price * 1.005
        else:
            entry_price = setup["entry_price"]

        print(f"\nSubmitting order: BUY {qty} {symbol} @ ${entry_price:.2f} (fresh quote)...")

        try:
            order = LimitOrderRequest(
                symbol=symbol,
                qty=qty,
                side=OrderSide.BUY,
                type=OrderType.LIMIT,
                time_in_force=TimeInForce.DAY,
                limit_price=round(entry_price, 2)
            )

            result = self.trading_client.submit_order(order)
            order_id = result.id

            print_colored(f"  Order submitted: {order_id}", "green")

            # Wait for fill (up to 15 seconds with partial fill handling)
            print("  Waiting for fill...", end="", flush=True)
            for _ in range(30):  # 30 iterations * 0.5s = 15 seconds max
                time.sleep(0.5)
                order_status = self.trading_client.get_order_by_id(order_id)

                if order_status.status == OrderStatus.FILLED:
                    filled_price = float(order_status.filled_avg_price)
                    filled_qty = int(order_status.filled_qty)
                    print()
                    print_colored(f"  FILLED: {filled_qty} shares @ ${filled_price:.2f}", "green")

                    # Create position
                    pos = self._create_position_from_fill(
                        symbol, filled_qty, filled_price, setup, order_id
                    )
                    return pos

                elif order_status.status == OrderStatus.PARTIALLY_FILLED:
                    # Check how much is filled - may complete soon
                    filled_qty = int(order_status.filled_qty)
                    filled_price = float(order_status.filled_avg_price) if order_status.filled_avg_price else entry_price
                    print(f"[{filled_qty}/{qty}]", end="", flush=True)
                    # Continue waiting - might fill completely

                elif order_status.status in [OrderStatus.CANCELED, OrderStatus.REJECTED]:
                    print()
                    print_colored(f"  Order {order_status.status.value}", "red")
                    return None

                else:
                    print(".", end="", flush=True)

            # Timeout reached - CRITICAL: Check for partial fills before cancelling
            print()
            order_status = self.trading_client.get_order_by_id(order_id)
            filled_qty = int(order_status.filled_qty) if order_status.filled_qty else 0

            if filled_qty > 0:
                # PARTIAL FILL EXISTS - we have shares!
                filled_price = float(order_status.filled_avg_price)
                print_colored(f"  PARTIAL FILL: {filled_qty}/{qty} shares @ ${filled_price:.2f}", "yellow")

                # Cancel remaining unfilled portion
                try:
                    self.trading_client.cancel_order_by_id(order_id)
                    print_colored(f"  Cancelled remaining {qty - filled_qty} unfilled shares", "yellow")
                except Exception as e:
                    # Order might already be fully processed
                    print_colored(f"  Note: Cancel returned: {e}", "yellow")

                # Create position with partial fill
                pos = self._create_position_from_fill(
                    symbol, filled_qty, filled_price, setup, order_id
                )
                return pos
            else:
                # No fill at all - safe to cancel
                print_colored("  Order timed out with no fill - cancelling", "yellow")
                try:
                    self.trading_client.cancel_order_by_id(order_id)
                except Exception:
                    pass
                return None

        except Exception as e:
            print_colored(f"  ERROR: {e}", "red")
            return None

    def _create_position_from_fill(self, symbol: str, filled_qty: int, filled_price: float,
                                    setup: dict, order_id: str) -> ActivePosition:
        """
        Create an ActivePosition from a fill (full or partial).

        This helper consolidates position creation logic to ensure consistent
        handling for both full fills and partial fills.
        """
        # Recalculate risk per share based on actual fill price
        risk_per_share = filled_price - setup["stop_price"]

        # Recalculate TP levels based on actual fill price (not limit price)
        tp1_price = filled_price + (risk_per_share * TP1_R_MULTIPLE)
        tp2_price = filled_price + (risk_per_share * TP2_R_MULTIPLE)

        pos = ActivePosition(
            symbol=symbol,
            entry_time=datetime.now(ET),
            entry_price=filled_price,
            stop_price=setup["stop_price"],
            risk_per_share=risk_per_share,
            total_qty=filled_qty,
            remaining_qty=filled_qty,
            tp1_price=tp1_price,
            tp2_price=tp2_price,
            highest_price=filled_price,
            entry_order_id=order_id
        )

        self.positions[symbol] = pos

        # Log the entry
        self._log_trade({
            "type": "ENTRY",
            "symbol": symbol,
            "timestamp": datetime.now(ET).isoformat(),
            "qty": filled_qty,
            "price": filled_price,
            "stop": setup["stop_price"],
            "tp1": tp1_price,
            "tp2": tp2_price,
            "partial_fill": filled_qty < setup["qty"],  # Track if this was a partial
            "requested_qty": setup["qty"],
        })

        return pos

    def check_positions(self) -> bool:
        """
        Check and manage all open positions. Returns True if positions exist.

        CRITICAL FIX: Uses last trade price for trigger decisions, NOT day high/low.
        Day high/low would cause false triggers from earlier price action.
        """
        if not self.positions:
            return False

        for symbol, pos in list(self.positions.items()):
            # CRITICAL: Always sync with Alpaca to detect closed positions
            actual_qty = self._sync_position_qty(pos)

            if actual_qty <= 0:
                # Position is gone from Alpaca - determine if it was OUR exit or external
                # Try to get final fill info from recent orders
                exit_price = self._get_last_fill_price(symbol, pos.entry_price)
                pnl = (exit_price - pos.entry_price) * pos.remaining_qty  # Use remaining_qty for accurate P&L

                # Determine exit reason based on whether we had a pending exit
                if pos.exit_pending:
                    # Our exit order filled - this is expected behavior
                    exit_reason = "OUR_EXIT_FILLED"
                    exit_note = "Exit order filled successfully"
                    print_colored(f"  [{symbol}] Exit order filled - position closed", "green")
                else:
                    # No pending exit = closed externally (Alpaca UI, mobile app, etc.)
                    exit_reason = "EXTERNAL"
                    exit_note = "Position closed externally (Alpaca UI/API)"
                    print_colored(f"  [{symbol}] Position closed externally - removing from tracking", "cyan")

                self.daily_stats.trades += 1
                if pnl >= 0:
                    self.daily_stats.winners += 1
                    self.daily_stats.consecutive_losses = 0
                else:
                    self.daily_stats.losers += 1
                    self.daily_stats.consecutive_losses += 1
                self.daily_stats.gross_pnl += pnl

                r_mult = (exit_price - pos.entry_price) / pos.risk_per_share if pos.risk_per_share > 0 else 0
                color = "green" if pnl >= 0 else "red"
                print_colored(f"  [{symbol}] Closed @ ~${exit_price:.2f} | P&L: {format_money(pnl)} ({r_mult:.2f}R)", color)

                self._log_trade({
                    "type": "EXIT",
                    "reason": exit_reason,
                    "symbol": symbol,
                    "timestamp": datetime.now(ET).isoformat(),
                    "qty": pos.remaining_qty,
                    "price": exit_price,
                    "pnl": pnl,
                    "r_mult": r_mult,
                    "note": exit_note
                })

                del self.positions[symbol]
                self._save_state()
                continue

            # Skip further processing if exit is pending (order submitted, waiting for fill)
            if pos.exit_pending:
                continue

            # Get current price data
            snapshot = self.market_data.get_snapshot(symbol)
            if not snapshot:
                continue

            # CRITICAL: Use last trade price, NOT day high/low
            current_price = snapshot.get("last_trade_price") or snapshot["price"]
            current_bid = snapshot.get("bid", current_price * 0.999)
            current_ask = snapshot.get("ask", current_price * 1.001)

            # For more accurate trigger detection, get bars since entry
            # This ensures we only react to price action AFTER we entered
            bars_since_entry = self.market_data.get_recent_bars(symbol, pos.entry_time)

            if bars_since_entry is not None and len(bars_since_entry) > 0:
                # Use high/low from bars SINCE entry for trigger detection
                high_since_entry = bars_since_entry["high"].max()
                low_since_entry = bars_since_entry["low"].min()
            else:
                # Fallback to current price if no bars available
                high_since_entry = current_price
                low_since_entry = current_price

            # Update tracking
            if current_price > pos.highest_price:
                pos.highest_price = current_price
            if current_price < pos.lowest_price_since_entry:
                pos.lowest_price_since_entry = current_price

            # Check stop loss - FIXED: uses low since entry, not day low
            if low_since_entry <= pos.stop_price:
                exit_price = pos.stop_price
                reason = ExitReason.BREAKEVEN if pos.be_active else ExitReason.STOP_LOSS
                self._exit_position(pos, exit_price, reason, current_bid)
                continue

            # Check TP1 - FIXED: uses high since entry, not day high
            if not pos.tp1_hit and high_since_entry >= pos.tp1_price:
                self._take_partial(pos, pos.tp1_price, TP1_SIZE_PCT, "TP1", current_bid)
                pos.tp1_hit = True

                # Move stop to breakeven
                pos.be_active = True
                new_stop = pos.entry_price + 0.01
                if new_stop > pos.stop_price:
                    pos.stop_price = new_stop
                    print_colored(f"  [{symbol}] Stop moved to breakeven: ${pos.stop_price:.2f}", "cyan")

            # Check TP2 - FIXED: uses high since entry, not day high
            if not pos.tp2_hit and high_since_entry >= pos.tp2_price:
                self._take_partial(pos, pos.tp2_price, TP2_SIZE_PCT, "TP2", current_bid)
                pos.tp2_hit = True

                # Activate trailing stop
                if USE_TRAILING_STOP:
                    pos.trail_active = True
                    pos.trail_stop = pos.highest_price * (1 - TRAIL_DISTANCE_PCT / 100)
                    print_colored(f"  [{symbol}] Trailing stop activated: ${pos.trail_stop:.2f}", "cyan")

            # Update trailing stop
            if pos.trail_active:
                new_trail = pos.highest_price * (1 - TRAIL_DISTANCE_PCT / 100)
                if new_trail > pos.trail_stop:
                    pos.trail_stop = new_trail

                # FIXED: Check current price against trail, not day low
                if current_price <= pos.trail_stop:
                    self._exit_position(pos, pos.trail_stop, ExitReason.TRAILING_STOP, current_bid)

        return bool(self.positions)

    def _take_partial(self, pos: ActivePosition, exit_price: float, size_pct: float, label: str,
                       current_bid: float = None):
        """
        Take partial profits using aggressive limit orders.

        CRITICAL FIX: Uses limit orders at bid - offset instead of market orders.
        This prevents slippage grenades on low-float small caps.
        """
        if not self.trading_client:
            return

        # Sync with Alpaca to ensure we have correct qty
        actual_qty = self._sync_position_qty(pos)
        if actual_qty <= 0:
            return

        partial_qty = int(pos.total_qty * size_pct)
        # Cap partial_qty to actual remaining
        if partial_qty > pos.remaining_qty:
            partial_qty = pos.remaining_qty
        if partial_qty <= 0:
            return

        # Determine session for order type
        session = get_market_session()

        try:
            # Calculate aggressive limit price (bid - small offset for fast fill)
            if current_bid and current_bid > 0:
                limit_price = round(current_bid - PANIC_EXIT_OFFSET, 2)
            else:
                limit_price = round(exit_price * 0.998, 2)  # 0.2% below target

            # Use IOC (Immediate or Cancel) for aggressive fills
            # In extended hours, always use limit orders
            if session in [MarketSession.PREMARKET, MarketSession.AFTERHOURS]:
                order = LimitOrderRequest(
                    symbol=pos.symbol,
                    qty=partial_qty,
                    side=OrderSide.SELL,
                    type=OrderType.LIMIT,
                    time_in_force=TimeInForce.DAY,  # Extended hours compatible
                    limit_price=limit_price,
                    extended_hours=True
                )
            else:
                # RTH: Use IOC limit for fast fill without market order risk
                order = LimitOrderRequest(
                    symbol=pos.symbol,
                    qty=partial_qty,
                    side=OrderSide.SELL,
                    type=OrderType.LIMIT,
                    time_in_force=TimeInForce.IOC,  # Immediate or Cancel
                    limit_price=limit_price
                )

            result = self.trading_client.submit_order(order)

            # Wait for fill (shorter wait for IOC)
            time.sleep(0.5)
            order_status = self.trading_client.get_order_by_id(result.id)

            if order_status.status == OrderStatus.FILLED:
                filled_price = float(order_status.filled_avg_price)
                pnl = (filled_price - pos.entry_price) * partial_qty
                pos.remaining_qty -= partial_qty
                self.daily_stats.gross_pnl += pnl

                r_mult = (filled_price - pos.entry_price) / pos.risk_per_share
                print_colored(f"  [{pos.symbol}] {label}: Sold {partial_qty} @ ${filled_price:.2f} "
                            f"| P&L: {format_money(pnl)} ({r_mult:.2f}R)", "green")

                self._log_trade({
                    "type": label,
                    "symbol": pos.symbol,
                    "timestamp": datetime.now(ET).isoformat(),
                    "qty": partial_qty,
                    "price": filled_price,
                    "pnl": pnl,
                    "r_mult": r_mult,
                    "order_type": "LIMIT_IOC",
                })

            elif order_status.status == OrderStatus.CANCELED:
                # IOC was canceled (no fill at our price) - retry with more aggressive price
                print_colored(f"  [{pos.symbol}] {label}: IOC not filled, retrying more aggressively", "yellow")
                self._take_partial_aggressive(pos, partial_qty, label, current_bid)

        except Exception as e:
            print_colored(f"  [{pos.symbol}] Partial exit failed: {e}", "red")

    def _take_partial_aggressive(self, pos: ActivePosition, qty: int, label: str, current_bid: float = None):
        """Fallback aggressive exit when IOC doesn't fill."""
        try:
            # More aggressive price (bid - 2x offset)
            if current_bid and current_bid > 0:
                limit_price = round(current_bid - (PANIC_EXIT_OFFSET * 2), 2)
            else:
                limit_price = round(pos.entry_price * 0.995, 2)  # 0.5% below entry

            order = LimitOrderRequest(
                symbol=pos.symbol,
                qty=qty,
                side=OrderSide.SELL,
                type=OrderType.LIMIT,
                time_in_force=TimeInForce.GTC,  # Good till cancel for fill
                limit_price=limit_price
            )

            result = self.trading_client.submit_order(order)
            print_colored(f"  [{pos.symbol}] {label}: Aggressive limit @ ${limit_price:.2f} submitted", "yellow")

            # Wait a bit longer for this one
            time.sleep(2)
            order_status = self.trading_client.get_order_by_id(result.id)

            if order_status.status == OrderStatus.FILLED:
                filled_price = float(order_status.filled_avg_price)
                pnl = (filled_price - pos.entry_price) * qty
                pos.remaining_qty -= qty
                self.daily_stats.gross_pnl += pnl
                r_mult = (filled_price - pos.entry_price) / pos.risk_per_share
                print_colored(f"  [{pos.symbol}] {label}: FILLED {qty} @ ${filled_price:.2f} "
                            f"| P&L: {format_money(pnl)} ({r_mult:.2f}R)", "green")
            else:
                print_colored(f"  [{pos.symbol}] {label}: Order pending - check positions", "yellow")

        except Exception as e:
            print_colored(f"  [{pos.symbol}] Aggressive exit failed: {e}", "red")

    def _sync_position_qty(self, pos: ActivePosition) -> int:
        """
        Sync position quantity with Alpaca to prevent qty mismatch errors.
        Returns the actual quantity available in Alpaca.
        """
        try:
            alpaca_pos = self.trading_client.get_open_position(pos.symbol)
            actual_qty = int(alpaca_pos.qty)
            if actual_qty != pos.remaining_qty:
                print_colored(f"  [{pos.symbol}] Qty sync: local={pos.remaining_qty}, Alpaca={actual_qty}", "yellow")
                pos.remaining_qty = actual_qty
                self._save_state()
            return actual_qty
        except Exception as e:
            # Position might not exist in Alpaca (already closed)
            if "position does not exist" in str(e).lower() or "404" in str(e):
                return 0
            # Other error - return what we have
            return pos.remaining_qty

    def _get_last_fill_price(self, symbol: str, fallback_price: float) -> float:
        """
        Get the last fill price for a symbol from recent orders.
        Used to determine actual exit price for externally closed positions.

        Args:
            symbol: Stock symbol
            fallback_price: Price to return if no recent fills found

        Returns:
            Last fill price or fallback
        """
        try:
            # Get recent filled orders for this symbol
            from alpaca.trading.requests import GetOrdersRequest
            from alpaca.trading.enums import QueryOrderStatus

            request = GetOrdersRequest(
                status=QueryOrderStatus.CLOSED,
                symbols=[symbol],
                limit=5,
                nested=False
            )
            orders = self.trading_client.get_orders(filter=request)

            # Find most recent SELL order that was filled
            for order in orders:
                if order.side.value == "sell" and order.filled_avg_price:
                    return float(order.filled_avg_price)

            # No sell orders found - use current market price
            snapshot = self.market_data.get_snapshot(symbol)
            if snapshot and snapshot.get("price"):
                return snapshot["price"]

            return fallback_price

        except Exception as e:
            # If we can't get order history, try current price
            try:
                snapshot = self.market_data.get_snapshot(symbol)
                if snapshot and snapshot.get("price"):
                    return snapshot["price"]
            except Exception:
                pass
            return fallback_price

    def _exit_position(self, pos: ActivePosition, exit_price: float, reason: ExitReason,
                        current_bid: float = None):
        """
        Fully exit a position using aggressive limit orders.

        CRITICAL FIX: Uses limit orders instead of market orders to prevent
        catastrophic slippage on low-float small caps.
        """
        if not self.trading_client:
            return

        # Mark exit as pending to prevent repeated attempts
        pos.exit_pending = True
        self._save_state()

        # CRITICAL: Sync with Alpaca to get actual qty before exit
        actual_qty = self._sync_position_qty(pos)
        if actual_qty <= 0:
            # Position already closed - clean up local state
            print_colored(f"  [{pos.symbol}] No shares to exit - removing from tracking", "cyan")
            del self.positions[pos.symbol]
            self._save_state()
            return

        session = get_market_session()
        bid_str = f"${current_bid:.2f}" if current_bid else "N/A"
        print_colored(f"  [{pos.symbol}] Attempting {reason.value} exit @ ${exit_price:.2f} (bid: {bid_str})", "yellow")

        try:
            # Calculate aggressive limit price
            if current_bid and current_bid > 0:
                limit_price = round(current_bid - PANIC_EXIT_OFFSET, 2)
            else:
                limit_price = round(exit_price * 0.998, 2)

            # Session-aware order creation
            if session in [MarketSession.PREMARKET, MarketSession.AFTERHOURS]:
                order = LimitOrderRequest(
                    symbol=pos.symbol,
                    qty=pos.remaining_qty,
                    side=OrderSide.SELL,
                    type=OrderType.LIMIT,
                    time_in_force=TimeInForce.DAY,
                    limit_price=limit_price,
                    extended_hours=True
                )
            else:
                # RTH: IOC for fast fill
                order = LimitOrderRequest(
                    symbol=pos.symbol,
                    qty=pos.remaining_qty,
                    side=OrderSide.SELL,
                    type=OrderType.LIMIT,
                    time_in_force=TimeInForce.IOC,
                    limit_price=limit_price
                )

            result = self.trading_client.submit_order(order)

            # Wait for fill
            time.sleep(0.5)
            order_status = self.trading_client.get_order_by_id(result.id)

            if order_status.status == OrderStatus.FILLED:
                self._process_exit_fill(pos, order_status, reason)

            elif order_status.status == OrderStatus.CANCELED:
                # IOC didn't fill - retry more aggressively
                print_colored(f"  [{pos.symbol}] Exit IOC not filled, retrying aggressively", "yellow")
                self._exit_aggressive(pos, reason, current_bid)

            elif order_status.status == OrderStatus.PARTIALLY_FILLED:
                # Partial fill - process what we got and retry rest
                filled_qty = int(order_status.filled_qty)
                if filled_qty > 0:
                    filled_price = float(order_status.filled_avg_price)
                    pnl = (filled_price - pos.entry_price) * filled_qty
                    self.daily_stats.gross_pnl += pnl
                    pos.remaining_qty -= filled_qty
                    print_colored(f"  [{pos.symbol}] Partial exit: {filled_qty} @ ${filled_price:.2f}", "yellow")

                # Retry rest
                if pos.remaining_qty > 0:
                    self._exit_aggressive(pos, reason, current_bid)

        except Exception as e:
            print_colored(f"  [{pos.symbol}] Exit failed: {e}", "red")
            # Last resort: try market order for emergency exit
            self._exit_market_emergency(pos, reason)

    def _exit_aggressive(self, pos: ActivePosition, reason: ExitReason, current_bid: float = None):
        """Aggressive exit when normal limit doesn't fill."""
        try:
            # Very aggressive price (bid - 3x offset or 1% below)
            if current_bid and current_bid > 0:
                limit_price = round(current_bid - (PANIC_EXIT_OFFSET * 3), 2)
            else:
                limit_price = round(pos.entry_price * 0.99, 2)

            order = LimitOrderRequest(
                symbol=pos.symbol,
                qty=pos.remaining_qty,
                side=OrderSide.SELL,
                type=OrderType.LIMIT,
                time_in_force=TimeInForce.GTC,
                limit_price=limit_price
            )

            result = self.trading_client.submit_order(order)
            print_colored(f"  [{pos.symbol}] Aggressive exit @ ${limit_price:.2f}", "yellow")

            time.sleep(2)
            order_status = self.trading_client.get_order_by_id(result.id)

            if order_status.status == OrderStatus.FILLED:
                self._process_exit_fill(pos, order_status, reason)
            else:
                print_colored(f"  [{pos.symbol}] Exit order pending - monitor closely!", "red")

        except Exception as e:
            print_colored(f"  [{pos.symbol}] Aggressive exit failed: {e}", "red")
            self._exit_market_emergency(pos, reason)

    def _exit_market_emergency(self, pos: ActivePosition, reason: ExitReason):
        """
        EMERGENCY ONLY: Market order exit when all else fails.

        This is a last resort - use only when position MUST be closed immediately.
        """
        try:
            print_colored(f"  [{pos.symbol}] EMERGENCY MARKET EXIT - {pos.remaining_qty} shares", "red")
            order = MarketOrderRequest(
                symbol=pos.symbol,
                qty=pos.remaining_qty,
                side=OrderSide.SELL,
                time_in_force=TimeInForce.DAY
            )

            result = self.trading_client.submit_order(order)
            time.sleep(1)
            order_status = self.trading_client.get_order_by_id(result.id)

            if order_status.status == OrderStatus.FILLED:
                self._process_exit_fill(pos, order_status, reason)
            else:
                # Order not filled yet - keep pending flag but warn user
                print_colored(f"  [{pos.symbol}] *** MANUAL INTERVENTION NEEDED ***", "red")
                print_colored(f"  [{pos.symbol}] Emergency order status: {order_status.status}", "red")
                print_colored(f"  [{pos.symbol}] Position still open - check Alpaca dashboard!", "red")

        except Exception as e:
            print_colored(f"  [{pos.symbol}] EMERGENCY EXIT FAILED: {e}", "red")
            print_colored(f"  [{pos.symbol}] *** MANUAL EXIT REQUIRED ***", "red")
            print_colored(f"  [{pos.symbol}] Remaining: {pos.remaining_qty} shares @ entry ${pos.entry_price:.2f}", "red")
            # Clear pending flag so user can try again or system retries
            pos.exit_pending = False
            self._save_state()

    def _process_exit_fill(self, pos: ActivePosition, order_status, reason: ExitReason):
        """Process a completed exit fill and update stats."""
        filled_price = float(order_status.filled_avg_price)
        filled_qty = int(order_status.filled_qty) if hasattr(order_status, 'filled_qty') else pos.remaining_qty
        pnl = (filled_price - pos.entry_price) * filled_qty
        self.daily_stats.gross_pnl += pnl
        self.daily_stats.trades += 1

        if pnl > 0:
            self.daily_stats.winners += 1
            self.daily_stats.consecutive_losses = 0
        else:
            self.daily_stats.losers += 1
            self.daily_stats.consecutive_losses += 1

        # Update size factor based on P&L and consecutive losses (percentage-based)
        _, _, max_daily_loss, full_size_cushion = self._calculate_risk_dollars()
        if self.daily_stats.gross_pnl >= full_size_cushion:
            self.daily_stats.size_factor = 1.0
        elif self.daily_stats.gross_pnl <= -max_daily_loss:
            self.daily_stats.trading_halted = True
            print_colored(f"  *** TRADING HALTED: Daily loss limit (${max_daily_loss:,.0f}) reached ***", "red")
        elif self.daily_stats.consecutive_losses >= MAX_CONSECUTIVE_LOSSES:
            # 3 strikes rule - reduce to minimum size
            self.daily_stats.size_factor = INITIAL_SIZE_FACTOR
            print_colored(f"  *** 3 STRIKES: Size reduced to {INITIAL_SIZE_FACTOR:.0%} ***", "yellow")

        r_mult = (filled_price - pos.entry_price) / pos.risk_per_share
        color = "green" if pnl > 0 else "red"
        print_colored(f"  [{pos.symbol}] EXIT ({reason.value}): {filled_qty} @ ${filled_price:.2f} "
                    f"| P&L: {format_money(pnl)} ({r_mult:.2f}R)", color)

        self._log_trade({
            "type": "EXIT",
            "reason": reason.value,
            "symbol": pos.symbol,
            "timestamp": datetime.now(ET).isoformat(),
            "qty": filled_qty,
            "price": filled_price,
            "pnl": pnl,
            "r_mult": r_mult,
            "order_type": "LIMIT",
            "consecutive_losses": self.daily_stats.consecutive_losses,
        })

        del self.positions[pos.symbol]
        self._save_state()

    def close_all_positions(self, reason: ExitReason = ExitReason.EOD_CLOSE):
        """Close all open positions."""
        for symbol, pos in list(self.positions.items()):
            print(f"Closing {symbol}...")
            snapshot = self.market_data.get_snapshot(symbol)
            if snapshot:
                exit_price = snapshot.get("last_trade_price") or snapshot["price"]
                current_bid = snapshot.get("bid", exit_price * 0.999)
            else:
                exit_price = pos.entry_price
                current_bid = None
            self._exit_position(pos, exit_price, reason, current_bid)

    def display_positions(self):
        """Display current positions."""
        if not self.positions:
            print("\n  No open positions")
            return

        print("\n" + "=" * 70)
        print(" OPEN POSITIONS")
        print("=" * 70)

        for symbol, pos in self.positions.items():
            snapshot = self.market_data.get_snapshot(symbol)
            # Use last_trade_price for accuracy (not day.c which can be stale)
            current_price = snapshot.get("last_trade_price") or snapshot.get("price", pos.entry_price) if snapshot else pos.entry_price

            unrealized_pnl = (current_price - pos.entry_price) * pos.remaining_qty
            r_mult = (current_price - pos.entry_price) / pos.risk_per_share
            pnl_pct = ((current_price - pos.entry_price) / pos.entry_price) * 100

            color = "green" if unrealized_pnl > 0 else "red"

            print()
            exit_status = " [EXIT PENDING]" if pos.exit_pending else ""
            print(f" {symbol}: {pos.remaining_qty}/{pos.total_qty} shares{exit_status}")
            print(f"   Entry: ${pos.entry_price:.2f} | Current: ${current_price:.2f}")
            print(f"   Stop:  ${pos.stop_price:.2f} | High: ${pos.highest_price:.2f}")
            print(f"   TP1:   ${pos.tp1_price:.2f} {'[HIT]' if pos.tp1_hit else ''}")
            print(f"   TP2:   ${pos.tp2_price:.2f} {'[HIT]' if pos.tp2_hit else ''}")
            if pos.trail_active:
                print(f"   Trail: ${pos.trail_stop:.2f} [ACTIVE]")
            print_colored(f"   P&L:   {format_money(unrealized_pnl)} ({r_mult:.2f}R, {pnl_pct:+.1f}%)", color)

        print()
        print("-" * 70)
        print(f" Daily: {self.daily_stats.trades} trades | "
              f"W:{self.daily_stats.winners} L:{self.daily_stats.losers} | "
              f"P&L: {format_money(self.daily_stats.gross_pnl)}")
        print("=" * 70)


# ============================================================
# MAIN FUNCTIONS
# ============================================================

def interactive_mode(executor: TradeExecutor):
    """Run in interactive mode."""
    clear_screen()

    print("=" * 60)
    print(" SMALLCAP EXECUTOR - Interactive Mode")
    print("=" * 60)
    print()
    print(" Commands:")
    print("   <TICKER>  - Analyze and trade a ticker")
    print("   positions - Show open positions")
    print("   close     - Close all positions")
    print("   stats     - Show daily statistics")
    print("   quit      - Exit")
    print()

    while True:
        try:
            user_input = input("\n Enter ticker (or command): ").strip().upper()

            if not user_input:
                continue

            if user_input == "QUIT" or user_input == "Q":
                break

            if user_input == "POSITIONS" or user_input == "P":
                executor.display_positions()
                continue

            if user_input == "CLOSE":
                confirm = input(" Close all positions? (y/n): ").strip().lower()
                if confirm == "y":
                    executor.close_all_positions(ExitReason.MANUAL)
                continue

            if user_input == "STATS":
                print(f"\n Daily Stats ({executor.daily_stats.date}):")
                print(f"   Trades: {executor.daily_stats.trades}")
                print(f"   Winners: {executor.daily_stats.winners}")
                print(f"   Losers: {executor.daily_stats.losers}")
                print(f"   P&L: {format_money(executor.daily_stats.gross_pnl)}")
                print(f"   Size Factor: {executor.daily_stats.size_factor:.0%}")
                continue

            # Assume it's a ticker
            setup = executor.validate_setup(user_input)
            if not setup:
                continue

            executor.display_setup(setup)

            # Confirm execution
            confirm = input("\n [ENTER] Execute | [S] Adjust stop | [Q] Cancel: ").strip().upper()

            if confirm == "Q":
                print(" Cancelled.")
                continue

            if confirm == "S":
                try:
                    new_stop = float(input(" New stop price: $").strip())
                    setup["stop_price"] = new_stop
                    setup["risk_per_share"] = setup["entry_price"] - new_stop
                    setup["stop_pct"] = (setup["risk_per_share"] / setup["entry_price"]) * 100

                    # Recalculate targets
                    setup["tp1_price"] = setup["entry_price"] + (setup["risk_per_share"] * TP1_R_MULTIPLE)
                    setup["tp2_price"] = setup["entry_price"] + (setup["risk_per_share"] * TP2_R_MULTIPLE)

                    # Recalculate position size
                    risk_dollars = MAX_RISK_PER_TRADE * executor.daily_stats.size_factor
                    setup["qty"] = int(risk_dollars / setup["risk_per_share"])
                    setup["position_value"] = setup["qty"] * setup["entry_price"]
                    setup["risk_dollars"] = setup["qty"] * setup["risk_per_share"]

                    executor.display_setup(setup)
                    confirm = input("\n [ENTER] Execute | [Q] Cancel: ").strip().upper()
                    if confirm == "Q":
                        print(" Cancelled.")
                        continue
                except ValueError:
                    print(" Invalid stop price.")
                    continue

            # Execute
            pos = executor.execute_entry(setup)
            if pos:
                print_colored("\n Position opened! Monitoring...", "green")

        except KeyboardInterrupt:
            print("\n\nExiting...")
            break


def check_keyboard_input() -> Optional[str]:
    """
    Check for keyboard input without blocking (Windows only).
    Returns the key pressed or None if no input.
    """
    try:
        if msvcrt.kbhit():
            key = msvcrt.getch().decode('utf-8', errors='ignore').lower()
            return key
    except Exception:
        pass
    return None


def monitor_mode(executor: TradeExecutor, return_on_flat: bool = False):
    """
    Run in position monitoring mode.

    Args:
        executor: TradeExecutor instance
        return_on_flat: If True, return when no positions remain (for prompt loop)

    Keys:
        x - Exit all positions manually
        1-9 - Exit specific position by number
        q - Quit monitoring (keeps positions open)
    """
    print("\n Monitoring positions...")
    print(" Keys: [X] Exit all | [1-9] Exit position # | [Q] Quit monitoring\n")

    try:
        while True:
            # Check EOD close
            now = datetime.now(ET)
            if now.time() >= EOD_CLOSE_TIME:
                print("\n EOD close triggered - closing all positions")
                executor.close_all_positions(ExitReason.EOD_CLOSE)
                break

            # Check for keyboard input (manual exit)
            key = check_keyboard_input()
            if key:
                if key == 'x':
                    # Exit all positions
                    print("\n")
                    confirm = input(" Exit ALL positions? (y/n): ").strip().lower()
                    if confirm == 'y':
                        print(" Closing all positions...")
                        executor.close_all_positions(ExitReason.MANUAL)
                        if return_on_flat:
                            print("\n All positions closed.")
                            return
                    else:
                        print(" Cancelled.")

                elif key == 'q':
                    # Quit monitoring but keep positions
                    print("\n\n Stopped monitoring (positions remain open).")
                    return

                elif key.isdigit() and int(key) >= 1:
                    # Exit specific position by number
                    pos_num = int(key)
                    symbols = list(executor.positions.keys())
                    if pos_num <= len(symbols):
                        symbol = symbols[pos_num - 1]
                        pos = executor.positions[symbol]
                        print(f"\n Exit {symbol}? (y/n): ", end="", flush=True)
                        confirm_key = None
                        while confirm_key not in ['y', 'n']:
                            confirm_key = check_keyboard_input()
                            if confirm_key is None:
                                time.sleep(0.1)
                        if confirm_key == 'y':
                            print("y")
                            snapshot = executor.market_data.get_snapshot(symbol)
                            if snapshot:
                                exit_price = snapshot.get("last_trade_price") or snapshot["price"]
                                current_bid = snapshot.get("bid", exit_price * 0.999)
                            else:
                                exit_price = pos.entry_price
                                current_bid = None
                            executor._exit_position(pos, exit_price, ExitReason.MANUAL, current_bid)
                        else:
                            print("n - Cancelled.")

            # Check positions
            has_positions = executor.check_positions()

            if has_positions:
                clear_screen()
                print(f" Monitoring... {now.strftime('%I:%M:%S %p ET')}")
                print(" Keys: [X] Exit all | [1-9] Exit position # | [Q] Quit\n")
                executor.display_positions()

                # Show position numbers for quick exit
                print("\n Position numbers for quick exit:")
                for i, symbol in enumerate(executor.positions.keys(), 1):
                    print(f"   [{i}] {symbol}")
            else:
                # No positions - return to prompt if requested
                if return_on_flat:
                    print("\n All positions closed.")
                    return  # Return to ticker prompt
                print(f"\r No open positions. Waiting... ({now.strftime('%I:%M:%S %p')})", end="")

            time.sleep(POSITION_CHECK_INTERVAL)

    except KeyboardInterrupt:
        print("\n\nStopped monitoring.")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Small Cap Momentum Executor")
    parser.add_argument("ticker", nargs="?", help="Ticker to trade")
    parser.add_argument("--monitor", action="store_true", help="Monitor existing positions")
    args = parser.parse_args()

    # Check dependencies
    if not ALPACA_AVAILABLE:
        print_colored("ERROR: Alpaca SDK not installed", "red")
        print("Install with: pip install alpaca-py")
        sys.exit(1)

    if not ALPACA_API_KEY:
        print_colored("ERROR: ALPACA_API_KEY not set", "red")
        print("Set with: set ALPACA_API_KEY=your_key")
        sys.exit(1)

    if not POLYGON_API_KEY:
        print_colored("ERROR: POLYGON_API_KEY not set", "red")
        print("Set with: set POLYGON_API_KEY=your_key")
        sys.exit(1)

    # Create executor
    executor = TradeExecutor()

    # Mode selection
    if args.monitor:
        monitor_mode(executor)
    elif args.ticker:
        # Direct ticker mode with loop for multiple trades
        ticker = args.ticker
        while True:
            setup = executor.validate_setup(ticker)
            if setup:
                executor.display_setup(setup)
                confirm = input("\n [ENTER] Execute | [Q] Cancel: ").strip().upper()
                if confirm != "Q":
                    pos = executor.execute_entry(setup)
                    if pos:
                        print_colored("\n Position opened! Monitoring...", "green")
                        # Monitor until position closes, then return to prompt
                        monitor_mode(executor, return_on_flat=True)

            # Prompt for next ticker
            print()
            print("=" * 60)
            next_ticker = input(" Enter next ticker (or 'q' to quit): ").strip().upper()
            if next_ticker in ["Q", "QUIT", ""]:
                break
            ticker = next_ticker
    else:
        interactive_mode(executor)


if __name__ == "__main__":
    main()
