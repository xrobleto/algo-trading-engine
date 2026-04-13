"""
Small Cap Pre-Market Executor
==============================

A specialized trade execution script for PRE-MARKET HOURS (4:00 AM - 9:30 AM ET).

Pre-market trading has fundamentally different characteristics than regular hours:

## KEY DIFFERENCES FROM REGULAR HOURS

### 1. LIQUIDITY
   - Volume is 5-10x LOWER than regular hours
   - Bid/ask depth is much thinner
   - Fills may take longer and partial fills are common
   - NEVER use market orders - always limit orders

### 2. SPREADS
   - Spreads are 2-5x WIDER than regular hours
   - A stock with 10 bps spread during RTH may have 50-100 bps pre-market
   - Wide spreads = slippage on entry AND exit
   - Must factor spread into risk/reward calculations

### 3. PRICE DISCOVERY
   - Prices can be highly volatile as market "discovers" fair value
   - Gaps can expand OR fade significantly before open
   - Volume surges around 7:00-8:00 AM as more traders wake up
   - Early pre-market (4-6 AM) is extremely thin - avoid unless necessary

### 4. ORDER TYPES
   - ONLY limit orders allowed (no market orders)
   - Extended hours flag MUST be set to True
   - IOC (Immediate or Cancel) is risky due to thin liquidity
   - Use DAY orders with extended_hours=True

### 5. NEWS/CATALYST DEPENDENCY
   - Pre-market moves are almost ALWAYS news-driven
   - Don't enter without a clear catalyst
   - Catalysts: FDA, earnings, contracts, offerings, squeeze potential

### 6. EXIT STRATEGIES
   - Option 1: CLOSE BEFORE OPEN (de-risk before volatility spike)
   - Option 2: HOLD THROUGH OPEN (ride momentum but higher risk)
   - Option 3: SCALE OUT (take partial profits pre-market, hold rest)
   - Trailing stops don't work well with thin liquidity

### 7. RISK MANAGEMENT
   - Smaller position sizes (50-60% of normal)
   - Fewer concurrent positions (max 2)
   - Tighter daily loss limits
   - Be prepared for gap risk if holding through open

## BEST PRACTICES

1. Focus on A+ setups ONLY - pre-market is for high-conviction plays
2. Wait for volume confirmation (7:00+ AM typically)
3. Use limit orders at midpoint or slightly above
4. Have exit plan BEFORE entry (close before open vs hold)
5. Size conservatively - liquidity constraints limit exits
6. Watch for gap fade - if gap starts closing, exit immediately

Usage:
    python smallcap_premarket_executor.py              # Interactive mode
    python smallcap_premarket_executor.py GLUE         # Direct ticker entry
    python smallcap_premarket_executor.py --monitor    # Monitor existing positions

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
from datetime import datetime, timedelta, time as dt_time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple
from enum import Enum
from zoneinfo import ZoneInfo
from pathlib import Path

# Windows keyboard input
try:
    import msvcrt
    WINDOWS = True
except ImportError:
    WINDOWS = False

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
# PRE-MARKET SPECIFIC CONFIGURATION
# ============================================================

ET = ZoneInfo("America/New_York")

# --- Directory Paths ---
ALGO_ROOT = Path(__file__).parent.parent
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

# ============================================================
# PRE-MARKET RISK MANAGEMENT (More Conservative)
# ============================================================

# Risk per trade - REDUCED for pre-market
RISK_PCT_PER_TRADE = 0.35        # Risk 0.35% (vs 0.5% regular hours)
MAX_DAILY_LOSS_PCT = 1.00        # Stop trading at 1% loss (vs 1.5%)
INITIAL_SIZE_FACTOR = 0.40       # Start at 40% of full risk
FULL_SIZE_CUSHION_PCT = 0.30     # Need 0.3% profit before full size

# Position limits - REDUCED
MAX_POSITIONS = 2                # Max 2 concurrent (vs 3)
MAX_POSITION_PCT = 10.0          # Max 10% of cash (vs 15%)
MAX_CONSECUTIVE_LOSSES = 2       # After 2 losses, stop (vs 3)

# Fallback minimums
MIN_RISK_DOLLARS = 30.00         # Lower minimum for pre-market
MAX_RISK_DOLLARS = 500.00        # Lower cap for pre-market

# ============================================================
# PRE-MARKET EXECUTION QUALITY GATES (Adapted for thin liquidity)
# ============================================================

# Spreads are wider pre-market - accept more but adjust sizing
MAX_SPREAD_BPS = 150             # 1.5% spread (vs 0.8% regular hours)
IDEAL_SPREAD_BPS = 80            # Ideal is still <0.8%
SPREAD_SIZE_PENALTY = 0.5        # Reduce size by 50% if spread > IDEAL

# Liquidity thresholds - LOWER expectations
MIN_BID_SIZE = 50                # 50 shares (vs 100)
MIN_ASK_SIZE = 50                # 50 shares (vs 100)
MIN_DOLLAR_VOLUME = 25_000       # $25K (vs $100K) - pre-market is thin

# Pre-market specific volume thresholds
PM_VOLUME_MINIMUM = 5_000        # Need at least 5K shares traded pre-market
PM_VOLUME_IDEAL = 50_000         # Ideal is 50K+ for good liquidity

# Entry offset for limit orders
ENTRY_OFFSET_PCT = 0.002         # Pay 0.2% above mid for fills
EXIT_OFFSET_PCT = 0.003          # Sell 0.3% below mid for exits

# ============================================================
# PRE-MARKET EXIT STRATEGY
# ============================================================

class ExitStrategy(Enum):
    """Pre-market exit strategy options."""
    CLOSE_BEFORE_OPEN = "CLOSE_BEFORE_OPEN"   # Exit before 9:30 AM
    HOLD_THROUGH_OPEN = "HOLD_THROUGH_OPEN"   # Hold into regular hours
    SCALE_OUT = "SCALE_OUT"                   # Partial exits

# Default exit strategy
DEFAULT_EXIT_STRATEGY = ExitStrategy.SCALE_OUT

# Scale-out parameters
SCALE_OUT_1_PCT = 0.50           # Take 50% at first target
SCALE_OUT_1_R = 0.75             # First target at 0.75R (quick profit)
SCALE_OUT_2_R = 1.5              # Second target at 1.5R
CLOSE_BEFORE_TIME = dt_time(9, 25)  # Close positions by 9:25 AM if CLOSE_BEFORE_OPEN

# ============================================================
# PRE-MARKET TIMING
# ============================================================

PREMARKET_START = dt_time(4, 0)      # Pre-market opens
PREMARKET_PRIME_START = dt_time(7, 0) # Volume picks up
PREMARKET_BEST_START = dt_time(8, 0)  # Best volume window
RTH_START = dt_time(9, 30)            # Regular hours start
CLOSE_POSITIONS_BY = dt_time(9, 25)   # Exit by this time if CLOSE_BEFORE_OPEN

# Minimum time before open to enter (avoid the chaos)
MIN_TIME_TO_OPEN_MINUTES = 10         # Don't enter within 10 min of open

# ============================================================
# PERSISTENCE FILES
# ============================================================

STATE_FILE = str(DATA_DIR / "premarket_executor_state.json")
TRADE_JOURNAL = str(DATA_DIR / "premarket_executor_trades.jsonl")

# ============================================================
# DATA STRUCTURES
# ============================================================

class ExitReason(Enum):
    STOP_LOSS = "STOP_LOSS"
    SCALE_OUT_1 = "SCALE_OUT_1"
    SCALE_OUT_2 = "SCALE_OUT_2"
    PRE_OPEN_CLOSE = "PRE_OPEN_CLOSE"    # Closed before market open
    TRAILING_STOP = "TRAILING_STOP"
    GAP_FADE = "GAP_FADE"                 # Gap is fading, exit early
    MANUAL = "MANUAL"


class PremarketPhase(Enum):
    """Current pre-market phase - affects trading decisions."""
    EARLY = "EARLY"          # 4:00-7:00 AM - Very thin, avoid
    PRIME = "PRIME"          # 7:00-8:00 AM - Volume picking up
    BEST = "BEST"            # 8:00-9:25 AM - Best liquidity
    CLOSE_ONLY = "CLOSE_ONLY" # 9:25-9:30 AM - Close positions only


def get_premarket_phase() -> Optional[PremarketPhase]:
    """Determine current pre-market phase."""
    now = datetime.now(ET)
    current_time = now.time()

    # Weekend check
    if now.weekday() >= 5:
        return None

    if current_time < PREMARKET_START:
        return None
    elif current_time < PREMARKET_PRIME_START:
        return PremarketPhase.EARLY
    elif current_time < PREMARKET_BEST_START:
        return PremarketPhase.PRIME
    elif current_time < CLOSE_POSITIONS_BY:
        return PremarketPhase.BEST
    elif current_time < RTH_START:
        return PremarketPhase.CLOSE_ONLY
    else:
        return None  # Regular hours - use regular executor


@dataclass
class PremarketPosition:
    """A position entered during pre-market."""
    symbol: str
    entry_time: datetime
    entry_price: float
    stop_price: float
    risk_per_share: float

    # Quantities
    total_qty: int
    remaining_qty: int

    # Exit targets
    target_1_price: float       # First scale-out target
    target_2_price: float       # Second target
    scale_1_hit: bool = False

    # Tracking
    highest_price: float = 0.0
    gap_at_entry: float = 0.0   # Gap % when we entered

    # Exit strategy for this position
    exit_strategy: ExitStrategy = ExitStrategy.SCALE_OUT

    # Order tracking
    entry_order_id: str = ""
    exit_pending: bool = False

    # Catalyst info
    catalyst: str = ""


@dataclass
class DailyStats:
    """Daily trading statistics."""
    date: str
    trades: int = 0
    winners: int = 0
    losers: int = 0
    gross_pnl: float = 0.0
    consecutive_losses: int = 0
    size_factor: float = 0.40
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
        "magenta": "\033[95m",
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


def minutes_to_open() -> int:
    """Calculate minutes until market open."""
    now = datetime.now(ET)
    market_open = now.replace(hour=9, minute=30, second=0, microsecond=0)
    if now >= market_open:
        return 0
    diff = market_open - now
    return int(diff.total_seconds() / 60)


# ============================================================
# MARKET DATA (Pre-market specific)
# ============================================================

class PremarketDataFetcher:
    """Fetches market data with pre-market awareness."""

    def __init__(self, alpaca_data_client=None):
        self.polygon_key = POLYGON_API_KEY
        self.alpaca_client = alpaca_data_client

    def get_alpaca_quote(self, symbol: str) -> Optional[dict]:
        """Get real-time quote from Alpaca."""
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

    def get_premarket_snapshot(self, symbol: str) -> Optional[dict]:
        """
        Get snapshot with PRE-MARKET specific data.

        Key fields:
        - premarket_price: Current pre-market price
        - premarket_volume: Volume traded pre-market
        - premarket_change: Change from previous close
        - prev_close: Previous day's close
        - gap_pct: Gap percentage from prev close
        """
        url = f"https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers/{symbol}"
        params = {"apiKey": self.polygon_key}

        try:
            resp = requests.get(url, params=params, timeout=5)
            if resp.status_code != 200:
                return None

            data = resp.json().get("ticker", {})
            if not data:
                return None

            # Extract pre-market data
            premarket = data.get("preMarket", {})
            day = data.get("day", {})
            prev_day = data.get("prevDay", {})

            # Get prices
            pm_price = premarket.get("close", 0) or premarket.get("last", 0)
            if pm_price == 0:
                # Fallback to current day price
                pm_price = day.get("c", 0) or day.get("vw", 0)

            prev_close = prev_day.get("c", 0)
            pm_volume = premarket.get("volume", 0)

            # Calculate gap
            gap_pct = 0
            if prev_close > 0 and pm_price > 0:
                gap_pct = ((pm_price - prev_close) / prev_close) * 100

            return {
                "symbol": symbol,
                "price": pm_price,
                "premarket_price": pm_price,
                "premarket_volume": pm_volume,
                "prev_close": prev_close,
                "gap_pct": gap_pct,
                "high": premarket.get("high", pm_price),
                "low": premarket.get("low", pm_price),
                "day_volume": day.get("v", 0),
                "todays_change_pct": data.get("todaysChangePerc", 0),
            }

        except Exception as e:
            print_colored(f"  Error fetching snapshot: {e}", "red")
            return None

    def get_premarket_bars(self, symbol: str, lookback_minutes: int = 60) -> Optional[pd.DataFrame]:
        """Get pre-market minute bars."""
        now = datetime.now(ET)
        start = now - timedelta(minutes=lookback_minutes)

        url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/minute/{start.strftime('%Y-%m-%d')}/{now.strftime('%Y-%m-%d')}"
        params = {
            "adjusted": "true",
            "sort": "asc",
            "apiKey": self.polygon_key
        }

        try:
            resp = requests.get(url, params=params, timeout=10)
            if resp.status_code != 200:
                return None

            results = resp.json().get("results", [])
            if not results:
                return None

            df = pd.DataFrame(results)
            df["timestamp"] = pd.to_datetime(df["t"], unit="ms", utc=True)
            df = df.rename(columns={"o": "open", "h": "high", "l": "low", "c": "close", "v": "volume"})

            # Filter to pre-market hours only (4:00 AM - 9:30 AM ET)
            df["timestamp"] = df["timestamp"].dt.tz_convert(ET)
            df = df[df["timestamp"].dt.time < dt_time(9, 30)]

            return df

        except Exception:
            return None

    def calculate_atr(self, bars: pd.DataFrame, period: int = 14) -> float:
        """Calculate ATR from bars."""
        if bars is None or len(bars) < period:
            return 0.0

        bars = bars.tail(period + 1).copy()
        bars["prev_close"] = bars["close"].shift(1)
        bars["tr"] = bars.apply(
            lambda x: max(
                x["high"] - x["low"],
                abs(x["high"] - x["prev_close"]) if pd.notna(x["prev_close"]) else 0,
                abs(x["low"] - x["prev_close"]) if pd.notna(x["prev_close"]) else 0
            ), axis=1
        )

        return bars["tr"].tail(period).mean()

    def get_gap_trend(self, symbol: str) -> str:
        """
        Determine if the gap is EXPANDING, HOLDING, or FADING.

        Returns: 'EXPANDING', 'HOLDING', 'FADING', or 'UNKNOWN'
        """
        bars = self.get_premarket_bars(symbol, lookback_minutes=30)
        if bars is None or len(bars) < 5:
            return "UNKNOWN"

        # Get recent bars
        recent = bars.tail(10)
        first_half = recent.head(5)["close"].mean()
        second_half = recent.tail(5)["close"].mean()

        # Get previous close for reference
        snapshot = self.get_premarket_snapshot(symbol)
        if not snapshot:
            return "UNKNOWN"

        prev_close = snapshot["prev_close"]
        if prev_close <= 0:
            return "UNKNOWN"

        # Calculate gap changes
        gap_first = ((first_half - prev_close) / prev_close) * 100
        gap_second = ((second_half - prev_close) / prev_close) * 100

        change = gap_second - gap_first

        if change > 0.5:
            return "EXPANDING"
        elif change < -0.5:
            return "FADING"
        else:
            return "HOLDING"


# ============================================================
# PRE-MARKET EXECUTOR
# ============================================================

class PremarketExecutor:
    """
    Trade executor specialized for pre-market hours.

    Key differences from regular executor:
    1. Only uses limit orders (never market)
    2. Always sets extended_hours=True
    3. More conservative position sizing
    4. Spread-adjusted sizing
    5. Different exit strategies (close before open, scale out)
    6. Gap fade detection
    """

    def __init__(self):
        """Initialize the pre-market executor."""
        if not ALPACA_AVAILABLE:
            raise RuntimeError("Alpaca SDK required. Install with: pip install alpaca-py")

        if not ALPACA_API_KEY or not ALPACA_SECRET_KEY:
            raise RuntimeError("Alpaca API keys not found in environment")

        # Initialize Alpaca clients
        self.trading_client = TradingClient(
            ALPACA_API_KEY,
            ALPACA_SECRET_KEY,
            paper=PAPER_TRADING
        )

        self.data_client = StockHistoricalDataClient(
            ALPACA_API_KEY,
            ALPACA_SECRET_KEY
        )

        # Market data fetcher
        self.market_data = PremarketDataFetcher(self.data_client)

        # State
        self.positions: Dict[str, PremarketPosition] = {}
        self.daily_stats = self._load_or_create_daily_stats()
        self.running = False
        self.monitor_thread = None

        # Get account info
        self._refresh_account_info()

    def _refresh_account_info(self):
        """Refresh account information."""
        try:
            account = self.trading_client.get_account()
            self.cash = float(account.cash)
            self.equity = float(account.equity)
            self.buying_power = float(account.buying_power)
        except Exception as e:
            print_colored(f"Warning: Could not fetch account info: {e}", "yellow")
            self.cash = 10000  # Fallback
            self.equity = 10000
            self.buying_power = 10000

    def _load_or_create_daily_stats(self) -> DailyStats:
        """Load or create daily stats."""
        today = datetime.now(ET).strftime("%Y-%m-%d")

        if os.path.exists(STATE_FILE):
            try:
                with open(STATE_FILE, 'r') as f:
                    data = json.load(f)
                    if data.get("date") == today:
                        return DailyStats(**data)
            except Exception:
                pass

        return DailyStats(date=today, size_factor=INITIAL_SIZE_FACTOR)

    def _save_state(self):
        """Save state to file."""
        try:
            with open(STATE_FILE, 'w') as f:
                json.dump({
                    "date": self.daily_stats.date,
                    "trades": self.daily_stats.trades,
                    "winners": self.daily_stats.winners,
                    "losers": self.daily_stats.losers,
                    "gross_pnl": self.daily_stats.gross_pnl,
                    "consecutive_losses": self.daily_stats.consecutive_losses,
                    "size_factor": self.daily_stats.size_factor,
                    "trading_halted": self.daily_stats.trading_halted,
                }, f, indent=2)
        except Exception:
            pass

    def _log_trade(self, trade_data: dict):
        """Log trade to journal."""
        try:
            trade_data["timestamp"] = datetime.now(ET).isoformat()
            trade_data["session"] = "PREMARKET"
            with open(TRADE_JOURNAL, 'a') as f:
                f.write(json.dumps(trade_data) + "\n")
        except Exception:
            pass

    # ========================================================
    # PRE-MARKET VALIDATION
    # ========================================================

    def validate_premarket_setup(self, symbol: str, catalyst: str = "") -> Optional[dict]:
        """
        Validate a ticker for pre-market entry.

        PRE-MARKET SPECIFIC CHECKS:
        1. Is it actually pre-market hours?
        2. Is there a catalyst? (required for PM trades)
        3. Is pre-market volume sufficient?
        4. Is spread acceptable (wider tolerance)?
        5. Is gap expanding, holding, or fading?
        6. Is there enough time before open?
        """
        symbol = symbol.upper().strip()
        print(f"\n{'='*60}")
        print(f"  PRE-MARKET VALIDATION: {symbol}")
        print(f"{'='*60}")

        # === PHASE CHECK ===
        phase = get_premarket_phase()
        if phase is None:
            print_colored("  BLOCKED: Not in pre-market hours", "red")
            print_colored("  Pre-market is 4:00 AM - 9:30 AM ET", "yellow")
            return None

        if phase == PremarketPhase.CLOSE_ONLY:
            print_colored("  BLOCKED: Within 5 min of open - close positions only", "red")
            return None

        if phase == PremarketPhase.EARLY:
            print_colored("  WARNING: Early pre-market (4-7 AM) - very thin liquidity", "yellow")

        mins_to_open = minutes_to_open()
        print(f"  Phase: {phase.value} | Minutes to open: {mins_to_open}")

        # === ENTRY GATES ===
        gates_passed, gate_reason = self._check_premarket_gates(symbol)
        if not gates_passed:
            print_colored(f"  BLOCKED: {gate_reason}", "red")
            return None

        # === TIME CHECK ===
        if mins_to_open < MIN_TIME_TO_OPEN_MINUTES:
            print_colored(f"  BLOCKED: Only {mins_to_open} min to open (min: {MIN_TIME_TO_OPEN_MINUTES})", "red")
            return None

        # === GET SNAPSHOT ===
        snapshot = self.market_data.get_premarket_snapshot(symbol)
        if not snapshot:
            print_colored("  ERROR: Could not fetch pre-market snapshot", "red")
            return None

        # === GET QUOTE ===
        quote = self.market_data.get_latest_quote(symbol)
        alpaca_quote = self.market_data.get_alpaca_quote(symbol)

        # Use best available quote
        if alpaca_quote and alpaca_quote.get("bid", 0) > 0:
            quote = alpaca_quote
            print_colored("  Using Alpaca quote (real-time)", "cyan")

        if not quote or quote.get("bid", 0) <= 0:
            print_colored("  ERROR: No valid bid/ask data", "red")
            return None

        bid = quote["bid"]
        ask = quote["ask"]
        mid = (bid + ask) / 2
        spread = ask - bid
        spread_bps = (spread / mid) * 10000 if mid > 0 else 9999

        # === EXECUTION QUALITY GATES ===
        print(f"\n  EXECUTION QUALITY:")

        # Spread check (wider tolerance for pre-market)
        if spread_bps <= IDEAL_SPREAD_BPS:
            spread_pass = True
            spread_penalty = 1.0
            spread_status = get_color("green") + f"GOOD ({spread_bps:.0f} bps)"
        elif spread_bps <= MAX_SPREAD_BPS:
            spread_pass = True
            spread_penalty = SPREAD_SIZE_PENALTY
            spread_status = get_color("yellow") + f"WIDE ({spread_bps:.0f} bps) - size reduced"
        else:
            spread_pass = False
            spread_penalty = 0
            spread_status = get_color("red") + f"TOO WIDE ({spread_bps:.0f} bps)"

        print(f"    Spread:     {spread_status}{get_color('reset')}")

        # Liquidity check
        bid_size = quote.get("bid_size", 0)
        ask_size = quote.get("ask_size", 0)
        liquidity_pass = bid_size >= MIN_BID_SIZE and ask_size >= MIN_ASK_SIZE
        liq_status = get_color("green") + "PASS" if liquidity_pass else get_color("red") + "FAIL"
        print(f"    Liquidity:  bid {bid_size} x ask {ask_size} [{liq_status}{get_color('reset')}]")

        # Pre-market volume check
        pm_volume = snapshot.get("premarket_volume", 0)
        if pm_volume >= PM_VOLUME_IDEAL:
            vol_pass = True
            vol_status = get_color("green") + f"GOOD ({pm_volume:,})"
        elif pm_volume >= PM_VOLUME_MINIMUM:
            vol_pass = True
            vol_status = get_color("yellow") + f"OK ({pm_volume:,})"
        else:
            vol_pass = False
            vol_status = get_color("red") + f"THIN ({pm_volume:,})"

        print(f"    PM Volume:  {vol_status}{get_color('reset')}")

        # === GAP ANALYSIS ===
        print(f"\n  GAP ANALYSIS:")
        gap_pct = snapshot.get("gap_pct", 0)
        prev_close = snapshot.get("prev_close", 0)
        current_price = snapshot.get("price", mid)

        gap_color = "green" if gap_pct > 10 else ("yellow" if gap_pct > 5 else "reset")
        print(f"    Gap:        {get_color(gap_color)}{gap_pct:+.1f}%{get_color('reset')}")
        print(f"    Prev Close: ${prev_close:.2f}")
        print(f"    Current:    ${current_price:.2f}")

        # Check gap trend
        gap_trend = self.market_data.get_gap_trend(symbol)
        trend_colors = {"EXPANDING": "green", "HOLDING": "yellow", "FADING": "red"}
        print(f"    Trend:      {get_color(trend_colors.get(gap_trend, 'reset'))}{gap_trend}{get_color('reset')}")

        if gap_trend == "FADING":
            print_colored("    WARNING: Gap is fading - higher risk entry", "yellow")

        # === CATALYST CHECK ===
        print(f"\n  CATALYST:")
        if catalyst:
            print(f"    Provided:   {catalyst}")
        else:
            print_colored("    WARNING: No catalyst provided - PM trades should have news", "yellow")

        # === COMBINED GATE RESULT ===
        gates_pass = spread_pass and liquidity_pass and vol_pass

        if not gates_pass:
            print()
            print_colored("  ⚠️  PRE-MARKET QUALITY GATES FAILED", "red")
            if not spread_pass:
                print_colored("     - Spread too wide for safe entry", "red")
            if not liquidity_pass:
                print_colored("     - Liquidity too thin", "red")
            if not vol_pass:
                print_colored("     - Pre-market volume too low", "red")
            return None

        # === CALCULATE ENTRY/EXIT LEVELS ===
        # Entry: slightly above mid to ensure fill
        entry_price = mid * (1 + ENTRY_OFFSET_PCT)
        entry_price = round(entry_price, 2)

        # ATR for stop calculation
        bars = self.market_data.get_premarket_bars(symbol)
        if bars is not None and len(bars) >= 5:
            atr = self.market_data.calculate_atr(bars)
        else:
            atr = current_price * 0.03  # Estimate 3% ATR for pre-market

        # Stop: 2.5x ATR below entry (tighter for PM)
        stop_distance = max(atr * 2.5, entry_price * 0.02)  # Min 2% stop
        stop_price = round(entry_price - stop_distance, 2)
        risk_per_share = entry_price - stop_price
        stop_pct = (risk_per_share / entry_price) * 100

        # Targets based on R multiples
        target_1_price = round(entry_price + (risk_per_share * SCALE_OUT_1_R), 2)
        target_2_price = round(entry_price + (risk_per_share * SCALE_OUT_2_R), 2)

        # === POSITION SIZING ===
        self._refresh_account_info()

        # Base risk calculation
        base_risk = self.cash * (RISK_PCT_PER_TRADE / 100) * self.daily_stats.size_factor

        # Apply spread penalty
        adjusted_risk = base_risk * spread_penalty

        # Clamp to limits
        risk_dollars = max(MIN_RISK_DOLLARS, min(MAX_RISK_DOLLARS, adjusted_risk))

        # Calculate shares
        qty = int(risk_dollars / risk_per_share) if risk_per_share > 0 else 0
        if qty < 1:
            print_colored("  ERROR: Calculated quantity is 0", "red")
            return None

        position_value = qty * entry_price

        # Check position size limit
        max_position = self.cash * (MAX_POSITION_PCT / 100)
        if position_value > max_position:
            qty = int(max_position / entry_price)
            position_value = qty * entry_price
            risk_dollars = qty * risk_per_share

        # === DISPLAY SETUP ===
        print(f"\n  TRADE SETUP:")
        print(f"    Entry:      ${entry_price:.2f} (mid + 0.2%)")
        print(f"    Stop:       ${stop_price:.2f} ({stop_pct:.1f}%)")
        print(f"    Target 1:   ${target_1_price:.2f} ({SCALE_OUT_1_R:.2f}R) - 50%")
        print(f"    Target 2:   ${target_2_price:.2f} ({SCALE_OUT_2_R:.2f}R) - 50%")
        print()
        print(f"    Shares:     {qty}")
        print(f"    Position:   {format_money(position_value)}")
        print(f"    Risk:       {format_money(risk_dollars)}")
        if spread_penalty < 1.0:
            print_colored(f"    (Size reduced {(1-spread_penalty)*100:.0f}% due to wide spread)", "yellow")

        return {
            "symbol": symbol,
            "phase": phase.value,
            "minutes_to_open": mins_to_open,
            "entry_price": entry_price,
            "stop_price": stop_price,
            "stop_pct": stop_pct,
            "target_1_price": target_1_price,
            "target_2_price": target_2_price,
            "risk_per_share": risk_per_share,
            "qty": qty,
            "position_value": position_value,
            "risk_dollars": risk_dollars,
            "spread_bps": spread_bps,
            "spread_penalty": spread_penalty,
            "gap_pct": gap_pct,
            "gap_trend": gap_trend,
            "pm_volume": pm_volume,
            "bid": bid,
            "ask": ask,
            "mid": mid,
            "catalyst": catalyst,
            "gates_pass": gates_pass,
        }

    def _check_premarket_gates(self, symbol: str) -> Tuple[bool, str]:
        """Check all pre-market entry gates."""
        # Trading halted?
        if self.daily_stats.trading_halted:
            return False, "Trading halted - daily loss limit reached"

        # Max positions?
        if len(self.positions) >= MAX_POSITIONS:
            return False, f"Max positions ({MAX_POSITIONS}) reached"

        # Already in this symbol?
        if symbol in self.positions:
            return False, f"Already have position in {symbol}"

        # Consecutive losses check
        if self.daily_stats.consecutive_losses >= MAX_CONSECUTIVE_LOSSES:
            return False, f"Max consecutive losses ({MAX_CONSECUTIVE_LOSSES}) - stop trading"

        return True, ""

    # ========================================================
    # ORDER EXECUTION (Pre-market specific)
    # ========================================================

    def execute_entry(self, setup: dict, exit_strategy: ExitStrategy = DEFAULT_EXIT_STRATEGY) -> bool:
        """
        Execute a pre-market entry.

        PRE-MARKET RULES:
        1. ONLY limit orders
        2. ALWAYS extended_hours=True
        3. No IOC - use DAY orders
        """
        symbol = setup["symbol"]
        entry_price = setup["entry_price"]
        qty = setup["qty"]

        print(f"\n{'='*60}")
        print(f"  EXECUTING PRE-MARKET ENTRY: {symbol}")
        print(f"{'='*60}")

        try:
            # PRE-MARKET ORDER: Limit with extended hours
            order = LimitOrderRequest(
                symbol=symbol,
                qty=qty,
                side=OrderSide.BUY,
                type=OrderType.LIMIT,
                time_in_force=TimeInForce.DAY,
                limit_price=round(entry_price, 2),
                extended_hours=True  # CRITICAL for pre-market
            )

            result = self.trading_client.submit_order(order)
            order_id = result.id

            print_colored(f"  Order submitted: {order_id}", "cyan")
            print(f"  Limit: ${entry_price:.2f} | Qty: {qty} | Extended Hours: True")

            # Wait for fill (longer timeout for pre-market)
            print("  Waiting for fill...", end="", flush=True)
            filled_qty = 0
            filled_price = 0

            for i in range(60):  # 60 iterations * 0.5s = 30 seconds max
                time.sleep(0.5)
                order_status = self.trading_client.get_order_by_id(order_id)

                if order_status.status == OrderStatus.FILLED:
                    filled_price = float(order_status.filled_avg_price)
                    filled_qty = int(order_status.filled_qty)
                    print()
                    print_colored(f"  FILLED: {filled_qty} @ ${filled_price:.2f}", "green")
                    break

                elif order_status.status == OrderStatus.PARTIALLY_FILLED:
                    filled_qty = int(order_status.filled_qty)
                    print(f"\r  Partial fill: {filled_qty}/{qty}...", end="", flush=True)

                elif order_status.status in [OrderStatus.CANCELED, OrderStatus.REJECTED]:
                    print()
                    print_colored(f"  Order {order_status.status.value}", "red")
                    return False

                else:
                    print(".", end="", flush=True)

            # Check final status
            if filled_qty == 0:
                print()
                print_colored("  NOT FILLED - canceling order", "yellow")
                try:
                    self.trading_client.cancel_order_by_id(order_id)
                except:
                    pass
                return False

            # Create position
            pos = PremarketPosition(
                symbol=symbol,
                entry_time=datetime.now(ET),
                entry_price=filled_price,
                stop_price=setup["stop_price"],
                risk_per_share=setup["risk_per_share"],
                total_qty=filled_qty,
                remaining_qty=filled_qty,
                target_1_price=setup["target_1_price"],
                target_2_price=setup["target_2_price"],
                highest_price=filled_price,
                gap_at_entry=setup["gap_pct"],
                exit_strategy=exit_strategy,
                entry_order_id=order_id,
                catalyst=setup.get("catalyst", ""),
            )

            self.positions[symbol] = pos
            self.daily_stats.trades += 1
            self._save_state()

            # Log entry
            self._log_trade({
                "type": "ENTRY",
                "symbol": symbol,
                "qty": filled_qty,
                "price": filled_price,
                "stop": setup["stop_price"],
                "target_1": setup["target_1_price"],
                "target_2": setup["target_2_price"],
                "gap_pct": setup["gap_pct"],
                "spread_bps": setup["spread_bps"],
                "pm_volume": setup["pm_volume"],
                "exit_strategy": exit_strategy.value,
            })

            print()
            print_colored(f"  Position opened: {symbol}", "green")
            print(f"  Strategy: {exit_strategy.value}")

            return True

        except Exception as e:
            print()
            print_colored(f"  ENTRY FAILED: {e}", "red")
            return False

    def execute_exit(self, symbol: str, reason: ExitReason, qty: int = None) -> bool:
        """
        Execute a pre-market exit.

        PRE-MARKET RULES:
        1. ONLY limit orders
        2. Set price at bid - offset for quick fills
        3. ALWAYS extended_hours=True
        """
        if symbol not in self.positions:
            print_colored(f"  No position in {symbol}", "red")
            return False

        pos = self.positions[symbol]
        if pos.exit_pending:
            print_colored(f"  Exit already pending for {symbol}", "yellow")
            return False

        exit_qty = qty if qty else pos.remaining_qty
        if exit_qty <= 0:
            return False

        pos.exit_pending = True

        print(f"\n  Exiting {symbol}: {exit_qty} shares ({reason.value})")

        try:
            # Get current quote for exit price
            quote = self.market_data.get_latest_quote(symbol)
            if quote and quote.get("bid", 0) > 0:
                # Price below bid for quick fill
                limit_price = round(quote["bid"] * (1 - EXIT_OFFSET_PCT), 2)
            else:
                # Fallback to last known price
                limit_price = round(pos.entry_price * 0.98, 2)

            # PRE-MARKET EXIT ORDER
            order = LimitOrderRequest(
                symbol=symbol,
                qty=exit_qty,
                side=OrderSide.SELL,
                type=OrderType.LIMIT,
                time_in_force=TimeInForce.DAY,
                limit_price=limit_price,
                extended_hours=True  # CRITICAL for pre-market
            )

            result = self.trading_client.submit_order(order)

            # Wait for fill
            time.sleep(1)
            order_status = self.trading_client.get_order_by_id(result.id)

            if order_status.status == OrderStatus.FILLED:
                filled_price = float(order_status.filled_avg_price)
                filled_qty = int(order_status.filled_qty)

                pnl = (filled_price - pos.entry_price) * filled_qty
                r_mult = (filled_price - pos.entry_price) / pos.risk_per_share

                pos.remaining_qty -= filled_qty
                self.daily_stats.gross_pnl += pnl

                # Update stats
                if pnl > 0:
                    self.daily_stats.winners += 1
                    self.daily_stats.consecutive_losses = 0
                else:
                    self.daily_stats.losers += 1
                    self.daily_stats.consecutive_losses += 1

                # Log exit
                self._log_trade({
                    "type": "EXIT",
                    "symbol": symbol,
                    "reason": reason.value,
                    "qty": filled_qty,
                    "entry_price": pos.entry_price,
                    "exit_price": filled_price,
                    "pnl": pnl,
                    "r_multiple": r_mult,
                })

                color = "green" if pnl > 0 else "red"
                print_colored(f"  {reason.value}: Sold {filled_qty} @ ${filled_price:.2f} | "
                            f"P&L: {format_money(pnl)} ({r_mult:.2f}R)", color)

                # Remove position if fully closed
                if pos.remaining_qty <= 0:
                    del self.positions[symbol]
                else:
                    pos.exit_pending = False

                self._save_state()
                return True

            else:
                print_colored(f"  Exit order status: {order_status.status.value}", "yellow")
                # Try more aggressive exit
                return self._aggressive_exit(pos, reason, exit_qty)

        except Exception as e:
            print_colored(f"  EXIT FAILED: {e}", "red")
            pos.exit_pending = False
            return False

    def _aggressive_exit(self, pos: PremarketPosition, reason: ExitReason, qty: int) -> bool:
        """Aggressive exit with lower price."""
        try:
            quote = self.market_data.get_latest_quote(pos.symbol)
            if quote and quote.get("bid", 0) > 0:
                limit_price = round(quote["bid"] * 0.995, 2)  # 0.5% below bid
            else:
                limit_price = round(pos.entry_price * 0.95, 2)

            order = LimitOrderRequest(
                symbol=pos.symbol,
                qty=qty,
                side=OrderSide.SELL,
                type=OrderType.LIMIT,
                time_in_force=TimeInForce.DAY,
                limit_price=limit_price,
                extended_hours=True
            )

            result = self.trading_client.submit_order(order)
            print_colored(f"  Aggressive exit @ ${limit_price:.2f} submitted", "yellow")

            time.sleep(2)
            order_status = self.trading_client.get_order_by_id(result.id)

            if order_status.status == OrderStatus.FILLED:
                filled_price = float(order_status.filled_avg_price)
                pnl = (filled_price - pos.entry_price) * qty

                pos.remaining_qty -= qty
                self.daily_stats.gross_pnl += pnl

                if pnl > 0:
                    self.daily_stats.winners += 1
                else:
                    self.daily_stats.losers += 1

                self._log_trade({
                    "type": "EXIT",
                    "symbol": pos.symbol,
                    "reason": f"{reason.value}_AGGRESSIVE",
                    "qty": qty,
                    "exit_price": filled_price,
                    "pnl": pnl,
                })

                print_colored(f"  Filled @ ${filled_price:.2f} | P&L: {format_money(pnl)}", "yellow")

                if pos.remaining_qty <= 0:
                    del self.positions[pos.symbol]
                else:
                    pos.exit_pending = False

                return True

            pos.exit_pending = False
            return False

        except Exception as e:
            print_colored(f"  Aggressive exit failed: {e}", "red")
            pos.exit_pending = False
            return False

    # ========================================================
    # POSITION MONITORING
    # ========================================================

    def monitor_positions(self):
        """Monitor pre-market positions with specific exit logic."""
        self.running = True

        print("\n" + "="*60)
        print("  PRE-MARKET POSITION MONITOR")
        print("  Controls: [Q]uit | [X] Close all | [S] Status")
        print("="*60)

        while self.running:
            phase = get_premarket_phase()

            # Check if we should close all (near market open)
            if phase == PremarketPhase.CLOSE_ONLY:
                print_colored("\n  *** CLOSE_ONLY PHASE - Closing all positions ***", "yellow")
                for symbol in list(self.positions.keys()):
                    self.execute_exit(symbol, ExitReason.PRE_OPEN_CLOSE)

            # Monitor each position
            for symbol, pos in list(self.positions.items()):
                if pos.exit_pending:
                    continue

                snapshot = self.market_data.get_premarket_snapshot(symbol)
                if not snapshot:
                    continue

                current_price = snapshot.get("price", 0)
                if current_price <= 0:
                    continue

                # Update highest price
                if current_price > pos.highest_price:
                    pos.highest_price = current_price

                # Calculate current P&L
                pnl = (current_price - pos.entry_price) * pos.remaining_qty
                r_mult = (current_price - pos.entry_price) / pos.risk_per_share

                # === CHECK EXIT CONDITIONS ===

                # 1. Stop loss
                if current_price <= pos.stop_price:
                    print_colored(f"\n  *** STOP HIT: {symbol} @ ${current_price:.2f} ***", "red")
                    self.execute_exit(symbol, ExitReason.STOP_LOSS)
                    continue

                # 2. Scale out at target 1
                if not pos.scale_1_hit and current_price >= pos.target_1_price:
                    print_colored(f"\n  *** TARGET 1 HIT: {symbol} @ ${current_price:.2f} ***", "green")
                    scale_qty = int(pos.total_qty * SCALE_OUT_1_PCT)
                    if scale_qty > 0:
                        self.execute_exit(symbol, ExitReason.SCALE_OUT_1, scale_qty)
                        pos.scale_1_hit = True
                        # Move stop to breakeven
                        pos.stop_price = pos.entry_price
                        print_colored(f"  Stop moved to breakeven: ${pos.entry_price:.2f}", "cyan")
                    continue

                # 3. Scale out at target 2
                if pos.scale_1_hit and current_price >= pos.target_2_price:
                    print_colored(f"\n  *** TARGET 2 HIT: {symbol} @ ${current_price:.2f} ***", "green")
                    self.execute_exit(symbol, ExitReason.SCALE_OUT_2)
                    continue

                # 4. Gap fade detection
                gap_trend = self.market_data.get_gap_trend(symbol)
                if gap_trend == "FADING" and r_mult > 0.5:
                    print_colored(f"\n  *** GAP FADING: {symbol} - Consider exit ***", "yellow")

                # Display status
                color = "green" if pnl > 0 else "red"
                print(f"\r  [{symbol}] ${current_price:.2f} | P&L: {format_money(pnl)} ({r_mult:.2f}R) | "
                      f"High: ${pos.highest_price:.2f} | Stop: ${pos.stop_price:.2f}  ", end="", flush=True)

            # Check for keyboard input
            if WINDOWS and msvcrt.kbhit():
                key = msvcrt.getch().decode('utf-8', errors='ignore').upper()
                if key == 'Q':
                    print("\n\n  Stopping monitor...")
                    self.running = False
                    break
                elif key == 'X':
                    print("\n\n  Closing all positions...")
                    for symbol in list(self.positions.keys()):
                        self.execute_exit(symbol, ExitReason.MANUAL)
                elif key == 'S':
                    self._print_status()

            time.sleep(2)

    def _print_status(self):
        """Print current status."""
        print("\n\n" + "="*60)
        print("  STATUS REPORT")
        print("="*60)

        phase = get_premarket_phase()
        mins = minutes_to_open()

        print(f"  Phase: {phase.value if phase else 'N/A'}")
        print(f"  Minutes to open: {mins}")
        print(f"  Positions: {len(self.positions)}/{MAX_POSITIONS}")
        print(f"  Daily P&L: {format_money(self.daily_stats.gross_pnl)}")
        print(f"  W/L: {self.daily_stats.winners}/{self.daily_stats.losers}")

        if self.positions:
            print("\n  Open Positions:")
            for symbol, pos in self.positions.items():
                print(f"    {symbol}: {pos.remaining_qty} @ ${pos.entry_price:.2f}")

        print("="*60 + "\n")

    # ========================================================
    # MAIN INTERFACE
    # ========================================================

    def run_interactive(self, ticker: str = None):
        """Run the executor in interactive mode."""
        clear_screen()

        print("="*60)
        print("  SMALL CAP PRE-MARKET EXECUTOR")
        print("  " + "="*56)
        print(f"  Mode: {'PAPER' if PAPER_TRADING else 'LIVE'}")
        print(f"  Max Risk: {RISK_PCT_PER_TRADE}% | Max Positions: {MAX_POSITIONS}")
        print("="*60)

        # Check phase
        phase = get_premarket_phase()
        if phase is None:
            print_colored("\n  ⚠️  NOT IN PRE-MARKET HOURS", "yellow")
            print("  Pre-market is 4:00 AM - 9:30 AM ET")
            print("  Use the regular executor during market hours.")
            return

        mins = minutes_to_open()
        print(f"\n  Phase: {phase.value}")
        print(f"  Minutes to open: {mins}")

        # Show account
        self._refresh_account_info()
        print(f"  Cash: {format_money(self.cash)}")
        print(f"  Daily P&L: {format_money(self.daily_stats.gross_pnl)}")

        if ticker:
            # Direct ticker entry
            setup = self.validate_premarket_setup(ticker)
            if setup and setup.get("gates_pass"):
                print("\n  Execute trade? [Y/N]: ", end="", flush=True)
                response = input().strip().upper()
                if response == 'Y':
                    self.execute_entry(setup)
                    if self.positions:
                        self.monitor_positions()
        else:
            # Interactive mode
            while True:
                print("\n" + "-"*40)
                print("  [T]icker entry | [M]onitor | [S]tatus | [Q]uit")
                cmd = input("  > ").strip().upper()

                if cmd == 'Q':
                    break
                elif cmd == 'T':
                    ticker = input("  Enter ticker: ").strip().upper()
                    if ticker:
                        catalyst = input("  Catalyst (optional): ").strip()
                        setup = self.validate_premarket_setup(ticker, catalyst)
                        if setup and setup.get("gates_pass"):
                            print("\n  Execute? [Y/N]: ", end="", flush=True)
                            if input().strip().upper() == 'Y':
                                self.execute_entry(setup)
                elif cmd == 'M':
                    if self.positions:
                        self.monitor_positions()
                    else:
                        print("  No open positions to monitor.")
                elif cmd == 'S':
                    self._print_status()


# ============================================================
# MAIN
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="Small Cap Pre-Market Executor")
    parser.add_argument("ticker", nargs="?", help="Ticker symbol to trade")
    parser.add_argument("--monitor", action="store_true", help="Monitor existing positions")
    args = parser.parse_args()

    # Load environment
    from dotenv import load_dotenv
    env_path = Path(__file__).parent.parent / "config" / "smallcap_scanner.env"
    if env_path.exists():
        load_dotenv(env_path)
        # Reload API keys after loading .env
        global ALPACA_API_KEY, ALPACA_SECRET_KEY, POLYGON_API_KEY
        ALPACA_API_KEY = os.getenv("ALPACA_API_KEY", "")
        ALPACA_SECRET_KEY = os.getenv("ALPACA_SECRET_KEY", "")
        POLYGON_API_KEY = (os.getenv("POLYGON_API_KEY") or os.getenv("MASSIVE_API_KEY") or "").strip()

    try:
        executor = PremarketExecutor()

        if args.monitor:
            executor.monitor_positions()
        else:
            executor.run_interactive(args.ticker)

    except KeyboardInterrupt:
        print("\n\nInterrupted by user.")
    except Exception as e:
        print_colored(f"\nError: {e}", "red")
        raise


if __name__ == "__main__":
    main()
