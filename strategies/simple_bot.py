"""
Momentum Trading Bot - Set It and Forget It
============================================

Strategy: High-probability intraday momentum with aggressive risk parameters

Key Features:
- RTH only (9:30 AM - 4:00 PM ET)
- VWAP + EMA trend confirmation
- Single-bracket architecture (100% scalp @ 1.0R) - v40 backtest optimal
- ATR-based stops (5x ATR, broker-native bracket orders)
- Hard daily loss limits with persistence
- Wider ADX filter (10-35) for more trades
- Opening Range Breakout filter (v41 - requires OR breakout after 10:00 AM)
- Multi-Timeframe Confluence (v41 - 5-min EMA confirmation)
- Enhanced 4-State Regime Detection (v41 - BULL/BEAR x LOW/HIGH_VOL quadrants)

v40 Backtest Results (252 days, $35k):
- +15.7% annual return ($5,509 profit)
- 60.0% win rate, 1.19 profit factor
- 10.9% max drawdown

Key v40 Optimizations:
- 5% risk per trade (was 1.25%)
- 90% max exposure (was 50%)
- ADX range 10-35 (was 12-30)
- 33% position size (was 10%)

v41 Improvements (Research-Backed):
- Opening Range Breakout filter: After 10:00 AM, require price above OR high
  - Research: 88% of daily high/low set by 10:30 AM, ORB strategies achieve 2.4+ Sharpe
- Multi-Timeframe Confluence: Require 5-min EMA9 > EMA20 for long entries
  - Filters out 1-min noise where higher timeframe doesn't confirm trend
- Enhanced 4-State Regime: BULL_LOW_VOL, BULL_HIGH_VOL, BEAR_LOW_VOL, BEAR_HIGH_VOL
  - Research: HMM regime detection achieved 98% return in 2008 vs -38% for SPY
  - Cleaner position sizing rules per quadrant

Architecture:
- Level 3 Production Quality
- Order lifecycle state machine
- Broker state reconciliation
- Graceful order/position cleanup

Author: Claude Code
Version: 4.1.0 (v41 ORB + MTF + 4-State Regime)
"""

from __future__ import annotations

import os
import sys
import json
import time
import signal
import logging
import threading
import datetime as dt
import hashlib
from dataclasses import dataclass, asdict, field
from enum import Enum
from typing import Dict, List, Optional, Tuple, Set

import requests
import pandas as pd
import websocket
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from zoneinfo import ZoneInfo

# v45: Import MarketScanner for enhanced dynamic universe
try:
    from market_scanner import MarketScanner as _MarketScanner
    _SCANNER_AVAILABLE = True
except ImportError:
    try:
        from strategies.market_scanner import MarketScanner as _MarketScanner
        _SCANNER_AVAILABLE = True
    except ImportError:
        _SCANNER_AVAILABLE = False

# ============================================================
# CONFIGURATION
# ============================================================

ET = ZoneInfo("America/New_York")

# --- Directory Paths (for organized folder structure) ---
# ALGO_ROOT is the parent of 'strategies/' folder (Algo_Trading root)
from pathlib import Path
ALGO_ROOT = Path(__file__).parent.parent
# Allow output dir override (useful when source lives on Google Drive)
_output_root = Path(os.getenv("ALGO_OUTPUT_DIR", "")) if os.getenv("ALGO_OUTPUT_DIR") else ALGO_ROOT
DATA_DIR = _output_root / "data" / "state"
LOGS_DIR = _output_root / "logs"
CONFIG_DIR = ALGO_ROOT / "config"

# Ensure directories exist
DATA_DIR.mkdir(parents=True, exist_ok=True)
LOGS_DIR.mkdir(exist_ok=True)

# --- API Credentials ---
ALPACA_API_KEY = os.getenv("ALPACA_API_KEY", "").strip()
ALPACA_SECRET_KEY = os.getenv("ALPACA_SECRET_KEY", "").strip()
ALPACA_TRADING_BASE_URL = os.getenv("ALPACA_BASE_URL", "https://paper-api.alpaca.markets").strip()
ALPACA_DATA_BASE_URL = os.getenv("ALPACA_DATA_BASE_URL", "https://data.alpaca.markets").strip()

# --- PRODUCTION SAFETY: Live Trading Arming Switch ---
# CRITICAL: Prevents accidental live trading. Requires EXPLICIT confirmation.
LIVE_TRADING_ENABLED = os.getenv("LIVE_TRADING", "0") == "1"
LIVE_TRADING_CONFIRMATION = os.getenv("I_UNDERSTAND_LIVE_TRADING", "").upper()

# Shutdown behavior
SHUTDOWN_POLICY = os.getenv("SHUTDOWN_POLICY", "CANCEL_ORDERS_ONLY").upper()  # "CANCEL_ORDERS_ONLY" or "FLATTEN_ALL"

# --- WebSocket Streaming (Level 3 Production) ---
# Alpaca WebSocket URLs for trade updates
ALPACA_WS_URL_PAPER = "wss://paper-api.alpaca.markets/stream"
ALPACA_WS_URL_LIVE = "wss://api.alpaca.markets/stream"
ENABLE_TRADE_UPDATES_STREAM = os.getenv("ENABLE_TRADE_STREAM", "1") == "1"  # Enable by default

# --- Alerting (Level 3 Production) ---
ENABLE_EMAIL_ALERTS = os.getenv("ENABLE_EMAIL_ALERTS", "0") == "1"
ENABLE_SLACK_ALERTS = os.getenv("ENABLE_SLACK_ALERTS", "0") == "1"
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "").strip()
ALERT_EMAIL_TO = os.getenv("ALERT_EMAIL_TO", "").strip()
ALERT_EMAIL_FROM = os.getenv("ALERT_EMAIL_FROM", "").strip()
SMTP_SERVER = os.getenv("SMTP_SERVER", "smtp.gmail.com").strip()
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USERNAME = os.getenv("SMTP_USERNAME", "").strip()
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "").strip()

POLYGON_API_KEY = (os.getenv("POLYGON_API_KEY") or os.getenv("MASSIVE_API_KEY") or "").strip()
POLYGON_REST_BASE = "https://api.polygon.io"
WS_URL = os.getenv("WS_URL", "wss://socket.massive.com/stocks").strip()

# --- Logging ---
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
METRICS_LOG_PATH = str(DATA_DIR / "momentum_bot_metrics.jsonl")

# --- Daily Equity Snapshot (Performance Tracking) ---
NOTES_DIR = _output_root / "project_notes"
NOTES_DIR.mkdir(exist_ok=True)
EQUITY_SNAPSHOT_PATH = str(NOTES_DIR / "simple_bot_equity.csv")
SCAN_DIAGNOSTICS_PATH = str(NOTES_DIR / "simple_bot_scan_diagnostics.csv")

# --- Trading Universe ---
# CRITICAL FIX: Expanded from 13 to 60+ symbols with sector diversification
# Prevents over-concentration in tech sector and provides more daily opportunities
CORE_SYMBOLS = [
    # Mega-Cap Tech (Core Holdings - High Liquidity)
    "NVDA", "AAPL", "MSFT", "GOOGL", "AMZN", "META", "TSLA",

    # Semiconductors (Reduced Concentration)
    "AMD", "AVGO",  # Kept 2 of 3 to reduce correlation with NVDA

    # High-Momentum Growth (v6: removed MSTR, SHOP - negative expectancy in backtest)
    "COIN", "PLTR", "SQ", "HOOD", "SOFI", "RBLX",

    # Financials (ADD: Diversification)
    "JPM", "GS", "BAC", "MS", "V", "MA", "AXP",

    # Healthcare (ADD: Defensive)
    "UNH", "LLY", "JNJ", "ABBV", "MRK", "TMO",

    # Energy (ADD: Sector Rotation) - XLE removed (managed by trend_bot)
    "CVX", "XOM", "COP", "SLB",

    # Consumer Discretionary (ADD: Stability)
    "COST", "WMT", "HD", "LOW", "MCD", "NKE", "SBUX",

    # Cloud/SaaS (Momentum Leaders) (v6: removed CRWD - poor backtest performance)
    "NET", "DDOG", "ZS", "PANW", "NOW", "CRM", "SNOW",

    # Communication Services
    "NFLX", "DIS", "CMCSA",

    # Industrials
    "CAT", "BA", "UPS", "LMT", "RTX",

    # Broad Market ETFs - DIA only (SPY/QQQ/IWM removed to avoid conflict with trend_bot)
    "DIA",

    # NOTE: Sector ETFs (XLF, XLV, XLI, XLY, XLP, XLB) removed to avoid conflict with trend_bot
    # trend_bot holds these as long-term positions; simple_bot would flatten them at EOD

    # REMOVED/LIMITED: Leveraged ETFs (was SOXL, TSLL, SMCX)
    # Reason: 3x leverage concentration risk - use sparingly or remove entirely
]

# Symbols managed by trend_bot - simple_bot must NEVER trade or manage these
# This prevents: EOD flatten killing trend_bot positions, orphan adoption, capital conflicts
TREND_BOT_SYMBOLS = {
    # trend_bot equity ETFs
    "SPY", "QQQ", "IWM",
    # trend_bot sector ETFs (v8: removed XLP, XLU, XLRE, XLB)
    "XLK", "XLF", "XLE", "XLV", "XLI", "XLY", "XLC",
    "SMH", "IBB", "XHB",
    # trend_bot factor ETFs (v8: removed VLUE)
    "MTUM", "QUAL",
    # trend_bot leveraged ETFs (v8: new)
    "TQQQ", "UPRO", "SOXL", "TECL", "FAS",
    # trend_bot momentum/thematic ETFs (v8: new)
    "ARKK", "XBI", "KWEB", "SOXX", "IGV", "CIBR", "SKYY",
    # trend_bot defensive ETFs
    "IEF", "TLT", "GLD", "DBC",
    # trend_bot cash
    "SGOV",
}

# Sector classification for correlation risk management
SECTOR_MAP = {
    # Technology
    "NVDA": "semiconductors", "AMD": "semiconductors", "AVGO": "semiconductors",
    "AAPL": "tech", "MSFT": "tech", "GOOGL": "tech", "META": "tech", "AMZN": "tech",

    # High-Beta Tech/Crypto
    "TSLA": "ev_auto", "COIN": "crypto", "MSTR": "crypto",
    "PLTR": "software", "SHOP": "ecommerce", "SQ": "fintech", "HOOD": "fintech",
    "SOFI": "fintech", "RBLX": "gaming",

    # Financials
    "JPM": "banks", "GS": "banks", "BAC": "banks", "MS": "banks",
    "V": "payments", "MA": "payments", "AXP": "payments",

    # Healthcare
    "UNH": "healthcare", "LLY": "pharma", "JNJ": "pharma", "ABBV": "pharma",
    "MRK": "pharma", "TMO": "healthcare",

    # Energy
    "CVX": "energy", "XOM": "energy", "COP": "energy", "SLB": "energy",

    # Consumer
    "COST": "retail", "WMT": "retail", "HD": "retail", "LOW": "retail",
    "MCD": "restaurants", "NKE": "apparel", "SBUX": "restaurants",

    # Cloud/SaaS
    "NET": "cloud", "DDOG": "cloud", "CRWD": "cybersecurity", "ZS": "cybersecurity",
    "PANW": "cybersecurity", "NOW": "enterprise_software", "CRM": "enterprise_software",
    "SNOW": "cloud",

    # Communication
    "NFLX": "streaming", "DIS": "media", "CMCSA": "media",

    # Industrials
    "CAT": "industrials", "BA": "aerospace", "UPS": "logistics",
    "LMT": "defense", "RTX": "defense",

    # Broad Market ETFs (SPY/QQQ/IWM removed - managed by trend_bot)
    "DIA": "dow_etf",
}

# --- Risk Management (v48c: conservative production sizing) ---
# Backtest used 4x margin (200% per position, 400% total). Production uses ~1.5x for safety.
MAX_CAPITAL_USAGE_PCT = 1.50    # v48c: 150% max exposure (moderate margin, backtest 400%)
MAX_RISK_PER_TRADE_PCT = 0.08   # v48c: 8% of capital per trade (moderate, backtest 15%)
MAX_DAILY_LOSS_PCT = 0.04       # v48c: 4% daily loss limit (slightly higher for margin)
MAX_POSITIONS = 4               # v48c: 4 max concurrent positions (backtest 4)
POSITION_SIZE_PCT = 0.35        # v49: 35% of deployable per position (was 25%) - more capital per trade, 4 positions = ~210% max

# MODERATE FIX: Max daily trades limit (prevents overtrading/churn)
MAX_DAILY_TRADES = 6            # Max 6 trades per day (prevents revenge trading and overtrading)

# ENHANCEMENT #1: Dynamic Universe Discovery
ENABLE_DYNAMIC_UNIVERSE = True   # Scan for high RVOL stocks beyond core universe
DYNAMIC_SCAN_INTERVAL_SEC = 120  # Scan for new movers every 2 minutes (v6: reduced from 5 min for faster discovery)

# v45: Enhanced Market Scanner (replaces basic discover_dynamic_universe when available)
# Uses multi-factor scoring (RVOL + change% + catalyst + volume accel + spread + dollar vol)
# Falls back to basic RVOL-based scanning if market_scanner.py is not importable
USE_ENHANCED_SCANNER = True and _SCANNER_AVAILABLE  # Auto-detect availability
DYNAMIC_MIN_PRICE = 10.0         # Lower bound for dynamic discovery
DYNAMIC_MAX_PRICE = 1000.0       # Upper bound for dynamic discovery
DYNAMIC_MIN_RVOL = 2.5           # 2.5x+ volume for dynamic discovery (higher than core MIN_RELATIVE_VOLUME)
DYNAMIC_MAX_UNIVERSE_SIZE = 20   # Max symbols to add from dynamic discovery
DYNAMIC_MIN_VOLUME_USD = 50_000_000  # Min $50M daily volume (liquidity filter)

# v6: Exclusion list - symbols with negative expectancy from backtesting
# These showed consistent losses despite high volume/momentum
# Also includes TREND_BOT_SYMBOLS to prevent dynamic discovery from picking up trend_bot holdings
DYNAMIC_EXCLUSION_LIST = {
    "MSTR",   # -$1,016 over 18 trades - too volatile, frequent stop-outs
    "SHOP",   # -$1,002 (100% stop-out rate) - mean-reverting against momentum
    "CRWD",   # -$800 over 4 trades - poor momentum characteristics
    # Add more based on ongoing analysis
} | TREND_BOT_SYMBOLS  # Union with trend_bot symbols to prevent dynamic discovery conflicts

# MODERATE FIX: Commission and fee tracking for accurate PnL
# Alpaca commission structure (update based on your actual plan)
COMMISSION_PER_SHARE = 0.0       # $0/share for most Alpaca accounts (commission-free)
COMMISSION_MIN = 0.0             # No minimum commission
SEC_FEE_RATE = 0.0000278         # SEC fee: $27.80 per $1M of principal (sells only)
TAF_FEE_RATE = 0.000166          # Trading Activity Fee: $0.000166 per share (sells only, capped)
TAF_FEE_MAX = 7.27               # TAF cap per trade
# Note: FINRA TAF only applies to sales, capped at $7.27 per trade
# For paper trading, fees may be simulated or zero

# --- Entry Criteria (Tuned for A+ Quality) ---
MIN_RELATIVE_VOLUME = 1.0       # Relaxed from 1.5x - compound filter effect made 1.5x too restrictive
RVOL_LOOKBACK_DAYS = 20         # Days of historical data for RVOL calculation
EMA_FAST = 9                    # Fast EMA for trend
EMA_SLOW = 20                   # Slow EMA for trend
MIN_PRICE = 10.0                # Minimum stock price (lowered for more mid-cap movers)
MAX_SPREAD_BPS = 5.0            # Tighter spread for mega-caps (was 10.0)

# --- v48c: Gap-Up Filter (Key Discovery) ---
# Only trade stocks that gapped up from previous day's close.
# Stocks with overnight catalysts (earnings, news, upgrades) have sustained intraday momentum.
# Backtest: PF 1.02 without → 1.36 with gap filter (most impactful single improvement).
USE_GAP_UP_FILTER = True
MIN_GAP_UP_PCT = 0.10           # Relaxed from 0.5% - still requires positive gap but catches more setups

# --- v48c: SPY VWAP Market Filter ---
# Only enter new positions when SPY is above its intraday VWAP.
# Gap-up stocks on bullish market days have strongest momentum continuation.
# Backtest: 40.4% return without → 75.2% with SPY filter (when combined with gap-up).
USE_SPY_VWAP_GATE = True
SPY_VWAP_MIN_DISTANCE_PCT = 0.0  # SPY must be at or above VWAP (any amount)

# --- Entry Execution ---
ENTRY_REPRICE_MAX_ATTEMPTS = 2     # Max number of repricing attempts (total: initial + 2 reprices = 3 tries)
ENTRY_REPRICE_INTERVAL_SEC = 1.5   # Wait time before repricing (1.5 seconds)
ENTRY_REPRICE_MAX_SLIPPAGE_BPS = 10  # Max slippage allowed: 10 bps (0.1%) above initial ask
ENTRY_FILL_TIMEOUT_SEC = 5.0       # Max time to wait for fill confirmation after all reprices

# --- Exit Strategy (Dual-Bracket: Scalp + Runner) ---
ATR_PERIOD = 14                 # ATR calculation period
ATR_TIMEFRAME = "1min"          # EXPLICIT: ATR computed on 1-minute bars (14-minute ATR for intraday volatility)
                                 # NOTE: This is INTRADAY volatility, NOT daily ATR
                                 # 14-minute ATR captures recent price action, adapts quickly to market conditions

# Symbol-specific ATR stop multipliers (adaptive to volatility)
# v30 BACKTEST OPTIMAL: 5.0x provides balanced stop distance
ATR_STOP_MULTIPLIER_DEFAULT = 4.0       # v48c: 4.0x (was 5.0) - tighter stops for gap-up intraday plays
ATR_STOP_MULTIPLIER_LEVERAGED = 4.0     # v50: Same as default — ATR already captures leverage volatility (was 7.0, double-counting)
LEVERAGED_SYMBOLS = {
    "SOXL", "SOXS", "TSLL",  # 2-3x semiconductors / Tesla
    "TQQQ", "SQQQ",           # 3x Nasdaq
    "SPXL", "SPXS",           # 3x S&P 500
    "UPRO", "SPXU",           # 3x S&P 500 (alt)
    "TNA", "TZA",             # 3x Russell 2000
    "LABU", "LABD",           # 3x Biotech
    "FAS", "FAZ",             # 3x Financials
    "GUSH", "DRIP",           # 2x Oil & Gas
    "NUGT", "DUST",           # 2x Gold Miners
    "TECL", "TECS",           # 3x Technology
    "SMCX",                   # 3x Small Cap
}  # Leveraged ETFs - position size divided by 3

# CRITICAL FIX: Minimum stop distance to prevent stops too close to entry
MIN_STOP_DISTANCE_PCT = 0.007   # v48c: Minimum 0.7% stop distance (tighter for gap-up intraday)

# Scalp bracket (100% of position) - v30 BACKTEST OPTIMAL
SCALP_BRACKET_PCT = 1.00        # v30: 100% scalp - runner underperforms, eliminated
SCALP_TP_R = 3.00               # v48c: 3.0R TP cap (higher cap lets trail capture bigger moves from gap-ups)
SCALP_USE_IOC = True            # Use IOC for scalp TP orders (immediate fill or cancel)

# Runner bracket (0% of position) - v30 BACKTEST OPTIMAL: ELIMINATED
RUNNER_BRACKET_PCT = 0.00       # v30: 0% runner - backtesting shows runner drags down returns
RUNNER_TP_R = 2.50              # Not used (runner eliminated)

# ENHANCEMENT #2: Trailing Stops - v30 BACKTEST OPTIMAL
# Note: With 100% scalp allocation, trailing stops apply to all positions
ENABLE_TRAILING_STOP = True     # Use trailing stop to protect profits
TRAILING_STOP_ACTIVATION_R = 0.75  # v50: Activate at 0.75R (was 0.60R — too tight, triggered by normal vol)
TRAILING_STOP_DISTANCE_R = 0.60    # v50: Trail by 0.60R (was 0.50R — more room for follow-through)

# v51: Multi-level take-profit ladder
# Replaces single 3.0R scalp exit with TP1 (scale out) → breakeven SL → TP2 (scale out) → runner (trail).
# Ladder is software-managed because Alpaca brackets close the full position atomically on TP hit.
# Entry order class switches from "bracket" to "oto" with a stop-loss child only.
USE_TP_LADDER = True                      # Set False to revert to single-TP bracket behavior
TP1_R = 1.5                               # First take-profit at 1.5R
TP2_R = 3.0                               # Second take-profit at 3.0R (= legacy SCALP_TP_R)
TP1_SCALE_PCT = 0.50                      # Sell 50% at TP1
TP2_SCALE_PCT = 0.25                      # Sell 25% at TP2
# Runner = 1 - TP1_SCALE_PCT - TP2_SCALE_PCT = 0.25 (rides trailing stop)
MOVE_SL_TO_BREAKEVEN_AFTER_TP1 = True     # After TP1 fills, resize stop to entry fill price

# v45 IMPROVEMENT #9: Adaptive Trailing Stop Distance
# Adjusts trail distance based on move strength and time of day.
# Strong runners get more room; weak grinds get taken off quickly.
# Afternoon trades get tighter stops (momentum fades late in the day).
USE_ADAPTIVE_TRAILING = False       # v45: Disabled - fixed 0.40/0.40 outperformed adaptive tiers in backtest
# Trail distance tiers based on peak R-multiple reached
TRAIL_DISTANCE_STRONG_R = 0.45     # Peak >= 1.2R: strong runner, give room
TRAIL_DISTANCE_NORMAL_R = 0.30     # Peak 0.7-1.2R: standard trail
TRAIL_DISTANCE_WEAK_R = 0.20       # Peak < 0.7R: just activated, protect quickly
TRAIL_STRONG_THRESHOLD = 1.2       # R-multiple to qualify as "strong"
TRAIL_WEAK_THRESHOLD = 0.7         # R-multiple below which move is "weak"
# Time-based trail tightening (afternoon momentum fades)
TRAIL_TIME_TIGHTEN_HOUR_1 = 13     # After 1 PM: reduce distance by 20%
TRAIL_TIME_TIGHTEN_MULT_1 = 0.80
TRAIL_TIME_TIGHTEN_HOUR_2 = 14     # After 2 PM: reduce distance by 35%
TRAIL_TIME_TIGHTEN_MULT_2 = 0.65

# Fallback (if ATR unavailable)
SCALP_TP_PCT = 0.08             # 8% gain for scalp
RUNNER_TP_PCT = 0.20            # 20% gain for runner
FALLBACK_STOP_PCT = 0.03        # 3% stop loss

AUTO_CLOSE_EOD = True           # Auto-close all positions at end of day to avoid overnight gap risk
EOD_CLOSE_TIME_ET = (15, 55)    # Close all positions at 3:55 PM ET (hour, minute)

# --- Gradual EOD Position Reduction ---
# Instead of flattening everything at once at 3:55, gradually reduce positions
# starting 30 minutes before close (3:25 PM). Reduces 10% every 5 minutes.
# This improves execution and reduces market impact at EOD.
GRADUAL_EOD_REDUCTION = True          # Enable gradual EOD reduction (set False for immediate flatten)
EOD_REDUCTION_START_MINUTES = 40      # v50: Start reducing 40 min before close (was 30 — only 47% reduced)
EOD_REDUCTION_INTERVAL_MINUTES = 5    # Reduce every 5 minutes
EOD_REDUCTION_PERCENT = 0.20          # v50: Reduce 20% per interval (was 10% — now ~83% sold gradually)

# --- Data & Indicators ---
LOOKBACK_BARS = 100             # Bars for indicator calculation
DATA_REFRESH_SECONDS = 10       # How often to refresh market data

# --- WebSocket ---
WS_PING_INTERVAL = 30
WS_PING_TIMEOUT = 10

# --- State Persistence ---
STATE_PATH = str(DATA_DIR / "momentum_bot_state.json")
STATE_FLUSH_SECONDS = 5.0
TRADE_INTENTS_PATH = str(DATA_DIR / "momentum_bot_trade_intents.json")  # Trade lifecycle state machine persistence
STATE_LOCK_PATH = str(DATA_DIR / "momentum_bot_state.lock")  # Lock file for atomic writes

# --- Metrics & Trade Journal ---
TRADE_JOURNAL_PATH = str(DATA_DIR / "momentum_bot_trades.jsonl")  # JSONL trade journal for analytics
HEARTBEAT_LOG_PATH = str(DATA_DIR / "momentum_bot_heartbeat.jsonl")  # Operational heartbeat metrics
HEARTBEAT_INTERVAL_SEC = 60  # Log heartbeat every 60 seconds

# Log Rotation (prevent unbounded growth)
MAX_LOG_SIZE_MB = 50  # Rotate logs at 50 MB
MAX_LOG_BACKUPS = 5   # Keep 5 backup files

# --- Kill Switch (Level 3 Production) ---
KILL_SWITCH_FILE = str(DATA_DIR / "HALT_TRADING")  # Create this file to emergency halt
KILL_SWITCH_ENV = "KILL_SWITCH"    # Or set this env var to "1"

# --- Order Lifecycle ---
ENTRY_TIMEOUT_SEC = 45  # Cancel unfilled entry brackets after 45 seconds (increased from 15 to allow larger orders to fill)

# --- Session Guards --- v6 OPTIMIZED
LATE_DAY_CUTOFF_TIME_ET = (15, 30)  # v49: No new entries after 3:30 PM ET (was 2:30) - opens power hour, EOD reduction starts 3:25 PM

# --- Circuit Breakers ---
MAX_SNAPSHOT_AGE_SEC = 10       # Max age for snapshot data before considering it stale
MAX_API_FAILURES_PER_MIN = 5    # Max API failures before halting (circuit breaker)
API_FAILURE_WINDOW_SEC = 60     # Window for counting API failures

# --- Phase 3: Optimization Parameters ---

# Smart Entry Timing
# v31: Tiered spread limits - wider for dynamic movers with high RVOL
MAX_ENTRY_SPREAD_BPS = 40.0     # v32: Increased from 25 to 40 bps to allow small/mid caps
                                 # Note: LQDA blocked at 29-33bps with 25bps limit
                                 # Small/mid caps commonly have 30-50bps spreads when moving
MAX_ENTRY_SPREAD_BPS_TIGHT = 12.0  # v32: Increased from 8 to 12 for mega-caps
MIN_QUOTE_SIZE = 100            # Minimum bid/ask size (shares) for quality entry

# --- v5 IMPROVEMENTS (from backtest optimization) ---

# IMPROVEMENT #1: ADX Filter (v40 BACKTEST OPTIMAL)
# ADX (Average Directional Index) measures trend strength
# Too high = strong trend (risky reversal), too low = choppy/no trend
USE_ADX_FILTER = True           # Enable ADX-based filtering
ADX_PERIOD = 14                 # Standard ADX period
MAX_ADX = 45.0                  # v49: Skip if ADX > 45 (was 35) - strong trends are best momentum setups, don't filter them
MIN_ADX = 10.0                  # v40: Skip if ADX < 10 (was 12) - wider range for more trades

# IMPROVEMENT #2: Time-Based Filters (avoid open/close volatility) - v6 OPTIMIZED
# First 30 minutes: High volatility, wide spreads, false breakouts
# Last 60 minutes: Reduced from 90 to get 30 more mins of trading
USE_TIME_FILTERS = True         # Enable time-based entry filters
NO_TRADE_FIRST_MINUTES = 20     # Skip first 20 minutes after open (9:30-9:50 AM ET) - relaxed from 30 (2026-01-29)
NO_TRADE_LAST_MINUTES = 30      # v49: Skip last 30 mins (was 60) - opens power hour trading, EOD gradual reduction starts at 3:25 PM

# IMPROVEMENT #4: Opening Range Breakout Filter (ORB)
# Research shows 88% of daily high/low is set by 10:30 AM. Strategies using Opening Range
# achieve 2.4+ Sharpe ratios. After 10:00 AM, require price to break above OR high for longs.
# This prevents entries into weak stocks that can't hold above their morning range.
USE_OPENING_RANGE_FILTER = True          # Enable Opening Range breakout requirement
OPENING_RANGE_MINUTES = 30               # OR period: 9:30-10:00 AM (first 30 mins)
OR_BREAKOUT_BUFFER_PCT = 0.0005          # v49: Require 0.05% above OR high (was 0.1%) - looser ORB, still confirms breakout
OR_MIN_RANGE_PCT = 0.003                 # Skip if OR range < 0.3% (too tight = no conviction)
OR_MAX_RANGE_PCT = 0.05                  # Skip if OR range > 5% (too wide = high risk)
OR_REQUIRE_VWAP_ABOVE_MID = True         # Additional filter: VWAP should be above OR midpoint

# IMPROVEMENT #5: Multi-Timeframe Confluence (5-Min EMA Confirmation)
# Using higher timeframe (5-min) to confirm 1-min signals reduces false breakouts.
# Requires 5-min EMA9 > EMA20 (uptrend on higher TF) for long entries.
# This filters out noise trades where 1-min shows momentum but 5-min doesn't confirm.
USE_5MIN_CONFLUENCE = True              # Enable 5-min timeframe confirmation
MTF_EMA_FAST = 9                        # Fast EMA on 5-min chart
MTF_EMA_SLOW = 20                       # Slow EMA on 5-min chart
MTF_MIN_EMA_SEPARATION_PCT = 0.001      # Require 0.1% EMA separation on 5-min (less strict than 1-min)
MTF_PRICE_ABOVE_5M_VWAP = False         # Optional: require price above 5-min VWAP (disabled - redundant)

# IMPROVEMENT #6: Daily Trend Context (v45)
# Require stock to be above its 20-day SMA before entering long.
# Prevents buying into daily downtrends that have a random 1-min bounce.
# Computed from existing minute bars (resampled to daily) — zero extra API calls.
USE_DAILY_TREND_FILTER = False          # v45: Disabled - cut 59% of trades without improving WR in backtest
DAILY_SMA_PERIOD = 20                   # 20-day simple moving average
DAILY_SMA_BUFFER_PCT = 0.005            # Allow 0.5% below SMA (minor tolerance for mean reversion)

# IMPROVEMENT #7: 5-Min Structure Check (v45)
# Beyond EMA alignment, require structural uptrend on 5-min chart.
# Checks that recent 5-min lows are rising (higher lows = uptrend structure).
USE_5MIN_STRUCTURE_FILTER = False       # v45: Disabled - too restrictive, filtered good trades equally in backtest
MTF_HIGHER_LOWS_COUNT = 2              # Need at least 2 of last 3 5-min lows to be rising

# IMPROVEMENT #8: Volume Confirmation on Entry Bar (v45)
# Require the most recent 1-min bar to have above-average volume.
# Low-volume breakouts are lower probability and more likely to fade.
USE_VOLUME_CONFIRMATION = False         # v45: Disabled - didn't improve quality in backtest
VOLUME_CONFIRM_MULT = 0.8              # Min 0.8x average recent bar volume (not too strict)

# IMPROVEMENT #3: Dynamic Position Sizing (Volatility-Based)
# Reduce size in high volatility, increase in low volatility
# This is ADDITIONAL to existing VOL_ADJUSTED_SIZING - more granular thresholds
USE_DYNAMIC_VOL_SIZING = True   # Enable dynamic volatility sizing
VOL_REGIME_LOW_THRESHOLD = 0.8  # ATR < 80% of MA = low volatility
VOL_REGIME_HIGH_THRESHOLD = 1.3 # ATR > 130% of MA = high volatility
SIZE_MULT_LOW_VOL = 1.25        # 25% larger positions in low volatility
SIZE_MULT_NORMAL_VOL = 1.00     # Normal sizing
SIZE_MULT_HIGH_VOL = 0.60       # 40% smaller positions in high volatility

# Volatility Regime Detection
ATR_MA_PERIOD = 20              # Moving average period for ATR regime detection
HIGH_VOL_ATR_THRESHOLD = 1.5    # ATR > 1.5x MA = high volatility regime
LOW_VOL_ATR_THRESHOLD = 0.7     # ATR < 0.7x MA = low volatility regime

# Position Sizing Optimization
VOL_ADJUSTED_SIZING = True      # Reduce size in high volatility, increase in low volatility
HIGH_VOL_SIZE_MULTIPLIER = 0.75 # 75% of normal size in high volatility
LOW_VOL_SIZE_MULTIPLIER = 1.25  # 125% of normal size in low volatility (capped by limits)

# Dynamic Stop Adjustment
# NOTE: Already implemented via ATR-based stops which adapt to current volatility
# ATR is recalculated on each scan, so stops naturally adjust as volatility changes
# Bracket orders use the latest ATR at entry time (no mid-flight adjustment needed)
DYNAMIC_STOPS = True            # Adjust stops based on intraday volatility changes
STOP_WIDEN_THRESHOLD = 1.3      # Widen stop if ATR increases by 30%+
STOP_TIGHTEN_THRESHOLD = 0.8    # Tighten stop if ATR decreases by 20%+

# Market Regime Detection
MARKET_TREND_SYMBOLS = ["SPY", "QQQ"]  # Symbols to monitor for market health
MIN_MARKET_RVOL = 0.4           # Pause if market-wide volume < 40% of normal (holiday/half-day)
MAX_MARKET_ATR_SPIKE = 2.0      # Pause if SPY/QQQ ATR > 2x normal (crisis mode)

# ============================================================
# NEWS SENTIMENT FILTERING (AI Phase 1)
# ============================================================
# Filter out stocks with bearish news, boost stocks with bullish catalysts
# Uses keyword-based sentiment analysis on Polygon news headlines

ENABLE_NEWS_FILTER = True       # Enable news sentiment filtering
NEWS_CACHE_MINUTES = 5          # Cache news for 5 minutes to reduce API calls
NEWS_LOOKBACK_HOURS = 24        # Look at news from last 24 hours
NEWS_MAX_ARTICLES = 10          # Max articles to analyze per symbol

# News sentiment thresholds
NEWS_BLOCK_ON_BEARISH = True    # Block entry if bearish news detected
NEWS_BOOST_ON_BULLISH = True    # Increase confidence on bullish news

# News age decay - reduce impact of stale news
NEWS_FULL_IMPACT_HOURS = 2      # News < 2 hours old has full impact
NEWS_NO_IMPACT_HOURS = 6        # News > 6 hours old doesn't block trades
# Between 2-6 hours, bearish news only warns but doesn't block

# Bearish keywords - avoid stocks with these in headlines
# NOTE: Use specific phrases to avoid false positives (e.g. "stock offering" not just "offering")
BEARISH_KEYWORDS = [
    # Legal/Regulatory
    "lawsuit", "sued", "litigation", "fraud", "investigation", "sec charges",
    "doj probe", "subpoena", "indictment", "settlement", "class action",
    # Financial Distress
    "bankruptcy", "default", "downgrade", "debt crisis", "layoffs", "restructuring",
    "going concern", "liquidity crisis", "cash burn", "insolvency",
    # Missed Expectations
    "misses estimates", "missed expectations", "disappoints", "below expectations", "guidance cut",
    "revenue miss", "earnings miss", "warns investors", "profit warning", "shortfall",
    # Dilution/Offerings (more specific to avoid false positives)
    "stock dilution", "stock offering", "secondary offering", "shelf registration", "stock sale",
    "equity raise", "convertible offering", "warrant exercise", "share dilution",
    # Negative Events
    "recall", "cyberattack", "data breach", "hack", "outage", "system failure",
    "accident", "contamination", "plant shutdown",
    # Analyst Downgrades
    "downgraded", "sell rating", "underweight", "price target cut",
    # Management Issues
    "ceo departs", "cfo resigns", "executive leaves", "leadership change",
    "accounting error", "restatement", "audit concern",
]

# Bullish keywords - boost confidence for stocks with these
BULLISH_KEYWORDS = [
    # Beats/Positive Results
    "beat", "beats", "exceeded", "surpassed", "record", "strong",
    "blowout", "crushes", "tops estimates", "raises guidance",
    # Upgrades
    "upgrade", "buy rating", "outperform", "overweight", "price target raised",
    # Growth Catalysts
    "partnership", "contract", "deal", "acquisition", "merger", "buyout",
    "expansion", "growth", "new product", "launch", "breakthrough",
    # Positive News
    "fda approval", "approved", "clearance", "patent", "innovation",
    "award", "wins", "selected", "chosen", "exclusive",
    # Financial Strength
    "dividend increase", "buyback", "share repurchase", "cash flow",
    "margin expansion", "profitability", "debt paydown",
]

# Great keywords - strong bullish catalyst (position size boost)
GREAT_KEYWORDS = [
    "fda approval", "fda approved", "breakthrough designation",
    "acquisition", "merger", "buyout", "takeover",
    "blowout earnings", "record revenue", "record profit",
    "major contract", "billion dollar", "multi-year deal",
    "stock split", "special dividend", "massive buyback",
]

# ============================================================
# ML SIGNAL SCORING (AI Phase 2)
# ============================================================
# Score setups 0-100 based on multiple factors
# Higher scores = higher probability of success

ENABLE_SIGNAL_SCORING = True    # Enable ML-based signal scoring
MIN_SIGNAL_SCORE = 50           # Minimum score to take trade (0-100) - Loosened for more trades
SCORE_BOOST_THRESHOLD = 75      # Score above this gets position size boost
SCORE_REDUCE_THRESHOLD = 65     # Score below this gets reduced size

# Score boosting for position sizing
SCORE_BOOST_MULTIPLIER = 1.25   # 25% larger position for high-score setups
SCORE_REDUCE_MULTIPLIER = 0.75  # 25% smaller position for low-score setups

# Feature logging for future ML training
LOG_SIGNAL_FEATURES = True      # Log features + outcomes for ML training
SIGNAL_FEATURES_LOG = str(DATA_DIR / "signal_features.jsonl")  # Feature log file

# Feature weights for scoring - MOMENTUM_HEAVY config (optimized via backtest)
# Backtest results: 58.3% win rate, 2.00 profit factor at MIN_SCORE=60
FEATURE_WEIGHTS = {
    "rvol": 15,           # Relative volume (0-15 points)
    "vwap_distance": 10,  # Distance above VWAP (0-10 points) - reduced
    "ema_separation": 15, # EMA crossover strength (0-15 points) - increased
    "adx": 10,            # Trend strength in sweet spot (0-10 points) - reduced
    "spread": 5,          # Tight spread (0-5 points) - reduced
    "momentum": 20,       # Recent price momentum (0-20 points) - DOUBLED (key predictor)
    "time_of_day": 10,    # Optimal trading hours (0-10 points)
    "news_sentiment": 15, # News catalyst (0-15 points)
}
# Total possible: 100 points


# ============================================================
# MARKET REGIME DETECTION (AI Phase 3)
# ============================================================
ENABLE_REGIME_DETECTION = True   # Enable market regime detection
REGIME_CHECK_INTERVAL = 300      # Check regime every 5 minutes (seconds)
REGIME_CACHE_SECONDS = 60        # Cache regime result for 60 seconds

# SPY/QQQ trend detection
REGIME_SPY_SYMBOL = "SPY"        # Market proxy for regime detection
REGIME_QQQ_SYMBOL = "QQQ"        # Tech sector proxy
REGIME_LOOKBACK_BARS = 20        # Bars to analyze for regime

# VIXY thresholds (volatility gauge) - Using VIXY directly, NOT converting to VIX
# VIXY tracks VIX futures (not spot VIX), so we use VIXY-specific thresholds
# Based on historical VIXY behavior:
#   VIXY < 20 = Low volatility environment
#   VIXY 20-30 = Normal volatility
#   VIXY 30-40 = Elevated volatility (reduce size)
#   VIXY > 40 = High volatility (pause trading)
REGIME_VIXY_CAUTION = 30.0       # VIXY above 30 = reduce size (elevated vol)
REGIME_VIXY_PAUSE = 40.0         # VIXY above 40 = pause trading (high vol)
REGIME_VIXY_LOW = 20.0           # VIXY below 20 = low volatility (can increase size)
USE_VIXY_DIRECT = True           # Use VIXY price directly (no conversion to VIX)
VIXY_SYMBOL = "VIXY"             # ProShares VIX Short-Term Futures ETF

# Legacy aliases for backward compatibility
REGIME_VIX_CAUTION = REGIME_VIXY_CAUTION
REGIME_VIX_PAUSE = REGIME_VIXY_PAUSE
USE_VIX_PROXY = USE_VIXY_DIRECT
VIX_PROXY_SYMBOL = VIXY_SYMBOL

# Regime classifications
REGIME_TRENDING_UP = "TRENDING_UP"
REGIME_TRENDING_DOWN = "TRENDING_DOWN"
REGIME_CHOPPY = "CHOPPY"
REGIME_VOLATILE = "VOLATILE"
REGIME_CALM = "CALM"

# Regime-based adjustments
REGIME_ADJUSTMENTS = {
    "TRENDING_UP": {
        "size_mult": 1.0,        # Full size
        "min_score": 45,         # Lowered for more trades (was 50)
        "tp1_mult": 1.0,         # Normal targets
        "tp2_mult": 1.0,
        "allow_trading": True,
    },
    "TRENDING_DOWN": {
        "size_mult": 0.5,        # Half size - counter-trend
        "min_score": 60,         # Lowered for more trades (was 75)
        "tp1_mult": 0.75,        # Quicker profits
        "tp2_mult": 0.75,
        "allow_trading": True,   # Still trade but carefully
    },
    "CHOPPY": {
        "size_mult": 0.5,        # Reduced size
        "min_score": 55,         # Lowered for more trades (was 65)
        "tp1_mult": 0.75,        # Tighter targets
        "tp2_mult": 0.75,
        "allow_trading": True,
    },
    "VOLATILE": {
        "size_mult": 0.25,       # Quarter size
        "min_score": 65,         # Lowered for more trades (was 80)
        "tp1_mult": 0.5,         # Very quick profits
        "tp2_mult": 0.5,
        "allow_trading": True,   # Trade with extreme caution
    },
    "CALM": {
        "size_mult": 1.25,       # Slightly larger - low risk
        "min_score": 40,         # Lowered for more trades (was 45)
        "tp1_mult": 1.0,         # Normal targets
        "tp2_mult": 1.25,        # Let winners run more
        "allow_trading": True,
    },
}

# Trend detection thresholds
REGIME_TREND_THRESHOLD = 0.3     # % above/below VWAP to consider trending
REGIME_CHOP_THRESHOLD = 0.15     # % range for choppy detection
REGIME_VOL_EXPANSION = 1.5       # ATR ratio for volatile regime

# IMPROVEMENT #6: Enhanced 4-State Regime Model (v41)
# Cleaner quadrant approach: TREND (bull/bear) x VOLATILITY (low/high)
# Research shows HMM regime detection achieved 98% return in 2008 vs -38% for SPY
# This simplified model provides clearer position sizing rules without complexity
USE_4STATE_REGIME = True          # Enable enhanced 4-state regime model

# 4-State definitions (overlays existing regime detection)
# Each state has specific position sizing and stop adjustments
REGIME_4STATE_ADJUSTMENTS = {
    "BULL_LOW_VOL": {
        "size_mult": 1.25,       # Optimal conditions - increase size
        "stop_mult": 1.0,        # Normal stops
        "tp_mult": 1.25,         # Let winners run
        "description": "Best trading conditions - full size, normal stops"
    },
    "BULL_HIGH_VOL": {
        "size_mult": 0.75,       # High vol = smaller size
        "stop_mult": 1.3,        # Wider stops to avoid noise
        "tp_mult": 1.0,          # Normal targets (vol can spike in your favor)
        "description": "Trending but volatile - reduced size, wider stops"
    },
    "BEAR_LOW_VOL": {
        "size_mult": 0.5,        # Counter-trend = smaller size
        "stop_mult": 0.8,        # Tighter stops (mean reversion expected)
        "tp_mult": 0.75,         # Take profits quickly
        "description": "Counter-trend, low vol - small size, tight stops, quick profits"
    },
    "BEAR_HIGH_VOL": {
        "size_mult": 0.25,       # Worst conditions - minimal size
        "stop_mult": 1.5,        # Very wide stops (high noise)
        "tp_mult": 0.5,          # Take any profits quickly
        "description": "Danger zone - minimal size, wide stops, quick exits"
    },
}

# 4-State thresholds
REGIME_4STATE_VOL_THRESHOLD = 1.2   # ATR ratio > 1.2 = HIGH_VOL, else LOW_VOL
REGIME_4STATE_TREND_THRESHOLD = 0.2  # SPY vs VWAP > 0.2% = BULL, else BEAR

# ============================================================
# POLYMARKET PREDICTION MARKET SENTIMENT (AI Phase 4)
# ============================================================
ENABLE_POLYMARKET_SENTIMENT = True   # Enable Polymarket integration
POLYMARKET_CACHE_SECONDS = 300       # Cache sentiment for 5 minutes

# Polymarket-based position sizing multipliers
# Applied on top of regime-based sizing
POLYMARKET_SIZE_ADJUSTMENTS = {
    "LOW": 1.0,       # Normal sizing
    "MEDIUM": 0.85,   # Slightly reduced
    "HIGH": 0.65,     # Significantly reduced
    "EXTREME": 0.40,  # Major reduction
}

# Polymarket risk thresholds for logging/alerts
POLYMARKET_RECESSION_WARNING = 0.30  # Log warning if recession prob > 30%
POLYMARKET_RECESSION_CAUTION = 0.50  # Major caution if recession prob > 50%


# ============================================================
# VALIDATION
# ============================================================

if not ALPACA_API_KEY or not ALPACA_SECRET_KEY:
    raise RuntimeError("Missing ALPACA_API_KEY / ALPACA_SECRET_KEY")
if not POLYGON_API_KEY:
    raise RuntimeError("Missing POLYGON_API_KEY")

# PRODUCTION SAFETY: Live trading validation
is_paper_trading = "paper" in ALPACA_TRADING_BASE_URL.lower()

if LIVE_TRADING_ENABLED:
    if is_paper_trading:
        raise RuntimeError(
            "LIVE_TRADING=1 but ALPACA_BASE_URL points to paper trading. "
            "Set ALPACA_BASE_URL to https://api.alpaca.markets for live trading."
        )
    if LIVE_TRADING_CONFIRMATION != "YES":
        raise RuntimeError(
            "LIVE_TRADING=1 requires I_UNDERSTAND_LIVE_TRADING=YES environment variable. "
            "This bot will trade REAL MONEY. Ensure you understand the risks."
        )
    print("\n" + "="*80)
    print("⚠️  LIVE TRADING MODE ENABLED - REAL MONEY AT RISK ⚠️")
    print("="*80)
    print(f"Base URL: {ALPACA_TRADING_BASE_URL}")
    print(f"Confirmation: {LIVE_TRADING_CONFIRMATION}")
    print("="*80 + "\n")
else:
    if not is_paper_trading:
        raise RuntimeError(
            f"ALPACA_BASE_URL ({ALPACA_TRADING_BASE_URL}) appears to be LIVE but LIVE_TRADING=0. "
            "Set LIVE_TRADING=1 and I_UNDERSTAND_LIVE_TRADING=YES to enable live trading, "
            "or change ALPACA_BASE_URL to https://paper-api.alpaca.markets for paper trading."
        )
    print(f"\n[OK] Paper Trading Mode - Base URL: {ALPACA_TRADING_BASE_URL}\n")


# ============================================================
# LOGGING SETUP
# ============================================================

from logging.handlers import RotatingFileHandler

# Console handler
console_handler = logging.StreamHandler()
console_handler.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))
console_handler.setFormatter(logging.Formatter('[%(asctime)s] %(levelname)s: %(message)s', datefmt='%H:%M:%S'))

# File handler with rotation (prevent unbounded growth)
file_handler = RotatingFileHandler(
    str(LOGS_DIR / "momentum_bot.log"),
    maxBytes=MAX_LOG_SIZE_MB * 1024 * 1024,
    backupCount=MAX_LOG_BACKUPS
)
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(logging.Formatter('[%(asctime)s] %(levelname)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S'))

# Configure root logger
logger = logging.getLogger("MomentumBot")
logger.setLevel(logging.DEBUG)
logger.addHandler(console_handler)
logger.addHandler(file_handler)


# ============================================================
# UTILITIES
# ============================================================

def now_et() -> dt.datetime:
    return dt.datetime.now(tz=ET)

def iso(dtobj: dt.datetime) -> str:
    return dtobj.isoformat()

def from_iso(s: str) -> dt.datetime:
    return dt.datetime.fromisoformat(s)

# ------------------------------------------------------------
# Ownership classification by client_order_id prefix
# ------------------------------------------------------------
# Used by EOD / reconcile routines to figure out which sleeve opened a
# broker position. Needed because dynamic-universe positions can fall out
# of position_manager.positions on restart (see reconcile_broker_state) —
# the broker's order history is the authoritative source of "who owns
# this symbol."
def _classify_order_owner(client_order_id: Optional[str]) -> str:
    """
    Return sleeve name for a client_order_id, or "UNKNOWN" if it doesn't
    match any known prefix pattern.

    Known prefixes:
      ENG_SIMPLE_  → SIMPLE   (engine adapter)
      ENG_TREND_   → TREND    (engine adapter)
      ENG_XASSET_  → XASSET   (engine adapter)
      TBOT_        → TREND    (standalone trend_bot)
      dir_         → DIRECTIONAL (standalone directional_bot)
      RECONCILE_   → UNKNOWN  (synthetic entries created by reconciler)
      "{sym}_scalp_..." / "{sym}_runner_..." → SIMPLE (standalone simple_bot)
    """
    cid = client_order_id or ""
    if cid.startswith("ENG_SIMPLE_"):
        return "SIMPLE"
    if cid.startswith(("ENG_TREND_", "TBOT_")):
        return "TREND"
    if cid.startswith("ENG_XASSET_"):
        return "XASSET"
    if cid.startswith("dir_"):
        return "DIRECTIONAL"
    if cid.startswith("RECONCILE_"):
        return "UNKNOWN"
    # Standalone simple_bot pattern: "{SYMBOL}_scalp_..." / "{SYMBOL}_runner_..."
    if "_scalp_" in cid or "_runner_" in cid:
        return "SIMPLE"
    return "UNKNOWN"


def generate_client_order_id(symbol: str, bracket_type: str, date_str: str) -> str:
    """
    Generate unique client_order_id for order submission.

    Format: {symbol}_{bracket_type}_{date}_{timestamp}_{random}
    Example: AAPL_scalp_20250125_093215_a3f9

    IMPORTANT: Each order submission needs a UNIQUE client_order_id.
    The previous deterministic approach caused 422 errors when re-entering
    a symbol on the same day (e.g., after a timeout cancellation).
    """
    # Use timestamp (HHMMSS) + random suffix to ensure uniqueness
    timestamp = dt.datetime.now().strftime("%H%M%S")
    random_suffix = hashlib.sha256(f"{symbol}_{time.time()}_{os.getpid()}".encode()).hexdigest()[:4]

    return f"{symbol}_{bracket_type}_{date_str}_{timestamp}_{random_suffix}"

def calculate_commissions(qty: int, fill_price: float, side: str = "buy") -> dict:
    """
    MODERATE FIX: Calculate realistic commission and fee costs for accurate PnL.

    Alpaca commission structure (2025):
    - Commission-free trading (most accounts)
    - SEC fees: $27.80 per $1M of principal (sells only)
    - FINRA TAF: $0.000166 per share (sells only, capped at $7.27)

    Args:
        qty: Number of shares
        fill_price: Fill price per share
        side: "buy" or "sell"

    Returns:
        dict with commission breakdown: {
            "commission": float,
            "sec_fee": float,
            "taf_fee": float,
            "total_cost": float
        }
    """
    commission = max(COMMISSION_PER_SHARE * qty, COMMISSION_MIN)
    sec_fee = 0.0
    taf_fee = 0.0

    if side == "sell":
        # SEC fee applies to sell side only
        principal = qty * fill_price
        sec_fee = principal * SEC_FEE_RATE

        # TAF fee applies to sell side only (capped)
        taf_fee = min(qty * TAF_FEE_RATE, TAF_FEE_MAX)

    total_cost = commission + sec_fee + taf_fee

    return {
        "commission": round(commission, 4),
        "sec_fee": round(sec_fee, 4),
        "taf_fee": round(taf_fee, 4),
        "total_cost": round(total_cost, 4)
    }

def atomic_write_json(file_path: str, data: dict, indent: int = 2):
    """
    Atomically write JSON to file using write-to-temp + os.replace pattern.

    This prevents state corruption on crash/interrupt:
    - Writes to temporary file first
    - Only replaces original after successful write
    - os.replace() is atomic on both Windows and Unix

    Args:
        file_path: Destination file path
        data: Dictionary to serialize as JSON
        indent: JSON indentation (default 2)
    """
    tmp_path = f"{file_path}.tmp"
    try:
        # Write to temporary file
        with open(tmp_path, "w") as f:
            json.dump(data, f, indent=indent)
            f.flush()  # Ensure data is written to disk
            os.fsync(f.fileno())  # Force OS to write to disk

        # Atomic replace (works on Windows and Unix)
        os.replace(tmp_path, file_path)

    except Exception as e:
        # Clean up temp file on error
        if os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except:
                pass
        raise e


# ============================================================
# ALERTING SYSTEM (Level 3)
# ============================================================

class Alerter:
    """
    Multi-channel alerting for unattended operation.

    Channels:
    - Slack (webhook)
    - Email (SMTP)

    Alert Levels:
    - INFO: Informational only
    - WARNING: Requires attention
    - CRITICAL: Immediate action required
    """

    def __init__(self):
        self.slack_enabled = ENABLE_SLACK_ALERTS and SLACK_WEBHOOK_URL
        self.email_enabled = ENABLE_EMAIL_ALERTS and ALERT_EMAIL_TO

        if self.slack_enabled:
            logger.info(f"[ALERTER] Slack alerts ENABLED | webhook={SLACK_WEBHOOK_URL[:50]}...")
        if self.email_enabled:
            logger.info(f"[ALERTER] Email alerts ENABLED | to={ALERT_EMAIL_TO}")

        if not (self.slack_enabled or self.email_enabled):
            logger.warning("[ALERTER] NO ALERTS CONFIGURED - unattended operation not recommended")

    def send_alert(self, level: str, title: str, message: str, context: dict = None):
        """
        Send alert via all enabled channels.

        Args:
            level: "INFO", "WARNING", or "CRITICAL"
            title: Alert title/subject
            message: Alert message body
            context: Optional dict with additional context
        """
        # Add timestamp and level to message
        timestamp = now_et().strftime("%Y-%m-%d %H:%M:%S ET")
        full_message = f"[{level}] {timestamp}\n\n{message}"

        if context:
            full_message += "\n\nContext:\n"
            for key, value in context.items():
                full_message += f"  {key}: {value}\n"

        # Send to all enabled channels
        if self.slack_enabled:
            self._send_slack(level, title, full_message)
        if self.email_enabled:
            self._send_email(level, title, full_message)

        # Always log locally
        if level == "CRITICAL":
            logger.error(f"[ALERT] {title}: {message}")
        elif level == "WARNING":
            logger.warning(f"[ALERT] {title}: {message}")
        else:
            logger.info(f"[ALERT] {title}: {message}")

    def _send_slack(self, level: str, title: str, message: str):
        """Send Slack webhook alert."""
        try:
            # Color coding
            color = {
                "INFO": "#36a64f",      # Green
                "WARNING": "#ff9900",   # Orange
                "CRITICAL": "#ff0000"   # Red
            }.get(level, "#808080")

            payload = {
                "attachments": [{
                    "color": color,
                    "title": title,
                    "text": message,
                    "footer": "Momentum Bot",
                    "ts": int(time.time())
                }]
            }

            response = requests.post(SLACK_WEBHOOK_URL, json=payload, timeout=5)
            response.raise_for_status()
            logger.debug(f"[ALERTER] Slack alert sent: {title}")

        except Exception as e:
            logger.error(f"[ALERTER] Failed to send Slack alert: {e}")

    def _send_email(self, level: str, title: str, message: str):
        """Send email alert via SMTP."""
        try:
            msg = MIMEMultipart()
            msg['From'] = ALERT_EMAIL_FROM
            msg['To'] = ALERT_EMAIL_TO
            msg['Subject'] = f"[{level}] Momentum Bot: {title}"

            msg.attach(MIMEText(message, 'plain'))

            with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
                server.starttls()
                if SMTP_USERNAME and SMTP_PASSWORD:
                    server.login(SMTP_USERNAME, SMTP_PASSWORD)
                server.send_message(msg)

            logger.debug(f"[ALERTER] Email alert sent: {title}")

        except Exception as e:
            logger.error(f"[ALERTER] Failed to send email alert: {e}")


# ============================================================
# WEBSOCKET TRADE UPDATES STREAM (Level 3)
# ============================================================

class TradeUpdatesStream:
    """
    Alpaca WebSocket stream for real-time trade updates.

    Events:
    - new: Order submitted
    - partial_fill: Partial fill occurred
    - fill: Order fully filled
    - canceled: Order cancelled
    - rejected: Order rejected
    - expired: Order expired
    """

    # Reconnection circuit breaker settings
    MAX_RECONNECT_ATTEMPTS = 5          # Max consecutive reconnect attempts before alerting
    RECONNECT_RESET_SECONDS = 300       # Reset reconnect counter after 5 minutes of stable connection
    RECONNECT_BACKOFF_BASE = 5          # Base seconds between reconnects
    RECONNECT_BACKOFF_MAX = 60          # Max seconds between reconnects (exponential backoff cap)

    def __init__(self, trade_manager, trade_journal, position_manager, alerter):
        self.trade_manager = trade_manager
        self.trade_journal = trade_journal
        self.position_manager = position_manager
        self.alerter = alerter
        self.ws = None
        self.running = False
        self.thread = None

        # Reconnection tracking
        self.reconnect_attempts = 0
        self.last_successful_connect = None
        self.alerted_on_failures = False

        # Determine WebSocket URL based on trading mode
        self.ws_url = ALPACA_WS_URL_LIVE if LIVE_TRADING_ENABLED else ALPACA_WS_URL_PAPER

    def start(self):
        """Start WebSocket stream in background thread."""
        if not ENABLE_TRADE_UPDATES_STREAM:
            logger.info("[TRADE_STREAM] Disabled via config - using polling only")
            return

        self.running = True
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()
        logger.info(f"[TRADE_STREAM] Started | URL={self.ws_url}")

    def stop(self):
        """Stop WebSocket stream."""
        self.running = False
        if self.ws:
            self.ws.close()
        if self.thread:
            self.thread.join(timeout=5)
        logger.info("[TRADE_STREAM] Stopped")

    def _run(self):
        """WebSocket main loop with reconnection circuit breaker."""
        while self.running:
            try:
                self.ws = websocket.WebSocketApp(
                    self.ws_url,
                    on_open=self._on_open,
                    on_message=self._on_message,
                    on_error=self._on_error,
                    on_close=self._on_close
                )
                self.ws.run_forever()

            except Exception as e:
                logger.error(f"[TRADE_STREAM] Error: {e}")

            # Handle reconnection with circuit breaker
            if self.running:
                self.reconnect_attempts += 1

                # Calculate backoff delay (exponential with cap)
                backoff_delay = min(
                    self.RECONNECT_BACKOFF_BASE * (2 ** (self.reconnect_attempts - 1)),
                    self.RECONNECT_BACKOFF_MAX
                )

                logger.warning(f"[TRADE_STREAM] Reconnect attempt {self.reconnect_attempts}/{self.MAX_RECONNECT_ATTEMPTS} "
                              f"in {backoff_delay}s...")

                # Check if we've exceeded max attempts and haven't alerted yet
                if self.reconnect_attempts >= self.MAX_RECONNECT_ATTEMPTS and not self.alerted_on_failures:
                    self.alerted_on_failures = True
                    logger.error(f"[TRADE_STREAM] *** CIRCUIT BREAKER: {self.reconnect_attempts} consecutive reconnect failures ***")
                    self.alerter.send_alert(
                        level="CRITICAL",
                        title="WebSocket Stream Failing",
                        message=f"Trade updates WebSocket has failed to reconnect {self.reconnect_attempts} times.\n"
                               f"Bot is falling back to polling for trade updates.\n"
                               f"Check network connectivity and Alpaca API status.\n"
                               f"WebSocket URL: {self.ws_url}",
                        context={
                            "reconnect_attempts": self.reconnect_attempts,
                            "ws_url": self.ws_url,
                            "last_successful_connect": self.last_successful_connect.isoformat() if self.last_successful_connect else "never"
                        }
                    )

                time.sleep(backoff_delay)

    def _on_open(self, ws):
        """Authenticate and subscribe to trade updates."""
        logger.info("[TRADE_STREAM] WebSocket opened - authenticating...")

        # Reset reconnection tracking on successful open
        if self.reconnect_attempts > 0:
            logger.info(f"[TRADE_STREAM] Connection restored after {self.reconnect_attempts} attempt(s)")
        self.reconnect_attempts = 0
        self.alerted_on_failures = False
        self.last_successful_connect = now_et()

        # Authenticate
        auth_msg = {
            "action": "authenticate",
            "data": {
                "key_id": ALPACA_API_KEY,
                "secret_key": ALPACA_SECRET_KEY
            }
        }
        ws.send(json.dumps(auth_msg))

        # Subscribe to trade updates
        subscribe_msg = {
            "action": "listen",
            "data": {
                "streams": ["trade_updates"]
            }
        }
        ws.send(json.dumps(subscribe_msg))
        logger.info("[TRADE_STREAM] Subscribed to trade_updates")

    def _on_message(self, ws, message):
        """Handle incoming trade update."""
        try:
            data = json.loads(message)

            # Ensure data is a list
            if not isinstance(data, list):
                logger.debug(f"[TRADE_STREAM] Unexpected message format: {message}")
                return

            # Handle authentication response
            if len(data) > 0 and data[0].get("T") == "success" and data[0].get("msg") == "authenticated":
                logger.info("[TRADE_STREAM] Authenticated successfully")
                return

            # Handle subscription confirmations
            if len(data) > 0 and data[0].get("T") == "success":
                logger.debug(f"[TRADE_STREAM] Subscription confirmed")
                return

            # Handle trade update
            for event in data:
                if event.get("T") == "trade_updates":
                    self._handle_trade_update(event)

        except Exception as e:
            logger.error(f"[TRADE_STREAM] Message handling error: {e}")
            logger.debug(f"[TRADE_STREAM] Problematic message: {message}")

    def _handle_trade_update(self, event):
        """
        Handle trade update event and update state machine.

        Event types:
        - new: Order accepted by broker
        - partial_fill: Partial fill occurred
        - fill: Order fully filled
        - canceled: Order cancelled
        - rejected: Order rejected by broker
        - expired: Order expired
        """
        order_data = event.get("order", {})
        symbol = order_data.get("symbol")
        order_id = order_data.get("id")
        client_order_id = order_data.get("client_order_id")
        event_type = event.get("event")
        filled_qty = int(float(order_data.get("filled_qty", 0)))
        filled_avg_price = float(order_data.get("filled_avg_price", 0)) if filled_qty > 0 else None

        logger.info(f"[TRADE_STREAM] {symbol}: {event_type} | order_id={order_id} client_id={client_order_id} filled_qty={filled_qty}")

        # Get intent for this symbol
        intent = self.trade_manager.get_intent(symbol)
        if not intent:
            logger.warning(f"[TRADE_STREAM] {symbol}: No intent found for order {order_id}")
            return

        # Match client_order_id to bracket type
        is_scalp = (client_order_id == intent.scalp_client_order_id)
        is_runner = (client_order_id == intent.runner_client_order_id)

        if not (is_scalp or is_runner):
            logger.warning(f"[TRADE_STREAM] {symbol}: Unknown client_order_id {client_order_id}")
            return

        bracket_type = "scalp" if is_scalp else "runner"

        # Handle different event types
        if event_type == "fill":
            self._handle_fill(symbol, intent, bracket_type, filled_avg_price)

        elif event_type == "partial_fill":
            self._handle_partial_fill(symbol, intent, bracket_type, filled_qty, filled_avg_price)

        elif event_type == "rejected":
            self._handle_rejection(symbol, intent, bracket_type, order_data.get("reject_reason", "unknown"))

        elif event_type == "canceled":
            self._handle_cancellation(symbol, intent, bracket_type)

    def _handle_fill(self, symbol: str, intent, bracket_type: str, fill_price: float):
        """Handle full fill event."""
        logger.info(f"[TRADE_STREAM] {symbol}: {bracket_type} bracket FILLED @ ${fill_price:.2f}")

        # Update intent
        if bracket_type == "scalp":
            self.trade_manager.update_intent(
                symbol,
                scalp_filled=True,
                scalp_fill_price=fill_price
            )
        else:
            self.trade_manager.update_intent(
                symbol,
                runner_filled=True,
                runner_fill_price=fill_price
            )

        # Check if both brackets filled
        intent = self.trade_manager.get_intent(symbol)
        if intent.scalp_filled and intent.runner_filled:
            # Both filled - transition to ACTIVE_EXITS
            self.trade_manager.transition_state(symbol, TradeState.ACTIVE_EXITS)
            logger.info(f"[TRADE_STREAM] {symbol}: BOTH brackets filled -> ACTIVE_EXITS")

        elif intent.scalp_filled or intent.runner_filled:
            # Partial fill - transition to PARTIALLY_FILLED
            self.trade_manager.transition_state(symbol, TradeState.PARTIALLY_FILLED)
            filled_qty = intent.get_filled_qty()
            logger.warning(f"[TRADE_STREAM] {symbol}: PARTIAL FILL -> state=PARTIALLY_FILLED ({filled_qty}/{intent.total_qty} shares)")

            # Alert on partial fill
            self.alerter.send_alert(
                level="WARNING",
                title=f"Partial Fill: {symbol}",
                message=f"{bracket_type} bracket filled ({filled_qty}/{intent.total_qty} shares). Other bracket still pending.",
                context={
                    "symbol": symbol,
                    "bracket_type": bracket_type,
                    "filled_qty": filled_qty,
                    "total_qty": intent.total_qty,
                    "state": intent.state.value
                }
            )

    def _handle_partial_fill(self, symbol: str, intent, bracket_type: str, filled_qty: int, fill_price: float):
        """Handle partial fill event (should not happen with bracket orders, but defensive)."""
        logger.warning(f"[TRADE_STREAM] {symbol}: {bracket_type} PARTIAL FILL | {filled_qty} @ ${fill_price:.2f}")

        # Alert - this is unexpected for bracket orders
        self.alerter.send_alert(
            level="WARNING",
            title=f"Unexpected Partial Fill: {symbol}",
            message=f"{bracket_type} bracket partially filled ({filled_qty} shares) - investigate",
            context={
                "symbol": symbol,
                "bracket_type": bracket_type,
                "filled_qty": filled_qty,
                "fill_price": fill_price
            }
        )

    def _handle_rejection(self, symbol: str, intent, bracket_type: str, reason: str):
        """Handle order rejection."""
        logger.error(f"[TRADE_STREAM] {symbol}: {bracket_type} bracket REJECTED | reason={reason}")

        # Transition to FAILED
        self.trade_manager.transition_state(symbol, TradeState.FAILED)

        # Log to journal
        self.trade_journal.log_exit(symbol, intent, reason=f"REJECTED_{bracket_type}_{reason}", outcome="FAILED")

        # Alert - rejection is critical
        self.alerter.send_alert(
            level="CRITICAL",
            title=f"Order Rejected: {symbol}",
            message=f"{bracket_type} bracket rejected by broker: {reason}",
            context={
                "symbol": symbol,
                "bracket_type": bracket_type,
                "reason": reason,
                "intent_state": intent.state.value
            }
        )

    def _handle_cancellation(self, symbol: str, intent, bracket_type: str):
        """Handle order cancellation."""
        logger.info(f"[TRADE_STREAM] {symbol}: {bracket_type} bracket CANCELLED")

        # Check if this was a timeout cancellation or manual
        if intent.is_entry_timed_out():
            logger.info(f"[TRADE_STREAM] {symbol}: Cancellation due to timeout")

    def _on_error(self, ws, error):
        """Handle WebSocket error."""
        logger.error(f"[TRADE_STREAM] WebSocket error: {error}")

    def _on_close(self, ws, close_status_code, close_msg):
        """Handle WebSocket close."""
        logger.warning(f"[TRADE_STREAM] WebSocket closed | code={close_status_code} msg={close_msg}")
        if self.running:
            # Calculate uptime since last successful connect
            if self.last_successful_connect:
                uptime = now_et() - self.last_successful_connect
                uptime_str = str(uptime).split('.')[0]  # Remove microseconds
                logger.info(f"[TRADE_STREAM] Session uptime was {uptime_str}")
            logger.info("[TRADE_STREAM] Will attempt to reconnect...")


# ============================================================
# TRADE JOURNAL (JSONL METRICS)
# ============================================================

class TradeJournal:
    """
    Comprehensive trade journal with JSONL logging for analytics.

    Logs all critical metrics for post-analysis:
    - Entry/exit prices and timing
    - Fill quality (slippage, spread)
    - Market conditions (RVOL, ATR, snapshot age)
    - Outcome (R-multiple, win/loss)
    - Failure reasons (timeout, partial fill, etc.)
    """

    def __init__(self):
        self._lock = threading.Lock()

    def log_entry(self, symbol: str, intent: 'TradeIntent', market_data: 'MarketData' = None,
                  snapshot_age_sec: float = None, spread_bps: float = None):
        """Log trade entry with setup metrics."""
        try:
            with self._lock:
                entry = {
                    "event": "ENTRY",
                    "timestamp": iso(now_et()),
                    "symbol": symbol,
                    "total_qty": intent.total_qty,
                    "scalp_qty": intent.scalp_qty,
                    "runner_qty": intent.runner_qty,
                    "entry_limit": intent.entry_limit,
                    "stop_price": intent.stop_price,
                    "scalp_tp": intent.scalp_tp_price,
                    "runner_tp": intent.runner_tp_price,
                    "scalp_client_order_id": intent.scalp_client_order_id,
                    "runner_client_order_id": intent.runner_client_order_id,
                    "timeout_at": intent.timeout_at,
                }

                # Add market condition metrics if available
                if market_data:
                    entry["rvol"] = round(market_data.relative_volume, 2)
                    entry["atr"] = round(market_data.atr, 4) if market_data.atr else None
                    entry["spread_bps"] = round(market_data.spread_bps, 2)
                    entry["last_price"] = round(market_data.last_price, 2)

                if snapshot_age_sec is not None:
                    entry["snapshot_age_sec"] = round(snapshot_age_sec, 2)
                if spread_bps is not None:
                    entry["entry_spread_bps"] = round(spread_bps, 2)

                self._append_jsonl(entry)

        except Exception as e:
            logger.debug(f"[JOURNAL] Failed to log entry for {symbol}: {e}")

    def log_exit(self, symbol: str, intent: 'TradeIntent', reason: str, outcome: str):
        """
        Log trade exit with outcome metrics.

        Args:
            symbol: Stock symbol
            intent: TradeIntent with fill data
            reason: Exit reason (TIMEOUT, PARTIAL_FILL, TP_HIT, SL_HIT, etc.)
            outcome: WIN/LOSS/SCRATCH/TIMEOUT
        """
        try:
            with self._lock:
                # Calculate metrics
                filled_qty = intent.get_filled_qty()
                avg_fill_price = intent.get_avg_fill_price()

                r_multiple = None
                pnl_dollars = None
                pnl_net = None  # PnL after commissions

                # MODERATE FIX: Calculate commissions for accurate PnL
                entry_commissions = calculate_commissions(filled_qty, intent.entry_limit, side="buy")
                exit_commissions = calculate_commissions(filled_qty, avg_fill_price, side="sell") if avg_fill_price else None
                total_commissions = entry_commissions["total_cost"] + (exit_commissions["total_cost"] if exit_commissions else 0.0)

                # FIX: Use entry_limit as entry price, not avg_fill_price for both entry and exit
                # avg_fill_price is the EXIT price (from sell order fill)
                # intent.entry_limit is the ENTRY price
                entry_price = intent.entry_limit
                exit_price = avg_fill_price  # This is correct - it's the exit fill price

                if exit_price and entry_price and intent.stop_price and intent.stop_price > 0:
                    risk_per_share = entry_price - intent.stop_price
                    if risk_per_share > 0:
                        # Calculate actual P&L from entry to exit
                        pnl_per_share = exit_price - entry_price  # FIX: Was exit_price - avg_fill_price (always 0!)
                        r_multiple = pnl_per_share / risk_per_share
                        pnl_dollars = pnl_per_share * filled_qty
                        pnl_net = pnl_dollars - total_commissions  # Net PnL after commissions

                exit_entry = {
                    "event": "EXIT",
                    "timestamp": iso(now_et()),
                    "symbol": symbol,
                    "reason": reason,
                    "outcome": outcome,
                    "filled_qty": filled_qty,
                    "intended_qty": intent.total_qty,
                    "entry_price": round(entry_price, 2) if entry_price else None,  # FIX: Added for transparency
                    "exit_price": round(exit_price, 2) if exit_price else None,  # FIX: Renamed from avg_fill_price
                    "stop_price": round(intent.stop_price, 2) if intent.stop_price else None,  # FIX: Added for R calculation verification
                    "scalp_filled": intent.scalp_filled,
                    "runner_filled": intent.runner_filled,
                    "scalp_fill_price": round(intent.scalp_fill_price, 2) if intent.scalp_fill_price else None,
                    "runner_fill_price": round(intent.runner_fill_price, 2) if intent.runner_fill_price else None,
                    "r_multiple": round(r_multiple, 2) if r_multiple else None,
                    "pnl_est": round(pnl_dollars, 2) if pnl_dollars else None,
                    "pnl_net": round(pnl_net, 2) if pnl_net else None,
                    "commissions": round(total_commissions, 4),
                    "entry_commission": round(entry_commissions["total_cost"], 4),
                    "exit_commission": round(exit_commissions["total_cost"], 4) if exit_commissions else None,
                    "sec_fee": round(exit_commissions["sec_fee"], 4) if exit_commissions else 0.0,
                    "taf_fee": round(exit_commissions["taf_fee"], 4) if exit_commissions else 0.0,
                    "entry_time": intent.created_at,
                    "exit_time": intent.closed_at or iso(now_et()),
                }

                self._append_jsonl(exit_entry)

                # EXPECTANCY GUARD: Record exit for per-symbol tracking
                # Detect stopouts from reason or outcome
                is_stopout = ("SL" in reason.upper() or "STOP" in reason.upper() or
                             outcome == "LOSS")
                try:
                    expectancy_guard.record_exit(symbol, is_stopout, r_multiple)
                except Exception:
                    pass  # Don't let expectancy tracking break trade logging

        except Exception as e:
            logger.debug(f"[JOURNAL] Failed to log exit for {symbol}: {e}")

    def _append_jsonl(self, entry: dict):
        """Append entry to JSONL file."""
        try:
            with open(TRADE_JOURNAL_PATH, "a") as f:
                f.write(json.dumps(entry) + "\n")
        except Exception as e:
            logger.debug(f"[JOURNAL] Failed to write JSONL: {e}")

    def get_performance_summary(self, lookback_hours: int = 24) -> dict:
        """
        Phase 3: Generate performance metrics dashboard from trade journal.

        Returns summary of recent trading performance including:
        - Win rate
        - Average R-multiple
        - Total PnL
        - Number of trades
        - Best/worst trades
        """
        try:
            if not os.path.exists(TRADE_JOURNAL_PATH):
                return {"error": "No trade journal found"}

            # Read JSONL file
            trades = []
            cutoff_time = now_et() - dt.timedelta(hours=lookback_hours)

            with open(TRADE_JOURNAL_PATH, "r") as f:
                for line in f:
                    try:
                        entry = json.loads(line.strip())
                        if entry.get("event") == "EXIT":
                            timestamp = from_iso(entry["timestamp"])
                            if timestamp >= cutoff_time:
                                trades.append(entry)
                    except:
                        continue

            if not trades:
                return {"message": f"No trades in last {lookback_hours} hours"}

            # Calculate metrics
            total_trades = len(trades)
            wins = sum(1 for t in trades if t.get("outcome") == "WIN")
            losses = sum(1 for t in trades if t.get("outcome") == "LOSS")
            scratches = sum(1 for t in trades if t.get("outcome") == "SCRATCH")

            win_rate = (wins / total_trades) if total_trades > 0 else 0

            # R-multiple stats
            r_multiples = [t["r_multiple"] for t in trades if t.get("r_multiple") is not None]
            avg_r = sum(r_multiples) / len(r_multiples) if r_multiples else 0
            best_r = max(r_multiples) if r_multiples else 0
            worst_r = min(r_multiples) if r_multiples else 0

            # PnL stats
            pnls = [t["pnl_est"] for t in trades if t.get("pnl_est") is not None]
            total_pnl = sum(pnls) if pnls else 0
            avg_pnl = total_pnl / len(pnls) if pnls else 0

            # Exit reasons breakdown
            timeout_count = sum(1 for t in trades if "TIMEOUT" in t.get("reason", ""))
            tp_count = sum(1 for t in trades if "TP" in t.get("reason", ""))
            sl_count = sum(1 for t in trades if "SL" in t.get("reason", "") or "STOP" in t.get("reason", ""))

            return {
                "lookback_hours": lookback_hours,
                "total_trades": total_trades,
                "wins": wins,
                "losses": losses,
                "scratches": scratches,
                "win_rate": round(win_rate, 3),
                "avg_r_multiple": round(avg_r, 2),
                "best_r": round(best_r, 2),
                "worst_r": round(worst_r, 2),
                "total_pnl_est": round(total_pnl, 2),
                "avg_pnl_per_trade": round(avg_pnl, 2),
                "exit_reasons": {
                    "timeouts": timeout_count,
                    "take_profits": tp_count,
                    "stop_losses": sl_count
                }
            }

        except Exception as e:
            logger.debug(f"[JOURNAL] Failed to generate performance summary: {e}")
            return {"error": str(e)}


trade_journal = TradeJournal()


# ============================================================
# EXPECTANCY PROTECTION GATES
# ============================================================

class ExpectancyGuard:
    """
    Monitors trading performance and pauses trading when edge degrades.

    Gates:
    1. Rolling expectancy check - pause if profit factor drops below threshold
    2. Per-symbol kill switch - block symbols with repeated stopouts
    3. Drawdown circuit breaker - reduce exposure during drawdowns
    4. Event filter - avoid trading around high-risk events

    These don't guarantee profit but prevent slow death from degraded edge.
    """

    # Configuration
    MIN_PROFIT_FACTOR = 1.0          # Pause new trades if PF drops below this
    MIN_TRADES_FOR_PF_CHECK = 10     # Need at least this many trades to evaluate PF
    PF_LOOKBACK_TRADES = 20          # Rolling window for PF calculation
    MAX_SYMBOL_STOPOUTS = 3          # Block symbol after this many consecutive stopouts
    SYMBOL_COOLDOWN_HOURS = 24       # How long to block a symbol
    DRAWDOWN_REDUCE_THRESHOLD = 0.05 # Reduce position size at 5% drawdown
    DRAWDOWN_PAUSE_THRESHOLD = 0.08  # Pause new trades at 8% drawdown

    def __init__(self):
        self._symbol_stopouts: Dict[str, List[float]] = {}  # symbol -> list of stopout timestamps
        self._blocked_symbols: Dict[str, float] = {}  # symbol -> block_until_timestamp
        self._last_pf_check: float = 0
        self._cached_pf: float = 1.0
        self._is_paused: bool = False
        self._pause_reason: str = ""

    def should_allow_entry(self, symbol: str) -> Tuple[bool, str]:
        """
        Check if a new entry should be allowed.

        Returns:
            (allowed: bool, reason: str)
        """
        # Check 1: Is trading paused globally?
        if self._is_paused:
            return False, f"Trading paused: {self._pause_reason}"

        # Check 2: Is this symbol blocked?
        if symbol in self._blocked_symbols:
            block_until = self._blocked_symbols[symbol]
            if time.time() < block_until:
                hours_left = (block_until - time.time()) / 3600
                return False, f"Symbol blocked for {hours_left:.1f}h after {self.MAX_SYMBOL_STOPOUTS} consecutive stopouts"
            else:
                # Cooldown expired, remove block
                del self._blocked_symbols[symbol]
                self._symbol_stopouts.pop(symbol, None)

        # Check 3: Rolling profit factor (checked every 5 minutes to avoid constant recalc)
        if time.time() - self._last_pf_check > 300:
            self._update_profit_factor()

        if self._cached_pf < self.MIN_PROFIT_FACTOR and self._has_enough_trades():
            return False, f"Profit factor {self._cached_pf:.2f} below minimum {self.MIN_PROFIT_FACTOR}"

        return True, "OK"

    def record_exit(self, symbol: str, is_stopout: bool, r_multiple: float = None):
        """
        Record a trade exit for expectancy tracking.

        Args:
            symbol: The stock symbol
            is_stopout: True if this was a stop-loss exit
            r_multiple: The R-multiple of the trade (negative for losses)
        """
        if is_stopout:
            # Track consecutive stopouts per symbol
            if symbol not in self._symbol_stopouts:
                self._symbol_stopouts[symbol] = []

            self._symbol_stopouts[symbol].append(time.time())

            # Check if we should block this symbol
            recent_stopouts = [ts for ts in self._symbol_stopouts[symbol]
                              if time.time() - ts < self.SYMBOL_COOLDOWN_HOURS * 3600]
            self._symbol_stopouts[symbol] = recent_stopouts  # Clean up old entries

            if len(recent_stopouts) >= self.MAX_SYMBOL_STOPOUTS:
                block_until = time.time() + self.SYMBOL_COOLDOWN_HOURS * 3600
                self._blocked_symbols[symbol] = block_until
                logger.warning(f"[EXPECTANCY] {symbol}: BLOCKED for {self.SYMBOL_COOLDOWN_HOURS}h after "
                             f"{len(recent_stopouts)} stopouts in {self.SYMBOL_COOLDOWN_HOURS}h window")
        else:
            # Winning trade resets the stopout counter for this symbol
            if symbol in self._symbol_stopouts:
                self._symbol_stopouts[symbol] = []

    def check_drawdown(self, current_equity: float, peak_equity: float) -> Tuple[float, bool]:
        """
        Check current drawdown and return position size multiplier.

        Returns:
            (size_multiplier: float, should_pause: bool)
        """
        if peak_equity <= 0:
            return 1.0, False

        drawdown = (peak_equity - current_equity) / peak_equity

        if drawdown >= self.DRAWDOWN_PAUSE_THRESHOLD:
            self._is_paused = True
            self._pause_reason = f"Drawdown {drawdown*100:.1f}% exceeds pause threshold {self.DRAWDOWN_PAUSE_THRESHOLD*100:.0f}%"
            logger.warning(f"[EXPECTANCY] {self._pause_reason}")
            return 0.0, True

        if drawdown >= self.DRAWDOWN_REDUCE_THRESHOLD:
            # Linear reduction: 100% at threshold, 50% at pause threshold
            reduction_range = self.DRAWDOWN_PAUSE_THRESHOLD - self.DRAWDOWN_REDUCE_THRESHOLD
            reduction_pct = (drawdown - self.DRAWDOWN_REDUCE_THRESHOLD) / reduction_range
            size_mult = max(0.5, 1.0 - (reduction_pct * 0.5))
            logger.info(f"[EXPECTANCY] Drawdown {drawdown*100:.1f}% - reducing position size to {size_mult*100:.0f}%")
            return size_mult, False

        # Check if we should unpause
        if self._is_paused and drawdown < self.DRAWDOWN_REDUCE_THRESHOLD:
            self._is_paused = False
            self._pause_reason = ""
            logger.info(f"[EXPECTANCY] Drawdown recovered to {drawdown*100:.1f}% - resuming trading")

        return 1.0, False

    def _update_profit_factor(self):
        """Update cached profit factor from recent trades."""
        self._last_pf_check = time.time()

        try:
            # Get recent trades from journal
            summary = trade_journal.get_performance_summary(lookback_hours=48)

            if summary.get("error") or summary.get("total_trades", 0) < self.MIN_TRADES_FOR_PF_CHECK:
                self._cached_pf = 1.0  # Not enough data, assume neutral
                return

            wins = summary.get("wins", 0)
            losses = summary.get("losses", 0)

            # Calculate profit factor from R-multiples if available
            # For now, use a simplified version based on win/loss ratio and average R
            avg_r = summary.get("avg_r_multiple", 0)
            win_rate = summary.get("win_rate", 0.5)

            if losses > 0 and wins > 0:
                # Estimate profit factor: (wins * avg_win) / (losses * avg_loss)
                # Using avg_r as a proxy
                if avg_r > 0:
                    self._cached_pf = 1.0 + avg_r  # Positive expectancy
                else:
                    self._cached_pf = 1.0 / (1.0 - avg_r) if avg_r > -1 else 0.5
            else:
                self._cached_pf = 1.0

            logger.debug(f"[EXPECTANCY] Updated profit factor: {self._cached_pf:.2f} "
                        f"(wins={wins} losses={losses} avg_r={avg_r:.2f})")

        except Exception as e:
            logger.debug(f"[EXPECTANCY] Failed to update profit factor: {e}")
            self._cached_pf = 1.0

    def _has_enough_trades(self) -> bool:
        """Check if we have enough trades to make PF decision."""
        try:
            summary = trade_journal.get_performance_summary(lookback_hours=48)
            return summary.get("total_trades", 0) >= self.MIN_TRADES_FOR_PF_CHECK
        except Exception:
            return False

    def get_status(self) -> dict:
        """Get current expectancy guard status."""
        return {
            "is_paused": self._is_paused,
            "pause_reason": self._pause_reason,
            "cached_pf": round(self._cached_pf, 2),
            "blocked_symbols": list(self._blocked_symbols.keys()),
            "symbols_with_stopouts": {s: len(v) for s, v in self._symbol_stopouts.items() if v}
        }


expectancy_guard = ExpectancyGuard()


# Global alerter instance (initialized early so other components can use it)
alerter = Alerter()

# Global trade updates stream (initialized in MomentumBot.run() after other components)
trade_updates_stream = None


# ============================================================
# HEARTBEAT LOGGER (Level 3 Production)
# ============================================================

class HeartbeatLogger:
    """
    Logs operational heartbeat metrics for monitoring.

    Logs every minute:
    - Bot alive status
    - Session state
    - Equity and PnL
    - Halted status
    - Open orders count
    - Position count
    - Circuit breaker status
    """

    def __init__(self):
        self.last_heartbeat_time = 0.0
        self._lock = threading.Lock()

    def log_heartbeat(self, session: 'MarketSession', halted: bool, halt_reason: Optional[str] = None):
        """Log heartbeat if interval has passed."""
        now = time.time()

        if now - self.last_heartbeat_time < HEARTBEAT_INTERVAL_SEC:
            return

        self.last_heartbeat_time = now

        try:
            with self._lock:
                # Gather metrics
                heartbeat = {
                    "event": "HEARTBEAT",
                    "timestamp": iso(now_et()),
                    "session": session.value if session else "UNKNOWN",
                    "halted": halted,
                    "halt_reason": halt_reason,
                }

                # Add risk manager state
                try:
                    heartbeat["start_equity"] = round(risk_manager.start_equity, 2)
                    heartbeat["current_equity"] = round(risk_manager.current_equity, 2)
                    heartbeat["daily_pnl"] = round(risk_manager.daily_pnl, 2)
                    heartbeat["buying_power"] = round(risk_manager.buying_power, 2)
                except Exception:
                    pass

                # Add position/order counts
                try:
                    heartbeat["positions_count"] = len(position_manager.positions)
                    heartbeat["intents_count"] = len(trade_manager.intents)

                    open_orders = alpaca.get_orders(status="open")
                    heartbeat["open_orders_count"] = len(open_orders)
                except Exception:
                    pass

                # Add circuit breaker state + API health metrics
                try:
                    heartbeat["circuit_breaker_halted"] = circuit_breaker.halted
                    heartbeat["circuit_breaker_reason"] = circuit_breaker.halt_reason
                    # API error count (for alerting on degradation)
                    # Note: circuit breaker tracks all API failures together, not by API name
                    recent_errors = len([t for t in circuit_breaker.api_failures
                                        if time.time() - t < API_FAILURE_WINDOW_SEC])
                    heartbeat["api_errors_recent"] = recent_errors
                except Exception:
                    pass

                # Write to heartbeat log
                with open(HEARTBEAT_LOG_PATH, "a") as f:
                    f.write(json.dumps(heartbeat) + "\n")

                logger.debug(f"[HEARTBEAT] equity=${heartbeat.get('current_equity', 0):.2f} "
                           f"pnl=${heartbeat.get('daily_pnl', 0):+.2f} pos={heartbeat.get('positions_count', 0)} "
                           f"orders={heartbeat.get('open_orders_count', 0)} "
                           f"api_err={heartbeat.get('api_errors_alpaca_1m', 0)}/{heartbeat.get('api_errors_polygon_1m', 0)}")

        except Exception as e:
            logger.debug(f"[HEARTBEAT] Failed to log: {e}")


heartbeat_logger = HeartbeatLogger()


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

    def log_snapshot(self):
        """Log daily snapshot if not yet logged today. Called from main loop."""
        today = now_et().date()
        if self._last_snapshot_date == today:
            return

        with self._lock:
            if self._last_snapshot_date == today:
                return  # Double-check under lock
            try:
                import csv as _csv
                equity = risk_manager.current_equity
                start_eq = risk_manager.start_equity
                daily_pnl = risk_manager.daily_pnl

                positions = alpaca.get_positions()
                positions_value = sum(float(p.get("market_value", 0)) for p in positions)
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
                        risk_manager.halted,
                        risk_manager.daily_trade_count
                    ])

                self._last_snapshot_date = today
                logger.info(f"[EQUITY_SNAPSHOT] {today}: ${equity:.2f} | "
                           f"PnL=${daily_pnl:+.2f} | {num_positions} positions")

            except Exception as e:
                logger.warning(f"[EQUITY_SNAPSHOT] Failed: {e}")

equity_snapshot_logger = EquitySnapshotLogger()


# ============================================================
# SCAN DIAGNOSTICS (Filter Funnel Tracking)
# ============================================================

# All possible rejection reasons from check_long_setup
_REJECTION_COLUMNS = [
    "no_data", "too_early", "too_late", "adx_high", "adx_low",
    "or_filter", "5min_trend", "daily_trend", "5min_structure",
    "bar_volume", "regime_pause", "bearish_news", "gap_up",
    "vwap_distance", "ema_separation", "rvol_low", "price_low",
    "spread_wide", "quote_quality", "clean_candles", "momentum",
    "score_low"
]


class ScanDiagnosticsLogger:
    """
    Aggregates per-scan rejection counts into a daily summary CSV.

    One row per trading day with total scans, total rejections per filter,
    total candidates found, and total trades entered. Gives a clear picture
    of where candidates are dropping out of the funnel.
    """

    def __init__(self):
        self._today = None
        self._daily_scans = 0
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
            logger.debug(f"Created scan diagnostics header at {SCAN_DIAGNOSTICS_PATH}")
        except Exception as e:
            logger.error(f"Failed to create scan diagnostics header: {e}")

    def _flush_day(self, date_str: str):
        """Write the accumulated daily stats to CSV."""
        try:
            import csv as _csv
            row = [
                date_str,
                self._daily_scans,
                self._symbols_scanned_total,
            ]
            for col in _REJECTION_COLUMNS:
                row.append(self._daily_rejections.get(col, 0))
            row.append(self._daily_candidates)
            row.append(self._daily_trades)

            with open(SCAN_DIAGNOSTICS_PATH, "a", newline="", encoding="utf-8") as f:
                w = _csv.writer(f)
                w.writerow(row)

            # Build top rejections summary for log
            top = sorted(self._daily_rejections.items(), key=lambda x: x[1], reverse=True)[:5]
            top_str = ", ".join(f"{k}={v}" for k, v in top)
            logger.info(f"[SCAN_DIAG] Daily summary written: {self._daily_scans} scans, "
                       f"{self._daily_candidates} candidates, {self._daily_trades} trades | "
                       f"Top rejections: {top_str}")
        except Exception as e:
            logger.warning(f"[SCAN_DIAG] Failed to flush daily stats: {e}")

    def record_scan(self, symbols_scanned: int, rejection_counts: dict,
                    candidates_found: int, trade_entered: bool):
        """Record one scan cycle's results."""
        with self._lock:
            today = now_et().date().isoformat()

            # New day? Flush previous day's stats first
            if self._today and self._today != today:
                self._flush_day(self._today)
                self._daily_scans = 0
                self._daily_rejections = {}
                self._daily_candidates = 0
                self._daily_trades = 0
                self._symbols_scanned_total = 0

            self._today = today
            self._daily_scans += 1
            if not hasattr(self, '_symbols_scanned_total'):
                self._symbols_scanned_total = 0
            self._symbols_scanned_total += symbols_scanned
            self._daily_candidates += candidates_found

            for reason, count in rejection_counts.items():
                self._daily_rejections[reason] = self._daily_rejections.get(reason, 0) + count

            if trade_entered:
                self._daily_trades += 1

    def mark_trade_entered(self):
        """Called when a trade is actually entered after scan."""
        with self._lock:
            self._daily_trades += 1

    def flush_if_needed(self):
        """Flush end-of-day stats. Call from shutdown or EOD."""
        with self._lock:
            if self._today and self._daily_scans > 0:
                self._flush_day(self._today)
                self._daily_scans = 0


scan_diagnostics_logger = ScanDiagnosticsLogger()


# ============================================================
# KILL SWITCH (Level 3 Production)
# ============================================================

class KillSwitch:
    """
    Emergency kill switch for production.

    Checks:
    - File-based kill switch (create HALT_TRADING file)
    - Environment variable (set KILL_SWITCH=1)

    When triggered:
    - Cancel all open orders
    - Flatten all positions
    - Write final journal event
    - Halt bot permanently
    """

    def __init__(self):
        self.triggered = False
        self.trigger_reason = None

    def is_triggered(self) -> Tuple[bool, Optional[str]]:
        """Check if kill switch has been triggered."""
        if self.triggered:
            return True, self.trigger_reason

        # Check file-based kill switch
        if os.path.exists(KILL_SWITCH_FILE):
            self.triggered = True
            self.trigger_reason = f"Kill switch file detected: {KILL_SWITCH_FILE}"
            logger.error(f"[KILL_SWITCH] {self.trigger_reason}")
            return True, self.trigger_reason

        # Check environment variable
        if os.getenv(KILL_SWITCH_ENV) == "1":
            self.triggered = True
            self.trigger_reason = f"Kill switch env var set: {KILL_SWITCH_ENV}=1"
            logger.error(f"[KILL_SWITCH] {self.trigger_reason}")
            return True, self.trigger_reason

        return False, None

    def execute_emergency_shutdown(self):
        """
        Execute emergency shutdown procedure.

        SHARED-ACCOUNT SAFETY: Only cancels simple_bot's own orders and flattens
        its own long positions. Preserves directional_bot's safety stops and
        trend_bot's positions. Each bot has its own independent kill switch.
        """
        logger.error("[KILL_SWITCH] !!!! EMERGENCY SHUTDOWN INITIATED !!!!")

        try:
            # Step 1: Cancel only our orders (not directional_bot's or trend_bot's)
            logger.error("[KILL_SWITCH] Step 1: Cancelling simple_bot open orders...")
            try:
                all_orders = alpaca.get_orders(status="open")
                our_orders = [o for o in all_orders
                             if not (o.get("client_order_id") or "").startswith(("dir_", "TBOT_"))]
                for order in our_orders:
                    try:
                        alpaca.cancel_order(order["id"])
                    except Exception:
                        pass
                other_count = len(all_orders) - len(our_orders)
                logger.error(f"[KILL_SWITCH] Cancelled {len(our_orders)} of our order(s)"
                           f"{f' (preserved {other_count} other bot orders)' if other_count else ''}")
            except Exception as e:
                logger.error(f"[KILL_SWITCH] Error cancelling orders: {e}")

            # Step 2: Flatten only our positions (skip trend_bot symbols and short positions)
            logger.error("[KILL_SWITCH] Step 2: Flattening simple_bot positions...")
            try:
                positions = alpaca.list_positions()
                our_positions = [p for p in positions
                                if p["symbol"] not in TREND_BOT_SYMBOLS
                                and p.get("side", "long") != "short"]
                for pos in our_positions:
                    symbol = pos["symbol"]
                    try:
                        result = alpaca.flatten_symbol(symbol)
                        logger.error(f"[KILL_SWITCH] Flattened {symbol}: {result}")
                    except Exception as e:
                        logger.error(f"[KILL_SWITCH] Error flattening {symbol}: {e}")
                other_count = len(positions) - len(our_positions)
                if other_count:
                    logger.error(f"[KILL_SWITCH] Preserved {other_count} other bot position(s)")
            except Exception as e:
                logger.error(f"[KILL_SWITCH] Error fetching positions: {e}")

            # Step 3: Write final journal event
            try:
                with open(TRADE_JOURNAL_PATH, "a") as f:
                    f.write(json.dumps({
                        "event": "KILL_SWITCH_TRIGGERED",
                        "timestamp": iso(now_et()),
                        "reason": self.trigger_reason
                    }) + "\n")
            except Exception as e:
                logger.error(f"[KILL_SWITCH] Error writing journal: {e}")

            logger.error("[KILL_SWITCH] !!!! EMERGENCY SHUTDOWN COMPLETE !!!!")
            logger.error(f"[KILL_SWITCH] Reason: {self.trigger_reason}")

        except Exception as e:
            logger.error(f"[KILL_SWITCH] Error during emergency shutdown: {e}")


kill_switch = KillSwitch()


# ============================================================
# CIRCUIT BREAKER
# ============================================================

class CircuitBreaker:
    """
    Production circuit breaker for API degradation and data staleness.

    Tracks API failures and halts trading when:
    - Too many API failures in short window
    - Alpaca clock unavailable
    - Polygon data unavailable
    - Snapshot data too stale
    """

    def __init__(self):
        self.api_failures: List[float] = []  # Timestamps of API failures
        self.halted = False
        self.halt_reason = None
        self._lock = threading.Lock()

    def record_api_failure(self, api_name: str):
        """Record an API failure and check if circuit should break."""
        with self._lock:
            now = time.time()
            self.api_failures.append(now)

            # Clean old failures outside window
            cutoff = now - API_FAILURE_WINDOW_SEC
            self.api_failures = [ts for ts in self.api_failures if ts >= cutoff]

            # Check if we've exceeded threshold
            if len(self.api_failures) >= MAX_API_FAILURES_PER_MIN:
                if not self.halted:
                    self.halted = True
                    self.halt_reason = f"{api_name} API degradation ({len(self.api_failures)} failures in {API_FAILURE_WINDOW_SEC}s)"
                    logger.error(f"[CIRCUIT_BREAKER] HALTED | {self.halt_reason}")

                    # LEVEL 3: Send critical alert
                    alerter.send_alert(
                        level="CRITICAL",
                        title="Circuit Breaker Tripped",
                        message=f"API failures exceeded threshold. Trading HALTED: {self.halt_reason}",
                        context={
                            "api_name": api_name,
                            "failure_count": len(self.api_failures),
                            "window_seconds": API_FAILURE_WINDOW_SEC,
                            "reason": self.halt_reason
                        }
                    )

    def check_alpaca_clock(self) -> bool:
        """Check if Alpaca clock API is available."""
        try:
            alpaca.get_clock()
            return True
        except Exception as e:
            logger.error(f"[CIRCUIT_BREAKER] Alpaca clock unavailable: {e}")
            if not self.halted:
                self.halted = True
                self.halt_reason = "Alpaca clock API unavailable"

                # LEVEL 3: Send critical alert
                alerter.send_alert(
                    level="CRITICAL",
                    title="Alpaca Clock Unavailable",
                    message=f"Cannot determine market hours - Alpaca clock API unavailable. Trading HALTED.",
                    context={"error": str(e), "reason": self.halt_reason}
                )
            return False

    def check_snapshot_freshness(self, snapshot: dict, symbol: str) -> bool:
        """
        Check if snapshot data is fresh enough for trading decisions.

        Returns True if fresh, False if stale (should skip this symbol).
        """
        if not snapshot:
            logger.warning(f"[CIRCUIT_BREAKER] {symbol}: No snapshot data available")
            return False

        # Check lastTrade timestamp
        last_trade = snapshot.get("lastTrade")
        if not last_trade or "t" not in last_trade:
            logger.warning(f"[CIRCUIT_BREAKER] {symbol}: No lastTrade timestamp in snapshot")
            return False

        # lastTrade.t is in nanoseconds
        last_trade_ns = last_trade["t"]
        last_trade_dt = dt.datetime.fromtimestamp(last_trade_ns / 1e9, tz=dt.timezone.utc).astimezone(ET)
        age_seconds = (now_et() - last_trade_dt).total_seconds()

        if age_seconds > MAX_SNAPSHOT_AGE_SEC:
            logger.warning(f"[CIRCUIT_BREAKER] {symbol}: Snapshot stale | age={age_seconds:.1f}s (max={MAX_SNAPSHOT_AGE_SEC}s)")
            return False

        return True

    def is_halted(self) -> bool:
        """Check if circuit breaker is tripped."""
        return self.halted

    def get_halt_reason(self) -> Optional[str]:
        """Get reason for halt."""
        return self.halt_reason

    def reset(self):
        """Reset circuit breaker (manual intervention required)."""
        with self._lock:
            self.halted = False
            self.halt_reason = None
            self.api_failures.clear()
            logger.info("[CIRCUIT_BREAKER] Reset - trading resumed")


circuit_breaker = CircuitBreaker()


# ============================================================
# ALPACA REST API
# ============================================================

class AlpacaClient:
    """Alpaca REST client with separate trading and data base URLs."""

    def __init__(self):
        self.trading_base = ALPACA_TRADING_BASE_URL
        self.data_base = ALPACA_DATA_BASE_URL
        self.headers = {
            "APCA-API-KEY-ID": ALPACA_API_KEY,
            "APCA-API-SECRET-KEY": ALPACA_SECRET_KEY,
            "Content-Type": "application/json",
        }

    def _request(self, method: str, path: str, base_url: str = None, retries: int = 3, **kwargs):
        """
        Make request to Alpaca API with retry logic and exponential backoff.

        Args:
            method: HTTP method (GET, POST, DELETE, etc.)
            path: API endpoint path
            base_url: Base URL (defaults to trading URL)
            retries: Number of retry attempts for transient errors (default 3)
            **kwargs: Additional arguments for requests.request()

        Returns:
            JSON response dict

        Raises:
            requests.HTTPError: For non-retryable errors or after exhausting retries
        """
        if base_url is None:
            base_url = self.trading_base
        url = f"{base_url}{path}"

        # CRITICAL: Do NOT retry order submissions to prevent duplicate orders
        # Even with client_order_id, safer to fail fast on network errors
        is_order_submission = method == "POST" and "/v2/orders" in path
        max_attempts = 1 if is_order_submission else retries

        last_exception = None
        for attempt in range(max_attempts):
            try:
                response = requests.request(method, url, headers=self.headers, timeout=15, **kwargs)
                response.raise_for_status()
                return response.json() if response.text else {}

            except requests.HTTPError as e:
                status_code = e.response.status_code if e.response else 0

                # Rate limit (429) or server error (5xx) - retry with backoff
                if status_code == 429 or 500 <= status_code < 600:
                    if attempt < max_attempts - 1:
                        # Exponential backoff: 1s, 2s, 4s
                        backoff_sec = 2 ** attempt
                        logger.warning(f"[API] {method} {path} | {status_code} | retry {attempt+1}/{max_attempts} in {backoff_sec}s")
                        time.sleep(backoff_sec)
                        last_exception = e
                        continue
                    else:
                        # Exhausted retries
                        logger.error(f"[API] {method} {path} | {status_code} | exhausted {max_attempts} retries")
                        raise
                else:
                    # Client error (4xx) - don't retry
                    # Try to extract detailed error message from Alpaca response
                    error_detail = "no response"
                    if e.response is not None:
                        try:
                            error_json = e.response.json()
                            error_detail = error_json.get("message", e.response.text)
                        except Exception:
                            error_detail = e.response.text[:500] if e.response.text else "empty response"
                    logger.error(f"[API] {method} {path} | {status_code} | {error_detail}")
                    raise

            except requests.RequestException as e:
                # Network error - retry if not order submission
                if attempt < max_attempts - 1 and not is_order_submission:
                    backoff_sec = 2 ** attempt
                    logger.warning(f"[API] {method} {path} | network error | retry {attempt+1}/{max_attempts} in {backoff_sec}s | {e}")
                    time.sleep(backoff_sec)
                    last_exception = e
                    continue
                else:
                    logger.error(f"[API] {method} {path} | network error after {attempt+1} attempt(s) | {e}")
                    raise

        # Should not reach here, but if we do, raise last exception
        if last_exception:
            raise last_exception

    def get_account(self) -> dict:
        return self._request("GET", "/v2/account")

    def get_positions(self) -> List[dict]:
        return self._request("GET", "/v2/positions")

    def list_positions(self) -> List[dict]:
        """Alias for get_positions() for consistency."""
        return self.get_positions()

    def get_position(self, symbol: str) -> Optional[dict]:
        try:
            return self._request("GET", f"/v2/positions/{symbol}")
        except requests.HTTPError:
            return None

    def get_orders(self, status: str = "open") -> List[dict]:
        return self._request("GET", "/v2/orders", params={"status": status, "limit": 500})

    def submit_order(self, symbol: str, qty: int, side: str, order_type: str = "market",
                    limit_price: float = None, stop_price: float = None,
                    trail_percent: float = None, time_in_force: str = "day",
                    extended_hours: bool = False, order_class: str = None,
                    take_profit: dict = None, stop_loss: dict = None,
                    client_order_id: str = None) -> dict:
        """
        Submit order with support for bracket/OCO orders and idempotency.

        For bracket orders, set order_class="bracket" and provide take_profit/stop_loss dicts:
        - take_profit = {"limit_price": "100.50"}
        - stop_loss = {"stop_price": "95.00", "limit_price": "94.95"} (optional limit for stop-limit)

        For idempotency, provide client_order_id (broker deduplicates on retry).
        """
        payload = {
            "symbol": symbol,
            "qty": str(qty),
            "side": side,
            "type": order_type,
            "time_in_force": time_in_force,
        }

        # Add price parameters (accept float or string, format consistently)
        if limit_price is not None:
            payload["limit_price"] = f"{float(limit_price):.2f}"
        if stop_price is not None:
            payload["stop_price"] = f"{float(stop_price):.2f}"
        if trail_percent is not None:
            payload["trail_percent"] = f"{float(trail_percent):.2f}"

        # Extended hours support
        if extended_hours:
            payload["extended_hours"] = True

        # Bracket/OCO support
        if order_class:
            payload["order_class"] = order_class
        if take_profit:
            payload["take_profit"] = take_profit
        if stop_loss:
            payload["stop_loss"] = stop_loss

        # Idempotency support (CRITICAL for production)
        if client_order_id:
            payload["client_order_id"] = client_order_id

        return self._request("POST", "/v2/orders", json=payload)

    def cancel_order(self, order_id: str):
        return self._request("DELETE", f"/v2/orders/{order_id}")

    def cancel_all_orders(self) -> List[dict]:
        """Cancel all open orders."""
        return self._request("DELETE", "/v2/orders")

    def cancel_orders_for_symbol(self, symbol: str, confirm: bool = True) -> int:
        """
        Cancel all open orders for a specific symbol (including bracket children).

        Args:
            symbol: Symbol to cancel orders for
            confirm: If True, verify all orders are actually cancelled before returning

        Returns:
            Number of orders cancelled

        CRITICAL for production: Cancels parent AND child orders to prevent orphaned exits.
        """
        try:
            open_orders = self.get_orders(status="open")
            symbol_orders = [o for o in open_orders if o["symbol"] == symbol]

            if not symbol_orders:
                return 0

            logger.info(f"[CANCEL] {symbol}: Found {len(symbol_orders)} open order(s) (includes bracket children)")

            cancelled_count = 0
            for order in symbol_orders:
                order_id = order["id"]
                order_class = order.get("order_class", "simple")
                try:
                    self.cancel_order(order_id)
                    cancelled_count += 1
                    logger.debug(f"[CANCEL] {symbol}: Cancelled {order_class} order {order_id}")
                except Exception as e:
                    # Ignore "order not found" errors (already cancelled/filled)
                    if "not found" not in str(e).lower():
                        logger.warning(f"[CANCEL] {symbol}: Failed to cancel order {order_id}: {e}")

            # CRITICAL: Confirm all orders are actually cancelled
            if confirm and cancelled_count > 0:
                time.sleep(0.5)  # Brief pause for cancellations to propagate
                remaining_orders = [o for o in self.get_orders(status="open") if o["symbol"] == symbol]
                if remaining_orders:
                    logger.warning(f"[CANCEL] {symbol}: {len(remaining_orders)} order(s) still open after cancellation!")
                else:
                    logger.info(f"[CANCEL] {symbol}: Confirmed all {cancelled_count} order(s) cancelled")

            return cancelled_count
        except Exception as e:
            logger.warning(f"[CANCEL] {symbol}: Failed to cancel orders: {e}")
            return 0

    def close_position(self, symbol: str):
        """Close position with market order (legacy method)."""
        return self._request("DELETE", f"/v2/positions/{symbol}")

    def close_position_limit(self, symbol: str, qty: int, side: str = "sell") -> dict:
        """
        MODERATE FIX: Close position with limit order at bid-$0.01 to save spread.

        Similar to entry repricing, this avoids paying the full spread on exits.
        For sells: bid - $0.01 (more aggressive than passive bid, saves vs ask)
        For buys: ask + $0.01 (more aggressive than passive ask, saves vs bid)

        Returns order dict or raises exception.
        """
        try:
            quote = self.get_latest_quote(symbol)
            bid_price = float(quote["bp"])
            ask_price = float(quote["ap"])

            if side == "sell":
                # Exit long: sell at bid-$0.01 (saves ~50% of spread vs market order)
                limit_price = round(bid_price - 0.01, 2)
                logger.info(f"[EXIT] {symbol}: Using bid-$0.01 exit pricing | "
                           f"bid=${bid_price:.2f} ask=${ask_price:.2f} limit=${limit_price:.2f} "
                           f"(saves ${ask_price - limit_price:.2f} vs market)")
            else:
                # Exit short: buy at ask+$0.01 (saves ~50% of spread vs market order)
                limit_price = round(ask_price + 0.01, 2)
                logger.info(f"[EXIT] {symbol}: Using ask+$0.01 exit pricing | "
                           f"bid=${bid_price:.2f} ask=${ask_price:.2f} limit=${limit_price:.2f} "
                           f"(saves ${limit_price - bid_price:.2f} vs market)")

            # Submit limit order with IOC time_in_force (if not filled, falls back to market)
            order = self.submit_order(
                symbol=symbol,
                qty=qty,
                side=side,
                order_type="limit",
                time_in_force="ioc",  # Immediate-or-cancel for fast execution
                limit_price=limit_price
            )

            return order

        except Exception as e:
            logger.warning(f"[EXIT] {symbol}: Limit exit failed ({e}), falling back to market order")
            # Fallback to market order if limit fails — use GTC so it works after hours
            return self.submit_order(
                symbol=symbol,
                qty=qty,
                side=side,
                order_type="market",
                time_in_force="gtc"
            )

    def flatten_symbol(self, symbol: str) -> dict:
        """
        Flatten a symbol completely: cancel all orders, then close position.
        This is the Level 3 way to close positions (avoids "insufficient qty" errors).

        Returns dict with status info.
        """
        result = {"symbol": symbol, "orders_cancelled": 0, "position_closed": False, "errors": []}

        # Step 1: Cancel all open orders for this symbol
        try:
            result["orders_cancelled"] = self.cancel_orders_for_symbol(symbol)
            if result["orders_cancelled"] > 0:
                logger.info(f"[FLATTEN] {symbol}: Cancelled {result['orders_cancelled']} order(s)")
                time.sleep(0.5)  # Brief pause for order cancellations to settle
        except Exception as e:
            result["errors"].append(f"Cancel orders failed: {e}")
            logger.warning(f"[FLATTEN] {symbol}: Error cancelling orders: {e}")

        # Step 2: Close position if it exists (MODERATE FIX: use limit order repricing)
        try:
            pos = self.get_position(symbol)
            if pos:
                qty = int(pos["qty"])
                side = "sell" if pos["side"] == "long" else "buy"

                # Try limit order exit first (saves spread)
                try:
                    order = self.close_position_limit(symbol, qty, side)
                    result["position_closed"] = True
                    logger.info(f"[FLATTEN] {symbol}: Position closed with limit order | order_id={order.get('id')}")
                except Exception as limit_err:
                    logger.warning(f"[FLATTEN] {symbol}: Limit close failed ({limit_err}), using market order")
                    # Fallback to market order
                    self.close_position(symbol)
                    result["position_closed"] = True
                    logger.info(f"[FLATTEN] {symbol}: Position closed with market order (fallback)")
            else:
                logger.debug(f"[FLATTEN] {symbol}: No position to close")
        except requests.HTTPError as e:
            if e.response.status_code in (403, 404):
                # Position doesn't exist - that's OK
                logger.debug(f"[FLATTEN] {symbol}: Position already closed (403/404)")
            else:
                result["errors"].append(f"Close position failed: {e}")
                logger.warning(f"[FLATTEN] {symbol}: Error closing position: {e}")
        except Exception as e:
            result["errors"].append(f"Close position failed: {e}")
            logger.warning(f"[FLATTEN] {symbol}: Error closing position: {e}")

        # Step 3: Verify flattened state
        try:
            open_orders = self.get_orders(status="open")
            symbol_orders = [o for o in open_orders if o["symbol"] == symbol]
            pos = self.get_position(symbol)

            if symbol_orders or pos:
                logger.warning(f"[FLATTEN] {symbol}: NOT fully flat | orders={len(symbol_orders)} position={bool(pos)}")
                result["errors"].append("Symbol not fully flattened")
            else:
                logger.info(f"[FLATTEN] {symbol}: Confirmed FLAT")
        except Exception as e:
            logger.warning(f"[FLATTEN] {symbol}: Could not verify flat state: {e}")

        return result

    def get_clock(self) -> dict:
        return self._request("GET", "/v2/clock")

    def get_calendar(self, start: Optional[str] = None, end: Optional[str] = None) -> List[dict]:
        """
        Get market calendar (trading days and hours).

        Args:
            start: Start date (YYYY-MM-DD)
            end: End date (YYYY-MM-DD)

        Returns:
            List of calendar entries with 'date', 'open', 'close' fields
        """
        params = {}
        if start:
            params["start"] = start
        if end:
            params["end"] = end
        return self._request("GET", "/v2/calendar", params=params)

    def get_order(self, order_id: str) -> dict:
        """Get order status by ID."""
        return self._request("GET", f"/v2/orders/{order_id}")

    def get_latest_quote(self, symbol: str) -> dict:
        """Get latest quote for a symbol from Alpaca data API."""
        response = self._request("GET", f"/v2/stocks/{symbol}/quotes/latest", base_url=self.data_base)
        # Alpaca wraps quote in "quote" key, return the inner dict for easier access
        return response.get("quote", {})

    def get_asset(self, symbol: str) -> Optional[dict]:
        """
        Get asset information for trading validation.

        Returns asset dict with:
        - tradable: bool (can be traded)
        - status: str (active, inactive)
        - tradeable: bool (alternate spelling)
        - fractionable: bool
        """
        try:
            return self._request("GET", f"/v2/assets/{symbol}")
        except Exception as e:
            logger.warning(f"[ALPACA] Failed to get asset info for {symbol}: {e}")
            return None

    def is_symbol_tradable(self, symbol: str) -> Tuple[bool, Optional[str]]:
        """
        Check if symbol is currently tradable.

        Returns:
            (tradable, reason) tuple
        """
        try:
            asset = self.get_asset(symbol)
            if not asset:
                return False, "Asset not found"

            # Check tradable flag
            tradable = asset.get("tradable", False)
            status = asset.get("status", "unknown")

            if not tradable:
                return False, f"Asset not tradable (status={status})"

            if status != "active":
                return False, f"Asset status={status} (not active)"

            return True, None

        except Exception as e:
            logger.warning(f"[ALPACA] Error checking if {symbol} is tradable: {e}")
            return False, f"Error: {e}"



alpaca = AlpacaClient()


# ============================================================
# POLYGON REST API
# ============================================================

class PolygonClient:
    """Polygon data client."""

    def __init__(self):
        self.base_url = POLYGON_REST_BASE
        self.api_key = POLYGON_API_KEY

    def _request(self, path: str, params: dict = None, retries: int = 3) -> dict:
        """
        Make request to Polygon API with retry logic for transient errors.

        Args:
            path: API endpoint path
            params: Query parameters
            retries: Number of retry attempts (default 3)

        Returns:
            JSON response dict
        """
        params = params or {}
        params["apiKey"] = self.api_key
        url = f"{self.base_url}{path}"

        last_exception = None
        for attempt in range(retries):
            try:
                response = requests.get(url, params=params, timeout=15)
                response.raise_for_status()
                return response.json()

            except requests.HTTPError as e:
                status_code = e.response.status_code if e.response else 0

                # Rate limit (429) or server error (5xx) - retry with backoff
                if status_code == 429 or 500 <= status_code < 600:
                    if attempt < retries - 1:
                        backoff_sec = 2 ** attempt
                        logger.debug(f"[POLYGON] {path} | {status_code} | retry {attempt+1}/{retries} in {backoff_sec}s")
                        time.sleep(backoff_sec)
                        last_exception = e
                        continue
                    else:
                        logger.error(f"[POLYGON] {path} | {status_code} | exhausted {retries} retries")
                        raise
                else:
                    # Client error (4xx) - don't retry
                    raise

            except requests.RequestException as e:
                # Network error - retry
                if attempt < retries - 1:
                    backoff_sec = 2 ** attempt
                    logger.debug(f"[POLYGON] {path} | network error | retry {attempt+1}/{retries} in {backoff_sec}s")
                    time.sleep(backoff_sec)
                    last_exception = e
                    continue
                else:
                    raise

        # Should not reach here
        if last_exception:
            raise last_exception

    def get_bars(self, symbol: str, timespan: str, from_date: str, to_date: str, limit: int = 5000) -> pd.DataFrame:
        """Get aggregate bars."""
        path = f"/v2/aggs/ticker/{symbol}/range/1/{timespan}/{from_date}/{to_date}"
        data = self._request(path, {"adjusted": "true", "sort": "asc", "limit": str(limit)})

        results = data.get("results", [])
        if not results:
            return pd.DataFrame()

        df = pd.DataFrame(results)
        df["timestamp"] = pd.to_datetime(df["t"], unit="ms", utc=True).dt.tz_convert(ET)
        df = df.rename(columns={"o": "open", "h": "high", "l": "low", "c": "close", "v": "volume", "vw": "vwap"})
        return df.set_index("timestamp")[["open", "high", "low", "close", "volume", "vwap"]]

    def get_snapshot(self, symbol: str) -> Optional[dict]:
        """
        Get real-time snapshot for a symbol.

        Returns dict with keys: lastTrade, lastQuote, day, min, prevDay
        - lastTrade: {p: price, s: size, t: timestamp_ns, x: exchange}
        - lastQuote: {P: bid, S: bid_size, p: ask, s: ask_size, t: timestamp_ns}
        - day: {o, h, l, c, v, vw} - aggregated day bar
        - min: {av, t, n, o, h, l, c, v, vw} - latest 1-min bar
        """
        try:
            path = f"/v2/snapshot/locale/us/markets/stocks/tickers/{symbol}"
            data = self._request(path)
            return data.get("ticker")
        except Exception as e:
            logger.debug(f"Failed to get snapshot for {symbol}: {e}")
            return None

    def get_current_price(self, snapshot: dict) -> Optional[float]:
        """
        Extract current price from snapshot.
        Priority: lastTrade.p > min.c > day.c
        """
        if not snapshot:
            return None

        # Priority 1: Last trade price (most recent)
        last_trade = snapshot.get("lastTrade")
        if last_trade and "p" in last_trade:
            return float(last_trade["p"])

        # Priority 2: Latest minute bar close
        min_bar = snapshot.get("min")
        if min_bar and "c" in min_bar:
            return float(min_bar["c"])

        # Priority 3: Day bar close (fallback, but this is the OLD way - not ideal)
        day_bar = snapshot.get("day")
        if day_bar and "c" in day_bar:
            return float(day_bar["c"])

        return None

    def get_spread_info(self, snapshot: dict) -> dict:
        """
        Calculate spread from lastQuote.
        Returns: {bid, ask, spread_dollars, spread_bps, bid_size, ask_size, mid}
        """
        if not snapshot:
            return {"spread_bps": 999.9}  # Invalid spread

        last_quote = snapshot.get("lastQuote")
        if not last_quote:
            return {"spread_bps": 999.9}

        bid = last_quote.get("P", last_quote.get("bp"))  # P=bid or bp=bid_price
        ask = last_quote.get("p", last_quote.get("ap"))  # p=ask or ap=ask_price
        bid_size = last_quote.get("S", last_quote.get("bs", 0))  # S=bid_size
        ask_size = last_quote.get("s", last_quote.get("as", 0))  # s=ask_size

        if not bid or not ask or bid <= 0 or ask <= 0:
            return {"spread_bps": 999.9}

        bid = float(bid)
        ask = float(ask)
        mid = (bid + ask) / 2.0
        spread_dollars = ask - bid
        spread_bps = (spread_dollars / mid) * 10000.0 if mid > 0 else 999.9

        return {
            "bid": bid,
            "ask": ask,
            "mid": mid,
            "spread_dollars": spread_dollars,
            "spread_bps": spread_bps,
            "bid_size": int(bid_size),
            "ask_size": int(ask_size),
        }

    def calculate_atr(self, df: pd.DataFrame, period: int = ATR_PERIOD) -> Optional[float]:
        """
        Calculate Average True Range (ATR) for volatility measurement.
        ATR = average of True Range over period.
        True Range = max(high - low, abs(high - prev_close), abs(low - prev_close))
        """
        if df.empty or len(df) < period + 1:
            return None

        try:
            # Calculate True Range
            high = df["high"]
            low = df["low"]
            close = df["close"]
            prev_close = close.shift(1)

            tr1 = high - low
            tr2 = (high - prev_close).abs()
            tr3 = (low - prev_close).abs()

            true_range = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

            # Calculate ATR as simple moving average of True Range
            atr = true_range.rolling(window=period).mean()

            # Return most recent ATR value
            return float(atr.iloc[-1]) if pd.notna(atr.iloc[-1]) else None

        except Exception as e:
            logger.debug(f"Failed to calculate ATR: {e}")
            return None

    def calculate_time_of_day_rvol(self, df: pd.DataFrame, current_time: dt.datetime,
                                   lookback_days: int = RVOL_LOOKBACK_DAYS) -> Optional[float]:
        """
        Calculate TRUE relative volume: time-of-day cumulative RVOL.

        Formula: cum_volume_today_to_now / avg_cum_volume_at_same_time_over_N_days

        This compares today's cumulative volume (from market open 9:30 AM to current time)
        against the average cumulative volume at the same time-of-day over the past N days.

        This is VERY different from "last 1-min volume / avg 20-min volume" which fires on noise.

        Args:
            df: DataFrame with 1-minute bars (must include multiple days of data)
            current_time: Current time (used to determine time-of-day)
            lookback_days: Number of historical days to average

        Returns:
            RVOL ratio (e.g., 2.5 = 2.5x normal volume for this time of day)
        """
        if df.empty:
            return None

        try:
            # Market open is 9:30 AM ET
            market_open_hour = 9
            market_open_minute = 30

            # Get current time-of-day (minutes since market open)
            current_hour = current_time.hour
            current_minute = current_time.minute

            # Calculate minutes since market open
            minutes_since_open = (current_hour - market_open_hour) * 60 + (current_minute - market_open_minute)

            # Only calculate RVOL during market hours (9:30 AM - 4:00 PM)
            if minutes_since_open < 0 or current_hour >= 16:
                return None

            # Get today's date
            today = current_time.date()

            # Calculate cumulative volume for today (from 9:30 AM to now)
            today_bars = df[df.index.date == today]
            if today_bars.empty:
                return None

            # Filter to market hours only (9:30 AM onwards)
            today_rth = today_bars[(today_bars.index.hour > market_open_hour) |
                                  ((today_bars.index.hour == market_open_hour) &
                                   (today_bars.index.minute >= market_open_minute))]

            if today_rth.empty:
                return None

            # Cumulative volume today up to current time
            cum_volume_today = today_rth["volume"].sum()

            # Now calculate average cumulative volume at same time-of-day over past N days
            historical_cum_volumes = []

            for days_back in range(1, lookback_days + 1):
                historical_date = today - dt.timedelta(days=days_back)

                # Get bars for that historical day
                hist_bars = df[df.index.date == historical_date]
                if hist_bars.empty:
                    continue

                # Filter to RTH only
                hist_rth = hist_bars[(hist_bars.index.hour > market_open_hour) |
                                    ((hist_bars.index.hour == market_open_hour) &
                                     (hist_bars.index.minute >= market_open_minute))]

                if hist_rth.empty:
                    continue

                # Filter to same time-of-day (up to current minutes since open)
                # Calculate time threshold for historical day
                target_time = dt.datetime.combine(
                    historical_date,
                    dt.time(current_hour, current_minute, tzinfo=ET)
                )

                hist_up_to_time = hist_rth[hist_rth.index <= target_time]

                if not hist_up_to_time.empty:
                    hist_cum_volume = hist_up_to_time["volume"].sum()
                    historical_cum_volumes.append(hist_cum_volume)

            # CRITICAL VALIDATION: Ensure we have enough ACTUAL trading days (not calendar days)
            # Minimum 10 trading days required for reliable RVOL calculation
            MIN_TRADING_DAYS_FOR_RVOL = 10

            if len(historical_cum_volumes) < MIN_TRADING_DAYS_FOR_RVOL:
                logger.warning(f"RVOL: Insufficient trading day sample | got={len(historical_cum_volumes)} days, "
                             f"need={MIN_TRADING_DAYS_FOR_RVOL}+ | cannot calculate reliable RVOL")
                return None

            avg_hist_cum_volume = sum(historical_cum_volumes) / len(historical_cum_volumes)

            # Calculate RVOL
            if avg_hist_cum_volume <= 0:
                return None

            rvol = cum_volume_today / avg_hist_cum_volume

            logger.debug(f"RVOL: today_cum={cum_volume_today:,.0f} avg_hist={avg_hist_cum_volume:,.0f} "
                        f"ratio={rvol:.2f}x (minutes_since_open={minutes_since_open} "
                        f"n_trading_days={len(historical_cum_volumes)}/{lookback_days} requested)")

            return float(rvol)

        except Exception as e:
            logger.debug(f"Failed to calculate time-of-day RVOL: {e}")
            return None

    def get_news(self, symbol: str, limit: int = 10) -> dict:
        """
        Fetch news articles for a symbol from Polygon.

        Args:
            symbol: Stock ticker symbol
            limit: Max number of articles to fetch

        Returns:
            Dict with 'results' list of articles, each containing:
            - title: Article headline
            - published_utc: ISO timestamp
            - publisher: {name: str}
            - article_url: URL to full article
        """
        try:
            path = "/v2/reference/news"
            params = {
                "ticker": symbol,
                "limit": str(limit),
                "order": "desc",
                "sort": "published_utc"
            }
            return self._request(path, params)
        except Exception as e:
            logger.debug(f"[POLYGON] Failed to get news for {symbol}: {e}")
            return {"results": []}


polygon = PolygonClient()

# Note: regime_detector initialized later after RegimeDetector class is defined


# ============================================================
# MARKET HOURS & SESSION
# ============================================================

class MarketSession(Enum):
    CLOSED = "CLOSED"
    RTH = "RTH"  # Regular Trading Hours only

def get_market_session() -> MarketSession:
    """
    Determine current market session (RTH only for this strategy).

    PRODUCTION: Uses Alpaca clock for accurate market hours including early closes and holidays.
    """
    try:
        clock = alpaca.get_clock()
        is_open = clock.get("is_open", False)

        if not is_open:
            return MarketSession.CLOSED

        # Use Alpaca's clock times (handles early closes automatically)
        now = now_et()
        next_open_str = clock.get("next_open")
        next_close_str = clock.get("next_close")

        if next_close_str:
            # Parse next close time to check for early close
            next_close = from_iso(next_close_str)
            close_hour = next_close.hour
            close_minute = next_close.minute

            # Log if early close detected
            if close_hour < 16 or (close_hour == 16 and close_minute == 0):
                if close_hour != 16:  # Not normal 4:00 PM close
                    logger.info(f"[CALENDAR] Early close detected today: {close_hour}:{close_minute:02d} ET")

        # Check if we're in RTH (not extended hours)
        hour = now.hour
        minute = now.minute

        # RTH: 9:30 AM - 4:00 PM (or early close time from calendar)
        if (hour == 9 and minute >= 30) or (10 <= hour < 16):
            return MarketSession.RTH

        return MarketSession.CLOSED
    except Exception as e:
        logger.error(f"Error getting market session: {e}")
        return MarketSession.CLOSED


# ============================================================
# NEWS SENTIMENT ANALYSIS (AI Phase 1)
# ============================================================

class NewsSentiment(Enum):
    """News sentiment classification."""
    GREAT = "GREAT"      # Strong bullish catalyst - boost position
    BULLISH = "BULLISH"  # Positive news - normal entry
    NEUTRAL = "NEUTRAL"  # No clear sentiment
    BEARISH = "BEARISH"  # Negative news - block entry


@dataclass
class NewsItem:
    """Single news article with sentiment."""
    title: str
    published: dt.datetime
    sentiment: NewsSentiment
    source: str = ""

    def age_hours(self) -> float:
        """Get age of news item in hours."""
        now = now_et()
        if self.published.tzinfo is None:
            # Assume UTC if no timezone
            published_et = self.published.replace(tzinfo=dt.timezone.utc).astimezone(ET)
        else:
            published_et = self.published.astimezone(ET)
        delta = now - published_et
        return delta.total_seconds() / 3600


class NewsFetcher:
    """
    Fetches and analyzes news from Polygon API with caching.

    Uses keyword-based sentiment analysis to classify news as:
    - GREAT: Strong bullish catalyst (FDA approval, acquisition, etc.)
    - BULLISH: Positive news (beats estimates, upgrades, etc.)
    - NEUTRAL: No clear direction
    - BEARISH: Negative news (lawsuits, misses, downgrades, etc.)
    """

    def __init__(self):
        self.cache: Dict[str, Tuple[dt.datetime, List[NewsItem]]] = {}
        self.cache_duration = dt.timedelta(minutes=NEWS_CACHE_MINUTES)

    def _analyze_sentiment(self, title: str) -> NewsSentiment:
        """Analyze news title for sentiment using keyword matching."""
        title_lower = title.lower()

        # Check for GREAT keywords first (strongest bullish)
        for keyword in GREAT_KEYWORDS:
            if keyword in title_lower:
                return NewsSentiment.GREAT

        # Check for BEARISH keywords (block trades)
        for keyword in BEARISH_KEYWORDS:
            if keyword in title_lower:
                return NewsSentiment.BEARISH

        # Check for BULLISH keywords
        for keyword in BULLISH_KEYWORDS:
            if keyword in title_lower:
                return NewsSentiment.BULLISH

        return NewsSentiment.NEUTRAL

    def get_news(self, symbol: str) -> List[NewsItem]:
        """
        Get recent news for a symbol with sentiment analysis.

        Returns cached results if available and fresh.
        """
        now = now_et()

        # Check cache
        if symbol in self.cache:
            cache_time, cached_news = self.cache[symbol]
            if now - cache_time < self.cache_duration:
                return cached_news

        # Fetch fresh news from Polygon
        try:
            news_data = polygon.get_news(symbol, limit=NEWS_MAX_ARTICLES)
            results = news_data.get("results", []) or []

            news_items = []
            cutoff = now - dt.timedelta(hours=NEWS_LOOKBACK_HOURS)

            for article in results:
                # IMPORTANT: Verify the article is actually about this ticker
                # Polygon sometimes returns news tagged with multiple/unrelated tickers
                article_tickers = article.get("tickers", []) or []
                if symbol not in article_tickers:
                    # Article is not specifically about this ticker - skip it
                    continue

                # Parse published time
                pub_str = article.get("published_utc", "")
                if not pub_str:
                    continue

                try:
                    # Handle ISO format with Z suffix
                    if pub_str.endswith("Z"):
                        pub_str = pub_str[:-1] + "+00:00"
                    published = dt.datetime.fromisoformat(pub_str)
                except (ValueError, TypeError):
                    continue

                # Skip old news
                if published.tzinfo:
                    published_et = published.astimezone(ET)
                else:
                    published_et = published.replace(tzinfo=dt.timezone.utc).astimezone(ET)

                if published_et < cutoff:
                    continue

                title = article.get("title", "")
                if not title:
                    continue

                # Analyze sentiment
                sentiment = self._analyze_sentiment(title)

                news_items.append(NewsItem(
                    title=title,
                    published=published,
                    sentiment=sentiment,
                    source=article.get("publisher", {}).get("name", "Unknown")
                ))

            # Cache results
            self.cache[symbol] = (now, news_items)
            return news_items

        except Exception as e:
            logger.debug(f"[NEWS] Error fetching news for {symbol}: {e}")
            return []

    def get_sentiment_summary(self, symbol: str) -> Tuple[NewsSentiment, List[NewsItem]]:
        """
        Get overall sentiment and news items for a symbol.

        Returns:
            Tuple of (overall_sentiment, news_items)

        Logic with age decay:
        - Fresh bearish news (< NEWS_FULL_IMPACT_HOURS): BEARISH (block trade)
        - Aging bearish news (between thresholds): NEUTRAL with warning (logged but not blocked)
        - Stale bearish news (> NEWS_NO_IMPACT_HOURS): ignored
        - Fresh great news: GREAT (boost position)
        - Fresh bullish news: BULLISH (normal entry)
        - Otherwise: NEUTRAL
        """
        news_items = self.get_news(symbol)

        if not news_items:
            return NewsSentiment.NEUTRAL, []

        # Check for bearish news with age decay
        # Only fresh bearish news blocks trades; stale bearish news is ignored
        fresh_bearish = []
        aging_bearish = []
        for n in news_items:
            if n.sentiment == NewsSentiment.BEARISH:
                age = n.age_hours()
                if age < NEWS_FULL_IMPACT_HOURS:
                    fresh_bearish.append(n)
                elif age < NEWS_NO_IMPACT_HOURS:
                    aging_bearish.append(n)
                # News older than NEWS_NO_IMPACT_HOURS is ignored

        if fresh_bearish:
            return NewsSentiment.BEARISH, news_items

        # Log aging bearish news but don't block (for monitoring)
        if aging_bearish:
            headlines = [f"{n.title[:40]}... ({n.age_hours():.1f}h ago)" for n in aging_bearish[:2]]
            logger.debug(f"[NEWS] {symbol}: Aging bearish news (not blocking): {headlines}")

        # Check for great news (strong catalyst) - also apply age decay
        fresh_great = any(n.sentiment == NewsSentiment.GREAT and n.age_hours() < NEWS_NO_IMPACT_HOURS
                        for n in news_items)
        if fresh_great:
            return NewsSentiment.GREAT, news_items

        # Check for bullish news - also apply age decay
        fresh_bullish = any(n.sentiment == NewsSentiment.BULLISH and n.age_hours() < NEWS_NO_IMPACT_HOURS
                          for n in news_items)
        if fresh_bullish:
            return NewsSentiment.BULLISH, news_items

        return NewsSentiment.NEUTRAL, news_items


# Global news fetcher instance
news_fetcher = NewsFetcher()


# ============================================================
# ML SIGNAL SCORER (AI Phase 2)
# ============================================================

@dataclass
class SignalFeatures:
    """Features extracted from a trading setup for ML scoring."""
    symbol: str
    timestamp: dt.datetime
    # Price/Volume features
    rvol: float                    # Relative volume
    vwap_distance_pct: float       # % distance from VWAP
    ema_separation_pct: float      # % separation between fast/slow EMA
    spread_bps: float              # Bid-ask spread in basis points
    # Trend features
    adx: Optional[float]           # ADX trend strength
    momentum_5min_pct: float       # 5-min price momentum %
    # Context features
    minutes_since_open: int        # Minutes since market open
    vol_regime: str                # "HIGH", "NORMAL", "LOW"
    # News features
    news_sentiment: Optional[str]  # "GREAT", "BULLISH", "NEUTRAL", "BEARISH"
    # Computed score
    score: float = 0.0             # Final score 0-100
    # For logging outcomes
    trade_id: Optional[str] = None
    outcome: Optional[str] = None  # "WIN", "LOSS", "PENDING"
    pnl: Optional[float] = None


class SignalScorer:
    """
    Scores trading setups based on multiple features.

    Phase 2 Implementation:
    - Rule-based scoring using feature weights
    - Logs features for future ML training
    - Can be upgraded to ML model once training data collected

    Scoring ranges:
    - 75-100: High quality setup (boost position size)
    - 60-74: Good setup (normal position size)
    - 50-59: Marginal setup (reduce position size)
    - 0-49: Poor setup (skip trade)
    """

    def __init__(self):
        self.feature_log_path = SIGNAL_FEATURES_LOG

    def extract_features(self, data, current_time: dt.datetime = None) -> SignalFeatures:
        """
        Extract scoring features from MarketData.

        Args:
            data: MarketData object with all indicators
            current_time: Optional override for current time

        Returns:
            SignalFeatures dataclass with extracted features
        """
        ts = current_time or now_et()

        # Calculate derived features
        vwap_distance_pct = ((data.last_price - data.vwap) / data.vwap * 100) if data.vwap > 0 else 0
        ema_separation_pct = ((data.ema_fast - data.ema_slow) / data.ema_slow * 100) if data.ema_slow > 0 else 0

        # Calculate 5-min momentum
        momentum_5min_pct = 0.0
        if len(data.df) >= 5:
            price_5min_ago = data.df["close"].iloc[-5]
            momentum_5min_pct = ((data.last_price - price_5min_ago) / price_5min_ago * 100) if price_5min_ago > 0 else 0

        # Calculate minutes since open
        minutes_since_open = (ts.hour - 9) * 60 + (ts.minute - 30)
        if minutes_since_open < 0:
            minutes_since_open = 0

        # Get news sentiment as string
        news_sentiment_str = data.news_sentiment.value if data.news_sentiment else None

        return SignalFeatures(
            symbol=data.symbol,
            timestamp=ts,
            rvol=data.relative_volume,
            vwap_distance_pct=vwap_distance_pct,
            ema_separation_pct=ema_separation_pct,
            spread_bps=data.spread_bps,
            adx=data.adx,
            momentum_5min_pct=momentum_5min_pct,
            minutes_since_open=minutes_since_open,
            vol_regime=data.vol_regime or "NORMAL",
            news_sentiment=news_sentiment_str
        )

    def score_features(self, features: SignalFeatures) -> float:
        """
        Calculate signal score based on features.

        Uses weighted scoring across multiple factors.
        Each factor contributes 0 to its max weight.

        Returns:
            Score from 0-100
        """
        score = 0.0

        # 1. RVOL scoring (0-15 points)
        # Sweet spot: 1.5x to 5x (too high can mean exhaustion)
        max_rvol = FEATURE_WEIGHTS["rvol"]
        if features.rvol >= 5.0:
            score += max_rvol  # Full points for strong volume
        elif features.rvol >= 3.0:
            score += max_rvol * 0.9  # 90% for good volume
        elif features.rvol >= 2.0:
            score += max_rvol * 0.7  # 70% for decent volume
        elif features.rvol >= 1.5:
            score += max_rvol * 0.5  # 50% for minimum volume
        # Below 1.5x = 0 points

        # 2. VWAP Distance scoring (0-15 points)
        # Sweet spot: 0.3% to 2% above VWAP (relaxed from 0.5% on 2026-01-29)
        max_vwap = FEATURE_WEIGHTS["vwap_distance"]
        if 0.3 <= features.vwap_distance_pct <= 2.0:
            score += max_vwap  # Full points for sweet spot
        elif 0.15 <= features.vwap_distance_pct < 0.3:
            score += max_vwap * 0.7  # 70% for close to VWAP
        elif 2.0 < features.vwap_distance_pct <= 3.0:
            score += max_vwap * 0.6  # 60% for extended
        elif features.vwap_distance_pct > 3.0:
            score += max_vwap * 0.3  # 30% for very extended (risky)
        # Below 0.15% or negative = 0 points

        # 3. EMA Separation scoring (0-10 points)
        # Sweet spot: 0.2% to 1.0% separation
        max_ema = FEATURE_WEIGHTS["ema_separation"]
        if 0.2 <= features.ema_separation_pct <= 1.0:
            score += max_ema  # Full points for clear trend
        elif 0.1 <= features.ema_separation_pct < 0.2:
            score += max_ema * 0.6  # 60% for emerging trend
        elif 1.0 < features.ema_separation_pct <= 2.0:
            score += max_ema * 0.7  # 70% for strong trend
        elif features.ema_separation_pct > 2.0:
            score += max_ema * 0.4  # 40% for extended trend
        # Below 0.1% = 0 points (flat/no trend)

        # 4. ADX scoring (0-15 points)
        # Sweet spot: 15-30 (emerging to moderate trend)
        max_adx = FEATURE_WEIGHTS["adx"]
        if features.adx is not None:
            if 15 <= features.adx <= 25:
                score += max_adx  # Full points for ideal range
            elif 25 < features.adx <= 35:
                score += max_adx * 0.8  # 80% for strong trend
            elif 10 <= features.adx < 15:
                score += max_adx * 0.5  # 50% for weak trend
            elif features.adx > 35:
                score += max_adx * 0.4  # 40% for very strong (exhaustion risk)
            # Below 10 = 0 points (no trend)
        else:
            score += max_adx * 0.5  # 50% if ADX unavailable

        # 5. Spread scoring (0-10 points)
        # Lower spread = better liquidity and execution
        max_spread = FEATURE_WEIGHTS["spread"]
        if features.spread_bps <= 3:
            score += max_spread  # Full points for tight spread
        elif features.spread_bps <= 5:
            score += max_spread * 0.8  # 80% for good spread
        elif features.spread_bps <= 10:
            score += max_spread * 0.5  # 50% for okay spread
        elif features.spread_bps <= 20:
            score += max_spread * 0.2  # 20% for wide spread
        # Above 20 bps = 0 points

        # 6. Momentum scoring (0-10 points)
        # Sweet spot: 0.15% to 0.8% gain in last 5 minutes
        max_momentum = FEATURE_WEIGHTS["momentum"]
        if 0.15 <= features.momentum_5min_pct <= 0.8:
            score += max_momentum  # Full points for healthy momentum
        elif 0.05 <= features.momentum_5min_pct < 0.15:
            score += max_momentum * 0.6  # 60% for weak momentum
        elif 0.8 < features.momentum_5min_pct <= 1.5:
            score += max_momentum * 0.7  # 70% for strong momentum
        elif features.momentum_5min_pct > 1.5:
            score += max_momentum * 0.3  # 30% for chasing (risky)
        # Negative momentum = 0 points

        # 7. Time of Day scoring (0-10 points)
        # Best: 30-120 min after open (10:00 AM - 11:30 AM)
        max_time = FEATURE_WEIGHTS["time_of_day"]
        if 30 <= features.minutes_since_open <= 120:
            score += max_time  # Full points for sweet spot
        elif 120 < features.minutes_since_open <= 180:
            score += max_time * 0.7  # 70% for late morning
        elif 180 < features.minutes_since_open <= 270:
            score += max_time * 0.4  # 40% for afternoon
        elif features.minutes_since_open > 270:
            score += max_time * 0.2  # 20% for late day
        # First 30 min = 0 points (too volatile)

        # 8. News Sentiment scoring (0-15 points)
        max_news = FEATURE_WEIGHTS["news_sentiment"]
        if features.news_sentiment == "GREAT":
            score += max_news  # Full points for great catalyst
        elif features.news_sentiment == "BULLISH":
            score += max_news * 0.7  # 70% for bullish news
        elif features.news_sentiment == "NEUTRAL" or features.news_sentiment is None:
            score += max_news * 0.4  # 40% for neutral/no news
        # BEARISH = 0 points (should have been filtered earlier)

        return round(score, 1)

    def score_setup(self, data, current_time: dt.datetime = None) -> Tuple[float, SignalFeatures]:
        """
        Score a trading setup.

        Args:
            data: MarketData object
            current_time: Optional time override

        Returns:
            Tuple of (score, features)
        """
        features = self.extract_features(data, current_time)
        score = self.score_features(features)
        features.score = score
        return score, features

    def log_features(self, features: SignalFeatures, trade_id: str = None):
        """
        Log features for future ML training.

        Writes to JSONL file for later analysis.
        """
        if not LOG_SIGNAL_FEATURES:
            return

        try:
            features.trade_id = trade_id
            features.outcome = "PENDING"  # Will be updated when trade closes

            # Convert to dict for JSON serialization
            log_entry = {
                "symbol": features.symbol,
                "timestamp": features.timestamp.isoformat(),
                "rvol": features.rvol,
                "vwap_distance_pct": features.vwap_distance_pct,
                "ema_separation_pct": features.ema_separation_pct,
                "spread_bps": features.spread_bps,
                "adx": features.adx,
                "momentum_5min_pct": features.momentum_5min_pct,
                "minutes_since_open": features.minutes_since_open,
                "vol_regime": features.vol_regime,
                "news_sentiment": features.news_sentiment,
                "score": features.score,
                "trade_id": trade_id,
                "outcome": "PENDING"
            }

            with open(self.feature_log_path, "a") as f:
                f.write(json.dumps(log_entry) + "\n")

        except Exception as e:
            logger.debug(f"[SCORER] Failed to log features: {e}")

    def get_position_size_multiplier(self, score: float) -> float:
        """
        Get position size multiplier based on score.

        Returns:
            Multiplier for position sizing (0.75, 1.0, or 1.25)
        """
        if score >= SCORE_BOOST_THRESHOLD:
            return SCORE_BOOST_MULTIPLIER
        elif score < SCORE_REDUCE_THRESHOLD:
            return SCORE_REDUCE_MULTIPLIER
        else:
            return 1.0


# Global signal scorer instance
signal_scorer = SignalScorer()


# ============================================================
# MARKET REGIME DETECTOR (AI Phase 3)
# ============================================================

class MarketRegime(Enum):
    """Market regime classifications."""
    TRENDING_UP = "TRENDING_UP"
    TRENDING_DOWN = "TRENDING_DOWN"
    CHOPPY = "CHOPPY"
    VOLATILE = "VOLATILE"
    CALM = "CALM"


@dataclass
class RegimeState:
    """Current market regime state."""
    regime: MarketRegime
    spy_trend: str              # "UP", "DOWN", "FLAT"
    spy_vs_vwap_pct: float      # % distance from VWAP
    qqq_trend: str              # "UP", "DOWN", "FLAT"
    vix_level: Optional[float]  # VIX or proxy level
    atr_ratio: float            # Current ATR vs average (volatility)
    timestamp: dt.datetime
    confidence: float           # 0-1 confidence in regime classification

    # Polymarket sentiment (AI Phase 4)
    polymarket_risk_level: str = "N/A"  # LOW, MEDIUM, HIGH, EXTREME
    polymarket_size_mult: float = 1.0   # Position size multiplier from prediction markets
    polymarket_recession_prob: Optional[float] = None

    # Enhanced 4-State Regime (v41)
    regime_4state: str = "BULL_LOW_VOL"  # BULL_LOW_VOL, BULL_HIGH_VOL, BEAR_LOW_VOL, BEAR_HIGH_VOL

    def get_adjustments(self) -> dict:
        """Get trading adjustments for current regime."""
        return REGIME_ADJUSTMENTS.get(self.regime.value, REGIME_ADJUSTMENTS["TRENDING_UP"])

    def get_4state_adjustments(self) -> dict:
        """Get enhanced 4-state regime adjustments (v41)."""
        if USE_4STATE_REGIME:
            return REGIME_4STATE_ADJUSTMENTS.get(self.regime_4state, REGIME_4STATE_ADJUSTMENTS["BULL_LOW_VOL"])
        return {"size_mult": 1.0, "stop_mult": 1.0, "tp_mult": 1.0}


class RegimeDetector:
    """
    Detects market regime based on SPY/QQQ trends and volatility.

    Uses rule-based classification:
    1. SPY/QQQ trend (above/below VWAP + EMA alignment)
    2. VIX level for fear gauge
    3. ATR expansion for volatility regime
    4. Price range analysis for chop detection
    5. Polymarket prediction market sentiment (AI Phase 4)
    """

    def __init__(self, polygon_client):
        self.polygon = polygon_client
        self.cache: Optional[RegimeState] = None
        self.cache_time: Optional[dt.datetime] = None
        self.spy_data: Optional[pd.DataFrame] = None
        self.qqq_data: Optional[pd.DataFrame] = None
        self.vix_data: Optional[float] = None

        # Polymarket sentiment cache
        self._polymarket_cache: Optional[dict] = None
        self._polymarket_cache_time: Optional[dt.datetime] = None

    def get_regime(self, force_refresh: bool = False) -> RegimeState:
        """
        Get current market regime, using cache if available.

        Args:
            force_refresh: Force refresh even if cache is valid

        Returns:
            RegimeState with current market classification
        """
        now = now_et()

        # Check cache validity
        if not force_refresh and self.cache and self.cache_time:
            cache_age = (now - self.cache_time).total_seconds()
            if cache_age < REGIME_CACHE_SECONDS:
                return self.cache

        # Fetch fresh data and classify regime
        try:
            regime_state = self._detect_regime(now)
            self.cache = regime_state
            self.cache_time = now

            poly_str = f"Polymarket={regime_state.polymarket_risk_level}" if regime_state.polymarket_risk_level != "N/A" else ""
            vixy_str = f"VIXY=${regime_state.vix_level:.2f}" if regime_state.vix_level else "VIXY=N/A"
            fourstate_str = f"4S={regime_state.regime_4state}" if USE_4STATE_REGIME else ""
            logger.info(f"[REGIME] {regime_state.regime.value} | "
                       f"SPY={regime_state.spy_trend} ({regime_state.spy_vs_vwap_pct:+.2f}% vs VWAP) | "
                       f"QQQ={regime_state.qqq_trend} | "
                       f"{vixy_str} | "
                       f"ATR_ratio={regime_state.atr_ratio:.2f}"
                       f"{' | ' + fourstate_str if fourstate_str else ''}"
                       f"{' | ' + poly_str if poly_str else ''}")

            return regime_state

        except Exception as e:
            logger.warning(f"[REGIME] Detection failed: {e} - defaulting to TRENDING_UP")
            # Cache the fallback too so we don't spam API on repeated failures
            fallback = RegimeState(
                regime=MarketRegime.TRENDING_UP,
                spy_trend="UNKNOWN",
                spy_vs_vwap_pct=0.0,
                qqq_trend="UNKNOWN",
                vix_level=None,
                atr_ratio=1.0,
                timestamp=now,
                confidence=0.0,
                regime_4state="BULL_LOW_VOL"  # Default to safest assumption
            )
            self.cache = fallback
            self.cache_time = now
            return fallback

    def _detect_regime(self, current_time: dt.datetime) -> RegimeState:
        """
        Detect current market regime based on multiple factors.

        Classification logic:
        1. VOLATILE: High VIX (>25) or ATR expansion (>1.5x average)
        2. TRENDING_UP: SPY above VWAP + EMAs aligned bullish
        3. TRENDING_DOWN: SPY below VWAP + EMAs aligned bearish
        4. CHOPPY: Price oscillating around VWAP, no clear trend
        5. CALM: Low volatility, tight range
        """
        # Fetch SPY data
        spy_bars = self._fetch_intraday_bars(REGIME_SPY_SYMBOL, REGIME_LOOKBACK_BARS)
        if spy_bars is None or spy_bars.empty:
            raise ValueError("Could not fetch SPY data")

        # Fetch QQQ data
        qqq_bars = self._fetch_intraday_bars(REGIME_QQQ_SYMBOL, REGIME_LOOKBACK_BARS)

        # Fetch VIX proxy
        vix_level = self._get_vix_level()

        # Analyze SPY trend
        spy_trend, spy_vs_vwap_pct = self._analyze_trend(spy_bars)

        # Analyze QQQ trend (for confirmation)
        qqq_trend = "FLAT"
        if qqq_bars is not None and not qqq_bars.empty:
            qqq_trend, _ = self._analyze_trend(qqq_bars)

        # Calculate volatility ratio
        atr_ratio = self._calculate_atr_ratio(spy_bars)

        # Classify regime
        regime, confidence = self._classify_regime(
            spy_trend, spy_vs_vwap_pct, qqq_trend, vix_level, atr_ratio
        )

        # Fetch Polymarket sentiment (AI Phase 4)
        poly_risk_level = "N/A"
        poly_size_mult = 1.0
        poly_recession_prob = None

        if ENABLE_POLYMARKET_SENTIMENT:
            poly_data = self._get_polymarket_sentiment()
            if poly_data:
                poly_risk_level = poly_data.get("risk_level", "N/A")
                poly_size_mult = POLYMARKET_SIZE_ADJUSTMENTS.get(poly_risk_level, 1.0)
                poly_recession_prob = poly_data.get("recession_prob")

                # Log warnings for elevated recession probability
                if poly_recession_prob:
                    if poly_recession_prob >= POLYMARKET_RECESSION_CAUTION:
                        logger.warning(f"[POLYMARKET] HIGH recession probability: {poly_recession_prob*100:.1f}% - major caution")
                    elif poly_recession_prob >= POLYMARKET_RECESSION_WARNING:
                        logger.info(f"[POLYMARKET] Elevated recession probability: {poly_recession_prob*100:.1f}%")

        # Enhanced 4-State Regime Classification (v41)
        # Quadrant approach: TREND (bull/bear) x VOLATILITY (low/high)
        regime_4state = "BULL_LOW_VOL"  # Default
        if USE_4STATE_REGIME:
            # Determine trend: BULL if SPY above VWAP by threshold, else BEAR
            is_bull = spy_vs_vwap_pct >= REGIME_4STATE_TREND_THRESHOLD

            # Determine volatility: HIGH if ATR ratio above threshold, else LOW
            is_high_vol = atr_ratio >= REGIME_4STATE_VOL_THRESHOLD

            # Map to 4-state
            if is_bull and not is_high_vol:
                regime_4state = "BULL_LOW_VOL"
            elif is_bull and is_high_vol:
                regime_4state = "BULL_HIGH_VOL"
            elif not is_bull and not is_high_vol:
                regime_4state = "BEAR_LOW_VOL"
            else:  # not is_bull and is_high_vol
                regime_4state = "BEAR_HIGH_VOL"

            logger.debug(f"[REGIME-4S] {regime_4state} | bull={is_bull} (vwap%={spy_vs_vwap_pct:.2f}) "
                        f"high_vol={is_high_vol} (atr_ratio={atr_ratio:.2f})")

        return RegimeState(
            regime=regime,
            spy_trend=spy_trend,
            spy_vs_vwap_pct=spy_vs_vwap_pct,
            qqq_trend=qqq_trend,
            vix_level=vix_level,
            atr_ratio=atr_ratio,
            timestamp=current_time,
            confidence=confidence,
            polymarket_risk_level=poly_risk_level,
            polymarket_size_mult=poly_size_mult,
            polymarket_recession_prob=poly_recession_prob,
            regime_4state=regime_4state
        )

    def _fetch_intraday_bars(self, symbol: str, limit: int) -> Optional[pd.DataFrame]:
        """Fetch intraday 5-minute bars for a symbol."""
        try:
            now = dt.datetime.now(dt.timezone.utc)

            # Include last 5 days of data to ensure we have enough bars
            # (handles weekends, holidays, early market hours)
            start_date = now - dt.timedelta(days=5)
            from_str = start_date.strftime("%Y-%m-%d")
            to_str = now.strftime("%Y-%m-%d")

            # Use correct Polygon API format: /v2/aggs/ticker/{symbol}/range/{multiplier}/{timespan}/{from}/{to}
            path = f"/v2/aggs/ticker/{symbol}/range/5/minute/{from_str}/{to_str}"

            params = {
                "adjusted": "true",
                "sort": "desc",
                "limit": str(limit * 3),  # Extra to ensure enough bars
            }

            data = self.polygon._request(path, params)

            if not data:
                logger.warning(f"[REGIME] {symbol}: API returned None/empty response")
                return None

            if "results" not in data:
                # Check for error status
                status = data.get("status", "unknown")
                error = data.get("error", data.get("message", "no results key"))
                logger.warning(f"[REGIME] {symbol}: API status={status}, error={error}")
                return None

            if not data["results"]:
                logger.warning(f"[REGIME] {symbol}: API returned empty results array")
                return None

            bars = data["results"][:limit]  # Most recent bars
            bars.reverse()  # Oldest first

            df = pd.DataFrame(bars)
            df.rename(columns={
                "o": "open", "h": "high", "l": "low", "c": "close", "v": "volume", "t": "timestamp"
            }, inplace=True)

            df["datetime"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
            df.set_index("datetime", inplace=True)

            return df

        except Exception as e:
            logger.warning(f"[REGIME] {symbol}: Exception fetching bars: {type(e).__name__}: {e}")
            return None

    def _get_vix_level(self) -> Optional[float]:
        """
        Get volatility level using VIXY directly.

        Returns VIXY price directly (NOT converted to VIX).
        Thresholds have been calibrated for VIXY values:
        - VIXY < 20 = Low volatility
        - VIXY 20-30 = Normal
        - VIXY 30-40 = Elevated (reduce size)
        - VIXY > 40 = High (pause trading)
        """
        try:
            if USE_VIXY_DIRECT:
                # Fetch latest VIXY trade price
                path = f"/v2/last/trade/{VIXY_SYMBOL}"
                data = self.polygon._request(path, {})

                if data and "results" in data:
                    vixy_price = data["results"].get("p", 0)
                    # Return VIXY price directly - thresholds are calibrated for VIXY
                    # No conversion needed since we're using VIXY-specific thresholds
                    logger.debug(f"[REGIME] VIXY price: ${vixy_price:.2f}")
                    return vixy_price

            return None

        except Exception as e:
            logger.debug(f"[REGIME] Failed to get VIXY price: {e}")
            return None

    def _get_polymarket_sentiment(self) -> Optional[dict]:
        """
        Get Polymarket prediction market sentiment (AI Phase 4).

        Returns dict with:
            - risk_level: "LOW", "MEDIUM", "HIGH", "EXTREME"
            - size_mult: Position sizing multiplier
            - recession_prob: Recession probability (0-1)
            - fed_dovish_prob: Fed rate cut probability
            - fed_hawkish_prob: Fed rate hike probability
        """
        now = now_et()

        # Check cache
        if self._polymarket_cache and self._polymarket_cache_time:
            cache_age = (now - self._polymarket_cache_time).total_seconds()
            if cache_age < POLYMARKET_CACHE_SECONDS:
                return self._polymarket_cache

        try:
            # Import here to avoid circular imports and make it optional
            import sys
            utilities_path = str(Path(__file__).parent.parent / "utilities")
            if utilities_path not in sys.path:
                sys.path.insert(0, utilities_path)

            from polymarket_client import PolymarketClient

            client = PolymarketClient()
            sentiment = client.get_market_sentiment()

            result = {
                "risk_level": sentiment.overall_risk_level,
                "size_mult": POLYMARKET_SIZE_ADJUSTMENTS.get(sentiment.overall_risk_level, 1.0),
                "recession_prob": sentiment.recession_prob,
                "fed_dovish_prob": sentiment.fed_dovish_prob,
                "fed_hawkish_prob": sentiment.fed_hawkish_prob,
                "market_bullish_prob": sentiment.market_bullish_prob,
            }

            # Cache the result
            self._polymarket_cache = result
            self._polymarket_cache_time = now

            logger.debug(f"[POLYMARKET] Sentiment fetched: {result['risk_level']} risk, "
                        f"recession={result['recession_prob']*100:.1f}%" if result['recession_prob'] else "")

            return result

        except ImportError:
            logger.debug("[POLYMARKET] polymarket_client not available - skipping")
            return None
        except Exception as e:
            logger.debug(f"[POLYMARKET] Failed to fetch sentiment: {e}")
            return None

    def _analyze_trend(self, df: pd.DataFrame) -> Tuple[str, float]:
        """
        Analyze price trend relative to VWAP and EMAs.

        Returns:
            (trend_direction, vwap_distance_pct)
        """
        if df.empty or len(df) < 5:
            return "FLAT", 0.0

        current_price = df["close"].iloc[-1]

        # Calculate VWAP
        pv = (df["close"] * df["volume"]).cumsum()
        v = df["volume"].cumsum().replace(0, float("nan"))
        vwap = (pv / v).iloc[-1]

        # Calculate EMAs
        ema_9 = df["close"].ewm(span=9, adjust=False).mean().iloc[-1]
        ema_20 = df["close"].ewm(span=20, adjust=False).mean().iloc[-1]

        # VWAP distance
        vwap_pct = ((current_price - vwap) / vwap) * 100 if vwap > 0 else 0.0

        # Determine trend
        if current_price > vwap and ema_9 > ema_20:
            trend = "UP"
        elif current_price < vwap and ema_9 < ema_20:
            trend = "DOWN"
        else:
            trend = "FLAT"

        return trend, vwap_pct

    def _calculate_atr_ratio(self, df: pd.DataFrame) -> float:
        """Calculate current ATR vs average ATR (volatility expansion/contraction)."""
        if df.empty or len(df) < 14:
            return 1.0

        high = df["high"]
        low = df["low"]
        close = df["close"]

        tr1 = high - low
        tr2 = abs(high - close.shift(1))
        tr3 = abs(low - close.shift(1))

        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        atr_current = tr.iloc[-5:].mean()  # Recent 5 bars
        atr_average = tr.mean()  # Overall average

        if atr_average > 0:
            return atr_current / atr_average
        return 1.0

    def _classify_regime(self, spy_trend: str, spy_vs_vwap_pct: float,
                         qqq_trend: str, vix_level: Optional[float],
                         atr_ratio: float) -> Tuple[MarketRegime, float]:
        """
        Classify market regime based on all factors.

        Note: vix_level is actually the raw VIXY price (not converted to VIX).

        Priority:
        1. VOLATILE (high VIXY or ATR expansion) - highest priority
        2. TRENDING_UP/DOWN (clear trend with confirmation)
        3. CHOPPY (no clear direction)
        4. CALM (low volatility, can trade normally)
        """
        confidence = 0.5  # Base confidence

        # Check for volatile regime first (highest priority) - using VIXY thresholds
        if vix_level and vix_level >= REGIME_VIXY_PAUSE:
            return MarketRegime.VOLATILE, 0.9

        if atr_ratio >= REGIME_VOL_EXPANSION:
            confidence = min(0.8, 0.5 + (atr_ratio - REGIME_VOL_EXPANSION) * 0.2)
            return MarketRegime.VOLATILE, confidence

        if vix_level and vix_level >= REGIME_VIXY_CAUTION:
            # Elevated VIXY but not extreme - factor into other classifications
            confidence -= 0.1

        # Check for clear trends
        if spy_trend == "UP":
            # Strong uptrend
            if abs(spy_vs_vwap_pct) >= REGIME_TREND_THRESHOLD:
                if qqq_trend == "UP":  # Confirmation
                    return MarketRegime.TRENDING_UP, 0.85
                return MarketRegime.TRENDING_UP, 0.7

        elif spy_trend == "DOWN":
            # Strong downtrend
            if abs(spy_vs_vwap_pct) >= REGIME_TREND_THRESHOLD:
                if qqq_trend == "DOWN":  # Confirmation
                    return MarketRegime.TRENDING_DOWN, 0.85
                return MarketRegime.TRENDING_DOWN, 0.7

        # Check for choppy conditions
        if abs(spy_vs_vwap_pct) < REGIME_CHOP_THRESHOLD:
            if spy_trend == "FLAT" or spy_trend != qqq_trend:
                return MarketRegime.CHOPPY, 0.7

        # Low volatility calm market (VIXY below low threshold)
        if atr_ratio < 0.8 and (not vix_level or vix_level < REGIME_VIXY_LOW):
            return MarketRegime.CALM, 0.75

        # Default to trending based on SPY
        if spy_trend == "UP":
            return MarketRegime.TRENDING_UP, 0.5
        elif spy_trend == "DOWN":
            return MarketRegime.TRENDING_DOWN, 0.5

        return MarketRegime.CHOPPY, 0.5

    def should_pause_trading(self) -> Tuple[bool, str]:
        """
        Check if trading should be paused based on regime.

        Returns:
            (should_pause, reason)
        """
        regime_state = self.get_regime()
        adjustments = regime_state.get_adjustments()

        if not adjustments.get("allow_trading", True):
            return True, f"Regime {regime_state.regime.value} does not allow trading"

        # Additional VIXY check (volatility too high)
        if regime_state.vix_level and regime_state.vix_level >= REGIME_VIXY_PAUSE:
            return True, f"VIXY too high (${regime_state.vix_level:.2f} >= ${REGIME_VIXY_PAUSE})"

        return False, ""


# Global regime detector - initialize now that class is defined
regime_detector: Optional[RegimeDetector] = None
if ENABLE_REGIME_DETECTION:
    regime_detector = RegimeDetector(polygon)


# ============================================================
# TECHNICAL INDICATORS
# ============================================================

def calculate_ema(series: pd.Series, period: int) -> pd.Series:
    """Calculate Exponential Moving Average."""
    return series.ewm(span=period, adjust=False).mean()

def calculate_vwap(df: pd.DataFrame, anchor_time: dt.datetime = None) -> pd.Series:
    """Calculate VWAP from anchor time."""
    if anchor_time:
        df_filtered = df[df.index >= anchor_time].copy()
    else:
        df_filtered = df.copy()

    if df_filtered.empty:
        return pd.Series(index=df.index, dtype=float)

    pv = (df_filtered["close"] * df_filtered["volume"]).cumsum()
    v = df_filtered["volume"].cumsum().replace(0, float("nan"))
    vwap_vals = pv / v

    # Align with original index
    result = pd.Series(index=df.index, dtype=float)
    result.loc[vwap_vals.index] = vwap_vals
    return result.ffill()


def calculate_adx(df: pd.DataFrame, period: int = ADX_PERIOD) -> pd.Series:
    """
    Calculate ADX (Average Directional Index).

    ADX measures trend strength (not direction):
    - ADX < 15: Weak/no trend (choppy market, avoid)
    - ADX 15-25: Emerging trend (ideal entry zone)
    - ADX 25-50: Strong trend (momentum, but higher risk of reversal)
    - ADX > 50: Very strong trend (exhaustion likely)

    Returns:
        pd.Series: ADX values for each bar
    """
    high = df["high"]
    low = df["low"]
    close = df["close"]

    # Calculate +DM and -DM (Directional Movement)
    plus_dm = high.diff()
    minus_dm = -low.diff()

    # +DM is valid only if > -DM and > 0
    plus_dm = plus_dm.where((plus_dm > minus_dm) & (plus_dm > 0), 0.0)
    # -DM is valid only if > +DM and > 0
    minus_dm = minus_dm.where((minus_dm > plus_dm) & (minus_dm > 0), 0.0)

    # Calculate True Range
    tr1 = high - low
    tr2 = abs(high - close.shift(1))
    tr3 = abs(low - close.shift(1))
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

    # Smoothed averages (Wilder's smoothing = EMA with period*2-1)
    atr = tr.rolling(period).mean()
    plus_di = 100 * (plus_dm.rolling(period).mean() / atr)
    minus_di = 100 * (minus_dm.rolling(period).mean() / atr)

    # DX = |+DI - -DI| / (+DI + -DI) * 100
    dx = 100 * abs(plus_di - minus_di) / (plus_di + minus_di + 1e-10)  # Small epsilon to avoid div by 0

    # ADX = smoothed average of DX
    adx = dx.rolling(period).mean()

    return adx


# ============================================================
# MOMENTUM SCANNER
# ============================================================

@dataclass
class MarketData:
    """Market data for a symbol."""
    symbol: str
    df: pd.DataFrame
    last_price: float
    vwap: float
    ema_fast: float
    ema_slow: float
    relative_volume: float
    spread_bps: float
    atr: Optional[float]  # Average True Range for volatility-adaptive stops
    # Phase 3: Optimization fields
    atr_ma: Optional[float] = None  # ATR moving average for regime detection
    vol_regime: Optional[str] = None  # "HIGH", "NORMAL", "LOW"
    bid_size: Optional[int] = None  # Bid size in shares
    ask_size: Optional[int] = None  # Ask size in shares
    quote_quality: Optional[str] = None  # "GOOD", "POOR"
    # v5 IMPROVEMENTS: ADX for trend strength filtering
    adx: Optional[float] = None  # ADX (Average Directional Index) for trend strength
    atr_ratio: Optional[float] = None  # Current ATR / ATR MA for volatility sizing
    # AI Phase 1: News sentiment
    news_sentiment: Optional[NewsSentiment] = None  # Overall news sentiment
    news_items: Optional[List[NewsItem]] = None  # Individual news articles
    # AI Phase 2: Signal scoring
    signal_score: Optional[float] = None  # ML signal score (0-100)
    signal_features: Optional[SignalFeatures] = None  # Extracted features for the score
    # AI Phase 3: Market regime
    market_regime: Optional[str] = None  # Current market regime (TRENDING_UP, CHOPPY, etc.)
    regime_size_mult: Optional[float] = None  # Regime-based position size multiplier
    regime_min_score: Optional[float] = None  # Regime-adjusted minimum signal score
    # v48c: Gap-up detection
    gap_pct: Optional[float] = None  # Gap % from prev close to today's open
    # Opening Range (ORB) fields
    or_high: Optional[float] = None  # Opening Range high (9:30-10:00 AM)
    or_low: Optional[float] = None   # Opening Range low (9:30-10:00 AM)
    or_set: bool = False             # Whether Opening Range has been established
    or_breakout_status: Optional[str] = None  # "ABOVE", "BELOW", "INSIDE"
    # Multi-Timeframe Confluence (5-min)
    mtf_ema_fast: Optional[float] = None      # 5-min EMA9
    mtf_ema_slow: Optional[float] = None      # 5-min EMA20
    mtf_trend_aligned: Optional[bool] = None  # True if 5-min EMA9 > EMA20
    # v45: Daily Trend Context
    daily_sma: Optional[float] = None         # Daily SMA20 (from resampled minute bars)
    daily_trend_bullish: Optional[bool] = None  # True if price > daily SMA20
    # v45: 5-min Structure Check
    mtf_higher_lows: Optional[bool] = None    # True if 5-min lows are rising
    # v45: Volume Confirmation
    entry_bar_vol_ratio: Optional[float] = None  # Last bar volume / avg bar volume


class MomentumScanner:
    """Scans for momentum setups based on strategy rules."""

    def __init__(self):
        self.cache: Dict[str, Tuple[float, MarketData]] = {}  # symbol -> (timestamp, data)
        self.cache_duration = DATA_REFRESH_SECONDS

    def get_market_data(self, symbol: str, force_refresh: bool = False) -> Optional[MarketData]:
        """Get market data with caching."""
        now_ts = time.time()

        # Check cache
        if not force_refresh and symbol in self.cache:
            cached_ts, cached_data = self.cache[symbol]
            if (now_ts - cached_ts) < self.cache_duration:
                return cached_data

        try:
            # Fetch bars - need RVOL_LOOKBACK_DAYS + buffer for time-of-day RVOL calculation
            end = now_et()
            # Fetch enough days for RVOL calculation (calendar days, accounting for weekends)
            # 20 trading days ≈ 28 calendar days, add buffer
            start = end - dt.timedelta(days=RVOL_LOOKBACK_DAYS + 15)

            df = polygon.get_bars(
                symbol=symbol,
                timespan="minute",
                from_date=start.date().isoformat(),
                to_date=end.date().isoformat(),
                limit=50000  # Increased limit to accommodate more historical data
            )

            if df.empty or len(df) < LOOKBACK_BARS:
                return None

            # Calculate indicators
            df["ema_fast"] = calculate_ema(df["close"], EMA_FAST)
            df["ema_slow"] = calculate_ema(df["close"], EMA_SLOW)

            # VWAP from market open (9:30 AM)
            today = end.date()
            market_open = dt.datetime(today.year, today.month, today.day, 9, 30, tzinfo=ET)
            df["vwap"] = calculate_vwap(df, anchor_time=market_open)

            # Get latest values
            last = df.iloc[-1]
            last_price = float(last["close"])
            vwap_val = float(last["vwap"]) if pd.notna(last["vwap"]) else last_price
            ema_fast_val = float(last["ema_fast"]) if pd.notna(last["ema_fast"]) else last_price
            ema_slow_val = float(last["ema_slow"]) if pd.notna(last["ema_slow"]) else last_price

            # Calculate TRUE relative volume (time-of-day cumulative)
            # This compares today's cumulative volume vs avg cumulative volume at same time-of-day
            rel_vol = polygon.calculate_time_of_day_rvol(df, end, lookback_days=RVOL_LOOKBACK_DAYS)
            if rel_vol is None:
                # Fallback to simple RVOL if time-of-day calculation fails
                # (e.g., not enough historical data, pre-market hours)
                avg_volume = df["volume"].tail(20).mean()
                current_volume = float(last["volume"])
                rel_vol = current_volume / avg_volume if avg_volume > 0 else 0.0
                logger.debug(f"[RVOL] {symbol}: Using fallback simple RVOL = {rel_vol:.2f}x")

            # Get REAL spread from snapshot (lastQuote bid/ask)
            snapshot = polygon.get_snapshot(symbol)
            spread_info = polygon.get_spread_info(snapshot)
            spread_bps = spread_info.get("spread_bps", 999.9)

            # Also update last_price with more current data if available
            current_price = polygon.get_current_price(snapshot)
            if current_price:
                last_price = current_price

            # v48c: Calculate gap percentage from previous close to today's open
            gap_pct = None
            if snapshot:
                try:
                    prev_close = snapshot.get("prevDay", {}).get("c", 0)
                    today_open = snapshot.get("day", {}).get("o", 0)
                    if prev_close and prev_close > 0 and today_open and today_open > 0:
                        gap_pct = (today_open - prev_close) / prev_close * 100
                except (TypeError, KeyError):
                    gap_pct = None

            # Calculate ATR for volatility-adaptive stops
            atr = polygon.calculate_atr(df, period=ATR_PERIOD)

            # Phase 3: Calculate ATR moving average for volatility regime detection
            atr_ma = None
            vol_regime = "NORMAL"
            if atr is not None and len(df) >= ATR_MA_PERIOD:
                # Calculate ATR for each bar
                df_with_atr = df.copy()
                atr_values = []
                for i in range(len(df_with_atr)):
                    if i >= ATR_PERIOD:
                        subset = df_with_atr.iloc[max(0, i-ATR_PERIOD):i+1]
                        period_atr = polygon.calculate_atr(subset, period=ATR_PERIOD)
                        atr_values.append(period_atr if period_atr else 0)
                    else:
                        atr_values.append(0)

                # Get recent ATR values and calculate MA
                recent_atr_values = [v for v in atr_values[-ATR_MA_PERIOD:] if v > 0]
                if recent_atr_values:
                    atr_ma = sum(recent_atr_values) / len(recent_atr_values)

                    # Determine volatility regime
                    if atr > atr_ma * HIGH_VOL_ATR_THRESHOLD:
                        vol_regime = "HIGH"
                    elif atr < atr_ma * LOW_VOL_ATR_THRESHOLD:
                        vol_regime = "LOW"
                    else:
                        vol_regime = "NORMAL"

            # Phase 3: Extract quote quality from snapshot
            bid_size = None
            ask_size = None
            quote_quality = "POOR"
            if snapshot:
                last_quote = snapshot.get("lastQuote", {})
                bid_size = last_quote.get("S")  # Bid size
                ask_size = last_quote.get("s")  # Ask size (lowercase 's' for Polygon)

                # Determine quote quality
                if bid_size and ask_size and bid_size >= MIN_QUOTE_SIZE and ask_size >= MIN_QUOTE_SIZE:
                    quote_quality = "GOOD"

            # v5 IMPROVEMENT #1: Calculate ADX for trend strength filtering
            adx_val = None
            if USE_ADX_FILTER and len(df) >= ADX_PERIOD * 2:
                adx_series = calculate_adx(df, ADX_PERIOD)
                if len(adx_series) > 0 and pd.notna(adx_series.iloc[-1]):
                    adx_val = float(adx_series.iloc[-1])

            # v5 IMPROVEMENT #3: Calculate ATR ratio for dynamic position sizing
            atr_ratio_val = None
            if USE_DYNAMIC_VOL_SIZING and atr is not None and atr_ma is not None and atr_ma > 0:
                atr_ratio_val = atr / atr_ma

            # AI Phase 1: Fetch news sentiment
            news_sentiment_val = None
            news_items_val = None
            if ENABLE_NEWS_FILTER:
                news_sentiment_val, news_items_val = news_fetcher.get_sentiment_summary(symbol)
                if news_sentiment_val != NewsSentiment.NEUTRAL:
                    logger.debug(f"[NEWS] {symbol}: sentiment={news_sentiment_val.value} ({len(news_items_val)} articles)")

            # Opening Range tracking - update OR with today's bar data
            or_high_val = None
            or_low_val = None
            or_set_val = False
            or_breakout_status_val = None

            if USE_OPENING_RANGE_FILTER:
                # Get today's bars for OR calculation
                today_df = df[df.index.date == end.date()] if hasattr(df.index, 'date') else df.tail(390)

                if len(today_df) > 0:
                    # Update OR tracker with today's high/low
                    today_high = today_df["high"].max()
                    today_low = today_df["low"].min()
                    or_tracker.update(symbol, today_high, today_low, end)

                    # Get OR status
                    or_status = or_tracker.get_or_status(symbol, last_price, vwap_val)
                    or_high_val = or_status["or_high"]
                    or_low_val = or_status["or_low"]
                    or_set_val = or_status["or_set"]
                    or_breakout_status_val = or_status["breakout_status"]

            # Multi-Timeframe Confluence: Calculate 5-minute EMAs
            mtf_ema_fast_val = None
            mtf_ema_slow_val = None
            mtf_trend_aligned_val = None

            if USE_5MIN_CONFLUENCE and len(df) >= 100:
                # Resample 1-min bars to 5-min bars
                # Use last 100 5-min bars (500 1-min bars)
                try:
                    df_5m = df.resample('5min').agg({
                        'open': 'first',
                        'high': 'max',
                        'low': 'min',
                        'close': 'last',
                        'volume': 'sum'
                    }).dropna()

                    if len(df_5m) >= MTF_EMA_SLOW + 5:
                        # Calculate EMAs on 5-min timeframe
                        df_5m["ema_fast"] = calculate_ema(df_5m["close"], MTF_EMA_FAST)
                        df_5m["ema_slow"] = calculate_ema(df_5m["close"], MTF_EMA_SLOW)

                        # Get latest 5-min EMA values
                        last_5m = df_5m.iloc[-1]
                        mtf_ema_fast_val = float(last_5m["ema_fast"]) if pd.notna(last_5m["ema_fast"]) else None
                        mtf_ema_slow_val = float(last_5m["ema_slow"]) if pd.notna(last_5m["ema_slow"]) else None

                        # Check trend alignment (5-min EMA9 > EMA20)
                        if mtf_ema_fast_val and mtf_ema_slow_val:
                            min_separation = mtf_ema_slow_val * (1 + MTF_MIN_EMA_SEPARATION_PCT)
                            mtf_trend_aligned_val = mtf_ema_fast_val >= min_separation
                except Exception as e:
                    logger.debug(f"[MTF] {symbol}: Failed to calculate 5-min EMAs: {e}")

            # v45 IMPROVEMENT #6: Daily Trend Context (SMA20 from resampled minute bars)
            daily_sma_val = None
            daily_trend_bullish_val = None

            if USE_DAILY_TREND_FILTER and len(df) >= DAILY_SMA_PERIOD * 390:
                # Resample 1-min bars to daily bars
                try:
                    df_daily = df.resample('D').agg({
                        'open': 'first',
                        'high': 'max',
                        'low': 'min',
                        'close': 'last',
                        'volume': 'sum'
                    }).dropna()

                    if len(df_daily) >= DAILY_SMA_PERIOD:
                        daily_sma_val = float(df_daily["close"].rolling(DAILY_SMA_PERIOD).mean().iloc[-1])
                        if pd.notna(daily_sma_val) and daily_sma_val > 0:
                            buffer = daily_sma_val * (1 - DAILY_SMA_BUFFER_PCT)
                            daily_trend_bullish_val = last_price >= buffer
                        else:
                            daily_sma_val = None
                except Exception as e:
                    logger.debug(f"[DAILY] {symbol}: Failed to calculate daily SMA: {e}")

            # v45 IMPROVEMENT #7: 5-min Higher Lows Structure
            mtf_higher_lows_val = None

            if USE_5MIN_STRUCTURE_FILTER:
                try:
                    df_5m_struct = df.resample('5min').agg({
                        'open': 'first', 'high': 'max',
                        'low': 'min', 'close': 'last', 'volume': 'sum'
                    }).dropna()

                    if len(df_5m_struct) >= 4:
                        recent_lows = df_5m_struct["low"].tail(4).values
                        # Check if at least MTF_HIGHER_LOWS_COUNT of last 3 transitions are higher
                        rising_count = sum(1 for i in range(1, len(recent_lows))
                                          if recent_lows[i] > recent_lows[i-1])
                        mtf_higher_lows_val = rising_count >= MTF_HIGHER_LOWS_COUNT
                except Exception as e:
                    logger.debug(f"[MTF] {symbol}: Failed to check 5-min structure: {e}")

            # v45 IMPROVEMENT #8: Volume Confirmation on Entry Bar
            entry_bar_vol_ratio_val = None

            if USE_VOLUME_CONFIRMATION and len(df) >= 20:
                try:
                    recent_vol = df["volume"].tail(20)
                    avg_vol = recent_vol.mean()
                    last_bar_vol = float(df["volume"].iloc[-1])
                    if avg_vol > 0:
                        entry_bar_vol_ratio_val = last_bar_vol / avg_vol
                except Exception as e:
                    logger.debug(f"[VOL] {symbol}: Failed to calculate bar volume ratio: {e}")

            market_data = MarketData(
                symbol=symbol,
                df=df,
                last_price=last_price,
                vwap=vwap_val,
                ema_fast=ema_fast_val,
                ema_slow=ema_slow_val,
                relative_volume=rel_vol,
                spread_bps=spread_bps,
                atr=atr,
                atr_ma=atr_ma,
                vol_regime=vol_regime,
                bid_size=bid_size,
                ask_size=ask_size,
                quote_quality=quote_quality,
                adx=adx_val,
                atr_ratio=atr_ratio_val,
                news_sentiment=news_sentiment_val,
                news_items=news_items_val,
                or_high=or_high_val,
                or_low=or_low_val,
                or_set=or_set_val,
                or_breakout_status=or_breakout_status_val,
                mtf_ema_fast=mtf_ema_fast_val,
                mtf_ema_slow=mtf_ema_slow_val,
                mtf_trend_aligned=mtf_trend_aligned_val,
                # v45: New multi-timeframe fields
                daily_sma=daily_sma_val,
                daily_trend_bullish=daily_trend_bullish_val,
                mtf_higher_lows=mtf_higher_lows_val,
                entry_bar_vol_ratio=entry_bar_vol_ratio_val,
                gap_pct=gap_pct,
            )

            self.cache[symbol] = (now_ts, market_data)
            return market_data

        except Exception as e:
            logger.debug(f"Failed to get market data for {symbol}: {e}")
            circuit_breaker.record_api_failure("Polygon")
            return None

    def check_long_setup(self, data: MarketData, current_time: dt.datetime = None) -> Tuple[bool, str]:
        """
        Check if symbol meets A+ long setup criteria.

        Returns:
            (passed, reason) - reason is "valid" if passed, or the filter name that rejected it.
        """
        # v5 IMPROVEMENT #2: Time-based filters (avoid open/close volatility)
        if USE_TIME_FILTERS:
            ts = current_time or now_et()
            hour, minute = ts.hour, ts.minute

            # Calculate minutes since market open (9:30 AM ET)
            minutes_since_open = (hour - 9) * 60 + (minute - 30)
            # Calculate minutes until market close (4:00 PM ET)
            minutes_until_close = (16 - hour) * 60 - minute

            if minutes_since_open < NO_TRADE_FIRST_MINUTES:
                logger.debug(f"[SETUP] {data.symbol}: REJECTED - too early in session | "
                            f"only {minutes_since_open} min since open (need {NO_TRADE_FIRST_MINUTES})")
                return False, "too_early"

            if minutes_until_close < NO_TRADE_LAST_MINUTES:
                logger.debug(f"[SETUP] {data.symbol}: REJECTED - too late in session | "
                            f"only {minutes_until_close} min until close (need {NO_TRADE_LAST_MINUTES})")
                return False, "too_late"

        # v5 IMPROVEMENT #1: ADX Filter (avoid strong trends and choppy markets)
        if USE_ADX_FILTER and data.adx is not None:
            if data.adx > MAX_ADX:
                logger.debug(f"[SETUP] {data.symbol}: REJECTED - ADX too high (strong trend) | "
                            f"adx={data.adx:.1f} > max={MAX_ADX}")
                return False, "adx_high"
            if data.adx < MIN_ADX:
                logger.debug(f"[SETUP] {data.symbol}: REJECTED - ADX too low (choppy market) | "
                            f"adx={data.adx:.1f} < min={MIN_ADX}")
                return False, "adx_low"

        # IMPROVEMENT #4: Opening Range Breakout Filter
        # After 10:00 AM, require price to be above Opening Range high
        # Research: 88% of daily high/low set by 10:30 AM, ORB strategies achieve 2.4+ Sharpe
        if USE_OPENING_RANGE_FILTER:
            ts = current_time or now_et()
            or_allowed, or_reason = or_tracker.should_allow_entry(
                data.symbol, data.last_price, data.vwap, ts
            )
            if not or_allowed:
                logger.debug(f"[SETUP] {data.symbol}: REJECTED - OR filter | {or_reason}")
                return False, "or_filter"
            # Log OR status for valid setups
            if data.or_set and data.or_breakout_status:
                logger.debug(f"[SETUP] {data.symbol}: OR confirmed | status={data.or_breakout_status} "
                            f"or_high={data.or_high:.2f} or_low={data.or_low:.2f}")

        # IMPROVEMENT #5: Multi-Timeframe Confluence (5-min EMA confirmation)
        # Require 5-min EMA9 > EMA20 to confirm uptrend on higher timeframe
        if USE_5MIN_CONFLUENCE:
            if data.mtf_trend_aligned is None:
                # No 5-min data available - allow trade but log warning
                logger.debug(f"[SETUP] {data.symbol}: 5-min confluence N/A (insufficient data)")
            elif not data.mtf_trend_aligned:
                logger.debug(f"[SETUP] {data.symbol}: REJECTED - 5-min trend not aligned | "
                            f"5m_ema9={data.mtf_ema_fast:.2f} 5m_ema20={data.mtf_ema_slow:.2f}")
                return False, "5min_trend"
            else:
                logger.debug(f"[SETUP] {data.symbol}: 5-min trend confirmed | "
                            f"5m_ema9={data.mtf_ema_fast:.2f} > 5m_ema20={data.mtf_ema_slow:.2f}")

        # v45 IMPROVEMENT #6: Daily Trend Context
        # Prevent buying stocks that are in a daily downtrend (below SMA20).
        # A random 1-min bounce in a daily downtrend is low probability.
        if USE_DAILY_TREND_FILTER:
            if data.daily_trend_bullish is None:
                logger.debug(f"[SETUP] {data.symbol}: Daily trend N/A (insufficient history)")
            elif not data.daily_trend_bullish:
                logger.debug(f"[SETUP] {data.symbol}: REJECTED - below daily SMA{DAILY_SMA_PERIOD} | "
                            f"price={data.last_price:.2f} sma={data.daily_sma:.2f}")
                return False, "daily_trend"

        # v45 IMPROVEMENT #7: 5-min Higher Lows Structure
        # Beyond EMA alignment, confirm structural uptrend (rising lows on 5-min chart).
        if USE_5MIN_STRUCTURE_FILTER:
            if data.mtf_higher_lows is None:
                logger.debug(f"[SETUP] {data.symbol}: 5-min structure N/A (insufficient data)")
            elif not data.mtf_higher_lows:
                logger.debug(f"[SETUP] {data.symbol}: REJECTED - 5-min lows not rising (no higher lows)")
                return False, "5min_structure"

        # v45 IMPROVEMENT #8: Volume Confirmation on Entry Bar
        # Low-volume breakouts fade more often. Require above-average volume.
        if USE_VOLUME_CONFIRMATION:
            if data.entry_bar_vol_ratio is not None:
                if data.entry_bar_vol_ratio < VOLUME_CONFIRM_MULT:
                    logger.debug(f"[SETUP] {data.symbol}: REJECTED - entry bar volume too low | "
                                f"vol_ratio={data.entry_bar_vol_ratio:.2f}x (need {VOLUME_CONFIRM_MULT}x)")
                    return False, "bar_volume"

        # AI Phase 3: Market Regime Check
        if ENABLE_REGIME_DETECTION and regime_detector is not None:
            # Check if trading should be paused
            should_pause, pause_reason = regime_detector.should_pause_trading()
            if should_pause:
                logger.warning(f"[SETUP] {data.symbol}: BLOCKED - {pause_reason}")
                return False, "regime_pause"

            # Get regime-based adjustments
            regime_state = regime_detector.get_regime()
            adjustments = regime_state.get_adjustments()

            # Store regime info in data for downstream use
            data.market_regime = regime_state.regime.value
            data.regime_min_score = adjustments["min_score"]

            # AI Phase 4: Combine regime size multiplier with Polymarket sentiment multiplier
            base_regime_mult = adjustments["size_mult"]
            polymarket_mult = regime_state.polymarket_size_mult if regime_state.polymarket_size_mult else 1.0

            # v41: Apply 4-state regime multiplier if enabled
            fourstate_mult = 1.0
            if USE_4STATE_REGIME:
                fourstate_adj = regime_state.get_4state_adjustments()
                fourstate_mult = fourstate_adj.get("size_mult", 1.0)

            # Multiply all factors together (compound the adjustments)
            # Use the more conservative of base regime or 4-state regime
            regime_mult = min(base_regime_mult, fourstate_mult)
            data.regime_size_mult = regime_mult * polymarket_mult

            # Log sizing adjustments
            if regime_mult != 1.0 or polymarket_mult != 1.0:
                logger.debug(f"[SETUP] {data.symbol}: Size adjustment - "
                           f"regime={base_regime_mult:.2f}x, 4state={fourstate_mult:.2f}x ({regime_state.regime_4state}), "
                           f"polymarket={polymarket_mult:.2f}x (combined={data.regime_size_mult:.2f}x)")

        # AI Phase 1: News Sentiment Filter
        if ENABLE_NEWS_FILTER and NEWS_BLOCK_ON_BEARISH and data.news_sentiment is not None:
            if data.news_sentiment == NewsSentiment.BEARISH:
                # Log the bearish news that caused the block (with age for transparency)
                bearish_items = [n for n in (data.news_items or [])
                                if n.sentiment == NewsSentiment.BEARISH][:2]
                bearish_headlines = [f"{n.title[:50]}... ({n.age_hours():.1f}h)" for n in bearish_items]
                logger.warning(f"[SETUP] {data.symbol}: BLOCKED - fresh bearish news (<{NEWS_FULL_IMPACT_HOURS}h) | "
                              f"headlines: {bearish_headlines}")
                return False, "bearish_news"

        # v48c: Gap-up filter - only trade stocks with overnight catalysts
        # Gap-up stocks have sustained intraday momentum (earnings, news, upgrades)
        if USE_GAP_UP_FILTER and data.gap_pct is not None:
            if data.gap_pct < MIN_GAP_UP_PCT:
                logger.debug(f"[SETUP] {data.symbol}: REJECTED - no gap-up | gap={data.gap_pct:.2f}% < {MIN_GAP_UP_PCT}%")
                return False, "gap_up"

        # Price must be above VWAP with minimum 0.3% separation
        # Relaxed from 0.5% to 0.3% (2026-01-29) - still confirms uptrend but catches more setups
        min_vwap_distance = data.vwap * 1.003  # 0.3% above VWAP
        if data.last_price < min_vwap_distance:
            logger.debug(f"[SETUP] {data.symbol}: REJECTED - price too close to VWAP | "
                        f"price={data.last_price:.2f} vwap={data.vwap:.2f} min_required={min_vwap_distance:.2f}")
            return False, "vwap_distance"

        # CRITICAL FIX: EMAs must be separated by at least 0.2%
        # Prevents whipsaw when EMAs are too close (flat trend)
        min_ema_separation = data.ema_slow * 1.002  # Fast must be 0.2% above slow
        if data.ema_fast < min_ema_separation:
            logger.debug(f"[SETUP] {data.symbol}: REJECTED - EMA separation too narrow | "
                        f"ema_fast={data.ema_fast:.2f} ema_slow={data.ema_slow:.2f} min_separation={min_ema_separation:.2f}")
            return False, "ema_separation"

        # Relative volume check
        if data.relative_volume < MIN_RELATIVE_VOLUME:
            return False, "rvol_low"

        # Price filter
        if data.last_price < MIN_PRICE:
            return False, "price_low"

        # Spread check
        if data.spread_bps > MAX_SPREAD_BPS:
            return False, "spread_wide"

        # Phase 3: Quote quality check (ensure deep liquidity at entry)
        if data.quote_quality == "POOR":
            logger.debug(f"[SETUP] {data.symbol}: REJECTED - poor quote quality (bid_size={data.bid_size}, ask_size={data.ask_size})")
            return False, "quote_quality"

        # Clean candles check - require 3 of 5 bars higher (relaxed from 4 of 5)
        # Still confirms trend but allows for minor pullbacks
        recent_closes = data.df["close"].tail(5)
        if len(recent_closes) >= 5:
            higher_count = sum(1 for i in range(1, len(recent_closes)) if recent_closes.iloc[i] > recent_closes.iloc[i-1])
            if higher_count < 3:  # Relaxed from 4 to 3
                logger.debug(f"[SETUP] {data.symbol}: REJECTED - insufficient clean candles | "
                            f"only {higher_count}/3 bars higher (need 3 of 5)")
                return False, "clean_candles"

        # Recent momentum filter - require 0.15% gain in last 5 minutes (relaxed from 0.3%)
        # Still requires upward movement but catches earlier in the move
        if len(data.df) >= 5:
            price_5min_ago = data.df["close"].iloc[-5]
            momentum_gain = (data.last_price - price_5min_ago) / price_5min_ago
            if momentum_gain < 0.0015:  # 0.15% minimum gain (was 0.3%)
                logger.debug(f"[SETUP] {data.symbol}: REJECTED - insufficient recent momentum | "
                            f"5-min gain={momentum_gain*100:.2f}% (need 0.15%)")
                return False, "momentum"

        adx_str = f"adx={data.adx:.1f}" if data.adx is not None else "adx=N/A"
        news_str = f"news={data.news_sentiment.value}" if data.news_sentiment else "news=N/A"
        regime_str = f"regime={data.market_regime}" if data.market_regime else "regime=N/A"

        # AI Phase 2: Score the setup using ML signal scorer
        if ENABLE_SIGNAL_SCORING:
            ts = current_time or now_et()
            score, features = signal_scorer.score_setup(data, ts)

            # AI Phase 3: Use regime-adjusted minimum score if available
            min_score = data.regime_min_score if data.regime_min_score else MIN_SIGNAL_SCORE

            # Check if score meets minimum threshold (regime-adjusted)
            if score < min_score:
                logger.info(f"[SETUP] {data.symbol}: REJECTED - signal score too low | "
                           f"score={score:.1f} < min={min_score} (regime={data.market_regime or 'N/A'})")
                return False, "score_low"

            # Store score in data for position sizing (attach to MarketData)
            data.signal_score = score
            data.signal_features = features

            # Log features for future ML training
            signal_scorer.log_features(features)

            score_str = f"score={score:.1f}"
            size_mult = signal_scorer.get_position_size_multiplier(score)
            # AI Phase 3: Apply regime size multiplier on top of score multiplier
            if data.regime_size_mult and data.regime_size_mult != 1.0:
                size_mult *= data.regime_size_mult
                score_str += f" (size={size_mult:.2f}x regime)"
            elif size_mult != 1.0:
                score_str += f" (size={size_mult:.2f}x)"
        else:
            score_str = "score=N/A"

        # Log bullish/great news for visibility
        if ENABLE_NEWS_FILTER and data.news_sentiment in (NewsSentiment.GREAT, NewsSentiment.BULLISH):
            catalyst_headlines = [n.title[:50] for n in (data.news_items or [])
                                 if n.sentiment in (NewsSentiment.GREAT, NewsSentiment.BULLISH)][:2]
            if catalyst_headlines:
                logger.info(f"[NEWS] {data.symbol}: {data.news_sentiment.value} catalyst | {catalyst_headlines}")

        or_str = f"or={data.or_breakout_status}" if data.or_set else "or=N/A"
        mtf_str = f"5m={'Y' if data.mtf_trend_aligned else 'N'}" if data.mtf_trend_aligned is not None else "5m=N/A"
        daily_str = f"d_sma={'Y' if data.daily_trend_bullish else 'N'}" if data.daily_trend_bullish is not None else "d_sma=N/A"
        struct_str = f"hl={'Y' if data.mtf_higher_lows else 'N'}" if data.mtf_higher_lows is not None else "hl=N/A"
        vol_bar_str = f"bar_vol={data.entry_bar_vol_ratio:.1f}x" if data.entry_bar_vol_ratio is not None else "bar_vol=N/A"
        logger.info(f"[SETUP] {data.symbol}: A+ LONG setup detected | price={data.last_price:.2f} vwap={data.vwap:.2f} "
                   f"ema9={data.ema_fast:.2f} ema20={data.ema_slow:.2f} relvol={data.relative_volume:.1f}x "
                   f"vol_regime={data.vol_regime} {adx_str} {or_str} {mtf_str} {daily_str} {struct_str} {vol_bar_str} "
                   f"{news_str} {score_str} {regime_str}")
        return True, "valid"


# ============================================================
# OPENING RANGE TRACKER
# ============================================================
# Tracks the Opening Range (first 30 minutes) for each symbol
# Research shows 88% of daily high/low set by 10:30 AM
# ORB strategies achieve 2.4+ Sharpe ratios

class OpeningRangeTracker:
    """
    Tracks Opening Range (9:30-10:00 AM ET) for each symbol.

    After 10:00 AM, entries require price to break above OR high (for longs).
    This filters out weak stocks that can't hold above their morning range.
    """

    def __init__(self):
        # symbol -> {"high": float, "low": float, "set": bool, "date": str}
        self.ranges: Dict[str, dict] = {}
        self._last_reset_date: Optional[str] = None

    def reset_if_new_day(self) -> None:
        """Reset all Opening Ranges at the start of a new trading day."""
        today = now_et().strftime("%Y-%m-%d")
        if self._last_reset_date != today:
            logger.info(f"[OR] New trading day {today} - resetting Opening Ranges")
            self.ranges.clear()
            self._last_reset_date = today

    def update(self, symbol: str, high: float, low: float, current_time: dt.datetime = None) -> dict:
        """
        Update Opening Range for a symbol during the OR period (9:30-10:00 AM).

        Returns the current OR state for the symbol:
        {"high": float, "low": float, "set": bool, "range_pct": float}
        """
        self.reset_if_new_day()
        ts = current_time or now_et()

        # Calculate minutes since market open
        minutes_since_open = (ts.hour - 9) * 60 + (ts.minute - 30)

        # Initialize if new symbol
        if symbol not in self.ranges:
            self.ranges[symbol] = {
                "high": high,
                "low": low,
                "set": False,
                "bars_seen": 1
            }

        # Update during OR period (first OPENING_RANGE_MINUTES)
        if minutes_since_open < OPENING_RANGE_MINUTES:
            entry = self.ranges[symbol]
            entry["high"] = max(entry["high"], high)
            entry["low"] = min(entry["low"], low)
            entry["bars_seen"] = entry.get("bars_seen", 0) + 1
            # Not set yet - still building
            entry["set"] = False
        elif not self.ranges[symbol]["set"]:
            # OR period just ended - finalize the range
            entry = self.ranges[symbol]
            entry["set"] = True
            range_pct = (entry["high"] - entry["low"]) / entry["low"] if entry["low"] > 0 else 0
            logger.info(f"[OR] {symbol}: Opening Range SET | high={entry['high']:.2f} low={entry['low']:.2f} "
                       f"range={range_pct*100:.2f}% (from {entry.get('bars_seen', 0)} bars)")

        return self.ranges.get(symbol, {"high": 0, "low": 0, "set": False})

    def get_or_status(self, symbol: str, current_price: float, vwap: float = None) -> dict:
        """
        Get Opening Range status for a symbol.

        Returns:
        {
            "or_high": float,
            "or_low": float,
            "or_set": bool,
            "breakout_status": str ("ABOVE", "BELOW", "INSIDE"),
            "range_pct": float,
            "vwap_above_mid": bool,
            "is_valid_range": bool
        }
        """
        entry = self.ranges.get(symbol)
        if not entry:
            return {
                "or_high": None,
                "or_low": None,
                "or_set": False,
                "breakout_status": None,
                "range_pct": None,
                "vwap_above_mid": None,
                "is_valid_range": False
            }

        or_high = entry["high"]
        or_low = entry["low"]
        or_set = entry["set"]

        # Calculate range percentage
        range_pct = (or_high - or_low) / or_low if or_low > 0 else 0

        # Determine breakout status
        breakout_buffer = or_high * OR_BREAKOUT_BUFFER_PCT
        if current_price > or_high + breakout_buffer:
            breakout_status = "ABOVE"
        elif current_price < or_low - breakout_buffer:
            breakout_status = "BELOW"
        else:
            breakout_status = "INSIDE"

        # Check if VWAP is above OR midpoint (bullish sign)
        or_mid = (or_high + or_low) / 2
        vwap_above_mid = vwap > or_mid if vwap else None

        # Validate range (not too tight, not too wide)
        is_valid_range = OR_MIN_RANGE_PCT <= range_pct <= OR_MAX_RANGE_PCT

        return {
            "or_high": or_high,
            "or_low": or_low,
            "or_set": or_set,
            "breakout_status": breakout_status,
            "range_pct": range_pct,
            "vwap_above_mid": vwap_above_mid,
            "is_valid_range": is_valid_range
        }

    def should_allow_entry(self, symbol: str, current_price: float, vwap: float = None,
                          current_time: dt.datetime = None) -> Tuple[bool, str]:
        """
        Check if entry should be allowed based on Opening Range.

        Returns:
        (allowed: bool, reason: str)
        """
        if not USE_OPENING_RANGE_FILTER:
            return (True, "OR filter disabled")

        ts = current_time or now_et()
        minutes_since_open = (ts.hour - 9) * 60 + (ts.minute - 30)

        # During OR period (first 30 min), use existing logic (no OR filter)
        if minutes_since_open < OPENING_RANGE_MINUTES:
            return (True, "Within OR period - using standard filters")

        status = self.get_or_status(symbol, current_price, vwap)

        # OR not set yet (shouldn't happen after 10:00 AM, but be safe)
        if not status["or_set"]:
            return (True, "OR not established - using standard filters")

        # Check range validity
        if not status["is_valid_range"]:
            range_pct = status["range_pct"] or 0
            if range_pct < OR_MIN_RANGE_PCT:
                return (False, f"OR range too tight ({range_pct*100:.2f}% < {OR_MIN_RANGE_PCT*100:.1f}%)")
            if range_pct > OR_MAX_RANGE_PCT:
                return (False, f"OR range too wide ({range_pct*100:.2f}% > {OR_MAX_RANGE_PCT*100:.1f}%)")

        # Require breakout above OR high for longs
        if status["breakout_status"] != "ABOVE":
            return (False, f"Price not above OR high | price={current_price:.2f} or_high={status['or_high']:.2f}")

        # Optional: Check VWAP above OR midpoint
        if OR_REQUIRE_VWAP_ABOVE_MID and status["vwap_above_mid"] is False:
            return (False, f"VWAP below OR midpoint | vwap={vwap:.2f} or_mid={(status['or_high']+status['or_low'])/2:.2f}")

        return (True, f"OR breakout confirmed | price={current_price:.2f} > or_high={status['or_high']:.2f}")


# Global instance
or_tracker = OpeningRangeTracker()

scanner = MomentumScanner()


# ============================================================
# TRADE INTENT STATE MACHINE
# ============================================================

class TradeState(Enum):
    """Trade lifecycle states for order group state machine."""
    NEW = "NEW"                           # Intent created, orders not yet submitted
    SUBMITTED = "SUBMITTED"               # Orders submitted to broker
    PARTIALLY_FILLED = "PARTIALLY_FILLED" # At least one bracket filled
    FILLED = "FILLED"                     # Both brackets filled (or single bracket filled if other cancelled)
    ACTIVE_EXITS = "ACTIVE_EXITS"         # Position open with active TP/SL brackets at broker
    CLOSED = "CLOSED"                     # All brackets closed (TP/SL hit or manual close)
    FAILED = "FAILED"                     # Entry timeout or error
    CANCELLED = "CANCELLED"               # Cancelled before fill


# ---------------------------------------------------------------------------
# Patch 8 — fill classification helper (2026-04-20)
#
# Background: on 2026-04-20 a ladder-mode entry on MP filled 18/18 shares via
# a single OTO scalp order, but `sync_intent_with_broker_orders` tagged the
# intent as PARTIALLY_FILLED (because only the "scalp" leg filled — the
# runner leg is software-managed in ladder mode and has no broker order to
# fill). The 45-second entry-timeout handler then read that state, took the
# "partial fill" branch, cancelled the protective stop, and flattened a
# fully-established position for a -$1.62 scratch.
#
# Root cause: legacy logic treated `runner_qty > 0` as "expecting a separate
# runner broker order". In ladder mode the full position ships in one OTO
# entry and `runner_qty` is re-purposed to track the software-managed runner
# leg. The caller needs a single authoritative classifier that understands
# all three entry shapes (single-bracket, two-bracket, ladder) and falls
# back to the broker position qty when flags lag reality.
# ---------------------------------------------------------------------------

def classify_fill_status(
    total_qty: int,
    runner_qty: int,
    runner_order_id: Optional[str],
    scalp_filled: bool,
    runner_filled: bool,
    scalp_qty: int = 0,
    broker_position_qty: int = 0,
) -> str:
    """Classify an entry's fill status as 'none' / 'partial' / 'full'.

    Priority of evidence, highest first:
      1. Broker position qty >= total_qty  -> 'full'   (authoritative, any mode)
      2. Broker position qty 1..total_qty-1 -> 'partial' (safety: never cancel
         stops while the broker reports ANY position on this symbol)
      3. Single-bracket mode (runner_qty == 0): scalp_filled and scalp_qty
         covers total_qty -> 'full', else 'partial' if scalp_filled else 'none'
      4. Ladder mode (runner_qty > 0 and runner_order_id is None): the single
         OTO entry covers the full position, so scalp_filled alone means 'full'
      5. Legacy two-bracket mode (runner_qty > 0 with a runner_order_id):
         both scalp_filled and runner_filled -> 'full'; exactly one -> 'partial'
      6. Otherwise -> 'none'
    """
    if total_qty <= 0:
        return 'none'

    # Broker is always authoritative when it reports a position.
    if broker_position_qty >= total_qty:
        return 'full'
    if 0 < broker_position_qty < total_qty:
        # Broker has SOMETHING. Do not treat as 'none' — we must not cancel
        # protective stops while naked shares are outstanding.
        return 'partial'

    is_single_bracket = (runner_qty == 0)
    is_ladder_mode = (runner_qty > 0 and runner_order_id is None)

    if is_single_bracket:
        if scalp_filled and scalp_qty >= total_qty:
            return 'full'
        return 'partial' if scalp_filled else 'none'

    if is_ladder_mode:
        # Single OTO carries the full position. Scalp fill == entire fill.
        return 'full' if scalp_filled else 'none'

    # Legacy two-bracket: need both legs.
    if scalp_filled and runner_filled:
        return 'full'
    if scalp_filled or runner_filled:
        return 'partial'
    return 'none'


@dataclass
class TradeIntent:
    """
    Single source of truth for a trade with dual-bracket architecture.

    Tracks both scalp and runner brackets as a cohesive order group.
    Persisted to disk to survive bot restarts.
    """
    symbol: str
    state: TradeState
    created_at: str  # ISO timestamp

    # Order tracking
    scalp_order_id: Optional[str] = None
    runner_order_id: Optional[str] = None
    scalp_client_order_id: Optional[str] = None  # For idempotency
    runner_client_order_id: Optional[str] = None  # For idempotency

    # Fill tracking
    scalp_filled: bool = False
    runner_filled: bool = False
    scalp_fill_price: Optional[float] = None
    runner_fill_price: Optional[float] = None

    # Position details
    total_qty: int = 0
    scalp_qty: int = 0
    runner_qty: int = 0
    entry_limit: float = 0.0
    stop_price: float = 0.0
    scalp_tp_price: float = 0.0
    runner_tp_price: float = 0.0

    # Lifecycle timestamps
    submitted_at: Optional[str] = None
    filled_at: Optional[str] = None
    closed_at: Optional[str] = None

    # Timeout tracking
    timeout_at: Optional[str] = None  # When to cancel if not filled

    # Software trailing stop state (persisted for crash recovery)
    trailing_activated: bool = False
    highest_price_seen: float = 0.0
    trailing_stop_price: float = 0.0

    # v51: Multi-level TP ladder state (software-managed, all persisted)
    tp1_price: float = 0.0
    tp1_qty: int = 0
    tp1_filled: bool = False
    tp1_fill_price: Optional[float] = None
    tp1_filled_at: Optional[str] = None

    tp2_price: float = 0.0
    tp2_qty: int = 0
    tp2_filled: bool = False
    tp2_fill_price: Optional[float] = None
    tp2_filled_at: Optional[str] = None

    # runner_qty already exists above (used for the runner leg)
    stop_order_id: Optional[str] = None       # Active broker-side stop order (OTO child or resized stop)
    sl_moved_to_breakeven: bool = False

    def to_dict(self) -> dict:
        """Convert to dict for JSON serialization."""
        return {
            "symbol": self.symbol,
            "state": self.state.value,
            "created_at": self.created_at,
            "scalp_order_id": self.scalp_order_id,
            "runner_order_id": self.runner_order_id,
            "scalp_client_order_id": self.scalp_client_order_id,
            "runner_client_order_id": self.runner_client_order_id,
            "scalp_filled": self.scalp_filled,
            "runner_filled": self.runner_filled,
            "scalp_fill_price": self.scalp_fill_price,
            "runner_fill_price": self.runner_fill_price,
            "total_qty": self.total_qty,
            "scalp_qty": self.scalp_qty,
            "runner_qty": self.runner_qty,
            "entry_limit": self.entry_limit,
            "stop_price": self.stop_price,
            "scalp_tp_price": self.scalp_tp_price,
            "runner_tp_price": self.runner_tp_price,
            "submitted_at": self.submitted_at,
            "filled_at": self.filled_at,
            "closed_at": self.closed_at,
            "timeout_at": self.timeout_at,
            "trailing_activated": self.trailing_activated,
            "highest_price_seen": self.highest_price_seen,
            "trailing_stop_price": self.trailing_stop_price,
            # v51: TP ladder state
            "tp1_price": self.tp1_price,
            "tp1_qty": self.tp1_qty,
            "tp1_filled": self.tp1_filled,
            "tp1_fill_price": self.tp1_fill_price,
            "tp1_filled_at": self.tp1_filled_at,
            "tp2_price": self.tp2_price,
            "tp2_qty": self.tp2_qty,
            "tp2_filled": self.tp2_filled,
            "tp2_fill_price": self.tp2_fill_price,
            "tp2_filled_at": self.tp2_filled_at,
            "stop_order_id": self.stop_order_id,
            "sl_moved_to_breakeven": self.sl_moved_to_breakeven,
        }

    @staticmethod
    def from_dict(data: dict) -> 'TradeIntent':
        """Create TradeIntent from dict (JSON deserialization)."""
        return TradeIntent(
            symbol=data["symbol"],
            state=TradeState(data["state"]),
            created_at=data["created_at"],
            scalp_order_id=data.get("scalp_order_id"),
            runner_order_id=data.get("runner_order_id"),
            scalp_client_order_id=data.get("scalp_client_order_id"),
            runner_client_order_id=data.get("runner_client_order_id"),
            scalp_filled=data.get("scalp_filled", False),
            runner_filled=data.get("runner_filled", False),
            scalp_fill_price=data.get("scalp_fill_price"),
            runner_fill_price=data.get("runner_fill_price"),
            total_qty=data.get("total_qty", 0),
            scalp_qty=data.get("scalp_qty", 0),
            runner_qty=data.get("runner_qty", 0),
            entry_limit=data.get("entry_limit", 0.0),
            stop_price=data.get("stop_price", 0.0),
            scalp_tp_price=data.get("scalp_tp_price", 0.0),
            runner_tp_price=data.get("runner_tp_price", 0.0),
            submitted_at=data.get("submitted_at"),
            filled_at=data.get("filled_at"),
            closed_at=data.get("closed_at"),
            timeout_at=data.get("timeout_at"),
            trailing_activated=data.get("trailing_activated", False),
            highest_price_seen=data.get("highest_price_seen", 0.0),
            trailing_stop_price=data.get("trailing_stop_price", 0.0),
            # v51: TP ladder state (defaults make old state files load cleanly)
            tp1_price=data.get("tp1_price", 0.0),
            tp1_qty=data.get("tp1_qty", 0),
            tp1_filled=data.get("tp1_filled", False),
            tp1_fill_price=data.get("tp1_fill_price"),
            tp1_filled_at=data.get("tp1_filled_at"),
            tp2_price=data.get("tp2_price", 0.0),
            tp2_qty=data.get("tp2_qty", 0),
            tp2_filled=data.get("tp2_filled", False),
            tp2_fill_price=data.get("tp2_fill_price"),
            tp2_filled_at=data.get("tp2_filled_at"),
            stop_order_id=data.get("stop_order_id"),
            sl_moved_to_breakeven=data.get("sl_moved_to_breakeven", False),
        )

    def is_entry_timed_out(self) -> bool:
        """Check if entry has timed out (for unfilled orders)."""
        if not self.timeout_at or self.state in (TradeState.FILLED, TradeState.ACTIVE_EXITS, TradeState.CLOSED, TradeState.CANCELLED, TradeState.FAILED):
            return False

        timeout_dt = from_iso(self.timeout_at)
        return now_et() >= timeout_dt

    def get_filled_qty(self) -> int:
        """Get total filled quantity across both brackets."""
        qty = 0
        if self.scalp_filled:
            qty += self.scalp_qty
        if self.runner_filled:
            qty += self.runner_qty
        return qty

    def get_avg_fill_price(self) -> Optional[float]:
        """Calculate weighted average fill price across filled brackets."""
        total_value = 0.0
        total_qty = 0

        if self.scalp_filled and self.scalp_fill_price:
            total_value += self.scalp_fill_price * self.scalp_qty
            total_qty += self.scalp_qty

        if self.runner_filled and self.runner_fill_price:
            total_value += self.runner_fill_price * self.runner_qty
            total_qty += self.runner_qty

        if total_qty == 0:
            return None

        return total_value / total_qty


class TradeManager:
    """
    Manages trade intent lifecycle with disk persistence.

    Provides single source of truth for all active trades.
    Handles state transitions and timeout logic.
    """

    def __init__(self):
        self.intents: Dict[str, TradeIntent] = {}  # symbol -> TradeIntent
        self._lock = threading.Lock()
        self._load_intents()

    def _load_intents(self):
        """Load persisted trade intents from disk."""
        try:
            if os.path.exists(TRADE_INTENTS_PATH):
                with open(TRADE_INTENTS_PATH, "r") as f:
                    data = json.load(f)
                    for symbol, intent_data in data.items():
                        try:
                            self.intents[symbol] = TradeIntent.from_dict(intent_data)
                        except Exception as e:
                            logger.warning(f"[TRADE_MGR] Failed to load intent for {symbol}: {e}")

                logger.info(f"[TRADE_MGR] Loaded {len(self.intents)} trade intent(s) from disk")
        except Exception as e:
            logger.warning(f"[TRADE_MGR] Could not load trade intents: {e}")

    def _save_intents(self):
        """Persist trade intents to disk with atomic write (prevents corruption)."""
        try:
            data = {symbol: intent.to_dict() for symbol, intent in self.intents.items()}
            atomic_write_json(TRADE_INTENTS_PATH, data, indent=2)
            logger.debug(f"[TRADE_MGR] Atomically saved {len(self.intents)} trade intent(s) to disk")
        except Exception as e:
            logger.warning(f"[TRADE_MGR] Could not save trade intents: {e}")

    def create_intent(self, symbol: str, total_qty: int, scalp_qty: int, runner_qty: int,
                     entry_limit: float, stop_price: float, scalp_tp_price: float,
                     runner_tp_price: float) -> TradeIntent:
        """Create new trade intent (NEW state)."""
        with self._lock:
            now = now_et()
            timeout_dt = now + dt.timedelta(seconds=ENTRY_TIMEOUT_SEC)

            intent = TradeIntent(
                symbol=symbol,
                state=TradeState.NEW,
                created_at=iso(now),
                total_qty=total_qty,
                scalp_qty=scalp_qty,
                runner_qty=runner_qty,
                entry_limit=entry_limit,
                stop_price=stop_price,
                scalp_tp_price=scalp_tp_price,
                runner_tp_price=runner_tp_price,
                timeout_at=iso(timeout_dt),
            )

            self.intents[symbol] = intent
            self._save_intents()

            logger.info(f"[TRADE_MGR] {symbol}: Created intent | qty={total_qty} timeout_at={timeout_dt.strftime('%H:%M:%S')}")
            return intent

    def update_intent(self, symbol: str, **kwargs):
        """Update intent fields and persist."""
        with self._lock:
            if symbol not in self.intents:
                logger.warning(f"[TRADE_MGR] {symbol}: Intent not found for update")
                return

            intent = self.intents[symbol]
            for key, value in kwargs.items():
                if hasattr(intent, key):
                    setattr(intent, key, value)

            self._save_intents()

    def transition_state(self, symbol: str, new_state: TradeState, **kwargs):
        """Transition intent to new state with optional field updates."""
        with self._lock:
            if symbol not in self.intents:
                logger.warning(f"[TRADE_MGR] {symbol}: Intent not found for state transition")
                return

            intent = self.intents[symbol]
            old_state = intent.state
            intent.state = new_state

            # Update timestamps based on state
            now_str = iso(now_et())
            if new_state == TradeState.SUBMITTED:
                intent.submitted_at = now_str
            elif new_state in (TradeState.FILLED, TradeState.ACTIVE_EXITS):
                if not intent.filled_at:
                    intent.filled_at = now_str
            elif new_state in (TradeState.CLOSED, TradeState.FAILED, TradeState.CANCELLED):
                intent.closed_at = now_str

            # Apply additional field updates
            for key, value in kwargs.items():
                if hasattr(intent, key):
                    setattr(intent, key, value)

            self._save_intents()
            logger.info(f"[TRADE_MGR] {symbol}: State transition | {old_state.value} -> {new_state.value}")

    def get_intent(self, symbol: str) -> Optional[TradeIntent]:
        """Get intent for symbol."""
        with self._lock:
            return self.intents.get(symbol)

    def remove_intent(self, symbol: str):
        """Remove intent (cleanup after close)."""
        with self._lock:
            if symbol in self.intents:
                del self.intents[symbol]
                self._save_intents()
                logger.debug(f"[TRADE_MGR] {symbol}: Intent removed")

    def get_active_symbols(self) -> List[str]:
        """Get all symbols with active intents (not CLOSED/FAILED/CANCELLED)."""
        with self._lock:
            return [
                symbol for symbol, intent in self.intents.items()
                if intent.state not in (TradeState.CLOSED, TradeState.FAILED, TradeState.CANCELLED)
            ]

    def get_timed_out_intents(self) -> List[TradeIntent]:
        """Get all intents that have timed out."""
        with self._lock:
            return [
                intent for intent in self.intents.values()
                if intent.is_entry_timed_out()
            ]

    def sync_intent_with_broker_orders(self, symbol: str) -> bool:
        """
        LEVEL 3: Sync intent state with broker order status.

        This is the deterministic state reconstruction function that ensures:
        1. Every symbol is in exactly one lifecycle state
        2. State transitions handle partial fills explicitly
        3. Restart can rebuild state from broker + intents deterministically

        Returns:
            True if state was updated, False if no change
        """
        with self._lock:
            intent = self.intents.get(symbol)
            if not intent:
                return False

            # Only sync if intent is in a state where broker updates matter
            if intent.state in (TradeState.CLOSED, TradeState.FAILED, TradeState.CANCELLED):
                return False

            try:
                # Query broker for order status
                scalp_filled = False
                runner_filled = False
                scalp_fill_price = None
                runner_fill_price = None

                # Check scalp bracket
                if intent.scalp_order_id:
                    try:
                        scalp_order = alpaca.get_order(intent.scalp_order_id)
                        if scalp_order["status"] == "filled":
                            scalp_filled = True
                            scalp_fill_price = float(scalp_order["filled_avg_price"])
                            logger.debug(f"[STATE_SYNC] {symbol}: Scalp bracket FILLED @ ${scalp_fill_price:.2f}")
                    except Exception as e:
                        logger.warning(f"[STATE_SYNC] {symbol}: Failed to get scalp order status: {e}")

                # Check runner bracket
                if intent.runner_order_id:
                    try:
                        runner_order = alpaca.get_order(intent.runner_order_id)
                        if runner_order["status"] == "filled":
                            runner_filled = True
                            runner_fill_price = float(runner_order["filled_avg_price"])
                            logger.debug(f"[STATE_SYNC] {symbol}: Runner bracket FILLED @ ${runner_fill_price:.2f}")
                    except Exception as e:
                        logger.warning(f"[STATE_SYNC] {symbol}: Failed to get runner order status: {e}")

                # Determine if state changed
                state_changed = False
                if scalp_filled != intent.scalp_filled or runner_filled != intent.runner_filled:
                    state_changed = True

                    # Update fill tracking
                    intent.scalp_filled = scalp_filled
                    intent.runner_filled = runner_filled
                    if scalp_fill_price:
                        intent.scalp_fill_price = scalp_fill_price
                    if runner_fill_price:
                        intent.runner_fill_price = runner_fill_price

                    # Patch 8: classify fill using mode-aware helper (handles
                    # ladder mode where runner_qty>0 but there's no separate
                    # runner broker order).
                    fill_status = classify_fill_status(
                        total_qty=intent.total_qty,
                        runner_qty=intent.runner_qty,
                        runner_order_id=intent.runner_order_id,
                        scalp_filled=scalp_filled,
                        runner_filled=runner_filled,
                        scalp_qty=intent.scalp_qty,
                        broker_position_qty=0,  # state-sync doesn't requery positions
                    )

                    if fill_status == 'full':
                        old_state = intent.state
                        intent.state = TradeState.ACTIVE_EXITS
                        if not intent.filled_at:
                            intent.filled_at = iso(now_et())
                        is_ladder = intent.runner_qty > 0 and intent.runner_order_id is None
                        mode_label = (
                            "ladder mode — single OTO covers full position"
                            if is_ladder else "both brackets filled"
                        )
                        logger.info(
                            f"[STATE_SYNC] {symbol}: State transition | {old_state.value} -> ACTIVE_EXITS "
                            f"({mode_label})"
                        )

                    elif fill_status == 'partial':
                        # True partial fill (two-bracket mode, only one leg filled).
                        old_state = intent.state
                        intent.state = TradeState.PARTIALLY_FILLED
                        filled_qty = intent.get_filled_qty()
                        logger.warning(
                            f"[STATE_SYNC] {symbol}: State transition | {old_state.value} -> PARTIALLY_FILLED "
                            f"({filled_qty}/{intent.total_qty} shares)"
                        )

                # Save if changed
                if state_changed:
                    self._save_intents()
                    return True

                return False

            except Exception as e:
                logger.error(f"[STATE_SYNC] {symbol}: Failed to sync intent with broker: {e}")
                return False

    def sync_all_active_intents(self):
        """
        LEVEL 3: Sync all active intents with broker state.

        Call this periodically in main loop to ensure state machine
        stays synchronized with broker reality.
        """
        active_symbols = self.get_active_symbols()
        if not active_symbols:
            return

        updated_count = 0
        for symbol in active_symbols:
            if self.sync_intent_with_broker_orders(symbol):
                updated_count += 1

        if updated_count > 0:
            logger.info(f"[STATE_SYNC] Updated {updated_count}/{len(active_symbols)} active intent(s)")

    def cleanup_terminal_intents(self, max_age_hours: int = 24):
        """
        LEVEL 3: Clean up old terminal-state intents to prevent unbounded disk growth.

        Removes intents in CLOSED/FAILED/CANCELLED states that are older than max_age_hours.
        This is safe because:
        1. Terminal states are final - no more transitions
        2. Trade journal has already logged the outcome
        3. Broker has no active orders/positions for these symbols
        """
        with self._lock:
            now = now_et()
            cutoff_time = now - dt.timedelta(hours=max_age_hours)
            removed_count = 0

            for symbol in list(self.intents.keys()):
                intent = self.intents[symbol]

                # Only clean up terminal states
                if intent.state not in (TradeState.CLOSED, TradeState.FAILED, TradeState.CANCELLED):
                    continue

                # Check age based on closed_at timestamp
                if intent.closed_at:
                    closed_dt = from_iso(intent.closed_at)
                    if closed_dt < cutoff_time:
                        del self.intents[symbol]
                        removed_count += 1
                        logger.debug(f"[CLEANUP] {symbol}: Removed old terminal intent (state={intent.state.value}, age={now - closed_dt})")
                        continue

                # Also check created_at — catch stale intents where closed_at was recently set
                # (e.g., ghost intents from weeks ago that were only marked FAILED on latest restart)
                if intent.created_at:
                    created_dt = from_iso(intent.created_at)
                    if created_dt < cutoff_time:
                        del self.intents[symbol]
                        removed_count += 1
                        logger.debug(f"[CLEANUP] {symbol}: Removed stale terminal intent (state={intent.state.value}, created={now - created_dt} ago)")

            if removed_count > 0:
                self._save_intents()
                logger.info(f"[CLEANUP] Removed {removed_count} old terminal intent(s) (>{max_age_hours}h)")


trade_manager = TradeManager()


# ============================================================
# POSITION MANAGEMENT
# ============================================================

@dataclass
class Position:
    """
    Position tracking for dual-bracket architecture.

    Note: Broker manages all exits via bracket orders.
    Bot only tracks for reconciliation and monitoring.
    """
    symbol: str
    qty: int  # Total quantity (scalp + runner combined)
    entry_price: float  # Weighted average fill price
    entry_time: str
    initial_stop: float  # Shared stop for both brackets


class PositionManager:
    """Manages open positions (monitoring only - broker handles exits)."""

    def __init__(self):
        self.positions: Dict[str, Position] = {}
        self._lock = threading.Lock()

    def add_position(self, symbol: str, qty: int, entry_price: float, initial_stop: float):
        """Add new position (both brackets combined)."""
        with self._lock:
            self.positions[symbol] = Position(
                symbol=symbol,
                qty=qty,
                entry_price=entry_price,
                entry_time=iso(now_et()),
                initial_stop=initial_stop
            )
            logger.info(f"[POSITION] Added {symbol}: qty={qty} entry=${entry_price:.2f} stop=${initial_stop:.2f}")

    def remove_position(self, symbol: str):
        """Remove position."""
        with self._lock:
            if symbol in self.positions:
                del self.positions[symbol]
                logger.info(f"[POSITION] Removed {symbol}")

    def get_position(self, symbol: str) -> Optional[Position]:
        """Get position by symbol."""
        with self._lock:
            return self.positions.get(symbol)


position_manager = PositionManager()


# ============================================================
# RISK MANAGER
# ============================================================

class RiskManager:
    """
    Manages risk limits and position sizing.

    Level 3 Enhancement: Uses buying_power for position sizing instead of equity.
    This is more robust for margin accounts and prevents over-leveraging.
    """

    def __init__(self):
        self.start_equity = 0.0
        self.current_equity = 0.0
        self.buying_power = 0.0  # Available capital for trading
        self.daily_pnl = 0.0
        self.halted = False
        self.start_date = None  # Track the date we started tracking
        self.daily_trade_count = 0  # MODERATE FIX: Track daily trades to prevent overtrading

    def _load_risk_state(self) -> Optional[dict]:
        """Load persisted risk state from file."""
        try:
            if os.path.exists(STATE_PATH):
                with open(STATE_PATH, "r") as f:
                    state = json.load(f)
                    return state.get("risk_state")
        except Exception as e:
            logger.debug(f"[RISK] Could not load risk state: {e}")
        return None

    def _save_risk_state(self):
        """Save risk state to file with atomic write (prevents corruption)."""
        try:
            # Load existing state
            state = {}
            if os.path.exists(STATE_PATH):
                with open(STATE_PATH, "r") as f:
                    state = json.load(f)

            # Update risk_state (MODERATE FIX: include daily_trade_count)
            state["risk_state"] = {
                "start_equity": self.start_equity,
                "start_date": self.start_date.isoformat() if self.start_date else None,
                "halted": self.halted,
                "daily_trade_count": self.daily_trade_count
            }

            # Atomic write (prevents corruption on crash)
            atomic_write_json(STATE_PATH, state, indent=2)

            logger.debug(f"[RISK] Atomically saved risk state | start_equity=${self.start_equity:.2f} date={self.start_date}")
        except Exception as e:
            logger.warning(f"[RISK] Could not save risk state: {e}")

    def initialize(self):
        """
        Initialize daily risk tracking with persistent state.

        This prevents daily loss limit bypass via restart:
        - If same trading day: use persisted start_equity
        - If new trading day: reset start_equity to current equity
        """
        account = alpaca.get_account()
        current_equity = float(account["equity"])
        today = now_et().date()

        # Try to load persisted state
        saved_state = self._load_risk_state()

        if saved_state and saved_state.get("start_date"):
            saved_date = dt.date.fromisoformat(saved_state["start_date"])

            if saved_date == today:
                # SAME DAY: Use persisted start_equity (prevents restart bypass)
                self.start_equity = saved_state["start_equity"]
                self.start_date = saved_date
                self.halted = saved_state.get("halted", False)
                self.daily_trade_count = saved_state.get("daily_trade_count", 0)  # MODERATE FIX
                logger.warning(f"[RISK] Restored state from earlier today | start_equity=${self.start_equity:,.2f} trades={self.daily_trade_count}")
                logger.warning(f"[RISK] Daily PnL tracking continues (prevents restart bypass)")
            else:
                # NEW DAY: Reset to current equity
                self.start_equity = current_equity
                self.start_date = today
                self.halted = False
                self.daily_trade_count = 0  # MODERATE FIX: Reset daily trade count
                logger.info(f"[RISK] New trading day detected | resetting start_equity=${self.start_equity:,.2f}")
        else:
            # NO SAVED STATE: Initialize fresh
            self.start_equity = current_equity
            self.start_date = today
            self.halted = False
            logger.info(f"[RISK] No saved state | initializing start_equity=${self.start_equity:,.2f}")

        self.current_equity = current_equity
        self.daily_pnl = self.current_equity - self.start_equity

        # Fetch buying power (Level 3: use this for position sizing instead of equity)
        self.buying_power = float(account.get("buying_power", current_equity))

        # Save state immediately
        self._save_risk_state()

        # Calculate deployable capital from buying power (not equity)
        # In cash accounts: buying_power = cash
        # In margin accounts: buying_power includes leverage (cap it)
        deployable = min(self.buying_power, self.start_equity * MAX_CAPITAL_USAGE_PCT)
        reserve = self.start_equity - deployable

        logger.info(f"[RISK] Initialized | starting_equity=${self.start_equity:,.2f} current=${self.current_equity:,.2f} daily_pnl=${self.daily_pnl:+.2f}")
        logger.info(f"[RISK] Capital allocation | buying_power=${self.buying_power:,.2f} deployable=${deployable:,.2f} ({MAX_CAPITAL_USAGE_PCT:.0%}) | "
                   f"reserve=${reserve:,.2f} ({1-MAX_CAPITAL_USAGE_PCT:.0%})")

        if self.halted:
            logger.error(f"[RISK] HALTED state restored from earlier session - trading remains HALTED")

    def update(self):
        """Update current equity, buying power, and check limits."""
        account = alpaca.get_account()
        self.current_equity = float(account["equity"])
        self.buying_power = float(account.get("buying_power", self.current_equity))
        self.daily_pnl = self.current_equity - self.start_equity

        # Check daily loss limit
        loss_pct = abs(self.daily_pnl / self.start_equity) if self.start_equity > 0 else 0

        if self.daily_pnl < 0 and loss_pct >= MAX_DAILY_LOSS_PCT:
            if not self.halted:
                self.halted = True
                logger.error(f"[RISK] DAILY LOSS LIMIT HIT | loss=${self.daily_pnl:.2f} ({loss_pct:.1%}) - HALTING TRADING")

                # LEVEL 3: Send critical alert
                alerter.send_alert(
                    level="CRITICAL",
                    title="Daily Loss Limit Hit",
                    message=f"Daily loss of {loss_pct:.1%} exceeded limit of {MAX_DAILY_LOSS_PCT:.1%}. Trading HALTED.",
                    context={
                        "daily_pnl": f"${self.daily_pnl:.2f}",
                        "loss_pct": f"{loss_pct:.1%}",
                        "equity": f"${self.current_equity:.2f}",
                        "start_equity": f"${self.start_equity:.2f}"
                    }
                )

                # CRITICAL: Persist halted state immediately (survives restarts)
                self._save_risk_state()
                logger.warning(f"[RISK] Halted state persisted to {STATE_PATH} - survives restarts")

        return self.halted

    def is_trade_limit_reached(self) -> Tuple[bool, Optional[str]]:
        """
        MODERATE FIX: Check if daily trade limit has been reached.

        Returns:
            (limit_reached, reason) tuple
        """
        if self.daily_trade_count >= MAX_DAILY_TRADES:
            reason = f"Daily trade limit reached ({self.daily_trade_count}/{MAX_DAILY_TRADES})"
            return True, reason
        return False, None

    def increment_trade_count(self):
        """
        MODERATE FIX: Increment daily trade counter and persist to state.

        Call this when a new entry order is submitted (not on fill, to prevent double counting).
        """
        self.daily_trade_count += 1
        self._save_risk_state()
        logger.info(f"[RISK] Daily trade count: {self.daily_trade_count}/{MAX_DAILY_TRADES}")

    def calculate_position_size(self, entry_price: float, stop_price: float, vol_regime: str = "NORMAL", symbol: str = None, atr_ratio: float = None, signal_score: float = None, regime_size_mult: float = None) -> int:
        """
        Calculate position size based on risk per trade.

        FIX: Apply all multipliers FIRST, then enforce hard caps at the end.
        This prevents position sizes from exceeding limits after multiplier application.

        Order of operations:
        1. Calculate base shares from risk
        2. Apply volatility multiplier
        3. Apply leveraged ETF adjustment
        4. Apply signal score multiplier
        5. Apply regime multiplier
        6. THEN enforce hard caps (max by value, buying power)
        """
        if entry_price <= 0 or stop_price <= 0:
            return 0

        risk_per_share = abs(entry_price - stop_price)
        if risk_per_share == 0:
            return 0

        # Use buying_power but cap it by MAX_CAPITAL_USAGE_PCT of start_equity
        # This prevents over-leveraging in margin accounts
        deployable_capital = min(self.buying_power, self.start_equity * MAX_CAPITAL_USAGE_PCT)

        # Risk amount in dollars (based on deployable capital)
        risk_dollars = deployable_capital * MAX_RISK_PER_TRADE_PCT

        # Calculate BASE shares from risk (before any multipliers)
        base_shares = int(risk_dollars / risk_per_share)

        # Track all multipliers for logging
        vol_multiplier = 1.0
        score_multiplier = 1.0
        regime_mult = 1.0
        leverage_divisor = 1

        # Step 1: Volatility multiplier
        if VOL_ADJUSTED_SIZING:
            if USE_DYNAMIC_VOL_SIZING and atr_ratio is not None:
                if atr_ratio < VOL_REGIME_LOW_THRESHOLD:
                    vol_multiplier = SIZE_MULT_LOW_VOL
                elif atr_ratio > VOL_REGIME_HIGH_THRESHOLD:
                    vol_multiplier = SIZE_MULT_HIGH_VOL
                else:
                    vol_multiplier = SIZE_MULT_NORMAL_VOL
                logger.debug(f"[RISK] v5 Dynamic sizing: atr_ratio={atr_ratio:.2f} -> multiplier={vol_multiplier:.2f}")
            else:
                if vol_regime == "HIGH":
                    vol_multiplier = HIGH_VOL_SIZE_MULTIPLIER
                elif vol_regime == "LOW":
                    vol_multiplier = LOW_VOL_SIZE_MULTIPLIER

        # Step 2: Leveraged ETF adjustment
        if symbol and symbol in LEVERAGED_SYMBOLS:
            leverage_divisor = 3
            logger.info(f"[RISK] {symbol}: Leveraged ETF detected - will reduce size by 3x")

        # Step 3: Signal score multiplier
        if ENABLE_SIGNAL_SCORING and signal_score is not None:
            score_multiplier = signal_scorer.get_position_size_multiplier(signal_score)

        # Step 4: Regime multiplier
        if ENABLE_REGIME_DETECTION and regime_size_mult is not None:
            regime_mult = regime_size_mult

        # Apply all multipliers to get adjusted shares
        combined_multiplier = vol_multiplier * score_multiplier * regime_mult / leverage_divisor
        adjusted_shares = int(base_shares * combined_multiplier)

        # FIX: ENFORCE HARD CAPS AFTER ALL MULTIPLIERS
        # Cap 1: Maximum position value (percentage of deployable capital)
        max_position_value = deployable_capital * POSITION_SIZE_PCT
        max_shares_by_value = int(max_position_value / entry_price)

        # Cap 2: Maximum risk per trade (ensure risk <= allowed risk even after multipliers)
        max_shares_by_risk = int(risk_dollars / risk_per_share)

        # Cap 3: Ensure we don't exceed buying power
        max_shares_by_buying_power = int(self.buying_power / entry_price)

        # Apply the most restrictive cap
        capped_shares = min(adjusted_shares, max_shares_by_value, max_shares_by_risk, max_shares_by_buying_power)

        # Log if we had to cap
        if capped_shares < adjusted_shares:
            logger.warning(f"[RISK] {symbol}: Position CAPPED | pre_cap={adjusted_shares} -> post_cap={capped_shares} | "
                          f"limits: by_value={max_shares_by_value} by_risk={max_shares_by_risk} by_bp={max_shares_by_buying_power}")

        logger.debug(f"[RISK] Position size: {capped_shares} shares | "
                    f"base={base_shares} combined_mult={combined_multiplier:.2f} "
                    f"(vol={vol_multiplier:.2f} score={score_multiplier:.2f} regime={regime_mult:.2f} leverage=1/{leverage_divisor}) | "
                    f"caps: value={max_shares_by_value} risk={max_shares_by_risk} bp={max_shares_by_buying_power} | "
                    f"deployable=${deployable_capital:.2f} risk_dollars=${risk_dollars:.2f}")

        return max(0, capped_shares)


risk_manager = RiskManager()


# ============================================================
# MARKET REGIME MONITOR (Phase 3)
# ============================================================

class MarketRegimeMonitor:
    """
    Monitors overall market health to pause trading in adverse conditions.

    Checks:
    - SPY/QQQ volatility spikes (crisis mode detection)
    - Market-wide volume collapse (low participation)
    - Trend degradation in market leaders
    """

    def __init__(self):
        self.market_data_cache: Dict[str, MarketData] = {}
        self.last_check_time = 0.0
        self.check_interval = 60.0  # Check every 60 seconds
        self.halted = False
        self.halt_reason = None

    def should_pause_trading(self) -> Tuple[bool, Optional[str]]:
        """
        Check if trading should be paused due to adverse market conditions.

        Returns:
            (should_pause, reason) tuple
        """
        now = time.time()

        # Throttle checks
        if now - self.last_check_time < self.check_interval:
            return self.halted, self.halt_reason

        self.last_check_time = now

        try:
            # Check market leader volatility (SPY/QQQ)
            for symbol in MARKET_TREND_SYMBOLS:
                market_data = scanner.get_market_data(symbol, force_refresh=True)
                if not market_data:
                    continue

                self.market_data_cache[symbol] = market_data

                # Check for ATR spike (crisis mode)
                if market_data.atr and market_data.atr_ma:
                    atr_ratio = market_data.atr / market_data.atr_ma
                    if atr_ratio > MAX_MARKET_ATR_SPIKE:
                        self.halted = True
                        self.halt_reason = f"MARKET CRISIS: {symbol} ATR spike {atr_ratio:.2f}x normal (threshold={MAX_MARKET_ATR_SPIKE}x)"
                        logger.error(f"[REGIME] {self.halt_reason}")
                        return True, self.halt_reason

                # Check for volume collapse
                if market_data.relative_volume < MIN_MARKET_RVOL:
                    self.halted = True
                    self.halt_reason = f"MARKET VOLUME LOW: {symbol} RVOL={market_data.relative_volume:.2f}x (min={MIN_MARKET_RVOL}x)"
                    logger.warning(f"[REGIME] {self.halt_reason}")
                    return True, self.halt_reason

            # All checks passed - resume if previously halted
            if self.halted:
                logger.info("[REGIME] Market conditions normalized - resuming trading")
                self.halted = False
                self.halt_reason = None

            return False, None

        except Exception as e:
            logger.debug(f"[REGIME] Error checking market regime: {e}")
            # On error, don't halt (fail open)
            return False, None

    def get_market_summary(self) -> str:
        """Get summary of market conditions."""
        if not self.market_data_cache:
            return "No market data"

        lines = []
        for symbol, data in self.market_data_cache.items():
            atr_str = f"ATR={data.atr:.2f}" if data.atr else "ATR=N/A"
            atr_ma_str = f"/{data.atr_ma:.2f}" if data.atr_ma else ""
            vol_str = f"RVOL={data.relative_volume:.2f}x" if data.relative_volume else "RVOL=N/A"
            lines.append(f"{symbol}: {atr_str}{atr_ma_str} {vol_str} regime={data.vol_regime}")

        return " | ".join(lines)


market_regime = MarketRegimeMonitor()


# ============================================================
# MAIN TRADING BOT
# ============================================================

class MomentumBot:
    """Main bot orchestrator."""

    def __init__(self):
        self.running = False
        self.last_scan_time = 0.0
        self.scan_interval = 30.0  # Scan every 30 seconds
        self.pending_symbols = set()  # Symbols with working orders (prevent double-entry)
        self._pending_lock = threading.Lock()  # Thread-safety for pending_symbols

        # Gradual EOD reduction tracking
        self.eod_reductions: Dict[str, int] = {}  # symbol -> number of reductions applied
        self.last_eod_reduction_time: Optional[dt.datetime] = None  # Last time we did a reduction pass

        # ENHANCEMENT #1: Dynamic universe discovery
        self.dynamic_universe = set()  # Additional symbols discovered via scanning
        self.last_dynamic_scan_time = 0.0  # Track last universe discovery scan

        # Broker-ownership map (symbol → sleeve name), rebuilt once per trading day.
        # Used by EOD flatten + reconcile to classify dynamic-universe positions
        # that aren't in position_manager.positions. Populated from the broker's
        # order history — client_order_id prefixes are the source of truth.
        self._ownership_map: Dict[str, str] = {}
        self._ownership_map_date: Optional[str] = None

        # v45: Enhanced Market Scanner
        self.market_scanner = None
        if USE_ENHANCED_SCANNER:
            try:
                self.market_scanner = _MarketScanner(
                    polygon_api_key=POLYGON_API_KEY,
                    excluded_symbols=DYNAMIC_EXCLUSION_LIST,
                    core_symbols=set(CORE_SYMBOLS),
                )
                logger.info("[SCANNER] Enhanced MarketScanner initialized")
            except Exception as e:
                logger.warning(f"[SCANNER] Failed to initialize MarketScanner: {e} — using basic discovery")
                self.market_scanner = None

    def run(self):
        """Main bot loop."""
        logger.info("="*60)
        logger.info("SIMPLE BOT STARTING")
        logger.info("="*60)

        # PRODUCTION SAFETY: Verify account and trading status
        self.verify_account_status()

        # Initialize risk manager
        risk_manager.initialize()

        # Reconcile with broker state on startup (avoid "assume flat" scenario)
        self.reconcile_broker_state()

        # LEVEL 3: Clean up old terminal intents (prevent unbounded disk growth)
        trade_manager.cleanup_terminal_intents(max_age_hours=24)

        # Phase 3: Log performance summary from previous sessions
        perf_summary = trade_journal.get_performance_summary(lookback_hours=24)
        if "error" not in perf_summary and "message" not in perf_summary:
            logger.info("[PERFORMANCE] Last 24h Summary:")
            logger.info(f"  Trades: {perf_summary['total_trades']} | "
                       f"Win Rate: {perf_summary['win_rate']:.1%} ({perf_summary['wins']}W-{perf_summary['losses']}L-{perf_summary['scratches']}S)")
            logger.info(f"  Avg R: {perf_summary['avg_r_multiple']:.2f}R | "
                       f"Best: {perf_summary['best_r']:.2f}R | Worst: {perf_summary['worst_r']:.2f}R")
            logger.info(f"  PnL: ${perf_summary['total_pnl_est']:.2f} | "
                       f"Avg/Trade: ${perf_summary['avg_pnl_per_trade']:.2f}")
            logger.info(f"  Exits: {perf_summary['exit_reasons']['take_profits']} TPs, "
                       f"{perf_summary['exit_reasons']['stop_losses']} SLs, "
                       f"{perf_summary['exit_reasons']['timeouts']} Timeouts")
        elif "message" in perf_summary:
            logger.info(f"[PERFORMANCE] {perf_summary['message']}")

        # LEVEL 3: Start WebSocket trade updates stream for real-time execution feedback
        global trade_updates_stream
        trade_updates_stream = TradeUpdatesStream(
            trade_manager=trade_manager,
            trade_journal=trade_journal,
            position_manager=position_manager,
            alerter=alerter
        )
        trade_updates_stream.start()

        # v45: Pre-market gap scan (if scanner available and before market open)
        if self.market_scanner:
            now = dt.datetime.now(ET)
            if now.hour < 9 or (now.hour == 9 and now.minute < 30):
                try:
                    logger.info("[SCANNER] Running pre-market gap scan...")
                    gappers = self.market_scanner.scan_premarket()
                    if gappers:
                        self.dynamic_universe = set(self.market_scanner.get_symbols()) - set(CORE_SYMBOLS)
                        logger.info(f"[SCANNER] Pre-market: {len(gappers)} gappers found, "
                                   f"{len(self.dynamic_universe)} added to dynamic universe")
                except Exception as e:
                    logger.warning(f"[SCANNER] Pre-market scan failed: {e}")

        self.running = True

        # Setup signal handler with graceful shutdown
        def signal_handler(sig, frame):
            logger.info("[SHUTDOWN] Received interrupt signal")
            self.running = False
            if trade_updates_stream:
                trade_updates_stream.stop()
            self.shutdown()
            # Force exit after shutdown to prevent loop from continuing
            sys.exit(0)

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        while self.running:
            try:
                # LEVEL 3 CRITICAL: Check kill switch FIRST (must work even if process stuck)
                kill_triggered, kill_reason = kill_switch.is_triggered()
                if kill_triggered:
                    logger.error(f"[KILL_SWITCH] TRIGGERED: {kill_reason}")
                    kill_switch.execute_emergency_shutdown()
                    self.running = False
                    break

                # Check market session
                session = get_market_session()

                # Log heartbeat (operational monitoring)
                halted = risk_manager.halted or circuit_breaker.is_halted()
                halt_reason = risk_manager.halt_reason if hasattr(risk_manager, 'halt_reason') else circuit_breaker.get_halt_reason()
                heartbeat_logger.log_heartbeat(session, halted, halt_reason)

                if session == MarketSession.CLOSED:
                    # SAFETY: Even when market appears closed, check for lingering positions
                    # that need to be flattened (e.g., Alpaca clock returned is_open=False
                    # before EOD flatten ran, or positions survived overnight)
                    if position_manager.positions:
                        # First check if positions still actually exist at broker
                        try:
                            broker_positions = alpaca.list_positions()
                            broker_symbols = {p["symbol"] for p in broker_positions}
                        except Exception:
                            broker_symbols = set(position_manager.positions.keys())  # assume still open

                        for pos in list(position_manager.positions.values()):
                            if pos.symbol not in broker_symbols:
                                # Position already closed at broker — clean up tracker
                                logger.info(f"[SAFETY] {pos.symbol}: Position already closed at broker — cleaning up tracker")
                                position_manager.remove_position(pos.symbol)
                                intent = trade_manager.get_intent(pos.symbol)
                                if intent:
                                    trade_manager.transition_state(pos.symbol, TradeState.CLOSED)
                                    trade_journal.log_exit(pos.symbol, intent, reason="SAFETY_FLATTEN_MARKET_CLOSED", outcome="UNKNOWN")
                                continue

                            # Position still open — check for pending sell orders before re-flattening
                            try:
                                open_orders = alpaca.get_orders(status="open")
                                has_pending_sell = any(
                                    o["symbol"] == pos.symbol and o.get("side") == "sell"
                                    for o in open_orders
                                )
                            except Exception:
                                has_pending_sell = False

                            if has_pending_sell:
                                logger.info(f"[SAFETY] {pos.symbol}: Pending sell order exists — waiting for fill")
                            else:
                                logger.warning(f"[SAFETY] {pos.symbol}: Market CLOSED, no pending sell — attempting flatten")
                                try:
                                    flatten_result = alpaca.flatten_symbol(pos.symbol)
                                    logger.warning(f"[SAFETY] {pos.symbol}: Emergency flatten | {flatten_result}")
                                    # Only remove from tracker if fully flat
                                    if not flatten_result.get("errors"):
                                        position_manager.remove_position(pos.symbol)
                                        intent = trade_manager.get_intent(pos.symbol)
                                        if intent:
                                            trade_manager.transition_state(pos.symbol, TradeState.CLOSED)
                                            trade_journal.log_exit(pos.symbol, intent, reason="SAFETY_FLATTEN_MARKET_CLOSED", outcome="UNKNOWN")
                                except Exception as e:
                                    logger.error(f"[SAFETY] {pos.symbol}: Failed to flatten: {e}")
                    logger.info("[STATUS] Market closed - waiting...")
                    time.sleep(60)
                    continue

                # Check circuit breaker (API degradation, clock availability)
                if circuit_breaker.is_halted():
                    logger.warning(f"[STATUS] Circuit breaker HALTED | reason: {circuit_breaker.get_halt_reason()}")
                    time.sleep(60)
                    continue

                # Verify Alpaca clock is available (critical for market hours)
                if not circuit_breaker.check_alpaca_clock():
                    time.sleep(60)
                    continue

                # Update risk limits
                if risk_manager.update():
                    logger.warning("[STATUS] Trading halted due to daily loss limit")
                    time.sleep(60)
                    continue

                # Daily equity snapshot (once per trading day)
                equity_snapshot_logger.log_snapshot()

                # Check for end-of-day close time
                if AUTO_CLOSE_EOD:
                    self.check_eod_close()

                # LEVEL 3: Sync all active intents with broker state (handles partial fills)
                trade_manager.sync_all_active_intents()

                # Check for timed out entry orders (CRITICAL: prevents late fills on stale setups)
                self.check_entry_timeouts()

                # Manage existing positions
                self.manage_positions()

                # Clean up stale pending symbols (prevents permanent blocking)
                self.cleanup_pending_symbols()

                # ENHANCEMENT #1: Discover dynamic universe (every 2-5 minutes)
                if ENABLE_DYNAMIC_UNIVERSE and (time.time() - self.last_dynamic_scan_time > DYNAMIC_SCAN_INTERVAL_SEC):
                    if self.market_scanner:
                        # v45: Enhanced scanner with multi-factor scoring
                        try:
                            watchlist = self.market_scanner.scan()
                            new_symbols = set(self.market_scanner.get_symbols())
                            # Exclude core symbols (already in universe) to get net additions
                            dynamic_additions = new_symbols - set(CORE_SYMBOLS)
                            added = dynamic_additions - self.dynamic_universe
                            removed = self.dynamic_universe - dynamic_additions
                            self.dynamic_universe = dynamic_additions

                            if added:
                                top_new = [w for w in watchlist if w.symbol in added][:5]
                                for w in top_new:
                                    logger.info(f"[SCANNER+] {w.symbol}: score={w.quality_score:.0f} | "
                                               f"chg={w.change_pct:+.1f}% RVOL={w.rvol:.1f}x "
                                               f"{'[NEWS] ' + w.catalyst_headline[:50] if w.has_catalyst else ''}")
                            if removed:
                                logger.debug(f"[SCANNER] Dropped {len(removed)}: {', '.join(sorted(removed))}")
                        except Exception as e:
                            logger.warning(f"[SCANNER] Enhanced scan failed: {e} — falling back to basic")
                            self.discover_dynamic_universe()
                    else:
                        self.discover_dynamic_universe()
                    self.last_dynamic_scan_time = time.time()

                # Scan for new setups (throttled)
                if time.time() - self.last_scan_time > self.scan_interval:
                    self.scan_for_setups()
                    self.last_scan_time = time.time()

                time.sleep(5)  # Main loop cadence

            except Exception as e:
                logger.error(f"[ERROR] Main loop error: {e}", exc_info=True)
                time.sleep(10)

        logger.info("[SHUTDOWN] Bot stopped")

    def _has_sector_correlation_conflict(self, new_symbol: str) -> Tuple[bool, Optional[str]]:
        """
        CRITICAL FIX: Check if new position would create sector correlation risk.

        Prevents concentration in correlated sectors (e.g., NVDA + AMD both semiconductors).

        Returns:
            (has_conflict, reason) tuple
        """
        new_sector = SECTOR_MAP.get(new_symbol)
        if not new_sector:
            # Symbol not in sector map - allow trade (fail open)
            return False, None

        # Check current positions for sector conflicts
        for pos in position_manager.positions.values():
            existing_sector = SECTOR_MAP.get(pos.symbol)
            if not existing_sector:
                continue

            if existing_sector == new_sector:
                reason = f"Already have {pos.symbol} in {existing_sector}"
                return True, reason

        # Check pending trade intents for sector conflicts
        for symbol in trade_manager.get_active_symbols():
            if symbol == new_symbol:
                continue  # Skip self
            existing_sector = SECTOR_MAP.get(symbol)
            if existing_sector == new_sector:
                reason = f"Pending trade in {symbol} ({existing_sector})"
                return True, reason

        # Special case: Block ETF + constituent exposure
        # E.g., block QQQ if holding AAPL/MSFT/GOOGL
        if new_sector in ["tech_etf", "broad_market"]:
            # Check if we have tech stocks
            for pos in position_manager.positions.values():
                existing_sector = SECTOR_MAP.get(pos.symbol)
                if existing_sector in ["tech", "semiconductors", "software", "cloud", "cybersecurity"]:
                    reason = f"Have tech stock {pos.symbol}, blocking tech ETF to avoid double exposure"
                    return True, reason

        # Reverse: Block tech stocks if holding tech ETFs
        if new_sector in ["tech", "semiconductors", "software", "cloud", "cybersecurity"]:
            for pos in position_manager.positions.values():
                existing_sector = SECTOR_MAP.get(pos.symbol)
                if existing_sector == "tech_etf":
                    reason = f"Have {pos.symbol} (tech ETF), blocking individual tech stocks"
                    return True, reason

        return False, None

    def scan_for_setups(self):
        """Scan universe for trading setups."""
        # Phase 3: Check market regime before scanning
        should_pause, reason = market_regime.should_pause_trading()
        if should_pause:
            logger.warning(f"[SCAN] Trading paused due to market conditions: {reason}")
            return

        # v48c: SPY VWAP gate - only enter when SPY is above intraday VWAP
        if USE_SPY_VWAP_GATE and ENABLE_REGIME_DETECTION and regime_detector is not None:
            try:
                regime_state = regime_detector.get_regime()
                if regime_state.spy_vs_vwap_pct < SPY_VWAP_MIN_DISTANCE_PCT:
                    logger.info(f"[SCAN] SPY VWAP gate: SPY {regime_state.spy_vs_vwap_pct:+.2f}% vs VWAP "
                               f"(need >= {SPY_VWAP_MIN_DISTANCE_PCT}%) - skipping entries")
                    return
            except Exception as e:
                logger.debug(f"[SCAN] SPY VWAP gate check failed: {e} - continuing anyway")

        # Check if we can take more positions
        current_positions = len(position_manager.positions)
        if current_positions >= MAX_POSITIONS:
            logger.debug(f"[SCAN] Max positions ({MAX_POSITIONS}) reached - skipping scan")
            return

        # MODERATE FIX: Check daily trade limit
        limit_reached, limit_reason = risk_manager.is_trade_limit_reached()
        if limit_reached:
            logger.warning(f"[SCAN] {limit_reason} - skipping scan")
            return

        # Log market conditions summary
        market_summary = market_regime.get_market_summary()

        # ENHANCEMENT #1: Combine core universe + dynamic universe
        # v45: When enhanced scanner is available, order by scanner quality score
        # so the best candidates get evaluated first (important for API rate limits)
        combined_universe = list(CORE_SYMBOLS) + list(self.dynamic_universe)

        if self.market_scanner:
            # Build priority order: scanner-scored symbols first (by score desc), then rest
            scored = {}
            for entry in self.market_scanner.get_watchlist():
                if entry.symbol not in scored:
                    scored[entry.symbol] = entry.quality_score
            # Partition: symbols with scanner scores vs without
            with_score = [(s, scored[s]) for s in combined_universe if s in scored]
            without_score = [s for s in combined_universe if s not in scored]
            with_score.sort(key=lambda x: x[1], reverse=True)
            combined_universe = [s for s, _ in with_score] + without_score

        logger.info(f"[SCAN] Scanning {len(combined_universe)} symbols ({len(CORE_SYMBOLS)} core + {len(self.dynamic_universe)} dynamic) | Market: {market_summary}")

        # Get active symbols from trade manager (prevents double-entry)
        active_symbols = set(trade_manager.get_active_symbols())

        # Collect all valid candidates, then enter the best one (ranked by signal score)
        candidates = []
        rejection_counts = {}

        for symbol in combined_universe:
            # Skip if already in position
            if position_manager.get_position(symbol):
                continue

            # CRITICAL: Skip if symbol has active trade intent (prevent double-entry)
            if symbol in active_symbols:
                intent = trade_manager.get_intent(symbol)
                logger.debug(f"[SCAN] {symbol}: Skipping - active intent in state {intent.state.value} (prevents double-entry)")
                continue

            # Legacy check: also skip if symbol in pending_symbols (belt and suspenders)
            if symbol in self.pending_symbols:
                logger.debug(f"[SCAN] {symbol}: Skipping - pending order exists (prevents double-entry)")
                continue

            # CRITICAL FIX: Check for sector correlation conflicts
            has_conflict, conflict_reason = self._has_sector_correlation_conflict(symbol)
            if has_conflict:
                logger.debug(f"[SCAN] {symbol}: BLOCKED - sector correlation risk | {conflict_reason}")
                continue

            # Get market data
            data = scanner.get_market_data(symbol)
            if not data:
                rejection_counts["no_data"] = rejection_counts.get("no_data", 0) + 1
                continue

            # Check for long setup
            passed, reason = scanner.check_long_setup(data)
            if passed:
                candidates.append((symbol, data))
            else:
                rejection_counts[reason] = rejection_counts.get(reason, 0) + 1

        # Log rejection summary for diagnostics
        scan_diagnostics_logger.record_scan(
            symbols_scanned=len(combined_universe),
            rejection_counts=rejection_counts,
            candidates_found=len(candidates),
            trade_entered=False  # Updated below if we enter
        )

        if rejection_counts:
            sorted_reasons = sorted(rejection_counts.items(), key=lambda x: x[1], reverse=True)
            summary = ", ".join(f"{r}={c}" for r, c in sorted_reasons[:8])
            logger.info(f"[SCAN] Filter funnel: {len(combined_universe)} scanned -> "
                       f"{len(candidates)} candidates | Rejections: {summary}")

        # Enter the highest-scoring candidate (only one trade per scan)
        if candidates:
            candidates.sort(key=lambda x: x[1].signal_score or 0, reverse=True)
            best_symbol, best_data = candidates[0]

            if len(candidates) > 1:
                runners_up = ", ".join(f"{s}({d.signal_score:.0f})" for s, d in candidates[1:4])
                logger.info(f"[SCAN] {len(candidates)} candidates found | BEST: {best_symbol} (score={best_data.signal_score:.0f}) | "
                           f"runners-up: {runners_up}")

            self.enter_long(best_symbol, best_data)
            scan_diagnostics_logger.mark_trade_entered()

    def enter_long(self, symbol: str, data: MarketData):
        """
        Enter long position with DUAL bracket orders (scalp + runner).

        Level 3+ Production Implementation:
        - Uses TradeIntent state machine for proper lifecycle tracking
        - Persists order group state to disk
        - Timeout logic prevents late fills on stale setups

        Architecture:
        - Bracket A (60% of position): Quick profit at 0.75R
        - Bracket B (40% of position): Big move target at 1.75R
        - Both brackets share same stop loss (broker-native protection)
        """
        try:
            # EXPECTANCY GUARD: Check if entry is allowed (PF, symbol blocks, drawdown)
            allowed, guard_reason = expectancy_guard.should_allow_entry(symbol)
            if not allowed:
                logger.warning(f"[ENTRY] {symbol}: BLOCKED by ExpectancyGuard - {guard_reason}")
                return

            # SESSION GUARD: No new entries after late-day cutoff (3:30 PM ET)
            now = now_et()
            cutoff_hour, cutoff_minute = LATE_DAY_CUTOFF_TIME_ET
            cutoff_time = dt.time(cutoff_hour, cutoff_minute, tzinfo=ET)
            current_time = now.time()

            if current_time >= cutoff_time:
                logger.warning(f"[ENTRY] {symbol}: BLOCKED - no new entries after {cutoff_hour}:{cutoff_minute:02d} ET "
                             f"(current time: {now.strftime('%H:%M:%S')})")
                return

            # LEVEL 3 DATA GUARDRAIL: Check if symbol is tradable (halts, suspensions, etc.)
            tradable, reason = alpaca.is_symbol_tradable(symbol)
            if not tradable:
                logger.error(f"[ENTRY] {symbol}: BLOCKED - symbol not tradable | reason: {reason}")
                return

            # CIRCUIT BREAKER: Check snapshot freshness before entry
            snapshot = polygon.get_snapshot(symbol)
            if not circuit_breaker.check_snapshot_freshness(snapshot, symbol):
                logger.warning(f"[ENTRY] {symbol}: Skipping - snapshot data stale or unavailable")
                circuit_breaker.record_api_failure("Polygon")
                return

            # Get quote for marketable limit pricing
            try:
                quote = alpaca.get_latest_quote(symbol)
                initial_ask = float(quote["ap"])
                bid_price = float(quote["bp"])
                mid_price = (bid_price + initial_ask) / 2

                # CRITICAL FIX: Start at mid-price instead of ask to save ~50% of spread
                # This is more aggressive than ask but saves significant cost on mega-caps
                # Will reprice if not filled quickly
                entry_limit = round(mid_price + 0.01, 2)  # Mid + 1 cent for faster fill

                spread_saved = initial_ask - entry_limit
                logger.info(f"[ENTRY] {symbol}: Using mid+$0.01 entry pricing | ask=${initial_ask:.2f} bid=${bid_price:.2f} "
                           f"entry=${entry_limit:.2f} (saves ${spread_saved:.2f} vs. ask)")
            except Exception as e:
                logger.error(f"[ENTRY] {symbol}: Could not get quote for dual bracket orders: {e}")
                circuit_breaker.record_api_failure("Alpaca")
                return

            # Calculate ATR-based stop loss and take profits
            if data.atr and data.atr > 0:
                # Use ATR-based stop (adapts to stock volatility)
                # Choose multiplier based on symbol type
                atr_multiplier = ATR_STOP_MULTIPLIER_LEVERAGED if symbol in LEVERAGED_SYMBOLS else ATR_STOP_MULTIPLIER_DEFAULT
                atr_stop_distance = data.atr * atr_multiplier

                # CRITICAL FIX: Apply minimum stop distance to prevent noise stop-outs
                min_stop_distance = entry_limit * MIN_STOP_DISTANCE_PCT
                stop_distance = max(atr_stop_distance, min_stop_distance)

                # Log if minimum was applied
                if stop_distance > atr_stop_distance:
                    logger.info(f"[ENTRY] {symbol}: ATR stop ${atr_stop_distance:.2f} below minimum ${min_stop_distance:.2f} - using minimum (prevents noise stops)")

                stop_price = round(entry_limit - stop_distance, 2)

                # Calculate risk per share (this is 1R)
                risk_per_share = entry_limit - stop_price

                # Scalp TP: 0.75R (conservative, based on risk)
                scalp_tp_distance = risk_per_share * SCALP_TP_R
                scalp_tp_price = round(entry_limit + scalp_tp_distance, 2)

                # Runner TP: 1.75R (aggressive, based on risk)
                runner_tp_distance = risk_per_share * RUNNER_TP_R
                runner_tp_price = round(entry_limit + runner_tp_distance, 2)

                # Enhanced logging with actual stop distance percentage
                stop_distance_pct = (stop_distance / entry_limit) * 100
                logger.info(f"[ENTRY] {symbol}: ATR={data.atr:.2f} mult={atr_multiplier}x -> stop_dist=${stop_distance:.2f} ({stop_distance_pct:.2f}%) | "
                           f"1R=${risk_per_share:.2f} | scalp_tp={SCALP_TP_R}R=${scalp_tp_price:.2f} | runner_tp={RUNNER_TP_R}R=${runner_tp_price:.2f}")
            else:
                # Fallback to fixed % if ATR unavailable
                stop_price = round(entry_limit * (1 - FALLBACK_STOP_PCT), 2)
                scalp_tp_price = round(entry_limit * (1 + SCALP_TP_PCT), 2)
                runner_tp_price = round(entry_limit * (1 + RUNNER_TP_PCT), 2)
                logger.warning(f"[ENTRY] {symbol}: ATR unavailable - using fixed % stops/targets")

            # Calculate TOTAL position size based on stop distance (with volatility adjustment)
            # CRITICAL FIX: Pass symbol to handle leveraged ETF position sizing
            # v5 IMPROVEMENT: Pass atr_ratio for dynamic position sizing
            # AI Phase 2: Pass signal_score for confidence-based sizing
            # AI Phase 3: Pass regime_size_mult for market regime-based sizing
            total_qty = risk_manager.calculate_position_size(
                entry_limit, stop_price,
                vol_regime=data.vol_regime or "NORMAL",
                symbol=symbol,
                atr_ratio=data.atr_ratio,
                signal_score=data.signal_score,
                regime_size_mult=data.regime_size_mult
            )

            if total_qty <= 0:
                logger.warning(f"[ENTRY] {symbol}: Position size is 0 - skipping")
                return

            # Split position based on config (default: 100% scalp, 0% runner)
            # FIX: Don't force runner_qty >= 1 when RUNNER_BRACKET_PCT is 0
            runner_qty = int(total_qty * RUNNER_BRACKET_PCT)
            scalp_qty = total_qty - runner_qty

            # Sanity check: ensure we have at least 1 share to trade
            if scalp_qty < 1 and runner_qty < 1:
                logger.warning(f"[ENTRY] {symbol}: Position size too small after split - skipping")
                return

            # If runner is disabled but we somehow have remainder, add to scalp
            if runner_qty == 0 and scalp_qty < total_qty:
                scalp_qty = total_qty

            # Calculate R-multiples for logging
            risk_per_share = entry_limit - stop_price
            scalp_r = (scalp_tp_price - entry_limit) / risk_per_share if risk_per_share > 0 else 0
            runner_r = (runner_tp_price - entry_limit) / risk_per_share if risk_per_share > 0 else 0

            # v51: Multi-level TP ladder computation.
            # When USE_TP_LADDER is True, we submit ONE OTO entry for total_qty
            # with a stop-loss child only — TPs are managed software-side via
            # manage_positions() firing partial market sells at TP1 / TP2.
            # Fields stored on TradeIntent for crash-recovery and exit logic.
            tp1_price = 0.0
            tp2_price = 0.0
            tp1_qty = 0
            tp2_qty = 0
            ladder_runner_qty = 0
            use_ladder = USE_TP_LADDER and risk_per_share > 0

            if use_ladder:
                tp1_price = round(entry_limit + (risk_per_share * TP1_R), 2)
                tp2_price = round(entry_limit + (risk_per_share * TP2_R), 2)
                tp1_qty = int(total_qty * TP1_SCALE_PCT)
                tp2_qty = int(total_qty * TP2_SCALE_PCT)
                ladder_runner_qty = total_qty - tp1_qty - tp2_qty

                # If the position is too small to split meaningfully (e.g.
                # total_qty < 4 means some leg rounds to 0), fall back to a
                # single bracket at TP2 — no ladder benefit available.
                if tp1_qty < 1 or tp2_qty < 1 or ladder_runner_qty < 1:
                    logger.warning(
                        f"[ENTRY] {symbol}: Position too small for TP ladder "
                        f"(total={total_qty}, splits={tp1_qty}/{tp2_qty}/{ladder_runner_qty}) "
                        f"— falling back to single bracket"
                    )
                    use_ladder = False
                    tp1_qty = tp2_qty = ladder_runner_qty = 0
                    tp1_price = tp2_price = 0.0

            # Log entry plan
            if use_ladder:
                logger.info(
                    f"[ENTRY] {symbol}: Submitting OTO entry + TP LADDER | "
                    f"total_qty={total_qty} entry=${entry_limit:.2f} stop=${stop_price:.2f}"
                )
                logger.info(
                    f"[ENTRY] {symbol}:   TP1: {tp1_qty} shares @ {TP1_R}R=${tp1_price:.2f} "
                    f"(SL->BE after fill)"
                )
                logger.info(
                    f"[ENTRY] {symbol}:   TP2: {tp2_qty} shares @ {TP2_R}R=${tp2_price:.2f}"
                )
                logger.info(
                    f"[ENTRY] {symbol}:   Runner: {ladder_runner_qty} shares (trailing stop "
                    f"{TRAILING_STOP_ACTIVATION_R}R act / {TRAILING_STOP_DISTANCE_R}R trail)"
                )
            elif runner_qty > 0:
                logger.info(f"[ENTRY] {symbol}: Submitting DUAL brackets | total_qty={total_qty} entry=${entry_limit:.2f} stop=${stop_price:.2f}")
                logger.info(f"[ENTRY] {symbol}:   Scalp bracket: {scalp_qty} shares @ {scalp_r:.1f}R (TP=${scalp_tp_price:.2f})")
                logger.info(f"[ENTRY] {symbol}:   Runner bracket: {runner_qty} shares @ {runner_r:.1f}R (TP=${runner_tp_price:.2f})")
            else:
                logger.info(f"[ENTRY] {symbol}: Submitting SINGLE bracket (scalp only) | qty={scalp_qty} entry=${entry_limit:.2f} stop=${stop_price:.2f}")
                logger.info(f"[ENTRY] {symbol}:   Scalp bracket: {scalp_qty} shares @ {scalp_r:.1f}R (TP=${scalp_tp_price:.2f})")

            # CRITICAL PRE-FLIGHT CHECK: Verify buying power and exposure limits
            # This prevents over-leveraging and rejected orders mid-execution
            try:
                account = alpaca.get_account()
                available_cash = float(account.get("cash", 0))
                current_buying_power = float(account.get("buying_power", 0))

                # Calculate position notional value
                position_notional = total_qty * entry_limit

                # Get current open orders notional
                open_orders = alpaca.get_orders(status="open")
                open_orders_notional = sum(
                    int(o.get("qty", 0)) * float(o.get("limit_price", 0))
                    for o in open_orders
                    if o.get("limit_price") and o.get("qty")
                )

                # Check 1: Position notional vs MAX_CAPITAL_USAGE_PCT
                max_allowed_notional = risk_manager.start_equity * MAX_CAPITAL_USAGE_PCT
                total_exposure = position_notional + open_orders_notional

                if total_exposure > max_allowed_notional:
                    logger.error(f"[PRE-FLIGHT] {symbol}: REJECTED - exceeds max capital usage | "
                               f"position=${position_notional:,.0f} open_orders=${open_orders_notional:,.0f} "
                               f"total=${total_exposure:,.0f} > max=${max_allowed_notional:,.0f}")
                    return

                # Check 2: Buying power available
                if position_notional > current_buying_power:
                    logger.error(f"[PRE-FLIGHT] {symbol}: REJECTED - insufficient buying power | "
                               f"need=${position_notional:,.0f} available=${current_buying_power:,.0f}")
                    return

                # Check 3: Cash available (for cash accounts)
                if position_notional > available_cash * 1.1:  # 10% buffer for price movement
                    logger.warning(f"[PRE-FLIGHT] {symbol}: WARNING - low cash reserves | "
                                 f"need=${position_notional:,.0f} cash=${available_cash:,.0f}")

                logger.debug(f"[PRE-FLIGHT] {symbol}: PASSED | position=${position_notional:,.0f} "
                           f"buying_power=${current_buying_power:,.0f} exposure={total_exposure:,.0f}/{max_allowed_notional:,.0f}")

            except Exception as e:
                logger.error(f"[PRE-FLIGHT] {symbol}: Pre-flight check failed: {e}")
                circuit_breaker.record_api_failure("Alpaca")
                return

            # LEVEL 3 DATA GUARDRAIL: Marketability check at order time (not just scan time)
            # Validate that bid/ask spread and quote sizes are still acceptable NOW
            latest_quote = alpaca.get_latest_quote(symbol)
            if latest_quote:
                bid_price = latest_quote["bp"]
                ask_price = latest_quote["ap"]
                bid_size = latest_quote["bs"]
                ask_size = latest_quote["as"]

                # Calculate spread in basis points
                if bid_price > 0 and ask_price > 0:
                    mid_price = (bid_price + ask_price) / 2
                    spread_bps = ((ask_price - bid_price) / mid_price) * 10000

                    # v31: Tiered spread limits - tighter for core mega-caps, wider for dynamic movers
                    # Core universe (mega-caps) have tighter spreads naturally
                    # Dynamic movers (small/mid caps) commonly have 50-80bps when moving
                    is_core_symbol = symbol in CORE_SYMBOLS
                    max_spread = MAX_ENTRY_SPREAD_BPS_TIGHT if is_core_symbol else MAX_ENTRY_SPREAD_BPS

                    # Check if spread is too wide
                    if spread_bps > max_spread:
                        logger.error(f"[ENTRY] {symbol}: BLOCKED - spread too wide at order time | "
                                   f"spread={spread_bps:.1f}bps > {max_spread:.1f}bps ({'core' if is_core_symbol else 'dynamic'})")
                        return

                    # Check if quote sizes are too small (poor liquidity)
                    if bid_size < MIN_QUOTE_SIZE or ask_size < MIN_QUOTE_SIZE:
                        logger.error(f"[ENTRY] {symbol}: BLOCKED - poor liquidity at order time | "
                                   f"bid_size={bid_size} ask_size={ask_size} < {MIN_QUOTE_SIZE}")
                        return

                    logger.debug(f"[ENTRY] {symbol}: Marketability check PASSED | "
                               f"spread={spread_bps:.1f}bps (max={max_spread:.1f}bps {'core' if is_core_symbol else 'dynamic'}) "
                               f"bid_size={bid_size} ask_size={ask_size}")
                else:
                    logger.warning(f"[ENTRY] {symbol}: Invalid quote data at order time | bid={bid_price} ask={ask_price}")
            else:
                logger.warning(f"[ENTRY] {symbol}: Could not get latest quote for marketability check")

            # CRITICAL: Generate deterministic client_order_ids for idempotency
            # This prevents duplicate submissions on retries
            date_str = now_et().strftime("%Y%m%d")
            scalp_client_order_id = generate_client_order_id(symbol, "scalp", date_str)
            runner_client_order_id = generate_client_order_id(symbol, "runner", date_str)

            # CRITICAL: Create TradeIntent BEFORE submitting orders (state machine NEW -> SUBMITTED)
            intent = trade_manager.create_intent(
                symbol=symbol,
                total_qty=total_qty,
                scalp_qty=scalp_qty,
                runner_qty=runner_qty,
                entry_limit=entry_limit,
                stop_price=stop_price,
                scalp_tp_price=scalp_tp_price,
                runner_tp_price=runner_tp_price
            )

            # Store client_order_ids in intent
            trade_manager.update_intent(
                symbol,
                scalp_client_order_id=scalp_client_order_id,
                runner_client_order_id=runner_client_order_id
            )

            # v51: Populate TP ladder fields on the intent (used by manage_positions
            # to fire TP1/TP2 partial sells; defaults to 0 when ladder is off).
            if use_ladder:
                trade_manager.update_intent(
                    symbol,
                    tp1_price=tp1_price,
                    tp1_qty=tp1_qty,
                    tp2_price=tp2_price,
                    tp2_qty=tp2_qty,
                    # Re-purpose existing runner_qty field to hold the ladder runner
                    runner_qty=ladder_runner_qty,
                )

            # METRICS: Log entry to trade journal
            trade_journal.log_entry(symbol, intent, market_data=data, spread_bps=data.spread_bps if data else None)

            # Legacy: Also add to pending_symbols (belt and suspenders)
            self.pending_symbols.add(symbol)
            logger.debug(f"[ENTRY] {symbol}: Created TradeIntent | timeout={ENTRY_TIMEOUT_SEC}s | client_order_ids={scalp_client_order_id},{runner_client_order_id}")

            # Submit entry order.
            # v51: When use_ladder is True, submit as OTO (stop-loss child only)
            # for the full total_qty — TPs are managed software-side via the ladder.
            # Otherwise use the legacy bracket (TP + SL children) for the scalp qty.
            try:
                if use_ladder:
                    scalp_order = alpaca.submit_order(
                        symbol=symbol,
                        qty=total_qty,  # full position in a single OTO entry
                        side="buy",
                        order_type="limit",
                        limit_price=entry_limit,
                        time_in_force="gtc",
                        order_class="oto",
                        stop_loss={"stop_price": f"{stop_price:.2f}"},
                        client_order_id=scalp_client_order_id  # IDEMPOTENCY
                    )
                    logger.info(
                        f"[ENTRY] {symbol}: OTO entry submitted (ladder mode) | "
                        f"order_id={scalp_order['id']} | client_order_id={scalp_client_order_id} | qty={total_qty}"
                    )
                else:
                    scalp_order = alpaca.submit_order(
                        symbol=symbol,
                        qty=scalp_qty,
                        side="buy",
                        order_type="limit",
                        limit_price=entry_limit,
                        time_in_force="gtc",
                        order_class="bracket",
                        take_profit={"limit_price": f"{scalp_tp_price:.2f}"},
                        stop_loss={"stop_price": f"{stop_price:.2f}"},
                        client_order_id=scalp_client_order_id  # IDEMPOTENCY
                    )
                    logger.info(f"[ENTRY] {symbol}: Scalp bracket submitted | order_id={scalp_order['id']} | client_order_id={scalp_client_order_id} | qty={scalp_qty}")

                # Update intent with parent order ID (used to find stop child leg later)
                trade_manager.update_intent(symbol, scalp_order_id=scalp_order["id"])

            except Exception as e:
                logger.error(f"[ENTRY] {symbol}: Failed to submit entry order: {e}")
                trade_manager.transition_state(symbol, TradeState.FAILED)
                self.pending_symbols.discard(symbol)
                return

            # Submit RUNNER bracket only if runner_qty > 0 and ladder is OFF.
            # In ladder mode the full position is already submitted as one OTO —
            # no separate runner bracket (runner qty is managed software-side).
            if runner_qty > 0 and not use_ladder:
                try:
                    # NOTE: Trailing stops are NOT supported as bracket stop_loss legs in Alpaca.
                    # The previous implementation with trail_amount in stop_loss would be rejected.
                    # FIX: Use standard fixed-stop bracket for runner. Trailing stop requires
                    # separate order management (monitor position, cancel/replace stop when in profit).
                    runner_order = alpaca.submit_order(
                        symbol=symbol,
                        qty=runner_qty,
                        side="buy",
                        order_type="limit",
                        limit_price=entry_limit,
                        time_in_force="gtc",
                        order_class="bracket",
                        take_profit={"limit_price": f"{runner_tp_price:.2f}"},
                        stop_loss={"stop_price": f"{stop_price:.2f}"},
                        client_order_id=runner_client_order_id  # IDEMPOTENCY
                    )
                    logger.info(f"[ENTRY] {symbol}: Runner bracket submitted | order_id={runner_order['id']} | client_order_id={runner_client_order_id} | qty={runner_qty}")

                    # Update intent with runner order ID and transition to SUBMITTED
                    trade_manager.transition_state(symbol, TradeState.SUBMITTED, runner_order_id=runner_order["id"])

                except Exception as e:
                    logger.error(f"[ENTRY] {symbol}: Failed to submit runner bracket: {e}")
                    # Try to cancel scalp bracket if runner fails
                    try:
                        alpaca.cancel_order(scalp_order["id"])
                        logger.warning(f"[ENTRY] {symbol}: Canceled scalp bracket due to runner failure")
                    except Exception:
                        pass
                    trade_manager.transition_state(symbol, TradeState.FAILED)
                    self.pending_symbols.discard(symbol)
                    return

                logger.info(f"[ENTRY] {symbol}: Dual brackets submitted | State: SUBMITTED -> waiting for fills or timeout")
            else:
                # Scalp-only mode (runner disabled) OR ladder mode (single OTO entry)
                trade_manager.transition_state(symbol, TradeState.SUBMITTED)
                mode_label = "OTO entry (ladder)" if use_ladder else "Single bracket (scalp only)"
                logger.info(f"[ENTRY] {symbol}: {mode_label} submitted | State: SUBMITTED -> waiting for fills or timeout")

            # MODERATE FIX: Increment daily trade count after successful order submission
            risk_manager.increment_trade_count()

        except Exception as e:
            logger.error(f"[ENTRY] {symbol}: Failed to enter dual bracket position: {e}")
            # Mark intent as FAILED if it exists
            if trade_manager.get_intent(symbol):
                trade_manager.transition_state(symbol, TradeState.FAILED)
            self.pending_symbols.discard(symbol)

    def check_entry_timeouts(self):
        """
        Check for timed out entry orders and cancel them.

        CRITICAL: Prevents late fills on stale setups after timeout expires.
        This is the production fix for "hope-based" order management.
        """
        timed_out_intents = trade_manager.get_timed_out_intents()

        if not timed_out_intents:
            return

        logger.warning(f"[TIMEOUT] Found {len(timed_out_intents)} timed out intent(s)")

        for intent in timed_out_intents:
            symbol = intent.symbol

            try:
                logger.warning(f"[TIMEOUT] {symbol}: Entry timeout ({ENTRY_TIMEOUT_SEC}s) - checking fill status")

                # CRITICAL FIX: Check fill status FIRST before cancelling any orders
                # This prevents cancelling the protective stop/TP orders when entry is filled
                scalp_filled = False
                runner_filled = False
                scalp_fill_price = None
                runner_fill_price = None

                if intent.scalp_order_id:
                    try:
                        scalp_status = alpaca.get_order(intent.scalp_order_id)
                        if scalp_status["status"] == "filled":
                            scalp_filled = True
                            scalp_fill_price = float(scalp_status["filled_avg_price"])
                    except Exception:
                        pass

                if intent.runner_order_id:
                    try:
                        runner_status = alpaca.get_order(intent.runner_order_id)
                        if runner_status["status"] == "filled":
                            runner_filled = True
                            runner_fill_price = float(runner_status["filled_avg_price"])
                    except Exception:
                        pass

                # Update intent with fill status
                trade_manager.update_intent(
                    symbol,
                    scalp_filled=scalp_filled,
                    runner_filled=runner_filled,
                    scalp_fill_price=scalp_fill_price,
                    runner_fill_price=runner_fill_price
                )

                # Patch 8: Query broker position FIRST — authoritative source of
                # truth that lets us correctly classify ladder-mode fills (where
                # runner_qty>0 but there's no separate runner order) and also
                # detects stale fill data (orders filled but position gone).
                try:
                    current_positions = alpaca.list_positions()
                    pos_entry = next(
                        (pos for pos in current_positions if pos["symbol"] == symbol),
                        None,
                    )
                    broker_position_qty = (
                        int(abs(float(pos_entry["qty"]))) if pos_entry else 0
                    )
                    position_exists = broker_position_qty > 0
                except Exception:
                    broker_position_qty = 0
                    position_exists = False

                fill_status = classify_fill_status(
                    total_qty=intent.total_qty,
                    runner_qty=intent.runner_qty,
                    runner_order_id=intent.runner_order_id,
                    scalp_filled=scalp_filled,
                    runner_filled=runner_filled,
                    scalp_qty=intent.scalp_qty,
                    broker_position_qty=broker_position_qty,
                )
                filled_qty = intent.get_filled_qty()
                is_fully_filled = (fill_status == 'full')

                # Handle based on classification
                if fill_status == 'full':
                    avg_fill_price = intent.get_avg_fill_price()

                    if not position_exists:
                        # Orders show filled but no position at broker — stale
                        # fill data (position was closed during downtime).
                        logger.warning(f"[TIMEOUT] {symbol}: STALE FILL DATA - orders show filled but NO POSITION at broker | "
                                      f"Position was likely closed during downtime. Marking FAILED")
                        trade_manager.transition_state(symbol, TradeState.FAILED)
                        self.pending_symbols.discard(symbol)
                    else:
                        # COMPLETE FILL - DO NOT cancel orders! The stop/TP are protecting the position
                        is_ladder = intent.runner_qty > 0 and intent.runner_order_id is None
                        mode_tag = "ladder mode" if is_ladder else "two-bracket"
                        fill_str = f"${avg_fill_price:.2f}" if avg_fill_price else "n/a"
                        logger.info(f"[TIMEOUT] {symbol}: FULLY FILLED on timeout ({mode_tag}) | "
                                    f"filled_qty={filled_qty}/{intent.total_qty} broker_qty={broker_position_qty} "
                                    f"avg_fill={fill_str} | Transitioning to ACTIVE_EXITS")

                        # Transition to ACTIVE_EXITS since bracket stop/TP are managing the position
                        trade_manager.transition_state(symbol, TradeState.ACTIVE_EXITS)

                        # Remove from pending - position is now active with broker-managed exits
                        self.pending_symbols.discard(symbol)

                        # Log successful entry - emphasize that stop/TP are PRESERVED
                        logger.info(f"[TIMEOUT] {symbol}: Position active - broker stop/TP orders preserved and managing risk")

                elif fill_status == 'partial':
                    # TRUE PARTIAL FILL - flatten immediately. We don't want
                    # partial positions with mismatched bracket legs.
                    avg_fill_price = intent.get_avg_fill_price()
                    fill_str = f"${avg_fill_price:.2f}" if avg_fill_price else "n/a"
                    logger.error(f"[TIMEOUT] {symbol}: PARTIAL FILL on timeout | "
                                 f"filled_qty={filled_qty}/{intent.total_qty} broker_qty={broker_position_qty} "
                                 f"avg_fill={fill_str} | FLATTENING IMMEDIATELY")

                    # Flatten immediately (cancel any remaining orders + close position)
                    try:
                        flatten_result = alpaca.flatten_symbol(symbol)
                        logger.warning(f"[TIMEOUT] {symbol}: Flatten complete | {flatten_result}")
                        trade_manager.transition_state(symbol, TradeState.CLOSED)

                        # METRICS: Log partial fill + flatten to journal
                        trade_journal.log_exit(symbol, intent, reason="TIMEOUT_PARTIAL_FILL", outcome="SCRATCH")
                    except Exception as flatten_err:
                        logger.error(f"[TIMEOUT] {symbol}: Failed to flatten after partial fill: {flatten_err}")
                        trade_manager.transition_state(symbol, TradeState.FAILED)

                        # METRICS: Log failed flatten
                        trade_journal.log_exit(symbol, intent, reason="TIMEOUT_PARTIAL_FILL_FLATTEN_FAILED", outcome="LOSS")

                    # Remove from pending
                    self.pending_symbols.discard(symbol)

                else:
                    # Nothing filled AND broker has no position - safe to cancel.
                    logger.info(f"[TIMEOUT] {symbol}: No fills - cancelling unfilled bracket orders")
                    orders_cancelled = alpaca.cancel_orders_for_symbol(symbol, confirm=True)
                    logger.info(f"[TIMEOUT] {symbol}: Cancelled {orders_cancelled} order(s)")

                    trade_manager.transition_state(symbol, TradeState.CANCELLED)

                    # METRICS: Log timeout cancellation
                    trade_journal.log_exit(symbol, intent, reason="TIMEOUT_NO_FILL", outcome="TIMEOUT")

                    # Remove from pending symbols
                    self.pending_symbols.discard(symbol)

            except Exception as e:
                logger.error(f"[TIMEOUT] {symbol}: Error handling timeout: {e}")
                # Mark as FAILED on error
                trade_manager.transition_state(symbol, TradeState.FAILED)
                self.pending_symbols.discard(symbol)

    # ------------------------------------------------------------
    # Broker-authoritative ownership helpers (for EOD + reconcile)
    # ------------------------------------------------------------
    def _refresh_ownership_map(self, force: bool = False) -> Dict[str, str]:
        """
        Build / refresh the symbol → sleeve ownership map from broker order history.

        Cached per trading day to avoid hammering `/v2/orders?status=all` on
        every 5s tick. Caller passes `force=True` to rebuild mid-day (e.g.
        after a new entry that we want reflected before EOD).
        """
        today = dt.datetime.now(tz=ET).strftime("%Y-%m-%d")
        if not force and self._ownership_map_date == today and self._ownership_map:
            return self._ownership_map

        try:
            all_orders = alpaca.get_orders(status="all")
        except Exception as e:
            logger.warning(f"[OWNERSHIP] get_orders(status=all) failed: {e} — keeping stale map")
            return self._ownership_map

        # Walk orders oldest-first so the LATEST buy for a symbol wins
        # (avoids stale classification when a symbol was re-entered by a
        # different sleeve — unlikely, but safer).
        owner_by_symbol: Dict[str, str] = {}
        for order in reversed(all_orders):
            if order.get("side") != "buy":
                continue
            sym = order.get("symbol")
            if not sym:
                continue
            owner = _classify_order_owner(order.get("client_order_id"))
            if owner != "UNKNOWN":
                owner_by_symbol[sym] = owner

        self._ownership_map = owner_by_symbol
        self._ownership_map_date = today
        logger.info(
            f"[OWNERSHIP] Rebuilt ownership map from broker orders | "
            f"{len(owner_by_symbol)} symbols classified | "
            f"SIMPLE={sum(1 for o in owner_by_symbol.values() if o == 'SIMPLE')}"
        )
        return owner_by_symbol

    def _is_simple_owned(self, symbol: str) -> bool:
        """Return True if the broker position for `symbol` was opened by SIMPLE."""
        return self._refresh_ownership_map().get(symbol) == "SIMPLE"

    def _get_simple_owned_broker_positions(self) -> List[dict]:
        """
        Broker-authoritative list of open positions owned by SIMPLE.

        Source of truth for EOD flatten routines — ensures dynamic-universe
        positions that fell out of `position_manager.positions` (e.g. after
        a mid-session restart where reconcile couldn't link them to an intent)
        still get closed. Filters:
          - TREND_BOT_SYMBOLS (defensive — shouldn't appear under SIMPLE anyway)
          - Short positions (directional_bot territory)
          - Non-SIMPLE prefix (TREND / XASSET / DIR / unknown)
        """
        try:
            broker_positions = alpaca.list_positions()
        except Exception as e:
            logger.error(f"[EOD] Broker position fetch failed: {e}")
            return []
        if not broker_positions:
            return []

        owner_map = self._refresh_ownership_map()
        owned: List[dict] = []
        for bp in broker_positions:
            sym = bp["symbol"]
            if sym in TREND_BOT_SYMBOLS:
                continue
            if bp.get("side", "long") == "short":
                continue  # directional_bot territory
            if owner_map.get(sym) != "SIMPLE":
                continue
            owned.append(bp)
        return owned

    def check_eod_close(self):
        """
        Check if it's time to close positions for end of day.

        Supports two modes:
        1. Gradual reduction (GRADUAL_EOD_REDUCTION=True):
           - Starts reducing EOD_REDUCTION_START_MINUTES before EOD_CLOSE_TIME_ET
           - Reduces EOD_REDUCTION_PERCENT of each position every 5 minutes
           - Improves execution quality and reduces market impact
        2. Immediate flatten (GRADUAL_EOD_REDUCTION=False):
           - Flattens all positions at EOD_CLOSE_TIME_ET

        Source of truth: BROKER POSITIONS classified by client_order_id prefix,
        not `position_manager.positions`. This ensures dynamic-universe symbols
        that fell out of the in-memory position_manager (e.g. after a mid-session
        restart where reconcile couldn't link them to a TradeIntent) still get
        flattened. See `_get_simple_owned_broker_positions()`.
        """
        now = dt.datetime.now(tz=ET)
        eod_hour, eod_minute = EOD_CLOSE_TIME_ET
        eod_time = dt.time(eod_hour, eod_minute, tzinfo=ET)
        current_time = now.time()

        # Calculate reduction window start time
        eod_datetime = now.replace(hour=eod_hour, minute=eod_minute, second=0, microsecond=0)
        reduction_start = eod_datetime - dt.timedelta(minutes=EOD_REDUCTION_START_MINUTES)
        reduction_start_time = reduction_start.time()

        # Fast exit when not in any EOD window (cheap time check; avoids broker call)
        in_reduction_window = (
            GRADUAL_EOD_REDUCTION
            and current_time >= reduction_start_time
            and current_time < eod_time
        )
        in_flatten_window = current_time >= eod_time
        if not in_reduction_window and not in_flatten_window:
            return

        # Broker-authoritative list of SIMPLE-owned positions (see docstring)
        own_broker_positions = self._get_simple_owned_broker_positions()
        if not own_broker_positions:
            return

        # --- MODE 1: Gradual EOD Reduction ---
        if in_reduction_window:
            # Check if enough time has passed since last reduction
            if self.last_eod_reduction_time:
                minutes_since_last = (now - self.last_eod_reduction_time).total_seconds() / 60
                if minutes_since_last < EOD_REDUCTION_INTERVAL_MINUTES:
                    return  # Not time for next reduction yet

            logger.info(f"[EOD-GRADUAL] Starting position reduction pass | {len(own_broker_positions)} position(s) | "
                       f"interval={EOD_REDUCTION_INTERVAL_MINUTES}min | reduction={EOD_REDUCTION_PERCENT*100:.0f}%")
            self.last_eod_reduction_time = now

            for bp in own_broker_positions:
                symbol = bp["symbol"]
                try:
                    current_qty = int(float(bp.get("qty", 0)))
                    if current_qty <= 0:
                        continue

                    # Calculate shares to reduce
                    reduce_qty = max(1, int(current_qty * EOD_REDUCTION_PERCENT))

                    # Track total reductions for logging
                    self.eod_reductions[symbol] = self.eod_reductions.get(symbol, 0) + 1
                    reduction_num = self.eod_reductions[symbol]

                    logger.info(f"[EOD-GRADUAL] {symbol}: Reducing {reduce_qty}/{current_qty} shares "
                               f"(reduction #{reduction_num})")

                    # Submit market sell order for partial reduction
                    order = alpaca.submit_order(
                        symbol=symbol,
                        qty=reduce_qty,
                        side="sell",
                        order_type="market",
                        time_in_force="gtc"
                    )

                    if order:
                        logger.info(f"[EOD-GRADUAL] {symbol}: Reduction order submitted | "
                                   f"order_id={order.get('id', 'N/A')}")

                        # Update position manager if it knows about this symbol
                        remaining_qty = current_qty - reduce_qty
                        pm_pos = position_manager.get_position(symbol)
                        if remaining_qty <= 0:
                            if pm_pos:
                                position_manager.remove_position(symbol)
                            self.pending_symbols.discard(symbol)
                            if trade_manager.get_intent(symbol):
                                trade_manager.transition_state(symbol, TradeState.CLOSED)
                            logger.info(f"[EOD-GRADUAL] {symbol}: Position fully closed")
                        elif pm_pos:
                            pm_pos.qty = remaining_qty

                except Exception as e:
                    logger.error(f"[EOD-GRADUAL] {symbol}: Error during reduction: {e}")

            return  # Don't proceed to full flatten yet

        # --- MODE 2: Full Flatten at EOD Time ---
        if in_flatten_window:
            # Reset reduction tracking for next day
            self.eod_reductions.clear()
            self.last_eod_reduction_time = None

            logger.warning(f"[EOD] Closing {len(own_broker_positions)} remaining position(s) at end of day")
            failed_flattens = []

            for bp in own_broker_positions:
                symbol = bp["symbol"]
                try:
                    logger.info(f"[EOD] Flattening {symbol} (cancel orders + close position)")
                    result = alpaca.flatten_symbol(symbol)

                    if not result["errors"]:
                        if position_manager.get_position(symbol):
                            position_manager.remove_position(symbol)
                        self.pending_symbols.discard(symbol)
                        if trade_manager.get_intent(symbol):
                            trade_manager.transition_state(symbol, TradeState.CLOSED)
                            logger.info(f"[EOD] {symbol}: Intent transitioned to CLOSED")
                    else:
                        logger.error(f"[EOD] {symbol}: Errors during flatten: {result['errors']}")
                        failed_flattens.append(symbol)
                except Exception as e:
                    logger.error(f"[EOD] Failed to flatten {symbol}: {e}")
                    failed_flattens.append(symbol)

            # LEVEL 3: Alert if any flattens failed
            if failed_flattens:
                remaining_positions = alpaca.get_positions()
                remaining_orders = alpaca.get_orders(status="open")

                alerter.send_alert(
                    level="CRITICAL",
                    title="EOD Flatten Failed",
                    message=f"Failed to flatten {len(failed_flattens)} position(s) at EOD. "
                           f"{len(remaining_positions)} positions and {len(remaining_orders)} orders remain.",
                    context={
                        "failed_symbols": failed_flattens,
                        "remaining_positions": len(remaining_positions),
                        "remaining_orders": len(remaining_orders),
                        "eod_time": f"{eod_hour}:{eod_minute:02d} ET"
                    }
                )

    def cleanup_pending_symbols(self):
        """
        Clean up pending_symbols set by removing symbols with no open orders and no position.

        This prevents symbols from getting stuck in pending_symbols forever due to:
        - Partial fills where one bracket filled but the other didn't
        - Order cancellations
        - Manual interventions
        """
        if not self.pending_symbols:
            return

        try:
            # Get all open orders and positions from broker
            open_orders = alpaca.get_orders(status="open")
            open_order_symbols = {order["symbol"] for order in open_orders}

            broker_positions = alpaca.list_positions()
            position_symbols = {pos["symbol"] for pos in broker_positions}

            # Symbols to remove from pending
            to_remove = set()

            for symbol in self.pending_symbols:
                # If symbol has NO open orders AND NO broker position, it's safe to remove
                if symbol not in open_order_symbols and symbol not in position_symbols:
                    to_remove.add(symbol)
                    logger.info(f"[CLEANUP] {symbol}: Removing from pending set (no orders, no position)")

            # Remove stale symbols
            for symbol in to_remove:
                self.pending_symbols.discard(symbol)

            if to_remove:
                logger.info(f"[CLEANUP] Cleared {len(to_remove)} stale symbol(s) from pending set")

        except Exception as e:
            logger.warning(f"[CLEANUP] Failed to cleanup pending symbols: {e}")

    # ------------------------------------------------------------
    # v51: TP-ladder helpers (used by manage_positions)
    # ------------------------------------------------------------
    def _sell_partial(self, symbol: str, qty: int, reason: str):
        """
        Market-sell a partial quantity of an open position.

        Used by the TP ladder for TP1/TP2 scale-out exits. Returns the
        submitted order dict (so caller can read `filled_qty` / `filled_avg_price`)
        or None on failure.
        """
        if qty <= 0:
            logger.warning(f"[{reason}] {symbol}: Skipping partial sell — qty={qty}")
            return None
        try:
            sell_order = alpaca.submit_order(
                symbol=symbol,
                qty=qty,
                side="sell",
                order_type="market",
                time_in_force="day",
            )
            logger.info(
                f"[{reason}] {symbol}: Partial sell submitted | qty={qty} "
                f"order_id={sell_order.get('id')}"
            )
            return sell_order
        except Exception as e:
            logger.error(f"[{reason}] {symbol}: Partial sell FAILED: {e}")
            return None

    def _resize_stop_order(self, symbol: str, new_qty: int, stop_price: float, reason: str = "RESIZE"):
        """
        Cancel any existing broker-side stop/bracket orders for `symbol` and
        (optionally) submit a fresh stop order at `stop_price` for `new_qty` shares.

        Used by the TP ladder to:
          - Move SL to breakeven after TP1 fills (new_qty = remaining position)
          - Clear the broker stop when runner begins software trailing (new_qty=0)

        Returns the new stop order dict, or None if no new stop was placed.
        """
        try:
            cancelled = alpaca.cancel_orders_for_symbol(symbol)
            logger.info(f"[{reason}] {symbol}: Cancelled {cancelled} existing order(s)")
            time.sleep(0.3)
        except Exception as cancel_err:
            logger.warning(f"[{reason}] {symbol}: Error cancelling brackets: {cancel_err}")

        if new_qty <= 0 or stop_price <= 0:
            logger.info(f"[{reason}] {symbol}: No replacement stop placed (qty={new_qty}, stop=${stop_price:.2f})")
            return None

        try:
            new_stop = alpaca.submit_order(
                symbol=symbol,
                qty=new_qty,
                side="sell",
                order_type="stop",
                stop_price=round(stop_price, 2),
                time_in_force="gtc",
            )
            logger.info(
                f"[{reason}] {symbol}: New stop submitted | qty={new_qty} stop=${stop_price:.2f} "
                f"order_id={new_stop.get('id')}"
            )
            return new_stop
        except Exception as e:
            logger.error(f"[{reason}] {symbol}: Failed to resubmit stop: {e}")
            return None

    def manage_positions(self):
        """
        Manage open positions - monitor status, software trailing stop, and reconcile with broker.

        Architecture:
        - Broker bracket orders handle fixed stop-loss and take-profit (safety net)
        - Software trailing stop activates at TRAILING_STOP_ACTIVATION_R and trails at TRAILING_STOP_DISTANCE_R
        - When trailing stop triggers: cancel bracket orders -> market sell -> cleanup
        - Trailing stop state persisted in TradeIntent for crash recovery
        """
        with position_manager._lock:
            positions = list(position_manager.positions.values())

        if not positions:
            return

        logger.info(f"[MANAGE] Checking {len(positions)} position(s)...")

        # Fetch broker positions ONCE per cycle (not per position!)
        try:
            broker_positions = alpaca.list_positions()
            broker_symbols = {p["symbol"] for p in broker_positions}
            broker_pos_map = {p["symbol"]: p for p in broker_positions}
        except Exception as e:
            logger.warning(f"[MANAGE] Could not fetch broker positions: {e}")
            broker_symbols = set()
            broker_pos_map = {}

        for pos in positions:
            try:
                # Check if position still exists at broker (bracket may have closed it)
                if pos.symbol not in broker_symbols:
                    # Position was closed by broker (bracket hit TP or SL)
                    intent = trade_manager.get_intent(pos.symbol)

                    # Determine exit type using parent bracket order's child legs
                    exit_reason = "BRACKET_CLOSED"
                    outcome = "UNKNOWN"
                    if intent and intent.scalp_order_id:
                        try:
                            parent_order = alpaca.get_order(intent.scalp_order_id)
                            for leg in parent_order.get("legs", []):
                                if leg.get("status") == "filled" and leg.get("side") == "sell":
                                    if leg.get("type") in ("stop", "stop_limit"):
                                        exit_reason = "STOP_LOSS"
                                        outcome = "LOSS"
                                    elif leg.get("type") == "limit":
                                        exit_reason = "TAKE_PROFIT"
                                        outcome = "WIN"
                                    break
                        except Exception as e:
                            logger.debug(f"[RECONCILE] {pos.symbol}: Could not determine exit type: {e}")

                    logger.info(f"[RECONCILE] {pos.symbol}: Position closed by broker bracket ({exit_reason}) - removing")
                    position_manager.remove_position(pos.symbol)

                    if intent:
                        trade_manager.transition_state(pos.symbol, TradeState.CLOSED)
                        trade_journal.log_exit(pos.symbol, intent, reason=exit_reason, outcome=outcome)
                        logger.info(f"[RECONCILE] {pos.symbol}: Intent → CLOSED | Exit logged: {exit_reason} ({outcome})")

                    continue

                # Get current price using snapshot for real-time data
                snapshot = polygon.get_snapshot(pos.symbol)
                if not snapshot:
                    logger.warning(f"[MANAGE] {pos.symbol}: No snapshot data - skipping")
                    continue

                # Use lastTrade/lastQuote for current price (NOT day.c which is stale)
                current_price = polygon.get_current_price(snapshot)
                if not current_price:
                    logger.warning(f"[MANAGE] {pos.symbol}: No current price available - skipping")
                    continue

                gain_pct = (current_price - pos.entry_price) / pos.entry_price
                risk_per_share = pos.entry_price - pos.initial_stop if pos.initial_stop else 0
                gain_r = (current_price - pos.entry_price) / risk_per_share if risk_per_share > 0 else 0

                # Broker-side qty (source of truth for remaining shares after partial fills)
                broker_qty = int(float(broker_pos_map.get(pos.symbol, {}).get("qty", pos.qty)))

                # --- v51: TP LADDER (TP1 → breakeven SL → TP2 → runner trailing) ---
                # Runs BEFORE the software trailing stop. When the ladder is active, the
                # trailing-stop block below only trails the remaining "runner" qty once
                # tp2_filled. TP1/TP2 partial exits are market-sell orders; the broker
                # stop is cancelled and re-submitted at breakeven after TP1.
                if USE_TP_LADDER and risk_per_share > 0:
                    intent = trade_manager.get_intent(pos.symbol)
                    if intent and intent.tp1_price > 0 and intent.tp2_price > 0:

                        # --- TP1: scale out TP1_SCALE_PCT, move SL to breakeven ---
                        if not intent.tp1_filled and current_price >= intent.tp1_price:
                            tp1_qty = min(intent.tp1_qty, broker_qty)
                            logger.info(
                                f"[TP1] {pos.symbol}: TRIGGERED! price=${current_price:.2f} "
                                f">= tp1=${intent.tp1_price:.2f} ({gain_r:+.2f}R) | "
                                f"Selling {tp1_qty} of {broker_qty} shares"
                            )
                            sell_order = self._sell_partial(pos.symbol, tp1_qty, "TP1")
                            if sell_order is not None:
                                try:
                                    filled_qty = int(float(sell_order.get("filled_qty") or tp1_qty))
                                except (TypeError, ValueError):
                                    filled_qty = tp1_qty
                                fill_price = sell_order.get("filled_avg_price")
                                try:
                                    fill_price = float(fill_price) if fill_price else current_price
                                except (TypeError, ValueError):
                                    fill_price = current_price

                                # Resize the broker stop: breakeven for remaining shares.
                                # `entry_price` is the actual fill price (tracked by PositionManager).
                                remaining_qty = max(broker_qty - filled_qty, 0)
                                breakeven_stop = (
                                    pos.entry_price
                                    if MOVE_SL_TO_BREAKEVEN_AFTER_TP1
                                    else pos.initial_stop
                                )
                                new_stop = self._resize_stop_order(
                                    pos.symbol, remaining_qty, breakeven_stop, reason="TP1_BE"
                                )

                                trade_manager.update_intent(
                                    pos.symbol,
                                    tp1_filled=True,
                                    tp1_fill_price=fill_price,
                                    tp1_filled_at=iso(now_et()),
                                    sl_moved_to_breakeven=MOVE_SL_TO_BREAKEVEN_AFTER_TP1,
                                    stop_order_id=(new_stop.get("id") if new_stop else None),
                                )
                                logger.info(
                                    f"[TP1] {pos.symbol}: Filled {filled_qty}@${fill_price:.2f} | "
                                    f"SL @ ${breakeven_stop:.2f} covering {remaining_qty} shares | "
                                    f"Waiting for TP2 (${intent.tp2_price:.2f}) or trailing"
                                )
                            continue

                        # --- TP2: scale out TP2_SCALE_PCT, runner goes to software trailing ---
                        if intent.tp1_filled and not intent.tp2_filled and current_price >= intent.tp2_price:
                            tp2_qty = min(intent.tp2_qty, broker_qty)
                            logger.info(
                                f"[TP2] {pos.symbol}: TRIGGERED! price=${current_price:.2f} "
                                f">= tp2=${intent.tp2_price:.2f} ({gain_r:+.2f}R) | "
                                f"Selling {tp2_qty} of {broker_qty} shares"
                            )
                            sell_order = self._sell_partial(pos.symbol, tp2_qty, "TP2")
                            if sell_order is not None:
                                try:
                                    filled_qty = int(float(sell_order.get("filled_qty") or tp2_qty))
                                except (TypeError, ValueError):
                                    filled_qty = tp2_qty
                                fill_price = sell_order.get("filled_avg_price")
                                try:
                                    fill_price = float(fill_price) if fill_price else current_price
                                except (TypeError, ValueError):
                                    fill_price = current_price

                                # Clear broker stop — runner now rides the software trailing stop only.
                                try:
                                    cancelled = alpaca.cancel_orders_for_symbol(pos.symbol)
                                    logger.info(
                                        f"[TP2] {pos.symbol}: Cancelled {cancelled} broker order(s) "
                                        f"— runner ({max(broker_qty - filled_qty, 0)} shares) now software-managed"
                                    )
                                    time.sleep(0.3)
                                except Exception as cancel_err:
                                    logger.warning(
                                        f"[TP2] {pos.symbol}: Error cancelling broker stop: {cancel_err}"
                                    )

                                trade_manager.update_intent(
                                    pos.symbol,
                                    tp2_filled=True,
                                    tp2_fill_price=fill_price,
                                    tp2_filled_at=iso(now_et()),
                                    stop_order_id=None,
                                )
                                logger.info(
                                    f"[TP2] {pos.symbol}: Filled {filled_qty}@${fill_price:.2f} | "
                                    f"Runner rides software trailing stop from here"
                                )
                            continue

                # --- SOFTWARE TRAILING STOP ---
                # Broker bracket stop remains as safety net; this tightens the stop as price moves favorably
                if ENABLE_TRAILING_STOP and risk_per_share > 0:
                    intent = trade_manager.get_intent(pos.symbol)

                    # Restore trailing state from intent (survives bot restarts)
                    trail_activated = intent.trailing_activated if intent else False
                    highest_seen = intent.highest_price_seen if intent else 0.0
                    trail_stop = intent.trailing_stop_price if intent else 0.0

                    if gain_r >= TRAILING_STOP_ACTIVATION_R:
                        # Activate trailing stop if not already active
                        if not trail_activated:
                            trail_activated = True
                            highest_seen = current_price

                        # Update highest price seen (ratchet up only)
                        if current_price > highest_seen:
                            highest_seen = current_price

                        # v45: Adaptive trail distance based on move strength + time of day
                        peak_r = (highest_seen - pos.entry_price) / risk_per_share if risk_per_share > 0 else 0
                        if USE_ADAPTIVE_TRAILING:
                            if peak_r >= TRAIL_STRONG_THRESHOLD:
                                effective_distance_r = TRAIL_DISTANCE_STRONG_R
                                move_label = "STRONG"
                            elif peak_r >= TRAIL_WEAK_THRESHOLD:
                                effective_distance_r = TRAIL_DISTANCE_NORMAL_R
                                move_label = "NORMAL"
                            else:
                                effective_distance_r = TRAIL_DISTANCE_WEAK_R
                                move_label = "WEAK"

                            # Time-based tightening (afternoon momentum fades)
                            current_hour = now_et().hour
                            time_mult = 1.0
                            if current_hour >= TRAIL_TIME_TIGHTEN_HOUR_2:
                                time_mult = TRAIL_TIME_TIGHTEN_MULT_2
                            elif current_hour >= TRAIL_TIME_TIGHTEN_HOUR_1:
                                time_mult = TRAIL_TIME_TIGHTEN_MULT_1
                            effective_distance_r *= time_mult
                        else:
                            effective_distance_r = TRAILING_STOP_DISTANCE_R
                            move_label = "FIXED"

                        trail_distance = effective_distance_r * risk_per_share
                        trail_stop = round(highest_seen - trail_distance, 2)

                        # Log activation on first activation
                        if not (intent and intent.trailing_activated):
                            logger.info(f"[TRAILING] {pos.symbol}: ACTIVATED at {gain_r:.2f}R | "
                                       f"trail_stop=${trail_stop:.2f} highest=${highest_seen:.2f} "
                                       f"dist={effective_distance_r:.2f}R ({move_label})")

                        # Persist trailing state to intent (crash recovery)
                        if intent:
                            trade_manager.update_intent(
                                pos.symbol,
                                trailing_activated=trail_activated,
                                highest_price_seen=highest_seen,
                                trailing_stop_price=trail_stop,
                            )

                        # Check if trailing stop was breached
                        if current_price <= trail_stop:
                            logger.info(f"[TRAILING] {pos.symbol}: TRIGGERED! price=${current_price:.2f} <= trail_stop=${trail_stop:.2f} | "
                                       f"highest=${highest_seen:.2f} gain={gain_r:.2f}R | Selling full position")

                            # Get actual broker qty for the sell order
                            broker_qty = int(broker_pos_map.get(pos.symbol, {}).get("qty", pos.qty))

                            # Step 1: Cancel bracket orders (stop + TP legs)
                            try:
                                cancelled = alpaca.cancel_orders_for_symbol(pos.symbol)
                                logger.info(f"[TRAILING] {pos.symbol}: Cancelled {cancelled} bracket order(s)")
                                time.sleep(0.3)
                            except Exception as cancel_err:
                                logger.warning(f"[TRAILING] {pos.symbol}: Error cancelling brackets: {cancel_err}")

                            # Step 2: Market sell to close position
                            try:
                                sell_order = alpaca.submit_order(
                                    symbol=pos.symbol,
                                    qty=broker_qty,
                                    side="sell",
                                    order_type="market",
                                    time_in_force="day",
                                )
                                logger.info(f"[TRAILING] {pos.symbol}: Market sell submitted | qty={broker_qty} order_id={sell_order['id']}")
                            except Exception as sell_err:
                                logger.error(f"[TRAILING] {pos.symbol}: Market sell FAILED: {sell_err} | "
                                            f"Bracket orders already cancelled - position exposed! Manual intervention may be needed.")
                                continue

                            # Step 3: Cleanup state
                            position_manager.remove_position(pos.symbol)
                            if intent:
                                trade_manager.transition_state(pos.symbol, TradeState.CLOSED)
                                trade_journal.log_exit(pos.symbol, intent, reason="TRAILING_STOP", outcome="WIN")
                            self.pending_symbols.discard(pos.symbol)

                            logger.info(f"[TRAILING] {pos.symbol}: Position closed via trailing stop | "
                                       f"entry=${pos.entry_price:.2f} exit~${current_price:.2f} gain={gain_pct:+.2%} ({gain_r:.2f}R)")
                            continue  # Move to next position

                        # Log active trailing stop status
                        logger.info(f"[MANAGE] {pos.symbol}: price=${current_price:.2f} entry=${pos.entry_price:.2f} "
                                   f"gain={gain_pct:+.2%} ({gain_r:+.2f}R) peak={peak_r:.2f}R | TRAILING: "
                                   f"stop=${trail_stop:.2f} dist={effective_distance_r:.2f}R ({move_label}) "
                                   f"highest=${highest_seen:.2f} | safety=${pos.initial_stop:.2f}")

                    else:
                        # Trailing not yet activated - log normal status
                        trail_info = f" | trail activates at {TRAILING_STOP_ACTIVATION_R}R" if ENABLE_TRAILING_STOP else ""
                        logger.info(f"[MANAGE] {pos.symbol}: price=${current_price:.2f} entry=${pos.entry_price:.2f} "
                                   f"gain={gain_pct:+.2%} ({gain_r:+.2f}R) | broker managing stop=${pos.initial_stop:.2f}{trail_info}")
                else:
                    # Trailing stops disabled or no valid risk_per_share
                    logger.info(f"[MANAGE] {pos.symbol}: price=${current_price:.2f} entry=${pos.entry_price:.2f} "
                               f"gain={gain_pct:+.2%} | broker managing stop=${pos.initial_stop:.2f}")

            except Exception as e:
                logger.error(f"[MANAGE] {pos.symbol}: Error managing position: {e}")

    def discover_dynamic_universe(self):
        """
        ENHANCEMENT #1: Discover high relative volume stocks beyond core universe.

        Scans for stocks meeting criteria:
        - High relative volume (2.5x+)
        - Adequate price range ($10-$1000)
        - High dollar volume ($50M+ daily)
        - Not already in core universe

        Updates self.dynamic_universe with discovered symbols.
        """
        if not ENABLE_DYNAMIC_UNIVERSE:
            return

        try:
            logger.info("[DYNAMIC] Scanning for high RVOL movers beyond core universe...")

            # Use Polygon snapshots API to get all active stocks
            # This is a simplified implementation - you may need to use Alpaca screener instead
            url = f"{POLYGON_REST_BASE}/v2/snapshot/locale/us/markets/stocks/tickers"
            headers = {"Authorization": f"Bearer {POLYGON_API_KEY}"}

            response = requests.get(url, headers=headers, timeout=10)
            if response.status_code != 200:
                logger.warning(f"[DYNAMIC] Failed to fetch snapshots: {response.status_code}")
                return

            data = response.json()
            tickers = data.get("tickers", [])

            discovered = []
            for ticker_data in tickers:
                try:
                    symbol = ticker_data.get("ticker")
                    if not symbol:
                        continue

                    # Skip if already in core universe
                    if symbol in CORE_SYMBOLS:
                        continue

                    # NOTE: Don't skip already-discovered symbols here
                    # We need to re-evaluate them each scan to maintain the top movers list

                    # v6: Skip excluded symbols (negative expectancy from backtest)
                    if symbol in DYNAMIC_EXCLUSION_LIST:
                        continue

                    # Get day stats
                    day_data = ticker_data.get("day", {})
                    prev_day = ticker_data.get("prevDay", {})

                    if not day_data or not prev_day:
                        continue

                    # Current price
                    last_price = day_data.get("c") or ticker_data.get("lastTrade", {}).get("p")
                    if not last_price:
                        continue

                    # Price filter
                    if last_price < DYNAMIC_MIN_PRICE or last_price > DYNAMIC_MAX_PRICE:
                        continue

                    # Volume check
                    current_volume = day_data.get("v", 0)
                    prev_volume = prev_day.get("v", 1)

                    if prev_volume == 0:
                        continue

                    rel_vol = current_volume / prev_volume

                    # RVOL filter
                    if rel_vol < DYNAMIC_MIN_RVOL:
                        continue

                    # Dollar volume filter (liquidity)
                    dollar_volume = current_volume * last_price
                    if dollar_volume < DYNAMIC_MIN_VOLUME_USD:
                        continue

                    discovered.append({
                        "symbol": symbol,
                        "price": last_price,
                        "rvol": rel_vol,
                        "dollar_volume": dollar_volume
                    })

                except Exception as e:
                    logger.debug(f"[DYNAMIC] Error processing ticker: {e}")
                    continue

            # Sort by RVOL descending, take top N
            discovered.sort(key=lambda x: x["rvol"], reverse=True)
            top_movers = discovered[:DYNAMIC_MAX_UNIVERSE_SIZE]

            # Update dynamic universe
            new_symbols = set([m["symbol"] for m in top_movers])
            added = new_symbols - self.dynamic_universe
            removed = self.dynamic_universe - new_symbols

            self.dynamic_universe = new_symbols

            if added:
                logger.info(f"[DYNAMIC] Added {len(added)} new movers: {', '.join(sorted(added))}")
                for mover in top_movers:
                    if mover["symbol"] in added:
                        logger.info(f"[DYNAMIC]   {mover['symbol']}: ${mover['price']:.2f} | "
                                   f"RVOL={mover['rvol']:.1f}x | Vol=${mover['dollar_volume']/1e6:.1f}M")

            if removed:
                logger.debug(f"[DYNAMIC] Removed {len(removed)} symbols: {', '.join(sorted(removed))}")

            # Only log when there are actual changes or no movers at all
            if not added and not removed and top_movers:
                logger.debug(f"[DYNAMIC] Universe stable: {len(new_symbols)} movers")
            elif not top_movers:
                logger.info("[DYNAMIC] No movers found meeting criteria")

        except Exception as e:
            logger.warning(f"[DYNAMIC] Error discovering dynamic universe: {e}")

    def reconcile_broker_state(self):
        """
        LEVEL 3: Reconcile bot state with broker on startup.

        This is the deterministic state reconstruction that prevents:
        1) "Assume flat" scenario (positions exist but bot doesn't know)
        2) Double-entry scenario (open orders working but bot re-enters)
        3) Orphaned intents (intent exists but broker has no orders)

        Algorithm:
        - Load intents from disk (already done by TradeManager.__init__)
        - Query broker for ALL open orders and positions
        - Match broker state to intents via client_order_id
        - Sync intent states with broker reality
        - Ensure every symbol is in exactly one state
        """
        try:
            logger.info("[RECONCILE] LEVEL 3: Deterministic state reconstruction from broker + intents...")

            # Step 1: Get all open orders from broker
            open_orders = alpaca.get_orders(status="open")
            open_orders_by_symbol = {}
            for order in open_orders:
                symbol = order["symbol"]
                if symbol not in open_orders_by_symbol:
                    open_orders_by_symbol[symbol] = []
                open_orders_by_symbol[symbol].append(order)

            if open_orders:
                logger.warning(f"[RECONCILE] Found {len(open_orders)} open order(s) across {len(open_orders_by_symbol)} symbol(s)")

            # Step 2: Match open orders to intents via client_order_id
            for symbol, orders in open_orders_by_symbol.items():
                intent = trade_manager.get_intent(symbol)

                for order in orders:
                    order_id = order["id"]
                    client_order_id = order.get("client_order_id")
                    side = order["side"]
                    qty = order["qty"]
                    status = order["status"]
                    order_type = order.get("type", "unknown")

                    logger.warning(f"[RECONCILE] {symbol}: Open order | "
                                 f"id={order_id} client_id={client_order_id} side={side} qty={qty} status={status} type={order_type}")

                    # Match to intent if possible
                    if intent and client_order_id:
                        if client_order_id == intent.scalp_client_order_id:
                            logger.info(f"[RECONCILE] {symbol}: Matched scalp bracket to intent")
                            trade_manager.update_intent(symbol, scalp_order_id=order_id)
                        elif client_order_id == intent.runner_client_order_id:
                            logger.info(f"[RECONCILE] {symbol}: Matched runner bracket to intent")
                            trade_manager.update_intent(symbol, runner_order_id=order_id)

                    # CRITICAL: Block re-entry for symbols with open orders
                    self.pending_symbols.add(symbol)
                    logger.warning(f"[RECONCILE] {symbol}: BLOCKED from re-entry (open orders exist)")

            # Step 3: Sync all active intents with broker to determine fill status
            logger.info("[RECONCILE] Syncing all active intents with broker fill status...")
            trade_manager.sync_all_active_intents()

            # Step 4: Clean up orphaned intents (intent exists but no broker state)
            for symbol in list(trade_manager.intents.keys()):
                intent = trade_manager.get_intent(symbol)
                if not intent:
                    continue

                # Skip if already in terminal state
                if intent.state in (TradeState.CLOSED, TradeState.FAILED, TradeState.CANCELLED):
                    continue

                # Check if broker has any orders or positions for this symbol
                has_broker_orders = symbol in open_orders_by_symbol
                has_broker_position = any(pos["symbol"] == symbol for pos in alpaca.list_positions())

                if not has_broker_orders and not has_broker_position:
                    # Orphaned intent - broker has no trace of it
                    logger.warning(f"[RECONCILE] {symbol}: ORPHANED intent detected | "
                                 f"state={intent.state.value} but no broker orders/positions | Marking as FAILED")
                    trade_manager.transition_state(symbol, TradeState.FAILED)
                    self.pending_symbols.discard(symbol)

            broker_positions = alpaca.list_positions()

            if not broker_positions:
                if not open_orders:
                    logger.info("[RECONCILE] No existing broker positions or orders - starting fresh")
                return

            logger.warning(f"[RECONCILE] Found {len(broker_positions)} existing position(s) at broker")

            # Track orphaned positions (position exists but no intent)
            orphaned_positions = []

            for broker_pos in broker_positions:
                symbol = broker_pos["symbol"]
                qty = int(float(broker_pos["qty"]))
                entry_price = float(broker_pos["avg_entry_price"])
                current_price = float(broker_pos["current_price"])
                unrealized_pl = float(broker_pos["unrealized_pl"])
                unrealized_plpc = float(broker_pos["unrealized_plpc"])

                # Safety: never manage trend_bot positions (belt-and-suspenders with CORE_SYMBOLS check)
                if symbol in TREND_BOT_SYMBOLS:
                    logger.info(f"[RECONCILE] {symbol}: Skipping - managed by trend_bot")
                    continue

                # Safety: never manage directional_bot SHORT positions
                # simple_bot is LONG-ONLY; any short position belongs to directional_bot
                position_side = broker_pos.get("side", "long")
                if position_side == "short":
                    logger.info(f"[RECONCILE] {symbol}: Skipping SHORT position - managed by directional_bot")
                    continue

                # Check if we have a corresponding TradeIntent
                existing_intent = trade_manager.get_intent(symbol)

                # Check if symbol is in our trading universe OR has an active intent
                # (bot may have entered a dynamically-discovered symbol that's not in CORE_SYMBOLS)
                if symbol not in CORE_SYMBOLS and not existing_intent:
                    # Before skipping, check broker order history: if SIMPLE opened
                    # this position (by client_order_id prefix), adopt it as an
                    # orphan so EOD / trailing / state-sync can manage it.
                    # Without this, a dynamic-universe symbol whose intent was
                    # cleaned up (by the 24h terminal-intent sweeper) would fall
                    # through ALL management paths and survive past market close.
                    if self._is_simple_owned(symbol):
                        logger.warning(
                            f"[RECONCILE] {symbol}: Not in CORE_SYMBOLS and no intent, "
                            f"but broker order history classifies it as SIMPLE-owned "
                            f"(dynamic-universe entry) — adopting as orphan"
                        )
                        # Fall through to the existing orphan-adoption path below
                    else:
                        logger.warning(
                            f"[RECONCILE] {symbol}: Position exists but NOT in our universe and no intent - "
                            f"will monitor but not manage"
                        )
                        continue
                elif symbol not in CORE_SYMBOLS and existing_intent:
                    logger.info(f"[RECONCILE] {symbol}: Not in CORE_SYMBOLS but has active intent "
                              f"(state={existing_intent.state.value}) - adopting position")

                # Estimate stop price: use intent's stop_price if available, otherwise conservative 5% estimate
                if existing_intent and existing_intent.stop_price:
                    estimated_stop = existing_intent.stop_price
                else:
                    estimated_stop = min(entry_price * 0.95, current_price * 0.95)

                if existing_intent:
                    # Intent exists - check if it's in a terminal state with a live position
                    if existing_intent.state in (TradeState.CLOSED, TradeState.FAILED, TradeState.CANCELLED):
                        # Position still open at broker but intent says closed — re-open it
                        logger.warning(f"[RECONCILE] {symbol}: Intent is {existing_intent.state.value} but POSITION STILL OPEN "
                                     f"at broker | qty={qty} entry=${entry_price:.2f} — re-activating intent")
                        trade_manager.transition_state(symbol, TradeState.ACTIVE_EXITS)
                        existing_intent = trade_manager.get_intent(symbol)  # refresh after transition
                    else:
                        logger.info(f"[RECONCILE] {symbol}: Intent exists (state={existing_intent.state.value}) | "
                                   f"qty={qty} entry=${entry_price:.2f}")
                else:
                    # ORPHANED POSITION: Position at broker but no TradeIntent
                    # This happens when bot stopped/crashed without properly cleaning up
                    logger.warning(f"[RECONCILE] {symbol}: *** ORPHANED POSITION DETECTED *** | "
                                 f"qty={qty} entry=${entry_price:.2f} current=${current_price:.2f} "
                                 f"PnL=${unrealized_pl:.2f} ({unrealized_plpc:+.2%})")

                    # Check if there are any stop/limit orders at broker for this symbol
                    # and extract actual stop/TP prices if available
                    symbol_orders = open_orders_by_symbol.get(symbol, [])
                    has_stop_order = any(o.get("type") in ("stop", "stop_limit") for o in symbol_orders)
                    has_limit_order = any(o.get("type") == "limit" and o.get("side") == "sell" for o in symbol_orders)

                    # Extract actual stop price from broker orders (more accurate than estimate)
                    actual_stop_price = None
                    actual_tp_price = None
                    for order in symbol_orders:
                        order_type = order.get("type", "")
                        order_side = order.get("side", "")

                        # Stop order (stop or stop_limit)
                        if order_type in ("stop", "stop_limit") and order_side == "sell":
                            stop_price_str = order.get("stop_price")
                            if stop_price_str:
                                actual_stop_price = float(stop_price_str)
                                logger.info(f"[RECONCILE] {symbol}: Found broker stop order @ ${actual_stop_price:.2f}")

                        # Take-profit limit order
                        if order_type == "limit" and order_side == "sell":
                            limit_price_str = order.get("limit_price")
                            if limit_price_str:
                                actual_tp_price = float(limit_price_str)
                                logger.info(f"[RECONCILE] {symbol}: Found broker TP order @ ${actual_tp_price:.2f}")

                    # Use actual stop price if found, otherwise fall back to estimate
                    final_stop_price = actual_stop_price if actual_stop_price else estimated_stop
                    final_tp_price = actual_tp_price if actual_tp_price else entry_price * 1.05

                    if has_stop_order or has_limit_order:
                        logger.info(f"[RECONCILE] {symbol}: Orphan has broker orders - stop={has_stop_order} limit={has_limit_order}")
                        if actual_stop_price:
                            logger.info(f"[RECONCILE] {symbol}: Using ACTUAL broker stop=${actual_stop_price:.2f} (not estimate)")
                    else:
                        logger.warning(f"[RECONCILE] {symbol}: *** ORPHAN HAS NO PROTECTIVE ORDERS *** - UNPROTECTED POSITION!")

                    orphaned_positions.append({
                        "symbol": symbol,
                        "qty": qty,
                        "entry_price": entry_price,
                        "current_price": current_price,
                        "estimated_stop": estimated_stop,
                        "actual_stop": actual_stop_price,
                        "actual_tp": actual_tp_price,
                        "final_stop": final_stop_price,
                        "final_tp": final_tp_price,
                        "has_stop": has_stop_order,
                        "has_limit": has_limit_order,
                        "unrealized_pl": unrealized_pl,
                    })

                # Add to position manager (whether intent exists or not)
                position_manager.add_position(
                    symbol=symbol,
                    qty=qty,
                    entry_price=entry_price,
                    initial_stop=estimated_stop
                )

            # Step 5: Create synthetic intents for orphaned positions
            # This allows the bot to properly manage them (EOD close, trailing stops, etc.)
            for orphan in orphaned_positions:
                symbol = orphan["symbol"]
                qty = orphan["qty"]
                entry_price = orphan["entry_price"]
                # Use actual broker stop/TP if available, otherwise use estimates
                final_stop = orphan["final_stop"]
                final_tp = orphan["final_tp"]
                actual_stop = orphan.get("actual_stop")
                actual_tp = orphan.get("actual_tp")

                # Create a synthetic intent in ACTIVE_EXITS state
                # This tells the bot "there's an open position, manage it"
                now = now_et()
                synthetic_intent = TradeIntent(
                    symbol=symbol,
                    state=TradeState.ACTIVE_EXITS,  # Position is open, exits should be active
                    created_at=iso(now),
                    total_qty=qty,
                    scalp_qty=qty,  # Treat entire position as "scalp" for simplicity
                    runner_qty=0,
                    entry_limit=entry_price,  # Use actual entry as limit
                    stop_price=final_stop,  # Use actual broker stop if found, else estimate
                    scalp_tp_price=final_tp,  # Use actual broker TP if found, else estimate
                    runner_tp_price=0.0,
                    scalp_filled=True,  # Position exists, so entry filled
                    scalp_fill_price=entry_price,
                    filled_at=iso(now),  # Mark as filled now (we don't know actual fill time)
                )

                # Add to trade manager
                with trade_manager._lock:
                    trade_manager.intents[symbol] = synthetic_intent
                    trade_manager._save_intents()

                # Block re-entry
                self.pending_symbols.add(symbol)

                # Log with indication of whether stop/TP were from broker or estimated
                stop_source = "BROKER" if actual_stop else "ESTIMATED"
                tp_source = "BROKER" if actual_tp else "ESTIMATED"
                logger.warning(f"[RECONCILE] {symbol}: Created SYNTHETIC intent for orphaned position | "
                             f"state=ACTIVE_EXITS qty={qty} entry=${entry_price:.2f} "
                             f"stop=${final_stop:.2f} ({stop_source}) tp=${final_tp:.2f} ({tp_source})")

                # Send alert about orphaned position
                alerter.send_alert(
                    level="WARNING",
                    title=f"ORPHANED POSITION RECOVERED: {symbol}",
                    message=f"Position found without intent at startup.\n"
                    f"Qty: {qty} | Entry: ${entry_price:.2f} | Current: ${orphan['current_price']:.2f}\n"
                    f"PnL: ${orphan['unrealized_pl']:.2f}\n"
                    f"Stop: ${final_stop:.2f} ({stop_source}) | TP: ${final_tp:.2f} ({tp_source})\n"
                    f"Has broker stop: {orphan['has_stop']} | Has broker limit: {orphan['has_limit']}\n"
                    f"Created synthetic intent to manage position.\n"
                    f"ACTION: Review position and consider placing protective orders if missing."
                )

            if orphaned_positions:
                logger.warning(f"[RECONCILE] *** RECOVERED {len(orphaned_positions)} ORPHANED POSITION(S) ***")

            logger.info(f"[RECONCILE] Reconciliation complete - tracking {len(position_manager.positions)} position(s)")

            # Step 6: Verify all tracked positions have broker-side stop protection
            # If bracket day orders expired (overnight hold) or were canceled (trailing stop exit
            # that didn't close), resubmit a standalone GTC stop order for safety
            for pos in list(position_manager.positions.values()):
                intent = trade_manager.get_intent(pos.symbol)
                if not intent:
                    continue

                # Check if this position has any active stop or pending sell orders at the broker
                symbol_orders = open_orders_by_symbol.get(pos.symbol, [])
                has_active_stop = any(
                    o.get("type") in ("stop", "stop_limit") and o.get("side") == "sell"
                    for o in symbol_orders
                )
                has_pending_sell = any(
                    o.get("side") == "sell" for o in symbol_orders
                )

                has_active_tp = any(
                    o.get("type") == "limit" and o.get("side") == "sell"
                    for o in symbol_orders
                )

                if has_pending_sell and not has_active_stop:
                    logger.info(f"[RECONCILE] {pos.symbol}: No stop order but has pending sell order(s) - skipping safety stop")
                elif not has_active_stop:
                    # Determine best stop price: trailing stop if activated, else intent stop
                    if intent.trailing_activated and intent.trailing_stop_price > 0:
                        safety_stop = intent.trailing_stop_price
                        stop_source = "TRAILING"
                    elif intent.stop_price:
                        safety_stop = intent.stop_price
                        stop_source = "INTENT"
                    else:
                        safety_stop = pos.entry_price * 0.95
                        stop_source = "ESTIMATED"

                    # If BOTH stop and TP are missing, submit as OCO (one-cancels-other)
                    # so they share the same share allocation and don't conflict
                    has_tp_price = intent.scalp_tp_price and intent.scalp_tp_price > 0
                    if not has_active_tp and has_tp_price:
                        logger.warning(f"[RECONCILE] {pos.symbol}: *** NO BROKER STOP OR TP *** | "
                                     f"Submitting GTC OCO: stop=${safety_stop:.2f} ({stop_source}) + "
                                     f"TP=${intent.scalp_tp_price:.2f}")
                        try:
                            oco_order = alpaca.submit_order(
                                symbol=pos.symbol,
                                qty=pos.qty,
                                side="sell",
                                order_type="limit",
                                limit_price=intent.scalp_tp_price,
                                time_in_force="gtc",
                                order_class="oco",
                                take_profit={"limit_price": f"{intent.scalp_tp_price:.2f}"},
                                stop_loss={"stop_price": f"{safety_stop:.2f}"}
                            )
                            logger.info(f"[RECONCILE] {pos.symbol}: GTC OCO submitted | "
                                      f"order_id={oco_order.get('id')} stop=${safety_stop:.2f} tp=${intent.scalp_tp_price:.2f}")
                        except Exception as e:
                            logger.warning(f"[RECONCILE] {pos.symbol}: OCO failed ({e}) - falling back to stop-only")
                            # Fallback: submit stop alone (critical protection)
                            try:
                                stop_order = alpaca.submit_order(
                                    symbol=pos.symbol,
                                    qty=pos.qty,
                                    side="sell",
                                    order_type="stop",
                                    stop_price=safety_stop,
                                    time_in_force="gtc"
                                )
                                logger.info(f"[RECONCILE] {pos.symbol}: GTC safety stop submitted (fallback) | "
                                          f"order_id={stop_order.get('id')} stop=${safety_stop:.2f}")
                            except Exception as e2:
                                logger.error(f"[RECONCILE] {pos.symbol}: Failed to submit safety stop: {e2}")
                                alerter.send_alert(
                                    level="CRITICAL",
                                    title=f"UNPROTECTED POSITION: {pos.symbol}",
                                    message=f"Position has NO broker stop order and failed to submit one.\n"
                                            f"Qty: {pos.qty} | Entry: ${pos.entry_price:.2f}\n"
                                            f"Error: {e2}\n"
                                            f"ACTION: Manually place a stop order immediately!"
                                )
                    else:
                        # Only stop is missing (TP already exists or no TP price available)
                        logger.warning(f"[RECONCILE] {pos.symbol}: *** NO BROKER STOP ORDER *** | "
                                     f"Submitting GTC safety stop @ ${safety_stop:.2f} ({stop_source})")
                        try:
                            stop_order = alpaca.submit_order(
                                symbol=pos.symbol,
                                qty=pos.qty,
                                side="sell",
                                order_type="stop",
                                stop_price=safety_stop,
                                time_in_force="gtc"
                            )
                            logger.info(f"[RECONCILE] {pos.symbol}: GTC safety stop submitted | "
                                      f"order_id={stop_order.get('id')} stop=${safety_stop:.2f}")
                        except Exception as e:
                            logger.error(f"[RECONCILE] {pos.symbol}: Failed to submit safety stop: {e}")
                            alerter.send_alert(
                                level="CRITICAL",
                                title=f"UNPROTECTED POSITION: {pos.symbol}",
                                message=f"Position has NO broker stop order and failed to submit one.\n"
                                        f"Qty: {pos.qty} | Entry: ${pos.entry_price:.2f}\n"
                                        f"Error: {e}\n"
                                        f"ACTION: Manually place a stop order immediately!"
                            )

        except Exception as e:
            logger.error(f"[RECONCILE] Failed to reconcile broker state: {e}")
            # Continue anyway - don't halt bot startup for reconciliation failures

    def verify_account_status(self):
        """
        PRODUCTION SAFETY: Verify account status and trading capability.

        This ensures:
        1. Account is accessible
        2. Trading is enabled
        3. Account has sufficient equity
        4. Displays account ID for verification
        """
        try:
            logger.info("[STARTUP] Verifying account status...")

            account = alpaca.get_account()
            account_id = account.get("id", "UNKNOWN")
            account_number = account.get("account_number", "UNKNOWN")
            status = account.get("status", "UNKNOWN")
            trading_blocked = account.get("trading_blocked", True)
            pattern_day_trader = account.get("pattern_day_trader", False)
            equity = float(account.get("equity", 0))
            buying_power = float(account.get("buying_power", 0))

            logger.info("="*70)
            logger.info("[ACCOUNT] Account Verification")
            logger.info(f"  Account ID: {account_id}")
            logger.info(f"  Account Number: {account_number}")
            logger.info(f"  Status: {status}")
            logger.info(f"  Trading Blocked: {trading_blocked}")
            logger.info(f"  Pattern Day Trader: {pattern_day_trader}")
            logger.info(f"  Equity: ${equity:,.2f}")
            logger.info(f"  Buying Power: ${buying_power:,.2f}")
            logger.info(f"  Trading Mode: {'LIVE' if LIVE_TRADING_ENABLED else 'PAPER'}")
            logger.info("="*70)

            # Critical checks
            if status != "ACTIVE":
                raise RuntimeError(f"Account status is {status}, not ACTIVE. Cannot trade.")

            if trading_blocked:
                raise RuntimeError("Trading is BLOCKED on this account. Cannot place orders.")

            if equity < 100:
                logger.warning(f"Account equity is low: ${equity:,.2f}")

            logger.info("[STARTUP] Account verification PASSED")

        except Exception as e:
            logger.error(f"[STARTUP] Account verification FAILED: {e}")
            raise RuntimeError(f"Cannot start bot - account verification failed: {e}")

    def shutdown(self):
        """
        PRODUCTION SAFETY: Graceful shutdown with configurable policy.

        Policies:
        - CANCEL_ORDERS_ONLY: Cancel our open orders, leave positions (default, safer)
        - FLATTEN_ALL: Cancel our orders AND close our positions (aggressive)

        SHARED-ACCOUNT SAFETY: Only cancels/flattens simple_bot's own orders and positions.
        Preserves directional_bot's safety stop orders and trend_bot's positions.
        """
        logger.warning("[SHUTDOWN] Initiating graceful shutdown...")
        logger.warning(f"[SHUTDOWN] Policy: {SHUTDOWN_POLICY}")

        try:
            # Cancel only OUR open orders (not directional_bot's safety stops or trend_bot's orders)
            # directional_bot uses "dir_" prefix, trend_bot uses "TBOT_" prefix
            try:
                all_orders = alpaca.get_orders(status="open")
                our_orders = [o for o in all_orders
                             if not (o.get("client_order_id") or "").startswith(("dir_", "TBOT_"))]
                other_bot_orders = len(all_orders) - len(our_orders)
                for order in our_orders:
                    try:
                        alpaca.cancel_order(order["id"])
                    except Exception:
                        pass
                logger.info(f"[SHUTDOWN] Cancelled {len(our_orders)} of our open order(s)"
                           f"{f' (preserved {other_bot_orders} other bot orders)' if other_bot_orders else ''}")
            except Exception as e:
                logger.error(f"[SHUTDOWN] Error cancelling orders: {e}")

            # Optionally flatten OUR positions (skip trend_bot symbols and short positions)
            if SHUTDOWN_POLICY == "FLATTEN_ALL":
                positions = alpaca.list_positions()
                our_positions = [p for p in positions
                                if p["symbol"] not in TREND_BOT_SYMBOLS
                                and p.get("side", "long") != "short"]
                other_positions = len(positions) - len(our_positions)
                if our_positions:
                    logger.warning(f"[SHUTDOWN] Flattening {len(our_positions)} of our position(s)"
                                  f"{f' (preserving {other_positions} other bot positions)' if other_positions else ''}...")
                    for pos in our_positions:
                        symbol = pos["symbol"]
                        try:
                            result = alpaca.flatten_symbol(symbol)
                            logger.info(f"[SHUTDOWN] Flattened {symbol}")
                        except Exception as e:
                            logger.error(f"[SHUTDOWN] Failed to flatten {symbol}: {e}")
                else:
                    logger.info("[SHUTDOWN] No positions to flatten")
            else:
                positions = alpaca.list_positions()
                if positions:
                    logger.warning(f"[SHUTDOWN] Leaving {len(positions)} position(s) open (policy=CANCEL_ORDERS_ONLY)")

            # Flush scan diagnostics for the day
            scan_diagnostics_logger.flush_if_needed()

            logger.info("[SHUTDOWN] Shutdown complete")

        except Exception as e:
            logger.error(f"[SHUTDOWN] Error during shutdown: {e}")


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    bot = MomentumBot()
    bot.run()
