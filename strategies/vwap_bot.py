"""
VWAP Reclaim Bot — v18 Backtest Optimized
==========================================

Strategy: VWAP Mean Reversion with aggressive risk management

v18 Backtest Results (252 days, $35k):
- +4.5% annual return ($1,566 profit)
- 69.6% win rate, 1.26 profit factor
- 1.4% max drawdown

Key optimizations from backtesting:
- 1:1 R/R exits (no break-even)
- Strict signal quality filters (ADX 25, RVOL 1.2)
- Aggressive position sizing (40% per position)
- 3 max concurrent positions

(WebSocket spam fix + state-compat still included.)
"""

from __future__ import annotations

import os
import json
import math
import time
import queue
import random
import signal
import shutil
import logging
import threading
import datetime as dt
import smtplib
import hashlib
from dataclasses import dataclass, asdict
from enum import Enum
from typing import Dict, List, Optional, Tuple, Any
from logging.handlers import RotatingFileHandler
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

import requests
import pandas as pd
import websocket
from zoneinfo import ZoneInfo

# Load config from .env file if present
from pathlib import Path
try:
    from dotenv import load_dotenv
    _env_path = Path(__file__).parent / "vwap_bot.env"
    if _env_path.exists():
        load_dotenv(_env_path)
except ImportError:
    pass  # dotenv not installed, rely on environment variables

# ============================================================
# CONFIG (Edit these)
# ============================================================

ET = ZoneInfo("America/New_York")

# --- Directory Paths (for organized folder structure) ---
ALGO_ROOT = Path(__file__).parent.parent  # Algo_Trading root
# Allow output dir override (useful when source lives on Google Drive)
_output_root = Path(os.getenv("ALGO_OUTPUT_DIR", "")) if os.getenv("ALGO_OUTPUT_DIR") else ALGO_ROOT
DATA_DIR = _output_root / "data"
LOGS_DIR = _output_root / "logs"
CONFIG_DIR = ALGO_ROOT / "config"
DATA_DIR.mkdir(exist_ok=True)
LOGS_DIR.mkdir(exist_ok=True)

# --- Logging ---
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()  # DEBUG, INFO, WARNING, ERROR
LOG_HEARTBEAT_SECONDS = 60
LOG_THROTTLE_SECONDS = 30
LOG_QUOTE_SAMPLE_N = 10

# --- Metrics & Monitoring ---
ENABLE_METRICS = True
METRICS_LOG_PATH = os.getenv("METRICS_LOG_PATH", str(DATA_DIR / "bot_metrics.jsonl"))  # JSON Lines format

# --- WebSocket ping stability (websocket-client requirement) ---
WS_PING_INTERVAL_SECONDS = 30
WS_PING_TIMEOUT_SECONDS = 10  # MUST be < interval; auto-corrects if not
WS_TRACE = False  # websocket-client trace (VERY verbose)

# --- Environment variables ---
ALPACA_API_KEY = os.getenv("ALPACA_API_KEY", "").strip()
ALPACA_SECRET_KEY = os.getenv("ALPACA_SECRET_KEY", "").strip()
ALPACA_BASE_URL = os.getenv("ALPACA_BASE_URL", "https://paper-api.alpaca.markets").strip()

POLYGON_API_KEY = (os.getenv("POLYGON_API_KEY") or os.getenv("MASSIVE_API_KEY") or "").strip()
POLYGON_REST_BASE = os.getenv("POLYGON_REST_BASE", "https://api.polygon.io").strip()

# Massive/Polygon WS URL (your vendor)
WS_URL = os.getenv("WS_URL", "wss://socket.massive.com/stocks").strip()

# --- Persistence ---
_state_raw = os.getenv("BOT_STATE_PATH", "bot_state.json")
STATE_PATH = _state_raw if os.path.isabs(_state_raw) else str(DATA_DIR / _state_raw)
STATE_FLUSH_SECONDS = 5.0
STATE_BACKUP_COUNT = 3

# --- Main loop pacing ---
SCAN_TICK_SECONDS = 0.50
SCAN_EVERY_N_SECONDS = 60  # scans roughly once per minute

# --- Universe ---
CORE_SYMBOLS = [
    "SPY", "QQQ", "IWM",
    "AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOGL", "TSLA", "AMD",
]
EXPAND_UNIVERSE = True
EXPANDED_UNIVERSE_SIZE = 35
MIN_PRICE = 5.0
MAX_PRICE = 500.0

# --- Risk / limits (v18 backtest optimized: aggressive) ---
MAX_OPEN_POSITIONS = 3             # v18: 3 concurrent positions (from 2)
MAX_PENDING_ENTRY_ORDERS = 1
MAX_ENTRIES_PER_MINUTE = 1  # keeps logs like "(1 trade max/min)"

TRADE_CASH_PCT_RTH = 0.40          # v18: 40% of capital per position (from 10%)
TRADE_CASH_PCT_EXT = 0.20          # v18: 20% for extended hours (from 5%)

# --- Exposure Caps (v18 backtest optimized) ---
MAX_EXPOSURE_PCT_OF_EQUITY = 0.80       # v18: 80% max exposure (from 50%)
MAX_EXPOSURE_PER_SYMBOL_USD = 15000.0   # v18: $15k max per position (from $5k)
MAX_CORRELATED_POSITIONS = 2            # Max positions in same sector/cluster
CORRELATION_CLUSTERS = {
    # Tech/Semis cluster
    "TECH": ["AAPL", "MSFT", "GOOGL", "META", "AMZN", "NVDA", "AMD", "TSLA", "QQQ"],
    # Financials cluster
    "FIN": ["JPM", "BAC", "GS", "MS", "WFC", "C"],
    # Energy cluster
    "ENERGY": ["XOM", "CVX", "COP", "SLB", "OXY"],
    # ETFs (treat separately)
    "ETF": ["SPY", "QQQ", "IWM", "DIA"],
}

# Daily loss limits (halt entries; optional flatten)
DAILY_MAX_LOSS_PCT = 0.025         # 2.5% drawdown from start_equity (increased from 2% to reduce premature halts)
DAILY_MAX_LOSS_DOLLARS = None      # e.g. 500.0 overrides pct if set
ON_DAILY_LOSS_CANCEL_ORDERS = True
ON_DAILY_LOSS_FLATTEN = True

# --- Session cutoffs ---
NO_NEW_ENTRIES_AFTER_ET = "15:45"
HARD_FLATTEN_ET = "15:55"
HARD_FLATTEN_ON_EARLY_CLOSE = True

# --- Time stops ---
MAX_HOLD_MINUTES_RTH = 45
MAX_HOLD_MINUTES_EXT = 25

# --- VWAP Reclaim signal parameters (v18 BACKTEST OPTIMAL) ---
LOOKBACK_MINUTES = 240
ATR_LEN = 14
ADX_LEN = 14
TREND_ADX_MAX = 25.0               # v18: 25 optimal (strict quality filter)
MIN_REL_VOL = 1.2                  # v18: 1.2 optimal (quality signals)
STRETCH_ATR = 1.0                  # v18: 1.0x ATR stretch required
RECLAIM_ATR = 0.20                 # v18: 0.20 tight reclaim threshold

# --- VWAP Signal Quality Filters (v18 BACKTEST OPTIMAL) ---
MIN_VWAP_DISPLACEMENT_PCT = 0.30   # v18: 0.30% displacement threshold
REQUIRE_CANDLE_CLOSE_CONFIRM = True  # Require candle close above/below VWAP (not just touch)
NO_TRADE_FIRST_MINUTES = 10        # v18: Skip first 10 minutes (avoid open volatility)
MIN_BARS_SINCE_STRETCH = 2         # v18: 2 bars before entry
MAX_BARS_SINCE_STRETCH = 20        # v18: Don't hold stale stretches

# --- Volatility Adaptation (Phase 2) ---
# VIX-based regime detection
VIX_SYMBOL = "VIX"                 # VIX index for volatility regime
VIX_LOW_THRESHOLD = 15.0           # Below this = low vol regime (more aggressive)
VIX_HIGH_THRESHOLD = 25.0          # Above this = high vol regime (defensive)
VIX_EXTREME_THRESHOLD = 35.0       # Above this = risk-off mode (halt entries)
VIX_CACHE_MINUTES = 5              # How often to refresh VIX reading

# Volatility regime multipliers for position sizing
VOL_REGIME_MULTIPLIERS = {
    "LOW": 1.25,                   # Low vol = slightly larger positions
    "NORMAL": 1.00,                # Normal baseline
    "HIGH": 0.60,                  # High vol = reduced size
    "EXTREME": 0.00,               # Extreme = no new entries (risk-off)
}

# Dynamic ATR multipliers based on volatility regime
ATR_MULTIPLIER_BY_REGIME = {
    "LOW": {"stretch": 1.0, "reclaim": 1.0, "tp": 1.0, "sl": 1.0},
    "NORMAL": {"stretch": 1.0, "reclaim": 1.0, "tp": 1.0, "sl": 1.0},
    "HIGH": {"stretch": 1.3, "reclaim": 0.8, "tp": 0.8, "sl": 1.2},    # Wider entry, tighter TP, wider SL
    "EXTREME": {"stretch": 1.5, "reclaim": 0.6, "tp": 0.6, "sl": 1.5},  # Very conservative
}

# Risk-off mode settings
RISK_OFF_COOLDOWN_MINUTES = 30     # Stay in risk-off for at least N minutes after trigger
RISK_OFF_FLATTEN_EXISTING = False  # If True, flatten positions when entering risk-off

# Exits in "R" where 1R = 1 * ATR dollars (FINAL OPTIMIZED - Balanced)
RTH_TP_R = 1.0                     # Symmetric 1R take profit
RTH_SL_R = 1.0                     # Symmetric 1R stop loss
EXT_TP_R = 0.8                     # Appropriate tight target for extended hours
EXT_SL_R = 1.0                     # Symmetric stop for extended hours

# CRITICAL FIX: Minimum stop distance to prevent noise stop-outs
# 1-minute ATR can be very small, especially in extended hours
# This ensures stops are at least MIN_STOP_DISTANCE_PCT% away from entry
# NOTE: 0.30% was too tight for leveraged ETFs (TQQQ, SOXL) - increased to 0.80%
MIN_STOP_DISTANCE_PCT = 0.80       # Minimum 0.80% stop distance (was 0.30% - too tight for 3x ETFs)
MIN_STOP_DISTANCE_ABS = 0.10       # Minimum $0.10 absolute stop distance (was $0.05)

# Break-even rule for EXT virtual brackets
# v18 BACKTEST: Disabled break-even - let trades run to TP for best results
ENABLE_BREAK_EVEN = False          # v18: DISABLED - letting trades run to TP is optimal
BREAK_EVEN_TRIGGER_R = 0.50
BREAK_EVEN_OFFSET_DOLLARS = 0.00

# --- Cooldowns ---
COOLDOWN_AFTER_ENTRY_MIN = 15
COOLDOWN_AFTER_CLOSE_MIN = 10

# --- News filter ---
SKIP_NEWS_MINUTES = 90
NEWS_CACHE_SECONDS = 300

# --- Spread & liquidity filters (from quotes) ---
REQUIRE_QUOTES = True
MAX_SPREAD_BPS = 12.0
MAX_SPREAD_ABS = 0.03
MIN_BID_SIZE_SHARES = 300
MIN_ASK_SIZE_SHARES = 300

# --- Feed health / stale detection ---
WS_STALE_SECONDS = 5.0
TRADE_STALE_SECONDS = 4.0
QUOTE_STALE_SECONDS = 4.0
CRITICAL_STALE_SECONDS = 12.0

# --- Entry pricing ---
ENTRY_SLIPPAGE_BPS_EXT = 15.0
MAX_ENTRY_SLIPPAGE_R = 0.30  # Max acceptable slippage in R-multiples for RTH fills

# --- Retry / circuit breaker ---
HTTP_TIMEOUT_SEC = 15
RETRY_MAX_ATTEMPTS = 4
RETRY_BASE_BACKOFF = 0.6
RETRY_MAX_BACKOFF = 6.0
RETRY_JITTER_MIN = 0.8  # Jitter multiplier min
RETRY_JITTER_MAX = 1.2  # Jitter multiplier max

CB_FAILS_TO_OPEN = 8

# --- Reliability & Robustness (Phase 3) ---
# Watchdog monitoring
WATCHDOG_ENABLED = True
WATCHDOG_MAX_LOOP_SECONDS = 120      # Alert if main loop takes longer than this
WATCHDOG_HEARTBEAT_FILE = "bot_heartbeat.txt"  # File updated each loop iteration
WATCHDOG_STALE_HEARTBEAT_SEC = 300   # Consider bot dead if heartbeat older than this

# API health tracking
API_HEALTH_WINDOW_SECONDS = 300      # Track API health over this window
API_LATENCY_WARN_MS = 2000           # Warn if API latency exceeds this
API_LATENCY_CRITICAL_MS = 5000       # Critical alert threshold
API_SUCCESS_RATE_WARN = 0.90         # Warn if success rate drops below 90%
API_SUCCESS_RATE_CRITICAL = 0.75     # Critical if below 75%

# Connection resilience
WS_RECONNECT_MAX_ATTEMPTS = 10       # Max reconnect attempts before alert
WS_RECONNECT_BACKOFF_BASE = 1.0      # Initial backoff for WS reconnect
WS_RECONNECT_BACKOFF_MAX = 60.0      # Max backoff between reconnects
WS_HEALTH_CHECK_INTERVAL = 30        # Check WS health every N seconds

# State persistence (note: STATE_FLUSH_SECONDS=5.0 debounces saves, STATE_BACKUP_COUNT=3 creates rolling backups)
STATE_INTEGRITY_CHECK = True         # Validate state JSON before saving
STATE_RECOVERY_MODE = "LATEST"       # LATEST (use most recent), BACKUP (use backup if main corrupt)

# Graceful degradation
DEGRADED_MODE_ENABLED = True         # Enable degraded mode on partial failures
DEGRADED_MODE_ACTIONS = ["MANAGE_ONLY", "NO_NEW_ENTRIES"]  # What to do in degraded mode
DATA_GAP_MAX_MINUTES = 5             # Max acceptable gap in market data
DATA_GAP_ACTION = "SKIP_SYMBOL"      # SKIP_SYMBOL, PAUSE_ALL, or CONTINUE
CB_WINDOW_SECONDS = 120
CB_OPEN_SLEEP_SECONDS = 30
CB_OPEN_MIN_SECONDS = 90

# --- Position reconciliation thresholds ---
QTY_MISMATCH_THRESHOLD_PCT = 0.10  # 10% mismatch triggers action
QTY_PARTIAL_FILL_FLATTEN_PCT = 0.50  # Flatten if < 50% filled

# If both quotes & trades are critically stale for a position, optionally flatten
ON_CRITICAL_DATA_MISSING_FLATTEN = True

# Startup reconcile policy for positions without local state
RECONCILE_IF_POSITION_NO_STATE = "RECONSTRUCT_OR_FLATTEN"  # or "FLATTEN"

# ============================================================
# LEVEL 3 PRODUCTION INFRASTRUCTURE
# ============================================================

# --- Live Trading Safety ---
LIVE_TRADING_ENABLED = os.getenv("LIVE_TRADING", "0") == "1"
LIVE_TRADING_CONFIRMATION = os.getenv("I_UNDERSTAND_LIVE_TRADING", "").upper()

# --- Multi-Channel Alerting ---
ENABLE_EMAIL_ALERTS = os.getenv("ENABLE_EMAIL_ALERTS", "0") == "1"
ENABLE_SLACK_ALERTS = os.getenv("ENABLE_SLACK_ALERTS", "0") == "1"
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "").strip()
ALERT_EMAIL_TO = os.getenv("ALERT_EMAIL_TO", "").strip()
ALERT_EMAIL_FROM = os.getenv("ALERT_EMAIL_FROM", "").strip()
SMTP_SERVER = os.getenv("SMTP_SERVER", "smtp.gmail.com").strip()
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USERNAME = os.getenv("SMTP_USERNAME", "").strip()
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "").strip()

# --- Log Rotation ---
LOG_FILE_PATH = os.getenv("LOG_FILE_PATH", str(LOGS_DIR / "vwap_bot.log"))
MAX_LOG_SIZE_MB = int(os.getenv("MAX_LOG_SIZE_MB", "50"))
MAX_LOG_BACKUPS = int(os.getenv("MAX_LOG_BACKUPS", "5"))

# --- Kill Switch ---
KILL_SWITCH_FILE = str(DATA_DIR / "KILL_SWITCH")
KILL_SWITCH_ENV = os.getenv("KILL_SWITCH", "0") == "1"

# --- Graceful Shutdown Policy ---
SHUTDOWN_POLICY = os.getenv("SHUTDOWN_POLICY", "CANCEL_ORDERS_ONLY").upper()  # CANCEL_ORDERS_ONLY | FLATTEN_ALL


# ============================================================
# Validation + ping sanity
# ============================================================

if not ALPACA_API_KEY or not ALPACA_SECRET_KEY:
    raise RuntimeError("Missing ALPACA_API_KEY / ALPACA_SECRET_KEY.")
if not POLYGON_API_KEY:
    raise RuntimeError("Missing POLYGON_API_KEY (or MASSIVE_API_KEY).")

# ============================================================
# Configuration Validation
# ============================================================

def validate_configuration():
    """Validate bot configuration and raise errors for invalid settings."""
    errors = []

    # API Key validation
    if not ALPACA_API_KEY or not ALPACA_SECRET_KEY:
        errors.append("Missing ALPACA_API_KEY or ALPACA_SECRET_KEY")
    if not POLYGON_API_KEY:
        errors.append("Missing POLYGON_API_KEY (or MASSIVE_API_KEY)")

    # Risk parameter validation
    if MAX_OPEN_POSITIONS <= 0:
        errors.append(f"MAX_OPEN_POSITIONS must be > 0, got {MAX_OPEN_POSITIONS}")
    if MAX_ENTRIES_PER_MINUTE <= 0:
        errors.append(f"MAX_ENTRIES_PER_MINUTE must be > 0, got {MAX_ENTRIES_PER_MINUTE}")

    if TRADE_CASH_PCT_RTH <= 0 or TRADE_CASH_PCT_RTH > 1:
        errors.append(f"TRADE_CASH_PCT_RTH must be between 0 and 1, got {TRADE_CASH_PCT_RTH}")
    if TRADE_CASH_PCT_EXT <= 0 or TRADE_CASH_PCT_EXT > 1:
        errors.append(f"TRADE_CASH_PCT_EXT must be between 0 and 1, got {TRADE_CASH_PCT_EXT}")

    if DAILY_MAX_LOSS_PCT is not None and (DAILY_MAX_LOSS_PCT <= 0 or DAILY_MAX_LOSS_PCT > 1):
        errors.append(f"DAILY_MAX_LOSS_PCT must be between 0 and 1, got {DAILY_MAX_LOSS_PCT}")

    # R-multiple validation
    if RTH_TP_R <= 0 or RTH_SL_R <= 0:
        errors.append(f"RTH R-multiples must be > 0 (TP={RTH_TP_R}, SL={RTH_SL_R})")
    if EXT_TP_R <= 0 or EXT_SL_R <= 0:
        errors.append(f"EXT R-multiples must be > 0 (TP={EXT_TP_R}, SL={EXT_SL_R})")

    if MAX_ENTRY_SLIPPAGE_R <= 0 or MAX_ENTRY_SLIPPAGE_R > 2:
        errors.append(f"MAX_ENTRY_SLIPPAGE_R should be between 0 and 2, got {MAX_ENTRY_SLIPPAGE_R}")

    # Indicator validation
    if ATR_LEN < 2 or ADX_LEN < 2:
        errors.append(f"ATR_LEN and ADX_LEN must be >= 2 (ATR={ATR_LEN}, ADX={ADX_LEN})")
    if LOOKBACK_MINUTES < max(ATR_LEN, ADX_LEN):
        errors.append(f"LOOKBACK_MINUTES must be >= max(ATR_LEN, ADX_LEN)")

    # Price filter validation
    if MIN_PRICE <= 0 or MAX_PRICE <= MIN_PRICE:
        errors.append(f"Invalid price range: MIN_PRICE={MIN_PRICE}, MAX_PRICE={MAX_PRICE}")

    # WebSocket ping validation
    if WS_PING_INTERVAL_SECONDS <= WS_PING_TIMEOUT_SECONDS:
        # Auto-correct instead of error
        pass

    # Spread/liquidity validation
    if MAX_SPREAD_BPS <= 0 or MAX_SPREAD_ABS <= 0:
        errors.append(f"Spread limits must be > 0 (BPS={MAX_SPREAD_BPS}, ABS={MAX_SPREAD_ABS})")

    # Universe validation
    if EXPAND_UNIVERSE and EXPANDED_UNIVERSE_SIZE < len(CORE_SYMBOLS):
        errors.append(f"EXPANDED_UNIVERSE_SIZE ({EXPANDED_UNIVERSE_SIZE}) should be >= len(CORE_SYMBOLS) ({len(CORE_SYMBOLS)})")

    # Phase 2: Volatility Adaptation validation
    if VIX_LOW_THRESHOLD >= VIX_HIGH_THRESHOLD:
        errors.append(f"VIX_LOW_THRESHOLD ({VIX_LOW_THRESHOLD}) must be < VIX_HIGH_THRESHOLD ({VIX_HIGH_THRESHOLD})")
    if VIX_HIGH_THRESHOLD >= VIX_EXTREME_THRESHOLD:
        errors.append(f"VIX_HIGH_THRESHOLD ({VIX_HIGH_THRESHOLD}) must be < VIX_EXTREME_THRESHOLD ({VIX_EXTREME_THRESHOLD})")
    if VIX_CACHE_MINUTES <= 0:
        errors.append(f"VIX_CACHE_MINUTES must be > 0, got {VIX_CACHE_MINUTES}")
    for regime in ["LOW", "NORMAL", "HIGH", "EXTREME"]:
        if regime not in VOL_REGIME_MULTIPLIERS:
            errors.append(f"VOL_REGIME_MULTIPLIERS missing required key: {regime}")
        if regime not in ATR_MULTIPLIER_BY_REGIME:
            errors.append(f"ATR_MULTIPLIER_BY_REGIME missing required key: {regime}")

    # Phase 3: Reliability validation
    if WATCHDOG_MAX_LOOP_SECONDS <= 0:
        errors.append(f"WATCHDOG_MAX_LOOP_SECONDS must be > 0, got {WATCHDOG_MAX_LOOP_SECONDS}")
    if API_HEALTH_WINDOW_SECONDS <= 0:
        errors.append(f"API_HEALTH_WINDOW_SECONDS must be > 0, got {API_HEALTH_WINDOW_SECONDS}")
    if API_SUCCESS_RATE_WARN <= API_SUCCESS_RATE_CRITICAL:
        errors.append(f"API_SUCCESS_RATE_WARN ({API_SUCCESS_RATE_WARN}) must be > API_SUCCESS_RATE_CRITICAL ({API_SUCCESS_RATE_CRITICAL})")
    if DATA_GAP_MAX_MINUTES <= 0:
        errors.append(f"DATA_GAP_MAX_MINUTES must be > 0, got {DATA_GAP_MAX_MINUTES}")
    if DATA_GAP_ACTION not in ["SKIP_SYMBOL", "PAUSE_ALL", "CONTINUE"]:
        errors.append(f"DATA_GAP_ACTION must be SKIP_SYMBOL, PAUSE_ALL, or CONTINUE, got {DATA_GAP_ACTION}")

    if errors:
        raise ValueError("Configuration validation failed:\n" + "\n".join(f"  - {e}" for e in errors))

# Validate configuration on load
validate_configuration()

# websocket-client requires ping_interval > ping_timeout
if WS_PING_INTERVAL_SECONDS <= WS_PING_TIMEOUT_SECONDS:
    WS_PING_TIMEOUT_SECONDS = max(1, WS_PING_INTERVAL_SECONDS - 5)


# ============================================================
# Helpers: time + logging
# ============================================================

def now_et() -> dt.datetime:
    return dt.datetime.now(tz=ET)

def ts() -> str:
    return now_et().strftime("%H:%M:%S")

def iso(dtobj: dt.datetime) -> str:
    return dtobj.isoformat()

def from_iso(s: str) -> dt.datetime:
    return dt.datetime.fromisoformat(s)

def parse_hhmm(hhmm: str) -> Tuple[int, int]:
    h, m = hhmm.split(":")
    return int(h), int(m)

def today_at(hhmm: str) -> dt.datetime:
    n = now_et()
    h, m = parse_hhmm(hhmm)
    return n.replace(hour=h, minute=m, second=0, microsecond=0)

def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))

class StatusLogger:
    """Enhanced logger with proper logging levels and throttling."""
    def __init__(self):
        self._last_t: Dict[str, float] = {}
        self._last_msg: Dict[str, str] = {}

        # Setup Python logging with rotation
        self.logger = logging.getLogger("VWAPBot")
        self.logger.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))

        # Prevent duplicate handlers
        if not self.logger.handlers:
            # Console handler
            console_handler = logging.StreamHandler()
            console_handler.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))
            formatter = logging.Formatter('[%(asctime)s] %(levelname)s: %(message)s', datefmt='%H:%M:%S')
            console_handler.setFormatter(formatter)
            self.logger.addHandler(console_handler)

            # File handler with rotation (Level 3)
            file_handler = RotatingFileHandler(
                LOG_FILE_PATH,
                maxBytes=MAX_LOG_SIZE_MB * 1024 * 1024,
                backupCount=MAX_LOG_BACKUPS
            )
            file_handler.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))
            file_handler.setFormatter(formatter)
            self.logger.addHandler(file_handler)

    def debug(self, msg: str):
        """Debug level logging."""
        self.logger.debug(msg)

    def info(self, msg: str):
        """Info level logging."""
        self.logger.info(msg)

    def warning(self, msg: str):
        """Warning level logging."""
        self.logger.warning(msg)

    def error(self, msg: str):
        """Error level logging."""
        self.logger.error(msg)

    def event(self, msg: str):
        """Legacy method - maps to info level."""
        self.logger.info(msg)

    def throttle(self, key: str, msg: str, every: float = LOG_THROTTLE_SECONDS, level: str = "INFO"):
        """Throttled logging to prevent spam."""
        t = time.time()
        lt = self._last_t.get(key, 0.0)
        lm = self._last_msg.get(key, "")
        if (t - lt) >= every or msg != lm:
            self._last_t[key] = t
            self._last_msg[key] = msg
            if level == "DEBUG":
                self.logger.debug(msg)
            elif level == "WARNING":
                self.logger.warning(msg)
            elif level == "ERROR":
                self.logger.error(msg)
            else:
                self.logger.info(msg)


# ============================================================
# LEVEL 3: Multi-Channel Alerter
# ============================================================

class Alerter:
    """Multi-channel alerting for unattended operation (Slack + Email)."""

    def __init__(self, logger: StatusLogger):
        self.logger = logger
        self.slack_enabled = ENABLE_SLACK_ALERTS and SLACK_WEBHOOK_URL
        self.email_enabled = ENABLE_EMAIL_ALERTS and ALERT_EMAIL_TO and SMTP_USERNAME

        if self.slack_enabled:
            self.logger.info("[ALERTER] Slack alerts ENABLED")
        if self.email_enabled:
            self.logger.info("[ALERTER] Email alerts ENABLED")
        if not self.slack_enabled and not self.email_enabled:
            self.logger.warning("[ALERTER] NO ALERTS CONFIGURED - unattended operation not recommended")

    def send_alert(self, level: str, title: str, message: str, context: dict = None):
        """
        Send alert via all enabled channels.

        Args:
            level: INFO, WARNING, CRITICAL
            title: Short summary
            message: Detailed message
            context: Optional dict with additional data
        """
        # Always log locally
        log_msg = f"[ALERT {level}] {title}: {message}"
        if context:
            log_msg += f" | {context}"

        if level == "CRITICAL":
            self.logger.error(log_msg)
        elif level == "WARNING":
            self.logger.warning(log_msg)
        else:
            self.logger.info(log_msg)

        # Send to external channels
        if self.slack_enabled:
            self._send_slack(level, title, message, context)
        if self.email_enabled:
            self._send_email(level, title, message, context)

    def _send_slack(self, level: str, title: str, message: str, context: dict = None):
        """Send Slack webhook notification."""
        try:
            # Color coding
            colors = {"INFO": "#36a64f", "WARNING": "#ff9900", "CRITICAL": "#ff0000"}
            color = colors.get(level, "#808080")

            payload = {
                "attachments": [{
                    "color": color,
                    "title": f"{level}: {title}",
                    "text": message,
                    "fields": [{"title": k, "value": str(v), "short": True} for k, v in (context or {}).items()],
                    "footer": "VWAP Bot",
                    "ts": int(time.time())
                }]
            }

            response = requests.post(SLACK_WEBHOOK_URL, json=payload, timeout=5)
            response.raise_for_status()
        except Exception as e:
            self.logger.error(f"[ALERTER] Slack send failed: {e}")

    def _send_email(self, level: str, title: str, message: str, context: dict = None):
        """Send email notification via SMTP."""
        try:
            msg = MIMEMultipart()
            msg['From'] = ALERT_EMAIL_FROM
            msg['To'] = ALERT_EMAIL_TO
            msg['Subject'] = f"[VWAP Bot {level}] {title}"

            body = f"{message}\n\n"
            if context:
                body += "Context:\n" + "\n".join(f"  {k}: {v}" for k, v in context.items())
            body += f"\n\nTimestamp: {now_et().isoformat()}"

            msg.attach(MIMEText(body, 'plain'))

            with smtplib.SMTP(SMTP_SERVER, SMTP_PORT, timeout=10) as server:
                server.starttls()
                server.login(SMTP_USERNAME, SMTP_PASSWORD)
                server.send_message(msg)
        except Exception as e:
            self.logger.error(f"[ALERTER] Email send failed: {e}")


# ============================================================
# LEVEL 3: Kill Switch
# ============================================================

class KillSwitch:
    """Emergency halt mechanism via file or environment variable."""

    def __init__(self, logger: StatusLogger):
        self.logger = logger

    def is_triggered(self) -> Tuple[bool, Optional[str]]:
        """Check if kill switch is activated."""
        # Check environment variable
        if KILL_SWITCH_ENV:
            return True, "environment variable KILL_SWITCH=1"

        # Check file
        if os.path.exists(KILL_SWITCH_FILE):
            return True, f"file '{KILL_SWITCH_FILE}' exists"

        return False, None

    def execute_emergency_shutdown(self, alerter: Alerter):
        """Execute emergency shutdown procedures."""
        triggered, reason = self.is_triggered()
        if not triggered:
            return

        self.logger.error(f"[KILL_SWITCH] TRIGGERED: {reason}")
        alerter.send_alert(
            "CRITICAL",
            "Kill Switch Activated",
            f"Emergency shutdown triggered by {reason}",
            {"reason": reason, "time": iso(now_et())}
        )

        try:
            # Cancel all orders
            self.logger.info("[KILL_SWITCH] Cancelling all orders...")
            cancel_all_orders()

            # Optionally flatten positions based on policy
            if SHUTDOWN_POLICY == "FLATTEN_ALL":
                self.logger.info("[KILL_SWITCH] Flattening all positions...")
                close_all_positions()
        except Exception as e:
            self.logger.error(f"[KILL_SWITCH] Shutdown error: {e}")
            alerter.send_alert("CRITICAL", "Kill Switch Error", str(e))


# ============================================================
# Circuit breaker + retry wrappers
# ============================================================

class CircuitBreaker:
    """API degradation detection with alerting (Level 3 enhanced)."""

    def __init__(self, logger: StatusLogger, alerter: Optional['Alerter'] = None):
        self.fail_times: List[float] = []
        self.open_until: float = 0.0
        self.logger = logger
        self.alerter = alerter
        self.last_alert_time: float = 0.0

    def record_fail(self, api_name: str = "API"):
        t = time.time()
        self.fail_times.append(t)
        cutoff = t - CB_WINDOW_SECONDS
        self.fail_times = [x for x in self.fail_times if x >= cutoff]

        if len(self.fail_times) >= CB_FAILS_TO_OPEN:
            was_closed = not self.is_open()
            self.open_until = max(self.open_until, t + CB_OPEN_MIN_SECONDS)

            # Alert on transition to open (avoid spam)
            if was_closed and self.alerter and (t - self.last_alert_time) > 300:
                self.last_alert_time = t
                self.logger.error(f"[CIRCUIT_BREAKER] TRIPPED: {api_name} degradation detected")
                self.alerter.send_alert(
                    "CRITICAL",
                    "Circuit Breaker Tripped",
                    f"{api_name} failure rate exceeded threshold",
                    {
                        "failures": len(self.fail_times),
                        "window_seconds": CB_WINDOW_SECONDS,
                        "threshold": CB_FAILS_TO_OPEN
                    }
                )

    def is_open(self) -> bool:
        return time.time() < self.open_until

    def reset(self):
        self.fail_times = []
        self.open_until = 0.0

# CB will be initialized in main() after logger and alerter are created
CB = None

def with_retries(fn, *, name: str):
    """
    Execute function with retries and health monitoring.

    Phase 3 enhancement: Tracks latency and success rate via APIHealthMonitor.
    """
    attempt = 0
    while True:
        attempt += 1
        start_time = time.time()
        try:
            result = fn()
            # Phase 3: Record successful request
            latency_ms = (time.time() - start_time) * 1000
            if api_health_monitor:
                api_health_monitor.record_request(name, latency_ms, success=True)
            return result
        except requests.HTTPError as e:
            latency_ms = (time.time() - start_time) * 1000
            status = getattr(e.response, "status_code", None)
            error_type = f"HTTP_{status}" if status else "HTTP_UNKNOWN"

            # Phase 3: Record failed request
            if api_health_monitor:
                api_health_monitor.record_request(name, latency_ms, success=False, error_type=error_type)

            if status == 429 and attempt < RETRY_MAX_ATTEMPTS:
                if CB:
                    CB.record_fail(name)
                ra = e.response.headers.get("Retry-After")
                sleep_s = float(ra) if ra and str(ra).isdigit() else (RETRY_BASE_BACKOFF * (2 ** (attempt - 1)))
                jitter = RETRY_JITTER_MIN + (RETRY_JITTER_MAX - RETRY_JITTER_MIN) * random.random()
                sleep_s = clamp(sleep_s, 0.5, RETRY_MAX_BACKOFF) * jitter
                time.sleep(sleep_s)
                continue
            if CB:
                CB.record_fail(name)
            raise
        except Exception as e:
            latency_ms = (time.time() - start_time) * 1000

            # Phase 3: Record failed request
            if api_health_monitor:
                api_health_monitor.record_request(name, latency_ms, success=False, error_type=type(e).__name__)

            if CB:
                CB.record_fail(name)
            if attempt >= RETRY_MAX_ATTEMPTS:
                raise
            sleep_s = (RETRY_BASE_BACKOFF * (2 ** (attempt - 1)))
            jitter = RETRY_JITTER_MIN + (RETRY_JITTER_MAX - RETRY_JITTER_MIN) * random.random()
            sleep_s = clamp(sleep_s, 0.5, RETRY_MAX_BACKOFF) * jitter
            time.sleep(sleep_s)


# ============================================================
# Alpaca REST
# ============================================================

def alpaca_headers() -> Dict[str, str]:
    return {
        "APCA-API-KEY-ID": ALPACA_API_KEY,
        "APCA-API-SECRET-KEY": ALPACA_SECRET_KEY,
        "Content-Type": "application/json",
    }

def alpaca_get(path: str, params: Optional[dict] = None) -> Any:
    def _do():
        r = requests.get(f"{ALPACA_BASE_URL}{path}", headers=alpaca_headers(), params=params, timeout=HTTP_TIMEOUT_SEC)
        r.raise_for_status()
        return r.json()
    return with_retries(_do, name=f"alpaca_get {path}")

def alpaca_post(path: str, payload: dict) -> Any:
    def _do():
        r = requests.post(
            f"{ALPACA_BASE_URL}{path}",
            headers=alpaca_headers(),
            data=json.dumps(payload),
            timeout=HTTP_TIMEOUT_SEC,
        )
        r.raise_for_status()
        return r.json()
    return with_retries(_do, name=f"alpaca_post {path}")

def alpaca_delete(path: str) -> Any:
    def _do():
        r = requests.delete(f"{ALPACA_BASE_URL}{path}", headers=alpaca_headers(), timeout=HTTP_TIMEOUT_SEC)
        r.raise_for_status()
        return r.json() if r.text else {}
    return with_retries(_do, name=f"alpaca_delete {path}")

def alpaca_account() -> dict:
    return alpaca_get("/v2/account")

def alpaca_positions() -> List[dict]:
    return alpaca_get("/v2/positions")

def alpaca_orders(status: str = "open", limit: int = 500) -> List[dict]:
    return alpaca_get("/v2/orders", params={"status": status, "limit": str(limit)})

def cancel_all_orders():
    alpaca_delete("/v2/orders")

def close_all_positions():
    alpaca_delete("/v2/positions")

def close_position(sym: str, logger=None, session: MarketSession = None, q_tape: 'QuoteTape' = None):
    """
    Close position at broker.

    For RTH: Uses Alpaca's liquidation API (market order).
    For PRE/AFTER: Submits aggressive limit order (market orders don't work in extended hours).

    Returns True if position was closed successfully or doesn't exist.
    Returns False if error occurred that should be retried.
    """
    # RTH: Use Alpaca liquidation API (market order, fast and reliable)
    if session == MarketSession.RTH or session is None:
        try:
            alpaca_delete(f"/v2/positions/{sym}")
            if logger:
                logger.info(f"[EXIT] {sym} position closed successfully (market order)")
            return True
        except requests.HTTPError as e:
            # 404 = position doesn't exist (already closed) - this is OK
            # 403 = forbidden (position doesn't exist or other auth issue) - treat as already closed
            if e.response.status_code in (403, 404):
                if logger:
                    logger.info(f"[EXIT] {sym} position already closed (HTTP {e.response.status_code})")
                return True  # Position doesn't exist - success from our perspective
            # Other errors (5xx, network, etc) - should retry
            if logger:
                logger.error(f"[EXIT] {sym} failed to close position: {e}")
            raise

    # PRE/AFTER: Submit aggressive limit order to exit (market orders rejected in extended hours)
    else:
        try:
            # Get current position to determine side
            pos = alpaca_get(f"/v2/positions/{sym}")
            qty = abs(int(float(pos["qty"])))
            current_side = pos["side"]  # "long" or "short"

            # Determine exit side (opposite of position side)
            exit_side = OrderSide.SELL if current_side == "long" else OrderSide.BUY

            # Get aggressive limit price from current quote
            limit_px = None
            if q_tape:
                q, q_ts = q_tape.get(sym)
                now_ts = time.time()
                quote_fresh = q_ts and (now_ts - q_ts) < 10.0  # Quote less than 10 seconds old

                if q and quote_fresh and q.bid > 0 and q.ask > 0 and q.ask >= q.bid:
                    if exit_side == OrderSide.BUY:
                        # Buying to close short: use ask + 0.5% to ensure fill
                        limit_px = round_price(q.ask * 1.005)
                    else:
                        # Selling to close long: use bid - 0.5% to ensure fill
                        limit_px = round_price(q.bid * 0.995)

            # Fallback: if no quote, use current market price + buffer
            if limit_px is None:
                current_px = float(pos["current_price"])
                if exit_side == OrderSide.BUY:
                    limit_px = round_price(current_px * 1.01)  # 1% above for buy
                else:
                    limit_px = round_price(current_px * 0.99)  # 1% below for sell

            # Submit aggressive limit order
            payload = {
                "symbol": sym,
                "qty": str(qty),
                "side": exit_side.value,
                "type": "limit",
                "limit_price": f"{limit_px:.6f}",
                "time_in_force": "day",
                "extended_hours": True,
            }
            order = alpaca_post("/v2/orders", payload)

            if logger:
                logger.info(f"[EXIT] {sym} limit exit order submitted @ ${limit_px:.2f} (extended hours, order_id={order['id']})")
            return True

        except requests.HTTPError as e:
            if e.response.status_code in (403, 404):
                if logger:
                    logger.info(f"[EXIT] {sym} position already closed (HTTP {e.response.status_code})")
                return True
            if logger:
                logger.error(f"[EXIT] {sym} failed to submit exit order: {e}")
            raise

def alpaca_clock() -> dict:
    return alpaca_get("/v2/clock")

def alpaca_calendar(day: dt.date) -> Optional[dict]:
    cal = alpaca_get("/v2/calendar", params={"start": day.isoformat(), "end": day.isoformat()})
    return cal[0] if cal else None

def get_order(order_id: str) -> dict:
    return alpaca_get(f"/v2/orders/{order_id}")


# ============================================================
# Polygon/Massive REST
# ============================================================

class PolyRest:
    def __init__(self, base: str, api_key: str):
        self.base = base.rstrip("/")
        self.api_key = api_key
        self.sess = requests.Session()

    def _get(self, path: str, params: Optional[dict] = None) -> dict:
        params = params or {}
        params["apiKey"] = self.api_key

        def _do():
            r = self.sess.get(f"{self.base}{path}", params=params, timeout=HTTP_TIMEOUT_SEC)
            r.raise_for_status()
            return r.json()
        return with_retries(_do, name=f"poly_get {path}")

    def grouped_daily(self, yyyy_mm_dd: str) -> List[dict]:
        out = self._get(
            f"/v2/aggs/grouped/locale/us/market/stocks/{yyyy_mm_dd}",
            params={"adjusted": "true", "include_otc": "false"},
        )
        return out.get("results", []) or []

    def agg_1m(self, sym: str, start_yyyy_mm_dd: str, end_yyyy_mm_dd: str, limit: int = 5000) -> pd.DataFrame:
        out = self._get(
            f"/v2/aggs/ticker/{sym}/range/1/minute/{start_yyyy_mm_dd}/{end_yyyy_mm_dd}",
            params={"adjusted": "true", "sort": "asc", "limit": str(limit)},
        )
        rows = out.get("results", []) or []
        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame(rows)
        df["ts"] = pd.to_datetime(df["t"], unit="ms", utc=True).dt.tz_convert(ET)
        df = df.rename(columns={"o": "open", "h": "high", "l": "low", "c": "close", "v": "volume", "vw": "vwap"})
        return df.set_index("ts")[["open", "high", "low", "close", "volume", "vwap"]].copy()

    def has_recent_news(self, sym: str, minutes: int) -> bool:
        since = (dt.datetime.now(tz=dt.timezone.utc) - dt.timedelta(minutes=minutes)).isoformat().replace("+00:00", "Z")
        out = self._get(
            "/v2/reference/news",
            params={
                "ticker": sym,
                "published_utc.gte": since,
                "limit": "1",
                "order": "desc",
                "sort": "published_utc",
            },
        )
        return bool(out.get("results"))

poly = PolyRest(POLYGON_REST_BASE, POLYGON_API_KEY)


# ============================================================
# WebSocket tapes + health (Massive schema: list of quote/trade objects)
# ============================================================

@dataclass
class Quote:
    bid: float
    ask: float
    bid_size_shares: int
    ask_size_shares: int
    ts_iso: str

class TradeTape:
    def __init__(self):
        self._lock = threading.Lock()
        self.last_price: Dict[str, float] = {}
        self.last_ts: Dict[str, float] = {}

    def update(self, sym: str, price: float):
        with self._lock:
            self.last_price[sym] = price
            self.last_ts[sym] = time.time()

    def get(self, sym: str) -> Tuple[Optional[float], Optional[float]]:
        with self._lock:
            return self.last_price.get(sym), self.last_ts.get(sym)

class QuoteTape:
    def __init__(self):
        self._lock = threading.Lock()
        self.last_quote: Dict[str, Quote] = {}
        self.last_ts: Dict[str, float] = {}

    def update(self, sym: str, bid: float, ask: float, bs_shares: int, as_shares: int, t_ms: Optional[int] = None):
        ts_ = (
            dt.datetime.fromtimestamp(t_ms / 1000, tz=dt.timezone.utc).astimezone(ET)
            if t_ms is not None else now_et()
        )
        q = Quote(
            bid=float(bid),
            ask=float(ask),
            bid_size_shares=int(bs_shares),
            ask_size_shares=int(as_shares),
            ts_iso=iso(ts_),
        )
        with self._lock:
            self.last_quote[sym] = q
            self.last_ts[sym] = time.time()

    def get(self, sym: str) -> Tuple[Optional[Quote], Optional[float]]:
        with self._lock:
            return self.last_quote.get(sym), self.last_ts.get(sym)

class WSHealth:
    def __init__(self):
        self._lock = threading.Lock()
        self.connected = False
        self.last_msg_time = 0.0

    def set_connected(self, v: bool):
        with self._lock:
            self.connected = v
            if v:
                self.last_msg_time = time.time()

    def touch(self):
        with self._lock:
            self.last_msg_time = time.time()

    def snapshot(self) -> Tuple[bool, float]:
        with self._lock:
            return self.connected, self.last_msg_time

class WSClient(threading.Thread):
    """
    Massive sends arrays: [{"ev":"Q",...}, {"ev":"T",...}]

    FIX: subscribe only once AFTER auth success.
    """
    def __init__(self, ws_url: str, api_key: str, t_tape: TradeTape, q_tape: QuoteTape, health: WSHealth, log: StatusLogger):
        super().__init__(daemon=True)
        self.ws_url = ws_url
        self.api_key = api_key
        self.t_tape = t_tape
        self.q_tape = q_tape
        self.health = health
        self.log = log

        self._stop = threading.Event()
        self._cmdq: "queue.Queue[List[str]]" = queue.Queue(maxsize=10)  # FIX: Limit queue size
        self._wsapp: Optional[websocket.WebSocketApp] = None
        self.error: Optional[Exception] = None

        self._sym_lock = threading.Lock()
        self._desired_symbols: List[str] = []
        self._subscribed: set = set()

        self._auth_lock = threading.Lock()
        self._authed: bool = False

    def set_symbols(self, symbols: List[str]):
        symbols = list(dict.fromkeys(symbols))
        with self._sym_lock:
            self._desired_symbols = symbols
        # FIX: Only enqueue if authenticated, clear old pending commands
        if self._is_authed():
            try:
                # Clear old pending commands
                while not self._cmdq.empty():
                    try:
                        self._cmdq.get_nowait()
                    except queue.Empty:
                        break
                self._cmdq.put(symbols)
            except queue.Full:
                self.log.event("[WS] command queue full, skipping subscription update")

    def _get_desired_symbols(self) -> List[str]:
        with self._sym_lock:
            return list(self._desired_symbols)

    def _set_authed(self, v: bool):
        with self._auth_lock:
            self._authed = v

    def _is_authed(self) -> bool:
        with self._auth_lock:
            return self._authed

    def stop(self):
        self._stop.set()
        try:
            if self._wsapp:
                self._wsapp.close()
        except Exception:
            pass

    def _subscribe_force(self, ws, symbols: List[str]):
        if not symbols:
            return
        params = ",".join([f"T.{s}" for s in symbols] + [f"Q.{s}" for s in symbols])
        ws.send(json.dumps({"action": "subscribe", "params": params}))
        self._subscribed = set(symbols)
        self.log.event(f"[WS] subscribed +{len(symbols)} symbols (total={len(self._subscribed)})")

    def _subscribe_add(self, ws, symbols: List[str]):
        new = [s for s in symbols if s not in self._subscribed]
        if not new:
            return
        params = ",".join([f"T.{s}" for s in new] + [f"Q.{s}" for s in new])
        ws.send(json.dumps({"action": "subscribe", "params": params}))
        self._subscribed.update(new)
        self.log.event(f"[WS] subscribed +{len(new)} symbols (total={len(self._subscribed)})")

    @staticmethod
    def _looks_like_auth_ok(msg: dict) -> bool:
        # Try to be strict so we don’t resubscribe on “authenticating…”
        parts = []
        for k in ("status", "message", "msg", "info", "reason"):
            v = msg.get(k)
            if isinstance(v, str):
                parts.append(v)
        s = " ".join(parts).lower()
        if "auth" not in s:
            return False
        if "authenticated" in s:
            return True
        if "auth_success" in s or "auth success" in s:
            return True
        if "successfully authenticated" in s:
            return True
        # Explicitly exclude “authenticating”
        if "authenticating" in s:
            return False
        # If vendor only says “success” on auth responses, this can be too broad;
        # keep it conservative:
        return False

    def _on_open(self, ws):
        self.health.set_connected(True)
        self._subscribed = set()
        self._set_authed(False)

        self.log.event("[WS] connected. authenticating… (will subscribe after auth)")
        ws.send(json.dumps({"action": "auth", "params": self.api_key}))

    def _on_close(self, ws, *_):
        self.health.set_connected(False)
        self._set_authed(False)
        self.log.throttle("ws_closed", "[WS] disconnected. waiting to reconnect…", every=5)

    def _on_message(self, ws, message: str):
        self.health.touch()
        try:
            data = json.loads(message)
        except Exception:
            return

        if isinstance(data, dict):
            data = [data]
        if not isinstance(data, list):
            return

        for msg in data:
            if not isinstance(msg, dict):
                continue

            ev = msg.get("ev")

            # Status / auth flow
            if ev in ("status", "STATUS") or ("status" in msg and ev is None) or ("message" in msg and ev is None):
                if (not self._is_authed()) and self._looks_like_auth_ok(msg):
                    self._set_authed(True)
                    desired = self._get_desired_symbols()
                    if desired:
                        # Subscribe exactly once after auth
                        self._subscribe_force(ws, desired)
                    self.log.event("[WS] authenticated.")
                continue

            sym = msg.get("sym")
            if not sym:
                continue

            if ev == "T":
                p = msg.get("p")
                if p is not None:
                    self.t_tape.update(sym, float(p))

            elif ev == "Q":
                bp = msg.get("bp")
                ap = msg.get("ap")
                bs = msg.get("bs")
                a_s = msg.get("as")
                t_ms = msg.get("t")
                if bp is None or ap is None or bs is None or a_s is None:
                    continue
                self.q_tape.update(
                    sym=sym,
                    bid=float(bp),
                    ask=float(ap),
                    bs_shares=int(bs),
                    as_shares=int(a_s),
                    t_ms=int(t_ms) if t_ms is not None else None,
                )

    def _on_error(self, ws, err):
        self.error = RuntimeError(f"WebSocket error: {err}")
        self.log.throttle("ws_err", f"[WS] error: {err}", every=5, level="ERROR")
        try:
            ws.close()
        except Exception:
            pass

    def run(self):
        while not self._stop.is_set():
            self.error = None
            self._wsapp = websocket.WebSocketApp(
                self.ws_url,
                on_open=self._on_open,
                on_message=self._on_message,
                on_error=self._on_error,
                on_close=self._on_close,
            )

            def cmd_worker():
                while not self._stop.is_set():
                    try:
                        symbols = self._cmdq.get(timeout=1.0)
                    except queue.Empty:
                        continue
                    try:
                        if not self._is_authed():
                            # Don’t spam subscribe before auth; just wait.
                            continue
                        if self._wsapp and self._wsapp.sock and self._wsapp.sock.connected:
                            self._subscribe_add(self._wsapp, symbols)
                    except Exception as e:
                        self.error = e
                        return

            threading.Thread(target=cmd_worker, daemon=True).start()

            try:
                self._wsapp.run_forever(
                    ping_interval=WS_PING_INTERVAL_SECONDS,
                    ping_timeout=WS_PING_TIMEOUT_SECONDS
                )
            except Exception as e:
                self.error = e
                self.log.error(f"[WS] run_forever crashed: {e}")

            time.sleep(2.0)


# ============================================================
# Sessions
# ============================================================

class MarketSession(Enum):
    PRE = "PRE"
    RTH = "RTH"
    AFTER = "AFTER"
    CLOSED = "CLOSED"

@dataclass
class MarketDay:
    day: str
    open_iso: str
    close_iso: str
    ext_open_iso: str
    ext_close_iso: str
    early_close: bool

def get_market_day(d: dt.date) -> Optional[MarketDay]:
    item = alpaca_calendar(d)
    if not item:
        return None

    oh, om = map(int, item["open"].split(":"))
    ch, cm = map(int, item["close"].split(":"))
    open_et = dt.datetime(d.year, d.month, d.day, oh, om, tzinfo=ET)
    close_et = dt.datetime(d.year, d.month, d.day, ch, cm, tzinfo=ET)
    ext_open = dt.datetime(d.year, d.month, d.day, 4, 0, tzinfo=ET)
    early = (ch == 13 and cm == 0)
    ext_close = dt.datetime(d.year, d.month, d.day, 17, 0, tzinfo=ET) if early else dt.datetime(d.year, d.month, d.day, 20, 0, tzinfo=ET)

    return MarketDay(
        day=d.isoformat(),
        open_iso=iso(open_et),
        close_iso=iso(close_et),
        ext_open_iso=iso(ext_open),
        ext_close_iso=iso(ext_close),
        early_close=early,
    )

def session_now(now: dt.datetime, md: Optional[MarketDay]) -> MarketSession:
    if md is None:
        return MarketSession.CLOSED
    open_et = from_iso(md.open_iso)
    close_et = from_iso(md.close_iso)
    ext_open = from_iso(md.ext_open_iso)
    ext_close = from_iso(md.ext_close_iso)

    if ext_open <= now < open_et:
        return MarketSession.PRE
    if open_et <= now < close_et:
        return MarketSession.RTH
    if close_et <= now < ext_close:
        return MarketSession.AFTER
    return MarketSession.CLOSED

def sleep_until_next_open_seconds() -> float:
    c = alpaca_get("/v2/clock")
    nxt = dt.datetime.fromisoformat(c["next_open"].replace("Z", "+00:00")).astimezone(ET)
    now = dt.datetime.fromisoformat(c["timestamp"].replace("Z", "+00:00")).astimezone(ET)
    pre = dt.datetime(nxt.year, nxt.month, nxt.day, 4, 0, tzinfo=ET)
    target = pre if pre > now else nxt
    return max(30.0, (target - now).total_seconds() - 120.0)

def no_new_entries_window(now: dt.datetime, md: Optional[MarketDay]) -> bool:
    if md is None:
        return True
    if md.early_close and HARD_FLATTEN_ON_EARLY_CLOSE:
        close_et = from_iso(md.close_iso)
        cutoff = close_et - dt.timedelta(minutes=15)
        return now >= cutoff
    return now >= today_at(NO_NEW_ENTRIES_AFTER_ET)

def hard_flatten_window(now: dt.datetime, md: Optional[MarketDay]) -> bool:
    if md is None:
        return False
    if md.early_close and HARD_FLATTEN_ON_EARLY_CLOSE:
        close_et = from_iso(md.close_iso)
        return now >= (close_et - dt.timedelta(minutes=5))
    return now >= today_at(HARD_FLATTEN_ET)


# ============================================================
# Indicators + VWAP reclaim signal
# ============================================================

def atr(df: pd.DataFrame, n: int) -> pd.Series:
    prev_close = df["close"].shift(1)
    tr = pd.concat([
        (df["high"] - df["low"]).abs(),
        (df["high"] - prev_close).abs(),
        (df["low"] - prev_close).abs(),
    ], axis=1).max(axis=1)
    return tr.rolling(n).mean()

def adx(df: pd.DataFrame, n: int) -> pd.Series:
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

def vwap_from_anchor(df: pd.DataFrame, anchor: dt.datetime) -> pd.Series:
    x = df[df.index >= anchor].copy()
    if x.empty:
        return pd.Series(index=df.index, dtype=float)
    pv = (x["close"] * x["volume"]).cumsum()
    vv = x["volume"].cumsum().replace(0, float("nan"))
    v = pv / vv
    out = pd.Series(index=df.index, dtype=float)
    out.loc[v.index] = v
    return out

@dataclass
class SignalFlags:
    stretched_long: bool = False
    stretched_short: bool = False
    stretch_bar_idx: int = -1  # Bar index when stretch was triggered (for timing validation)

class OrderSide(Enum):
    BUY = "buy"
    SELL = "sell"

def anchor_for_session(now: dt.datetime, md: Optional[MarketDay], session: MarketSession) -> dt.datetime:
    if md is None:
        return now - dt.timedelta(hours=6)
    d = now.date()
    ext_open = dt.datetime(d.year, d.month, d.day, 4, 0, tzinfo=ET)
    rth_open = from_iso(md.open_iso)
    return rth_open if session == MarketSession.RTH else ext_open

def evaluate_vwap_reclaim(sym: str, df_1m: pd.DataFrame, anchor: dt.datetime, flags: SignalFlags) -> Optional[Tuple[OrderSide, float]]:
    """
    Enhanced VWAP reclaim signal with quality filters.

    Phase 1 improvements:
    - Minimum % displacement from VWAP (not just ATR-based)
    - Candle close confirmation (not just touch)
    - Timing filters (no trades in first N minutes, stretch staleness)
    - Bar count validation between stretch and reclaim

    Phase 2 improvements:
    - Dynamic ATR multipliers based on volatility regime
    """
    # Validate sufficient bars
    min_bars_needed = max(ATR_LEN, ADX_LEN) + 10
    if df_1m.empty or len(df_1m) < min_bars_needed:
        return None

    df = df_1m.copy()
    df["atr"] = atr(df, ATR_LEN)
    df["adx"] = adx(df, ADX_LEN)
    df["vwap_sess"] = vwap_from_anchor(df, anchor)

    # Validate we have enough valid (non-NaN) ATR and ADX values
    valid_atr = df["atr"].notna().sum()
    valid_adx = df["adx"].notna().sum()
    if valid_atr < ATR_LEN or valid_adx < ADX_LEN:
        return None

    current_bar_idx = len(df) - 1
    last = df.iloc[-1]
    a = float(last["atr"]) if pd.notna(last["atr"]) else 0.0
    x = float(last["adx"]) if pd.notna(last["adx"]) else 999.0
    vwap = float(last["vwap_sess"]) if pd.notna(last["vwap_sess"]) else float("nan")
    px = float(last["close"])

    if a <= 0 or math.isnan(vwap):
        return None
    if x > TREND_ADX_MAX:
        return None

    # === Phase 1: No trades in first N minutes after session open ===
    bars_since_anchor = len(df[df.index >= anchor])
    if bars_since_anchor < NO_TRADE_FIRST_MINUTES:
        return None

    vol = float(last["volume"])
    vol_mean = float(df["volume"].rolling(30).mean().iloc[-1] or 0.0)
    relvol = vol / vol_mean if vol_mean > 0 else 0.0
    if relvol < MIN_REL_VOL:
        return None

    # === Phase 2: Get volatility-adjusted ATR multipliers ===
    atr_mults = ATR_MULTIPLIER_BY_REGIME["NORMAL"]  # Default
    if volatility_manager:
        atr_mults = volatility_manager.get_atr_multipliers()

    stretch = STRETCH_ATR * a * atr_mults.get("stretch", 1.0)
    reclaim = RECLAIM_ATR * a * atr_mults.get("reclaim", 1.0)

    # === Phase 1: Check for stretch with enhanced criteria ===
    # Calculate percentage displacement from VWAP
    displacement_pct = abs(px - vwap) / vwap * 100.0 if vwap > 0 else 0.0

    # Long stretch: price below VWAP by ATR threshold AND percentage threshold
    if px < vwap - stretch and displacement_pct >= MIN_VWAP_DISPLACEMENT_PCT:
        if not flags.stretched_long:
            flags.stretched_long = True
            flags.stretch_bar_idx = current_bar_idx  # Record when stretch occurred

    # Short stretch: price above VWAP by ATR threshold AND percentage threshold
    if px > vwap + stretch and displacement_pct >= MIN_VWAP_DISPLACEMENT_PCT:
        if not flags.stretched_short:
            flags.stretched_short = True
            flags.stretch_bar_idx = current_bar_idx  # Record when stretch occurred

    # === Phase 1: Expire stale stretches ===
    if flags.stretch_bar_idx >= 0:
        bars_since_stretch = current_bar_idx - flags.stretch_bar_idx
        if bars_since_stretch > MAX_BARS_SINCE_STRETCH:
            # Stretch is too old, reset
            flags.stretched_long = False
            flags.stretched_short = False
            flags.stretch_bar_idx = -1
            return None

    # === Check for reclaim with enhanced criteria ===
    if flags.stretched_long and px >= (vwap - reclaim):
        bars_since_stretch = current_bar_idx - flags.stretch_bar_idx if flags.stretch_bar_idx >= 0 else 0

        # Phase 1: Require minimum bars between stretch and reclaim (avoid whipsaws)
        if bars_since_stretch < MIN_BARS_SINCE_STRETCH:
            return None

        # Phase 1: Require candle CLOSE above reclaim threshold (not just wick)
        if REQUIRE_CANDLE_CLOSE_CONFIRM:
            # For long reclaim, close must be above (vwap - reclaim)
            if px < (vwap - reclaim):
                return None

        # Valid long reclaim signal
        flags.stretched_long = False
        flags.stretch_bar_idx = -1
        return (OrderSide.BUY, a)

    if flags.stretched_short and px <= (vwap + reclaim):
        bars_since_stretch = current_bar_idx - flags.stretch_bar_idx if flags.stretch_bar_idx >= 0 else 0

        # Phase 1: Require minimum bars between stretch and reclaim (avoid whipsaws)
        if bars_since_stretch < MIN_BARS_SINCE_STRETCH:
            return None

        # Phase 1: Require candle CLOSE below reclaim threshold (not just wick)
        if REQUIRE_CANDLE_CLOSE_CONFIRM:
            # For short reclaim, close must be below (vwap + reclaim)
            if px > (vwap + reclaim):
                return None

        # Valid short reclaim signal
        flags.stretched_short = False
        flags.stretch_bar_idx = -1
        return (OrderSide.SELL, a)

    return None


# ============================================================
# Filters: spread/liquidity + feed health
# ============================================================

def spread_liquidity_ok(sym: str, q_tape: QuoteTape, now_ts: float) -> bool:
    if not REQUIRE_QUOTES:
        return True
    q, q_ts = q_tape.get(sym)
    if q is None or q_ts is None:
        return False
    if (now_ts - q_ts) > QUOTE_STALE_SECONDS:
        return False

    bid = q.bid
    ask = q.ask
    if bid <= 0 or ask <= 0 or ask < bid:
        return False

    spread = ask - bid
    mid = (ask + bid) / 2.0
    spread_bps = (spread / mid) * 10000.0

    if spread > MAX_SPREAD_ABS:
        return False
    if spread_bps > MAX_SPREAD_BPS:
        return False
    if q.bid_size_shares < MIN_BID_SIZE_SHARES:
        return False
    if q.ask_size_shares < MIN_ASK_SIZE_SHARES:
        return False

    return True

def feed_ok_for_entries(universe: List[str], q_tape: QuoteTape, ws_health: WSHealth) -> Tuple[bool, str]:
    connected, last_msg = ws_health.snapshot()
    if not connected:
        return False, "ws_disconnected"

    age = time.time() - last_msg if last_msg else 999.0
    if age > WS_STALE_SECONDS:
        return False, "ws_stale"

    if REQUIRE_QUOTES and universe:
        now_ts = time.time()
        fresh = 0
        checked = 0
        for s in universe[: min(20, len(universe))]:
            _, q_ts = q_tape.get(s)
            checked += 1
            if q_ts is not None and (now_ts - q_ts) <= QUOTE_STALE_SECONDS:
                fresh += 1
        if fresh < 3:
            return False, "quotes_insufficient"

    return True, "ok"

def critical_data_missing_for_positions(pos_syms: List[str], t_tape: TradeTape, q_tape: QuoteTape) -> bool:
    now_ts = time.time()
    for s in pos_syms:
        _, t_ts = t_tape.get(s)
        _, q_ts = q_tape.get(s)
        t_stale = (t_ts is None) or ((now_ts - t_ts) > CRITICAL_STALE_SECONDS)
        q_stale = (q_ts is None) or ((now_ts - q_ts) > CRITICAL_STALE_SECONDS)
        if t_stale and q_stale:
            return True
    return False


# ============================================================
# Order helpers
# ============================================================

def round_price(price: float) -> float:
    """Dynamic price rounding based on price level."""
    if price < 1.0:
        return round(price, 4)  # Penny stocks need more precision
    elif price < 10.0:
        return round(price, 3)
    elif price < 100.0:
        return round(price, 2)
    else:
        return round(price, 2)  # Standard 2 decimals for higher prices


def get_stop_distance(entry_px: float, r_atr: float, sl_multiplier: float) -> float:
    """
    Calculate stop distance with minimum enforcement.

    Ensures stops are at least MIN_STOP_DISTANCE_PCT% away from entry,
    or MIN_STOP_DISTANCE_ABS in absolute terms, whichever is larger.

    This prevents noise stop-outs when ATR is very small (common in extended hours
    or with low-volatility stocks).
    """
    # Standard ATR-based stop distance
    atr_stop_distance = sl_multiplier * r_atr

    # Calculate minimum distances
    min_pct_distance = entry_px * (MIN_STOP_DISTANCE_PCT / 100)
    min_abs_distance = MIN_STOP_DISTANCE_ABS

    # Use the largest of the three
    stop_distance = max(atr_stop_distance, min_pct_distance, min_abs_distance)

    return stop_distance


def submit_rth_bracket_with_price(sym: str, side: OrderSide, qty: int, entry_px: float, r_atr: float, q_tape: QuoteTape) -> str:
    """Submit RTH bracket order with aggressive limit pricing for better fill tracking."""

    # Get current quote for aggressive limit pricing (avoids market order slippage)
    try:
        q, _ = q_tape.get(sym)
        if q and q.bid > 0 and q.ask > 0 and q.ask >= q.bid:
            if side == OrderSide.BUY:
                limit_px = round_price(q.ask * 1.002)  # 0.2% above ask for aggressive fill
            else:
                limit_px = round_price(q.bid * 0.998)  # 0.2% below bid for aggressive fill
        else:
            # Fallback to entry_px with slippage buffer
            limit_px = round_price(entry_px * 1.002 if side == OrderSide.BUY else entry_px * 0.998)
    except Exception:
        # Fallback to entry_px with slippage buffer
        limit_px = round_price(entry_px * 1.002 if side == OrderSide.BUY else entry_px * 0.998)

    # Calculate stop/take-profit based on limit price for accurate R calculations
    # CRITICAL FIX: Use get_stop_distance to enforce minimum stop distance
    stop_dist = get_stop_distance(limit_px, r_atr, RTH_SL_R)
    tp_dist = RTH_TP_R * r_atr  # Keep TP based on ATR

    if side == OrderSide.BUY:
        sl = round_price(limit_px - stop_dist)
        tp = round_price(limit_px + tp_dist)
    else:
        sl = round_price(limit_px + stop_dist)
        tp = round_price(limit_px - tp_dist)

    payload = {
        "symbol": sym,
        "qty": str(qty),
        "side": side.value,
        "type": "limit",  # Changed from "market" to avoid slippage
        "limit_price": f"{limit_px:.6f}",  # Aggressive limit price
        "time_in_force": "day",
        "order_class": "bracket",
        "take_profit": {"limit_price": f"{tp:.6f}"},
        "stop_loss": {"stop_price": f"{sl:.6f}"},
    }
    o = alpaca_post("/v2/orders", payload)
    return o["id"]  # Return order ID for tracking

def submit_ext_entry_limit(sym: str, side: OrderSide, qty: int, limit_px: float) -> str:
    limit_px = round_price(limit_px)
    payload = {
        "symbol": sym,
        "qty": str(qty),
        "side": side.value,
        "type": "limit",
        "time_in_force": "day",
        "limit_price": f"{limit_px:.6f}",
        "extended_hours": True,
    }
    o = alpaca_post("/v2/orders", payload)
    return o["id"]


# ============================================================
# Persistence: state + virtual brackets + pending entries
# ============================================================

@dataclass
class VirtualBracketState:
    sym: str
    side: str         # "BUY" or "SELL"
    qty: int
    entry: float
    stop: float
    tp: float
    entry_time_iso: str
    r_atr: float
    moved_be: bool = False
    active: bool = True

@dataclass
class PendingEntry:
    order_id: str
    sym: str
    side: str        # "buy"/"sell"
    qty: int
    session: str     # PRE/RTH/AFTER
    r_atr: float
    entry_hint: float
    created_iso: str
    is_rth_bracket: bool = False  # Track RTH bracket orders for fill validation

@dataclass
class BotState:
    day_key: str = ""
    start_equity: float = 0.0
    halted_today: bool = False

    # Option B: keep old state compatible
    entries_paused_until_iso: str = ""

    cooldown_until: Dict[str, str] = None
    signal_flags: Dict[str, Dict[str, bool]] = None
    vbrackets: Dict[str, Dict[str, Any]] = None
    pending_entries: Dict[str, Dict[str, Any]] = None  # order_id -> dict(PendingEntry)

    def __post_init__(self):
        if self.cooldown_until is None:
            self.cooldown_until = {}
        if self.signal_flags is None:
            self.signal_flags = {}
        if self.vbrackets is None:
            self.vbrackets = {}
        if self.pending_entries is None:
            self.pending_entries = {}

class StateStore:
    def __init__(self, path: str):
        self.path = path
        self._lock = threading.Lock()
        self._dirty = False
        self._last_flush = 0.0
        self.state = BotState()

    def mark_dirty(self):
        with self._lock:
            self._dirty = True

    # Option B: schema-tolerant load
    def load(self) -> BotState:
        if not os.path.exists(self.path):
            return self.state
        with open(self.path, "r", encoding="utf-8") as f:
            raw = json.load(f)

        allowed = set(BotState.__dataclass_fields__.keys())
        cleaned = {k: v for k, v in raw.items() if k in allowed}

        self.state = BotState(**cleaned)
        return self.state

    def _atomic_write(self, data: dict):
        tmp = self.path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, sort_keys=True)

        if os.path.exists(self.path):
            for i in range(STATE_BACKUP_COUNT - 1, 0, -1):
                src = f"{self.path}.bak{i}"
                dst = f"{self.path}.bak{i+1}"
                if os.path.exists(src):
                    shutil.copy2(src, dst)
            shutil.copy2(self.path, f"{self.path}.bak1")

        os.replace(tmp, self.path)

    def flush_if_needed(self, force: bool = False):
        with self._lock:
            if not force:
                if not self._dirty:
                    return
                if time.time() - self._last_flush < STATE_FLUSH_SECONDS:
                    return
            self._dirty = False
            self._last_flush = time.time()
            data = asdict(self.state)
        self._atomic_write(data)

store = StateStore(STATE_PATH)


# ============================================================
# News cache
# ============================================================

class NewsCache:
    def __init__(self):
        self._lock = threading.Lock()
        self._cache: Dict[str, Tuple[float, bool]] = {}  # sym -> (ts, has_news)
        self._pending: set = set()  # Symbols currently being fetched
        self._fetch_queue: "queue.Queue[str]" = queue.Queue()
        self._worker_thread: Optional[threading.Thread] = None
        self._stop = threading.Event()

    def start_worker(self):
        """Start background thread for async news fetching."""
        if self._worker_thread is None or not self._worker_thread.is_alive():
            self._stop.clear()
            self._worker_thread = threading.Thread(target=self._worker, daemon=True)
            self._worker_thread.start()

    def stop_worker(self):
        """Stop background worker thread."""
        self._stop.set()

    def _worker(self):
        """Background worker that fetches news asynchronously."""
        while not self._stop.is_set():
            try:
                sym = self._fetch_queue.get(timeout=1.0)
            except queue.Empty:
                continue

            try:
                val = poly.has_recent_news(sym, SKIP_NEWS_MINUTES)
                with self._lock:
                    self._cache[sym] = (time.time(), bool(val))
                    self._pending.discard(sym)
            except Exception:
                with self._lock:
                    self._pending.discard(sym)

    def prefetch_batch(self, symbols: List[str]):
        """Queue multiple symbols for background fetching."""
        now_ts = time.time()
        to_fetch = []

        with self._lock:
            for sym in symbols:
                if sym in self._pending:
                    continue
                if sym in self._cache and (now_ts - self._cache[sym][0]) < NEWS_CACHE_SECONDS:
                    continue
                to_fetch.append(sym)
                self._pending.add(sym)

        for sym in to_fetch:
            try:
                self._fetch_queue.put_nowait(sym)
            except queue.Full:
                with self._lock:
                    self._pending.discard(sym)

    def has_recent_news(self, sym: str) -> Optional[bool]:
        """
        Check if symbol has recent news.
        Returns None if not cached (still fetching), True/False if cached.
        """
        now_ts = time.time()
        with self._lock:
            if sym in self._cache and (now_ts - self._cache[sym][0]) < NEWS_CACHE_SECONDS:
                return self._cache[sym][1]
            # Not cached, assume no news (conservative: allows trading)
            return False

news_cache = NewsCache()


# ============================================================
# Metrics & Performance Tracking
# ============================================================

@dataclass
class TradeMetrics:
    """Metrics for a completed trade."""
    sym: str
    side: str
    entry_time: str
    exit_time: str
    entry_price: float
    exit_price: float
    qty: int
    pnl_dollars: float
    pnl_r: float  # P&L in R-multiples
    hold_minutes: float
    exit_reason: str  # "TP", "SL", "TIME", "FLATTEN", "BE"
    session: str

class MetricsTracker:
    """Track and log trading performance metrics."""
    def __init__(self, log_path: str):
        self.log_path = log_path
        self._lock = threading.Lock()
        self.daily_metrics = {
            "trades": 0,
            "wins": 0,
            "losses": 0,
            "breakevens": 0,
            "total_pnl": 0.0,
            "total_r": 0.0,
            "signals_generated": 0,
            "signals_taken": 0,
            "orders_rejected": 0,
            "avg_hold_minutes": 0.0,
        }

    def log_trade(self, trade: TradeMetrics):
        """Log a completed trade and update metrics."""
        with self._lock:
            self.daily_metrics["trades"] += 1
            self.daily_metrics["total_pnl"] += trade.pnl_dollars
            self.daily_metrics["total_r"] += trade.pnl_r

            if trade.pnl_dollars > 0.01:
                self.daily_metrics["wins"] += 1
            elif trade.pnl_dollars < -0.01:
                self.daily_metrics["losses"] += 1
            else:
                self.daily_metrics["breakevens"] += 1

            # Update average hold time
            prev_avg = self.daily_metrics["avg_hold_minutes"]
            n = self.daily_metrics["trades"]
            self.daily_metrics["avg_hold_minutes"] = (prev_avg * (n - 1) + trade.hold_minutes) / n

            # Write to JSONL file
            try:
                with open(self.log_path, "a", encoding="utf-8") as f:
                    record = {
                        "timestamp": dt.datetime.now(tz=ET).isoformat(),
                        "trade": asdict(trade),
                        "daily_summary": self.get_summary(),
                    }
                    f.write(json.dumps(record) + "\n")
            except Exception:
                pass  # Don't crash bot if metrics logging fails

    def log_signal(self, taken: bool):
        """Log when a signal is generated."""
        with self._lock:
            self.daily_metrics["signals_generated"] += 1
            if taken:
                self.daily_metrics["signals_taken"] += 1

    def log_order_rejected(self):
        """Log when an order is rejected."""
        with self._lock:
            self.daily_metrics["orders_rejected"] += 1

    def get_summary(self) -> dict:
        """Get current metrics summary."""
        with self._lock:
            m = self.daily_metrics.copy()
            if m["trades"] > 0:
                m["win_rate"] = m["wins"] / m["trades"]
                m["avg_r_per_trade"] = m["total_r"] / m["trades"]
            else:
                m["win_rate"] = 0.0
                m["avg_r_per_trade"] = 0.0
            return m

    def reset_daily(self):
        """Reset daily metrics (called at day change)."""
        with self._lock:
            self.daily_metrics = {
                "trades": 0,
                "wins": 0,
                "losses": 0,
                "breakevens": 0,
                "total_pnl": 0.0,
                "total_r": 0.0,
                "signals_generated": 0,
                "signals_taken": 0,
                "orders_rejected": 0,
                "avg_hold_minutes": 0.0,
            }

metrics_tracker = MetricsTracker(METRICS_LOG_PATH) if ENABLE_METRICS else None


# ============================================================
# Universe builder
# ============================================================

class UniverseCache:
    """Cache for grouped daily data to avoid refetching throughout the day."""
    def __init__(self):
        self._lock = threading.Lock()
        self._cache: Optional[Tuple[str, List[dict]]] = None  # (date_key, data)

    def get_grouped_daily(self, date_key: str) -> Optional[List[dict]]:
        """Get cached grouped daily data for the given date."""
        with self._lock:
            if self._cache and self._cache[0] == date_key:
                return self._cache[1]
        return None

    def set_grouped_daily(self, date_key: str, data: List[dict]):
        """Cache grouped daily data for the given date."""
        with self._lock:
            self._cache = (date_key, data)

universe_cache = UniverseCache()

def previous_trading_date(now: dt.datetime) -> Optional[dt.date]:
    start = (now.date() - dt.timedelta(days=14)).isoformat()
    end = now.date().isoformat()
    cal = alpaca_get("/v2/calendar", params={"start": start, "end": end})
    days = [dt.date.fromisoformat(x["date"]) for x in cal] if cal else []
    days = sorted(days)
    prev = [d for d in days if d < now.date()]
    return prev[-1] if prev else None

def build_universe(now: dt.datetime) -> List[str]:
    syms = list(dict.fromkeys(CORE_SYMBOLS))
    if not EXPAND_UNIVERSE:
        return syms

    prev = previous_trading_date(now)
    if not prev:
        return syms

    # FIX: Use cached data to avoid refetching throughout the day
    date_key = prev.isoformat()
    rows = universe_cache.get_grouped_daily(date_key)
    if rows is None:
        rows = poly.grouped_daily(date_key)
        universe_cache.set_grouped_daily(date_key, rows)

    scored: List[Tuple[float, str]] = []
    for r in rows:
        sym = r.get("T") or r.get("sym") or r.get("ticker")
        if not sym:
            continue
        try:
            c = float(r.get("c", 0))
            v = float(r.get("v", 0))
        except Exception:
            continue
        if c <= 0 or v <= 0:
            continue
        if not (MIN_PRICE <= c <= MAX_PRICE):
            continue
        scored.append((c * v, sym))

    scored.sort(reverse=True)
    for _, s in scored[:EXPANDED_UNIVERSE_SIZE]:
        if s not in syms:
            syms.append(s)

    return syms


# ============================================================
# Virtual brackets management (EXT exits + break-even)
# ============================================================

def vbracket_time_stop_minutes(session: MarketSession) -> int:
    return MAX_HOLD_MINUTES_RTH if session == MarketSession.RTH else MAX_HOLD_MINUTES_EXT

def mid_from_quote_or_trade(sym: str, t_tape: TradeTape, q_tape: QuoteTape) -> Optional[float]:
    now_ts = time.time()
    last_trade, t_ts = t_tape.get(sym)
    q, q_ts = q_tape.get(sym)

    trade_stale = (t_ts is None) or ((now_ts - t_ts) > TRADE_STALE_SECONDS)
    quote_stale = (q_ts is None) or ((now_ts - q_ts) > QUOTE_STALE_SECONDS)

    if q and not quote_stale and q.bid > 0 and q.ask > 0 and q.ask >= q.bid:
        return (q.bid + q.ask) / 2.0
    if last_trade is not None and not trade_stale:
        return float(last_trade)
    return None

def manage_virtual_brackets(state: BotState, session: MarketSession, t_tape: TradeTape, q_tape: QuoteTape, log: StatusLogger):
    now = now_et()

    # Fetch broker positions ONCE per cycle (not per position!) - lesson from simple_bot
    try:
        broker_positions = alpaca_positions()
        pos_map = {p["symbol"]: float(p["qty"]) for p in broker_positions}
    except Exception as e:
        log.warning(f"[VBRACKET] Failed to fetch broker positions: {e}")
        return  # Skip this cycle if we can't reconcile state

    for sym, raw in list(state.vbrackets.items()):
        vb = VirtualBracketState(**raw)
        if not vb.active:
            continue

        # Reconcile: If broker has no position, mark bracket as inactive immediately
        live_qty = pos_map.get(sym, 0.0)
        if abs(live_qty) == 0:
            if vb.active:  # Only log if this is a state change
                log.info(f"[RECONCILE] {sym}: Broker closed position - marking bracket inactive")
            vb.active = False
            state.vbrackets[sym] = asdict(vb)
            state.cooldown_until[sym] = iso(now + dt.timedelta(minutes=COOLDOWN_AFTER_CLOSE_MIN))
            store.mark_dirty()
            continue

        # FIX: Reconcile position quantity mismatch
        expected_qty = vb.qty if vb.side == "BUY" else -vb.qty
        qty_mismatch = abs(live_qty - expected_qty)
        if qty_mismatch > 0:
            mismatch_pct = qty_mismatch / abs(expected_qty) if expected_qty != 0 else 1.0
            if mismatch_pct > QTY_MISMATCH_THRESHOLD_PCT:
                log.warning(f"[VBRACKET] {sym}: qty mismatch (expected={expected_qty} actual={live_qty}) -> adjusting or flattening")
                if abs(live_qty) < abs(expected_qty) * QTY_PARTIAL_FILL_FLATTEN_PCT:
                    log.warning(f"[VBRACKET] {sym}: significant partial fill -> flattening remainder")
                    try:
                        close_position(sym, logger=log, session=session, q_tape=q_tape)
                    except Exception as e:
                        log.warning(f"[VBRACKET] {sym}: Error closing position (may already be closed): {e}")
                    # Mark inactive regardless of whether close succeeded
                    vb.active = False
                    state.vbrackets[sym] = asdict(vb)
                    store.mark_dirty()
                    continue
                else:
                    # Adjust virtual bracket qty to match reality
                    vb.qty = int(abs(live_qty))
                    state.vbrackets[sym] = asdict(vb)
                    store.mark_dirty()
                    log.info(f"[VBRACKET] {sym}: adjusted qty to {vb.qty}")

        px = mid_from_quote_or_trade(sym, t_tape, q_tape)
        if px is None:
            if ON_CRITICAL_DATA_MISSING_FLATTEN:
                now_ts = time.time()
                _, t_ts = t_tape.get(sym)
                _, q_ts = q_tape.get(sym)
                t_crit = (t_ts is None) or ((now_ts - t_ts) > CRITICAL_STALE_SECONDS)
                q_crit = (q_ts is None) or ((now_ts - q_ts) > CRITICAL_STALE_SECONDS)
                if t_crit and q_crit:
                    log.event(f"[VBRACKET] {sym}: critical stale data -> flattening.")
                    try:
                        close_position(sym, logger=log, session=session, q_tape=q_tape)
                    except Exception as e:
                        log.warning(f"[VBRACKET] {sym}: Error closing position (may already be closed): {e}")
                    # Mark inactive regardless of whether close succeeded
                    vb.active = False
                    state.vbrackets[sym] = asdict(vb)
                    store.mark_dirty()
            continue

        try:
            entry_time = from_iso(vb.entry_time_iso)
        except Exception:
            entry_time = now
        if (now - entry_time).total_seconds() >= (vbracket_time_stop_minutes(session) * 60):
            log.event(f"[TIME-STOP] {sym}: held too long -> flattening.")
            try:
                close_position(sym, logger=log, session=session, q_tape=q_tape)
            except Exception as e:
                log.warning(f"[VBRACKET] {sym}: Error closing position (may already be closed): {e}")
            # Mark inactive regardless of whether close succeeded
            vb.active = False
            state.vbrackets[sym] = asdict(vb)
            store.mark_dirty()
            continue

        if ENABLE_BREAK_EVEN and (not vb.moved_be) and vb.r_atr > 0:
            if vb.side == "BUY":
                if px >= vb.entry + (BREAK_EVEN_TRIGGER_R * vb.r_atr):
                    vb.stop = round_price(vb.entry + BREAK_EVEN_OFFSET_DOLLARS)
                    vb.moved_be = True
                    log.event(f"[BE] {sym}: moved stop -> {vb.stop:.4f}")
            else:
                if px <= vb.entry - (BREAK_EVEN_TRIGGER_R * vb.r_atr):
                    vb.stop = round_price(vb.entry - BREAK_EVEN_OFFSET_DOLLARS)
                    vb.moved_be = True
                    log.event(f"[BE] {sym}: moved stop -> {vb.stop:.4f}")

        # FIX: Ensure position is closed immediately and state updated atomically
        should_close = False
        close_reason = ""

        if vb.side == "BUY":
            if px <= vb.stop:
                should_close = True
                close_reason = f"[VSTOP] {sym}: px={px:.2f} stop={vb.stop:.2f}"
            elif px >= vb.tp:
                should_close = True
                close_reason = f"[VTP] {sym}: px={px:.2f} tp={vb.tp:.2f}"
        else:
            if px >= vb.stop:
                should_close = True
                close_reason = f"[VSTOP] {sym}: px={px:.2f} stop={vb.stop:.2f}"
            elif px <= vb.tp:
                should_close = True
                close_reason = f"[VTP] {sym}: px={px:.2f} tp={vb.tp:.2f}"

        if should_close:
            log.info(close_reason)
            try:
                close_position(sym, logger=log, session=session, q_tape=q_tape)
            except Exception as e:
                log.warning(f"[VBRACKET] {sym}: Error closing position (may already be closed): {e}")

            # Mark inactive and record metrics regardless of whether close succeeded
            vb.active = False
            state.vbrackets[sym] = asdict(vb)
            state.cooldown_until[sym] = iso(now + dt.timedelta(minutes=COOLDOWN_AFTER_CLOSE_MIN))
            store.mark_dirty()

            # Log metrics for completed trade
            if metrics_tracker:
                try:
                    entry_time = from_iso(vb.entry_time_iso)
                    hold_minutes = (now - entry_time).total_seconds() / 60.0

                    # Calculate P&L
                    if vb.side == "BUY":
                        pnl_dollars = (px - vb.entry) * vb.qty
                    else:
                        pnl_dollars = (vb.entry - px) * vb.qty

                    pnl_r = pnl_dollars / (vb.r_atr * vb.qty) if vb.r_atr > 0 else 0.0

                    # Determine exit reason
                    if "VSTOP" in close_reason:
                        exit_reason = "BE" if vb.moved_be else "SL"
                    elif "VTP" in close_reason:
                        exit_reason = "TP"
                    elif "TIME" in close_reason:
                        exit_reason = "TIME"
                    else:
                        exit_reason = "FLATTEN"

                    trade = TradeMetrics(
                        sym=sym,
                        side=vb.side,
                        entry_time=vb.entry_time_iso,
                        exit_time=iso(now),
                        entry_price=vb.entry,
                        exit_price=px,
                        qty=vb.qty,
                        pnl_dollars=pnl_dollars,
                        pnl_r=pnl_r,
                        hold_minutes=hold_minutes,
                        exit_reason=exit_reason,
                        session=session.value,
                    )
                    metrics_tracker.log_trade(trade)
                except Exception as e:
                    log.debug(f"[METRICS] Failed to log trade for {sym}: {e}")
        else:
            # Only update state if bracket params changed (e.g., break-even move)
            state.vbrackets[sym] = asdict(vb)
            store.mark_dirty()


# ============================================================
# Reconcile: reconstruct EXT virtual bracket if missing
# ============================================================

def reconstruct_ext_vbracket(sym: str, qty_float: float, state: BotState, log: StatusLogger, session: MarketSession = None, q_tape: 'QuoteTape' = None):
    qty = int(abs(qty_float))
    if qty <= 0:
        return
    if sym in state.vbrackets:
        return

    side = "BUY" if qty_float > 0 else "SELL"

    pos_list = alpaca_positions()
    pos = next((p for p in pos_list if p.get("symbol") == sym), None)
    avg_entry = float(pos.get("avg_entry_price")) if pos and pos.get("avg_entry_price") else None

    try:
        end = now_et()
        start = end - dt.timedelta(minutes=LOOKBACK_MINUTES + 10)
        df = poly.agg_1m(sym, start.date().isoformat(), end.date().isoformat(), limit=5000)
        min_bars_needed = ATR_LEN + 10
        if df.empty or len(df) < min_bars_needed:
            raise RuntimeError(f"Not enough bars: got {len(df)}, need {min_bars_needed}")
        df["atr"] = atr(df, ATR_LEN)
        # FIX: Validate we have valid ATR value
        valid_atr = df["atr"].notna().sum()
        if valid_atr < ATR_LEN:
            raise RuntimeError(f"Not enough valid ATR values: got {valid_atr}, need {ATR_LEN}")
        r_atr = float(df["atr"].iloc[-1])
        if pd.isna(r_atr) or r_atr <= 0:
            raise RuntimeError(f"Invalid ATR value: {r_atr}")
    except Exception:
        r_atr = None

    if avg_entry is None or r_atr is None or r_atr <= 0:
        if RECONCILE_IF_POSITION_NO_STATE == "FLATTEN" or ON_CRITICAL_DATA_MISSING_FLATTEN:
            log.event(f"[RECONCILE] {sym}: cannot reconstruct safely -> flattening.")
            close_position(sym, logger=log, session=session, q_tape=q_tape)
        return

    entry = avg_entry
    # CRITICAL FIX: Use get_stop_distance to enforce minimum stop distance
    stop_dist = get_stop_distance(entry, r_atr, EXT_SL_R)
    tp_dist = EXT_TP_R * r_atr

    if side == "BUY":
        sl = round_price(entry - stop_dist)
        tp = round_price(entry + tp_dist)
    else:
        sl = round_price(entry + stop_dist)
        tp = round_price(entry - tp_dist)

    vb = VirtualBracketState(
        sym=sym,
        side=side,
        qty=qty,
        entry=entry,
        stop=sl,
        tp=tp,
        entry_time_iso=iso(now_et()),
        r_atr=r_atr,
        moved_be=False,
        active=True,
    )
    state.vbrackets[sym] = asdict(vb)
    store.mark_dirty()
    log.event(f"[RECONCILE] {sym}: reconstructed VB entry={entry:.2f} stop={sl:.4f} tp={tp:.4f} qty={qty}")


# ============================================================
# Exposure Manager (Phase 1 Safety)
# ============================================================

class ExposureManager:
    """
    Manages position exposure limits to prevent runaway risk.

    Enforces:
    - Max total exposure as % of equity
    - Max exposure per individual symbol
    - Max correlated positions in same sector/cluster
    """

    def __init__(self, log: StatusLogger):
        self.log = log
        # Build reverse lookup: symbol -> cluster name
        self._sym_to_cluster: Dict[str, str] = {}
        for cluster_name, symbols in CORRELATION_CLUSTERS.items():
            for sym in symbols:
                self._sym_to_cluster[sym] = cluster_name

    def get_cluster(self, sym: str) -> Optional[str]:
        """Get the cluster/sector a symbol belongs to."""
        return self._sym_to_cluster.get(sym)

    def calculate_current_exposure(self, positions: List[dict], equity: float) -> Dict[str, Any]:
        """
        Calculate current exposure metrics.

        Returns dict with:
        - total_exposure_usd: Total $ exposed across all positions
        - exposure_pct: Total exposure as % of equity
        - per_symbol: Dict of symbol -> exposure_usd
        - per_cluster: Dict of cluster -> (count, total_usd)
        """
        result = {
            "total_exposure_usd": 0.0,
            "exposure_pct": 0.0,
            "per_symbol": {},
            "per_cluster": {},
        }

        for pos in positions:
            sym = pos.get("symbol", "")
            qty = abs(float(pos.get("qty", 0)))
            market_value = abs(float(pos.get("market_value", 0)))

            if qty <= 0:
                continue

            result["total_exposure_usd"] += market_value
            result["per_symbol"][sym] = market_value

            cluster = self.get_cluster(sym)
            if cluster:
                if cluster not in result["per_cluster"]:
                    result["per_cluster"][cluster] = {"count": 0, "total_usd": 0.0, "symbols": []}
                result["per_cluster"][cluster]["count"] += 1
                result["per_cluster"][cluster]["total_usd"] += market_value
                result["per_cluster"][cluster]["symbols"].append(sym)

        if equity > 0:
            result["exposure_pct"] = result["total_exposure_usd"] / equity

        return result

    def can_open_position(
        self,
        sym: str,
        proposed_notional: float,
        positions: List[dict],
        equity: float,
        pending_entries: Dict[str, Dict[str, Any]]
    ) -> Tuple[bool, str]:
        """
        Check if opening a new position is allowed under exposure limits.

        Args:
            sym: Symbol to open
            proposed_notional: Proposed position size in USD
            positions: Current broker positions
            equity: Current account equity
            pending_entries: Pending entry orders (not yet filled)

        Returns:
            (allowed: bool, reason: str)
        """
        if equity <= 0:
            return False, "invalid_equity"

        # Include pending entries in exposure calculation
        pending_notional = 0.0
        pending_symbols = set()
        for pe_raw in pending_entries.values():
            pe_sym = pe_raw.get("sym", "")
            pe_qty = pe_raw.get("qty", 0)
            pe_entry_hint = pe_raw.get("entry_hint", 0.0)
            pending_notional += pe_qty * pe_entry_hint
            pending_symbols.add(pe_sym)

        # Calculate current exposure
        exposure = self.calculate_current_exposure(positions, equity)

        # === Check 1: Max exposure per symbol ===
        if proposed_notional > MAX_EXPOSURE_PER_SYMBOL_USD:
            self.log.event(f"[EXPOSURE] {sym}: blocked - proposed ${proposed_notional:.0f} > max ${MAX_EXPOSURE_PER_SYMBOL_USD:.0f}")
            return False, f"exceeds_per_symbol_limit_${MAX_EXPOSURE_PER_SYMBOL_USD:.0f}"

        # === Check 2: Max total exposure ===
        projected_total = exposure["total_exposure_usd"] + pending_notional + proposed_notional
        max_total_exposure = equity * MAX_EXPOSURE_PCT_OF_EQUITY

        if projected_total > max_total_exposure:
            self.log.event(
                f"[EXPOSURE] {sym}: blocked - projected ${projected_total:.0f} > "
                f"max {MAX_EXPOSURE_PCT_OF_EQUITY*100:.0f}% of equity (${max_total_exposure:.0f})"
            )
            return False, f"exceeds_total_exposure_{MAX_EXPOSURE_PCT_OF_EQUITY*100:.0f}%"

        # === Check 3: Max correlated positions ===
        cluster = self.get_cluster(sym)
        if cluster:
            cluster_info = exposure["per_cluster"].get(cluster, {"count": 0, "symbols": []})
            current_count = cluster_info["count"]

            # Count pending entries in same cluster
            for pe_sym in pending_symbols:
                if self.get_cluster(pe_sym) == cluster:
                    current_count += 1

            if current_count >= MAX_CORRELATED_POSITIONS:
                self.log.event(
                    f"[EXPOSURE] {sym}: blocked - {cluster} cluster already has "
                    f"{current_count} positions (max={MAX_CORRELATED_POSITIONS})"
                )
                return False, f"exceeds_cluster_limit_{cluster}"

        return True, "ok"

    def adjust_qty_for_limits(
        self,
        sym: str,
        desired_qty: int,
        px: float,
        positions: List[dict],
        equity: float,
        pending_entries: Dict[str, Dict[str, Any]]
    ) -> int:
        """
        Adjust quantity down if needed to fit within exposure limits.

        Returns the maximum allowed quantity (may be 0 if blocked entirely).
        """
        if desired_qty <= 0 or px <= 0:
            return 0

        desired_notional = desired_qty * px

        # Check if desired size is allowed
        allowed, reason = self.can_open_position(sym, desired_notional, positions, equity, pending_entries)
        if allowed:
            return desired_qty

        # Try to find a reduced size that works
        # First, cap at per-symbol max
        max_qty_per_symbol = int(MAX_EXPOSURE_PER_SYMBOL_USD / px)

        # Then, check total exposure headroom
        exposure = self.calculate_current_exposure(positions, equity)
        pending_notional = sum(
            pe.get("qty", 0) * pe.get("entry_hint", 0.0)
            for pe in pending_entries.values()
        )
        current_total = exposure["total_exposure_usd"] + pending_notional
        max_total = equity * MAX_EXPOSURE_PCT_OF_EQUITY
        headroom = max(0, max_total - current_total)
        max_qty_total = int(headroom / px) if px > 0 else 0

        # Take the minimum
        adjusted_qty = min(desired_qty, max_qty_per_symbol, max_qty_total)

        if adjusted_qty < desired_qty and adjusted_qty > 0:
            self.log.event(
                f"[EXPOSURE] {sym}: reduced qty from {desired_qty} to {adjusted_qty} "
                f"(notional ${adjusted_qty * px:.0f})"
            )

        return max(0, adjusted_qty)


# ============================================================
# Idempotency Manager (Phase 1 Safety)
# ============================================================

class IdempotencyManager:
    """
    Prevents duplicate actions in fast-moving markets.

    State machine rules:
    - Only one entry attempt per symbol at a time
    - Don't submit exit if one is already working
    - Track in-flight operations with timestamps for cleanup
    """

    def __init__(self, log: StatusLogger):
        self.log = log
        self._lock = threading.Lock()

        # Track in-flight operations: symbol -> {"type": "entry"|"exit", "started": timestamp, "order_id": str}
        self._in_flight: Dict[str, Dict[str, Any]] = {}

        # Stale operation timeout (seconds) - if an operation is older than this, assume it failed
        self._stale_timeout = 120.0

    def can_submit_entry(self, sym: str) -> Tuple[bool, str]:
        """Check if we can submit an entry order for this symbol."""
        with self._lock:
            self._cleanup_stale()

            if sym in self._in_flight:
                op = self._in_flight[sym]
                op_type = op.get("type", "unknown")
                age = time.time() - op.get("started", 0)
                return False, f"in_flight_{op_type}_age_{age:.0f}s"

            return True, "ok"

    def can_submit_exit(self, sym: str) -> Tuple[bool, str]:
        """Check if we can submit an exit order for this symbol."""
        with self._lock:
            self._cleanup_stale()

            if sym in self._in_flight:
                op = self._in_flight[sym]
                if op.get("type") == "exit":
                    age = time.time() - op.get("started", 0)
                    return False, f"exit_already_in_flight_age_{age:.0f}s"

            return True, "ok"

    def mark_entry_started(self, sym: str, order_id: str):
        """Mark that an entry order has been submitted."""
        with self._lock:
            self._in_flight[sym] = {
                "type": "entry",
                "started": time.time(),
                "order_id": order_id,
            }

    def mark_exit_started(self, sym: str, order_id: Optional[str] = None):
        """Mark that an exit order has been submitted."""
        with self._lock:
            self._in_flight[sym] = {
                "type": "exit",
                "started": time.time(),
                "order_id": order_id or "",
            }

    def mark_completed(self, sym: str):
        """Mark that an operation has completed (filled, cancelled, etc)."""
        with self._lock:
            self._in_flight.pop(sym, None)

    def _cleanup_stale(self):
        """Remove operations that are older than the stale timeout."""
        now = time.time()
        stale = [
            sym for sym, op in self._in_flight.items()
            if (now - op.get("started", 0)) > self._stale_timeout
        ]
        for sym in stale:
            self.log.warning(f"[IDEMPOTENCY] {sym}: cleaning up stale operation {self._in_flight[sym]}")
            del self._in_flight[sym]

    def get_in_flight(self) -> Dict[str, Dict[str, Any]]:
        """Get current in-flight operations (for debugging/logging)."""
        with self._lock:
            return dict(self._in_flight)


# ============================================================
# Volatility Regime Manager (Phase 2)
# ============================================================

class VolatilityRegimeManager:
    """
    Manages volatility-based regime detection and adaptive sizing.

    Tracks VIX to determine:
    - LOW: VIX < 15 (bullish, low vol environment)
    - NORMAL: VIX 15-25 (typical conditions)
    - HIGH: VIX 25-35 (elevated volatility, reduce risk)
    - EXTREME: VIX > 35 (risk-off mode, halt entries)
    """

    def __init__(self, log: StatusLogger):
        self.log = log
        self._lock = threading.Lock()

        # Current regime state
        self._regime: str = "NORMAL"
        self._vix_value: float = 20.0  # Default assumption
        self._last_vix_fetch: float = 0.0

        # Risk-off state
        self._risk_off_until: Optional[dt.datetime] = None

        # Session tracking for intraday VIX
        self._session_vix_high: float = 0.0
        self._session_vix_low: float = 999.0
        self._last_session_date: Optional[dt.date] = None

    @property
    def regime(self) -> str:
        """Current volatility regime."""
        with self._lock:
            return self._regime

    @property
    def vix_value(self) -> float:
        """Latest VIX reading."""
        with self._lock:
            return self._vix_value

    def is_risk_off(self) -> bool:
        """Check if we're in risk-off mode."""
        with self._lock:
            if self._risk_off_until is None:
                return False
            return now_et() < self._risk_off_until

    def get_position_size_multiplier(self) -> float:
        """Get position sizing multiplier based on current regime."""
        with self._lock:
            return VOL_REGIME_MULTIPLIERS.get(self._regime, 1.0)

    def get_atr_multipliers(self) -> Dict[str, float]:
        """Get ATR multipliers for current regime."""
        with self._lock:
            return ATR_MULTIPLIER_BY_REGIME.get(self._regime, ATR_MULTIPLIER_BY_REGIME["NORMAL"])

    def update_vix(self, vix_price: float):
        """Update VIX reading and recalculate regime."""
        with self._lock:
            now = time.time()
            self._vix_value = vix_price
            self._last_vix_fetch = now

            # Track session high/low
            today = now_et().date()
            if self._last_session_date != today:
                self._session_vix_high = vix_price
                self._session_vix_low = vix_price
                self._last_session_date = today
            else:
                self._session_vix_high = max(self._session_vix_high, vix_price)
                self._session_vix_low = min(self._session_vix_low, vix_price)

            # Determine regime
            old_regime = self._regime
            if vix_price >= VIX_EXTREME_THRESHOLD:
                self._regime = "EXTREME"
            elif vix_price >= VIX_HIGH_THRESHOLD:
                self._regime = "HIGH"
            elif vix_price < VIX_LOW_THRESHOLD:
                self._regime = "LOW"
            else:
                self._regime = "NORMAL"

            # Enter risk-off mode if EXTREME
            if self._regime == "EXTREME" and old_regime != "EXTREME":
                self._risk_off_until = now_et() + dt.timedelta(minutes=RISK_OFF_COOLDOWN_MINUTES)
                self.log.warning(
                    f"[VOLATILITY] RISK-OFF MODE ACTIVATED - VIX={vix_price:.2f} >= {VIX_EXTREME_THRESHOLD} "
                    f"(no entries until {self._risk_off_until.strftime('%H:%M')} ET)"
                )

            # Log regime changes
            if old_regime != self._regime:
                self.log.event(
                    f"[VOLATILITY] Regime change: {old_regime} -> {self._regime} "
                    f"(VIX={vix_price:.2f}, session range {self._session_vix_low:.2f}-{self._session_vix_high:.2f})"
                )

    def fetch_vix_from_polygon(self) -> Optional[float]:
        """Fetch current VIX from Polygon API, with SPY-based proxy fallback."""
        # Check cache
        if time.time() - self._last_vix_fetch < VIX_CACHE_MINUTES * 60:
            return self._vix_value

        # First try direct VIX API (may not be available on all Polygon plans)
        try:
            url = f"{POLYGON_REST_BASE}/v2/last/trade/I:{VIX_SYMBOL}"
            params = {"apiKey": POLYGON_API_KEY}
            resp = requests.get(url, params=params, timeout=10)
            resp.raise_for_status()
            data = resp.json()

            if data.get("status") == "OK" and "results" in data:
                price = float(data["results"].get("p", 0))
                if price > 0:
                    self.update_vix(price)
                    return price

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 403:
                # VIX index data not available on this plan, use SPY proxy
                return self._fetch_vix_from_spy_proxy()
            self.log.warning(f"[VOLATILITY] Failed to fetch VIX: {e}")
        except Exception as e:
            self.log.warning(f"[VOLATILITY] Failed to fetch VIX: {e}")

        # Fallback to SPY proxy
        return self._fetch_vix_from_spy_proxy()

    def _fetch_vix_from_spy_proxy(self) -> Optional[float]:
        """
        Calculate VIX proxy from SPY intraday volatility.

        Uses Parkinson volatility (high-low range) which is faster to react
        than close-to-close volatility and doesn't require index data access.
        """
        try:
            # Fetch recent SPY daily bars (last 10 days)
            end_date = now_et().strftime("%Y-%m-%d")
            start_date = (now_et() - dt.timedelta(days=15)).strftime("%Y-%m-%d")

            url = f"{POLYGON_REST_BASE}/v2/aggs/ticker/SPY/range/1/day/{start_date}/{end_date}"
            params = {"apiKey": POLYGON_API_KEY, "adjusted": "true", "sort": "asc", "limit": 15}
            resp = requests.get(url, params=params, timeout=10)
            resp.raise_for_status()
            data = resp.json()

            results = data.get("results", [])
            if len(results) < 5:
                self.log.debug("[VOLATILITY] Insufficient SPY data for VIX proxy")
                return None

            # Calculate Parkinson volatility (high-low range based)
            # Formula: sqrt(1/(4*ln(2)) * sum(ln(H/L)^2)) * sqrt(252) * 100
            log_hl_squared = []
            for bar in results[-5:]:  # Use last 5 days
                high = bar.get("h", 0)
                low = bar.get("l", 0)
                if high > 0 and low > 0 and high > low:
                    log_hl = math.log(high / low) ** 2
                    log_hl_squared.append(log_hl)

            if len(log_hl_squared) < 3:
                return None

            avg_log_hl_sq = sum(log_hl_squared) / len(log_hl_squared)
            parkinson_vol = math.sqrt(avg_log_hl_sq / (4 * math.log(2))) * math.sqrt(252) * 100

            # Clip to reasonable VIX range
            vix_proxy = max(10.0, min(50.0, parkinson_vol))

            self.update_vix(vix_proxy)
            self.log.debug(f"[VOLATILITY] VIX proxy from SPY: {vix_proxy:.1f}")
            return vix_proxy

        except Exception as e:
            self.log.debug(f"[VOLATILITY] Failed to calculate VIX proxy: {e}")
            return None

    def should_allow_entry(self) -> Tuple[bool, str]:
        """Check if entries are allowed based on volatility regime."""
        with self._lock:
            if self._regime == "EXTREME":
                return False, f"extreme_volatility_VIX_{self._vix_value:.1f}"

            if self._risk_off_until and now_et() < self._risk_off_until:
                remaining = (self._risk_off_until - now_et()).total_seconds() / 60
                return False, f"risk_off_mode_{remaining:.0f}min_remaining"

            return True, "ok"

    def get_status(self) -> Dict[str, Any]:
        """Get current volatility status for logging/display."""
        with self._lock:
            return {
                "regime": self._regime,
                "vix": self._vix_value,
                "size_multiplier": VOL_REGIME_MULTIPLIERS.get(self._regime, 1.0),
                "risk_off": self._risk_off_until.isoformat() if self._risk_off_until else None,
                "session_vix_high": self._session_vix_high,
                "session_vix_low": self._session_vix_low,
            }


# Global instances (initialized in main())
exposure_manager: Optional[ExposureManager] = None
idempotency_manager: Optional[IdempotencyManager] = None
volatility_manager: Optional[VolatilityRegimeManager] = None


# ============================================================
# API Health Monitor (Phase 3)
# ============================================================

class APIHealthMonitor:
    """
    Tracks API health metrics for reliability monitoring.

    Monitors:
    - Request latency (avg, p95, max)
    - Success/failure rates
    - Error patterns and types
    """

    def __init__(self, log: StatusLogger, alerter: Optional['Alerter'] = None):
        self.log = log
        self.alerter = alerter
        self._lock = threading.Lock()

        # Metrics storage (rolling window)
        self._requests: List[Dict[str, Any]] = []
        self._last_alert_time: float = 0.0

    def record_request(self, api_name: str, latency_ms: float, success: bool, error_type: Optional[str] = None):
        """Record an API request for health tracking."""
        with self._lock:
            now = time.time()
            self._requests.append({
                "ts": now,
                "api": api_name,
                "latency_ms": latency_ms,
                "success": success,
                "error_type": error_type,
            })

            # Prune old entries
            cutoff = now - API_HEALTH_WINDOW_SECONDS
            self._requests = [r for r in self._requests if r["ts"] >= cutoff]

            # Check for alerts
            self._check_health_alerts(api_name, latency_ms, success)

    def _check_health_alerts(self, api_name: str, latency_ms: float, success: bool):
        """Check if health thresholds are breached and send alerts."""
        now = time.time()

        # Don't spam alerts
        if (now - self._last_alert_time) < 60:
            return

        # Latency alert
        if latency_ms >= API_LATENCY_CRITICAL_MS:
            self._last_alert_time = now
            self.log.error(f"[API-HEALTH] CRITICAL: {api_name} latency {latency_ms:.0f}ms")
            if self.alerter:
                self.alerter.send_alert("CRITICAL", "API Latency Critical",
                    f"{api_name} response time {latency_ms:.0f}ms", {"api": api_name, "latency_ms": latency_ms})
        elif latency_ms >= API_LATENCY_WARN_MS:
            self.log.warning(f"[API-HEALTH] WARNING: {api_name} latency {latency_ms:.0f}ms")

        # Success rate alert
        if len(self._requests) >= 10:
            success_count = sum(1 for r in self._requests if r["success"])
            success_rate = success_count / len(self._requests)

            if success_rate < API_SUCCESS_RATE_CRITICAL:
                self._last_alert_time = now
                self.log.error(f"[API-HEALTH] CRITICAL: success rate {success_rate*100:.1f}%")
                if self.alerter:
                    self.alerter.send_alert("CRITICAL", "API Success Rate Critical",
                        f"Only {success_rate*100:.1f}% of requests succeeding",
                        {"success_rate": success_rate, "window_requests": len(self._requests)})
            elif success_rate < API_SUCCESS_RATE_WARN:
                self.log.warning(f"[API-HEALTH] WARNING: success rate {success_rate*100:.1f}%")

    def get_metrics(self) -> Dict[str, Any]:
        """Get current health metrics."""
        with self._lock:
            if not self._requests:
                return {"requests": 0, "success_rate": 1.0, "avg_latency_ms": 0, "p95_latency_ms": 0}

            latencies = [r["latency_ms"] for r in self._requests]
            success_count = sum(1 for r in self._requests if r["success"])

            latencies_sorted = sorted(latencies)
            p95_idx = int(len(latencies_sorted) * 0.95)

            return {
                "requests": len(self._requests),
                "success_rate": success_count / len(self._requests),
                "avg_latency_ms": sum(latencies) / len(latencies),
                "p95_latency_ms": latencies_sorted[p95_idx] if latencies_sorted else 0,
                "max_latency_ms": max(latencies) if latencies else 0,
                "window_seconds": API_HEALTH_WINDOW_SECONDS,
            }


# ============================================================
# Watchdog Monitor (Phase 3)
# ============================================================

class WatchdogMonitor:
    """
    Monitors bot health and detects hangs/freezes.

    Features:
    - Heartbeat file updates
    - Loop iteration timing
    - Stale detection
    """

    def __init__(self, log: StatusLogger, alerter: Optional['Alerter'] = None):
        self.log = log
        self.alerter = alerter
        self._lock = threading.Lock()

        self._last_heartbeat: float = time.time()
        self._loop_start_time: Optional[float] = None
        self._slow_loop_count: int = 0
        self._heartbeat_file = WATCHDOG_HEARTBEAT_FILE

    def heartbeat(self):
        """Update heartbeat timestamp and file."""
        with self._lock:
            now = time.time()
            self._last_heartbeat = now

            if WATCHDOG_ENABLED:
                try:
                    with open(self._heartbeat_file, "w") as f:
                        f.write(f"{now}\n{now_et().isoformat()}\n")
                except Exception as e:
                    self.log.warning(f"[WATCHDOG] Failed to write heartbeat file: {e}")

    def start_loop_iteration(self):
        """Mark the start of a main loop iteration."""
        with self._lock:
            self._loop_start_time = time.time()

    def end_loop_iteration(self) -> float:
        """
        Mark the end of a main loop iteration.
        Returns the loop duration in seconds.
        """
        with self._lock:
            if self._loop_start_time is None:
                return 0.0

            duration = time.time() - self._loop_start_time
            self._loop_start_time = None

            if duration > WATCHDOG_MAX_LOOP_SECONDS:
                self._slow_loop_count += 1
                self.log.warning(f"[WATCHDOG] Slow loop iteration: {duration:.1f}s (count={self._slow_loop_count})")

                if self._slow_loop_count >= 3 and self.alerter:
                    self.alerter.send_alert("WARNING", "Bot Loop Slowdown",
                        f"Main loop taking {duration:.1f}s (threshold: {WATCHDOG_MAX_LOOP_SECONDS}s)",
                        {"duration_s": duration, "slow_count": self._slow_loop_count})
                    self._slow_loop_count = 0  # Reset after alert

            return duration

    def is_healthy(self) -> Tuple[bool, str]:
        """Check if the bot is healthy based on heartbeat."""
        with self._lock:
            age = time.time() - self._last_heartbeat
            if age > WATCHDOG_STALE_HEARTBEAT_SEC:
                return False, f"stale_heartbeat_{age:.0f}s"
            return True, "ok"

    def get_status(self) -> Dict[str, Any]:
        """Get watchdog status."""
        with self._lock:
            return {
                "last_heartbeat": self._last_heartbeat,
                "heartbeat_age_s": time.time() - self._last_heartbeat,
                "slow_loop_count": self._slow_loop_count,
                "healthy": (time.time() - self._last_heartbeat) <= WATCHDOG_STALE_HEARTBEAT_SEC,
            }


# ============================================================
# Degraded Mode Manager (Phase 3)
# ============================================================

class DegradedModeManager:
    """
    Manages graceful degradation when partial failures occur.

    Modes:
    - NORMAL: Full functionality
    - DEGRADED: Limited functionality (manage positions, no new entries)
    - CRITICAL: Emergency mode (flatten and halt)
    """

    def __init__(self, log: StatusLogger, alerter: Optional['Alerter'] = None):
        self.log = log
        self.alerter = alerter
        self._lock = threading.Lock()

        self._mode: str = "NORMAL"
        self._degraded_until: Optional[dt.datetime] = None
        self._degraded_reasons: List[str] = []
        self._data_gaps: Dict[str, dt.datetime] = {}  # symbol -> last good data time

    @property
    def mode(self) -> str:
        with self._lock:
            return self._mode

    def enter_degraded_mode(self, reason: str, duration_minutes: int = 15):
        """Enter degraded mode for specified duration."""
        with self._lock:
            if self._mode == "NORMAL":
                self.log.warning(f"[DEGRADED] Entering degraded mode: {reason}")
                if self.alerter:
                    self.alerter.send_alert("WARNING", "Degraded Mode Active",
                        f"Bot operating in degraded mode: {reason}",
                        {"reason": reason, "duration_min": duration_minutes})

            self._mode = "DEGRADED"
            self._degraded_until = now_et() + dt.timedelta(minutes=duration_minutes)
            if reason not in self._degraded_reasons:
                self._degraded_reasons.append(reason)

    def exit_degraded_mode(self):
        """Exit degraded mode and return to normal."""
        with self._lock:
            if self._mode != "NORMAL":
                self.log.info(f"[DEGRADED] Exiting degraded mode, returning to normal")
                self._mode = "NORMAL"
                self._degraded_until = None
                self._degraded_reasons = []

    def check_auto_recovery(self):
        """Check if we should auto-recover from degraded mode."""
        with self._lock:
            if self._mode == "DEGRADED" and self._degraded_until:
                if now_et() >= self._degraded_until:
                    self.log.info("[DEGRADED] Auto-recovering from degraded mode")
                    self._mode = "NORMAL"
                    self._degraded_until = None
                    self._degraded_reasons = []

    def record_data_gap(self, sym: str, last_good_time: dt.datetime):
        """Record a data gap for a symbol."""
        with self._lock:
            self._data_gaps[sym] = last_good_time

    def clear_data_gap(self, sym: str):
        """Clear a data gap when data is restored."""
        with self._lock:
            self._data_gaps.pop(sym, None)

    def should_skip_symbol(self, sym: str) -> Tuple[bool, str]:
        """Check if a symbol should be skipped due to data gaps."""
        with self._lock:
            if sym not in self._data_gaps:
                return False, "ok"

            gap_start = self._data_gaps[sym]
            gap_minutes = (now_et() - gap_start).total_seconds() / 60.0

            if gap_minutes > DATA_GAP_MAX_MINUTES:
                if DATA_GAP_ACTION == "SKIP_SYMBOL":
                    return True, f"data_gap_{gap_minutes:.1f}min"
                elif DATA_GAP_ACTION == "PAUSE_ALL":
                    self.enter_degraded_mode(f"data_gap_{sym}", duration_minutes=5)
                    return True, "paused_all"

            return False, "ok"

    def can_open_new_positions(self) -> Tuple[bool, str]:
        """Check if new positions can be opened in current mode."""
        with self._lock:
            if self._mode == "NORMAL":
                return True, "ok"

            if self._mode == "DEGRADED":
                if "NO_NEW_ENTRIES" in DEGRADED_MODE_ACTIONS:
                    return False, f"degraded_mode_{','.join(self._degraded_reasons)}"

            if self._mode == "CRITICAL":
                return False, "critical_mode"

            return True, "ok"

    def get_status(self) -> Dict[str, Any]:
        """Get degraded mode status."""
        with self._lock:
            return {
                "mode": self._mode,
                "degraded_until": self._degraded_until.isoformat() if self._degraded_until else None,
                "reasons": list(self._degraded_reasons),
                "data_gaps": {s: t.isoformat() for s, t in self._data_gaps.items()},
            }


# Global Phase 3 instances (initialized in main())
api_health_monitor: Optional[APIHealthMonitor] = None
watchdog_monitor: Optional[WatchdogMonitor] = None
degraded_mode_mgr: Optional[DegradedModeManager] = None


# ============================================================
# Entry sizing + submission
# ============================================================

def cash_pct_for_session(session: MarketSession) -> float:
    return TRADE_CASH_PCT_RTH if session == MarketSession.RTH else TRADE_CASH_PCT_EXT

def compute_qty(available_cash: float, px: float, pct: float, apply_vol_scaling: bool = True) -> int:
    """
    Compute position size in shares.

    Args:
        available_cash: Available cash for trading
        px: Current price
        pct: Percentage of cash to use
        apply_vol_scaling: If True, apply volatility regime multiplier (Phase 2)

    Returns:
        Number of shares to trade
    """
    notional = max(0.0, available_cash * pct)
    if px <= 0:
        return 0

    # Phase 2: Apply volatility regime multiplier
    if apply_vol_scaling and volatility_manager:
        vol_mult = volatility_manager.get_position_size_multiplier()
        notional *= vol_mult

    qty = int(notional // px)
    return max(0, qty)

def ext_marketable_limit(sym: str, side: OrderSide, q_tape: QuoteTape, fallback_px: float) -> float:
    q, _ = q_tape.get(sym)
    if q and q.bid > 0 and q.ask > 0 and q.ask >= q.bid:
        if side == OrderSide.BUY:
            base = q.ask
            return base * (1.0 + ENTRY_SLIPPAGE_BPS_EXT / 10000.0)
        else:
            base = q.bid
            return base * (1.0 - ENTRY_SLIPPAGE_BPS_EXT / 10000.0)
    if side == OrderSide.BUY:
        return fallback_px * (1.0 + ENTRY_SLIPPAGE_BPS_EXT / 10000.0)
    else:
        return fallback_px * (1.0 - ENTRY_SLIPPAGE_BPS_EXT / 10000.0)

def submit_entry(sym: str, side: OrderSide, session: MarketSession, qty: int, entry_hint: float, r_atr: float,
                 state: BotState, q_tape: QuoteTape, log: StatusLogger):
    now = now_et()

    # === Phase 1: Idempotency check ===
    if idempotency_manager:
        can_submit, reason = idempotency_manager.can_submit_entry(sym)
        if not can_submit:
            log.event(f"[ENTRY-SKIP] {sym}: idempotency blocked - {reason}")
            return

    # Check for existing open orders for this symbol to prevent duplicates
    try:
        open_orders = alpaca_orders(status="open", limit=500)
        for o in open_orders:
            if o.get("symbol") == sym:
                log.event(f"[ENTRY-SKIP] {sym}: existing open order {o.get('id', '')[:6]}… found")
                return
    except Exception as e:
        log.event(f"[ENTRY-ERROR] {sym}: failed to check existing orders: {e}")
        raise

    # Check pending entries for this symbol
    for pe_raw in state.pending_entries.values():
        pe = PendingEntry(**pe_raw)
        if pe.sym == sym:
            log.event(f"[ENTRY-SKIP] {sym}: pending entry already exists")
            return

    if session == MarketSession.RTH:
        # Track RTH bracket orders for fill price validation
        order_id = submit_rth_bracket_with_price(sym, side, qty, entry_hint, r_atr, q_tape)

        # === Phase 1: Mark entry as in-flight ===
        if idempotency_manager:
            idempotency_manager.mark_entry_started(sym, order_id)

        pe = PendingEntry(
            order_id=order_id,
            sym=sym,
            side=side.value,
            qty=qty,
            session=session.value,
            r_atr=r_atr,
            entry_hint=float(entry_hint),
            created_iso=iso(now),
            is_rth_bracket=True,
        )
        state.pending_entries[order_id] = asdict(pe)
        state.cooldown_until[sym] = iso(now + dt.timedelta(minutes=COOLDOWN_AFTER_ENTRY_MIN))
        store.mark_dirty()
        log.event(f"[ENTRY] {sym} {side.name} qty={qty} RTH bracket (atr={r_atr:.2f})")
        return

    limit_px = ext_marketable_limit(sym, side, q_tape, entry_hint)
    order_id = submit_ext_entry_limit(sym, side, qty, limit_px)

    # === Phase 1: Mark entry as in-flight ===
    if idempotency_manager:
        idempotency_manager.mark_entry_started(sym, order_id)

    pe = PendingEntry(
        order_id=order_id,
        sym=sym,
        side=side.value,
        qty=qty,
        session=session.value,
        r_atr=r_atr,
        entry_hint=float(entry_hint),
        created_iso=iso(now),
    )
    state.pending_entries[order_id] = asdict(pe)
    state.cooldown_until[sym] = iso(now + dt.timedelta(minutes=COOLDOWN_AFTER_ENTRY_MIN))
    store.mark_dirty()
    log.event(f"[ENTRY] {sym} {side.name} qty={qty} EXT limit={limit_px:.2f} (atr={r_atr:.2f})")


# ============================================================
# Pending entry polling (EXT only)
# ============================================================

def process_pending_entries(state: BotState, log: StatusLogger, session: MarketSession = None, q_tape: 'QuoteTape' = None):
    if not state.pending_entries:
        return

    # FIX: Fetch all orders once to avoid race conditions
    try:
        all_orders = alpaca_orders(status="all", limit=500)
        orders_map = {o["id"]: o for o in all_orders if "id" in o}
    except Exception as e:
        log.throttle("pending_fetch_err", f"[PENDING] failed to fetch orders: {e}", every=30)
        return

    to_delete: List[str] = []
    for oid, raw in list(state.pending_entries.items()):
        pe = PendingEntry(**raw)

        o = orders_map.get(oid)
        if not o:
            log.throttle(f"pe_{oid}", f"[PENDING] {pe.sym} order={oid[:6]}… not found -> dropping", every=30)
            to_delete.append(oid)
            continue

        status = (o.get("status") or "").lower()

        if status == "filled":
            filled_avg = float(o.get("filled_avg_price") or pe.entry_hint or 0.0)
            if filled_avg <= 0:
                filled_avg = pe.entry_hint

            side_str = "BUY" if pe.side == "buy" else "SELL"

            # Calculate slippage for all fills
            slippage_dollars = filled_avg - pe.entry_hint
            slippage_pct = (slippage_dollars / pe.entry_hint) * 100 if pe.entry_hint > 0 else 0

            # Log slippage details
            if pe.r_atr > 0:
                slippage_r = abs(slippage_dollars) / pe.r_atr
                slippage_info = f"slippage={slippage_dollars:+.2f} / {slippage_r:.2f}R / {slippage_pct:+.2f}%"
            else:
                slippage_info = f"slippage={slippage_dollars:+.2f} / {slippage_pct:+.2f}%"

            # NOTE: RTH bracket orders use aggressive limit pricing (bid*0.998 for sells, ask*1.002 for buys)
            # The entry_hint is the signal price, NOT the actual limit price submitted.
            # Since brackets have SL/TP already attached, we skip the slippage check here.
            # The SL/TP will protect against adverse moves regardless of fill price.
            # (Previously this caused false positives - e.g., selling at $72.44 vs entry_hint $72.51
            # looked like bad slippage, but the limit was actually $72.29, so $72.44 was a good fill)

            # For EXT orders, use EXT R-multiples; for RTH, brackets are already set
            if pe.is_rth_bracket:
                # RTH bracket orders have brackets attached, just remove from pending
                log.event(f"[FILL] {pe.sym} {side_str} qty={pe.qty} entry=${filled_avg:.2f} (expected=${pe.entry_hint:.2f}, {slippage_info}) RTH bracket active")
                to_delete.append(oid)
            else:
                # EXT orders need virtual brackets
                # CRITICAL FIX: Use get_stop_distance to enforce minimum stop distance
                stop_dist = get_stop_distance(filled_avg, pe.r_atr, EXT_SL_R)
                tp_dist = EXT_TP_R * pe.r_atr

                if side_str == "BUY":
                    sl = round_price(filled_avg - stop_dist)
                    tp = round_price(filled_avg + tp_dist)
                else:
                    sl = round_price(filled_avg + stop_dist)
                    tp = round_price(filled_avg - tp_dist)

                vb = VirtualBracketState(
                    sym=pe.sym,
                    side=side_str,
                    qty=int(pe.qty),
                    entry=float(filled_avg),
                    stop=sl,
                    tp=tp,
                    entry_time_iso=iso(now_et()),
                    r_atr=float(pe.r_atr),
                    moved_be=False,
                    active=True,
                )
                state.vbrackets[pe.sym] = asdict(vb)
                store.mark_dirty()
                log.event(f"[FILL] {pe.sym} {side_str} qty={pe.qty} entry=${filled_avg:.2f} (expected=${pe.entry_hint:.2f}, {slippage_info}) -> VB stop=${sl:.4f} tp=${tp:.4f}")
                to_delete.append(oid)

        elif status in ("canceled", "expired", "rejected"):
            log.event(f"[PENDING] {pe.sym} order={oid[:6]}… {status} -> dropping pending")
            to_delete.append(oid)
        else:
            log.throttle(f"pe_{oid}", f"[PENDING] {pe.sym} order={oid[:6]}… {status}", every=60)

    for oid in to_delete:
        # === Phase 1: Mark operation as completed in idempotency manager ===
        pe_raw = state.pending_entries.get(oid)
        if pe_raw and idempotency_manager:
            idempotency_manager.mark_completed(pe_raw.get("sym", ""))

        state.pending_entries.pop(oid, None)
        store.mark_dirty()


# ============================================================
# Risk checks: daily loss limit
# ============================================================

def check_daily_loss_and_halt(state: BotState, log: StatusLogger, alerter: Optional[Alerter] = None):
    if state.halted_today:
        return
    try:
        eq = float(alpaca_account()["equity"])
    except Exception:
        return
    start_eq = float(state.start_equity or 0.0)
    if start_eq <= 0:
        return

    dd = max(0.0, start_eq - eq)
    dd_pct = dd / start_eq

    if DAILY_MAX_LOSS_DOLLARS is not None:
        hit = dd >= float(DAILY_MAX_LOSS_DOLLARS)
    else:
        hit = dd_pct >= float(DAILY_MAX_LOSS_PCT)

    if hit:
        state.halted_today = True
        store.mark_dirty()
        log.event(f"[RISK] daily loss limit hit -> HALT entries (start={start_eq:.2f} equity={eq:.2f} dd={dd:.2f}/{dd_pct*100:.2f}%)")

        # === LEVEL 3: Alert on daily loss limit ===
        if alerter:
            alerter.send_alert(
                "CRITICAL",
                "Daily Loss Limit Hit",
                f"Trading halted due to daily loss limit breach",
                {
                    "start_equity": f"${start_eq:.2f}",
                    "current_equity": f"${eq:.2f}",
                    "drawdown": f"${dd:.2f}",
                    "drawdown_pct": f"{dd_pct*100:.2f}%",
                    "limit": f"{DAILY_MAX_LOSS_PCT*100:.2f}%" if DAILY_MAX_LOSS_DOLLARS is None else f"${DAILY_MAX_LOSS_DOLLARS}",
                    "orders_cancelled": ON_DAILY_LOSS_CANCEL_ORDERS,
                    "positions_flattened": ON_DAILY_LOSS_FLATTEN
                }
            )

        if ON_DAILY_LOSS_CANCEL_ORDERS:
            try:
                cancel_all_orders()
            except Exception:
                pass
        if ON_DAILY_LOSS_FLATTEN:
            try:
                close_all_positions()
            except Exception:
                pass


# ============================================================
# Strategy class
# ============================================================

class Strategy:
    """
    Base strategy class that defines the interface for trading strategies.

    Subclass this to create new strategies with custom:
    - Universe selection
    - Indicator calculation
    - Signal generation logic
    """

    def __init__(self, name: str):
        self.name = name

    def prepare_universe(self, now: dt.datetime) -> List[str]:
        """
        Prepare the list of symbols to scan.

        Args:
            now: Current datetime

        Returns:
            List of ticker symbols to monitor
        """
        raise NotImplementedError

    def compute_indicators(self, sym: str, now: dt.datetime) -> pd.DataFrame:
        """
        Fetch data and compute technical indicators for a symbol.

        Args:
            sym: Ticker symbol
            now: Current datetime

        Returns:
            DataFrame with OHLCV data and computed indicators
        """
        raise NotImplementedError

    def generate_signal(self, sym: str, df: pd.DataFrame, anchor: dt.datetime, flags: SignalFlags) -> Optional[Tuple[OrderSide, float]]:
        """
        Generate trading signals based on indicators.

        Args:
            sym: Ticker symbol
            df: DataFrame with price data and indicators
            anchor: Session anchor time for calculations
            flags: Stateful signal flags (stretched conditions, etc.)

        Returns:
            Tuple of (OrderSide, r_atr) if signal generated, None otherwise
            r_atr is the ATR value in dollars for position sizing
        """
        raise NotImplementedError

class VWAPReclaimStrategy(Strategy):
    """
    Mean reversion strategy that buys/sells when price reclaims VWAP
    after being stretched away from it.

    Entry Conditions:
    - Price was stretched >1.15 ATR from session VWAP
    - Price reclaims within 0.30 ATR of VWAP
    - ADX < 22 (low trend strength, choppy conditions)
    - Relative volume > 1.5x average

    Exits:
    - RTH: 1R target, 1R stop
    - EXT: 0.8R target, 1R stop with break-even at 0.5R
    """

    def __init__(self):
        super().__init__("VWAP Reclaim")

    def prepare_universe(self, now: dt.datetime) -> List[str]:
        return build_universe(now)

    def compute_indicators(self, sym: str, now: dt.datetime) -> pd.DataFrame:
        end = now
        start = end - dt.timedelta(minutes=LOOKBACK_MINUTES + 10)
        df = poly.agg_1m(sym, start.date().isoformat(), end.date().isoformat(), limit=5000)
        return df

    def generate_signal(self, sym: str, df: pd.DataFrame, anchor: dt.datetime, flags: SignalFlags) -> Optional[Tuple[OrderSide, float]]:
        return evaluate_vwap_reclaim(sym, df, anchor, flags)

# Initialize the strategy (easily swap to a different strategy here)
strategy = VWAPReclaimStrategy()


# ============================================================
# Main loop
# ============================================================

def main():
    if WS_TRACE:
        websocket.enableTrace(True)

    log = StatusLogger()

    # === LEVEL 3: Initialize Production Infrastructure ===
    alerter = Alerter(log)
    kill_switch = KillSwitch(log)

    # Initialize enhanced circuit breaker with alerting
    global CB
    CB = CircuitBreaker(log, alerter)

    # === Phase 1: Initialize Exposure and Idempotency Managers ===
    global exposure_manager, idempotency_manager, volatility_manager
    exposure_manager = ExposureManager(log)
    idempotency_manager = IdempotencyManager(log)
    log.info("[STARTUP] Phase 1 safety managers initialized (exposure caps, idempotency)")

    # === Phase 2: Initialize Volatility Regime Manager ===
    volatility_manager = VolatilityRegimeManager(log)
    log.info("[STARTUP] Phase 2 volatility regime manager initialized")

    # === Phase 3: Initialize Reliability & Robustness Managers ===
    global api_health_monitor, watchdog_monitor, degraded_mode_mgr
    api_health_monitor = APIHealthMonitor(log, alerter)
    watchdog_monitor = WatchdogMonitor(log, alerter)
    degraded_mode_mgr = DegradedModeManager(log, alerter)
    log.info("[STARTUP] Phase 3 reliability managers initialized (API health, watchdog, degraded mode)")

    # === LEVEL 3: Live Trading Validation ===
    # Check if using live API (not paper-api)
    is_live = "api.alpaca.markets" in ALPACA_BASE_URL.lower() and "paper-api" not in ALPACA_BASE_URL.lower()

    if is_live and LIVE_TRADING_ENABLED:
        if LIVE_TRADING_CONFIRMATION != "YES":
            log.error("[STARTUP] LIVE TRADING BLOCKED: I_UNDERSTAND_LIVE_TRADING must be set to 'YES'")
            log.error("[STARTUP] Set environment variable: export I_UNDERSTAND_LIVE_TRADING=YES")
            return
        log.warning("=" * 80)
        log.warning("LIVE TRADING MODE - REAL MONEY AT RISK")
        log.warning("=" * 80)
        alerter.send_alert("WARNING", "Live Trading Started", "Bot running with real money", {"mode": "LIVE"})
    elif is_live and not LIVE_TRADING_ENABLED:
        log.error("[STARTUP] URL is LIVE but LIVE_TRADING=0. Set LIVE_TRADING=1 or change URL to paper.")
        return
    else:
        log.info("[STARTUP] Paper trading mode (simulated money)")

    # === LEVEL 3: Check Kill Switch on Startup ===
    triggered, reason = kill_switch.is_triggered()
    if triggered:
        log.error(f"[STARTUP] Kill switch already triggered: {reason}")
        kill_switch.execute_emergency_shutdown(alerter)
        return

    state = store.load()

    t_tape = TradeTape()
    q_tape = QuoteTape()
    ws_health = WSHealth()

    ws: Optional[WSClient] = None
    universe: List[str] = []
    last_hb = 0.0
    last_scan = 0.0
    last_minute_key: Optional[str] = None
    entries_this_minute = 0

    stop_flag = {"v": False}

    # === LEVEL 3: Enhanced Graceful Shutdown Handler ===
    def _sigint(*_):
        log.warning("[SHUTDOWN] Received interrupt signal, initiating graceful shutdown...")
        alerter.send_alert("WARNING", "Bot Shutting Down", "Graceful shutdown initiated", {"policy": SHUTDOWN_POLICY})
        stop_flag["v"] = True

    signal.signal(signal.SIGINT, _sigint)
    signal.signal(signal.SIGTERM, _sigint)

    def ensure_ws_running():
        nonlocal ws
        if ws is None or not ws.is_alive():
            if ws is not None:
                log.event("[WS] thread not alive -> restarting WS client")
            ws = WSClient(WS_URL, POLYGON_API_KEY, t_tape, q_tape, ws_health, log)
            ws.start()
            if universe:
                ws.set_symbols(universe)

    def heartbeat(session: MarketSession, scan_stats: Optional[Dict] = None):
        """
        Consolidated heartbeat + scan output. Cleaner format:
        - Only show abnormal status flags
        - Combine HB + SCAN into one line when scan_stats provided
        - Show position P&L when holding
        """
        nonlocal last_hb
        if (time.time() - last_hb) < LOG_HEARTBEAT_SECONDS:
            return
        last_hb = time.time()

        connected, last_msg = ws_health.snapshot()
        ws_age = (time.time() - last_msg) if last_msg else 999.0

        # Build status flags (only show if abnormal)
        flags = []
        if not connected:
            flags.append("WS_DISCONNECTED")
        if ws_age > 30:
            flags.append(f"ws_stale_{ws_age:.0f}s")
        if state.halted_today:
            flags.append("HALTED")
        cb_status = CB.is_open() if CB else False
        if cb_status:
            flags.append("CB_OPEN")
        if degraded_mode_mgr and degraded_mode_mgr.mode != "NORMAL":
            flags.append(f"MODE={degraded_mode_mgr.mode}")

        flags_str = f" [{', '.join(flags)}]" if flags else ""

        # Volatility regime (always show - useful context)
        vol_str = ""
        if volatility_manager:
            vol_status = volatility_manager.get_status()
            regime = vol_status['regime']
            # Only show size_mult if not 1.0
            if abs(vol_status['size_multiplier'] - 1.0) > 0.01:
                vol_str = f" VIX={vol_status['vix']:.1f}({regime},x{vol_status['size_multiplier']:.2f})"
            else:
                vol_str = f" VIX={vol_status['vix']:.1f}({regime})"

        # Metrics summary (compact)
        metrics_str = ""
        if metrics_tracker:
            m = metrics_tracker.get_summary()
            if m['trades'] > 0:
                metrics_str = f" day:{m['trades']}T/{m['win_rate']:.0%}WR/{m['avg_r_per_trade']:.2f}R"

        # API health (only show if degraded)
        health_str = ""
        if api_health_monitor:
            health = api_health_monitor.get_metrics()
            if health["requests"] > 0 and health['success_rate'] < 0.95:
                health_str = f" API:{health['success_rate']*100:.0f}%ok"

        # Position status with P&L
        pos_str = ""
        try:
            positions = alpaca_positions()
            open_pos = [p for p in positions if abs(float(p["qty"])) > 0]
            if open_pos:
                pos_details = []
                for p in open_pos:
                    sym = p["symbol"]
                    unrealized = float(p.get("unrealized_pl", 0))
                    pnl_pct = float(p.get("unrealized_plpc", 0)) * 100
                    side_char = "L" if float(p["qty"]) > 0 else "S"
                    pos_details.append(f"{sym}{side_char}:{unrealized:+.0f}({pnl_pct:+.1f}%)")
                pos_str = f" pos=[{', '.join(pos_details)}]"
        except Exception:
            pass

        # Scan stats (if provided)
        scan_str = ""
        if scan_stats:
            scan_str = f" scan:{scan_stats['scanned']}/{scan_stats['eligible']}/{scan_stats['signals']}"
            if scan_stats.get('rejections'):
                # Show top rejection reasons
                top_reasons = sorted(scan_stats['rejections'].items(), key=lambda x: -x[1])[:3]
                if top_reasons:
                    reasons_str = ','.join([f"{r[0]}:{r[1]}" for r in top_reasons])
                    scan_str += f" skip:[{reasons_str}]"

        log.info(
            f"[HB] {session.value} u={len(universe)}{vol_str}{metrics_str}{pos_str}{scan_str}{health_str}{flags_str}"
        )

    def daily_reset_if_needed(now: dt.datetime):
        nonlocal universe, entries_this_minute, last_minute_key
        k = now.strftime("%Y-%m-%d")
        if state.day_key == k:
            return

        # Log final daily metrics before reset
        if metrics_tracker:
            summary = metrics_tracker.get_summary()
            log.info(f"[DAILY-SUMMARY] {state.day_key}: {summary}")
            metrics_tracker.reset_daily()

        state.day_key = k
        state.halted_today = False
        state.entries_paused_until_iso = ""

        eq = float(alpaca_account()["equity"])
        state.start_equity = eq

        state.signal_flags = {}
        state.cooldown_until = {}

        live_syms = {p["symbol"] for p in alpaca_positions() if abs(float(p["qty"])) > 0}
        state.vbrackets = {s: vb for s, vb in (state.vbrackets or {}).items() if s in live_syms}
        state.pending_entries = {}

        universe = strategy.prepare_universe(now)
        ensure_ws_running()
        ws.set_symbols(universe)

        try:
            cancel_all_orders()
        except Exception:
            pass

        entries_this_minute = 0
        last_minute_key = None

        store.mark_dirty()
        store.flush_if_needed(force=True)

        # Market context at session start
        vol_ctx = ""
        if volatility_manager:
            try:
                volatility_manager.fetch_vix_from_polygon()
                vol_status = volatility_manager.get_status()
                vol_ctx = f" VIX={vol_status['vix']:.1f}({vol_status['regime']})"
            except Exception:
                pass

        log.info(f"[RESET] {k} equity=${eq:.2f} universe={len(universe)}{vol_ctx}")

    def startup_reconcile():
        nonlocal universe
        now = now_et()
        md = get_market_day(now.date())
        session = session_now(now, md)

        universe = strategy.prepare_universe(now)
        ensure_ws_running()
        ws.set_symbols(universe)

        pos = alpaca_positions()
        pos_syms = [p["symbol"] for p in pos if abs(float(p["qty"])) > 0]

        # Market context at startup
        vol_ctx = ""
        if volatility_manager:
            try:
                volatility_manager.fetch_vix_from_polygon()
                vol_status = volatility_manager.get_status()
                vol_ctx = f" VIX={vol_status['vix']:.1f}({vol_status['regime']})"
            except Exception:
                pass

        acct_ctx = ""
        try:
            acct = alpaca_account()
            equity = float(acct.get("equity", 0))
            buying_power = float(acct.get("buying_power", 0))
            acct_ctx = f" equity=${equity:.0f} bp=${buying_power:.0f}"
        except Exception:
            pass

        pos_str = f"pos={pos_syms}" if pos_syms else "pos=[]"
        log.event(f"[START] {session.value} {pos_str} u={len(universe)}{acct_ctx}{vol_ctx}")

        for p in pos:
            sym = p["symbol"]
            qty = float(p["qty"])
            if abs(qty) == 0:
                continue
            if sym not in state.vbrackets:
                if RECONCILE_IF_POSITION_NO_STATE == "FLATTEN":
                    log.event(f"[RECONCILE] {sym}: no state -> flatten (policy).")
                    try:
                        close_position(sym, logger=log, session=session, q_tape=q_tape)
                    except Exception:
                        pass
                else:
                    reconstruct_ext_vbracket(sym, qty, state, log, session=session, q_tape=q_tape)

        try:
            open_orders = alpaca_orders(status="open", limit=500)
            open_ids = {o.get("id") for o in open_orders if o.get("id")}
            for oid in list(state.pending_entries.keys()):
                if oid not in open_ids:
                    state.pending_entries.pop(oid, None)
                    store.mark_dirty()
        except Exception:
            pass

        store.flush_if_needed(force=True)

    if not state.day_key:
        state.day_key = now_et().strftime("%Y-%m-%d")
        try:
            state.start_equity = float(alpaca_account()["equity"])
        except Exception:
            state.start_equity = 0.0
        store.mark_dirty()
        store.flush_if_needed(force=True)

    startup_reconcile()

    # FIX: Start async news cache worker
    news_cache.start_worker()

    while not stop_flag["v"]:
        # === Phase 3: Watchdog loop start ===
        if watchdog_monitor:
            watchdog_monitor.start_loop_iteration()
            watchdog_monitor.heartbeat()

        # === LEVEL 3: Kill Switch Check (highest priority) ===
        triggered, reason = kill_switch.is_triggered()
        if triggered:
            log.error(f"[KILL_SWITCH] Triggered during runtime: {reason}")
            kill_switch.execute_emergency_shutdown(alerter)
            break

        # === Phase 3: Check degraded mode auto-recovery ===
        if degraded_mode_mgr:
            degraded_mode_mgr.check_auto_recovery()

        ensure_ws_running()

        now = now_et()
        md = get_market_day(now.date())
        session = session_now(now, md)

        daily_reset_if_needed(now)

        minute_key = now.strftime("%Y-%m-%d %H:%M")
        if minute_key != last_minute_key:
            last_minute_key = minute_key
            entries_this_minute = 0

        heartbeat(session)

        if session == MarketSession.CLOSED:
            store.flush_if_needed()
            sleep_s = sleep_until_next_open_seconds()
            log.throttle("sleep", f"[STATE] market closed -> sleeping {sleep_s/60:.1f} min", every=10)
            time.sleep(sleep_s)
            continue

        if CB and CB.is_open():
            log.throttle("cb", f"[STATE] circuit-breaker open -> sleeping {CB_OPEN_SLEEP_SECONDS:.0f}s", every=10)
            time.sleep(CB_OPEN_SLEEP_SECONDS)
            continue

        if hard_flatten_window(now, md):
            try:
                cancel_all_orders()
            except Exception:
                pass
            try:
                close_all_positions()
            except Exception:
                pass
            log.throttle("flatten", "[STATE] hard-flatten window -> flattening/canceling", every=30)
            time.sleep(5.0)
            continue

        try:
            manage_virtual_brackets(state, session, t_tape, q_tape, log)
        except Exception as e:
            log.event(f"[EXIT-MGMT-ERROR] {e}")

        try:
            process_pending_entries(state, log, session=session, q_tape=q_tape)
        except Exception as e:
            log.event(f"[PENDING-ERROR] {e}")

        try:
            check_daily_loss_and_halt(state, log, alerter)
        except Exception:
            pass

        if state.halted_today:
            store.flush_if_needed()
            time.sleep(SCAN_TICK_SECONDS)
            continue

        ok_feed, feed_reason = feed_ok_for_entries(universe, q_tape, ws_health)
        if not ok_feed:
            log.throttle("feed_wait", f"[STATE] waiting on market data feed -> {feed_reason}", every=LOG_THROTTLE_SECONDS)
            try:
                pos_syms = [p["symbol"] for p in alpaca_positions() if abs(float(p["qty"])) > 0]
                if pos_syms and critical_data_missing_for_positions(pos_syms, t_tape, q_tape):
                    if ON_CRITICAL_DATA_MISSING_FLATTEN:
                        log.event("[STATE] critical data missing for position(s) -> flattening")
                        try:
                            close_all_positions()
                        except Exception:
                            pass
            except Exception:
                pass

            store.flush_if_needed()
            time.sleep(SCAN_TICK_SECONDS)
            continue

        if session == MarketSession.RTH and no_new_entries_window(now, md):
            log.throttle("nonew", "[STATE] no-new-entries window (RTH) -> managing only", every=60)
            store.flush_if_needed()
            time.sleep(SCAN_TICK_SECONDS)
            continue

        if (time.time() - last_scan) < SCAN_EVERY_N_SECONDS:
            store.flush_if_needed()
            time.sleep(SCAN_TICK_SECONDS)
            continue
        last_scan = time.time()

        # === Phase 2: Update VIX and check volatility regime ===
        if volatility_manager:
            try:
                volatility_manager.fetch_vix_from_polygon()
            except Exception as e:
                log.warning(f"[VOLATILITY] VIX fetch error: {e}")

            # Check if risk-off mode is active
            vol_allowed, vol_reason = volatility_manager.should_allow_entry()
            if not vol_allowed:
                log.throttle("vol_riskoff", f"[VOLATILITY] Entries blocked: {vol_reason}", every=60)
                store.flush_if_needed()
                time.sleep(SCAN_TICK_SECONDS)
                continue

        # === Phase 3: Check degraded mode ===
        if degraded_mode_mgr:
            can_open, degraded_reason = degraded_mode_mgr.can_open_new_positions()
            if not can_open:
                log.throttle("degraded", f"[DEGRADED] Entries blocked: {degraded_reason}", every=60)
                store.flush_if_needed()
                time.sleep(SCAN_TICK_SECONDS)
                continue

        try:
            positions = alpaca_positions()
        except Exception as e:
            # Phase 3: Enter degraded mode on API failure
            if degraded_mode_mgr and DEGRADED_MODE_ENABLED:
                degraded_mode_mgr.enter_degraded_mode(f"positions_api_error: {type(e).__name__}", duration_minutes=5)
            positions = []
        open_pos_syms = {p["symbol"] for p in positions if abs(float(p["qty"])) > 0}

        pending_count = len(state.pending_entries)
        if len(open_pos_syms) >= MAX_OPEN_POSITIONS or pending_count >= MAX_PENDING_ENTRY_ORDERS:
            log.event(f"[SCAN] {minute_key} scanned={len(universe)} eligible=0 signals=0 (1 trade max/min)")
            store.flush_if_needed()
            continue

        if entries_this_minute >= MAX_ENTRIES_PER_MINUTE:
            log.event(f"[SCAN] {minute_key} scanned={len(universe)} eligible=0 signals=0 (1 trade max/min)")
            store.flush_if_needed()
            continue

        try:
            acct = alpaca_account()
            cash = float(acct.get("cash") or 0.0)
            equity = float(acct.get("equity") or 0.0)
        except Exception:
            cash = 0.0
            equity = 0.0

        pct = cash_pct_for_session(session)

        # FIX: Prefetch news for entire universe asynchronously
        news_cache.prefetch_batch(universe)

        eligible = 0
        signals = 0
        took_trade = False
        skip_reasons: Dict[str, int] = {}  # Track why symbols were skipped

        anchor = anchor_for_session(now, md, session)
        now_ts = time.time()

        for sym in universe:
            if sym in open_pos_syms:
                skip_reasons["in_pos"] = skip_reasons.get("in_pos", 0) + 1
                continue

            cd_iso = state.cooldown_until.get(sym)
            if cd_iso:
                try:
                    if now < from_iso(cd_iso):
                        skip_reasons["cooldown"] = skip_reasons.get("cooldown", 0) + 1
                        continue
                except Exception:
                    pass

            if not spread_liquidity_ok(sym, q_tape, now_ts):
                skip_reasons["spread"] = skip_reasons.get("spread", 0) + 1
                continue

            # FIX: Use async news check (non-blocking)
            has_news = news_cache.has_recent_news(sym)
            if has_news:  # Skip if news found
                skip_reasons["news"] = skip_reasons.get("news", 0) + 1
                continue

            eligible += 1

            try:
                df = strategy.compute_indicators(sym, now)
            except Exception:
                skip_reasons["data_err"] = skip_reasons.get("data_err", 0) + 1
                continue
            if df.empty:
                skip_reasons["no_data"] = skip_reasons.get("no_data", 0) + 1
                continue

            last_close = float(df["close"].iloc[-1])
            if not (MIN_PRICE <= last_close <= MAX_PRICE):
                skip_reasons["price"] = skip_reasons.get("price", 0) + 1
                continue

            if sym not in state.signal_flags:
                state.signal_flags[sym] = asdict(SignalFlags())
            flags = SignalFlags(**state.signal_flags[sym])

            sig = evaluate_vwap_reclaim(sym, df, anchor, flags)
            state.signal_flags[sym] = asdict(flags)
            store.mark_dirty()

            if not sig:
                skip_reasons["no_setup"] = skip_reasons.get("no_setup", 0) + 1
                continue

            side, r_atr = sig
            signals += 1

            # Log signal generation
            if metrics_tracker:
                metrics_tracker.log_signal(taken=False)  # Will update to True if actually taken

            qty = compute_qty(cash, last_close, pct)
            if qty <= 0:
                if metrics_tracker:
                    metrics_tracker.log_order_rejected()
                continue

            # === Phase 1: Exposure check and quantity adjustment ===
            if exposure_manager and equity > 0:
                qty = exposure_manager.adjust_qty_for_limits(
                    sym=sym,
                    desired_qty=qty,
                    px=last_close,
                    positions=positions,
                    equity=equity,
                    pending_entries=state.pending_entries
                )
                if qty <= 0:
                    log.event(f"[EXPOSURE] {sym}: qty reduced to 0 by exposure limits, skipping")
                    if metrics_tracker:
                        metrics_tracker.log_order_rejected()
                    continue

            try:
                submit_entry(sym, side, session, qty, entry_hint=last_close, r_atr=float(r_atr), state=state, q_tape=q_tape, log=log)
                entries_this_minute += 1
                took_trade = True
                if metrics_tracker:
                    metrics_tracker.log_signal(taken=True)  # Signal was actually taken
                break
            except Exception as e:
                log.error(f"[ORDER-ERROR] {sym} {side.name}: {e}")
                if metrics_tracker:
                    metrics_tracker.log_order_rejected()
                break

        # Consolidated scan output with heartbeat (replaces separate [SCAN] line)
        scan_stats = {
            "scanned": len(universe),
            "eligible": eligible,
            "signals": signals,
            "rejections": skip_reasons if eligible == 0 else {}  # Only show reasons if no eligible
        }
        # Force heartbeat update with scan stats
        last_hb = 0  # Reset to force immediate heartbeat
        heartbeat(session, scan_stats=scan_stats)

        store.flush_if_needed()

        # === Phase 3: Watchdog loop end ===
        if watchdog_monitor:
            loop_duration = watchdog_monitor.end_loop_iteration()

        time.sleep(2.0 if took_trade else SCAN_TICK_SECONDS)

    # === LEVEL 3: Graceful Shutdown ===
    log.info("[SHUTDOWN] Initiating graceful shutdown...")

    # Stop background threads
    if ws:
        log.info("[SHUTDOWN] Stopping WebSocket...")
        ws.stop()
    news_cache.stop_worker()

    # Execute shutdown policy
    try:
        if SHUTDOWN_POLICY == "FLATTEN_ALL":
            log.info("[SHUTDOWN] Policy: FLATTEN_ALL - cancelling orders and closing positions...")
            cancel_all_orders()
            close_all_positions()
        else:
            log.info("[SHUTDOWN] Policy: CANCEL_ORDERS_ONLY - cancelling open orders...")
            cancel_all_orders()
    except Exception as e:
        log.error(f"[SHUTDOWN] Policy execution error: {e}")

    # Final state save
    store.flush_if_needed(force=True)

    log.info("[SHUTDOWN] Shutdown complete.")
    alerter.send_alert("INFO", "Bot Shutdown Complete", "Bot stopped successfully", {"policy": SHUTDOWN_POLICY})


if __name__ == "__main__":
    main()

