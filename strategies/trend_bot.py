"""
Volatility-Targeted Trend Following Bot (Long-Only, ETF Portfolio)
=================================================================

What it does
------------
- Trades a diversified ETF basket using a simple trend filter (Close > SMA200 => long, else flat)
- Sizes positions using inverse-volatility weights to target smoother risk
- Rebalances weekly (Fridays near the close) to reduce churn
- Applies portfolio risk caps:
  - Max gross exposure
  - Per-asset max weight
  - Drawdown circuit breaker (v42: smooth scaling instead of binary trigger)
  - Equity "risk-off" regime filter when SPY is below SMA200
  - Dynamic vol floor based on vol clustering (v42)

v42 Improvements (Research-Backed):
- Smooth Drawdown Scaling: Exposure scales continuously from 1.0x to 0.25x as drawdown
  increases from 3% to 20%, reducing whipsaw vs binary 10% trigger
- Dynamic Vol Floor: Raises vol floor by 50% when short-term vol >> long-term vol,
  preventing over-concentration right after market shocks (GARCH-like effect)

Data/Execution
--------------
- Uses Alpaca for orders + (by default) Alpaca market data for daily bars
- Optionally supports Polygon daily bars if you set DATA_SOURCE="polygon"

IMPORTANT SECURITY NOTE
-----------------------
Do NOT paste your API keys into this file.
Set environment variables instead:
  - ALPACA_API_KEY
  - ALPACA_SECRET_KEY
  - (optional) POLYGON_API_KEY

Dependencies
------------
pip install alpaca-py pandas numpy requests pytz

Run
---
python volatility_targeted_bot.py

This script runs continuously and will only place orders during the configured rebalance window.
Errors intentionally halt execution (per your preference).
"""

from __future__ import annotations

import os
import json
import time
import math
import csv
import traceback
import logging
import threading
import signal
import smtplib
import hashlib
import argparse
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta, date, time as dt_time
from typing import Any, Dict, List, Tuple, Optional
from logging.handlers import RotatingFileHandler
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

import yaml
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
    # Try config directory first (preferred location)
    _env_path = Path(__file__).parent.parent / "config" / "trend_bot.env"
    if not _env_path.exists():
        # Fall back to strategies directory for backward compatibility
        _env_path = Path(__file__).parent / "trend_bot.env"
    if _env_path.exists():
        load_dotenv(_env_path)
except ImportError:
    pass  # dotenv not installed, rely on environment variables


# =========================
# CONFIG (exact tickers, params, caps)
# =========================
#
# SOURCES OF TRUTH (to prevent configuration conflicts):
# -------------------------------------------------------
# 1. REGIME STATE:       state.spy_regime with hysteresis (REGIME_BUFFER_ON/OFF)
#                        - Single source for both weekly rebalance and daily monitoring
#                        - Uses compute_regime_with_hysteresis() + update_regime_state()
#
# 2. CAPITAL DEPLOYMENT: compute_dynamic_capital_usage() with CAPITAL_DEPLOYMENT_TIERS
#                        - Replaces static MAX_CAPITAL_USAGE_PCT (deprecated)
#                        - Dynamic based on regime + risk_score + events
#
# 3. LEVERAGE:           ENABLE_CONDITIONAL_LEVERAGE is the MASTER SWITCH
#                        - Must be True for any leverage (vol-targeted or static)
#                        - ENABLE_VOL_TARGETED_LEVERAGE selects the method
#
# 4. REBALANCE SCHEDULE: REBALANCE_WEEKDAY + REBALANCE_TIME_ET (pre-close)
#                        - Weekly only, Friday 3:50 PM ET
#
# 5. DAILY MONITORING:   Exit-only, never rebalances or buys
#                        - Uses same regime state as weekly
#                        - See DECISION HIERARCHY section for rules
#
# 6. EVENT CALENDAR:     Loaded from system/config/event_calendar.yaml
#                        - No hardcoded event dates in this file
#

ET = pytz.timezone("America/New_York")

# --- Directory Paths (for organized folder structure) ---
ALGO_ROOT = Path(__file__).parent.parent  # Algo_Trading root
# Allow output dir override (useful when source lives on Google Drive)
_output_root = Path(os.getenv("ALGO_OUTPUT_DIR", "")) if os.getenv("ALGO_OUTPUT_DIR") else ALGO_ROOT
DATA_DIR = _output_root / "data" / "state"
LOGS_DIR = _output_root / "logs"
CONFIG_DIR = ALGO_ROOT / "config"
DATA_DIR.mkdir(parents=True, exist_ok=True)
LOGS_DIR.mkdir(exist_ok=True)

# System config directory (for event_calendar.yaml)
SYSTEM_CONFIG_DIR = ALGO_ROOT / "config"


# =========================
# EVENT CALENDAR LOADER
# =========================
def load_event_calendar() -> Dict[str, Any]:
    """
    Load macro event calendar from YAML config file.

    Returns:
        Dict containing event_risk settings and calendar events.
        Falls back to empty calendar if file not found.
    """
    calendar_path = SYSTEM_CONFIG_DIR / "event_calendar.yaml"

    if not calendar_path.exists():
        # Fallback: check local config dir
        calendar_path = CONFIG_DIR / "event_calendar.yaml"

    if not calendar_path.exists():
        print(f"[WARNING] Event calendar not found at {calendar_path}, using empty calendar")
        return {
            "event_risk": {
                "multipliers": {"high": 0.80, "medium": 0.90, "low": 0.95, "none": 1.00},
                "lead_days": {"high": 1, "medium": 0, "low": 0}
            },
            "calendar": {"year": datetime.now().year, "events": {}}
        }

    try:
        with open(calendar_path, "r") as f:
            config = yaml.safe_load(f)
        print(f"[INFO] Loaded event calendar from {calendar_path}")
        return config
    except Exception as e:
        print(f"[ERROR] Failed to load event calendar: {e}")
        return {
            "event_risk": {
                "multipliers": {"high": 0.80, "medium": 0.90, "low": 0.95, "none": 1.00},
                "lead_days": {"high": 1, "medium": 0, "low": 0}
            },
            "calendar": {"year": datetime.now().year, "events": {}}
        }


# Load event calendar at module init
_EVENT_CALENDAR_CONFIG = load_event_calendar()

# Core tradable universe (liquid ETFs)
# Broad market (always eligible)
EQUITY_TICKERS = ["SPY", "QQQ", "IWM"]  # v8: Removed EFA, EEM (low-beta international)

# Sector/Factor ETFs (for rotation - pick top performers)
SECTOR_TICKERS = [
    "XLK",   # Technology
    "XLF",   # Financials
    "XLE",   # Energy
    "XLV",   # Healthcare
    "XLI",   # Industrials
    "XLY",   # Consumer Discretionary
    "XLC",   # Communication Services
    "SMH",   # Semiconductors (high beta tech)
    "IBB",   # Biotech
    "XHB",   # Homebuilders
]  # v8: Removed XLP, XLU, XLRE, XLB (low-beta defensive sectors)

# Factor ETFs (momentum/quality tilt)
FACTOR_TICKERS = [
    "MTUM",  # Momentum Factor
    "QUAL",  # Quality Factor
]  # v8: Removed VLUE (value factor underperforms in momentum regime)

# Defensive ETFs (risk-off allocation)
DEFENSIVE_TICKERS = ["IEF", "TLT", "GLD", "DBC"]
CASH_TICKER = "SGOV"

# v8: Leveraged ETFs (high-beta amplifiers for strong trends)
LEVERAGED_ETFS = ["TQQQ", "UPRO", "SOXL", "TECL", "FAS"]

# v8: Momentum/Thematic ETFs (high-momentum sectors)
MOMENTUM_ETFS = ["ARKK", "XBI", "KWEB", "SOXX", "IGV", "CIBR", "SKYY"]

# Combined universe
ALL_EQUITY = sorted(list(set(
    EQUITY_TICKERS + SECTOR_TICKERS + FACTOR_TICKERS +
    LEVERAGED_ETFS + MOMENTUM_ETFS
)))
ALL_TICKERS = sorted(list(set(ALL_EQUITY + DEFENSIVE_TICKERS + [CASH_TICKER])))

# Wash sale substitute mapping: when a symbol is sold at a loss, buy the substitute
# instead for 31 days to avoid triggering IRS wash sale rules.
# Bidirectional pairs — each side maps to the other.
# Symbols with no entry here use cooldown-only (no re-entry for 31 days).
WASH_SALE_SUBSTITUTES = {
    # Broad Market
    "SPY": "VOO",   "VOO": "SPY",
    "QQQ": "QQQM",  "QQQM": "QQQ",
    "IWM": "VTWO",  "VTWO": "IWM",
    # Sectors (SPDR ↔ Vanguard)
    "XLK": "VGT",   "VGT": "XLK",
    "XLF": "VFH",   "VFH": "XLF",
    "XLE": "VDE",   "VDE": "XLE",
    "XLV": "VHT",   "VHT": "XLV",
    "XLI": "VIS",   "VIS": "XLI",
    "XLY": "VCR",   "VCR": "XLY",
    "XLC": "VOX",   "VOX": "XLC",
    # In-universe pairs (swap within existing universe)
    "SMH": "SOXX",  "SOXX": "SMH",
    "IBB": "XBI",   "XBI": "IBB",
    # Specialty
    "XHB": "ITB",   "ITB": "XHB",
    # Factor
    "MTUM": "JMOM", "JMOM": "MTUM",
    "QUAL": "JQUA", "JQUA": "QUAL",
    # Defensive
    "IEF": "VGIT",  "VGIT": "IEF",
    "TLT": "VGLT",  "VGLT": "TLT",
    "GLD": "IAU",   "IAU": "GLD",
    "DBC": "PDBC",  "PDBC": "DBC",
    # Cash
    "SGOV": "BIL",  "BIL": "SGOV",
    # Thematic
    "KWEB": "MCHI", "MCHI": "KWEB",
    "IGV": "WCLD",  "WCLD": "IGV",
    "CIBR": "HACK", "HACK": "CIBR",
    "SKYY": "CLOU", "CLOU": "SKYY",
    # NO SUBSTITUTE (cooldown only):
    # TQQQ, UPRO, SOXL, TECL, FAS — leveraged, no equivalent
    # ARKK — unique active strategy
}
WASH_SALE_COOLDOWN_DAYS = 31

# Tactical overlay ETFs (for risk signals)
# HYG/LQD = credit spreads, TLT/IEF = rates, RSP = breadth (equal-weight S&P)
TACTICAL_ETFS = ["HYG", "LQD", "TLT", "IEF", "RSP"]

# Helper: is this an equity-like ETF? (applies risk-off multiplier)
ALL_EQUITY_SET = set(ALL_EQUITY)
def is_equity_like(sym: str) -> bool:
    """Check if symbol is equity-like (should get risk-off multiplier)."""
    return sym in ALL_EQUITY_SET

# Strategy parameters (optimized for robustness)
SMA_LOOKBACK_DAYS = 50                # v8: Faster trend filter (was 200). Note: "SMA200" in variable names is legacy naming convention
VOL_LOOKBACK_DAYS = 60                # realized vol lookback (increased from 20 to reduce noise)
VOL_FLOOR_ANNUAL = 0.12               # floor to prevent over-concentration (increased from 0.08)
TARGET_GROSS_EXPOSURE = 1.00          # max gross exposure WITHIN deployed capital (long-only => <= 1)

# v42: Dynamic Volatility Floor (Research-Backed)
# Volatility clusters - high vol periods persist (GARCH effect)
# Raise floor during vol clusters to prevent over-leveraging after shocks
USE_DYNAMIC_VOL_FLOOR = True          # Enable dynamic vol floor based on vol clustering
VOL_SHORT_LOOKBACK = 20               # Short-term vol (recent conditions)
VOL_CLUSTER_THRESHOLD = 1.3           # If short_vol > long_vol * 1.3 = vol cluster detected
VOL_FLOOR_CLUSTER_MULT = 1.5          # Raise floor by 50% during cluster (0.12 -> 0.18)
MAX_WEIGHT_PER_ASSET = 0.40           # v8: Higher concentration (was 0.22) - top 4 with up to 40% each
MAX_WEIGHT_CASH = 1.00                # cash proxy can take the remainder
MIN_WEIGHT_PER_ASSET = 0.03           # minimum position size to avoid tiny positions

# =========================
# DYNAMIC CAPITAL DEPLOYMENT (replaces static MAX_CAPITAL_USAGE_PCT)
# =========================
# Capital usage is now a function of regime + risk_score + event_risk
# This eliminates the permanent "35% cash drag" that kills CAGR
ENABLE_DYNAMIC_CAPITAL = True         # Enable dynamic capital deployment
CAPITAL_USAGE_BASE = 0.65             # Base deployment (used when dynamic is disabled)

# Capital deployment tiers by risk conditions
# Format: (min_risk_score, spy_risk_on_required, capital_pct)
CAPITAL_DEPLOYMENT_TIERS = [
    # v8: More aggressive deployment - fully deployed in risk-on
    (40, True, 1.00),   # Risk-on + score >= 40 -> fully deployed
    (0, True, 0.85),    # Risk-on + any score -> 85% deployed
    # Risk-off -> defensive
    (0, False, 0.40),   # Risk-off -> 40% deployed
    # Fallback
    (0, None, 0.40),
]

# NOTE: MAX_CAPITAL_USAGE_PCT is deprecated - dynamic deployment replaces it.
# This alias is kept only for backward compatibility with old logs/diagnostics.
# DO NOT use in new code - use compute_dynamic_capital_usage() instead.
MAX_CAPITAL_USAGE_PCT = CAPITAL_USAGE_BASE  # DEPRECATED

# =========================
# TOP-N ROTATION (Phase 2 CAGR Upgrade)
# =========================
# Instead of holding all eligible ETFs, rotate into top N by momentum score
ENABLE_TOP_N_ROTATION = True          # Enable rotation strategy
TOP_N_EQUITY = 4                      # v8: More concentrated (was 6) - hold top 4 by momentum score
TOP_N_DEFENSIVE = 2                   # Hold top N defensive ETFs

# =========================
# ENHANCED MOMENTUM RANKING (CAGR Edge)
# =========================
# Key improvements over naive momentum:
# 1. Relative strength vs SPY (not just absolute returns)
# 2. Skip last month (12-1 momentum) to avoid reversal noise
# 3. Vol-adjusted score to prefer "quality trends"

ENABLE_RELATIVE_STRENGTH = True       # Rank by RS vs SPY (not absolute momentum)
ENABLE_SKIP_MONTH = True              # Skip last ~21 days (12-1 momentum)
ENABLE_VOL_ADJUSTED_MOMENTUM = True   # Divide momentum by vol for ranking
SKIP_MONTH_DAYS = 21                  # Days to skip for 12-1 momentum

# Momentum scoring weights (for ranking)
# Score = w1*ret_63d + w2*ret_126d + w3*ret_252d (normalized)
MOMENTUM_WEIGHT_63D = 0.40            # 3-month momentum weight
MOMENTUM_WEIGHT_126D = 0.35           # 6-month momentum weight
MOMENTUM_WEIGHT_252D = 0.25           # 12-month momentum weight

# Momentum lookback periods (trading days)
MOMENTUM_LOOKBACK_63D = 63            # ~3 months
MOMENTUM_LOOKBACK_126D = 126          # ~6 months
MOMENTUM_LOOKBACK_252D = 252          # ~12 months

# Sector RS confirmation: don't hold sector unless RS vs SPY is positive
ENABLE_SECTOR_RS_FILTER = True        # Filter out sectors with negative RS

# =========================
# RISK SCORE OVERLAY (Phase 2 CAGR Upgrade)
# =========================
# Market risk score (0-100) determines exposure multiplier
# Score components: VIX regime, breadth, trend health
ENABLE_RISK_SCORE = True              # Enable risk score overlay

# VIX thresholds for regime
VIX_LOW_THRESHOLD = 15.0              # Below = low vol = risk-on boost
VIX_MEDIUM_THRESHOLD = 20.0           # 15-20 = normal
VIX_HIGH_THRESHOLD = 25.0             # 20-25 = elevated caution
VIX_EXTREME_THRESHOLD = 30.0          # Above = high vol = de-risk
VIX_CIRCUIT_BREAKER = 35.0            # Emergency cap: max 50% exposure if VIX > 35
VIX_CIRCUIT_BREAKER_MAX_EXPOSURE = 0.50  # Hard cap on exposure when VIX > circuit breaker

# Risk score -> exposure multiplier mapping
# Score 80-100: full risk-on, exposure up to 1.2x (mild leverage)
# Score 60-80:  normal, exposure 1.0x
# Score 40-60:  caution, exposure 0.8x
# Score 20-40:  defensive, exposure 0.6x
# Score 0-20:   risk-off, exposure 0.4x
RISK_SCORE_EXPOSURE_MAP = {
    80: 1.20,   # risk_score >= 80 -> 1.2x exposure
    60: 1.00,   # risk_score >= 60 -> 1.0x exposure
    40: 0.80,   # risk_score >= 40 -> 0.8x exposure
    20: 0.60,   # risk_score >= 20 -> 0.6x exposure
    0:  0.40,   # risk_score >= 0  -> 0.4x exposure
}

# =========================
# CONDITIONAL LEVERAGE (Phase 2 CAGR Upgrade)
# =========================
# Allow exposure > 1.0 in strong conditions (requires margin)
# WARNING: This is a MASTER SWITCH - must be True for any leverage to apply.
# Set to True to enable leverage (vol-targeted or static), False to disable all leverage.
ENABLE_CONDITIONAL_LEVERAGE = True    # v8: ENABLED - 25% margin leverage in strong conditions

MAX_LEVERAGE_RATIO = 1.25             # Max leverage when conditions are ideal
LEVERAGE_RISK_SCORE_MIN = 80          # Only leverage when risk score >= 80
LEVERAGE_VIX_MAX = 18.0               # Only leverage when VIX < 18
LEVERAGE_REGIME_REQUIRED = "risk_on"  # Only leverage in risk-on regime

# Vol-targeted leverage: smarter than static VIX thresholds
# NOTE: Only active when ENABLE_CONDITIONAL_LEVERAGE = True (master switch)
ENABLE_VOL_TARGETED_LEVERAGE = True   # Use portfolio vol targeting instead of static max
LEVERAGE_TARGET_VOL = 0.12            # Target 12% annualized vol
LEVERAGE_VOL_LOOKBACK = 20            # 20-day realized vol for targeting

# =========================
# REBALANCE SCHEDULE (Mid-Morning for Stable Spreads)
# =========================
# Decision: Rebalance at 11:00 AM ET when spreads have tightened after open volatility.
# Trade-off: Signals use "as of yesterday close" data.
# Note: is_rebalance_window_dynamic() requires clock.is_open=True.
REBALANCE_WEEKDAY = 4                 # 0=Mon ... 4=Fri
REBALANCE_TIME_ET = (11, 0)           # 11:00 AM ET (stable spreads, good liquidity)
REBALANCE_DEADLINE_ET = (11, 15)      # do not start new rebalance after 11:15 AM ET

# =========================
# TURNOVER GOVERNOR + RANK STABILITY
# =========================
# Controls to reduce unnecessary trading churn and improve after-fee returns.
# Key insight: Excessive turnover kills returns via spreads, slippage, and taxes.

ENABLE_TURNOVER_GOVERNOR = False      # v8: Disabled - allows faster rotation into momentum leaders

# Max turnover per rebalance (% of portfolio that can change hands)
# Example: 0.25 means max 25% of portfolio trades per rebalance
MAX_TURNOVER_PER_REBALANCE = 0.25     # Cap rebalance turnover at 25%

# No-trade zone: skip rebalance entirely if drift < threshold
# Saves transaction costs when portfolio is close to target
NO_TRADE_DRIFT_THRESHOLD = 0.03       # Skip if max weight drift < 3%

# Rank stability: require N consecutive weeks in top ranks before adding
# Prevents buying on one lucky week's ranking
RANK_STABILITY_WEEKS = 2              # Must be in top N for 2 weeks to add
ENABLE_RANK_STABILITY = False         # v8: Disabled - enter top momentum positions immediately

# Trade buffer zone: don't trade unless delta exceeds threshold
# Prevents micro-trades that don't meaningfully improve allocation
WEIGHT_CHANGE_MIN_THRESHOLD = 0.02    # Ignore weight changes < 2%

# =========================
# DRIFT-BASED MINI-REBALANCE
# =========================
# Allow small intra-week rebalances when drift exceeds threshold.
# This catches large moves without waiting for weekly rebalance.
# Key: Only trades positions that drifted significantly, not full rebalance.

ENABLE_DRIFT_MINI_REBALANCE = True    # Enable drift-triggered mini-rebalance
DRIFT_TRIGGER_THRESHOLD = 0.08        # Trigger mini-rebalance if any position drifts > 8%
DRIFT_MIN_DAYS_SINCE_REBAL = 2        # Wait at least 2 days after last rebalance
DRIFT_MAX_TURNOVER = 0.10             # Cap mini-rebalance at 10% turnover
DRIFT_CHECK_INTERVAL_MIN = 60         # Check for drift every 60 minutes during market hours

# Risk controls (exact)
DRAWDOWN_TRIGGER = 0.30               # v8: 30% drawdown triggers risk reduction (was 0.10, raised for leveraged ETFs)
DRAWDOWN_COOLDOWN_DAYS = 10           # v8: reduce exposure for 10 trading days (was 20)
DRAWDOWN_EXPOSURE_MULT = 0.60         # v8: scale total exposure by 0.60 during cooldown (was 0.50)

# v42: Smooth Drawdown Scaling (Research-Backed)
# Instead of binary trigger, scale exposure smoothly based on drawdown depth
# Research: Smooth scaling reduces whipsaw and timing risk
USE_SMOOTH_DRAWDOWN_SCALING = True    # Enable smooth drawdown-based position sizing
DRAWDOWN_SCALE_START = 0.10           # v8: Start reducing exposure at 10% drawdown (was 3%, raised for leveraged ETFs)
DRAWDOWN_SCALE_FLOOR = 0.30           # v8: Minimum exposure multiplier at max drawdown (was 0.25)
DRAWDOWN_SCALE_MAX = 0.35             # v8: Max drawdown for floor (35% dd = 0.30x exposure, was 0.20)
# Formula: exposure = max(FLOOR, 1.0 - (drawdown - START) / (MAX - START) * (1.0 - FLOOR))
# At 3% dd: 1.0x, at 10% dd: 0.58x, at 15% dd: 0.42x, at 20% dd: 0.25x

# Regime filter (improved with hysteresis)
# When SPY is below SMA200 => "risk-off": reduce equity sleeve weight
RISK_OFF_EQUITY_MULT = 0.55           # equity weights multiplied by 0.55 in risk-off

# Hysteresis buffers to prevent flip-flopping at SMA crossover
# Risk-ON trigger:  close > SMA200 * (1 + REGIME_BUFFER_ON)  = must rally 2% above SMA to go risk-on
# Risk-OFF trigger: close < SMA200 * (1 - REGIME_BUFFER_OFF) = must fall 2% below SMA to go risk-off
REGIME_BUFFER_ON = 0.01               # v8: +1% above SMA to trigger risk-on (faster entry, was 0.02)
REGIME_BUFFER_OFF = 0.02              # -2% below SMA200 to trigger risk-off

# Legacy buffer (kept for per-asset trend signal, distinct from regime)
SMA_TREND_BUFFER_PCT = 0.02           # require close > SMA * 1.02 to be bullish (per-asset trend filter)

# Execution thresholds (exact)
MIN_TRADE_NOTIONAL_USD = 200.0        # ignore tiny adjustments
MIN_TRADE_SHARES = 0.01               # fractional shares supported for many ETFs
ALLOW_FRACTIONAL = True               # Alpaca supports fractional for many ETFs

# Price cache settings
PRICE_CACHE_DURATION_SEC = 60.0       # Cache prices for 60 seconds

# Position reconciliation threshold
POSITION_RECONCILIATION_THRESHOLD_PCT = 0.05  # Warn if actual vs expected weight differs by > 5%

# =========================
# DECISION HIERARCHY (prevents weekly/daily conflicts)
# =========================
# This bot has TWO decision layers: weekly rebalance + daily monitoring.
# To prevent conflicts (e.g., daily sells what weekly just bought), we enforce:
#
# WEEKLY REBALANCE (Fridays 3:50 PM ET):
#   - Full portfolio rebalancing to target weights
#   - Uses canonical spy_regime() with hysteresis for regime
#   - Applies profit tilt for extended positions (reduces target weight)
#   - Entry/exit decisions for all positions
#
# DAILY MONITORING (every 30 min during market hours):
#   - EXIT-ONLY (never buys or rebalances)
#   - Decision hierarchy (first matching rule wins):
#     1. Emergency gap-down exit (>8% intraday gap) - immediate
#     2. Close-based trend exit (2 consecutive CLOSES below SMA200) - close-based
#     3. Position drawdown exit (>15% loss from cost basis) - immediate
#   - Does NOT do mid-cycle profit-taking (handled at rebalance via tilt)
#   - Does NOT do regime-change sells (handled at rebalance via canonical regime)
#
# This design ensures:
#   - Weekly layer makes allocation decisions
#   - Daily layer only protects capital (never fights weekly)
#   - Regime state is consistent across both layers (single source of truth)
#   - Profit-taking reduces turnover by tilting at rebalance (not mid-cycle)

# =========================
# DAILY MONITORING (Risk Management)
# =========================
# Daily position monitoring runs during market hours to protect capital
# EXIT-ONLY: Does NOT rebalance or buy - only exits on risk triggers

ENABLE_DAILY_MONITORING = True        # Enable daily position checks (recommended)
DAILY_MONITORING_INTERVAL_MIN = 30    # Check positions every 30 minutes during market hours
DAILY_MONITORING_START_TIME = (10, 0) # Start monitoring at 10:00 AM ET (after open volatility)
DAILY_MONITORING_END_TIME = (15, 45)  # Stop monitoring at 3:45 PM ET (before close)

# Stop-loss triggers (exit full position)
# NOTE: "consecutive days" = consecutive DAILY CLOSES below threshold (not intraday touches)
STOP_LOSS_SMA200_DAYS = 2             # Exit if 2 consecutive daily CLOSES < SMA200 * buffer
STOP_LOSS_SMA200_BUFFER = 0.93        # v8: Exit if close < SMA * 0.93 (wider buffer for leveraged ETFs, was 0.98)
STOP_LOSS_POSITION_DD_PCT = 0.30      # v8: Exit if position down >30% from cost basis (wider for leveraged ETFs, was 0.15)

# Intraday emergency exit (separate from close-based trend exit)
# These trigger on live price, not daily close - for fast-moving situations only
INTRADAY_GAP_EXIT_PCT = 0.08          # Exit if live price gaps down >8% from yesterday's close
ENABLE_INTRADAY_GAP_EXIT = True       # Enable gap-down emergency exit

# Profit-taking: REBALANCE TILT (not mid-cycle sell)
# Instead of selling mid-cycle, we reduce target weight at rebalance time
PROFIT_TILT_EXTENSION_THRESHOLD = 0.20  # Start tilting target weight when >20% above SMA200
PROFIT_TILT_MAX_REDUCTION = 0.40        # Max reduction = 40% of base weight (e.g., 20% -> 12%)
PROFIT_TILT_K = 1.5                     # Tilt steepness: tilt = 1 - k*(extension - threshold)

# DEPRECATED: Mid-cycle profit-taking constants
# These are no longer used - profit-taking is now handled via profit_tilt at rebalance time.
# Kept for reference only - DO NOT use in new code.
# PROFIT_TAKE_EXTENSION_PCT = 0.20      # DEPRECATED
# PROFIT_TAKE_PORTION = 0.50            # DEPRECATED
# PROFIT_TAKE_MIN_NOTIONAL = 50.0       # DEPRECATED
# PROFIT_TAKE_COOLDOWN_HOURS = 24       # DEPRECATED

# Regime change (now handled by canonical spy_regime() with hysteresis)
# Daily monitoring uses same regime state as weekly rebalance - no separate "days" counter needed

# Data source
DATA_SOURCE = os.getenv("DATA_SOURCE", "alpaca").lower()  # "alpaca" or "polygon"

# Dry-run mode
DRY_RUN = os.getenv("DRY_RUN", "false").lower() in ("1", "true", "yes", "y")

# Paths (resolve relative names against DATA_DIR so they land in the local output dir)
_state_raw = os.getenv("BOT_STATE_PATH", "trend_vol_target_state.json")
_log_raw = os.getenv("BOT_LOG_PATH", "trend_vol_target_trades.csv")
STATE_PATH = _state_raw if os.path.isabs(_state_raw) else str(DATA_DIR / _state_raw)
LOG_PATH = _log_raw if os.path.isabs(_log_raw) else str(DATA_DIR / _log_raw)

# Performance tracking log — env-configurable to prevent live/paper cross-write
NOTES_DIR = _output_root / "project_notes"
NOTES_DIR.mkdir(exist_ok=True)
_rebalance_log_name = os.getenv("REBALANCE_LOG_PATH", "trend_bot_rebalances.csv")
_equity_snap_name = os.getenv("EQUITY_SNAPSHOT_PATH", "trend_bot_equity.csv")
REBALANCE_LOG_PATH = str(NOTES_DIR / _rebalance_log_name)
EQUITY_SNAPSHOT_PATH = str(NOTES_DIR / _equity_snap_name)

# Logging — env-configurable filename to prevent live/paper log collision
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
_log_file_name = os.getenv("BOT_LOG_FILE", "trend_bot.log")
MAX_LOG_SIZE_MB = 50
MAX_LOG_BACKUPS = 5

# ===========================
# LEVEL 3 PRODUCTION CONFIG
# ===========================

# Live Trading Safety
LIVE_TRADING_ENABLED = os.getenv("LIVE_TRADING", "0") == "1"
LIVE_TRADING_CONFIRMATION = os.getenv("I_UNDERSTAND_LIVE_TRADING", "").upper()

# Alerting
ENABLE_EMAIL_ALERTS = os.getenv("ENABLE_EMAIL_ALERTS", "0") == "1"
ENABLE_SLACK_ALERTS = os.getenv("ENABLE_SLACK_ALERTS", "0") == "1"
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "").strip()
ALERT_EMAIL_TO = os.getenv("ALERT_EMAIL_TO", "").strip()
ALERT_EMAIL_FROM = os.getenv("ALERT_EMAIL_FROM", "").strip()
SMTP_SERVER = os.getenv("SMTP_SERVER", "smtp.gmail.com").strip()
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USERNAME = os.getenv("SMTP_USERNAME", "").strip()
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "").strip()

# Circuit Breaker
MAX_API_FAILURES_PER_MIN = 5
API_FAILURE_WINDOW_SEC = 60

# Kill Switch
KILL_SWITCH_FILE = str(DATA_DIR / "KILL_SWITCH")
KILL_SWITCH_ENV = os.getenv("KILL_SWITCH", "0") == "1"

# Shutdown Policy
SHUTDOWN_POLICY = os.getenv("SHUTDOWN_POLICY", "CANCEL_ORDERS_ONLY").upper()


# =========================
# LOGGING SETUP
# =========================

def setup_logging() -> logging.Logger:
    """Setup Python logging framework with configurable level and rotation."""
    import sys
    logger = logging.getLogger("VolTargetBot")
    logger.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))

    # Console handler with UTF-8 encoding (fixes emoji display on Windows)
    console_handler = logging.StreamHandler(stream=sys.stdout)
    console_handler.setLevel(logging.INFO)
    # Reconfigure stdout for UTF-8 on Windows to support emoji characters
    if sys.platform == 'win32':
        try:
            sys.stdout.reconfigure(encoding='utf-8', errors='replace')
        except AttributeError:
            pass  # Python < 3.7 doesn't have reconfigure
    console_formatter = logging.Formatter(
        fmt='[%(asctime)s] %(levelname)s - %(message)s',
        datefmt='%H:%M:%S'
    )
    console_handler.setFormatter(console_formatter)

    # File handler with rotation (Level 3)
    file_handler = RotatingFileHandler(
        str(LOGS_DIR / _log_file_name),
        maxBytes=MAX_LOG_SIZE_MB * 1024 * 1024,
        backupCount=MAX_LOG_BACKUPS
    )
    file_handler.setLevel(logging.DEBUG)
    file_formatter = logging.Formatter(
        fmt='[%(asctime)s] %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    file_handler.setFormatter(file_formatter)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    return logger


log = setup_logging()


# =========================
# NETWORK HARDENING
# =========================

# Default timeout for HTTP requests (seconds)
HTTP_TIMEOUT_DEFAULT = 15
HTTP_TIMEOUT_SLACK = 10
HTTP_TIMEOUT_POLYGON = 30

# Create a retry-enabled session for HTTP requests
def create_retry_session(
    retries: int = 3,
    backoff_factor: float = 1.0,
    status_forcelist: tuple = (429, 500, 502, 503, 504),
    timeout: float = HTTP_TIMEOUT_DEFAULT
) -> requests.Session:
    """
    Create a requests Session with retry/backoff for transient errors.

    FIX: Network hardening - ensures consistent timeouts and retries
    across all HTTP requests (Slack, Polygon, etc.).

    Args:
        retries: Number of retry attempts
        backoff_factor: Exponential backoff factor (1.0 = 1s, 2s, 4s)
        status_forcelist: HTTP status codes to retry on
        timeout: Default timeout for requests

    Returns:
        Configured requests.Session
    """
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


# Global retry-enabled session (reuse for efficiency)
_http_session: Optional[requests.Session] = None


def get_http_session() -> requests.Session:
    """Get or create the global retry-enabled HTTP session."""
    global _http_session
    if _http_session is None:
        _http_session = create_retry_session()
    return _http_session


# ============================================================
# LEVEL 3 PRODUCTION COMPONENTS
# ============================================================

class Alerter:
    """Multi-channel alerting for unattended operation."""

    def __init__(self):
        self.slack_enabled = ENABLE_SLACK_ALERTS and SLACK_WEBHOOK_URL
        self.email_enabled = ENABLE_EMAIL_ALERTS and ALERT_EMAIL_TO

        if self.slack_enabled:
            log.info(f"[ALERTER] Slack alerts ENABLED")
        if self.email_enabled:
            log.info(f"[ALERTER] Email alerts ENABLED | to={ALERT_EMAIL_TO}")

        if not (self.slack_enabled or self.email_enabled):
            log.warning("[ALERTER] NO ALERTS CONFIGURED - unattended operation not recommended")

    def send_alert(self, level: str, title: str, message: str, context: dict = None):
        """Send alert via all enabled channels."""
        timestamp = datetime.now(ET).strftime("%Y-%m-%d %H:%M:%S ET")
        full_message = f"[{level}] {timestamp}\n\n{message}"

        if context:
            full_message += "\n\nContext:\n"
            for key, value in context.items():
                full_message += f"  {key}: {value}\n"

        if self.slack_enabled:
            self._send_slack(level, title, full_message)
        if self.email_enabled:
            self._send_email(level, title, full_message)

        # Always log locally
        if level == "CRITICAL":
            log.error(f"[ALERT] {title}: {message}")
        elif level == "WARNING":
            log.warning(f"[ALERT] {title}: {message}")
        else:
            log.info(f"[ALERT] {title}: {message}")

    def _send_slack(self, level: str, title: str, message: str):
        """Send Slack webhook alert."""
        try:
            color = {"INFO": "#36a64f", "WARNING": "#ff9900", "CRITICAL": "#ff0000"}.get(level, "#808080")
            payload = {
                "attachments": [{
                    "color": color,
                    "title": title,
                    "text": message,
                    "footer": "Trend Bot",
                    "ts": int(time.time())
                }]
            }
            # FIX: Use retry-enabled session for network hardening
            session = get_http_session()
            response = session.post(SLACK_WEBHOOK_URL, json=payload, timeout=HTTP_TIMEOUT_SLACK)
            response.raise_for_status()
            log.debug(f"[ALERTER] Slack alert sent: {title}")
        except Exception as e:
            log.error(f"[ALERTER] Failed to send Slack alert: {e}")

    def _send_email(self, level: str, title: str, message: str):
        """Send email alert via SMTP."""
        try:
            msg = MIMEMultipart()
            msg['From'] = ALERT_EMAIL_FROM
            msg['To'] = ALERT_EMAIL_TO
            msg['Subject'] = f"[{level}] Trend Bot: {title}"
            msg.attach(MIMEText(message, 'plain'))

            with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
                server.starttls()
                if SMTP_USERNAME and SMTP_PASSWORD:
                    server.login(SMTP_USERNAME, SMTP_PASSWORD)
                server.send_message(msg)
            log.debug(f"[ALERTER] Email alert sent: {title}")
        except Exception as e:
            log.error(f"[ALERTER] Failed to send email alert: {e}")


class CircuitBreaker:
    """API degradation detection and automatic halt."""

    def __init__(self):
        self.api_failures: Dict[str, List[float]] = {}
        self.halted = False
        self.halt_reason = None
        self._lock = threading.Lock()

    def record_api_failure(self, api_name: str):
        """Record an API failure and check if circuit should break."""
        with self._lock:
            now = time.time()
            if api_name not in self.api_failures:
                self.api_failures[api_name] = []

            self.api_failures[api_name].append(now)

            # Clean old failures outside window
            cutoff = now - API_FAILURE_WINDOW_SEC
            self.api_failures[api_name] = [ts for ts in self.api_failures[api_name] if ts >= cutoff]

            # Check if we've exceeded threshold
            failure_count = len(self.api_failures[api_name])
            if failure_count >= MAX_API_FAILURES_PER_MIN:
                if not self.halted:
                    self.halted = True
                    self.halt_reason = f"{api_name} API degradation ({failure_count} failures in {API_FAILURE_WINDOW_SEC}s)"
                    log.error(f"[CIRCUIT_BREAKER] HALTED | {self.halt_reason}")

                    alerter.send_alert(
                        level="CRITICAL",
                        title="Circuit Breaker Tripped",
                        message=f"API failures exceeded threshold. Trading HALTED: {self.halt_reason}",
                        context={"api_name": api_name, "failure_count": failure_count}
                    )

    def is_halted(self) -> bool:
        return self.halted

    def get_halt_reason(self) -> Optional[str]:
        return self.halt_reason

    def reset(self):
        with self._lock:
            self.halted = False
            self.halt_reason = None
            self.api_failures.clear()
            log.info("[CIRCUIT_BREAKER] Reset - trading resumed")


class KillSwitch:
    """Emergency halt mechanism."""

    def is_triggered(self) -> Tuple[bool, Optional[str]]:
        """Check if kill switch is triggered."""
        if KILL_SWITCH_ENV:
            return True, "KILL_SWITCH environment variable set"
        if os.path.exists(KILL_SWITCH_FILE):
            return True, f"{KILL_SWITCH_FILE} file detected"
        return False, None

    def execute_emergency_shutdown(self, trading_client):
        """Execute emergency shutdown.

        SHARED-ACCOUNT SAFETY: Only cancels trend_bot's own orders (TBOT_ prefix)
        and only closes positions in trend_bot's universe (ALL_TICKERS).
        Preserves simple_bot's and directional_bot's orders/positions.
        """
        log.error("[KILL_SWITCH] EMERGENCY SHUTDOWN INITIATED")
        try:
            if not DRY_RUN:
                # Cancel only TBOT_ prefixed orders (not other bots' orders)
                try:
                    all_orders = trading_client.get_orders()
                    our_orders = [o for o in all_orders
                                 if (o.client_order_id or "").startswith("TBOT_")]
                    for order in our_orders:
                        try:
                            trading_client.cancel_order_by_id(order.id)
                        except Exception:
                            pass
                    log.warning(f"[KILL_SWITCH] Cancelled {len(our_orders)} of our open orders "
                               f"(preserved {len(all_orders) - len(our_orders)} other bot orders)")
                except Exception as e:
                    log.error(f"[KILL_SWITCH] Error cancelling orders: {e}")
                    # Fallback: cancel all if filtered cancel fails
                    trading_client.cancel_orders()

                if SHUTDOWN_POLICY == "FLATTEN_ALL":
                    positions = trading_client.get_all_positions()
                    our_positions = [p for p in positions if p.symbol in ALL_TICKERS]
                    for pos in our_positions:
                        trading_client.close_position(pos.symbol)
                    log.warning(f"[KILL_SWITCH] Closed {len(our_positions)} of our positions "
                               f"(preserved {len(positions) - len(our_positions)} other bot positions)")
        except Exception as e:
            log.error(f"[KILL_SWITCH] Error during emergency shutdown: {e}")


# Global instances
alerter = Alerter()
circuit_breaker = CircuitBreaker()
kill_switch = KillSwitch()


# =========================
# STATE / LOGGING
# =========================

@dataclass
class BotState:
    last_rebalance_date_iso: Optional[str] = None
    equity_peak: Optional[float] = None
    last_equity: Optional[float] = None
    drawdown_cooldown_until_iso: Optional[str] = None
    last_target_weights: Optional[Dict[str, float]] = None
    rebalance_in_progress: bool = False  # Track rebalance state to prevent race condition
    rebalance_started_at_iso: Optional[str] = None  # Timestamp when rebalance started (for stale detection)
    last_daily_monitoring_timestamp: Optional[float] = None  # Unix timestamp of last daily monitoring run
    last_drift_mini_iso: Optional[str] = None  # ISO timestamp of last drift mini-rebalance attempt

    # CANONICAL REGIME STATE (with hysteresis)
    # "risk_on" or "risk_off" - persists until opposite trigger is hit
    spy_regime: str = "risk_on"  # Current regime state (default risk_on)
    spy_regime_changed_date_iso: Optional[str] = None  # Date regime last changed

    # CLOSE-BASED STOP TRACKING
    # symbol -> list of recent daily closes (most recent N days)
    # Used to check "2 consecutive closes below SMA200"
    position_daily_closes: Optional[Dict[str, List[float]]] = None

    # RANK STABILITY TRACKING
    # symbol -> list of ISO date strings when symbol was in top N
    # Used to require N consecutive weeks in top ranks before adding
    rank_history: Optional[Dict[str, List[str]]] = None

    # WASH SALE TRACKING
    # {symbol: ISO_date_of_loss_sale} — triggers 31-day cooldown on re-buying that symbol
    loss_sales: Optional[Dict[str, str]] = None
    # {substitute_sym: original_sym} — tracks which substitutes are standing in for originals
    active_substitutions: Optional[Dict[str, str]] = None

    # Legacy fields (deprecated but kept for state file compatibility)
    position_stop_triggers: Optional[Dict[str, int]] = None  # DEPRECATED: was intraday-based
    spy_regime_trigger_days: int = 0  # DEPRECATED: regime now uses hysteresis
    last_regime_change_date_iso: Optional[str] = None  # DEPRECATED: now spy_regime_changed_date_iso
    profit_take_cooldowns: Optional[Dict[str, str]] = None  # symbol -> ISO timestamp of last profit-take
    last_daily_signal_date_iso: Optional[str] = None  # Last date we recorded daily closes


def load_state(path: str) -> BotState:
    if not os.path.exists(path):
        log.info("No existing state file found. Starting fresh.")
        return BotState()
    try:
        with open(path, "r", encoding="utf-8") as f:
            raw = json.load(f)

        # Filter out unknown keys for backward compatibility
        from dataclasses import fields
        allowed = {f.name for f in fields(BotState)}
        filtered = {k: v for k, v in raw.items() if k in allowed}

        dropped = sorted(set(raw.keys()) - allowed)
        if dropped:
            log.warning(f"[STATE] Dropped unknown keys from state: {dropped}")

        log.info(f"Loaded state from {path}")
        return BotState(**filtered)
    except Exception as e:
        log.error(f"Failed to load state from {path}: {e}")
        raise


# Stale rebalance threshold (minutes) - clear flag if older than this
STALE_REBALANCE_THRESHOLD_MIN = 30


def clear_stale_rebalance_flag(state: BotState, state_path: str) -> None:
    """
    Clear rebalance_in_progress flag if it's stale (from a crash or hard-kill).

    If rebalance_in_progress is True but the timestamp is older than threshold
    (or from a different day), the flag is considered stale and cleared.
    This prevents deadlocking rebalance on restart after a crash.
    """
    if not state.rebalance_in_progress:
        return

    # If no timestamp, flag is definitely stale
    if not state.rebalance_started_at_iso:
        log.warning("[STATE] rebalance_in_progress=True but no timestamp; clearing as stale")
        state.rebalance_in_progress = False
        state.rebalance_started_at_iso = None
        save_state(state_path, state)
        return

    # Parse the timestamp
    try:
        started_at = datetime.fromisoformat(state.rebalance_started_at_iso)
        # Ensure timezone awareness
        if started_at.tzinfo is None:
            started_at = ET.localize(started_at)

        now = now_et()
        elapsed_minutes = (now - started_at).total_seconds() / 60.0

        # Clear if older than threshold OR from a different day
        is_stale = elapsed_minutes > STALE_REBALANCE_THRESHOLD_MIN
        is_different_day = started_at.date() != now.date()

        if is_stale or is_different_day:
            reason = "older than threshold" if is_stale else "from different day"
            log.warning(f"[STATE] rebalance_in_progress=True but {reason} "
                       f"(started {elapsed_minutes:.1f}min ago); clearing as stale")
            state.rebalance_in_progress = False
            state.rebalance_started_at_iso = None
            save_state(state_path, state)
        else:
            log.warning(f"[STATE] rebalance_in_progress=True (started {elapsed_minutes:.1f}min ago); "
                       f"keeping flag - rebalance may have been interrupted")
    except Exception as e:
        log.warning(f"[STATE] Failed to parse rebalance timestamp: {e}; clearing flag as stale")
        state.rebalance_in_progress = False
        state.rebalance_started_at_iso = None
        save_state(state_path, state)


def save_state(path: str, state: BotState) -> None:
    """Atomically save state by writing to temp file then renaming."""
    temp_path = path + ".tmp"
    try:
        with open(temp_path, "w", encoding="utf-8") as f:
            json.dump(asdict(state), f, indent=2, sort_keys=True)
        # Atomic replace (works on both POSIX and Windows)
        os.replace(temp_path, path)
        log.debug(f"State saved to {path}")
    except Exception as e:
        log.error(f"Failed to save state to {path}: {e}")
        raise


def is_wash_sale_cooldown_active(loss_sales: Optional[Dict[str, str]], symbol: str) -> bool:
    """Check if symbol has an active wash sale cooldown (sold at loss within 31 days)."""
    if not loss_sales or symbol not in loss_sales:
        return False
    sale_date = datetime.fromisoformat(loss_sales[symbol])
    if hasattr(sale_date, 'date'):
        sale_date = sale_date.date() if isinstance(sale_date, datetime) else sale_date
    else:
        sale_date = datetime.strptime(loss_sales[symbol][:10], "%Y-%m-%d").date()
    days_since = (datetime.now(ET).date() - sale_date).days
    return days_since < WASH_SALE_COOLDOWN_DAYS


def clean_expired_wash_sales(state: BotState) -> int:
    """Remove expired loss_sales and active_substitutions entries. Returns count removed."""
    removed = 0
    if state.loss_sales:
        expired = [sym for sym in state.loss_sales
                   if not is_wash_sale_cooldown_active(state.loss_sales, sym)]
        for sym in expired:
            del state.loss_sales[sym]
            removed += 1
        if not state.loss_sales:
            state.loss_sales = None
    if state.active_substitutions:
        expired_subs = [sub for sub, orig in state.active_substitutions.items()
                        if not is_wash_sale_cooldown_active(state.loss_sales, orig)]
        for sub in expired_subs:
            del state.active_substitutions[sub]
        if not state.active_substitutions:
            state.active_substitutions = None
    return removed


def ensure_log_header(path: str) -> None:
    if os.path.exists(path):
        return
    try:
        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow([
                "timestamp_et",
                "action",
                "symbol",
                "qty",
                "est_price",
                "notional",
                "reason",
                "target_weight",
                "current_weight",
                "portfolio_equity",
                "order_id"
            ])
        log.debug(f"Created trade log header at {path}")
    except Exception as e:
        log.error(f"Failed to create log header: {e}")
        raise


# Lock for atomic CSV writes
_csv_lock = threading.Lock()


def generate_client_order_id(reason: str, symbol: str, side: str, date_str: Optional[str] = None) -> str:
    """
    Generate a unique client_order_id for order submission.

    The ID includes a timestamp component to ensure uniqueness across multiple
    rebalances on the same day, while still providing short-term idempotency
    protection (orders submitted within the same second get the same ID).

    Format: {prefix}_{date}_{HHMMSS}_{reason}_{symbol}_{side}
    Example: TBOT_2025-01-15_154532_reb_SPY_BUY

    Args:
        reason: Order reason (e.g., "rebalance", "daily_monitor_stop", "profit_take")
        symbol: Ticker symbol
        side: "BUY" or "SELL"
        date_str: Date string (defaults to today in ET)

    Returns:
        Unique client_order_id string (max 48 chars for Alpaca)
    """
    if date_str is None:
        date_str = today_iso_et()

    # Get current time in ET for uniqueness across same-day rebalances
    et_now = datetime.now(ET)
    time_suffix = et_now.strftime("%H%M%S")

    # Abbreviate reason to save chars (max 4 chars)
    reason_abbrev = reason[:4] if reason else "ord"

    # Create a unique ID with timestamp
    # Format: TBOT_YYYY-MM-DD_HHMMSS_reason_SYMBOL_SIDE
    base = f"TBOT_{date_str}_{time_suffix}_{reason_abbrev}_{symbol}_{side}"

    # Alpaca requires <= 48 chars; hash if too long
    if len(base) <= 48:
        return base

    # If too long, use hash suffix instead
    h = hashlib.md5(base.encode()).hexdigest()[:8]
    return f"TBOT_{date_str}_{time_suffix}_{symbol}_{h}"


def log_trade(
    path: str,
    action: str,
    symbol: str,
    qty: float,
    est_price: float,
    notional: float,
    reason: str,
    target_weight: float,
    current_weight: float,
    portfolio_equity: float,
    order_id: Optional[str] = None
) -> None:
    """Atomically log trade to CSV."""
    ensure_log_header(path)
    with _csv_lock:
        try:
            with open(path, "a", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                w.writerow([
                    now_et().strftime("%Y-%m-%d %H:%M:%S"),
                    action,
                    symbol,
                    f"{qty:.6f}",
                    f"{est_price:.4f}",
                    f"{notional:.2f}",
                    reason,
                    f"{target_weight:.6f}",
                    f"{current_weight:.6f}",
                    f"{portfolio_equity:.2f}",
                    order_id or ""
                ])
            log.debug(f"Logged trade: {action} {qty:.4f} {symbol} @ ${est_price:.2f}")
        except Exception as e:
            log.error(f"Failed to log trade: {e}")


def _ensure_rebalance_log_header() -> None:
    """Create rebalance summary log header if file doesn't exist."""
    if os.path.exists(REBALANCE_LOG_PATH):
        return
    try:
        with open(REBALANCE_LOG_PATH, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow([
                "date",
                "time_et",
                "event",
                "regime",
                "spy_price",
                "spy_vs_sma200_pct",
                "equity",
                "equity_peak",
                "drawdown_pct",
                "exposure_mult",
                "capital_usage_pct",
                "deployable_capital",
                "cash_reserve",
                "num_positions",
                "positions_json",
                "top_weights",
                "risk_score",
                "event_mult",
                "turnover_pct",
                "orders_placed",
                "notes"
            ])
        log.debug(f"Created rebalance log header at {REBALANCE_LOG_PATH}")
    except Exception as e:
        log.error(f"Failed to create rebalance log header: {e}")


def log_rebalance_summary(
    event: str,
    regime: str,
    spy_price: float,
    spy_vs_sma200_pct: float,
    equity: float,
    equity_peak: float,
    drawdown_pct: float,
    exposure_mult: float,
    capital_usage_pct: float,
    deployable_capital: float,
    cash_reserve: float,
    positions: Dict[str, float],
    target_weights: Dict[str, float],
    risk_score: float,
    event_mult: float,
    turnover_pct: float,
    orders_placed: int,
    notes: str = ""
) -> None:
    """
    Log rebalance summary to CSV for performance tracking.

    This creates a single row per rebalance event with all key metrics needed
    for post-hoc analysis of strategy performance.
    """
    _ensure_rebalance_log_header()

    # Format positions as compact JSON
    positions_json = json.dumps({k: round(v, 4) for k, v in positions.items() if v > 0.001})

    # Get top 5 weights for quick view
    sorted_weights = sorted(target_weights.items(), key=lambda x: x[1], reverse=True)
    top_weights = ", ".join([f"{sym}:{w:.1%}" for sym, w in sorted_weights[:5] if w > 0.01])

    with _csv_lock:
        try:
            with open(REBALANCE_LOG_PATH, "a", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                w.writerow([
                    now_et().strftime("%Y-%m-%d"),
                    now_et().strftime("%H:%M:%S"),
                    event,
                    regime,
                    f"{spy_price:.2f}",
                    f"{spy_vs_sma200_pct:.2f}",
                    f"{equity:.2f}",
                    f"{equity_peak:.2f}",
                    f"{drawdown_pct:.2f}",
                    f"{exposure_mult:.2f}",
                    f"{capital_usage_pct:.2f}",
                    f"{deployable_capital:.2f}",
                    f"{cash_reserve:.2f}",
                    len([k for k, v in positions.items() if v > 0.001]),
                    positions_json,
                    top_weights,
                    f"{risk_score:.1f}",
                    f"{event_mult:.2f}",
                    f"{turnover_pct:.2f}",
                    orders_placed,
                    notes
                ])
            log.info(f"Logged rebalance summary: {event} | Equity=${equity:.2f} | DD={drawdown_pct:.1f}%")
        except Exception as e:
            log.error(f"Failed to log rebalance summary: {e}")


def _ensure_equity_snapshot_header() -> None:
    """Create equity snapshot CSV header if file doesn't exist."""
    if os.path.exists(EQUITY_SNAPSHOT_PATH):
        return
    try:
        with open(EQUITY_SNAPSHOT_PATH, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow([
                "date", "equity", "equity_peak", "drawdown_pct",
                "positions_value", "cash", "num_positions",
                "regime", "spy_price", "positions_json"
            ])
        log.debug(f"Created equity snapshot header at {EQUITY_SNAPSHOT_PATH}")
    except Exception as e:
        log.error(f"Failed to create equity snapshot header: {e}")


def log_equity_snapshot(
    date_str: str,
    equity: float,
    equity_peak: float,
    positions: Dict[str, Any],
    regime: str,
    spy_price: float
) -> None:
    """
    Append a daily equity snapshot row. Called once per trading day from daily monitoring.

    Provides a continuous equity curve even when rebalances fail or are skipped.
    """
    _ensure_equity_snapshot_header()

    positions_value = sum(
        float(p.get("market_value", 0)) if isinstance(p, dict) else 0
        for p in positions.values()
    )
    cash = equity - positions_value
    num_positions = sum(1 for k in positions if k != CASH_TICKER)
    drawdown_pct = ((equity_peak - equity) / equity_peak * 100) if equity_peak > 0 else 0.0

    # Compact position weights
    weights = {}
    for sym, p in positions.items():
        if sym == CASH_TICKER:
            continue
        mv = float(p.get("market_value", 0)) if isinstance(p, dict) else 0
        if equity > 0 and mv > 0:
            weights[sym] = round(mv / equity, 4)
    positions_json = json.dumps(weights)

    with _csv_lock:
        try:
            with open(EQUITY_SNAPSHOT_PATH, "a", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                w.writerow([
                    date_str,
                    f"{equity:.2f}",
                    f"{equity_peak:.2f}",
                    f"{drawdown_pct:.2f}",
                    f"{positions_value:.2f}",
                    f"{cash:.2f}",
                    num_positions,
                    regime,
                    f"{spy_price:.2f}",
                    positions_json
                ])
            log.info(f"[EQUITY_SNAPSHOT] {date_str}: ${equity:.2f} | "
                     f"DD={drawdown_pct:.1f}% | {num_positions} positions | {regime}")
        except Exception as e:
            log.error(f"Failed to log equity snapshot: {e}")


# =========================
# TIME / CALENDAR
# =========================

def now_et() -> datetime:
    return datetime.now(tz=ET)


def is_rebalance_window(dt: datetime) -> bool:
    """
    Check if current time is within rebalance window.

    NOTE: This is the fallback check using hard-coded times. For production,
    use is_rebalance_window_dynamic() which handles early-close days.
    """
    if dt.weekday() != REBALANCE_WEEKDAY:
        return False
    hh, mm = dt.hour, dt.minute
    start_h, start_m = REBALANCE_TIME_ET
    end_h, end_m = REBALANCE_DEADLINE_ET
    return (hh, mm) >= (start_h, start_m) and (hh, mm) <= (end_h, end_m)


def is_rebalance_window_dynamic(trading_client: TradingClient) -> bool:
    """
    Check if current time is within rebalance window using Alpaca clock API.

    Uses the configured REBALANCE_TIME_ET and REBALANCE_DEADLINE_ET settings.
    Verifies market is open via Alpaca clock before allowing rebalance.

    Args:
        trading_client: Alpaca TradingClient for clock API

    Returns:
        True if we're in the rebalance window, False otherwise
    """
    now_dt = now_et()

    # Only rebalance on configured weekday (typically Friday)
    if now_dt.weekday() != REBALANCE_WEEKDAY:
        return False

    try:
        clock = trading_client.get_clock()

        # Check if market is open
        if not clock.is_open:
            return False

        # Use static window times (11:00 AM - 11:15 AM ET)
        start_h, start_m = REBALANCE_TIME_ET
        end_h, end_m = REBALANCE_DEADLINE_ET

        current_time = now_dt.time()
        window_start = dt_time(start_h, start_m)
        window_end = dt_time(end_h, end_m)

        in_window = window_start <= current_time <= window_end

        if in_window:
            log.debug(f"[REBALANCE] In window: {start_h:02d}:{start_m:02d} - {end_h:02d}:{end_m:02d} ET")

        return in_window

    except Exception as e:
        log.warning(f"Failed to get clock for rebalance window check: {e}. "
                   f"Falling back to static window.")
        # Fallback to static check
        return is_rebalance_window(now_dt)


def today_iso_et() -> str:
    return now_et().date().isoformat()


def parse_iso_date(s: Optional[str]) -> Optional[date]:
    if not s:
        return None
    try:
        return datetime.fromisoformat(s).date()
    except Exception as e:
        log.warning(f"Failed to parse ISO date '{s}': {e}")
        return None


def get_trading_calendar(trading_client: TradingClient, start: date, end: date) -> List[date]:
    """Get exact trading days from Alpaca calendar API."""
    try:
        calendar = trading_client.get_calendar(start=start.isoformat(), end=end.isoformat())
        return [cal.date for cal in calendar]
    except Exception as e:
        log.warning(f"Failed to fetch Alpaca calendar: {e}. Falling back to approximation.")
        return []


def add_trading_days_exact(trading_client: TradingClient, d: date, n_days: int) -> date:
    """Add/subtract exact trading days using Alpaca calendar."""
    # Fetch calendar for a generous window
    if n_days >= 0:
        start = d
        end = d + timedelta(days=n_days * 2 + 30)
    else:
        start = d + timedelta(days=n_days * 2 - 30)
        end = d

    cal = get_trading_calendar(trading_client, start, end)
    if not cal:
        log.warning("Calendar unavailable, using approximate method.")
        return add_trading_days_approx(d, n_days)

    # Find current date index
    try:
        idx = cal.index(d)
    except ValueError:
        # d not in calendar, find nearest
        cal_sorted = sorted(cal)
        idx = next((i for i, cd in enumerate(cal_sorted) if cd >= d), len(cal_sorted) - 1)
        log.debug(f"Date {d} not in calendar, using index {idx}")

    target_idx = idx + n_days
    if 0 <= target_idx < len(cal):
        return cal[target_idx]
    else:
        log.warning(f"Trading day calculation out of bounds. Using approximation.")
        return add_trading_days_approx(d, n_days)


def add_trading_days_approx(d: date, n_days: int) -> date:
    """Approximation (good enough for cooldown). Skips weekends only."""
    step = 1 if n_days >= 0 else -1
    remaining = abs(n_days)
    cur = d
    while remaining > 0:
        cur = cur + timedelta(days=step)
        if cur.weekday() < 5:
            remaining -= 1
    return cur


# =========================
# PRICE CACHE
# =========================

@dataclass
class PriceCache:
    """Cache for latest prices to avoid redundant API calls."""
    prices: Dict[str, Tuple[float, float]] = None  # symbol -> (price, timestamp)
    cache_duration: float = PRICE_CACHE_DURATION_SEC

    def __post_init__(self):
        if self.prices is None:
            self.prices = {}

    def get_price(self, symbol: str, data_client: StockHistoricalDataClient) -> float:
        """Get cached price or fetch if stale."""
        now = time.time()
        if symbol in self.prices:
            price, ts = self.prices[symbol]
            if (now - ts) < self.cache_duration:
                log.debug(f"Cache hit for {symbol}: ${price:.2f}")
                return price

        # Cache miss or stale - fetch
        log.debug(f"Cache miss for {symbol}, fetching...")
        price = get_latest_price_alpaca(data_client, symbol)
        self.prices[symbol] = (price, now)
        return price

    def batch_update(self, prices: Dict[str, float]) -> None:
        """Batch update cache with multiple prices."""
        now = time.time()
        for symbol, price in prices.items():
            self.prices[symbol] = (price, now)
        log.debug(f"Batch updated {len(prices)} prices in cache")

    def clear(self) -> None:
        """Clear all cached prices."""
        self.prices.clear()
        log.debug("Price cache cleared")


# Global price cache instance
price_cache = PriceCache()


# =========================
# DATA CACHE
# =========================

@dataclass
class DataCache:
    """Cache for daily bars to avoid redundant fetches.

    Re-fetches when the caller needs more tickers or a longer lookback
    than what is currently cached.
    """
    bars: Optional[pd.DataFrame] = None
    cache_date: Optional[str] = None
    cached_tickers: Optional[set] = None
    cached_lookback: int = 0

    def get_bars(
        self,
        data_client: StockHistoricalDataClient,
        tickers: List[str],
        lookback_days: int
    ) -> pd.DataFrame:
        """Get cached bars or fetch if stale / insufficient."""
        today = today_iso_et()
        requested = set(tickers)

        if (
            self.bars is not None
            and self.cache_date == today
            and self.cached_tickers is not None
            and requested.issubset(self.cached_tickers)
            and lookback_days <= self.cached_lookback
        ):
            log.debug(f"Data cache hit for {today}")
            return self.bars

        # Determine superset of tickers (union of cached + requested)
        fetch_tickers = sorted(
            requested | (self.cached_tickers or set())
        ) if self.cache_date == today else sorted(requested)
        fetch_lookback = max(lookback_days, self.cached_lookback) if self.cache_date == today else lookback_days

        log.info(f"Data cache miss, fetching {fetch_lookback} days of data for {len(fetch_tickers)} tickers...")

        end_dt_utc = now_et().astimezone(pytz.UTC)
        start_dt_utc = (now_et() - timedelta(days=fetch_lookback)).astimezone(pytz.UTC)

        if DATA_SOURCE == "polygon":
            bars = fetch_daily_bars_polygon(
                tickers=fetch_tickers,
                start_date=(now_et().date() - timedelta(days=fetch_lookback)),
                end_date=now_et().date()
            )
        else:
            bars = fetch_daily_bars_alpaca(
                data_client=data_client,
                tickers=fetch_tickers,
                start=start_dt_utc,
                end=end_dt_utc
            )
            # reduce columns for speed
            bars = bars[["timestamp", "symbol", "close"]]

        self.bars = bars
        self.cache_date = today
        self.cached_tickers = set(fetch_tickers)
        self.cached_lookback = fetch_lookback
        log.info(f"Data cached for {today}: {len(bars)} bars, {len(fetch_tickers)} tickers, {fetch_lookback}d lookback")
        return bars


# Global data cache instance
data_cache = DataCache()


# =========================
# DATA FETCH
# =========================

def fetch_daily_bars_alpaca(
    data_client: StockHistoricalDataClient,
    tickers: List[str],
    start: datetime,
    end: datetime
) -> pd.DataFrame:
    """Fetch daily bars from Alpaca."""
    try:
        req = StockBarsRequest(
            symbol_or_symbols=tickers,
            timeframe=TimeFrame.Day,
            start=start,
            end=end,
            adjustment="all"
        )
        bars = data_client.get_stock_bars(req).df
        # Expected columns: open, high, low, close, volume, trade_count, vwap
        # Index often: timestamp + symbol (MultiIndex)
        if bars.empty:
            raise RuntimeError("No bars returned from Alpaca.")
        if isinstance(bars.index, pd.MultiIndex):
            bars = bars.reset_index()
        # Normalize to columns: timestamp, symbol, close
        bars["timestamp"] = pd.to_datetime(bars["timestamp"], utc=True).dt.tz_convert(ET).dt.date
        bars = bars.sort_values(["symbol", "timestamp"])
        log.debug(f"Fetched {len(bars)} bars from Alpaca")
        return bars
    except Exception as e:
        log.error(f"Failed to fetch daily bars from Alpaca: {e}")
        raise


POLYGON_RATE_LIMIT_BATCH = 10  # Stocks Advanced = unlimited; batch for progress logging
POLYGON_RATE_LIMIT_SLEEP = 1   # minimal courtesy pause between batches


def fetch_daily_bars_polygon(
    tickers: List[str],
    start_date: date,
    end_date: date
) -> pd.DataFrame:
    """Fetch daily bars from Polygon with rate limit pacing."""
    api_key = os.getenv("POLYGON_API_KEY", "")
    if not api_key:
        raise RuntimeError("POLYGON_API_KEY not set but DATA_SOURCE='polygon'.")
    rows = []
    failed_symbols = []
    session = get_http_session()

    for i, sym in enumerate(tickers):
        # Batch pacing: brief pause for connection courtesy (Stocks Advanced = unlimited)
        if i > 0 and i % POLYGON_RATE_LIMIT_BATCH == 0:
            log.debug(f"[POLYGON] Rate limit pause after {i}/{len(tickers)} symbols...")
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
                    "close": float(item["c"]),
                })
            if not results:
                failed_symbols.append(sym)
                log.warning(f"[POLYGON] No bars returned for {sym}")
        except Exception as e:
            failed_symbols.append(sym)
            log.error(f"Failed to fetch Polygon data for {sym}: {e}")
            # Continue instead of crashing — partial data is better than no data
            continue

    if failed_symbols:
        log.warning(f"[POLYGON] Failed/empty for {len(failed_symbols)}/{len(tickers)} symbols: "
                    f"{failed_symbols[:10]}{'...' if len(failed_symbols) > 10 else ''}")

    df = pd.DataFrame(rows)
    if df.empty:
        raise RuntimeError("No bars returned from Polygon.")
    df = df.sort_values(["symbol", "timestamp"])
    log.info(f"Fetched {len(df)} bars from Polygon for "
             f"{len(df['symbol'].unique())}/{len(tickers)} symbols")
    return df


def fetch_real_vix() -> Optional[float]:
    """Fetch real VIX value from Polygon Indices Snapshot API.

    Requires Polygon Indices Starter plan or higher.
    Falls back to None if unavailable (caller should use realized vol estimate).
    """
    api_key = os.getenv("POLYGON_API_KEY", "")
    if not api_key:
        return None
    try:
        session = get_http_session()
        resp = session.get(
            "https://api.polygon.io/v3/snapshot/indices",
            params={"ticker.any_of": "I:VIX", "apiKey": api_key},
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
        results = data.get("results", [])
        if results:
            vix_val = results[0].get("value")
            if vix_val is not None:
                log.info(f"[VIX] Real VIX fetched from Polygon: {vix_val:.2f}")
                return float(vix_val)
    except Exception as e:
        log.info(f"[VIX] Real VIX fetch failed (using estimate): {e}")
    return None


def fetch_real_spx_data(period: int = 200) -> Optional[Dict[str, float]]:
    """Fetch real S&P 500 index value and SMA from Polygon Indices API.

    Requires Polygon Indices Starter plan or higher.
    Returns dict with 'value' and 'sma' keys, or None if unavailable.
    """
    api_key = os.getenv("POLYGON_API_KEY", "")
    if not api_key:
        return None
    try:
        session = get_http_session()
        # Fetch current index value
        snap_resp = session.get(
            "https://api.polygon.io/v3/snapshot/indices",
            params={"ticker.any_of": "I:SPX", "apiKey": api_key},
            timeout=10,
        )
        snap_resp.raise_for_status()
        snap_data = snap_resp.json()
        results = snap_data.get("results", [])
        if not results:
            return None
        spx_value = results[0].get("value")
        if spx_value is None:
            return None

        # Fetch SMA from Polygon technical indicators
        sma_resp = session.get(
            f"https://api.polygon.io/v1/indicators/sma/I:SPX",
            params={
                "timespan": "day",
                "window": period,
                "series_type": "close",
                "order": "desc",
                "limit": 1,
                "apiKey": api_key,
            },
            timeout=10,
        )
        sma_resp.raise_for_status()
        sma_data = sma_resp.json()
        sma_results = sma_data.get("results", {}).get("values", [])
        sma_val = sma_results[0].get("value") if sma_results else None

        result = {"value": float(spx_value)}
        if sma_val is not None:
            result["sma"] = float(sma_val)
            pct_vs_sma = (spx_value - sma_val) / sma_val * 100
            result["pct_vs_sma"] = round(pct_vs_sma, 2)
            log.info(f"[INDEX] Real S&P 500: {spx_value:.2f}, SMA{period}: {sma_val:.2f} "
                     f"({pct_vs_sma:+.2f}%)")
        return result

    except Exception as e:
        log.info(f"[INDEX] Real S&P 500 fetch failed: {e}")
    return None


def drop_incomplete_daily_bar(
    close_series: pd.Series,
    trading_client: Optional[TradingClient] = None
) -> pd.Series:
    """
    Strip today's bar if market is currently open (bar is incomplete).

    During market hours, today's daily bar is only partial and can distort
    SMA/RSI/other indicator calculations. Use only completed bars for signal logic,
    and fetch live snapshot prices separately for current price comparisons.

    Args:
        close_series: Time-indexed Series of close prices
        trading_client: If provided, uses Alpaca clock API; otherwise falls back to time-based check

    Returns:
        Series with today's bar removed if market is open, otherwise unchanged
    """
    if close_series.empty:
        return close_series

    # Check if market is open
    market_open = False
    if trading_client is not None:
        try:
            clock = trading_client.get_clock()
            market_open = clock.is_open
        except Exception:
            # Fallback to time-based check
            dt_now = now_et()
            if dt_now.weekday() < 5:
                market_open = (9, 30) <= (dt_now.hour, dt_now.minute) < (16, 0)
    else:
        # No client provided - use time-based check
        dt_now = now_et()
        if dt_now.weekday() < 5:
            market_open = (9, 30) <= (dt_now.hour, dt_now.minute) < (16, 0)

    if not market_open:
        return close_series

    # Market is open - check if last bar is today
    today_date = now_et().date()
    last_bar_date = close_series.index[-1].date() if hasattr(close_series.index[-1], 'date') else None

    if last_bar_date == today_date:
        log.debug(f"Dropping incomplete daily bar for {today_date} (market is open)")
        return close_series.iloc[:-1]

    return close_series


def get_close_series(
    bars: pd.DataFrame,
    symbol: str,
    strip_incomplete: bool = False,
    trading_client: Optional[TradingClient] = None
) -> pd.Series:
    """
    Extract close price series for a symbol.

    Args:
        bars: DataFrame with bars data
        symbol: Ticker symbol
        strip_incomplete: If True, remove today's bar when market is open (for indicator calculations)
        trading_client: Required if strip_incomplete=True, for market hours check

    Returns:
        Time-indexed Series of close prices
    """
    sub = bars[bars["symbol"] == symbol]
    if sub.empty:
        raise RuntimeError(f"Missing bars for {symbol}.")
    s = pd.Series(sub["close"].values, index=pd.to_datetime(sub["timestamp"]))
    s = s.sort_index()

    if strip_incomplete:
        s = drop_incomplete_daily_bar(s, trading_client)

    return s


def batch_get_latest_prices(
    data_client: StockHistoricalDataClient,
    symbols: List[str]
) -> Dict[str, float]:
    """Batch fetch latest prices for multiple symbols."""
    try:
        end = now_et().astimezone(pytz.UTC)
        start = (now_et() - timedelta(days=10)).astimezone(pytz.UTC)
        bars = fetch_daily_bars_alpaca(data_client, symbols, start, end)

        prices = {}
        for sym in symbols:
            try:
                s = get_close_series(bars, sym)
                prices[sym] = float(s.iloc[-1])
            except Exception as e:
                log.warning(f"Failed to extract price for {sym}: {e}")

        log.debug(f"Batch fetched {len(prices)} prices")
        return prices
    except Exception as e:
        log.error(f"Batch price fetch failed: {e}")
        return {}


# =========================
# SIGNALS / WEIGHTS
# =========================

def annualized_realized_vol(close: pd.Series, lookback: int) -> float:
    """Calculate annualized realized volatility with validation."""
    if len(close) < lookback + 1:
        raise RuntimeError(f"Not enough data to compute vol (need {lookback + 1}, got {len(close)}).")

    rets = close.pct_change().dropna()
    window = rets.iloc[-lookback:]

    # Validate no NaN or inf values
    if window.isna().any():
        raise RuntimeError("NaN values detected in returns for volatility calculation.")
    if np.isinf(window).any():
        raise RuntimeError("Infinite values detected in returns for volatility calculation.")

    vol_daily = float(window.std(ddof=1))

    # Validate vol is positive
    if vol_daily <= 0 or np.isnan(vol_daily) or np.isinf(vol_daily):
        raise RuntimeError(f"Invalid volatility calculated: {vol_daily}")

    vol_annual = vol_daily * math.sqrt(252.0)
    return max(vol_annual, VOL_FLOOR_ANNUAL)


def compute_dynamic_vol_floor(close: pd.Series) -> float:
    """
    Compute dynamic volatility floor based on vol clustering (v42).

    Research: Volatility clusters - high vol periods persist (GARCH effect).
    When short-term vol >> long-term vol, we're in a vol cluster and should
    raise the floor to prevent over-concentration right after a shock.

    Returns: Dynamic vol floor (>= VOL_FLOOR_ANNUAL)
    """
    if not USE_DYNAMIC_VOL_FLOOR:
        return VOL_FLOOR_ANNUAL

    try:
        # Need enough data for both short and long lookbacks
        if len(close) < VOL_LOOKBACK_DAYS + 1:
            return VOL_FLOOR_ANNUAL

        rets = close.pct_change().dropna()

        # Short-term vol (recent conditions)
        short_window = min(VOL_SHORT_LOOKBACK, len(rets))
        vol_short = float(rets.iloc[-short_window:].std(ddof=1)) * math.sqrt(252.0)

        # Long-term vol (baseline)
        long_window = min(VOL_LOOKBACK_DAYS, len(rets))
        vol_long = float(rets.iloc[-long_window:].std(ddof=1)) * math.sqrt(252.0)

        if vol_long <= 0:
            return VOL_FLOOR_ANNUAL

        # Detect vol cluster: short vol significantly higher than long vol
        vol_ratio = vol_short / vol_long

        if vol_ratio >= VOL_CLUSTER_THRESHOLD:
            # Vol cluster detected - raise floor
            dynamic_floor = VOL_FLOOR_ANNUAL * VOL_FLOOR_CLUSTER_MULT
            log.debug(f"[VOL_CLUSTER] Detected: short={vol_short:.2%} / long={vol_long:.2%} = {vol_ratio:.2f}x. "
                     f"Raising floor to {dynamic_floor:.2%}")
            return dynamic_floor

        return VOL_FLOOR_ANNUAL

    except Exception as e:
        log.debug(f"[VOL_CLUSTER] Error computing dynamic floor: {e}")
        return VOL_FLOOR_ANNUAL


def sma(close: pd.Series, lookback: int) -> float:
    """Calculate simple moving average with validation."""
    if len(close) < lookback:
        raise RuntimeError(f"Not enough data to compute SMA (need {lookback}, got {len(close)}).")

    avg = float(close.iloc[-lookback:].mean())

    # Validate result
    if np.isnan(avg) or np.isinf(avg):
        raise RuntimeError(f"Invalid SMA calculated: {avg}")

    return avg


def compute_smooth_drawdown_mult(drawdown: float) -> float:
    """
    Compute smooth exposure multiplier based on drawdown (v42).

    Research: Binary triggers create whipsaw. Smooth scaling reduces timing risk.

    Instead of: if dd >= 10%: exposure = 0.5x
    We use:     exposure = smooth_scale(dd) from 1.0x to 0.25x

    Args:
        drawdown: Current drawdown as fraction (e.g., 0.10 = 10%)

    Returns:
        Exposure multiplier between DRAWDOWN_SCALE_FLOOR and 1.0
    """
    if not USE_SMOOTH_DRAWDOWN_SCALING:
        # Fall back to legacy binary logic
        return DRAWDOWN_EXPOSURE_MULT if drawdown >= DRAWDOWN_TRIGGER else 1.0

    if drawdown <= DRAWDOWN_SCALE_START:
        # Below start threshold - full exposure
        return 1.0

    if drawdown >= DRAWDOWN_SCALE_MAX:
        # At or beyond max - floor exposure
        return DRAWDOWN_SCALE_FLOOR

    # Linear interpolation between start and max
    # At start: 1.0, at max: FLOOR
    scale_range = DRAWDOWN_SCALE_MAX - DRAWDOWN_SCALE_START
    dd_progress = (drawdown - DRAWDOWN_SCALE_START) / scale_range
    exposure_mult = 1.0 - dd_progress * (1.0 - DRAWDOWN_SCALE_FLOOR)

    return max(DRAWDOWN_SCALE_FLOOR, exposure_mult)


def trend_signal(close: pd.Series, lookback_sma: int) -> int:
    """
    Determine trend signal (1=bullish, 0=bearish) with buffer zone.
    Requires close > SMA * (1 + buffer) to prevent whipsaw at crossover.

    NOTE: This is for per-ASSET trend gating, not SPY regime.
    For SPY regime, use spy_regime() which has proper hysteresis.
    """
    s = sma(close, lookback_sma)
    c = float(close.iloc[-1])
    threshold = s * (1.0 + SMA_TREND_BUFFER_PCT)
    return 1 if c > threshold else 0


# =========================
# CANONICAL REGIME FUNCTION
# =========================

def spy_regime(
    spy_close: pd.Series,
    current_regime: str,
    lookback_sma: int = SMA_LOOKBACK_DAYS
) -> Tuple[str, bool]:
    """
    CANONICAL SPY regime determination with hysteresis.

    This is the SINGLE SOURCE OF TRUTH for market regime.
    Both weekly rebalance and daily monitoring MUST use this function.

    Hysteresis logic:
    - If currently RISK_ON:  stay risk_on UNLESS close < SMA200 * (1 - REGIME_BUFFER_OFF)
    - If currently RISK_OFF: stay risk_off UNLESS close > SMA200 * (1 + REGIME_BUFFER_ON)

    This prevents flip-flopping when price oscillates near SMA200.

    Args:
        spy_close: SPY daily close series (COMPLETED bars only, not intraday)
        current_regime: Current regime state ("risk_on" or "risk_off")
        lookback_sma: SMA lookback period (default 200)

    Returns:
        Tuple of (new_regime, changed)
        - new_regime: "risk_on" or "risk_off"
        - changed: True if regime changed from current_regime
    """
    sma_val = sma(spy_close, lookback_sma)
    last_close = float(spy_close.iloc[-1])

    # Thresholds with hysteresis
    risk_on_threshold = sma_val * (1.0 + REGIME_BUFFER_ON)   # Must rally above this to go risk-on
    risk_off_threshold = sma_val * (1.0 - REGIME_BUFFER_OFF)  # Must fall below this to go risk-off

    new_regime = current_regime  # Default: stay in current regime

    if current_regime == "risk_on":
        # Currently risk-on: only switch to risk-off if we break DOWN through lower threshold
        if last_close < risk_off_threshold:
            new_regime = "risk_off"
            log.warning(f"[REGIME] RISK-OFF triggered: SPY close ${last_close:.2f} < "
                       f"SMA200*(1-{REGIME_BUFFER_OFF:.1%}) = ${risk_off_threshold:.2f}")
    else:
        # Currently risk-off: only switch to risk-on if we break UP through upper threshold
        if last_close > risk_on_threshold:
            new_regime = "risk_on"
            log.info(f"[REGIME] RISK-ON triggered: SPY close ${last_close:.2f} > "
                    f"SMA200*(1+{REGIME_BUFFER_ON:.1%}) = ${risk_on_threshold:.2f}")

    changed = new_regime != current_regime

    # Debug log current state
    log.debug(f"[REGIME] SPY=${last_close:.2f}, SMA200=${sma_val:.2f}, "
             f"risk_on_thresh=${risk_on_threshold:.2f}, risk_off_thresh=${risk_off_threshold:.2f}, "
             f"regime={new_regime} (changed={changed})")

    return new_regime, changed


def update_regime_state(
    state: BotState,
    spy_close: pd.Series,
    state_path: str
) -> Tuple[str, bool]:
    """
    Update regime state using canonical spy_regime() function.

    Call this at the START of both rebalance and daily monitoring
    to ensure consistent regime across both layers.

    Enhanced with:
    - Real VIX override: VIX > 35 forces risk_off regardless of SPY vs SMA200
    - Real S&P 500 index cross-check logged for comparison

    Args:
        state: BotState to update
        spy_close: SPY daily close series (completed bars only)
        state_path: Path to save state

    Returns:
        Tuple of (current_regime, changed_today)
    """
    new_regime, changed = spy_regime(spy_close, state.spy_regime)

    # VIX extreme override: force risk_off if VIX > circuit breaker level
    real_vix = fetch_real_vix()
    if real_vix is not None and real_vix > VIX_CIRCUIT_BREAKER and new_regime == "risk_on":
        new_regime = "risk_off"
        changed = (new_regime != state.spy_regime)
        log.warning(f"[REGIME] VIX override: VIX={real_vix:.1f} > {VIX_CIRCUIT_BREAKER} — "
                    f"forcing RISK_OFF despite SPY vs SMA200")

    # Cross-check with real S&P 500 index data (informational, not decision-making)
    spx_data = fetch_real_spx_data()
    if spx_data and spx_data.get("pct_vs_sma") is not None:
        spx_regime_hint = "risk_on" if spx_data["pct_vs_sma"] > 0 else "risk_off"
        if spx_regime_hint != new_regime:
            log.info(f"[REGIME] Note: S&P 500 index ({spx_data['value']:,.0f}, "
                     f"{spx_data['pct_vs_sma']:+.2f}% vs SMA200) suggests {spx_regime_hint}, "
                     f"but SPY-based regime is {new_regime}")

    if changed:
        state.spy_regime = new_regime
        state.spy_regime_changed_date_iso = today_iso_et()
        save_state(state_path, state)
        log.warning(f"[REGIME] State persisted: {new_regime}")

    return new_regime, changed


def compute_profit_tilt(extension_pct: float) -> float:
    """
    Compute profit-taking tilt multiplier for extended positions.

    Instead of selling mid-cycle, we REDUCE target weight at rebalance time.
    This keeps turnover low and lets trends run while still taking some profit.

    Args:
        extension_pct: How far price is above SMA200 (e.g., 0.25 = 25% above)

    Returns:
        Tilt multiplier (0.6 to 1.0) to apply to base target weight
        - 1.0 = no tilt (price not extended)
        - 0.6 = max tilt (price very extended)
    """
    if extension_pct < PROFIT_TILT_EXTENSION_THRESHOLD:
        return 1.0  # Not extended, no tilt

    # Linear tilt: tilt = 1 - k * (extension - threshold)
    # Clamped to [1 - max_reduction, 1.0]
    excess_extension = extension_pct - PROFIT_TILT_EXTENSION_THRESHOLD
    tilt = 1.0 - (PROFIT_TILT_K * excess_extension)

    # Clamp to minimum (e.g., 60% of base weight)
    min_tilt = 1.0 - PROFIT_TILT_MAX_REDUCTION
    tilt = max(tilt, min_tilt)

    log.debug(f"[PROFIT_TILT] extension={extension_pct:.1%}, tilt={tilt:.2f}")

    return tilt


def check_consecutive_closes_below_sma(
    closes: List[float],
    sma_val: float,
    buffer: float = STOP_LOSS_SMA200_BUFFER,
    required_days: int = STOP_LOSS_SMA200_DAYS
) -> bool:
    """
    Check if last N daily CLOSES were consecutively below SMA200 * buffer.

    This is CLOSE-BASED logic (not intraday touch).

    Args:
        closes: List of recent daily closes (most recent last)
        sma_val: Current SMA200 value
        buffer: Buffer multiplier (e.g., 0.98 = 2% below SMA)
        required_days: How many consecutive days required

    Returns:
        True if last `required_days` closes were ALL below threshold
    """
    if len(closes) < required_days:
        return False  # Not enough data

    threshold = sma_val * buffer

    # Check last N closes
    recent_closes = closes[-required_days:]
    all_below = all(close < threshold for close in recent_closes)

    if all_below:
        log.debug(f"[STOP] {required_days} consecutive closes below SMA200*{buffer}: "
                 f"closes={[f'${c:.2f}' for c in recent_closes]}, threshold=${threshold:.2f}")

    return all_below


# =========================
# MOMENTUM SCORING (Enhanced for CAGR Edge)
# =========================

def compute_momentum_score(
    close: pd.Series,
    spy_close: Optional[pd.Series] = None,
    lookback_63d: int = MOMENTUM_LOOKBACK_63D,
    lookback_126d: int = MOMENTUM_LOOKBACK_126D,
    lookback_252d: int = MOMENTUM_LOOKBACK_252D,
    skip_days: int = 0,
    vol_adjust: bool = False
) -> Optional[float]:
    """
    Compute enhanced momentum score for an asset.

    Enhancements over naive momentum:
    1. Relative strength vs SPY (if spy_close provided)
    2. Skip last N days (12-1 momentum) to avoid reversal noise
    3. Vol-adjusted score to prefer quality trends

    Args:
        close: Price series (completed bars only)
        spy_close: SPY price series for relative strength (optional)
        lookback_63d: 3-month lookback
        lookback_126d: 6-month lookback
        lookback_252d: 12-month lookback
        skip_days: Days to skip from end (for 12-1 momentum)
        vol_adjust: Divide by volatility for risk-adjusted momentum

    Returns:
        Momentum score (can be negative), or None if insufficient data
    """
    min_length = lookback_252d + skip_days + 1
    if len(close) < min_length:
        return None  # Insufficient data

    try:
        # Apply skip_days offset (for 12-1 momentum)
        if skip_days > 0:
            price_series = close.iloc[:-skip_days] if skip_days < len(close) else close
        else:
            price_series = close

        # If using relative strength, compute RS ratio vs SPY
        if spy_close is not None and ENABLE_RELATIVE_STRENGTH:
            if len(spy_close) < min_length:
                # Fall back to absolute momentum
                rs_series = price_series
            else:
                spy_series = spy_close.iloc[:-skip_days] if skip_days > 0 and skip_days < len(spy_close) else spy_close
                # Align lengths
                min_len = min(len(price_series), len(spy_series))
                price_aligned = price_series.iloc[-min_len:]
                spy_aligned = spy_series.iloc[-min_len:]
                # RS = price / SPY (relative strength ratio)
                rs_series = price_aligned / spy_aligned
        else:
            rs_series = price_series

        # Calculate returns for each period (using RS or absolute)
        end_price = float(rs_series.iloc[-1])

        ret_63d = 0.0
        ret_126d = 0.0
        ret_252d = 0.0

        if len(rs_series) > lookback_63d:
            start_63d = float(rs_series.iloc[-lookback_63d])
            ret_63d = (end_price / start_63d - 1.0) if start_63d > 0 else 0.0

        if len(rs_series) > lookback_126d:
            start_126d = float(rs_series.iloc[-lookback_126d])
            ret_126d = (end_price / start_126d - 1.0) if start_126d > 0 else 0.0

        if len(rs_series) > lookback_252d:
            start_252d = float(rs_series.iloc[-lookback_252d])
            ret_252d = (end_price / start_252d - 1.0) if start_252d > 0 else 0.0

        # Weighted momentum score
        score = (
            MOMENTUM_WEIGHT_63D * ret_63d +
            MOMENTUM_WEIGHT_126D * ret_126d +
            MOMENTUM_WEIGHT_252D * ret_252d
        )

        # Vol-adjust if enabled (prefer quality trends)
        if vol_adjust and ENABLE_VOL_ADJUSTED_MOMENTUM:
            vol = annualized_realized_vol(close, VOL_LOOKBACK_DAYS)
            vol = max(vol, VOL_FLOOR_ANNUAL)
            score = score / vol  # Risk-adjusted momentum

        return float(score)

    except Exception as e:
        log.warning(f"[MOMENTUM] Failed to compute score: {e}")
        return None


def compute_relative_strength(
    close: pd.Series,
    spy_close: pd.Series,
    lookback: int = 63
) -> Optional[float]:
    """
    Compute relative strength vs SPY over lookback period.

    RS = (sym_price / SPY_price) momentum

    Args:
        close: Symbol price series
        spy_close: SPY price series
        lookback: Lookback period in days

    Returns:
        RS value (positive = outperforming SPY), or None if insufficient data
    """
    if len(close) < lookback + 1 or len(spy_close) < lookback + 1:
        return None

    try:
        # Align series
        min_len = min(len(close), len(spy_close))
        close_aligned = close.iloc[-min_len:]
        spy_aligned = spy_close.iloc[-min_len:]

        # RS ratio
        rs = close_aligned / spy_aligned

        # RS momentum (change in ratio over lookback)
        rs_now = float(rs.iloc[-1])
        rs_then = float(rs.iloc[-lookback]) if len(rs) > lookback else float(rs.iloc[0])

        if rs_then > 0:
            return (rs_now / rs_then - 1.0)
        return None

    except Exception as e:
        log.debug(f"[RS] Failed to compute: {e}")
        return None


def rank_by_momentum(
    bars: pd.DataFrame,
    symbols: List[str],
    trading_client: TradingClient,
    top_n: int = 0,
    spy_close: Optional[pd.Series] = None
) -> List[Tuple[str, float]]:
    """
    Rank symbols by enhanced momentum score.

    Includes:
    - Relative strength vs SPY (if ENABLE_RELATIVE_STRENGTH)
    - Skip-month (12-1 momentum) if ENABLE_SKIP_MONTH
    - Vol-adjusted scoring if ENABLE_VOL_ADJUSTED_MOMENTUM
    - Sector RS filter if ENABLE_SECTOR_RS_FILTER

    Args:
        bars: Historical price data
        symbols: List of symbols to rank
        trading_client: For market hours check (strip_incomplete)
        top_n: Return top N symbols (0 = return all ranked)
        spy_close: Pre-computed SPY close series (optional, will fetch if not provided)

    Returns:
        List of (symbol, momentum_score) tuples, sorted descending by score
    """
    # Get SPY close for relative strength
    if spy_close is None and ENABLE_RELATIVE_STRENGTH:
        try:
            spy_close = get_close_series(bars, "SPY", strip_incomplete=True, trading_client=trading_client)
        except Exception as e:
            log.warning(f"[MOMENTUM] Failed to get SPY for RS: {e}")
            spy_close = None

    skip_days = SKIP_MONTH_DAYS if ENABLE_SKIP_MONTH else 0

    scored = []
    skipped_reasons = {}  # Track why symbols were skipped

    # Calculate minimum required length for momentum
    min_length_needed = MOMENTUM_LOOKBACK_252D + skip_days + 1

    for sym in symbols:
        try:
            close = get_close_series(bars, sym, strip_incomplete=True, trading_client=trading_client)

            # Check data sufficiency BEFORE computing score
            if len(close) < min_length_needed:
                skipped_reasons[sym] = f"insufficient_data ({len(close)}/{min_length_needed} bars)"
                continue

            # Compute enhanced momentum score
            score = compute_momentum_score(
                close,
                spy_close=spy_close,
                skip_days=skip_days,
                vol_adjust=ENABLE_VOL_ADJUSTED_MOMENTUM
            )

            if score is None:
                skipped_reasons[sym] = f"score_none (had {len(close)} bars)"
                continue

            # Apply sector RS filter if enabled
            if ENABLE_SECTOR_RS_FILTER and sym in SECTOR_TICKERS and spy_close is not None:
                rs = compute_relative_strength(close, spy_close, lookback=63)
                if rs is not None and rs < 0:
                    # Skip sectors with negative RS vs SPY
                    skipped_reasons[sym] = f"negative_RS ({rs:.2%})"
                    log.debug(f"[MOMENTUM] Skipping {sym}: negative RS ({rs:.2%})")
                    continue

            scored.append((sym, score))

        except Exception as e:
            skipped_reasons[sym] = f"exception: {e}"
            log.debug(f"[MOMENTUM] Skipping {sym}: {e}")
            continue

    # Log summary at INFO level if many symbols were skipped
    if skipped_reasons and len(skipped_reasons) > len(symbols) * 0.5:
        log.warning(f"[MOMENTUM] Skipped {len(skipped_reasons)}/{len(symbols)} symbols - possible data issue!")
        # Log first few reasons
        for sym, reason in list(skipped_reasons.items())[:5]:
            log.warning(f"[MOMENTUM]   {sym}: {reason}")

    # Sort by momentum score (descending)
    scored.sort(key=lambda x: x[1], reverse=True)

    if top_n > 0:
        scored = scored[:top_n]

    return scored


# =========================
# RISK SCORE OVERLAY (Phase 2)
# =========================

def compute_risk_score(
    bars: pd.DataFrame,
    trading_client: TradingClient
) -> Tuple[float, Dict[str, Any]]:
    """
    Compute market risk score (0-100) based on multiple factors.

    Components:
    1. VIX regime (40% weight) - volatility level
    2. SPY trend health (30% weight) - distance from SMA200, slope
    3. Breadth proxy (30% weight) - % of universe above SMA50

    Args:
        bars: Historical price data
        trading_client: For market hours check

    Returns:
        Tuple of (risk_score, components_dict)
        risk_score: 0-100 where higher = more bullish/risk-on
        components_dict: breakdown of score components
    """
    components = {}

    # === 1. VIX COMPONENT (40% weight) ===
    # Prefer real VIX from Polygon Indices API; fall back to SPY realized vol estimate
    vix_score = 50.0  # Default neutral
    try:
        # Try real VIX first (requires Polygon Indices Starter plan)
        real_vix = fetch_real_vix()
        if real_vix is not None:
            estimated_vix = real_vix
            components["vix_source"] = "polygon"
        else:
            # Fallback: estimate from SPY realized vol
            spy_close = get_close_series(bars, "SPY", strip_incomplete=True, trading_client=trading_client)
            spy_vol = annualized_realized_vol(spy_close, VOL_LOOKBACK_DAYS)
            estimated_vix = spy_vol * 100  # Rough approximation
            components["vix_source"] = "estimated"

        if estimated_vix < VIX_LOW_THRESHOLD:
            vix_score = 90.0  # Very bullish
        elif estimated_vix < VIX_MEDIUM_THRESHOLD:
            vix_score = 70.0  # Normal
        elif estimated_vix < VIX_HIGH_THRESHOLD:
            vix_score = 45.0  # Caution
        elif estimated_vix < VIX_EXTREME_THRESHOLD:
            vix_score = 25.0  # Elevated risk
        else:
            vix_score = 10.0  # High vol = risk-off

        components["vix_estimated"] = estimated_vix
        components["vix_score"] = vix_score

    except Exception as e:
        log.warning(f"[RISK_SCORE] VIX component failed: {e}")
        components["vix_score"] = 50.0

    # === 2. SPY TREND HEALTH (30% weight) ===
    trend_score = 50.0  # Default neutral
    try:
        spy_close = get_close_series(bars, "SPY", strip_incomplete=True, trading_client=trading_client)
        spy_sma200 = sma(spy_close, SMA_LOOKBACK_DAYS)
        spy_sma50 = sma(spy_close, 50) if len(spy_close) >= 50 else spy_sma200
        last_close = float(spy_close.iloc[-1])

        # Distance from SMA200 (positive = above)
        dist_sma200 = (last_close - spy_sma200) / spy_sma200

        # SMA50 slope (20-day change)
        if len(spy_close) >= 70:
            sma50_20d_ago = sma(spy_close.iloc[:-20], 50)
            sma50_slope = (spy_sma50 - sma50_20d_ago) / sma50_20d_ago
        else:
            sma50_slope = 0.0

        # Score based on trend health
        # Above SMA200 with rising SMA50 = bullish
        # Below SMA200 with falling SMA50 = bearish
        if dist_sma200 > 0.05 and sma50_slope > 0.01:
            trend_score = 90.0
        elif dist_sma200 > 0.02 and sma50_slope > 0:
            trend_score = 75.0
        elif dist_sma200 > 0:
            trend_score = 60.0
        elif dist_sma200 > -0.03:
            trend_score = 40.0
        elif dist_sma200 > -0.05:
            trend_score = 25.0
        else:
            trend_score = 10.0

        components["spy_dist_sma200"] = dist_sma200
        components["spy_sma50_slope"] = sma50_slope
        components["trend_score"] = trend_score

    except Exception as e:
        log.warning(f"[RISK_SCORE] Trend component failed: {e}")
        components["trend_score"] = 50.0

    # === 3. BREADTH PROXY (30% weight) ===
    # Upgraded: Use RSP/SPY and IWM/SPY ratios for better breadth signal
    # RSP = equal-weight S&P500 (breadth), IWM = small caps (risk appetite)
    # When RSP/SPY rising: broad participation = healthy
    # When IWM/SPY rising: risk appetite = bullish
    breadth_score = 50.0  # Default neutral
    try:
        # Try enhanced breadth using RSP and IWM ratios
        rsp_score = None
        iwm_score = None

        # RSP/SPY ratio trend (equal-weight vs cap-weight)
        try:
            rsp_close = get_close_series(bars, "RSP", strip_incomplete=True, trading_client=trading_client)
            spy_close_b = get_close_series(bars, "SPY", strip_incomplete=True, trading_client=trading_client)

            if len(rsp_close) >= 50 and len(spy_close_b) >= 50:
                # Align series
                min_len = min(len(rsp_close), len(spy_close_b))
                rsp_close = rsp_close.iloc[-min_len:]
                spy_close_b = spy_close_b.iloc[-min_len:]

                # Compute ratio and its SMA20
                rsp_spy_ratio = rsp_close / spy_close_b
                ratio_sma20 = sma(rsp_spy_ratio, 20)
                current_ratio = float(rsp_spy_ratio.iloc[-1])

                # Score: ratio above SMA = breadth improving
                if current_ratio > ratio_sma20 * 1.01:
                    rsp_score = 80.0  # Strong breadth
                elif current_ratio > ratio_sma20:
                    rsp_score = 65.0  # Improving
                elif current_ratio > ratio_sma20 * 0.99:
                    rsp_score = 45.0  # Flat
                else:
                    rsp_score = 25.0  # Narrowing breadth

                components["rsp_spy_ratio"] = current_ratio
                components["rsp_spy_sma20"] = ratio_sma20
        except Exception:
            pass  # RSP not available, fallback to legacy

        # IWM/SPY ratio trend (small cap vs large cap risk appetite)
        try:
            iwm_close = get_close_series(bars, "IWM", strip_incomplete=True, trading_client=trading_client)
            spy_close_b = get_close_series(bars, "SPY", strip_incomplete=True, trading_client=trading_client)

            if len(iwm_close) >= 50 and len(spy_close_b) >= 50:
                min_len = min(len(iwm_close), len(spy_close_b))
                iwm_close = iwm_close.iloc[-min_len:]
                spy_close_b = spy_close_b.iloc[-min_len:]

                iwm_spy_ratio = iwm_close / spy_close_b
                ratio_sma20 = sma(iwm_spy_ratio, 20)
                current_ratio = float(iwm_spy_ratio.iloc[-1])

                # Score: IWM outperforming = risk-on
                if current_ratio > ratio_sma20 * 1.02:
                    iwm_score = 85.0  # Strong risk appetite
                elif current_ratio > ratio_sma20:
                    iwm_score = 65.0  # Risk-on
                elif current_ratio > ratio_sma20 * 0.98:
                    iwm_score = 40.0  # Neutral
                else:
                    iwm_score = 20.0  # Risk-off flight to quality

                components["iwm_spy_ratio"] = current_ratio
                components["iwm_spy_sma20"] = ratio_sma20
        except Exception:
            pass  # IWM not available

        # Combine RSP and IWM scores (if available)
        if rsp_score is not None and iwm_score is not None:
            # Weight RSP slightly higher (breadth more important than risk appetite)
            breadth_score = 0.55 * rsp_score + 0.45 * iwm_score
            components["breadth_method"] = "rsp_iwm"
        elif rsp_score is not None:
            breadth_score = rsp_score
            components["breadth_method"] = "rsp_only"
        elif iwm_score is not None:
            breadth_score = iwm_score
            components["breadth_method"] = "iwm_only"
        else:
            # Fallback: legacy % above SMA50
            above_sma50 = 0
            total_checked = 0
            for sym in ALL_EQUITY[:15]:
                try:
                    close = get_close_series(bars, sym, strip_incomplete=True, trading_client=trading_client)
                    if len(close) >= 50:
                        sym_sma50 = sma(close, 50)
                        if float(close.iloc[-1]) > sym_sma50:
                            above_sma50 += 1
                        total_checked += 1
                except Exception:
                    continue

            if total_checked > 0:
                breadth_pct = above_sma50 / total_checked
                breadth_score = breadth_pct * 100.0
                components["breadth_pct"] = breadth_pct
            components["breadth_method"] = "legacy_sma50"

        components["breadth_score"] = breadth_score

    except Exception as e:
        log.warning(f"[RISK_SCORE] Breadth component failed: {e}")
        components["breadth_score"] = 50.0

    # === COMPOSITE SCORE ===
    # Weighted average of components
    risk_score = (
        0.40 * components.get("vix_score", 50.0) +
        0.30 * components.get("trend_score", 50.0) +
        0.30 * components.get("breadth_score", 50.0)
    )

    # Clamp to [0, 100]
    risk_score = max(0.0, min(100.0, risk_score))
    components["composite_score"] = risk_score

    log.info(f"[RISK_SCORE] Score={risk_score:.1f} | VIX={components.get('vix_score', 0):.1f} | "
            f"Trend={components.get('trend_score', 0):.1f} | Breadth={components.get('breadth_score', 0):.1f}")

    return risk_score, components


def get_exposure_multiplier_from_risk_score(risk_score: float) -> float:
    """
    Map risk score to exposure multiplier.

    Uses RISK_SCORE_EXPOSURE_MAP to determine exposure based on score.

    Args:
        risk_score: 0-100 risk score

    Returns:
        Exposure multiplier (e.g., 0.4 to 1.2)
    """
    # Find the appropriate tier
    for threshold in sorted(RISK_SCORE_EXPOSURE_MAP.keys(), reverse=True):
        if risk_score >= threshold:
            return RISK_SCORE_EXPOSURE_MAP[threshold]

    # Fallback
    return 0.40


# =========================
# TURNOVER GOVERNOR + RANK STABILITY
# =========================

def compute_portfolio_turnover(
    current_weights: Dict[str, float],
    target_weights: Dict[str, float]
) -> float:
    """
    Compute one-way turnover between current and target portfolios.

    Turnover = sum(|target - current|) / 2
    100% turnover means entire portfolio changes hands.

    Args:
        current_weights: Current position weights
        target_weights: Target position weights

    Returns:
        Turnover as fraction (0.0 to 1.0)
    """
    all_symbols = set(current_weights.keys()) | set(target_weights.keys())
    total_delta = sum(
        abs(target_weights.get(sym, 0.0) - current_weights.get(sym, 0.0))
        for sym in all_symbols
    )
    return total_delta / 2.0  # One-way turnover


def apply_turnover_cap(
    current_weights: Dict[str, float],
    target_weights: Dict[str, float],
    max_turnover: float
) -> Tuple[Dict[str, float], float, bool]:
    """
    Apply turnover cap to target weights, limiting changes proportionally.

    If computed turnover exceeds max_turnover, scale down weight changes
    proportionally so total turnover = max_turnover.

    Args:
        current_weights: Current position weights
        target_weights: Target position weights
        max_turnover: Maximum allowed turnover (e.g., 0.25 = 25%)

    Returns:
        Tuple of (capped_weights, actual_turnover, was_capped)
    """
    turnover = compute_portfolio_turnover(current_weights, target_weights)

    if turnover <= max_turnover or turnover < 0.001:
        return target_weights.copy(), turnover, False

    # Scale factor to bring turnover down to limit
    scale = max_turnover / turnover

    # Apply scaled changes
    capped = {}
    for sym in set(current_weights.keys()) | set(target_weights.keys()):
        curr = current_weights.get(sym, 0.0)
        tgt = target_weights.get(sym, 0.0)
        delta = tgt - curr
        capped[sym] = curr + (delta * scale)

    # Normalize if needed (shouldn't change much)
    total = sum(capped.values())
    if abs(total - 1.0) > 0.01 and total > 0:
        capped = {k: v / total for k, v in capped.items()}

    return capped, max_turnover, True


def compute_max_drift(
    current_weights: Dict[str, float],
    target_weights: Dict[str, float]
) -> Tuple[float, str]:
    """
    Compute maximum weight drift from target for any single position.

    Args:
        current_weights: Current position weights
        target_weights: Target position weights

    Returns:
        Tuple of (max_drift, symbol_with_max_drift)
    """
    all_symbols = set(current_weights.keys()) | set(target_weights.keys())
    max_drift = 0.0
    max_sym = ""

    for sym in all_symbols:
        drift = abs(target_weights.get(sym, 0.0) - current_weights.get(sym, 0.0))
        if drift > max_drift:
            max_drift = drift
            max_sym = sym

    return max_drift, max_sym


def update_rank_history(
    state: BotState,
    top_n_symbols: List[str],
    rebalance_date: str
) -> None:
    """
    Update rank history with today's top N symbols.

    Args:
        state: BotState to update
        top_n_symbols: Symbols that are in top N today
        rebalance_date: ISO date string of this rebalance
    """
    if state.rank_history is None:
        state.rank_history = {}

    # Add today's date to each top N symbol's history
    for sym in top_n_symbols:
        if sym not in state.rank_history:
            state.rank_history[sym] = []
        state.rank_history[sym].append(rebalance_date)

    # Clean up: remove symbols not in top N (they lose their streak)
    # and trim old history (keep last ~8 weeks)
    for sym in list(state.rank_history.keys()):
        if sym not in top_n_symbols:
            # Symbol dropped out - reset streak
            del state.rank_history[sym]
        else:
            # Keep last 8 entries max
            state.rank_history[sym] = state.rank_history[sym][-8:]


def check_rank_stability(
    symbol: str,
    state: BotState,
    required_weeks: int = RANK_STABILITY_WEEKS
) -> bool:
    """
    Check if symbol has been in top N for required consecutive weeks.

    Args:
        symbol: Symbol to check
        state: BotState with rank history
        required_weeks: Number of consecutive weeks required

    Returns:
        True if symbol has sufficient rank stability
    """
    if not ENABLE_RANK_STABILITY:
        return True  # Feature disabled

    if state.rank_history is None:
        return False

    history = state.rank_history.get(symbol, [])
    return len(history) >= required_weeks


def filter_by_rank_stability(
    ranked_symbols: List[str],
    state: BotState,
    current_holdings: List[str]
) -> List[str]:
    """
    Filter ranked symbols by rank stability, preserving current holdings.

    Symbols already held get grandfathered in (no stability requirement).
    New symbols must meet rank stability requirement.

    Args:
        ranked_symbols: Symbols sorted by rank (best first)
        state: BotState with rank history
        current_holdings: Symbols currently held

    Returns:
        Filtered list of symbols meeting stability criteria
    """
    if not ENABLE_RANK_STABILITY:
        return ranked_symbols

    result = []
    for sym in ranked_symbols:
        # Current holdings are grandfathered
        if sym in current_holdings:
            result.append(sym)
        # New additions must have stability
        elif check_rank_stability(sym, state):
            result.append(sym)
        else:
            log.debug(f"[RANK_STABILITY] {sym} filtered - insufficient history")

    return result


def should_skip_rebalance_for_drift(
    current_weights: Dict[str, float],
    target_weights: Dict[str, float]
) -> Tuple[bool, float, str]:
    """
    Check if rebalance should be skipped because drift is below threshold.

    Args:
        current_weights: Current position weights
        target_weights: Target position weights

    Returns:
        Tuple of (should_skip, max_drift, reason)
    """
    if not ENABLE_TURNOVER_GOVERNOR:
        return False, 0.0, ""

    max_drift, max_sym = compute_max_drift(current_weights, target_weights)

    if max_drift < NO_TRADE_DRIFT_THRESHOLD:
        return True, max_drift, f"Max drift {max_drift:.1%} < {NO_TRADE_DRIFT_THRESHOLD:.1%} threshold (at {max_sym})"

    return False, max_drift, ""


def check_drift_mini_rebalance_needed(
    state: BotState,
    positions: Dict[str, Dict],
    total_equity: float,
    trading_client: TradingClient
) -> Tuple[bool, float, str]:
    """
    Check if a drift-based mini-rebalance is needed.

    Triggers when:
    1. Feature enabled
    2. Sufficient time since last rebalance
    3. Any position drifted beyond threshold from last target weights

    Args:
        state: BotState with last rebalance info
        positions: Current positions dict
        total_equity: Current portfolio equity
        trading_client: For market hours check

    Returns:
        Tuple of (should_rebalance, max_drift, reason)
    """
    if not ENABLE_DRIFT_MINI_REBALANCE:
        return False, 0.0, "disabled"

    # Check minimum days since last rebalance
    if state.last_rebalance_date_iso is None:
        return False, 0.0, "no_prior_rebalance"

    try:
        last_rebal_date = datetime.fromisoformat(state.last_rebalance_date_iso).date()
        days_since = (now_et().date() - last_rebal_date).days
    except Exception:
        return False, 0.0, "invalid_date"

    if days_since < DRIFT_MIN_DAYS_SINCE_REBAL:
        return False, 0.0, f"too_soon ({days_since}d < {DRIFT_MIN_DAYS_SINCE_REBAL}d)"

    # Need last target weights to compare
    if state.last_target_weights is None:
        return False, 0.0, "no_target_weights"

    # Compute current weights using same basis as weekly rebalance:
    # 1) Attribute wash-sale substitutes to original symbols
    # 2) Use deployable_capital (not total_equity) as denominator
    current_mv = {sym: positions.get(sym, {}).get("market_value", 0.0) for sym in ALL_TICKERS}

    # Attribute substitute holdings (e.g., PDBC -> DBC, BIL -> SGOV)
    if state.active_substitutions:
        for sub_sym, orig_sym in state.active_substitutions.items():
            if sub_sym in positions and is_wash_sale_cooldown_active(state.loss_sales, orig_sym):
                sub_mv = positions[sub_sym]["market_value"]
                current_mv[orig_sym] = current_mv.get(orig_sym, 0.0) + sub_mv

    # Use deployable_capital as denominator (matches weekly rebalance weight basis)
    spy_risk_on = (state.spy_regime == "risk_on")
    capital_usage_pct, _ = compute_dynamic_capital_usage(
        risk_score=50.0, spy_risk_on=spy_risk_on
    )
    deployable_capital = total_equity * capital_usage_pct
    denom = deployable_capital if deployable_capital > 0 else total_equity

    current_w = {sym: (current_mv[sym] / denom if denom > 0 else 0.0) for sym in ALL_TICKERS}

    # Compare to last target weights
    max_drift = 0.0
    max_sym = ""
    for sym in set(current_w.keys()) | set(state.last_target_weights.keys()):
        curr = current_w.get(sym, 0.0)
        tgt = state.last_target_weights.get(sym, 0.0)
        drift = abs(curr - tgt)
        if drift > max_drift:
            max_drift = drift
            max_sym = sym

    if max_drift >= DRIFT_TRIGGER_THRESHOLD:
        return True, max_drift, f"{max_sym} drifted {max_drift:.1%} (threshold: {DRIFT_TRIGGER_THRESHOLD:.1%})"

    return False, max_drift, ""


def execute_drift_mini_rebalance(
    trading: TradingClient,
    data_client: StockHistoricalDataClient,
    state: BotState,
    max_drift_sym: str
) -> None:
    """
    Execute a limited mini-rebalance to correct drifted positions.

    Only rebalances the most drifted positions, capped at DRIFT_MAX_TURNOVER.

    Args:
        trading: Trading client
        data_client: Data client for prices
        state: Bot state
        max_drift_sym: Symbol with max drift (for logging)
    """
    log.info(f"[DRIFT_MINI] Starting mini-rebalance triggered by {max_drift_sym}")

    try:
        total_equity = get_portfolio_equity(trading)
        positions = get_positions(trading)

        # Get current weights using same basis as weekly rebalance:
        # 1) Attribute wash-sale substitutes to original symbols
        # 2) Use deployable_capital (not total_equity) as denominator
        current_mv = {sym: positions.get(sym, {}).get("market_value", 0.0) for sym in ALL_TICKERS}

        # Attribute substitute holdings (e.g., PDBC -> DBC, BIL -> SGOV)
        if state.active_substitutions:
            for sub_sym, orig_sym in state.active_substitutions.items():
                if sub_sym in positions and is_wash_sale_cooldown_active(state.loss_sales, orig_sym):
                    sub_mv = positions[sub_sym]["market_value"]
                    current_mv[orig_sym] = current_mv.get(orig_sym, 0.0) + sub_mv
                    log.info(f"[DRIFT_MINI] Attributing {sub_sym} (${sub_mv:,.2f}) to {orig_sym} (wash sale substitute)")

        # Use deployable_capital as denominator (matches weekly rebalance weight basis)
        spy_risk_on = (state.spy_regime == "risk_on")
        capital_usage_pct, _ = compute_dynamic_capital_usage(
            risk_score=50.0, spy_risk_on=spy_risk_on
        )
        deployable_capital = total_equity * capital_usage_pct
        denom = deployable_capital if deployable_capital > 0 else total_equity

        current_w = {sym: (current_mv[sym] / denom if denom > 0 else 0.0) for sym in ALL_TICKERS}

        # Target weights are the stored weights from last full rebalance
        target_w = state.last_target_weights or {}

        # Apply mini-rebalance turnover cap
        capped_w, actual_turnover, was_capped = apply_turnover_cap(
            current_w, target_w, DRIFT_MAX_TURNOVER
        )

        if was_capped:
            log.info(f"[DRIFT_MINI] Capped turnover to {DRIFT_MAX_TURNOVER:.1%}")

        # Build sell and buy order lists (separate so sells execute first)
        sell_orders = []
        buy_orders = []

        for sym in ALL_TICKERS:
            tw = float(capped_w.get(sym, 0.0))
            cw = float(current_w.get(sym, 0.0))
            delta_w = tw - cw
            delta_notional = delta_w * deployable_capital

            if abs(delta_notional) < MIN_TRADE_NOTIONAL_USD:
                continue

            if abs(delta_w) < WEIGHT_CHANGE_MIN_THRESHOLD:
                continue

            # Get price
            try:
                est_price = price_cache.get_price(sym, data_client)
            except Exception:
                continue

            if est_price <= 0:
                continue

            qty = abs(delta_notional) / est_price
            if qty < MIN_TRADE_SHARES:
                continue

            side = OrderSide.BUY if delta_notional > 0 else OrderSide.SELL

            # For sells, clamp to available
            if side == OrderSide.SELL:
                available_qty = positions.get(sym, {}).get("qty", 0.0)
                qty = min(qty, available_qty)
                if qty <= 0:
                    continue

            use_fractional = ALLOW_FRACTIONAL and validate_fractional_support(trading, sym)
            final_qty = qty if use_fractional else int(qty)

            if final_qty <= 0:
                continue

            order_info = {
                "symbol": sym, "qty": final_qty, "side": side,
                "est_price": est_price, "target_weight": tw, "current_weight": cw,
                "delta_notional": delta_notional,
            }
            if side == OrderSide.SELL:
                sell_orders.append(order_info)
            else:
                buy_orders.append(order_info)

        # Execute SELLS first to free up buying power
        trades_executed = 0
        for o in sell_orders:
            try:
                order_req = MarketOrderRequest(
                    symbol=o["symbol"], qty=o["qty"],
                    side=OrderSide.SELL, time_in_force=TimeInForce.DAY
                )
                trading.submit_order(order_req)
                trades_executed += 1
                log_trade(
                    path=LOG_PATH, action="sell", symbol=o["symbol"],
                    qty=o["qty"], est_price=o["est_price"],
                    notional=o["qty"] * o["est_price"],
                    reason="drift_mini_rebalance",
                    target_weight=o["target_weight"],
                    current_weight=o["current_weight"],
                    portfolio_equity=total_equity
                )
                log.info(f"[DRIFT_MINI] sell {o['qty']:.4f} {o['symbol']} @ ~${o['est_price']:.2f}")
            except Exception as e:
                log.error(f"[DRIFT_MINI] Failed to sell {o['symbol']}: {e}")

        # Brief pause for sells to process before buying
        if sell_orders and not DRY_RUN:
            time.sleep(3)

        # Check buying power before executing buys
        if buy_orders and not DRY_RUN:
            try:
                acct = trading.get_account()
                available_bp = float(acct.buying_power)
                total_buy_notional = sum(o["qty"] * o["est_price"] for o in buy_orders)
                effective_bp = available_bp * 0.995  # 0.5% safety buffer

                if total_buy_notional > effective_bp:
                    # Minimum $5 buying power required to place any meaningful order
                    if effective_bp < 5.0:
                        log.warning(f"[DRIFT_MINI] No usable buying power (${available_bp:.2f}) — skipping all buys")
                        buy_orders = []
                    else:
                        scale_factor = effective_bp / total_buy_notional
                        log.warning(f"[DRIFT_MINI] Insufficient buying power | "
                                   f"need=${total_buy_notional:,.0f} available=${available_bp:,.0f} | "
                                   f"scaling buys to {scale_factor:.0%}")
                        scaled = []
                        for o in buy_orders:
                            scaled_qty = o["qty"] * scale_factor
                            if not ALLOW_FRACTIONAL:
                                scaled_qty = int(scaled_qty)
                            # Ensure order meets minimum notional ($1 Alpaca minimum)
                            scaled_notional = scaled_qty * o["est_price"]
                            if scaled_qty > 0 and scaled_notional >= 1.0:
                                o = dict(o)
                                o["qty"] = scaled_qty
                                scaled.append(o)
                            elif scaled_qty > 0:
                                log.info(f"[DRIFT_MINI] Skipping {o['symbol']} — notional ${scaled_notional:.2f} below $1 minimum")
                        buy_orders = scaled
                else:
                    log.info(f"[DRIFT_MINI] Buying power OK | need=${total_buy_notional:,.0f} available=${available_bp:,.0f}")
            except Exception as bp_err:
                log.warning(f"[DRIFT_MINI] Could not check buying power: {bp_err} | proceeding cautiously")

        for o in buy_orders:
            try:
                order_req = MarketOrderRequest(
                    symbol=o["symbol"], qty=o["qty"],
                    side=OrderSide.BUY, time_in_force=TimeInForce.DAY
                )
                trading.submit_order(order_req)
                trades_executed += 1
                log_trade(
                    path=LOG_PATH, action="buy", symbol=o["symbol"],
                    qty=o["qty"], est_price=o["est_price"],
                    notional=o["qty"] * o["est_price"],
                    reason="drift_mini_rebalance",
                    target_weight=o["target_weight"],
                    current_weight=o["current_weight"],
                    portfolio_equity=total_equity
                )
                log.info(f"[DRIFT_MINI] buy {o['qty']:.4f} {o['symbol']} @ ~${o['est_price']:.2f}")
            except Exception as e:
                log.error(f"[DRIFT_MINI] Failed to buy {o['symbol']}: {e}")

        # Record timestamp so main loop respects DRIFT_CHECK_INTERVAL_MIN
        state.last_drift_mini_iso = now_et().isoformat()

        log.info(f"[DRIFT_MINI] Complete - {trades_executed} trades executed")

    except Exception as e:
        log.error(f"[DRIFT_MINI] Failed: {e}")
        traceback.print_exc()


# =========================
# DYNAMIC CAPITAL DEPLOYMENT (CAGR Edge)
# =========================

def compute_dynamic_capital_usage(
    risk_score: float,
    spy_risk_on: bool,
    event_mult: float = 1.0,
    in_cooldown: bool = False
) -> Tuple[float, str]:
    """
    Compute dynamic capital deployment based on market conditions.

    Replaces static MAX_CAPITAL_USAGE_PCT to eliminate permanent cash drag.

    Args:
        risk_score: Market risk score (0-100)
        spy_risk_on: SPY regime (True = risk-on)
        event_mult: Event risk multiplier (0.8-1.0)
        in_cooldown: Drawdown cooldown active

    Returns:
        Tuple of (capital_usage_pct, reason_string)
    """
    if not ENABLE_DYNAMIC_CAPITAL:
        return CAPITAL_USAGE_BASE, "static"

    # During drawdown cooldown, use very conservative deployment
    if in_cooldown:
        return 0.35, "cooldown"

    # Find matching tier
    for min_score, regime_required, capital_pct in CAPITAL_DEPLOYMENT_TIERS:
        # Check if this tier matches
        if risk_score >= min_score:
            if regime_required is None:
                # Fallback tier - always matches
                base_pct = capital_pct
                reason = f"fallback (score={risk_score:.0f})"
                break
            elif regime_required == spy_risk_on:
                base_pct = capital_pct
                regime_str = "risk_on" if spy_risk_on else "risk_off"
                reason = f"{regime_str} score={risk_score:.0f}"
                break
    else:
        # No tier matched - use base
        base_pct = CAPITAL_USAGE_BASE
        reason = "no_match"

    # Apply event multiplier (reduces deployment before FOMC etc)
    final_pct = base_pct * event_mult

    if event_mult < 1.0:
        reason += f" (event: {event_mult:.0%})"

    return final_pct, reason


# =========================
# VOL-TARGETED LEVERAGE (CAGR Edge)
# =========================

def compute_portfolio_vol(
    bars: pd.DataFrame,
    weights: Dict[str, float],
    trading_client: TradingClient,
    lookback: int = LEVERAGE_VOL_LOOKBACK
) -> float:
    """
    Estimate portfolio realized volatility based on current weights.

    Uses a simplified approach: weighted average of individual vols.
    (Full covariance approach would be more accurate but more complex)

    Args:
        bars: Historical price data
        weights: Current target weights {symbol: weight}
        trading_client: For market hours check
        lookback: Vol lookback period

    Returns:
        Estimated annualized portfolio vol
    """
    if not weights:
        return 0.15  # Default

    weighted_vol = 0.0
    total_weight = 0.0

    for sym, weight in weights.items():
        if weight <= 0 or sym == CASH_TICKER:
            continue

        try:
            close = get_close_series(bars, sym, strip_incomplete=True, trading_client=trading_client)
            vol = annualized_realized_vol(close, lookback) if len(close) >= lookback else 0.15
            vol = max(vol, VOL_FLOOR_ANNUAL)
            weighted_vol += vol * weight
            total_weight += weight
        except Exception:
            continue

    if total_weight > 0:
        return weighted_vol / total_weight
    return 0.15


def compute_vol_targeted_leverage(
    portfolio_vol: float,
    risk_score: float,
    spy_risk_on: bool,
    vix_estimate: float
) -> Tuple[float, str]:
    """
    Compute leverage ratio using vol-targeting.

    Leverage = clamp(target_vol / realized_vol, 1.0, MAX_LEVERAGE_RATIO)

    Only applies leverage when conditions are favorable.

    Args:
        portfolio_vol: Estimated portfolio realized vol
        risk_score: Market risk score
        spy_risk_on: SPY regime
        vix_estimate: Estimated VIX

    Returns:
        Tuple of (leverage_ratio, reason_string)
    """
    if not ENABLE_CONDITIONAL_LEVERAGE:
        return 1.0, "disabled"

    # Gate checks (must pass all)
    if not spy_risk_on:
        return 1.0, "risk_off_regime"

    if risk_score < LEVERAGE_RISK_SCORE_MIN:
        return 1.0, f"risk_score_low ({risk_score:.0f} < {LEVERAGE_RISK_SCORE_MIN})"

    if vix_estimate >= LEVERAGE_VIX_MAX:
        return 1.0, f"vix_high ({vix_estimate:.1f} >= {LEVERAGE_VIX_MAX})"

    # Vol-targeting approach
    if ENABLE_VOL_TARGETED_LEVERAGE and portfolio_vol > 0:
        raw_leverage = LEVERAGE_TARGET_VOL / portfolio_vol
        leverage = max(1.0, min(raw_leverage, MAX_LEVERAGE_RATIO))
        return leverage, f"vol_target ({portfolio_vol:.1%} -> {leverage:.2f}x)"
    else:
        # Static leverage (old approach)
        return MAX_LEVERAGE_RATIO, "static_max"


# =========================
# TACTICAL OVERLAYS (CAGR Edge)
# =========================

def compute_credit_risk_signal(
    bars: pd.DataFrame,
    trading_client: TradingClient
) -> Tuple[float, str]:
    """
    Compute credit/risk appetite signal from HYG/IEF ratio.

    HYG = High Yield Corporate Bonds (risk-on)
    IEF = 7-10 Year Treasury (risk-off)

    Rising HYG/IEF = risk appetite increasing (bullish)
    Falling HYG/IEF = risk appetite decreasing (bearish)

    Returns:
        Tuple of (signal, reason) where signal is -1 to +1
    """
    try:
        hyg_close = get_close_series(bars, "HYG", strip_incomplete=True, trading_client=trading_client)
        ief_close = get_close_series(bars, "IEF", strip_incomplete=True, trading_client=trading_client)

        if len(hyg_close) < 50 or len(ief_close) < 50:
            return 0.0, "insufficient_data"

        # Align series
        min_len = min(len(hyg_close), len(ief_close))
        hyg = hyg_close.iloc[-min_len:]
        ief = ief_close.iloc[-min_len:]

        # Credit spread ratio
        ratio = hyg / ief
        ratio_sma20 = ratio.rolling(20).mean()

        current_ratio = float(ratio.iloc[-1])
        sma_ratio = float(ratio_sma20.iloc[-1])

        # Signal: above SMA = risk-on, below = risk-off
        if current_ratio > sma_ratio * 1.01:
            return 0.5, f"credit_risk_on (ratio {current_ratio:.3f} > sma {sma_ratio:.3f})"
        elif current_ratio < sma_ratio * 0.99:
            return -0.5, f"credit_risk_off (ratio {current_ratio:.3f} < sma {sma_ratio:.3f})"
        else:
            return 0.0, "credit_neutral"

    except Exception as e:
        log.debug(f"[TACTICAL] Credit signal failed: {e}")
        return 0.0, f"error: {e}"


def compute_rates_vol_signal(
    bars: pd.DataFrame,
    trading_client: TradingClient
) -> Tuple[float, str]:
    """
    Compute rates volatility signal from TLT vol.

    High TLT vol = rates uncertainty = risk-off
    Low TLT vol = rates stable = risk-on

    Returns:
        Tuple of (signal, reason) where signal is -1 to +1
    """
    try:
        tlt_close = get_close_series(bars, "TLT", strip_incomplete=True, trading_client=trading_client)

        if len(tlt_close) < 60:
            return 0.0, "insufficient_data"

        # TLT realized vol (proxy for rates uncertainty)
        tlt_vol = annualized_realized_vol(tlt_close, 20)

        # Thresholds (TLT typically 10-20% vol)
        if tlt_vol < 0.12:
            return 0.3, f"rates_stable (vol {tlt_vol:.1%})"
        elif tlt_vol > 0.18:
            return -0.3, f"rates_volatile (vol {tlt_vol:.1%})"
        else:
            return 0.0, f"rates_normal (vol {tlt_vol:.1%})"

    except Exception as e:
        log.debug(f"[TACTICAL] Rates signal failed: {e}")
        return 0.0, f"error: {e}"


def compute_tactical_adjustment(
    bars: pd.DataFrame,
    trading_client: TradingClient
) -> Tuple[float, Dict[str, Any]]:
    """
    Compute combined tactical adjustment from credit and rates signals.

    Returns:
        Tuple of (exposure_adjustment, components_dict)
        exposure_adjustment: multiplier 0.85-1.15 to apply to base exposure
    """
    components = {}

    credit_signal, credit_reason = compute_credit_risk_signal(bars, trading_client)
    rates_signal, rates_reason = compute_rates_vol_signal(bars, trading_client)

    components["credit_signal"] = credit_signal
    components["credit_reason"] = credit_reason
    components["rates_signal"] = rates_signal
    components["rates_reason"] = rates_reason

    # Combined signal (weighted average)
    combined_signal = 0.6 * credit_signal + 0.4 * rates_signal
    components["combined_signal"] = combined_signal

    # Map to exposure adjustment (small impact: ±15%)
    # Signal +1 -> 1.15x, Signal -1 -> 0.85x
    adjustment = 1.0 + (combined_signal * 0.15)
    adjustment = max(0.85, min(1.15, adjustment))

    components["exposure_adjustment"] = adjustment

    log.debug(f"[TACTICAL] Credit={credit_signal:+.2f}, Rates={rates_signal:+.2f}, "
             f"Combined={combined_signal:+.2f}, Adj={adjustment:.2f}x")

    return adjustment, components


# =========================
# POSITION HEALTH REPORT (Phase 3)
# =========================

@dataclass
class PositionHealth:
    """Health metrics for a single position."""
    symbol: str
    qty: float
    cost_basis: float
    current_price: float
    market_value: float
    unrealized_pnl: float
    unrealized_pnl_pct: float

    # Health indicators
    above_sma200: bool
    distance_from_sma200_pct: float
    above_sma50: bool
    consecutive_closes_below_sma200: int
    volatility_annual: float
    momentum_score: Optional[float]

    # Risk status
    health_score: float  # 0-100, higher = healthier
    risk_level: str  # "healthy", "caution", "distressed", "critical"
    alerts: List[str]


def compute_position_health(
    symbol: str,
    position: Any,  # Alpaca Position object
    bars: pd.DataFrame,
    trading_client: TradingClient,
    state: BotState
) -> PositionHealth:
    """
    Compute health metrics for a single position.

    Health score components:
    - Trend alignment (above/below SMA200)
    - Distance from SMA200
    - Momentum score
    - Consecutive closes below SMA
    - P&L status

    Args:
        symbol: Ticker symbol
        position: Alpaca Position object
        bars: Historical price data
        trading_client: For market hours check
        state: Bot state for historical close tracking

    Returns:
        PositionHealth dataclass with all metrics
    """
    alerts = []

    # Extract position info
    qty = float(position.qty)
    cost_basis = float(position.avg_entry_price) * qty
    current_price = float(position.current_price)
    market_value = float(position.market_value)
    unrealized_pnl = float(position.unrealized_pl)
    unrealized_pnl_pct = float(position.unrealized_plpc) * 100

    # Get price series
    try:
        close = get_close_series(bars, symbol, strip_incomplete=True, trading_client=trading_client)
    except Exception as e:
        log.warning(f"[HEALTH] Failed to get close series for {symbol}: {e}")
        # Return with minimal data
        return PositionHealth(
            symbol=symbol,
            qty=qty,
            cost_basis=cost_basis,
            current_price=current_price,
            market_value=market_value,
            unrealized_pnl=unrealized_pnl,
            unrealized_pnl_pct=unrealized_pnl_pct,
            above_sma200=True,  # Default
            distance_from_sma200_pct=0.0,
            above_sma50=True,
            consecutive_closes_below_sma200=0,
            volatility_annual=0.20,
            momentum_score=None,
            health_score=50.0,
            risk_level="unknown",
            alerts=[f"Failed to compute metrics: {e}"]
        )

    # Calculate indicators
    last_close = float(close.iloc[-1])

    # SMA200
    sma200_val = sma(close, SMA_LOOKBACK_DAYS) if len(close) >= SMA_LOOKBACK_DAYS else last_close
    above_sma200 = last_close > sma200_val
    distance_from_sma200_pct = ((last_close - sma200_val) / sma200_val * 100) if sma200_val > 0 else 0.0

    # SMA50
    sma50_val = sma(close, 50) if len(close) >= 50 else last_close
    above_sma50 = last_close > sma50_val

    # Consecutive closes below SMA200 (count how many recent closes are below)
    closes_below = 0
    threshold = sma200_val * STOP_LOSS_SMA200_BUFFER
    for i in range(1, min(6, len(close) + 1)):  # Check last 5 closes
        if float(close.iloc[-i]) < threshold:
            closes_below += 1
        else:
            break  # Stop at first close above threshold

    # Volatility
    vol_annual = annualized_realized_vol(close, VOL_LOOKBACK_DAYS) if len(close) >= VOL_LOOKBACK_DAYS else 0.20
    vol_annual = max(vol_annual, VOL_FLOOR_ANNUAL)

    # Momentum score
    mom_score = compute_momentum_score(close)

    # === HEALTH SCORE CALCULATION ===
    # Start at 100, deduct for risk factors
    health_score = 100.0

    # Trend alignment (max -30)
    if not above_sma200:
        health_score -= 20
        if not above_sma50:
            health_score -= 10

    # Distance from SMA200 (max -20)
    if distance_from_sma200_pct < -5:
        health_score -= 10
        alerts.append(f"Price {distance_from_sma200_pct:.1f}% below SMA200")
    if distance_from_sma200_pct < -10:
        health_score -= 10
        alerts.append("Significant trend breakdown")

    # Consecutive closes below SMA (max -20)
    if closes_below >= 1:
        health_score -= 10
        alerts.append(f"{closes_below} consecutive close(s) below SMA200")
    if closes_below >= 2:
        health_score -= 10
        alerts.append("EXIT SIGNAL: 2+ closes below SMA200")

    # P&L status (max -20)
    if unrealized_pnl_pct < -5:
        health_score -= 10
        alerts.append(f"Underwater {unrealized_pnl_pct:.1f}%")
    if unrealized_pnl_pct < -10:
        health_score -= 10

    # Momentum (max -10)
    if mom_score is not None and mom_score < -0.05:
        health_score -= 5
        alerts.append(f"Negative momentum: {mom_score:.2%}")
    if mom_score is not None and mom_score < -0.10:
        health_score -= 5

    # High volatility (max -10)
    if vol_annual > 0.40:
        health_score -= 5
        alerts.append(f"High volatility: {vol_annual:.0%} annualized")
    if vol_annual > 0.60:
        health_score -= 5

    # Clamp to [0, 100]
    health_score = max(0.0, min(100.0, health_score))

    # Determine risk level
    if health_score >= 70:
        risk_level = "healthy"
    elif health_score >= 50:
        risk_level = "caution"
    elif health_score >= 30:
        risk_level = "distressed"
    else:
        risk_level = "critical"

    return PositionHealth(
        symbol=symbol,
        qty=qty,
        cost_basis=cost_basis,
        current_price=current_price,
        market_value=market_value,
        unrealized_pnl=unrealized_pnl,
        unrealized_pnl_pct=unrealized_pnl_pct,
        above_sma200=above_sma200,
        distance_from_sma200_pct=distance_from_sma200_pct,
        above_sma50=above_sma50,
        consecutive_closes_below_sma200=closes_below,
        volatility_annual=vol_annual,
        momentum_score=mom_score,
        health_score=health_score,
        risk_level=risk_level,
        alerts=alerts
    )


def generate_position_health_report(
    bars: pd.DataFrame,
    trading_client: TradingClient,
    state: BotState,
    include_healthy: bool = False
) -> Tuple[List[PositionHealth], Dict[str, Any]]:
    """
    Generate health report for all positions.

    Args:
        bars: Historical price data
        trading_client: Alpaca trading client
        state: Bot state
        include_healthy: Include healthy positions in alerts (default False)

    Returns:
        Tuple of (list of PositionHealth, summary dict)
    """
    positions = trading_client.get_all_positions()

    health_reports = []
    critical_count = 0
    distressed_count = 0
    caution_count = 0
    healthy_count = 0
    total_unrealized_pnl = 0.0

    for pos in positions:
        symbol = pos.symbol

        # Skip cash-like positions
        if symbol == CASH_TICKER:
            continue

        health = compute_position_health(symbol, pos, bars, trading_client, state)
        health_reports.append(health)

        total_unrealized_pnl += health.unrealized_pnl

        if health.risk_level == "critical":
            critical_count += 1
        elif health.risk_level == "distressed":
            distressed_count += 1
        elif health.risk_level == "caution":
            caution_count += 1
        else:
            healthy_count += 1

    # Sort by health score (worst first)
    health_reports.sort(key=lambda x: x.health_score)

    summary = {
        "total_positions": len(health_reports),
        "critical_count": critical_count,
        "distressed_count": distressed_count,
        "caution_count": caution_count,
        "healthy_count": healthy_count,
        "total_unrealized_pnl": total_unrealized_pnl,
        "portfolio_health_score": (
            sum(h.health_score for h in health_reports) / len(health_reports)
            if health_reports else 100.0
        ),
        "needs_attention": critical_count > 0 or distressed_count > 0,
    }

    # Log report
    log.info(f"[HEALTH_REPORT] Portfolio: {summary['portfolio_health_score']:.0f}/100 | "
            f"Critical: {critical_count}, Distressed: {distressed_count}, "
            f"Caution: {caution_count}, Healthy: {healthy_count}")

    # Log alerts for non-healthy positions
    for health in health_reports:
        if health.risk_level != "healthy" or include_healthy:
            log.warning(f"[HEALTH] {health.symbol}: {health.risk_level.upper()} "
                       f"(score={health.health_score:.0f}) | "
                       f"P&L={health.unrealized_pnl_pct:+.1f}% | "
                       f"Alerts: {', '.join(health.alerts) if health.alerts else 'none'}")

    return health_reports, summary


def format_health_report_email(
    health_reports: List[PositionHealth],
    summary: Dict[str, Any]
) -> str:
    """
    Format health report as email content.

    Args:
        health_reports: List of PositionHealth objects
        summary: Summary dictionary

    Returns:
        Formatted email string
    """
    lines = [
        "TREND BOT - POSITION HEALTH REPORT",
        "=" * 40,
        "",
        f"Portfolio Health Score: {summary['portfolio_health_score']:.0f}/100",
        f"Total Positions: {summary['total_positions']}",
        f"Unrealized P&L: ${summary['total_unrealized_pnl']:,.2f}",
        "",
        f"Critical: {summary['critical_count']} | "
        f"Distressed: {summary['distressed_count']} | "
        f"Caution: {summary['caution_count']} | "
        f"Healthy: {summary['healthy_count']}",
        "",
    ]

    # Add position details (worst first)
    if health_reports:
        lines.append("-" * 40)
        lines.append("POSITION DETAILS (sorted by health score):")
        lines.append("")

        for h in health_reports:
            status_icon = {
                "critical": "[!!!]",
                "distressed": "[!!]",
                "caution": "[!]",
                "healthy": "[OK]",
                "unknown": "[?]",
            }.get(h.risk_level, "[?]")

            lines.append(f"{status_icon} {h.symbol}: {h.risk_level.upper()} (score={h.health_score:.0f})")
            lines.append(f"   Price: ${h.current_price:.2f} | P&L: {h.unrealized_pnl_pct:+.1f}%")
            lines.append(f"   SMA200: {'Above' if h.above_sma200 else 'BELOW'} ({h.distance_from_sma200_pct:+.1f}%)")
            if h.consecutive_closes_below_sma200 > 0:
                lines.append(f"   WARNING: {h.consecutive_closes_below_sma200} consecutive close(s) below SMA200")
            if h.alerts:
                for alert in h.alerts:
                    lines.append(f"   → {alert}")
            lines.append("")

    lines.append("-" * 40)
    lines.append(f"Generated: {now_et().strftime('%Y-%m-%d %H:%M:%S')} ET")

    return "\n".join(lines)


# =========================
# MARKET HEALTH (Phase 3.1b)
# =========================

@dataclass
class MarketHealth:
    """
    Market-level health metrics for daily monitoring context.

    These metrics inform (but do not trade) - they provide context for
    understanding position health and anticipating upcoming risk events.
    """
    # Regime state
    spy_regime: str                    # "risk_on" or "risk_off"
    spy_close: float
    spy_sma200: float
    spy_distance_from_sma200_pct: float

    # Breadth proxies
    rsp_spy_ratio_vs_sma: float        # RSP/SPY ratio vs its 20-day SMA (>1 = broad participation)
    iwm_spy_ratio_vs_sma: float        # IWM/SPY ratio vs its 20-day SMA (>1 = risk-on small caps)
    breadth_score: float               # Combined breadth score (0-100)

    # Volatility context
    spy_volatility_annual: float       # SPY annualized realized vol
    vix_level: Optional[float]         # VIX level if available (real or proxy)
    vix_source: str                    # "polygon" (real) or "estimated" (from realized vol)
    vol_regime: str                    # "low", "normal", "elevated", "high"

    # Real index data (from Polygon Indices API, if available)
    real_spx_value: Optional[float]    # Real S&P 500 index value
    real_spx_sma200: Optional[float]   # Real S&P 500 SMA200
    real_spx_pct_vs_sma: Optional[float]  # Real S&P 500 % vs SMA200

    # Event risk
    upcoming_events: List[str]         # List of upcoming macro events
    event_risk_level: str              # "high", "medium", "low", "none"
    event_risk_multiplier: float       # Exposure multiplier (e.g., 0.80 for 20% reduction)

    # Composite scores
    risk_score: float                  # Overall risk score (0-100, higher = more bullish)
    market_health_score: float         # Composite market health (0-100)
    health_level: str                  # "healthy", "caution", "stressed", "critical"

    # Timestamps
    as_of_timestamp: str               # ISO timestamp of computation


def compute_market_health(
    bars: pd.DataFrame,
    state: BotState,
    trading_client: TradingClient
) -> MarketHealth:
    """
    Compute market-level health metrics for daily monitoring.

    This function aggregates:
    - Canonical regime state (SPY vs SMA200 with hysteresis)
    - Breadth proxies (RSP/SPY, IWM/SPY)
    - Volatility regime
    - Upcoming macro event risk

    The output is INFORMATIVE ONLY - it does not trigger trades.
    Daily monitoring uses this for context when reporting position health.

    Args:
        bars: Historical price data (must include SPY, RSP, IWM)
        state: Bot state (for canonical regime)
        trading_client: For market hours check

    Returns:
        MarketHealth dataclass with all metrics
    """
    # === SPY REGIME STATE ===
    try:
        spy_close_series = get_close_series(bars, "SPY", strip_incomplete=True, trading_client=trading_client)
        spy_close = float(spy_close_series.iloc[-1])
        spy_sma200 = sma(spy_close_series, SMA_LOOKBACK_DAYS) if len(spy_close_series) >= SMA_LOOKBACK_DAYS else spy_close
        spy_distance_pct = ((spy_close - spy_sma200) / spy_sma200 * 100) if spy_sma200 > 0 else 0.0
        spy_vol_annual = annualized_realized_vol(spy_close_series, VOL_LOOKBACK_DAYS) if len(spy_close_series) >= VOL_LOOKBACK_DAYS else 0.15
    except Exception as e:
        log.warning(f"[MARKET_HEALTH] Failed to get SPY data: {e}")
        spy_close = 0.0
        spy_sma200 = 0.0
        spy_distance_pct = 0.0
        spy_vol_annual = 0.15

    # Get canonical regime from state
    spy_regime = state.spy_regime if state.spy_regime else "risk_on"

    # === BREADTH PROXIES ===
    # RSP/SPY ratio (equal-weight vs cap-weight - measures breadth of participation)
    try:
        rsp_close_series = get_close_series(bars, "RSP", strip_incomplete=True, trading_client=trading_client)
        rsp_close = float(rsp_close_series.iloc[-1])
        rsp_spy_ratio = rsp_close / spy_close if spy_close > 0 else 1.0

        # Calculate ratio vs its 20-day SMA
        rsp_spy_ratio_series = rsp_close_series / spy_close_series.iloc[-len(rsp_close_series):]
        rsp_spy_sma20 = sma(rsp_spy_ratio_series, 20) if len(rsp_spy_ratio_series) >= 20 else rsp_spy_ratio
        rsp_spy_ratio_vs_sma = rsp_spy_ratio / rsp_spy_sma20 if rsp_spy_sma20 > 0 else 1.0
    except Exception:
        rsp_spy_ratio_vs_sma = 1.0  # Neutral if data unavailable

    # IWM/SPY ratio (small cap vs large cap - measures risk appetite)
    try:
        iwm_close_series = get_close_series(bars, "IWM", strip_incomplete=True, trading_client=trading_client)
        iwm_close = float(iwm_close_series.iloc[-1])
        iwm_spy_ratio = iwm_close / spy_close if spy_close > 0 else 1.0

        # Calculate ratio vs its 20-day SMA
        iwm_spy_ratio_series = iwm_close_series / spy_close_series.iloc[-len(iwm_close_series):]
        iwm_spy_sma20 = sma(iwm_spy_ratio_series, 20) if len(iwm_spy_ratio_series) >= 20 else iwm_spy_ratio
        iwm_spy_ratio_vs_sma = iwm_spy_ratio / iwm_spy_sma20 if iwm_spy_sma20 > 0 else 1.0
    except Exception:
        iwm_spy_ratio_vs_sma = 1.0  # Neutral if data unavailable

    # Combined breadth score (50 = neutral, >50 = healthy breadth, <50 = narrow)
    # RSP/SPY > 1 means equal-weight outperforming = broad participation
    # IWM/SPY > 1 means small caps outperforming = risk appetite strong
    rsp_component = 50 + (rsp_spy_ratio_vs_sma - 1.0) * 500  # Scale: 1% deviation = 5 points
    iwm_component = 50 + (iwm_spy_ratio_vs_sma - 1.0) * 500
    breadth_score = 0.55 * rsp_component + 0.45 * iwm_component
    breadth_score = max(0.0, min(100.0, breadth_score))  # Clamp to 0-100

    # === VOLATILITY REGIME ===
    # Prefer real VIX from Polygon; fall back to SPY realized vol estimate
    real_vix = fetch_real_vix()
    vix_source = "estimated"
    if real_vix is not None:
        vix_proxy = real_vix
        vix_source = "polygon"
    else:
        vix_proxy = spy_vol_annual * 100  # Rough approximation (realized vol as VIX proxy)

    # === REAL INDEX DATA ===
    # Fetch actual S&P 500 index data from Polygon for comparison
    spx_data = fetch_real_spx_data()
    real_spx_value = spx_data.get("value") if spx_data else None
    real_spx_sma200 = spx_data.get("sma") if spx_data else None
    real_spx_pct_vs_sma = spx_data.get("pct_vs_sma") if spx_data else None

    if vix_proxy < 15:
        vol_regime = "low"
    elif vix_proxy < 20:
        vol_regime = "normal"
    elif vix_proxy < 30:
        vol_regime = "elevated"
    else:
        vol_regime = "high"

    # === EVENT RISK ===
    upcoming = get_upcoming_events(days_ahead=3)
    event_names = [f"{e.name} ({e.days_until}d)" for e in upcoming]

    if upcoming:
        # Get highest risk level from upcoming events
        risk_levels = [e.risk_level for e in upcoming]
        if "high" in risk_levels:
            event_risk_level = "high"
            event_risk_mult = EVENT_RISK_MULTIPLIERS.get("high", 0.80)
        elif "medium" in risk_levels:
            event_risk_level = "medium"
            event_risk_mult = EVENT_RISK_MULTIPLIERS.get("medium", 0.90)
        elif "low" in risk_levels:
            event_risk_level = "low"
            event_risk_mult = EVENT_RISK_MULTIPLIERS.get("low", 0.95)
        else:
            event_risk_level = "none"
            event_risk_mult = 1.0
    else:
        event_risk_level = "none"
        event_risk_mult = 1.0

    # === COMPOSITE RISK SCORE ===
    # Use the existing compute_risk_score function if available
    try:
        risk_score = compute_risk_score(bars, trading_client)
    except Exception:
        # Fallback: simple regime-based score
        if spy_regime == "risk_on":
            risk_score = 60.0 + (breadth_score - 50) * 0.5
        else:
            risk_score = 40.0 + (breadth_score - 50) * 0.3
        risk_score = max(0.0, min(100.0, risk_score))

    # === MARKET HEALTH SCORE ===
    # Composite of regime, breadth, volatility, and event risk
    health_score = 100.0

    # Regime penalty (-20 if risk_off)
    if spy_regime == "risk_off":
        health_score -= 20

    # Breadth adjustment (-20 to +10)
    if breadth_score < 40:
        health_score -= 20
    elif breadth_score < 50:
        health_score -= 10
    elif breadth_score > 60:
        health_score += 5
    elif breadth_score > 70:
        health_score += 10

    # Volatility penalty
    if vol_regime == "high":
        health_score -= 20
    elif vol_regime == "elevated":
        health_score -= 10

    # Event risk penalty
    if event_risk_level == "high":
        health_score -= 15
    elif event_risk_level == "medium":
        health_score -= 10

    # SPY distance from SMA200
    if spy_distance_pct < -5:
        health_score -= 15
    elif spy_distance_pct < 0:
        health_score -= 5

    health_score = max(0.0, min(100.0, health_score))

    # Determine health level
    if health_score >= 70:
        health_level = "healthy"
    elif health_score >= 50:
        health_level = "caution"
    elif health_score >= 30:
        health_level = "stressed"
    else:
        health_level = "critical"

    return MarketHealth(
        spy_regime=spy_regime,
        spy_close=spy_close,
        spy_sma200=spy_sma200,
        spy_distance_from_sma200_pct=spy_distance_pct,
        rsp_spy_ratio_vs_sma=rsp_spy_ratio_vs_sma,
        iwm_spy_ratio_vs_sma=iwm_spy_ratio_vs_sma,
        breadth_score=breadth_score,
        spy_volatility_annual=spy_vol_annual,
        vix_level=vix_proxy,
        vix_source=vix_source,
        vol_regime=vol_regime,
        real_spx_value=real_spx_value,
        real_spx_sma200=real_spx_sma200,
        real_spx_pct_vs_sma=real_spx_pct_vs_sma,
        upcoming_events=event_names,
        event_risk_level=event_risk_level,
        event_risk_multiplier=event_risk_mult,
        risk_score=risk_score,
        market_health_score=health_score,
        health_level=health_level,
        as_of_timestamp=now_et().isoformat()
    )


def format_market_health_section(market_health: MarketHealth) -> str:
    """
    Format market health as a text section for reports.

    Args:
        market_health: MarketHealth dataclass

    Returns:
        Formatted string section
    """
    # Health level indicator (ASCII-safe for Windows console)
    level_icon = {
        "healthy": "[OK]",
        "caution": "[!]",
        "stressed": "[!!]",
        "critical": "[!!!]",
    }.get(market_health.health_level, "[?]")

    # Extract values with defensive type handling (in case any are accidentally tuples)
    def safe_float(val, default=0.0):
        if isinstance(val, tuple):
            return float(val[0]) if val else default
        return float(val) if val is not None else default

    spy_vol = safe_float(market_health.spy_volatility_annual, 0.15)
    risk_score = safe_float(market_health.risk_score, 50.0)
    health_score = safe_float(market_health.market_health_score, 50.0)
    spy_close = safe_float(market_health.spy_close, 0.0)
    spy_dist = safe_float(market_health.spy_distance_from_sma200_pct, 0.0)
    breadth = safe_float(market_health.breadth_score, 50.0)
    rsp_ratio = safe_float(market_health.rsp_spy_ratio_vs_sma, 1.0)
    iwm_ratio = safe_float(market_health.iwm_spy_ratio_vs_sma, 1.0)

    lines = [
        "MARKET CONTEXT",
        "-" * 40,
        f"{level_icon} Market Health: {market_health.health_level.upper()} ({health_score:.0f}/100)",
        "",
        f"Regime: {market_health.spy_regime.upper()}",
        f"SPY: ${spy_close:.2f} ({spy_dist:+.1f}% vs SMA200)",
        "",
        f"Breadth Score: {breadth:.0f}/100",
        f"  RSP/SPY vs SMA: {rsp_ratio:.3f}",
        f"  IWM/SPY vs SMA: {iwm_ratio:.3f}",
        "",
        f"Volatility: {market_health.vol_regime.upper()} ({spy_vol:.0%} ann.)",
    ]

    # VIX with source info
    vix_val = safe_float(market_health.vix_level, 0.0)
    vix_src = getattr(market_health, 'vix_source', 'estimated')
    lines.append(f"VIX: {vix_val:.1f} (source: {vix_src})")

    # Real S&P 500 index data if available
    real_spx = getattr(market_health, 'real_spx_value', None)
    real_sma = getattr(market_health, 'real_spx_sma200', None)
    real_pct = getattr(market_health, 'real_spx_pct_vs_sma', None)
    if real_spx is not None:
        spx_line = f"S&P 500: {real_spx:,.2f}"
        if real_sma is not None and real_pct is not None:
            spx_line += f" ({real_pct:+.2f}% vs SMA200={real_sma:,.2f})"
        lines.append(spx_line)

    lines.append(f"Risk Score: {risk_score:.0f}/100")

    # Add event risk if present
    if market_health.upcoming_events:
        event_mult = safe_float(market_health.event_risk_multiplier, 1.0)
        lines.append("")
        lines.append(f"Event Risk: {market_health.event_risk_level.upper()} (mult={event_mult:.0%})")
        for event in market_health.upcoming_events[:3]:  # Show up to 3 events
            lines.append(f"   -> {event}")

    return "\n".join(lines)


# =========================
# CALIBRATED PROBABILITY MODEL (Phase 3.2)
# =========================

@dataclass
class StopOutProbability:
    """Probability estimate for a position stopping out."""
    symbol: str
    prob_stop_5d: float       # Probability of stop within 5 trading days
    prob_stop_20d: float      # Probability of stop within 20 trading days
    confidence: str           # "high", "medium", "low" based on data quality
    factors: Dict[str, float]  # Contributing factors


def estimate_stop_probability(
    symbol: str,
    current_price: float,
    sma200: float,
    volatility_annual: float,
    risk_score: float,
    consecutive_below: int = 0,
    lookback_days_5d: int = 5,
    lookback_days_20d: int = 20
) -> StopOutProbability:
    """
    Estimate probability of a position triggering the 2-close stop rule.

    This model uses a simplified volatility-based approach:
    1. Calculate expected daily moves based on volatility
    2. Estimate probability of N consecutive closes below SMA200
    3. Adjust for current market regime and distance from SMA

    The model assumes:
    - Daily returns are roughly normal (simplified)
    - Stop triggers after 2 consecutive closes below SMA200
    - If already 1 close below, only need 1 more

    Args:
        symbol: Ticker symbol
        current_price: Current price
        sma200: Current SMA200 value
        volatility_annual: Annualized volatility
        risk_score: Market risk score (0-100)
        consecutive_below: Already consecutive closes below SMA
        lookback_days_5d: Short-term horizon
        lookback_days_20d: Medium-term horizon

    Returns:
        StopOutProbability dataclass
    """
    import math

    factors = {}

    # Convert annual vol to daily
    daily_vol = volatility_annual / math.sqrt(252)
    factors["daily_vol"] = daily_vol

    # Distance from SMA200 as percentage
    distance_pct = (current_price - sma200) / sma200 if sma200 > 0 else 0.0
    factors["distance_from_sma"] = distance_pct

    # Calculate probability of closing below SMA on any given day
    # Using a simplified normal approximation
    # P(close < SMA) ≈ P(return < -distance_pct)
    # = P(Z < -distance_pct / daily_vol)

    def normal_cdf(x: float) -> float:
        """Approximate standard normal CDF using error function approximation."""
        return 0.5 * (1 + math.erf(x / math.sqrt(2)))

    # Z-score for being below SMA tomorrow
    if daily_vol > 0:
        z_score = -distance_pct / daily_vol
        prob_below_sma_single_day = normal_cdf(z_score)
    else:
        prob_below_sma_single_day = 0.5  # Default to 50%

    factors["prob_below_single_day"] = prob_below_sma_single_day

    # Adjust for consecutive days needed
    # If already 1 below, need only 1 more
    # If 0 below, need 2 consecutive
    closes_needed = max(0, 2 - consecutive_below)
    factors["closes_needed"] = closes_needed

    if closes_needed == 0:
        # Already at stop level
        prob_stop_5d = 1.0
        prob_stop_20d = 1.0
    elif closes_needed == 1:
        # Need one more close below
        # In N days, probability of at least one close below
        # = 1 - (1 - p)^N
        prob_stop_5d = 1 - (1 - prob_below_sma_single_day) ** lookback_days_5d
        prob_stop_20d = 1 - (1 - prob_below_sma_single_day) ** lookback_days_20d
    else:
        # Need 2 consecutive closes below
        # Probability of 2 consecutive in N trials is complex
        # Simplified: P(at least one pair of consecutive) ≈ (N-1) * p^2 * (1-p)^... (approximation)
        # For simplicity, use: 1 - (1 - p^2)^(N/2)
        prob_two_consecutive = prob_below_sma_single_day ** 2
        prob_stop_5d = 1 - (1 - prob_two_consecutive) ** (lookback_days_5d / 2)
        prob_stop_20d = 1 - (1 - prob_two_consecutive) ** (lookback_days_20d / 2)

    factors["prob_stop_raw_5d"] = prob_stop_5d
    factors["prob_stop_raw_20d"] = prob_stop_20d

    # === REGIME ADJUSTMENT ===
    # Low risk score (bearish market) increases stop probability
    # High risk score (bullish market) decreases stop probability
    regime_mult = 1.0
    if risk_score < 30:
        regime_mult = 1.3  # 30% increase in stop probability during risk-off
    elif risk_score < 50:
        regime_mult = 1.1
    elif risk_score > 70:
        regime_mult = 0.85  # 15% decrease during risk-on
    elif risk_score > 85:
        regime_mult = 0.75

    factors["regime_mult"] = regime_mult

    # Apply regime adjustment
    prob_stop_5d = min(1.0, prob_stop_5d * regime_mult)
    prob_stop_20d = min(1.0, prob_stop_20d * regime_mult)

    # === DISTANCE ADJUSTMENT ===
    # Very close to SMA has higher stop probability regardless of vol
    if abs(distance_pct) < 0.02:  # Within 2% of SMA
        distance_boost = 1.2
        prob_stop_5d = min(1.0, prob_stop_5d * distance_boost)
        prob_stop_20d = min(1.0, prob_stop_20d * distance_boost)
        factors["distance_boost"] = distance_boost

    # Determine confidence based on data quality
    if daily_vol > 0.10 and abs(distance_pct) > 0.05:
        confidence = "high"
    elif daily_vol > 0.05:
        confidence = "medium"
    else:
        confidence = "low"

    return StopOutProbability(
        symbol=symbol,
        prob_stop_5d=float(prob_stop_5d),
        prob_stop_20d=float(prob_stop_20d),
        confidence=confidence,
        factors=factors
    )


def compute_position_probabilities(
    bars: pd.DataFrame,
    trading_client: TradingClient,
    state: BotState,
    risk_score: float = 50.0
) -> List[StopOutProbability]:
    """
    Compute stop-out probabilities for all positions.

    Args:
        bars: Historical price data
        trading_client: Alpaca trading client
        state: Bot state
        risk_score: Current market risk score

    Returns:
        List of StopOutProbability objects
    """
    positions = trading_client.get_all_positions()
    probabilities = []

    for pos in positions:
        symbol = pos.symbol

        # Skip cash
        if symbol == CASH_TICKER:
            continue

        try:
            close = get_close_series(bars, symbol, strip_incomplete=True, trading_client=trading_client)
            current_price = float(close.iloc[-1])

            # Calculate indicators
            sma200_val = sma(close, SMA_LOOKBACK_DAYS) if len(close) >= SMA_LOOKBACK_DAYS else current_price
            vol_annual = annualized_realized_vol(close, VOL_LOOKBACK_DAYS) if len(close) >= VOL_LOOKBACK_DAYS else 0.20

            # Get consecutive closes below SMA (count)
            consecutive = 0
            threshold = sma200_val * STOP_LOSS_SMA200_BUFFER
            for i in range(1, min(6, len(close) + 1)):
                if float(close.iloc[-i]) < threshold:
                    consecutive += 1
                else:
                    break

            prob = estimate_stop_probability(
                symbol=symbol,
                current_price=current_price,
                sma200=sma200_val,
                volatility_annual=vol_annual,
                risk_score=risk_score,
                consecutive_below=consecutive
            )
            probabilities.append(prob)

            log.debug(f"[PROB] {symbol}: P(stop 5d)={prob.prob_stop_5d:.1%}, "
                     f"P(stop 20d)={prob.prob_stop_20d:.1%}, conf={prob.confidence}")

        except Exception as e:
            log.warning(f"[PROB] Failed to compute for {symbol}: {e}")
            continue

    # Sort by 5-day probability (highest first)
    probabilities.sort(key=lambda x: x.prob_stop_5d, reverse=True)

    return probabilities


def format_probability_report(probabilities: List[StopOutProbability]) -> str:
    """
    Format probability report as readable text.

    Args:
        probabilities: List of StopOutProbability objects

    Returns:
        Formatted string
    """
    lines = [
        "TREND BOT - STOP PROBABILITY REPORT",
        "=" * 45,
        "",
        "Position probabilities of triggering 2-close stop:",
        "",
        f"{'Symbol':<8} {'5-Day':>8} {'20-Day':>8} {'Conf':>8}",
        "-" * 45,
    ]

    for p in probabilities:
        # Risk indicator
        if p.prob_stop_5d > 0.30:
            indicator = "⚠️ HIGH"
        elif p.prob_stop_5d > 0.15:
            indicator = "MODERATE"
        else:
            indicator = "LOW"

        lines.append(f"{p.symbol:<8} {p.prob_stop_5d:>7.1%} {p.prob_stop_20d:>8.1%} {p.confidence:>8} {indicator}")

    lines.append("-" * 45)
    lines.append(f"Generated: {now_et().strftime('%Y-%m-%d %H:%M:%S')} ET")
    lines.append("")
    lines.append("Note: Probabilities are estimates based on volatility and current")
    lines.append("distance from SMA200. Actual outcomes may vary.")

    return "\n".join(lines)


# =========================
# MACRO EVENT-RISK CALENDAR (Phase 3.3)
# =========================
# Now loaded from config/event_calendar.yaml for easy annual updates.
# See _EVENT_CALENDAR_CONFIG loaded at module init.

# Extract settings from loaded config (with fallbacks)
_event_risk_cfg = _EVENT_CALENDAR_CONFIG.get("event_risk", {})
EVENT_RISK_MULTIPLIERS = _event_risk_cfg.get("multipliers", {
    "high": 0.80,
    "medium": 0.90,
    "low": 0.95,
    "none": 1.00,
})
EVENT_LEAD_DAYS = _event_risk_cfg.get("lead_days", {
    "high": 1,
    "medium": 0,
    "low": 0,
})


def _get_event_dates_from_config(event_type: str) -> List[Tuple[int, int]]:
    """
    Extract event dates from loaded YAML config.

    Args:
        event_type: Event type key (e.g., "fomc", "cpi", "nfp", "gdp", "opex")

    Returns:
        List of (month, day) tuples for the event type
    """
    calendar = _EVENT_CALENDAR_CONFIG.get("calendar", {})
    events = calendar.get("events", {})
    event_cfg = events.get(event_type, {})
    dates_raw = event_cfg.get("dates", [])

    # Convert [[1, 29], [3, 19]] to [(1, 29), (3, 19)]
    return [(d[0], d[1]) for d in dates_raw if len(d) >= 2]


def _get_event_risk_level(event_type: str) -> str:
    """Get risk level for event type from config."""
    calendar = _EVENT_CALENDAR_CONFIG.get("calendar", {})
    events = calendar.get("events", {})
    event_cfg = events.get(event_type, {})
    return event_cfg.get("risk_level", "medium")


@dataclass
class MacroEvent:
    """Represents a macro event."""
    date: datetime.date
    name: str
    risk_level: str  # "high", "medium", "low"
    days_until: int


def get_upcoming_events(
    reference_date: Optional[datetime.date] = None,
    days_ahead: int = 5
) -> List[MacroEvent]:
    """
    Get upcoming macro events within the specified window.

    Loads events from config/event_calendar.yaml for easy annual updates.

    Args:
        reference_date: Date to check from (default: today)
        days_ahead: How many days ahead to look

    Returns:
        List of MacroEvent objects sorted by date
    """
    if reference_date is None:
        reference_date = now_et().date()

    events = []
    year = reference_date.year

    # Build event list for current year
    def add_events(dates: List[Tuple[int, int]], name: str, risk_level: str):
        for month, day in dates:
            try:
                event_date = date(year, month, day)
                days_until = (event_date - reference_date).days
                if 0 <= days_until <= days_ahead:
                    events.append(MacroEvent(
                        date=event_date,
                        name=name,
                        risk_level=risk_level,
                        days_until=days_until
                    ))
            except ValueError:
                # Invalid date (e.g., Feb 30)
                continue

    # Add all event types from loaded config
    for event_type in ["fomc", "cpi", "nfp", "gdp", "opex"]:
        dates = _get_event_dates_from_config(event_type)
        risk_level = _get_event_risk_level(event_type)
        add_events(dates, event_type.upper(), risk_level)

    # Sort by date
    events.sort(key=lambda x: x.date)

    return events


def get_event_risk_multiplier(
    reference_date: Optional[datetime.date] = None
) -> Tuple[float, List[MacroEvent]]:
    """
    Get the exposure multiplier based on upcoming macro events.

    Args:
        reference_date: Date to check (default: today)

    Returns:
        Tuple of (multiplier, list of active events)
    """
    if reference_date is None:
        reference_date = now_et().date()

    # Get events for the next few days
    events = get_upcoming_events(reference_date, days_ahead=2)

    if not events:
        return 1.0, []

    # Find the most impactful event considering lead days
    active_events = []
    min_multiplier = 1.0

    for event in events:
        lead_days = EVENT_LEAD_DAYS.get(event.risk_level, 0)

        # Check if we're within the event window
        if event.days_until <= lead_days:
            active_events.append(event)
            mult = EVENT_RISK_MULTIPLIERS.get(event.risk_level, 1.0)
            min_multiplier = min(min_multiplier, mult)

    return min_multiplier, active_events


def format_event_calendar(days_ahead: int = 10) -> str:
    """
    Format upcoming macro events as readable text.

    Args:
        days_ahead: How many days ahead to show

    Returns:
        Formatted string
    """
    events = get_upcoming_events(days_ahead=days_ahead)

    lines = [
        "TREND BOT - MACRO EVENT CALENDAR",
        "=" * 45,
        "",
        f"Upcoming events (next {days_ahead} days):",
        "",
        f"{'Date':<12} {'Event':<8} {'Risk':<8} {'Days':>6}",
        "-" * 45,
    ]

    if not events:
        lines.append("No major events in the upcoming window.")
    else:
        for event in events:
            risk_icon = {
                "high": "[!!!]",
                "medium": "[!]",
                "low": "[OK]",
            }.get(event.risk_level, "[?]")

            lines.append(
                f"{event.date.strftime('%Y-%m-%d'):<12} "
                f"{event.name:<8} "
                f"{risk_icon} {event.risk_level:<6} "
                f"{event.days_until:>4}d"
            )

    lines.append("-" * 45)

    # Show current risk adjustment
    mult, active = get_event_risk_multiplier()
    if active:
        lines.append("")
        lines.append(f"ACTIVE EVENT RISK: {mult:.0%} exposure")
        for event in active:
            lines.append(f"  → {event.name} ({event.date})")
    else:
        lines.append("")
        lines.append("No active event risk - normal exposure")

    lines.append("")
    lines.append(f"Generated: {now_et().strftime('%Y-%m-%d %H:%M:%S')} ET")

    return "\n".join(lines)


def apply_event_risk_to_exposure(
    base_exposure: float,
    reference_date: Optional[datetime.date] = None
) -> Tuple[float, List[MacroEvent]]:
    """
    Apply macro event risk adjustment to exposure.

    Args:
        base_exposure: Base exposure multiplier (e.g., from risk score)
        reference_date: Date to check (default: today)

    Returns:
        Tuple of (adjusted_exposure, list of active events)
    """
    event_mult, active_events = get_event_risk_multiplier(reference_date)

    adjusted = base_exposure * event_mult

    if active_events:
        event_names = ", ".join(e.name for e in active_events)
        log.info(f"[EVENT_RISK] Active events: {event_names} | "
                f"Exposure: {base_exposure:.2f} -> {adjusted:.2f} ({event_mult:.0%})")

    return adjusted, active_events


def compute_target_weights(
    bars: pd.DataFrame,
    state: BotState,
    total_equity: float,
    deployable_capital: float,
    trading_client: TradingClient,
    current_regime: Optional[str] = None
) -> Tuple[Dict[str, float], Dict[str, Dict[str, float]]]:
    """
    Compute target portfolio weights.

    Args:
        bars: Historical price data
        state: Bot state for drawdown tracking
        total_equity: TOTAL account equity (for drawdown calculations)
        deployable_capital: Capital available for trading (for weight sizing)
        trading_client: Alpaca trading client
        current_regime: Pre-computed regime ("risk_on" or "risk_off"). If None, reads from state.

    Returns:
      target_weights: dict symbol -> weight (sums to <= 1.0 for long-only)
      diagnostics: per-symbol details (signal, vol, raw_weight, capped_weight, etc.)

    NOTE: Drawdown is calculated off total_equity (not deployable_capital) to avoid
    the 35% reserve being incorrectly interpreted as a permanent loss.

    NOTE: This function is pure computation with NO side effects (no state persistence).
    Regime updates should happen in rebalance() BEFORE calling this function.
    """
    # === REGIME (passed in or from state) ===
    # NOTE: Regime persistence is handled by caller (rebalance/daily_monitoring)
    # This function does NOT update state - it only reads
    if current_regime is None:
        current_regime = state.spy_regime
    spy_risk_on = (current_regime == "risk_on")

    # Drawdown circuit breaker with smooth scaling (v42)
    # NOTE: Use total_equity for drawdown, NOT deployable_capital, to avoid
    # the capital reserve being interpreted as permanent loss
    equity_peak = state.equity_peak if state.equity_peak is not None else total_equity
    equity_peak = max(equity_peak, total_equity)
    drawdown = (equity_peak - total_equity) / equity_peak if equity_peak > 0 else 0.0

    cooldown_until = parse_iso_date(state.drawdown_cooldown_until_iso)
    today = now_et().date()

    in_cooldown = cooldown_until is not None and today <= cooldown_until

    # v42: Use smooth drawdown scaling instead of binary trigger
    if USE_SMOOTH_DRAWDOWN_SCALING:
        # Smooth scaling: exposure reduces continuously as drawdown increases
        exposure_mult = compute_smooth_drawdown_mult(drawdown)

        # Log when drawdown scaling is active
        if drawdown >= DRAWDOWN_SCALE_START:
            log.info(f"[DRAWDOWN] Smooth scaling active: dd={drawdown:.2%} -> exposure_mult={exposure_mult:.2f}x")

        # Track if we're in significant drawdown (for legacy cooldown compatibility)
        in_cooldown = drawdown >= DRAWDOWN_TRIGGER

        # Clear legacy cooldown state if using smooth scaling
        if state.drawdown_cooldown_until_iso:
            state.drawdown_cooldown_until_iso = None
    else:
        # Legacy binary cooldown logic
        if drawdown >= DRAWDOWN_TRIGGER:
            if not in_cooldown:
                # Trigger new cooldown
                cooldown_until = add_trading_days_exact(trading_client, today, DRAWDOWN_COOLDOWN_DAYS)
                in_cooldown = True
                state.drawdown_cooldown_until_iso = cooldown_until.isoformat()
                log.warning(f"Drawdown trigger! {drawdown:.2%} >= {DRAWDOWN_TRIGGER:.2%}. "
                           f"Cooldown until {cooldown_until.isoformat()}")
        else:
            # Clear cooldown if drawdown has recovered
            if in_cooldown and state.drawdown_cooldown_until_iso:
                state.drawdown_cooldown_until_iso = None
                in_cooldown = False
                log.info("Drawdown recovered. Cooldown cleared.")

        exposure_mult = DRAWDOWN_EXPOSURE_MULT if in_cooldown else 1.0

    equity_mult = 1.0 if spy_risk_on else RISK_OFF_EQUITY_MULT

    # === RISK SCORE OVERLAY (Phase 2) ===
    # Compute market risk score and adjust exposure accordingly
    risk_score = 50.0  # Default neutral
    risk_components = {}
    risk_exposure_mult = 1.0

    if ENABLE_RISK_SCORE:
        try:
            risk_score, risk_components = compute_risk_score(bars, trading_client)
            risk_exposure_mult = get_exposure_multiplier_from_risk_score(risk_score)

            # Risk score can increase exposure, but respect drawdown cooldown cap
            if in_cooldown:
                # During cooldown, risk score can only REDUCE further, never increase
                risk_exposure_mult = min(risk_exposure_mult, 1.0)

            log.info(f"[RISK_SCORE] Exposure mult={risk_exposure_mult:.2f} from score={risk_score:.1f}")
        except Exception as e:
            log.error(f"[RISK_SCORE] Failed to compute: {e}")
            risk_exposure_mult = 1.0  # Fall back to neutral

    # Combine drawdown and risk score multipliers
    # Drawdown is a hard cap (multiplicative), risk score adjusts within that
    combined_exposure_mult = exposure_mult * risk_exposure_mult

    # === CONDITIONAL LEVERAGE (Phase 2) ===
    # Allow >1.0x exposure only under strict conditions
    leverage_applied = False
    if ENABLE_CONDITIONAL_LEVERAGE and combined_exposure_mult > 1.0:
        # Check all leverage conditions
        vix_estimate = risk_components.get("vix_estimated", 30.0)
        meets_risk_score = risk_score >= LEVERAGE_RISK_SCORE_MIN
        meets_vix = vix_estimate < LEVERAGE_VIX_MAX
        meets_regime = spy_risk_on if LEVERAGE_REGIME_REQUIRED == "risk_on" else True

        if meets_risk_score and meets_vix and meets_regime:
            # Cap leverage at MAX_LEVERAGE_RATIO
            combined_exposure_mult = min(combined_exposure_mult, MAX_LEVERAGE_RATIO)
            leverage_applied = True
            log.info(f"[LEVERAGE] Conditions met: exposure={combined_exposure_mult:.2f}x "
                    f"(risk_score={risk_score:.0f}, vix={vix_estimate:.1f})")
        else:
            # Conditions not met - cap at 1.0
            combined_exposure_mult = min(combined_exposure_mult, 1.0)
            log.debug(f"[LEVERAGE] Conditions NOT met (score={meets_risk_score}, "
                     f"vix={meets_vix}, regime={meets_regime})")
    else:
        # Leverage disabled - cap at 1.0
        combined_exposure_mult = min(combined_exposure_mult, 1.0)

    # === MACRO EVENT RISK (Phase 3.3) ===
    # Reduce exposure on/before high-impact macro events
    event_exposure_mult = 1.0
    active_events: List[MacroEvent] = []
    try:
        event_exposure_mult, active_events = get_event_risk_multiplier()
        if active_events:
            combined_exposure_mult *= event_exposure_mult
            event_names = ", ".join(e.name for e in active_events)
            log.info(f"[EVENT_RISK] Active events: {event_names} | "
                    f"Event mult={event_exposure_mult:.0%} | "
                    f"Combined exposure={combined_exposure_mult:.2f}")
    except Exception as e:
        log.warning(f"[EVENT_RISK] Failed to check events: {e}")
        event_exposure_mult = 1.0

    # === VIX CIRCUIT BREAKER ===
    # Emergency hard cap on exposure when VIX is extremely elevated
    vix_for_breaker = risk_components.get("vix_estimated", None)
    if vix_for_breaker is not None and vix_for_breaker > VIX_CIRCUIT_BREAKER:
        old_mult = combined_exposure_mult
        combined_exposure_mult = min(combined_exposure_mult, VIX_CIRCUIT_BREAKER_MAX_EXPOSURE)
        log.warning(f"[VIX_BREAKER] VIX={vix_for_breaker:.1f} > {VIX_CIRCUIT_BREAKER} — "
                    f"capping exposure {old_mult:.2f} -> {combined_exposure_mult:.2f}")

    # Build raw weights (inverse vol) for tickers with signal=1
    diag: Dict[str, Dict[str, float]] = {}
    raw = {}
    extensions: Dict[str, float] = {}  # Track price extension above SMA200 for profit tilt

    # === TOP-N ROTATION (Phase 2) ===
    # Use expanded universe with momentum ranking instead of static basket
    if ENABLE_TOP_N_ROTATION and TOP_N_EQUITY > 0:
        # Rank equity/sector ETFs by momentum
        equity_ranked = rank_by_momentum(bars, ALL_EQUITY, trading_client, top_n=0)
        top_equity_syms = [sym for sym, _ in equity_ranked[:TOP_N_EQUITY]]

        # Rank defensive ETFs by momentum (for defensive allocation)
        defensive_ranked = rank_by_momentum(bars, DEFENSIVE_TICKERS, trading_client, top_n=0)
        top_defensive_syms = [sym for sym, _ in defensive_ranked[:TOP_N_DEFENSIVE]]

        # Combined basket: top N equity + top N defensive
        basket = [t for t in (top_equity_syms + top_defensive_syms) if t != CASH_TICKER]

        # DATA QUALITY SAFEGUARD: If ALL momentum symbols failed for BOTH
        # equity and defensive, this is almost certainly a data issue (Polygon
        # rate limit, API outage, etc.) — NOT a genuine "everything is flat" signal.
        # Abort rather than liquidating all positions to 100% cash.
        if not equity_ranked and not defensive_ranked:
            log.warning("[DATA_QUALITY] ALL momentum rankings empty — suspected data issue, aborting")
            diag["_data_quality_abort"] = True
            diag["_abort_reason"] = (
                f"All momentum rankings failed (0/{len(ALL_EQUITY)} equity, "
                f"0/{len(DEFENSIVE_TICKERS)} defensive) — suspected Polygon data/rate limit issue"
            )
            return {}, diag

        # Log momentum rankings for diagnostics
        log.info(f"[TOP_N] Equity ({TOP_N_EQUITY}): {top_equity_syms}")
        log.info(f"[TOP_N] Defensive ({TOP_N_DEFENSIVE}): {top_defensive_syms}")

        # Store rankings in diagnostics
        diag["_momentum_ranking"] = {
            "equity_ranked": [(s, f"{sc:.3f}") for s, sc in equity_ranked[:10]],
            "defensive_ranked": [(s, f"{sc:.3f}") for s, sc in defensive_ranked],
            "selected_equity": top_equity_syms,
            "selected_defensive": top_defensive_syms,
        }
    else:
        # Legacy behavior: use all equity + defensive tickers
        basket = [t for t in (EQUITY_TICKERS + DEFENSIVE_TICKERS) if t != CASH_TICKER]

    for sym in basket:
        try:
            # FIX: Strip incomplete daily bar during market hours for accurate SMA/vol calculation
            close = get_close_series(bars, sym, strip_incomplete=True, trading_client=trading_client)
            sig = trend_signal(close, SMA_LOOKBACK_DAYS)
            vol = annualized_realized_vol(close, VOL_LOOKBACK_DAYS)

            # v42: Use dynamic vol floor based on vol clustering
            dynamic_floor = compute_dynamic_vol_floor(close)
            vol = max(vol, dynamic_floor)

            # FIX: Apply risk-off multiplier to ALL equity-like ETFs (sectors, factors)
            # not just the legacy EQUITY_TICKERS list
            mult = equity_mult if is_equity_like(sym) else 1.0
            rw = (sig * mult) * (1.0 / vol)

            # Calculate extension above SMA200 for profit tilt
            sma_val = sma(close, SMA_LOOKBACK_DAYS)
            last_close = float(close.iloc[-1])
            extension = (last_close - sma_val) / sma_val if sma_val > 0 else 0.0
            extensions[sym] = extension

            raw[sym] = rw
            diag[sym] = {
                "signal": float(sig),
                "vol_annual": float(vol),
                "multiplier": float(mult),
                "raw": float(rw),
                "extension_pct": float(extension),
                "vol_floor_used": float(dynamic_floor),  # v42: track which floor was used
            }
        except Exception as e:
            log.error(f"Failed to compute indicators for {sym}: {e}")
            # Skip this symbol
            diag[sym] = {"error": str(e)}
            continue

    raw_sum = float(sum(raw.values()))
    if raw_sum <= 0:
        # Everything is "flat" => go to cash
        log.info("All signals flat. Going 100% cash.")
        target = {t: 0.0 for t in basket}
        target[CASH_TICKER] = min(TARGET_GROSS_EXPOSURE, MAX_WEIGHT_CASH) * combined_exposure_mult
        diag[CASH_TICKER] = {"signal": 1.0, "vol_annual": 0.0, "multiplier": 1.0, "raw": 1.0}

        diag["_portfolio"] = {
            "spy_regime": current_regime,
            "spy_risk_on": 1.0 if spy_risk_on else 0.0,
            "equity_mult": float(equity_mult),
            "drawdown": float(drawdown),
            "in_cooldown": 1.0 if in_cooldown else 0.0,
            "drawdown_exposure_mult": float(exposure_mult),
            "risk_score": float(risk_score),
            "risk_exposure_mult": float(risk_exposure_mult),
            "event_exposure_mult": float(event_exposure_mult),
            "active_events": [e.name for e in active_events] if active_events else [],
            "combined_exposure_mult": float(combined_exposure_mult),
            "leverage_applied": leverage_applied,
            "gross_target": float(TARGET_GROSS_EXPOSURE * combined_exposure_mult),
            "invested_weight": 0.0,
            "cash_weight": float(target[CASH_TICKER]),
        }
        if risk_components:
            diag["_risk_components"] = risk_components
        return target, diag

    # Normalize to gross exposure target
    # Only include symbols that successfully computed (exist in raw dict)
    target = {}
    valid_symbols = [sym for sym in basket if sym in raw]
    for sym in valid_symbols:
        w = (raw[sym] / raw_sum) * TARGET_GROSS_EXPOSURE * combined_exposure_mult
        target[sym] = float(w)

    # Set failed symbols to 0 weight
    for sym in basket:
        if sym not in target:
            target[sym] = 0.0
            log.warning(f"Symbol {sym} failed indicator calculation - setting weight to 0")

    # === PROFIT TILT (replaces mid-cycle profit-taking) ===
    # Reduce target weight for extended positions at rebalance time
    for sym in basket:
        if target[sym] > 0 and sym in extensions:
            extension = extensions[sym]
            tilt = compute_profit_tilt(extension)
            if tilt < 1.0:
                old_weight = target[sym]
                target[sym] = old_weight * tilt
                diag[sym]["profit_tilt"] = float(tilt)
                log.info(f"[PROFIT_TILT] {sym}: extension={extension:.1%}, "
                        f"tilt={tilt:.2f}, weight {old_weight:.2%} -> {target[sym]:.2%}")

    # Cap per-asset weights (non-cash) and apply minimum position size filter
    for sym in basket:
        target[sym] = float(min(target[sym], MAX_WEIGHT_PER_ASSET))
        # Filter out positions below minimum size
        if target[sym] < MIN_WEIGHT_PER_ASSET:
            target[sym] = 0.0

    # Re-normalize after caps and filters (keep <= TARGET_GROSS_EXPOSURE * combined_exposure_mult)
    capped_sum = float(sum(target[sym] for sym in basket))
    gross_cap = TARGET_GROSS_EXPOSURE * combined_exposure_mult
    if capped_sum > gross_cap and capped_sum > 0:
        scale = gross_cap / capped_sum
        for sym in basket:
            target[sym] *= scale

    # Cash gets leftover to reach gross exposure (or 0 if fully allocated)
    invested = float(sum(target[sym] for sym in basket))
    cash_w = max(0.0, gross_cap - invested)
    cash_w = min(cash_w, MAX_WEIGHT_CASH)
    target[CASH_TICKER] = float(cash_w)

    # Diagnostics enrich
    diag[CASH_TICKER] = {"signal": 1.0, "vol_annual": 0.0, "multiplier": 1.0, "raw": 0.0}
    diag["_portfolio"] = {
        "spy_regime": current_regime,
        "spy_risk_on": 1.0 if spy_risk_on else 0.0,
        "equity_mult": float(equity_mult),
        "drawdown": float(drawdown),
        "in_cooldown": 1.0 if in_cooldown else 0.0,
        "drawdown_exposure_mult": float(exposure_mult),
        "smooth_drawdown_scaling": USE_SMOOTH_DRAWDOWN_SCALING,  # v42
        "dynamic_vol_floor": USE_DYNAMIC_VOL_FLOOR,  # v42
        "risk_score": float(risk_score),
        "risk_exposure_mult": float(risk_exposure_mult),
        "event_exposure_mult": float(event_exposure_mult),
        "active_events": [e.name for e in active_events] if active_events else [],
        "combined_exposure_mult": float(combined_exposure_mult),
        "leverage_applied": leverage_applied,
        "gross_target": float(gross_cap),
        "invested_weight": float(invested),
        "cash_weight": float(target[CASH_TICKER]),
        "top_n_rotation_enabled": ENABLE_TOP_N_ROTATION,
        "risk_score_enabled": ENABLE_RISK_SCORE,
    }
    if risk_components:
        diag["_risk_components"] = risk_components

    return target, diag


# =========================
# BROKER / PORTFOLIO
# =========================

def get_trading_client() -> TradingClient:
    """
    Initialize Alpaca TradingClient with proper live/paper mode handling.

    Safety Logic:
    - LIVE_TRADING_ENABLED=1 is the ONLY way to enable live trading
    - When LIVE_TRADING_ENABLED=0 (default), always use paper=True
    - Ignores ALPACA_PAPER and ALPACA_BASE_URL to prevent confusion
    """
    key = os.getenv("ALPACA_API_KEY", "")
    secret = os.getenv("ALPACA_SECRET_KEY", "")
    if not key or not secret:
        raise RuntimeError("Missing ALPACA_API_KEY / ALPACA_SECRET_KEY env vars.")

    # FIX: Single source of truth for live/paper mode
    # LIVE_TRADING_ENABLED is the ONLY control - ignore ALPACA_PAPER/ALPACA_BASE_URL
    paper = not LIVE_TRADING_ENABLED

    if not paper:
        log.warning("*** LIVE TRADING MODE - TradingClient using REAL money ***")
    else:
        log.info("Paper trading mode - TradingClient using simulated money")

    return TradingClient(api_key=key, secret_key=secret, paper=paper)


def get_data_client() -> StockHistoricalDataClient:
    key = os.getenv("ALPACA_API_KEY", "")
    secret = os.getenv("ALPACA_SECRET_KEY", "")
    if not key or not secret:
        raise RuntimeError("Missing ALPACA_API_KEY / ALPACA_SECRET_KEY env vars.")
    log.debug("Initializing Alpaca DataClient")
    return StockHistoricalDataClient(api_key=key, secret_key=secret)


def is_market_open(trading_client: TradingClient) -> bool:
    """Check if market is currently open using Alpaca clock API."""
    try:
        clock = trading_client.get_clock()
        return clock.is_open
    except Exception as e:
        log.warning(f"Failed to check market hours: {e}")
        # Fallback: assume open during RTH on weekdays
        dt = now_et()
        if dt.weekday() >= 5:
            return False
        return (9, 30) <= (dt.hour, dt.minute) < (16, 0)


def get_portfolio_equity(trading: TradingClient) -> float:
    try:
        acct = trading.get_account()
        equity = float(acct.equity)
        log.debug(f"Portfolio equity: ${equity:.2f}")
        return equity
    except Exception as e:
        log.error(f"Failed to get portfolio equity: {e}")
        raise


def get_positions(trading: TradingClient) -> Dict[str, Dict[str, float]]:
    """Get all current positions."""
    try:
        pos = trading.get_all_positions()
        out: Dict[str, Dict[str, float]] = {}
        for p in pos:
            sym = p.symbol
            qty = float(p.qty)
            mv = float(p.market_value)
            avg_entry = float(p.avg_entry_price) if p.avg_entry_price else 0.0
            out[sym] = {"qty": qty, "market_value": mv, "avg_entry_price": avg_entry}
        log.debug(f"Retrieved {len(out)} positions")
        return out
    except Exception as e:
        log.error(f"Failed to get positions: {e}")
        raise


def get_latest_price_alpaca(data_client: StockHistoricalDataClient, symbol: str) -> float:
    """Get most recent daily bar close as a reasonable estimate for sizing."""
    try:
        end = now_et().astimezone(pytz.UTC)
        start = (now_et() - timedelta(days=10)).astimezone(pytz.UTC)
        bars = fetch_daily_bars_alpaca(data_client, [symbol], start, end)
        s = get_close_series(bars, symbol)
        price = float(s.iloc[-1])
        log.debug(f"Latest price for {symbol}: ${price:.2f}")
        return price
    except Exception as e:
        log.error(f"Failed to get latest price for {symbol}: {e}")
        raise


def validate_fractional_support(trading_client: TradingClient, symbol: str) -> bool:
    """Check if a symbol supports fractional shares on Alpaca."""
    try:
        asset = trading_client.get_asset(symbol)
        is_fractionable = asset.fractionable if hasattr(asset, 'fractionable') else False
        log.debug(f"{symbol} fractional support: {is_fractionable}")
        return is_fractionable
    except Exception as e:
        log.warning(f"Failed to check fractional support for {symbol}: {e}")
        # Assume true for ETFs
        return True


def reconcile_positions(
    target_weights: Dict[str, float],
    positions: Dict[str, Dict[str, float]],
    equity: float
) -> None:
    """Reconcile actual positions vs expected weights and log discrepancies."""
    log.info("=== Position Reconciliation ===")
    for sym, target_w in target_weights.items():
        target_mv = target_w * equity
        actual_mv = positions.get(sym, {}).get("market_value", 0.0)
        actual_w = actual_mv / equity if equity > 0 else 0.0

        diff_w = abs(actual_w - target_w)
        diff_pct = (diff_w / target_w * 100) if target_w > 0 else 0.0

        if diff_pct > POSITION_RECONCILIATION_THRESHOLD_PCT * 100:
            log.warning(
                f"{sym}: Target={target_w:.2%}, Actual={actual_w:.2%}, "
                f"Diff={diff_pct:.1f}% (>${POSITION_RECONCILIATION_THRESHOLD_PCT * 100:.0f}%)"
            )
        else:
            log.debug(f"{sym}: Target={target_w:.2%}, Actual={actual_w:.2%} ✓")


def verify_order_execution(trading_client: TradingClient, order_id: str, timeout_sec: int = 60, cancel_on_timeout: bool = True) -> Tuple[bool, Optional[str], Optional[float]]:
    """
    Poll order status until filled or timeout.

    Returns:
        Tuple of (success, final_status, filled_qty)
        - success: True if fully filled, False otherwise
        - final_status: The order's final status string
        - filled_qty: Amount that was filled (may be partial)

    Args:
        trading_client: Alpaca trading client
        order_id: Order ID to verify
        timeout_sec: Max seconds to wait for fill (default 60)
        cancel_on_timeout: If True, cancel unfilled remainder on timeout (default True)
    """
    from alpaca.trading.enums import OrderStatus

    start = time.time()
    last_status = None
    filled_qty = 0.0
    total_qty = 0.0

    while (time.time() - start) < timeout_sec:
        try:
            order = trading_client.get_order_by_id(order_id)
            # Use enum comparison, not string - str(OrderStatus.FILLED) != "filled"
            status = order.status
            last_status = str(status.value) if hasattr(status, 'value') else str(status)
            filled_qty = float(order.filled_qty) if order.filled_qty else 0.0
            total_qty = float(order.qty) if order.qty else 0.0

            log.debug(f"Order {order_id} status: {last_status} ({filled_qty}/{total_qty} filled)")

            if status == OrderStatus.FILLED:
                log.info(f"Order {order_id} FULLY FILLED: {filled_qty}/{total_qty} @ ${order.filled_avg_price}")
                return True, last_status, filled_qty

            elif status == OrderStatus.PARTIALLY_FILLED:
                # Don't return True for partial fills - keep waiting
                log.warning(f"Order {order_id} PARTIAL FILL: {filled_qty}/{total_qty} @ ${order.filled_avg_price}")
                # Continue polling - it may fill completely

            elif status in (OrderStatus.CANCELED, OrderStatus.EXPIRED, OrderStatus.REJECTED, OrderStatus.REPLACED):
                log.error(f"Order {order_id} terminal status: {last_status} (filled: {filled_qty}/{total_qty})")
                return False, last_status, filled_qty

            time.sleep(2)
        except Exception as e:
            log.error(f"Failed to check order status for {order_id}: {e}")
            return False, "error", filled_qty

    # Timeout reached - handle unfilled remainder
    if filled_qty > 0:
        log.warning(f"Order {order_id} TIMEOUT with PARTIAL FILL: {filled_qty}/{total_qty} shares filled")
        if cancel_on_timeout and filled_qty < total_qty:
            try:
                trading_client.cancel_order_by_id(order_id)
                unfilled = total_qty - filled_qty
                log.warning(f"Order {order_id} cancelled unfilled remainder: {unfilled:.4f} shares")
            except Exception as e:
                # Order may have filled or been cancelled already
                log.warning(f"Order {order_id} cancel attempt returned: {e}")
    else:
        log.warning(f"Order {order_id} verification timeout after {timeout_sec}s (no fills)")
        if cancel_on_timeout:
            try:
                trading_client.cancel_order_by_id(order_id)
                log.warning(f"Order {order_id} cancelled (no fills)")
            except Exception as e:
                log.warning(f"Order {order_id} cancel attempt returned: {e}")

    return False, last_status, filled_qty


# =========================
# DAILY POSITION MONITORING (Risk Management)
# =========================

def is_daily_monitoring_window(dt: datetime) -> bool:
    """Check if current time is within daily monitoring window."""
    if dt.weekday() >= 5:  # Weekend
        return False
    hh, mm = dt.hour, dt.minute
    start_h, start_m = DAILY_MONITORING_START_TIME
    end_h, end_m = DAILY_MONITORING_END_TIME
    return (hh, mm) >= (start_h, start_m) and (hh, mm) <= (end_h, end_m)


def daily_position_monitoring(
    trading: TradingClient,
    data_client: StockHistoricalDataClient,
    state: BotState
) -> None:
    """
    Daily position monitoring for risk management.

    DECISION HIERARCHY (enforced order):
    1. Emergency exits (intraday gap-down >8%)
    2. Close-based trend exits (2 consecutive CLOSES below SMA200*buffer)
    3. Position drawdown exits (>15% loss from cost basis)

    WHAT THIS FUNCTION DOES NOT DO:
    - Mid-cycle profit-taking (handled at rebalance via profit tilt)
    - Regime-change sells (handled at rebalance via canonical regime)
    - Full portfolio rebalancing (only exits on risk triggers)

    Close-based logic:
    - "2 consecutive days below SMA200" means 2 consecutive DAILY CLOSES, not intraday touches
    - We record yesterday's close at end-of-day, then check the pattern next day
    """

    try:
        # Skip if market is closed
        if not is_market_open(trading):
            log.debug("[DAILY_MONITOR] Market closed, skipping")
            return

        # Check if enough time has elapsed since last run (timestamp-based gating)
        now = time.time()
        if state.last_daily_monitoring_timestamp is not None:
            elapsed_minutes = (now - state.last_daily_monitoring_timestamp) / 60.0
            if elapsed_minutes < DAILY_MONITORING_INTERVAL_MIN:
                log.debug(f"[DAILY_MONITOR] Too soon since last run ({elapsed_minutes:.1f} min ago), skipping")
                return

        log.info("=" * 60)
        log.info("[DAILY_MONITOR] Starting daily position monitoring")
        log.info("=" * 60)

        # Get today's date for tracking
        today = today_iso_et()

        # Get current positions and equity
        total_equity = get_portfolio_equity(trading)
        positions = get_positions(trading)

        if not positions:
            log.info("[DAILY_MONITOR] No positions to monitor")
            state.last_daily_monitoring_timestamp = now
            save_state(STATE_PATH, state)
            return

        # Initialize close-based tracking if needed
        if state.position_daily_closes is None:
            state.position_daily_closes = {}

        # Fetch bars for all positions (include RSP, IWM for breadth metrics)
        symbols_to_check = list(positions.keys())
        lookback_days = int((SMA_LOOKBACK_DAYS + 10) * 1.43) + 30
        health_symbols = ["SPY", "RSP", "IWM"]  # For market health metrics
        bars = data_cache.get_bars(data_client, symbols_to_check + health_symbols, lookback_days)

        # === CANONICAL REGIME UPDATE ===
        # Use completed bars only for regime calculation
        spy_close_for_regime = get_close_series(bars, "SPY", strip_incomplete=True, trading_client=trading)
        current_regime, regime_changed = update_regime_state(state, spy_close_for_regime, STATE_PATH)

        if regime_changed:
            log.warning(f"[DAILY_MONITOR] Regime changed to {current_regime.upper()} - will affect next rebalance")

        # Log current regime status
        spy_sma200 = sma(spy_close_for_regime, SMA_LOOKBACK_DAYS)
        spy_last_close = float(spy_close_for_regime.iloc[-1])
        log.info(f"[DAILY_MONITOR] SPY regime={current_regime} | "
                f"close=${spy_last_close:.2f} | SMA200=${spy_sma200:.2f}")

        # Check if this is the first monitoring run of the trading day
        is_new_trading_day = state.last_daily_signal_date_iso != today

        # === HEALTH REPORT (informative, does not trade) ===
        # Generate comprehensive health report once per day (first monitoring run)
        if is_new_trading_day:
            try:
                # Market health context
                market_health = compute_market_health(bars, state, trading)
                log.info(f"[HEALTH_REPORT] Market: {market_health.health_level.upper()} "
                        f"({market_health.market_health_score:.0f}/100) | "
                        f"Breadth={market_health.breadth_score:.0f} | "
                        f"Vol={market_health.vol_regime} | "
                        f"Events={market_health.event_risk_level}")

                # Position health report
                position_health_list, summary = generate_position_health_report(bars, trading, state)

                # Log high-risk positions with stop probabilities
                for ph in position_health_list:
                    if ph.risk_level in ("critical", "distressed"):
                        log.warning(f"[HEALTH_REPORT] ALERT {ph.symbol}: {ph.risk_level.upper()} "
                                   f"(score={ph.health_score:.0f}) | "
                                   f"P&L={ph.unrealized_pnl_pct:+.1f}% | "
                                   f"SMA200={'Above' if ph.above_sma200 else 'BELOW'}")

                # Log market context section
                market_section = format_market_health_section(market_health)
                for line in market_section.split("\n"):
                    log.info(f"[HEALTH_REPORT] {line}")

                # Send alert if portfolio needs attention
                if summary["needs_attention"]:
                    alerter.send_alert(
                        level="WARNING",
                        title="Portfolio Health Alert",
                        message=f"Portfolio needs attention:\n"
                                f"Health Score: {summary['portfolio_health_score']:.0f}/100\n"
                                f"Critical: {summary['critical_count']}, "
                                f"Distressed: {summary['distressed_count']}\n\n"
                                f"Market: {market_health.health_level.upper()} "
                                f"(regime={market_health.spy_regime})",
                        context={
                            "portfolio_health": f"{summary['portfolio_health_score']:.0f}",
                            "market_health": f"{market_health.market_health_score:.0f}",
                            "critical": summary["critical_count"],
                            "distressed": summary["distressed_count"]
                        }
                    )

            except Exception as e:
                log.warning(f"[HEALTH_REPORT] Failed to generate health report: {e}")
                # Continue with monitoring - health report is informative only

            # === DAILY EQUITY SNAPSHOT ===
            try:
                log_equity_snapshot(
                    date_str=today,
                    equity=total_equity,
                    equity_peak=state.equity_peak,
                    positions=positions,
                    regime=current_regime,
                    spy_price=spy_last_close
                )
            except Exception as e:
                log.warning(f"[EQUITY_SNAPSHOT] Failed: {e}")

        # === RECORD DAILY CLOSES (once per day at first monitoring run) ===
        if is_new_trading_day:
            # Record yesterday's close for each position (from completed bars)
            for symbol in symbols_to_check:
                if symbol == CASH_TICKER:
                    continue
                try:
                    close_series = get_close_series(bars, symbol, strip_incomplete=True, trading_client=trading)
                    if len(close_series) > 0:
                        yesterday_close = float(close_series.iloc[-1])
                        if symbol not in state.position_daily_closes:
                            state.position_daily_closes[symbol] = []
                        # Keep last N closes (N = stop-loss days + buffer)
                        state.position_daily_closes[symbol].append(yesterday_close)
                        # Trim to last 5 days (more than we need)
                        state.position_daily_closes[symbol] = state.position_daily_closes[symbol][-5:]
                        log.debug(f"[DAILY_MONITOR] {symbol}: recorded close ${yesterday_close:.2f}")
                except Exception as e:
                    log.warning(f"[DAILY_MONITOR] Failed to record close for {symbol}: {e}")

        # Check each position
        exits = []

        for symbol, pos_data in positions.items():
            if symbol == CASH_TICKER:
                continue  # Don't monitor cash

            try:
                # Get position details from Alpaca (includes live price and P&L)
                alpaca_position = trading.get_open_position(symbol)
                qty = float(alpaca_position.qty)
                avg_entry_price = float(alpaca_position.avg_entry_price)
                unrealized_plpc = float(alpaca_position.unrealized_plpc)
                current_price = float(alpaca_position.current_price)

                # Use completed bars only for SMA calculation
                close_for_sma = get_close_series(bars, symbol, strip_incomplete=True, trading_client=trading)
                sma200_val = sma(close_for_sma, SMA_LOOKBACK_DAYS)
                position_pnl_pct = unrealized_plpc

                # Get yesterday's close for gap calculation
                yesterday_close = float(close_for_sma.iloc[-1]) if len(close_for_sma) > 0 else current_price

                # === HIERARCHY 1: EMERGENCY INTRADAY GAP-DOWN EXIT ===
                if ENABLE_INTRADAY_GAP_EXIT:
                    gap_pct = (current_price - yesterday_close) / yesterday_close if yesterday_close > 0 else 0.0
                    if gap_pct <= -INTRADAY_GAP_EXIT_PCT:
                        log.warning(f"[DAILY_MONITOR] {symbol}: EMERGENCY GAP-DOWN | "
                                   f"Price ${current_price:.2f} is {gap_pct:.1%} below yesterday's close ${yesterday_close:.2f}")
                        exits.append({
                            "symbol": symbol,
                            "qty": qty,
                            "reason": f"emergency_gap_down_{abs(gap_pct):.0%}",
                            "current_price": current_price,
                            "sma200": sma200_val,
                            "pnl_pct": position_pnl_pct
                        })
                        continue  # Skip other checks - emergency exit takes priority

                # === HIERARCHY 2: CLOSE-BASED TREND EXIT ===
                # Check if last N daily CLOSES were below SMA200 * buffer
                daily_closes = state.position_daily_closes.get(symbol, [])
                if check_consecutive_closes_below_sma(daily_closes, sma200_val, STOP_LOSS_SMA200_BUFFER, STOP_LOSS_SMA200_DAYS):
                    log.warning(f"[DAILY_MONITOR] {symbol}: CLOSE-BASED STOP | "
                               f"{STOP_LOSS_SMA200_DAYS} consecutive closes below SMA200*{STOP_LOSS_SMA200_BUFFER} | "
                               f"closes={[f'${c:.2f}' for c in daily_closes[-STOP_LOSS_SMA200_DAYS:]]} | "
                               f"threshold=${sma200_val * STOP_LOSS_SMA200_BUFFER:.2f}")
                    exits.append({
                        "symbol": symbol,
                        "qty": qty,
                        "reason": f"stop_loss_sma200_close_{STOP_LOSS_SMA200_DAYS}d",
                        "current_price": current_price,
                        "sma200": sma200_val,
                        "pnl_pct": position_pnl_pct
                    })
                    continue  # Skip other checks

                # === HIERARCHY 3: POSITION DRAWDOWN EXIT ===
                if position_pnl_pct <= -STOP_LOSS_POSITION_DD_PCT:
                    log.warning(f"[DAILY_MONITOR] {symbol}: POSITION DRAWDOWN | "
                               f"{position_pnl_pct:.1%} loss exceeds -{STOP_LOSS_POSITION_DD_PCT:.0%} threshold")
                    exits.append({
                        "symbol": symbol,
                        "qty": qty,
                        "reason": f"stop_loss_position_dd_{abs(position_pnl_pct):.0%}",
                        "current_price": current_price,
                        "avg_price": avg_entry_price,
                        "pnl_pct": position_pnl_pct
                    })
                    continue

                # === LOG STATUS FOR HEALTHY POSITIONS ===
                extension_pct = (current_price - sma200_val) / sma200_val if sma200_val > 0 else 0.0
                log.debug(f"[DAILY_MONITOR] {symbol}: OK | "
                         f"price=${current_price:.2f} | SMA200=${sma200_val:.2f} | "
                         f"extension={extension_pct:+.1%} | PnL={position_pnl_pct:+.1%}")

            except Exception as e:
                log.error(f"[DAILY_MONITOR] Error checking {symbol}: {e}")
                continue

        # Execute exits (full positions)
        for exit_trade in exits:
            symbol = exit_trade["symbol"]
            qty = exit_trade["qty"]
            reason = exit_trade["reason"]
            current_price = exit_trade["current_price"]
            pnl_pct = exit_trade["pnl_pct"]

            log.warning(f"[DAILY_MONITOR] EXITING {symbol}: {reason} | "
                       f"Qty={qty:.4f} | Price=${current_price:.2f} | PnL={pnl_pct:+.1%}")

            if DRY_RUN:
                log.info(f"[DAILY_MONITOR] [DRY-RUN] Would SELL {qty:.4f} {symbol} @ ${current_price:.2f}")
                log_trade(
                    path=LOG_PATH,
                    action="DRY_RUN_SELL",
                    symbol=symbol,
                    qty=qty,
                    est_price=current_price,
                    notional=qty * current_price,
                    reason=f"daily_monitor_{reason}",
                    target_weight=0.0,
                    current_weight=0.0,
                    portfolio_equity=total_equity
                )
            else:
                try:
                    # SAFETY: Cancel any pending TBOT_ orders for this symbol before exit
                    # Prevents conflicts with pending rebalance or drift orders
                    try:
                        pending_orders = trading.get_orders()
                        symbol_orders = [o for o in pending_orders
                                        if o.symbol == symbol and (o.client_order_id or "").startswith("TBOT_")]
                        if symbol_orders:
                            for pending_order in symbol_orders:
                                try:
                                    trading.cancel_order_by_id(pending_order.id)
                                except Exception:
                                    pass  # Order may already be filled/cancelled
                            log.info(f"[DAILY_MONITOR] {symbol}: Cancelled {len(symbol_orders)} pending order(s) before exit")
                            time.sleep(0.5)  # Brief pause for cancellations to settle
                    except Exception as cancel_err:
                        log.warning(f"[DAILY_MONITOR] {symbol}: Could not check/cancel pending orders: {cancel_err}")

                    # FIX: Use deterministic client_order_id for idempotency
                    client_oid = generate_client_order_id(f"dm_{reason}", symbol, "SELL")
                    req = MarketOrderRequest(
                        symbol=symbol,
                        qty=qty,
                        side=OrderSide.SELL,
                        time_in_force=TimeInForce.DAY,
                        client_order_id=client_oid
                    )
                    order = trading.submit_order(req)
                    log.warning(f"[DAILY_MONITOR] Order submitted: SELL {qty:.4f} {symbol} (Order ID: {order.id}, Client ID: {client_oid})")

                    # VERIFY: Confirm the exit order fills (critical for risk management)
                    fill_ok, fill_status, filled_qty = verify_order_execution(trading, str(order.id), timeout_sec=30)
                    if fill_ok:
                        log.info(f"[DAILY_MONITOR] {symbol}: Exit CONFIRMED filled | qty={filled_qty:.4f}")
                    elif filled_qty and filled_qty > 0:
                        log.warning(f"[DAILY_MONITOR] {symbol}: Exit PARTIAL FILL | filled={filled_qty:.4f}/{qty:.4f} | "
                                   f"status={fill_status} | Position may still be exposed!")
                        alerter.send_alert(
                            level="CRITICAL",
                            title=f"Partial Exit Fill: {symbol}",
                            message=f"Daily monitoring exit for {symbol} only partially filled!\n"
                                    f"Filled: {filled_qty:.4f} / {qty:.4f}\n"
                                    f"Reason: {reason}\n"
                                    f"Manual intervention may be needed.",
                            context={"symbol": symbol, "reason": reason, "filled_qty": f"{filled_qty:.4f}"}
                        )
                    else:
                        log.error(f"[DAILY_MONITOR] {symbol}: Exit FAILED to fill | status={fill_status} | "
                                 f"Position is STILL OPEN and exposed!")
                        alerter.send_alert(
                            level="CRITICAL",
                            title=f"Exit Order Failed: {symbol}",
                            message=f"Daily monitoring exit for {symbol} did NOT fill!\n"
                                    f"Status: {fill_status}\n"
                                    f"Reason: {reason}\n"
                                    f"MANUAL INTERVENTION REQUIRED - position still open.",
                            context={"symbol": symbol, "reason": reason}
                        )

                    # Get market value from position data for weight calculation
                    # FIX: Look up pos_data by symbol (not stale reference from outer loop)
                    exit_pos_data = positions.get(symbol, {})
                    pos_market_value = exit_pos_data.get("market_value", qty * current_price)
                    log_trade(
                        path=LOG_PATH,
                        action="SELL",
                        symbol=symbol,
                        qty=qty,
                        est_price=current_price,
                        notional=qty * current_price,
                        reason=f"daily_monitor_{reason}",
                        target_weight=0.0,
                        current_weight=pos_market_value / total_equity if total_equity > 0 else 0.0,
                        portfolio_equity=total_equity,
                        order_id=order.id
                    )

                    # Send alert
                    alerter.send_alert(
                        level="WARNING",
                        title=f"Position Exit: {symbol}",
                        message=f"Exited {symbol} position via daily monitoring\n"
                                f"Reason: {reason}\n"
                                f"Qty: {qty:.4f}\n"
                                f"Price: ${current_price:.2f}\n"
                                f"PnL: {pnl_pct:+.1%}\n"
                                f"Fill status: {'FILLED' if fill_ok else fill_status}",
                        context={"symbol": symbol, "reason": reason, "pnl_pct": f"{pnl_pct:+.1%}"}
                    )
                except Exception as e:
                    log.error(f"[DAILY_MONITOR] Failed to exit {symbol}: {e}")
                    log_trade(
                        path=LOG_PATH,
                        action="FAILED",
                        symbol=symbol,
                        qty=qty,
                        est_price=current_price,
                        notional=qty * current_price,
                        reason=f"daily_monitor_{reason}_FAILED: {e}",
                        target_weight=0.0,
                        current_weight=0.0,
                        portfolio_equity=total_equity
                    )
                    alerter.send_alert(
                        level="CRITICAL",
                        title=f"Exit Order FAILED: {symbol}",
                        message=f"Failed to submit exit order for {symbol}!\n"
                                f"Error: {e}\n"
                                f"Reason: {reason}\n"
                                f"MANUAL INTERVENTION REQUIRED.",
                        context={"symbol": symbol, "reason": reason, "error": str(e)}
                    )

        # NOTE: Mid-cycle profit-taking REMOVED
        # Profit-taking is now handled at rebalance time via profit tilt (compute_profit_tilt)
        # This prevents fighting the trend premise and reduces turnover

        # Clean up position_daily_closes for exited positions
        for exit_trade in exits:
            symbol = exit_trade["symbol"]
            if symbol in state.position_daily_closes:
                del state.position_daily_closes[symbol]

        # Update state
        state.last_daily_monitoring_timestamp = now
        # Mark that we've processed signals for this trading day
        state.last_daily_signal_date_iso = today
        save_state(STATE_PATH, state)

        log.info("[DAILY_MONITOR] Complete | "
                f"Exits: {len(exits)} | Regime: {state.spy_regime}")
        log.info("=" * 60)

    except Exception as e:
        log.error(f"[DAILY_MONITOR] Failed: {e}")
        traceback.print_exc()
        # Don't raise - daily monitoring failures shouldn't halt the bot


# =========================
# REBALANCE / ORDERS
# =========================

def rebalance(
    trading: TradingClient,
    data_client: StockHistoricalDataClient,
    state: BotState
) -> None:
    """Execute rebalance with all improvements."""

    # Race condition fix: Set rebalance flag atomically with timestamp
    if state.rebalance_in_progress:
        log.warning("Rebalance already in progress. Skipping.")
        return

    state.rebalance_in_progress = True
    state.rebalance_started_at_iso = now_et().isoformat()  # Track when rebalance started
    save_state(STATE_PATH, state)

    try:
        # Market hours validation
        if not is_market_open(trading):
            log.warning("Market is closed. Skipping rebalance.")
            state.rebalance_in_progress = False
            state.rebalance_started_at_iso = None
            save_state(STATE_PATH, state)
            return

        # 1) Get equity + positions
        total_equity = get_portfolio_equity(trading)
        positions = get_positions(trading)

        # Update equity peak / last equity (use total equity for drawdown tracking)
        if state.equity_peak is None:
            state.equity_peak = total_equity
        else:
            state.equity_peak = max(state.equity_peak, total_equity)
        state.last_equity = total_equity

        # 2) Fetch bars for signals (use data cache)
        # Convert trading days to calendar days (multiply by ~1.43 to account for weekends/holidays)
        # Include momentum lookback requirements (252 + skip_days for 12-1 momentum)
        momentum_days_needed = MOMENTUM_LOOKBACK_252D + SKIP_MONTH_DAYS + 10 if ENABLE_TOP_N_ROTATION else 0
        max_trading_days_needed = max(SMA_LOOKBACK_DAYS, VOL_LOOKBACK_DAYS, momentum_days_needed) + 10
        lookback_days = int(max_trading_days_needed * 1.43) + 30  # ~1.43 = 5/7 ratio + holidays
        # Include tactical ETFs for credit/rates signals
        all_symbols = list(set(ALL_TICKERS + TACTICAL_ETFS))
        bars = data_cache.get_bars(data_client, all_symbols, lookback_days)

        # 2.5) Update canonical regime BEFORE weight computation
        # This keeps state persistence out of the pure compute function
        spy_close = get_close_series(bars, "SPY", strip_incomplete=True, trading_client=trading)
        current_regime, regime_changed = update_regime_state(state, spy_close, STATE_PATH)
        if regime_changed:
            log.warning(f"[REBALANCE] Regime changed to {current_regime.upper()}")

        # 2.6) DYNAMIC CAPITAL DEPLOYMENT (CAGR Edge)
        # Compute conditions needed for dynamic deployment
        spy_risk_on = current_regime == "risk_on"

        # Get risk score for capital deployment decision
        try:
            risk_score_for_capital, _ = compute_risk_score(bars, trading)
        except Exception as e:
            log.warning(f"Failed to compute risk score for capital deployment: {e}")
            risk_score_for_capital = 50.0

        # Get event multiplier
        event_mult_for_capital, active_events = get_event_risk_multiplier()

        # Check drawdown cooldown
        in_cooldown = False
        if state.drawdown_cooldown_until_iso:
            cooldown_date = datetime.fromisoformat(state.drawdown_cooldown_until_iso).date()
            in_cooldown = now_et().date() < cooldown_date

        # Compute dynamic capital usage
        capital_usage_pct, capital_reason = compute_dynamic_capital_usage(
            risk_score=risk_score_for_capital,
            spy_risk_on=spy_risk_on,
            event_mult=event_mult_for_capital,
            in_cooldown=in_cooldown
        )

        deployable_capital = total_equity * capital_usage_pct
        cash_reserve = total_equity - deployable_capital

        log.info(f"Starting rebalance: Total Equity=${total_equity:.2f}, "
                f"Deployable Capital=${deployable_capital:.2f} ({capital_usage_pct:.0%} - {capital_reason}), "
                f"Cash Reserve=${cash_reserve:.2f}, Positions={len(positions)}")

        # 3) Compute target weights (pure computation, no side effects)
        # Pass both total_equity (for drawdown) and deployable_capital (for sizing)
        target_w, diag = compute_target_weights(
            bars, state, total_equity, deployable_capital, trading,
            current_regime=current_regime
        )

        # 3.1) DATA QUALITY CHECK: Abort if momentum scoring failed due to data issues
        if diag.get("_data_quality_abort"):
            reason = diag.get("_abort_reason", "data quality issue")
            log.warning(f"[REBALANCE] ABORTED — {reason}")
            alerter.send_alert(
                level="WARNING",
                title="Rebalance Aborted: Data Issue",
                message=f"{reason}\nExisting positions preserved.\n"
                        f"Check Polygon API / data source availability."
            )
            state.rebalance_in_progress = False
            state.rebalance_started_at_iso = None
            state.last_rebalance_date_iso = now_et().date().isoformat()
            save_state(STATE_PATH, state)
            return

        # 3.5) Clean expired wash sale cooldowns
        expired_ws = clean_expired_wash_sales(state)
        if expired_ws:
            log.info(f"[WASH_SALE] Cleaned {expired_ws} expired cooldown(s)")

        # 4) Determine current weights using market value (relative to deployable capital)
        current_mv = {sym: positions.get(sym, {}).get("market_value", 0.0) for sym in ALL_TICKERS}

        # Attribute substitute holdings to original symbols (wash sale tracking)
        if state.active_substitutions:
            for sub_sym, orig_sym in state.active_substitutions.items():
                if sub_sym in positions and is_wash_sale_cooldown_active(state.loss_sales, orig_sym):
                    sub_mv = positions[sub_sym]["market_value"]
                    current_mv[orig_sym] = current_mv.get(orig_sym, 0.0) + sub_mv
                    log.info(f"[WASH_SALE] Attributing {sub_sym} (${sub_mv:,.2f}) "
                             f"to {orig_sym} (cooldown active)")

        current_w = {sym: (current_mv[sym] / deployable_capital if deployable_capital > 0 else 0.0) for sym in ALL_TICKERS}

        # 4.5) TURNOVER GOVERNOR: Check if we should skip rebalance or cap turnover
        if ENABLE_TURNOVER_GOVERNOR:
            # Check no-trade zone (skip entire rebalance if drift is minimal)
            skip_rebal, max_drift, skip_reason = should_skip_rebalance_for_drift(current_w, target_w)
            if skip_rebal:
                log.info(f"[TURNOVER_GOV] Skipping rebalance: {skip_reason}")

                # Log the skip event for performance tracking
                try:
                    spy_price = float(spy_close.iloc[-1]) if len(spy_close) > 0 else 0.0
                    spy_sma200 = sma(spy_close, SMA_LOOKBACK_DAYS) if len(spy_close) >= SMA_LOOKBACK_DAYS else spy_price
                    spy_vs_sma200_pct = ((spy_price - spy_sma200) / spy_sma200 * 100) if spy_sma200 > 0 else 0.0
                    drawdown_pct = ((state.equity_peak - total_equity) / state.equity_peak * 100) if state.equity_peak and state.equity_peak > 0 else 0.0
                    position_weights = {sym: mv / total_equity for sym, mv in current_mv.items() if mv > 0.001}

                    log_rebalance_summary(
                        event="SKIP_LOW_DRIFT",
                        regime=current_regime,
                        spy_price=spy_price,
                        spy_vs_sma200_pct=spy_vs_sma200_pct,
                        equity=total_equity,
                        equity_peak=state.equity_peak or total_equity,
                        drawdown_pct=drawdown_pct,
                        exposure_mult=1.0,
                        capital_usage_pct=capital_usage_pct,
                        deployable_capital=deployable_capital,
                        cash_reserve=cash_reserve,
                        positions=position_weights,
                        target_weights=target_w,
                        risk_score=risk_score_for_capital,
                        event_mult=event_mult_for_capital,
                        turnover_pct=max_drift * 100,
                        orders_placed=0,
                        notes=f"Skipped: {skip_reason}"
                    )
                except Exception as e:
                    log.warning(f"Failed to log skip event: {e}")

                state.rebalance_in_progress = False
                state.rebalance_started_at_iso = None
                state.last_rebalance_date_iso = now_et().date().isoformat()  # Still mark as "checked"
                save_state(STATE_PATH, state)
                return

            # Apply turnover cap (scale down changes if too aggressive)
            original_turnover = compute_portfolio_turnover(current_w, target_w)
            target_w, actual_turnover, was_capped = apply_turnover_cap(
                current_w, target_w, MAX_TURNOVER_PER_REBALANCE
            )
            if was_capped:
                log.info(f"[TURNOVER_GOV] Capped turnover: {original_turnover:.1%} -> {actual_turnover:.1%}")
            else:
                log.info(f"[TURNOVER_GOV] Turnover OK: {actual_turnover:.1%} < {MAX_TURNOVER_PER_REBALANCE:.1%} cap")

        # 4.6) Update rank history for stability tracking
        if ENABLE_RANK_STABILITY:
            # Extract top N symbols from target weights (non-zero, excluding CASH_TICKER)
            top_n_symbols = [sym for sym, w in target_w.items()
                            if w > 0.01 and sym != CASH_TICKER]
            rebalance_date = now_et().date().isoformat()
            update_rank_history(state, top_n_symbols, rebalance_date)

        # 5) Batch fetch latest prices and update cache
        symbols_to_trade = [sym for sym in ALL_TICKERS
                           if abs(target_w.get(sym, 0.0) - current_w.get(sym, 0.0)) * deployable_capital >= MIN_TRADE_NOTIONAL_USD]

        if symbols_to_trade:
            batch_prices = batch_get_latest_prices(data_client, symbols_to_trade)
            price_cache.batch_update(batch_prices)

        # 6) Build order plan: compute deltas, separate sells from buys
        # FIX: Execute sells FIRST to free up buying power, then buys
        sell_orders = []
        buy_orders = []

        for sym in ALL_TICKERS:
            tw = float(target_w.get(sym, 0.0))
            cw = float(current_w.get(sym, 0.0))
            delta_w = tw - cw
            delta_notional = delta_w * deployable_capital

            if abs(delta_notional) < MIN_TRADE_NOTIONAL_USD:
                continue

            # Get price from cache
            try:
                est_price = price_cache.get_price(sym, data_client)
            except Exception as e:
                log.error(f"Failed to get price for {sym}: {e}")
                log_trade(
                    path=LOG_PATH,
                    action="SKIP",
                    symbol=sym,
                    qty=0.0,
                    est_price=0.0,
                    notional=abs(delta_notional),
                    reason=f"price_fetch_failed: {e}",
                    target_weight=tw,
                    current_weight=cw,
                    portfolio_equity=total_equity
                )
                continue

            if est_price <= 0:
                log.warning(f"Invalid price for {sym}: ${est_price}")
                continue

            qty = abs(delta_notional) / est_price
            if qty < MIN_TRADE_SHARES:
                log.debug(f"Skipping {sym}: qty {qty:.4f} < min {MIN_TRADE_SHARES}")
                continue

            side = OrderSide.BUY if delta_notional > 0 else OrderSide.SELL

            # FIX: For sells, clamp quantity to available shares to prevent overselling
            if side == OrderSide.SELL:
                available_qty = positions.get(sym, {}).get("qty", 0.0)
                if qty > available_qty:
                    shortfall_pct = ((qty - available_qty) / qty) * 100 if qty > 0 else 0
                    log.warning(f"Clamping {sym} sell qty from {qty:.4f} to available {available_qty:.4f} "
                               f"(shortfall: {shortfall_pct:.1f}%)")
                    # Log the clamping event for analysis
                    log_trade(
                        path=LOG_PATH,
                        action="SELL_CLAMPED",
                        symbol=sym,
                        qty=available_qty,
                        est_price=est_price,
                        notional=available_qty * est_price,
                        reason=f"sell_clamped_from_{qty:.4f}_shortfall_{shortfall_pct:.1f}pct",
                        target_weight=tw,
                        current_weight=cw,
                        portfolio_equity=total_equity
                    )
                    qty = available_qty
                if qty <= 0:
                    log.info(f"Skipping {sym} sell: no shares available")
                    continue

            # Check fractional support
            use_fractional = ALLOW_FRACTIONAL and validate_fractional_support(trading, sym)
            final_qty = qty if use_fractional else int(qty)

            if final_qty == 0:
                log.info(f"Skipping {sym}: rounded qty to 0")
                log_trade(
                    path=LOG_PATH,
                    action="SKIP",
                    symbol=sym,
                    qty=qty,
                    est_price=est_price,
                    notional=abs(delta_notional),
                    reason="qty_rounded_to_zero",
                    target_weight=tw,
                    current_weight=cw,
                    portfolio_equity=total_equity
                )
                continue

            order_info = {
                "symbol": sym,
                "qty": final_qty,
                "side": side,
                "est_price": est_price,
                "delta_notional": delta_notional,
                "tw": tw,
                "cw": cw,
                "avg_entry_price": positions.get(sym, {}).get("avg_entry_price", 0.0),
            }

            if side == OrderSide.SELL:
                sell_orders.append(order_info)
            else:
                # Wash sale check: redirect buy to substitute if cooldown active
                if is_wash_sale_cooldown_active(state.loss_sales, sym):
                    sub = WASH_SALE_SUBSTITUTES.get(sym)
                    if sub:
                        log.info(f"[WASH_SALE] Redirecting buy {sym} -> {sub} (cooldown active)")
                        order_info["symbol"] = sub
                        order_info["original_symbol"] = sym
                        if state.active_substitutions is None:
                            state.active_substitutions = {}
                        state.active_substitutions[sub] = sym
                    else:
                        log.warning(f"[WASH_SALE] Skipping buy of {sym} — no substitute, "
                                    f"cooldown active until {state.loss_sales[sym]}")
                        continue
                buy_orders.append(order_info)

        # Helper to submit a single order
        def submit_order(order_info: dict) -> Optional[str]:
            sym = order_info["symbol"]
            final_qty = order_info["qty"]
            side = order_info["side"]
            est_price = order_info["est_price"]
            delta_notional = order_info["delta_notional"]
            tw = order_info["tw"]
            cw = order_info["cw"]

            # Dry-run mode check
            if DRY_RUN:
                log.info(f"[DRY-RUN] Would {side.name} {final_qty:.4f} {sym} @ ${est_price:.2f} "
                        f"(notional=${abs(delta_notional):.2f})")
                log_trade(
                    path=LOG_PATH,
                    action=f"DRY_RUN_{side.name}",
                    symbol=sym,
                    qty=final_qty,
                    est_price=est_price,
                    notional=abs(delta_notional),
                    reason="dry_run_weekly_rebalance",
                    target_weight=tw,
                    current_weight=cw,
                    portfolio_equity=total_equity
                )
                return None

            # Submit market order
            try:
                # FIX: Use deterministic client_order_id for idempotency
                client_oid = generate_client_order_id("rebalance", sym, side.name)
                req = MarketOrderRequest(
                    symbol=sym,
                    qty=final_qty,
                    side=side,
                    time_in_force=TimeInForce.DAY,
                    client_order_id=client_oid
                )

                order = trading.submit_order(req)
                order_id = order.id

                log.info(f"Submitted order {order_id}: {side.name} {final_qty:.4f} {sym} @ ~${est_price:.2f} (Client ID: {client_oid})")

                log_trade(
                    path=LOG_PATH,
                    action=side.name,
                    symbol=sym,
                    qty=final_qty,
                    est_price=est_price,
                    notional=abs(delta_notional),
                    reason="weekly_rebalance_trend_vol_target",
                    target_weight=tw,
                    current_weight=cw,
                    portfolio_equity=total_equity,
                    order_id=order_id
                )
                return order_id
            except Exception as e:
                log.error(f"Failed to submit order for {sym}: {e}")
                log_trade(
                    path=LOG_PATH,
                    action="FAILED",
                    symbol=sym,
                    qty=final_qty,
                    est_price=est_price,
                    notional=abs(delta_notional),
                    reason=f"order_failed: {e}",
                    target_weight=tw,
                    current_weight=cw,
                    portfolio_equity=total_equity
                )
                return None

        # 7) Execute orders: SELLS FIRST to free up buying power
        order_ids = []

        if sell_orders:
            log.info(f"Executing {len(sell_orders)} SELL orders first...")
            for order_info in sell_orders:
                oid = submit_order(order_info)
                if oid:
                    order_ids.append(oid)

            # Wait for sell orders to be accepted/filled before buying
            if order_ids and not DRY_RUN:
                log.info("Waiting for sell orders to process before buying...")
                time.sleep(5)  # Brief pause for order acceptance

            # Record loss sales for wash sale tracking
            for order_info in sell_orders:
                sym = order_info["symbol"]
                avg_entry = order_info.get("avg_entry_price", 0.0)
                est_price = order_info["est_price"]
                if avg_entry > 0 and est_price < avg_entry:
                    loss_pct = ((est_price - avg_entry) / avg_entry) * 100
                    if state.loss_sales is None:
                        state.loss_sales = {}
                    state.loss_sales[sym] = today_iso_et()
                    log.info(f"[WASH_SALE] Recorded loss sale: {sym} @ ${est_price:.2f} "
                             f"(entry ${avg_entry:.2f}, {loss_pct:+.1f}%) — "
                             f"{WASH_SALE_COOLDOWN_DAYS}-day cooldown started")

        if buy_orders:
            # PRE-FLIGHT: Check buying power before submitting BUY orders
            # Sells have already executed; verify we have enough capital for buys
            if not DRY_RUN:
                try:
                    acct = trading.get_account()
                    available_bp = float(acct.buying_power)
                    total_buy_notional = sum(abs(o["delta_notional"]) for o in buy_orders)

                    # Apply 0.5% safety buffer for fill slippage and rounding
                    # Prevents failures like: need=$6770 available=$6769 (off by $1)
                    effective_bp = available_bp * 0.995

                    if total_buy_notional > effective_bp:
                        # Minimum $5 buying power required to place any meaningful order
                        if effective_bp < 5.0:
                            log.warning(f"[REBALANCE] No usable buying power (${available_bp:.2f}) — skipping all buys")
                            buy_orders = []
                        else:
                            # Scale down buy orders proportionally to fit available buying power
                            scale_factor = effective_bp / total_buy_notional if total_buy_notional > 0 else 0
                            log.warning(f"[REBALANCE] Insufficient buying power for all buys | "
                                       f"need=${total_buy_notional:,.0f} available=${available_bp:,.0f} "
                                       f"(effective=${effective_bp:,.0f} after 0.5% buffer) | "
                                       f"scaling buys to {scale_factor:.0%}")

                            scaled_buy_orders = []
                            for o in buy_orders:
                                scaled_qty = o["qty"] * scale_factor
                                # Respect fractional vs whole share rules
                                if not ALLOW_FRACTIONAL:
                                    scaled_qty = int(scaled_qty)
                                # Ensure order meets minimum notional ($1 Alpaca minimum)
                                scaled_notional = scaled_qty * o.get("est_price", o.get("delta_notional", 0) / max(o.get("qty", 1), 0.001))
                                if scaled_qty > 0 and scaled_notional >= 1.0:
                                    scaled_order = dict(o)
                                    scaled_order["qty"] = scaled_qty
                                    scaled_order["delta_notional"] = scaled_qty * o["est_price"]
                                    scaled_buy_orders.append(scaled_order)
                                elif scaled_qty > 0:
                                    log.info(f"[REBALANCE] Skipping {o['symbol']} buy — notional ${scaled_notional:.2f} below $1 minimum")
                                else:
                                    log.info(f"[REBALANCE] Skipping {o['symbol']} buy (scaled qty too small)")
                            buy_orders = scaled_buy_orders

                        alerter.send_alert(
                            level="WARNING",
                            title="Rebalance: Buy Orders Scaled Down",
                            message=f"Insufficient buying power for full rebalance.\n"
                                    f"Needed: ${total_buy_notional:,.0f}\n"
                                    f"Available: ${available_bp:,.0f}\n"
                                    f"Scaled to {scale_factor:.0%} of target buys.\n"
                                    f"Portfolio may have drift from target weights.",
                            context={"scale_factor": f"{scale_factor:.2f}"}
                        )
                    else:
                        log.info(f"[REBALANCE] Buying power OK | need=${total_buy_notional:,.0f} available=${available_bp:,.0f}")
                except Exception as bp_err:
                    log.warning(f"[REBALANCE] Could not check buying power: {bp_err} | proceeding with buy orders")

            log.info(f"Executing {len(buy_orders)} BUY orders...")
            for order_info in buy_orders:
                oid = submit_order(order_info)
                if oid:
                    order_ids.append(oid)

        # 8) Verify order execution and track partial fills
        partial_fills = []
        if order_ids and not DRY_RUN:
            log.info(f"Verifying {len(order_ids)} orders...")
            for oid in order_ids:
                success, status, filled_qty = verify_order_execution(trading, oid, timeout_sec=60)
                if not success and filled_qty > 0:
                    partial_fills.append({"order_id": oid, "status": status, "filled_qty": filled_qty})

            if partial_fills:
                log.warning(f"WARNING: {len(partial_fills)} orders had partial fills - drift may exist")
                alerter.send_alert(
                    level="WARNING",
                    title="Partial Order Fills Detected",
                    message=f"{len(partial_fills)} orders were only partially filled during rebalance. "
                            f"Portfolio may have drift from target weights.",
                    context={"partial_fills": str(partial_fills)}
                )

        # 8) Position reconciliation
        if not DRY_RUN:
            time.sleep(10)  # Give orders time to settle
            updated_positions = get_positions(trading)
            reconcile_positions(target_w, updated_positions, deployable_capital)

        # 9) Save targets to state
        state.last_target_weights = target_w
        state.last_rebalance_date_iso = today_iso_et()
        state.rebalance_in_progress = False
        state.rebalance_started_at_iso = None  # Clear timestamp on success
        save_state(STATE_PATH, state)

        # 10) Log diagnostics
        port_diag = diag.get("_portfolio", {})
        log.info("=== Rebalance Complete ===")
        log.info(f"Portfolio Diagnostics:\n{json.dumps(port_diag, indent=2)}")

        # 11) Log rebalance summary for performance tracking
        try:
            # Compute SPY vs SMA200 percentage
            spy_price = float(spy_close.iloc[-1]) if len(spy_close) > 0 else 0.0
            spy_sma200 = sma(spy_close, SMA_LOOKBACK_DAYS) if len(spy_close) >= SMA_LOOKBACK_DAYS else spy_price
            spy_vs_sma200_pct = ((spy_price - spy_sma200) / spy_sma200 * 100) if spy_sma200 > 0 else 0.0

            # Compute drawdown
            drawdown_pct = ((state.equity_peak - total_equity) / state.equity_peak * 100) if state.equity_peak and state.equity_peak > 0 else 0.0

            # Get exposure multiplier from diag
            exposure_mult = port_diag.get("exposure_mult", 1.0)

            # Compute actual turnover (or get from earlier computation)
            try:
                final_turnover = compute_portfolio_turnover(current_w, target_w)
            except Exception:
                final_turnover = 0.0

            # Get final position values for the log
            final_positions = get_positions(trading) if not DRY_RUN else positions
            position_weights = {sym: pos.get("market_value", 0.0) / total_equity
                              for sym, pos in final_positions.items() if pos.get("market_value", 0.0) > 0}

            log_rebalance_summary(
                event="REBALANCE",
                regime=current_regime,
                spy_price=spy_price,
                spy_vs_sma200_pct=spy_vs_sma200_pct,
                equity=total_equity,
                equity_peak=state.equity_peak or total_equity,
                drawdown_pct=drawdown_pct,
                exposure_mult=exposure_mult,
                capital_usage_pct=capital_usage_pct,
                deployable_capital=deployable_capital,
                cash_reserve=cash_reserve,
                positions=position_weights,
                target_weights=target_w,
                risk_score=risk_score_for_capital,
                event_mult=event_mult_for_capital,
                turnover_pct=final_turnover * 100,
                orders_placed=len(order_ids),
                notes=f"{capital_reason}; orders={len(sell_orders)}S/{len(buy_orders)}B"
            )
        except Exception as e:
            log.warning(f"Failed to log rebalance summary: {e}")

    except Exception as e:
        log.error(f"Rebalance failed: {e}")
        traceback.print_exc()
        # Clear rebalance flag on error
        state.rebalance_in_progress = False
        state.rebalance_started_at_iso = None  # Clear timestamp on error
        save_state(STATE_PATH, state)
        raise


# =========================
# CONFIGURATION VALIDATION
# =========================

def validate_configuration() -> None:
    """Validate all configuration parameters before starting."""
    log.info("=== Validating Configuration ===")

    errors = []

    # Check environment variables
    if not os.getenv("ALPACA_API_KEY"):
        errors.append("Missing ALPACA_API_KEY environment variable")
    if not os.getenv("ALPACA_SECRET_KEY"):
        errors.append("Missing ALPACA_SECRET_KEY environment variable")

    if DATA_SOURCE == "polygon" and not os.getenv("POLYGON_API_KEY"):
        errors.append("DATA_SOURCE=polygon but POLYGON_API_KEY not set")

    # Validate parameters
    if SMA_LOOKBACK_DAYS <= 0:
        errors.append(f"Invalid SMA_LOOKBACK_DAYS: {SMA_LOOKBACK_DAYS}")
    if VOL_LOOKBACK_DAYS <= 0:
        errors.append(f"Invalid VOL_LOOKBACK_DAYS: {VOL_LOOKBACK_DAYS}")
    if not (0 < TARGET_GROSS_EXPOSURE <= 1.0):
        errors.append(f"Invalid TARGET_GROSS_EXPOSURE: {TARGET_GROSS_EXPOSURE}")
    if not (0 < MAX_WEIGHT_PER_ASSET <= 1.0):
        errors.append(f"Invalid MAX_WEIGHT_PER_ASSET: {MAX_WEIGHT_PER_ASSET}")
    if not (0 <= DRAWDOWN_TRIGGER <= 1.0):
        errors.append(f"Invalid DRAWDOWN_TRIGGER: {DRAWDOWN_TRIGGER}")
    if not (0 < DRAWDOWN_EXPOSURE_MULT <= 1.0):
        errors.append(f"Invalid DRAWDOWN_EXPOSURE_MULT: {DRAWDOWN_EXPOSURE_MULT}")

    # Validate tickers
    if not ALL_TICKERS:
        errors.append("No tickers defined in universe")
    if CASH_TICKER not in ALL_TICKERS:
        errors.append(f"CASH_TICKER {CASH_TICKER} not in ALL_TICKERS")

    # Validate rebalance schedule
    if not (0 <= REBALANCE_WEEKDAY <= 4):
        errors.append(f"Invalid REBALANCE_WEEKDAY: {REBALANCE_WEEKDAY}")

    if errors:
        for err in errors:
            log.error(f"Config error: {err}")
        raise RuntimeError(f"Configuration validation failed with {len(errors)} error(s)")

    log.info("Configuration validation passed")
    log.info(f"Universe: {len(ALL_TICKERS)} tickers")
    log.info(f"Data source: {DATA_SOURCE}")
    log.info(f"Dry-run mode: {DRY_RUN}")
    log.info(f"Rebalance: {['Mon','Tue','Wed','Thu','Fri'][REBALANCE_WEEKDAY]} @ "
            f"{REBALANCE_TIME_ET[0]:02d}:{REBALANCE_TIME_ET[1]:02d} ET")


# =========================
# EMERGENCY LIQUIDATION
# =========================

def liquidate(trading: TradingClient) -> None:
    """Emergency liquidation: cancel all TBOT_ orders and close all trend_bot positions.

    SHARED-ACCOUNT SAFETY: Only cancels TBOT_ prefixed orders and closes
    positions in ALL_TICKERS universe. Preserves other bots' orders/positions.
    Ignores DRY_RUN — this is an explicit safety action.
    """
    # 1. Cancel TBOT_ orders
    try:
        all_orders = trading.get_orders()
        our_orders = [o for o in all_orders
                      if (o.client_order_id or "").startswith("TBOT_")]
        for order in our_orders:
            try:
                trading.cancel_order_by_id(order.id)
            except Exception:
                pass
        other_count = len(all_orders) - len(our_orders)
        log.info(f"[LIQUIDATE] Cancelled {len(our_orders)} TBOT_ orders"
                 f"{f' (preserved {other_count} other bot orders)' if other_count else ''}")
    except Exception as e:
        log.error(f"[LIQUIDATE] Error cancelling orders: {e}")

    # 2. Close positions in ALL_TICKERS
    positions = trading.get_all_positions()
    our_positions = [p for p in positions if p.symbol in ALL_TICKERS]
    other_count = len(positions) - len(our_positions)

    if not our_positions:
        log.info("[LIQUIDATE] No trend_bot positions to close.")
        return

    log.info(f"[LIQUIDATE] Closing {len(our_positions)} positions"
             f"{f' (preserving {other_count} other bot positions)' if other_count else ''}...")

    failed = []
    for pos in our_positions:
        try:
            trading.close_position(pos.symbol)
            log.info(f"  Submitted close for {pos.symbol} "
                     f"({pos.qty} shares, ${float(pos.market_value):,.2f})")
        except Exception as e:
            log.error(f"  FAILED to close {pos.symbol}: {e}")
            failed.append(pos.symbol)

    # 3. Verify closures (if market is open)
    if is_market_open(trading):
        time.sleep(5)
        remaining = trading.get_all_positions()
        still_open = [p.symbol for p in remaining if p.symbol in ALL_TICKERS]
        if still_open:
            log.warning(f"[LIQUIDATE] Positions still open after 5s: {still_open}")
        else:
            log.info("[LIQUIDATE] All positions confirmed closed.")
    else:
        log.info("[LIQUIDATE] Market closed — close orders queued for next open.")

    if failed:
        log.error(f"[LIQUIDATE] Failed to submit close for: {failed}")


# =========================
# MAIN LOOP (continuous)
# =========================

def main(force_rebalance: bool = False) -> None:
    """Main continuous loop with Level 3 production features.

    Args:
        force_rebalance: If True, execute one immediate rebalance and exit.
    """

    # LEVEL 3: Live trading validation
    # FIX: Single source of truth - LIVE_TRADING_ENABLED is the ONLY control
    # We no longer check ALPACA_BASE_URL since get_trading_client() ignores it

    if LIVE_TRADING_ENABLED:
        if LIVE_TRADING_CONFIRMATION != "YES":
            raise RuntimeError(
                "LIVE_TRADING=1 requires I_UNDERSTAND_LIVE_TRADING=YES environment variable. "
                "This bot will trade REAL MONEY. Ensure you understand the risks."
            )
        log.warning("="*60)
        log.warning("***  LIVE TRADING ENABLED - REAL MONEY AT RISK  ***")
        log.warning("="*60)
    else:
        log.info("[MODE] Paper trading (simulated money)")

    # Validate configuration
    validate_configuration()

    # Initialize clients
    trading = get_trading_client()
    data_client = get_data_client()

    # Validate API credentials by making a test call
    log.info("Validating API credentials...")
    try:
        account = trading.get_account()
        log.info(f"API credentials valid | Account: {account.account_number} | "
                f"Equity: ${float(account.equity):,.2f} | "
                f"Status: {account.status}")
        if account.status != "ACTIVE":
            log.warning(f"Account status is {account.status}, not ACTIVE - trading may be restricted")
    except Exception as e:
        log.error(f"Failed to validate Alpaca API credentials: {e}")
        raise RuntimeError(f"API credential validation failed: {e}")

    state = load_state(STATE_PATH)

    # FIX: Clear stale rebalance_in_progress flag on startup (prevents deadlock after crash)
    clear_stale_rebalance_flag(state, STATE_PATH)

    log.info("=" * 60)
    log.info("LEVEL 3 Volatility-Targeted Trend Following Bot")
    log.info("=" * 60)
    log.info(f"State file: {STATE_PATH}")
    log.info(f"Trade log: {LOG_PATH}")

    if DRY_RUN:
        log.warning("DRY-RUN MODE: No actual orders will be placed")

    # FORCE REBALANCE MODE: Execute one rebalance and exit
    if force_rebalance:
        log.info("=" * 60)
        log.info("FORCE REBALANCE MODE - Executing immediate rebalance")
        log.info("=" * 60)

        # Verify market is open
        if not is_market_open(trading):
            log.error("[FORCE_REBALANCE] Market is closed. Cannot rebalance.")
            log.info("Market must be open to execute trades. Try again during market hours.")
            return

        try:
            rebalance(trading, data_client, state)
            log.info("[FORCE_REBALANCE] Rebalance completed successfully.")
        except Exception as e:
            log.error(f"[FORCE_REBALANCE] Error during rebalance: {e}")
            traceback.print_exc()
            raise

        log.info("=" * 60)
        log.info("FORCE REBALANCE COMPLETE - Exiting")
        log.info("=" * 60)
        return  # Exit after force rebalance

    # LEVEL 3: Setup graceful shutdown
    def shutdown_handler(sig, frame):
        log.info("[SHUTDOWN] Received interrupt signal")
        if not DRY_RUN:
            # SHARED-ACCOUNT SAFETY: Only cancel/close trend_bot's own orders and positions
            # Preserves simple_bot's and directional_bot's orders/positions
            def _cancel_our_orders():
                """Cancel only TBOT_ prefixed orders."""
                try:
                    all_orders = trading.get_orders()
                    our_orders = [o for o in all_orders
                                 if (o.client_order_id or "").startswith("TBOT_")]
                    for order in our_orders:
                        try:
                            trading.cancel_order_by_id(order.id)
                        except Exception:
                            pass
                    other_count = len(all_orders) - len(our_orders)
                    log.info(f"[SHUTDOWN] Cancelled {len(our_orders)} of our orders"
                            f"{f' (preserved {other_count} other bot orders)' if other_count else ''}")
                except Exception as e:
                    log.error(f"[SHUTDOWN] Error cancelling orders: {e}")

            if SHUTDOWN_POLICY == "FLATTEN_ALL":
                log.warning("[SHUTDOWN] FLATTEN_ALL policy - closing our positions")
                try:
                    _cancel_our_orders()
                    positions = trading.get_all_positions()
                    our_positions = [p for p in positions if p.symbol in ALL_TICKERS]
                    for pos in our_positions:
                        trading.close_position(pos.symbol)
                    other_count = len(positions) - len(our_positions)
                    log.info(f"[SHUTDOWN] Closed {len(our_positions)} of our positions"
                            f"{f' (preserved {other_count} other bot positions)' if other_count else ''}")
                except Exception as e:
                    log.error(f"[SHUTDOWN] Error flattening: {e}")
            else:
                log.info("[SHUTDOWN] CANCEL_ORDERS_ONLY policy")
                _cancel_our_orders()

        save_state(STATE_PATH, state)
        log.info("[SHUTDOWN] State saved. Exiting.")
        exit(0)

    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)

    while True:
        dt = now_et()

        try:
            # LEVEL 3: Check kill switch FIRST
            kill_triggered, kill_reason = kill_switch.is_triggered()
            if kill_triggered:
                log.error(f"[KILL_SWITCH] TRIGGERED: {kill_reason}")
                kill_switch.execute_emergency_shutdown(trading)
                alerter.send_alert(
                    level="CRITICAL",
                    title="Kill Switch Activated",
                    message=f"Emergency shutdown triggered: {kill_reason}",
                    context={"reason": kill_reason}
                )
                break

            # LEVEL 3: Check circuit breaker
            if circuit_breaker.is_halted():
                log.warning(f"[STATUS] Circuit breaker HALTED | reason: {circuit_breaker.get_halt_reason()}")
                time.sleep(60)
                continue

            # DAILY MONITORING: Run during market hours for risk management
            if ENABLE_DAILY_MONITORING and is_daily_monitoring_window(dt) and not state.rebalance_in_progress:
                try:
                    daily_position_monitoring(trading, data_client, state)
                except Exception as e:
                    log.error(f"[DAILY_MONITOR] Error during daily monitoring: {e}")
                    # Don't halt on daily monitoring errors - just log and continue
                    traceback.print_exc()

            # DRIFT-BASED MINI-REBALANCE: Check for significant position drift
            # Runs during market hours, outside of regular rebalance window
            # Respects DRIFT_CHECK_INTERVAL_MIN to avoid spamming on repeated failures
            _drift_ready = True
            if state.last_drift_mini_iso:
                try:
                    _last_drift = datetime.fromisoformat(state.last_drift_mini_iso)
                    _minutes_since = (now_et() - _last_drift).total_seconds() / 60
                    if _minutes_since < DRIFT_CHECK_INTERVAL_MIN:
                        _drift_ready = False
                except Exception:
                    pass  # Invalid timestamp — allow check

            if (ENABLE_DRIFT_MINI_REBALANCE
                and _drift_ready
                and is_market_open(trading)
                and not state.rebalance_in_progress
                and not is_rebalance_window_dynamic(trading)):  # Don't interfere with regular rebalance
                try:
                    positions = get_positions(trading)
                    total_equity = get_portfolio_equity(trading)
                    needs_mini, max_drift, drift_reason = check_drift_mini_rebalance_needed(
                        state, positions, total_equity, trading
                    )
                    if needs_mini:
                        log.info(f"[DRIFT_MINI] Triggered: {drift_reason}")
                        # Extract symbol from reason (format: "SYM drifted X%...")
                        max_sym = drift_reason.split()[0] if drift_reason else "unknown"
                        execute_drift_mini_rebalance(trading, data_client, state, max_sym)
                        save_state(STATE_PATH, state)
                except Exception as e:
                    log.error(f"[DRIFT_MINI] Error during drift check: {e}")
                    # Don't halt on drift errors - just log and continue

            # Only rebalance during window, only once per date
            already_done_today = (state.last_rebalance_date_iso == dt.date().isoformat())

            # FIX: Use dynamic rebalance window that handles early-close days
            if is_rebalance_window_dynamic(trading) and not already_done_today and not state.rebalance_in_progress:
                log.info(f"Rebalance window open. Running rebalance...")
                try:
                    rebalance(trading, data_client, state)
                except Exception as e:
                    log.error(f"[REBALANCE] Error during rebalance: {e}")
                    circuit_breaker.record_api_failure("Rebalance")

                    # Alert on rebalance failure
                    alerter.send_alert(
                        level="CRITICAL",
                        title="Rebalance Failed",
                        message=f"Weekly rebalance failed: {str(e)}",
                        context={"error": str(e), "date": dt.date().isoformat()}
                    )
                    raise
            else:
                # Status heartbeat
                cooldown = state.drawdown_cooldown_until_iso or "none"
                rebal_status = " [IN PROGRESS]" if state.rebalance_in_progress else ""
                log.info(
                    f"Waiting{rebal_status}... "
                    f"last_rebalance={state.last_rebalance_date_iso}, "
                    f"cooldown_until={cooldown}"
                )

            time.sleep(60)

        except KeyboardInterrupt:
            log.info("Received keyboard interrupt. Shutting down...")
            shutdown_handler(None, None)
        except Exception as e:
            # LEVEL 3: Record API failure before halting
            circuit_breaker.record_api_failure("Main")

            log.critical(f"FATAL ERROR: {repr(e)}")
            traceback.print_exc()

            # Alert before halting
            alerter.send_alert(
                level="CRITICAL",
                title="Bot Fatal Error",
                message=f"Bot encountered fatal error and halted: {str(e)}",
                context={"error": str(e), "traceback": traceback.format_exc()[:500]}
            )
            raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Volatility-Targeted Trend Following Bot",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python trend_bot.py                  # Normal continuous operation
  python trend_bot.py --rebalance      # Force immediate rebalance and exit
  python trend_bot.py --status         # Show current state and positions
  python trend_bot.py --liquidate      # Emergency: close all positions and exit
        """
    )
    parser.add_argument(
        "--rebalance", "-r",
        action="store_true",
        help="Force an immediate rebalance (market must be open) and exit"
    )
    parser.add_argument(
        "--status", "-s",
        action="store_true",
        help="Show current bot state and positions, then exit"
    )
    parser.add_argument(
        "--diagnose", "-d",
        action="store_true",
        help="Run data diagnostics to check momentum ranking inputs"
    )
    parser.add_argument(
        "--liquidate", "-l",
        action="store_true",
        help="Emergency liquidation: cancel all trend_bot orders and close all positions, then exit"
    )

    args = parser.parse_args()

    if args.liquidate:
        log.info("=" * 60)
        log.info("EMERGENCY LIQUIDATION MODE")
        log.info("=" * 60)
        trading = get_trading_client()
        liquidate(trading)
        log.info("=" * 60)
        log.info("LIQUIDATION COMPLETE - Exiting")
        log.info("=" * 60)
    elif args.diagnose:
        # Run data diagnostics without trading
        print("=" * 70)
        print("TREND BOT DATA DIAGNOSTICS")
        print("=" * 70)
        print()

        # Initialize clients
        trading = get_trading_client()
        data_client = get_data_client()
        diag_cache = DataCache()

        # Calculate required lookback
        min_length_needed = MOMENTUM_LOOKBACK_252D + SKIP_MONTH_DAYS + 1
        max_trading_days_needed = max(SMA_LOOKBACK_DAYS, VOL_LOOKBACK_DAYS, min_length_needed) + 10
        lookback_days = int(max_trading_days_needed * 1.43) + 30

        print(f"[CONFIG] Momentum requires {min_length_needed} trading days (252 + {SKIP_MONTH_DAYS} skip + 1)")
        print(f"[CONFIG] SMA lookback: {SMA_LOOKBACK_DAYS} days")
        print(f"[CONFIG] Fetching {lookback_days} calendar days of data")
        print()

        # Fetch bars
        all_symbols = list(set(ALL_TICKERS + TACTICAL_ETFS))
        print(f"[DATA] Fetching bars for {len(all_symbols)} symbols...")
        bars = diag_cache.get_bars(data_client, all_symbols, lookback_days)
        print(f"[DATA] Total bars fetched: {len(bars)}")
        print()

        # Check each symbol's data length
        print(f"{'Symbol':<8} {'Bars':>6} {'Required':>10} {'Status':<20}")
        print("-" * 50)

        sufficient_count = 0
        insufficient_syms = []

        for sym in sorted(ALL_EQUITY + DEFENSIVE_TICKERS):
            try:
                close = get_close_series(bars, sym, strip_incomplete=True, trading_client=trading)
                bar_count = len(close)
                if bar_count >= min_length_needed:
                    status = "OK"
                    sufficient_count += 1
                else:
                    status = f"INSUFFICIENT (-{min_length_needed - bar_count})"
                    insufficient_syms.append(sym)
                print(f"{sym:<8} {bar_count:>6} {min_length_needed:>10} {status:<20}")
            except Exception as e:
                print(f"{sym:<8} {'N/A':>6} {min_length_needed:>10} ERROR: {e}")
                insufficient_syms.append(sym)

        print("-" * 50)
        print(f"Total: {sufficient_count}/{len(ALL_EQUITY) + len(DEFENSIVE_TICKERS)} symbols have sufficient data")
        print()

        if insufficient_syms:
            print(f"[WARNING] Insufficient data for: {insufficient_syms}")
            print()

        # Test momentum ranking
        print("[TEST] Running momentum ranking...")
        equity_ranked = rank_by_momentum(bars, ALL_EQUITY, trading, top_n=0)
        defensive_ranked = rank_by_momentum(bars, DEFENSIVE_TICKERS, trading, top_n=0)

        print(f"[RESULT] Equity ranked: {len(equity_ranked)}/{len(ALL_EQUITY)} symbols")
        if equity_ranked:
            print(f"         Top 6: {[s for s, _ in equity_ranked[:6]]}")
        else:
            print("         WARNING: Empty list - this is the bug!")

        print(f"[RESULT] Defensive ranked: {len(defensive_ranked)}/{len(DEFENSIVE_TICKERS)} symbols")
        if defensive_ranked:
            print(f"         Top 2: {[s for s, _ in defensive_ranked[:2]]}")
        else:
            print("         WARNING: Empty list - this is the bug!")

        print()
        print("=" * 70)

    elif args.status:
        # Quick status check without entering main loop
        state = load_state(STATE_PATH)
        trading = get_trading_client()
        positions = get_positions(trading)

        print("=" * 60)
        print("TREND BOT STATUS")
        print("=" * 60)
        print(f"State file: {STATE_PATH}")
        print(f"Last rebalance: {state.last_rebalance_date_iso}")
        print(f"SPY regime: {state.spy_regime}")
        print(f"Equity peak: ${state.equity_peak:,.2f}" if state.equity_peak else "Equity peak: N/A")
        print(f"Last equity: ${state.last_equity:,.2f}" if state.last_equity else "Last equity: N/A")
        print(f"Drawdown cooldown: {state.drawdown_cooldown_until_iso or 'None'}")
        print(f"Rebalance in progress: {state.rebalance_in_progress}")
        print()
        print(f"POSITIONS ({len(positions)}):")
        print("-" * 40)
        if positions:
            for sym, data in sorted(positions.items()):
                qty = data["qty"]
                mv = data["market_value"]
                print(f"  {sym}: {qty:.4f} shares (${mv:,.2f})")
        else:
            print("  (no positions)")
        print("=" * 60)
    else:
        main(force_rebalance=args.rebalance)
