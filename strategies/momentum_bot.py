"""
Catalyst Momentum Bot - Explosive Move Capture
===============================================

Strategy: Capture violent, asymmetric upside following binary catalysts

Target Profile:
- Biotech/pharma with FDA news, trial results, EUA announcements
- Small-cap tech/energy with binary events
- Gap +20-30% minimum
- Extreme relative volume (10x-50x)
- Low-to-mid float preferred

Entry:
- Breakout of pre-market high, halt high, or first 5-min candle
- Market orders only (speed > precision)
- Enter strength, not pullbacks

Exit:
- Aggressive scaling: 25-50% into first spike
- Momentum-based exits (volume climax, bid collapse, momentum break)
- Hard stops below structure

Philosophy:
- One winner covers multiple losers (option-like behavior)
- Profits taken fast and unapologetically
- Trade crowd psychology, not fundamentals

Mode:
- HYBRID: Automated scanning + alerts for manual execution
- Best setups require tape reading and discretion

Author: Claude Code
Version: 1.0.0
"""

from __future__ import annotations

import os
import json
import time
import signal
import logging
import threading
import datetime as dt
import smtplib
import hashlib
from dataclasses import dataclass, asdict
from enum import Enum
from typing import Dict, List, Optional, Tuple, Set
from logging.handlers import RotatingFileHandler
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

import requests
import pandas as pd
from zoneinfo import ZoneInfo

# ============================================================
# CONFIGURATION
# ============================================================

ET = ZoneInfo("America/New_York")

# --- Directory Paths (for organized folder structure) ---
from pathlib import Path
ALGO_ROOT = Path(__file__).parent.parent  # Algo_Trading root
# Allow output dir override (useful when source lives on Google Drive)
_output_root = Path(os.getenv("ALGO_OUTPUT_DIR", "")) if os.getenv("ALGO_OUTPUT_DIR") else ALGO_ROOT
DATA_DIR = _output_root / "data"
LOGS_DIR = _output_root / "logs"
CONFIG_DIR = ALGO_ROOT / "config"
DATA_DIR.mkdir(exist_ok=True)
LOGS_DIR.mkdir(exist_ok=True)

# --- API Credentials ---
ALPACA_API_KEY = os.getenv("ALPACA_API_KEY", "").strip()
ALPACA_SECRET_KEY = os.getenv("ALPACA_SECRET_KEY", "").strip()
ALPACA_BASE_URL = os.getenv("ALPACA_BASE_URL", "https://paper-api.alpaca.markets").strip()

POLYGON_API_KEY = (os.getenv("POLYGON_API_KEY") or os.getenv("MASSIVE_API_KEY") or "").strip()
POLYGON_REST_BASE = "https://api.polygon.io"

# --- Alert Configuration ---
ENABLE_ALERTS = True
ALERT_LOG_PATH = str(DATA_DIR / "catalyst_alerts.jsonl")
ENABLE_SOUND_ALERTS = os.getenv("ENABLE_SOUND", "false").lower() == "true"

# --- Logging ---
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
TRADE_LOG_PATH = str(DATA_DIR / "catalyst_trades.jsonl")

# --- Catalyst Detection ---
MIN_GAP_PERCENT = 20.0          # Minimum gap to trigger (20%)
IDEAL_GAP_PERCENT = 30.0        # Ideal gap size (30%+)
MIN_RELATIVE_VOLUME = 10.0      # Minimum relative volume (10x normal)
MAX_PRICE_PREFILTER = 50.0      # Max price for initial scan
MIN_PRICE_PREFILTER = 1.0       # Min price for initial scan

# --- Sector Focus ---
BIOTECH_SECTORS = [
    "Biotechnology", "Pharmaceuticals", "Healthcare", "Medical Devices",
    "Life Sciences", "Drug Manufacturers", "Biotech"
]

# --- Trading Sessions ---
TRADE_PREMARKET = True          # Enable pre-market trading
TRADE_MARKET_OPEN = True        # Enable market open (9:30-10:30 ET)
OPTIMAL_WINDOW_MINUTES = 30     # First 30 minutes = prime time

# --- Position Sizing (OPTIMIZED) ---
MAX_POSITION_SIZE_PCT = 0.03    # 3% of capital max (reduced from 5% for safety)
RISK_PER_TRADE_PCT = 0.015      # 1.5% risk per trade (reduced from 2% for safety)
MAX_CONCURRENT_POSITIONS = 1    # Only one explosive position at a time

# --- Entry Rules (OPTIMIZED) ---
BREAKOUT_BUFFER_PCT = 0.01      # 1% above breakout (reduced from 2% for better fills)
VOLUME_SPIKE_MULTIPLIER = 1.5   # Volume 1.5x average (reduced from 2.0 to catch more setups)

# --- Exit Strategy (OPTIMIZED) ---
FIRST_SCALE_PERCENT = 0.33      # Sell 33% into first spike (increased from 25% to lock profit)
FIRST_SCALE_GAIN_PCT = 0.10     # Take first partial at +10% (reduced from 15% to catch spike)
SECOND_SCALE_PERCENT = 0.33     # Sell another 33% (increased from 25%)
SECOND_SCALE_GAIN_PCT = 0.20    # Take second partial at +20% (reduced from 30% for realism)
THIRD_SCALE_PERCENT = 0.34      # Sell final 34% on momentum break or target
MOMENTUM_BREAK_EXIT = True      # Exit all on momentum break

# Stop Loss (OPTIMIZED)
INITIAL_STOP_PCT = 0.06         # 6% initial stop (reduced from 8% for tighter risk)
BREAKEVEN_MOVE_TRIGGER = 0.08   # Move stop to breakeven at +8% (reduced from 10%)

# --- Momentum Detection ---
VOLUME_CLIMAX_THRESHOLD = 3.0   # Volume spike 3x = possible climax
WICK_WARNING_PCT = 0.05         # Upper wick >5% of candle = warning sign

# --- Scanning ---
SCAN_INTERVAL_SECONDS = 10      # Scan every 10 seconds during active hours
HALT_CHECK_INTERVAL = 5         # Check for halt resumes every 5 seconds

# --- Risk Management ---
MAX_DAILY_LOSS_PCT = 0.05       # 5% daily loss limit (higher due to strategy nature)
MAX_LOSING_TRADES_PER_DAY = 3   # Stop after 3 losing trades in a day


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
LOG_FILE_PATH = os.getenv("LOG_FILE_PATH", str(LOGS_DIR / "momentum_bot.log"))
MAX_LOG_SIZE_MB = int(os.getenv("MAX_LOG_SIZE_MB", "50"))
MAX_LOG_BACKUPS = int(os.getenv("MAX_LOG_BACKUPS", "5"))

# --- Kill Switch ---
KILL_SWITCH_FILE = str(DATA_DIR / "KILL_SWITCH")
KILL_SWITCH_ENV = os.getenv("KILL_SWITCH", "0") == "1"

# --- Graceful Shutdown Policy ---
SHUTDOWN_POLICY = os.getenv("SHUTDOWN_POLICY", "CANCEL_ORDERS_ONLY").upper()  # CANCEL_ORDERS_ONLY | FLATTEN_ALL


# ============================================================
# VALIDATION
# ============================================================

if not ALPACA_API_KEY or not ALPACA_SECRET_KEY:
    raise RuntimeError("Missing ALPACA_API_KEY / ALPACA_SECRET_KEY")
if not POLYGON_API_KEY:
    raise RuntimeError("Missing POLYGON_API_KEY")


# ============================================================
# LOGGING SETUP (LEVEL 3: with rotation)
# ============================================================

logger = logging.getLogger("CatalystBot")
logger.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))

# Prevent duplicate handlers
if not logger.handlers:
    formatter = logging.Formatter('[%(asctime)s] %(levelname)s: %(message)s', datefmt='%H:%M:%S')

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # File handler with rotation (Level 3)
    file_handler = RotatingFileHandler(
        LOG_FILE_PATH,
        maxBytes=MAX_LOG_SIZE_MB * 1024 * 1024,
        backupCount=MAX_LOG_BACKUPS
    )
    file_handler.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))
    file_handler.setFormatter(formatter)
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

def play_alert_sound():
    """Play alert sound (platform-specific)."""
    if not ENABLE_SOUND_ALERTS:
        return
    try:
        # Windows
        if os.name == 'nt':
            import winsound
            winsound.Beep(1000, 500)  # 1000 Hz for 500ms
        # macOS/Linux
        else:
            os.system('printf "\a"')
    except Exception:
        pass


# ============================================================
# LEVEL 3: Alerter & Kill Switch
# ============================================================

class Alerter:
    """Multi-channel alerting for unattended operation (Slack + Email)."""

    def __init__(self):
        self.slack_enabled = ENABLE_SLACK_ALERTS and SLACK_WEBHOOK_URL
        self.email_enabled = ENABLE_EMAIL_ALERTS and ALERT_EMAIL_TO and SMTP_USERNAME

        if self.slack_enabled:
            logger.info("[ALERTER] Slack alerts ENABLED")
        if self.email_enabled:
            logger.info("[ALERTER] Email alerts ENABLED")
        if not self.slack_enabled and not self.email_enabled:
            logger.warning("[ALERTER] NO ALERTS CONFIGURED - unattended operation not recommended")

    def send_alert(self, level: str, title: str, message: str, context: dict = None):
        """Send alert via all enabled channels (INFO, WARNING, CRITICAL)."""
        # Always log locally
        log_msg = f"[ALERT {level}] {title}: {message}"
        if context:
            log_msg += f" | {context}"

        if level == "CRITICAL":
            logger.error(log_msg)
        elif level == "WARNING":
            logger.warning(log_msg)
        else:
            logger.info(log_msg)

        # Send to external channels
        if self.slack_enabled:
            self._send_slack(level, title, message, context)
        if self.email_enabled:
            self._send_email(level, title, message, context)

        # Play sound for critical alerts
        if level == "CRITICAL":
            play_alert_sound()

    def _send_slack(self, level: str, title: str, message: str, context: dict = None):
        """Send Slack webhook notification."""
        try:
            colors = {"INFO": "#36a64f", "WARNING": "#ff9900", "CRITICAL": "#ff0000"}
            color = colors.get(level, "#808080")

            payload = {
                "attachments": [{
                    "color": color,
                    "title": f"{level}: {title}",
                    "text": message,
                    "fields": [{"title": k, "value": str(v), "short": True} for k, v in (context or {}).items()],
                    "footer": "Momentum Bot",
                    "ts": int(time.time())
                }]
            }

            response = requests.post(SLACK_WEBHOOK_URL, json=payload, timeout=5)
            response.raise_for_status()
        except Exception as e:
            logger.error(f"[ALERTER] Slack send failed: {e}")

    def _send_email(self, level: str, title: str, message: str, context: dict = None):
        """Send email notification via SMTP."""
        try:
            msg = MIMEMultipart()
            msg['From'] = ALERT_EMAIL_FROM
            msg['To'] = ALERT_EMAIL_TO
            msg['Subject'] = f"[Momentum Bot {level}] {title}"

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
            logger.error(f"[ALERTER] Email send failed: {e}")


class KillSwitch:
    """Emergency halt mechanism via file or environment variable."""

    def is_triggered(self) -> Tuple[bool, Optional[str]]:
        """Check if kill switch is activated."""
        if KILL_SWITCH_ENV:
            return True, "environment variable KILL_SWITCH=1"
        if os.path.exists(KILL_SWITCH_FILE):
            return True, f"file '{KILL_SWITCH_FILE}' exists"
        return False, None

    def execute_emergency_shutdown(self, alerter: Alerter, alpaca_client):
        """Execute emergency shutdown procedures."""
        triggered, reason = self.is_triggered()
        if not triggered:
            return

        logger.error(f"[KILL_SWITCH] TRIGGERED: {reason}")
        alerter.send_alert(
            "CRITICAL",
            "Kill Switch Activated",
            f"Emergency shutdown triggered by {reason}",
            {"reason": reason, "time": iso(now_et())}
        )

        try:
            # Cancel all orders
            logger.info("[KILL_SWITCH] Cancelling all orders...")
            alpaca_client.cancel_all_orders()

            # Optionally flatten positions based on policy
            if SHUTDOWN_POLICY == "FLATTEN_ALL":
                logger.info("[KILL_SWITCH] Flattening all positions...")
                alpaca_client.close_all_positions()
        except Exception as e:
            logger.error(f"[KILL_SWITCH] Shutdown error: {e}")
            alerter.send_alert("CRITICAL", "Kill Switch Error", str(e))


# ============================================================
# ALPACA REST API
# ============================================================

class AlpacaClient:
    """Alpaca REST client."""

    def __init__(self):
        self.base_url = ALPACA_BASE_URL
        self.headers = {
            "APCA-API-KEY-ID": ALPACA_API_KEY,
            "APCA-API-SECRET-KEY": ALPACA_SECRET_KEY,
            "Content-Type": "application/json",
        }

    def _request(self, method: str, path: str, **kwargs):
        url = f"{self.base_url}{path}"
        response = requests.request(method, url, headers=self.headers, timeout=15, **kwargs)
        response.raise_for_status()
        return response.json() if response.text else {}

    def get_account(self) -> dict:
        return self._request("GET", "/v2/account")

    def get_positions(self) -> List[dict]:
        return self._request("GET", "/v2/positions")

    def get_position(self, symbol: str) -> Optional[dict]:
        try:
            return self._request("GET", f"/v2/positions/{symbol}")
        except requests.HTTPError:
            return None

    def submit_market_order(self, symbol: str, qty: int, side: str) -> dict:
        """Submit market order (SPEED is critical)."""
        payload = {
            "symbol": symbol,
            "qty": str(qty),
            "side": side,
            "type": "market",
            "time_in_force": "day",
            "extended_hours": True  # Allow pre-market execution
        }
        return self._request("POST", "/v2/orders", json=payload)

    def close_position(self, symbol: str, qty: Optional[int] = None):
        """Close position (full or partial)."""
        if qty:
            # Partial close
            pos = self.get_position(symbol)
            if not pos:
                return
            current_qty = abs(int(float(pos["qty"])))
            side = "sell" if float(pos["qty"]) > 0 else "buy"
            return self.submit_market_order(symbol, min(qty, current_qty), side)
        else:
            # Full close
            return self._request("DELETE", f"/v2/positions/{symbol}")

    def get_clock(self) -> dict:
        return self._request("GET", "/v2/clock")


alpaca = AlpacaClient()


# ============================================================
# POLYGON REST API
# ============================================================

class PolygonClient:
    """Polygon data client."""

    def __init__(self):
        self.base_url = POLYGON_REST_BASE
        self.api_key = POLYGON_API_KEY

    def _request(self, path: str, params: dict = None) -> dict:
        params = params or {}
        params["apiKey"] = self.api_key
        url = f"{self.base_url}{path}"
        response = requests.get(url, params=params, timeout=15)
        response.raise_for_status()
        return response.json()

    def get_previous_close(self, symbol: str) -> Optional[float]:
        """Get previous day's close."""
        try:
            data = self._request(f"/v2/aggs/ticker/{symbol}/prev", {"adjusted": "true"})
            results = data.get("results", [])
            if results:
                return float(results[0]["c"])
        except Exception as e:
            logger.debug(f"Failed to get previous close for {symbol}: {e}")
        return None

    def get_snapshot(self, symbol: str) -> Optional[dict]:
        """Get real-time snapshot."""
        try:
            data = self._request(f"/v2/snapshot/locale/us/markets/stocks/tickers/{symbol}")
            return data.get("ticker")
        except Exception as e:
            logger.debug(f"Failed to get snapshot for {symbol}: {e}")
        return None

    def get_recent_bars(self, symbol: str, timespan: str = "minute", limit: int = 100) -> pd.DataFrame:
        """Get recent bars."""
        try:
            end = now_et()
            start = end - dt.timedelta(hours=8)

            path = f"/v2/aggs/ticker/{symbol}/range/1/{timespan}/{start.date().isoformat()}/{end.date().isoformat()}"
            data = self._request(path, {"adjusted": "true", "sort": "desc", "limit": str(limit)})

            results = data.get("results", [])
            if not results:
                return pd.DataFrame()

            df = pd.DataFrame(results)
            df["timestamp"] = pd.to_datetime(df["t"], unit="ms", utc=True).dt.tz_convert(ET)
            df = df.rename(columns={"o": "open", "h": "high", "l": "low", "c": "close", "v": "volume"})
            return df.set_index("timestamp")[["open", "high", "low", "close", "volume"]].sort_index()
        except Exception as e:
            logger.debug(f"Failed to get bars for {symbol}: {e}")
            return pd.DataFrame()

    def get_ticker_details(self, symbol: str) -> Optional[dict]:
        """Get ticker details including sector."""
        try:
            data = self._request(f"/v3/reference/tickers/{symbol}")
            return data.get("results")
        except Exception:
            return None


polygon = PolygonClient()


# ============================================================
# MARKET SESSION
# ============================================================

class MarketSession(Enum):
    CLOSED = "CLOSED"
    PREMARKET = "PREMARKET"  # 4:00 AM - 9:30 AM ET
    MARKET_OPEN = "MARKET_OPEN"  # 9:30 AM - 10:30 AM ET (prime time)
    RTH = "RTH"  # 10:30 AM - 4:00 PM ET
    AFTERHOURS = "AFTERHOURS"

def get_market_session() -> MarketSession:
    """Determine current market session."""
    now = now_et()
    hour = now.hour
    minute = now.minute

    # Pre-market: 4:00 AM - 9:30 AM
    if (hour == 4 and minute >= 0) or (5 <= hour < 9) or (hour == 9 and minute < 30):
        return MarketSession.PREMARKET

    # Market open (prime time): 9:30 AM - 10:30 AM
    if (hour == 9 and minute >= 30) or (hour == 10 and minute < 30):
        return MarketSession.MARKET_OPEN

    # Regular hours: 10:30 AM - 4:00 PM
    if (hour == 10 and minute >= 30) or (11 <= hour < 16):
        return MarketSession.RTH

    # After hours: 4:00 PM - 8:00 PM
    if 16 <= hour < 20:
        return MarketSession.AFTERHOURS

    return MarketSession.CLOSED


# ============================================================
# CATALYST DETECTION
# ============================================================

@dataclass
class CatalystAlert:
    """Alert for potential catalyst move."""
    symbol: str
    gap_percent: float
    relative_volume: float
    prev_close: float
    current_price: float
    premarket_high: Optional[float]
    sector: str
    alert_time: str
    reason: str
    priority: str  # "HIGH", "MEDIUM", "LOW"

class CatalystScanner:
    """Scans for gap-up stocks with explosive potential."""

    def __init__(self):
        self.watchlist: Set[str] = set()
        self.alerted: Set[str] = set()  # Track alerted symbols to avoid spam
        self.last_scan_time = 0.0

    def scan_for_catalysts(self) -> List[CatalystAlert]:
        """Scan market for catalyst setups."""
        alerts = []

        # Get list of active stocks (simplified - in production use stock screener API)
        symbols_to_scan = self._get_scan_universe()

        logger.info(f"[SCAN] Scanning {len(symbols_to_scan)} symbols for catalyst setups...")

        for symbol in symbols_to_scan:
            if symbol in self.alerted:
                continue  # Already alerted

            alert = self._check_symbol(symbol)
            if alert:
                alerts.append(alert)
                self.alerted.add(symbol)
                self._log_alert(alert)

        return alerts

    def _get_scan_universe(self) -> List[str]:
        """
        Get universe of symbols to scan.
        In production, use:
        - Polygon grouped daily with filters
        - Unusual volume scanners
        - News API feeds
        - Biotech watchlists
        """
        # For now, use a curated biotech/pharma watchlist
        # TODO: Integrate with real-time scanner API
        biotech_watchlist = [
            # Major biotech
            "ABBV", "GILD", "BIIB", "VRTX", "REGN", "AMGN", "MRNA", "BNTX",
            # Mid-cap biotech (more volatile)
            "SAVA", "AVXL", "OCGN", "BNGO", "PACB", "ARKG",
            # Recent movers (update manually or via API)
            # Add symbols that had news recently
        ]
        return biotech_watchlist

    def _check_symbol(self, symbol: str) -> Optional[CatalystAlert]:
        """Check if symbol meets catalyst criteria."""
        try:
            # Get previous close
            prev_close = polygon.get_previous_close(symbol)
            if not prev_close:
                return None

            # Get current snapshot
            snapshot = polygon.get_snapshot(symbol)
            if not snapshot:
                return None

            # Get current price
            current_price = snapshot.get("day", {}).get("c")
            if not current_price:
                current_price = snapshot.get("prevDay", {}).get("c")
            if not current_price:
                return None

            current_price = float(current_price)

            # Check price range filter
            if not (MIN_PRICE_PREFILTER <= current_price <= MAX_PRICE_PREFILTER):
                return None

            # Calculate gap
            gap_pct = ((current_price - prev_close) / prev_close) * 100

            # Check minimum gap
            if gap_pct < MIN_GAP_PERCENT:
                return None

            # Get volume data
            day_volume = snapshot.get("day", {}).get("v", 0)
            prev_volume = snapshot.get("prevDay", {}).get("v", 1)

            rel_vol = day_volume / prev_volume if prev_volume > 0 else 0

            # Check relative volume
            if rel_vol < MIN_RELATIVE_VOLUME:
                return None

            # Get premarket high if available
            premarket_high = None
            bars = polygon.get_recent_bars(symbol, "minute", limit=50)
            if not bars.empty:
                # Get today's bars
                today = now_et().date()
                today_bars = bars[bars.index.date == today]
                if not today_bars.empty:
                    premarket_bars = today_bars[today_bars.index.hour < 9]
                    if not premarket_bars.empty:
                        premarket_high = float(premarket_bars["high"].max())

            # Get sector info
            details = polygon.get_ticker_details(symbol)
            sector = "Unknown"
            if details:
                sector = details.get("sic_description", "Unknown")

            # Determine priority
            priority = "LOW"
            if gap_pct >= IDEAL_GAP_PERCENT and rel_vol >= 20:
                priority = "HIGH"
            elif gap_pct >= IDEAL_GAP_PERCENT or rel_vol >= 15:
                priority = "MEDIUM"

            # Check for biotech sector (bonus priority)
            if any(bio in sector for bio in BIOTECH_SECTORS):
                if priority == "MEDIUM":
                    priority = "HIGH"
                elif priority == "LOW":
                    priority = "MEDIUM"

            # Create alert
            reason = f"Gap {gap_pct:.1f}% | Vol {rel_vol:.1f}x | Sector: {sector}"

            alert = CatalystAlert(
                symbol=symbol,
                gap_percent=gap_pct,
                relative_volume=rel_vol,
                prev_close=prev_close,
                current_price=current_price,
                premarket_high=premarket_high,
                sector=sector,
                alert_time=iso(now_et()),
                reason=reason,
                priority=priority
            )

            logger.warning(f"[CATALYST] {priority} ALERT: {symbol} | {reason}")
            return alert

        except Exception as e:
            logger.debug(f"Error checking {symbol}: {e}")
            return None

    def _log_alert(self, alert: CatalystAlert):
        """Log alert to file for review."""
        if not ENABLE_ALERTS:
            return

        try:
            with open(ALERT_LOG_PATH, "a") as f:
                f.write(json.dumps(asdict(alert)) + "\n")

            # Play sound for HIGH priority
            if alert.priority == "HIGH":
                play_alert_sound()

        except Exception as e:
            logger.debug(f"Failed to log alert: {e}")


scanner = CatalystScanner()


# ============================================================
# POSITION MANAGER
# ============================================================

@dataclass
class Position:
    symbol: str
    qty: int
    entry_price: float
    entry_time: str
    stop_loss: float
    breakout_level: float
    highest_price: float
    partials_taken: List[dict]  # List of partial exits

class PositionManager:
    """Manages explosive momentum positions."""

    def __init__(self):
        self.positions: Dict[str, Position] = {}
        self._lock = threading.Lock()

    def add_position(self, symbol: str, qty: int, entry_price: float, stop_loss: float, breakout_level: float):
        """Add new position."""
        with self._lock:
            self.positions[symbol] = Position(
                symbol=symbol,
                qty=qty,
                entry_price=entry_price,
                entry_time=iso(now_et()),
                stop_loss=stop_loss,
                breakout_level=breakout_level,
                highest_price=entry_price,
                partials_taken=[]
            )
            logger.info(f"[POSITION] Opened {symbol}: qty={qty} entry=${entry_price:.2f} stop=${stop_loss:.2f}")

    def update_highest_price(self, symbol: str, price: float):
        """Track highest price for momentum analysis."""
        with self._lock:
            pos = self.positions.get(symbol)
            if pos and price > pos.highest_price:
                pos.highest_price = price

    def record_partial(self, symbol: str, qty: int, price: float, reason: str):
        """Record partial profit taking."""
        with self._lock:
            pos = self.positions.get(symbol)
            if pos:
                partial = {
                    "qty": qty,
                    "price": price,
                    "time": iso(now_et()),
                    "reason": reason
                }
                pos.partials_taken.append(partial)
                pos.qty -= qty
                logger.info(f"[PARTIAL] {symbol}: Sold {qty} @ ${price:.2f} ({reason}) | remaining={pos.qty}")

    def remove_position(self, symbol: str):
        """Remove position."""
        with self._lock:
            if symbol in self.positions:
                del self.positions[symbol]
                logger.info(f"[POSITION] Closed {symbol}")

    def get_position(self, symbol: str) -> Optional[Position]:
        """Get position by symbol."""
        with self._lock:
            return self.positions.get(symbol)


position_manager = PositionManager()


# ============================================================
# RISK MANAGER
# ============================================================

class RiskManager:
    """Manages risk for explosive momentum trading."""

    def __init__(self):
        self.start_equity = 0.0
        self.current_equity = 0.0
        self.daily_pnl = 0.0
        self.losing_trades_today = 0
        self.halted = False

    def initialize(self):
        """Initialize daily risk tracking."""
        account = alpaca.get_account()
        self.start_equity = float(account["equity"])
        self.current_equity = self.start_equity
        self.daily_pnl = 0.0
        self.losing_trades_today = 0
        self.halted = False
        logger.info(f"[RISK] Initialized | equity=${self.start_equity:,.2f}")

    def update(self):
        """Update risk limits."""
        account = alpaca.get_account()
        self.current_equity = float(account["equity"])
        self.daily_pnl = self.current_equity - self.start_equity

        # Check daily loss limit
        if self.daily_pnl < 0:
            loss_pct = abs(self.daily_pnl / self.start_equity)
            if loss_pct >= MAX_DAILY_LOSS_PCT:
                if not self.halted:
                    self.halted = True
                    logger.error(f"[RISK] DAILY LOSS LIMIT HIT | {loss_pct:.1%} | HALTING")

        # Check losing trade limit
        if self.losing_trades_today >= MAX_LOSING_TRADES_PER_DAY:
            if not self.halted:
                self.halted = True
                logger.error(f"[RISK] MAX LOSING TRADES ({MAX_LOSING_TRADES_PER_DAY}) REACHED | HALTING")

        return self.halted

    def record_trade(self, pnl: float):
        """Record trade result."""
        if pnl < 0:
            self.losing_trades_today += 1

    def calculate_position_size(self, entry_price: float, stop_price: float) -> int:
        """Calculate position size based on risk."""
        if entry_price <= 0 or stop_price <= 0:
            return 0

        risk_per_share = abs(entry_price - stop_price)
        if risk_per_share == 0:
            return 0

        # Risk-based sizing
        risk_dollars = self.current_equity * RISK_PER_TRADE_PCT
        shares_by_risk = int(risk_dollars / risk_per_share)

        # Also limit by max position size
        max_position_dollars = self.current_equity * MAX_POSITION_SIZE_PCT
        shares_by_size = int(max_position_dollars / entry_price)

        # Take smaller of the two
        return max(0, min(shares_by_risk, shares_by_size))


risk_manager = RiskManager()


# ============================================================
# MOMENTUM ANALYZER
# ============================================================

class MomentumAnalyzer:
    """Analyzes momentum quality for entry/exit decisions."""

    def check_momentum_quality(self, symbol: str, bars: pd.DataFrame) -> dict:
        """
        Analyze momentum quality.
        Returns dict with quality indicators.
        """
        if bars.empty or len(bars) < 5:
            return {"quality": "UNKNOWN", "signals": []}

        signals = []
        recent = bars.tail(5)

        # Volume analysis
        avg_vol = recent["volume"].mean()
        last_vol = recent["volume"].iloc[-1]
        vol_ratio = last_vol / avg_vol if avg_vol > 0 else 0

        if vol_ratio > VOLUME_CLIMAX_THRESHOLD:
            signals.append(f"VOLUME_CLIMAX:{vol_ratio:.1f}x")

        # Wick analysis (upper wick signals rejection)
        last_bar = recent.iloc[-1]
        body = abs(last_bar["close"] - last_bar["open"])
        upper_wick = last_bar["high"] - max(last_bar["close"], last_bar["open"])

        if body > 0:
            wick_ratio = upper_wick / body
            if wick_ratio > WICK_WARNING_PCT:
                signals.append(f"UPPER_WICK_WARNING:{wick_ratio:.1%}")

        # Price momentum (are we still trending?)
        closes = recent["close"]
        rising = sum(1 for i in range(1, len(closes)) if closes.iloc[i] > closes.iloc[i-1])

        if rising >= 4:
            quality = "STRONG"
        elif rising >= 3:
            quality = "GOOD"
        elif rising >= 2:
            quality = "WEAK"
        else:
            quality = "FAILING"
            signals.append("MOMENTUM_BREAK")

        return {
            "quality": quality,
            "signals": signals,
            "volume_ratio": vol_ratio,
            "rising_bars": rising
        }


momentum_analyzer = MomentumAnalyzer()


# ============================================================
# TRADING ENGINE
# ============================================================

class CatalystTradingBot:
    """Main bot for catalyst momentum trading."""

    def __init__(self):
        self.running = False
        self.mode = "SCAN_ONLY"  # "SCAN_ONLY" or "AUTO_TRADE"
        self.last_scan_time = 0.0

    def run(self):
        """Main bot loop."""
        logger.info("="*70)
        logger.info("CATALYST MOMENTUM BOT - EXPLOSIVE MOVE CAPTURE")
        logger.info("="*70)
        logger.info(f"Mode: {self.mode}")
        logger.info(f"Min Gap: {MIN_GAP_PERCENT}%")
        logger.info(f"Min RelVol: {MIN_RELATIVE_VOLUME}x")
        logger.info("="*70)

        risk_manager.initialize()
        self.running = True

        # Signal handler
        def signal_handler(sig, frame):
            logger.info("[SHUTDOWN] Stopping bot...")
            self.running = False

        signal.signal(signal.SIGINT, signal_handler)

        while self.running:
            try:
                session = get_market_session()

                # Only scan during active sessions
                if session in [MarketSession.PREMARKET, MarketSession.MARKET_OPEN]:

                    # Update risk
                    if risk_manager.update():
                        logger.warning("[STATUS] Trading halted")
                        time.sleep(60)
                        continue

                    # Manage existing positions
                    if position_manager.positions:
                        self.manage_positions()

                    # Scan for new setups
                    if time.time() - self.last_scan_time > SCAN_INTERVAL_SECONDS:
                        alerts = scanner.scan_for_catalysts()

                        if alerts:
                            logger.warning(f"[ALERTS] Found {len(alerts)} catalyst setups!")
                            for alert in alerts:
                                self.handle_alert(alert)

                        self.last_scan_time = time.time()

                    time.sleep(2)

                else:
                    logger.info(f"[STATUS] Session={session.value} - waiting for active hours...")
                    time.sleep(30)

            except Exception as e:
                logger.error(f"[ERROR] Main loop error: {e}", exc_info=True)
                time.sleep(10)

        logger.info("[SHUTDOWN] Bot stopped")

    def handle_alert(self, alert: CatalystAlert):
        """Handle catalyst alert (currently just logs - can add auto-trade logic)."""
        logger.warning("="*70)
        logger.warning(f"[{alert.priority}] CATALYST DETECTED: {alert.symbol}")
        logger.warning(f"Gap: {alert.gap_percent:.1f}% | RelVol: {alert.relative_volume:.1f}x")
        logger.warning(f"Price: ${alert.current_price:.2f} | Prev Close: ${alert.prev_close:.2f}")
        if alert.premarket_high:
            logger.warning(f"Premarket High: ${alert.premarket_high:.2f}")
        logger.warning(f"Sector: {alert.sector}")
        logger.warning("="*70)

        # In AUTO_TRADE mode, could automatically enter here
        # For safety, keeping as SCAN_ONLY by default
        if self.mode == "AUTO_TRADE" and alert.priority == "HIGH":
            self.attempt_entry(alert)

    def attempt_entry(self, alert: CatalystAlert):
        """Attempt to enter position (aggressive breakout entry)."""
        try:
            symbol = alert.symbol

            # Check if we can take more positions
            if len(position_manager.positions) >= MAX_CONCURRENT_POSITIONS:
                logger.info(f"[ENTRY] {symbol}: Max positions reached - skipping")
                return

            # Get recent bars for breakout detection
            bars = polygon.get_recent_bars(symbol, "minute", limit=20)
            if bars.empty:
                logger.warning(f"[ENTRY] {symbol}: No bar data - skipping")
                return

            # Determine breakout level
            if alert.premarket_high:
                breakout_level = alert.premarket_high
            else:
                # Use recent high
                breakout_level = bars["high"].tail(10).max()

            # Current price should be near or above breakout
            current_price = alert.current_price

            # Entry with buffer (confirm breakout)
            entry_trigger = breakout_level * (1 + BREAKOUT_BUFFER_PCT)

            if current_price < entry_trigger:
                logger.info(f"[ENTRY] {symbol}: Price ${current_price:.2f} below trigger ${entry_trigger:.2f} - waiting")
                return

            # Calculate stop (tight - below breakout structure)
            stop_price = breakout_level * (1 - INITIAL_STOP_PCT)

            # Calculate position size
            qty = risk_manager.calculate_position_size(current_price, stop_price)
            if qty <= 0:
                logger.warning(f"[ENTRY] {symbol}: Position size is 0 - skipping")
                return

            # EXECUTE MARKET ORDER (SPEED > PRECISION)
            logger.warning(f"[ENTRY] {symbol}: EXECUTING BREAKOUT ENTRY | qty={qty} @ MARKET")

            order = alpaca.submit_market_order(symbol, qty, "buy")

            # Add to position manager
            position_manager.add_position(
                symbol=symbol,
                qty=qty,
                entry_price=current_price,
                stop_loss=stop_price,
                breakout_level=breakout_level
            )

            logger.warning(f"[ENTRY] {symbol}: ORDER FILLED | qty={qty} entry~${current_price:.2f} stop=${stop_price:.2f}")

        except Exception as e:
            logger.error(f"[ENTRY] {symbol}: Failed to enter: {e}")

    def manage_positions(self):
        """Manage open positions - aggressive profit taking and stops."""
        logger.info(f"[MANAGE] Checking {len(position_manager.positions)} position(s)...")

        for pos in list(position_manager.positions.values()):
            try:
                symbol = pos.symbol

                # Get current price
                snapshot = polygon.get_snapshot(symbol)
                if not snapshot:
                    logger.warning(f"[MANAGE] {symbol}: No snapshot data - skipping")
                    continue

                current_price = float(snapshot.get("day", {}).get("c", pos.entry_price))

                # Update highest price
                position_manager.update_highest_price(symbol, current_price)

                # Calculate gain
                gain_pct = (current_price - pos.entry_price) / pos.entry_price

                # Log position status
                logger.info(f"[MANAGE] {symbol}: price=${current_price:.2f} entry=${pos.entry_price:.2f} "
                           f"gain={gain_pct:+.2%} stop=${pos.stop_loss:.2f} highest=${pos.highest_price:.2f}")

                # Check stop loss
                if current_price <= pos.stop_loss:
                    logger.warning(f"[EXIT] {symbol}: STOP HIT @ ${current_price:.2f}")
                    alpaca.close_position(symbol)
                    position_manager.remove_position(symbol)
                    continue

                # Move stop to breakeven at +10%
                if gain_pct >= BREAKEVEN_MOVE_TRIGGER and pos.stop_loss < pos.entry_price:
                    pos.stop_loss = pos.entry_price
                    logger.info(f"[STOP] {symbol}: Moved to BREAKEVEN @ ${pos.entry_price:.2f}")

                # First partial (25% at +15%)
                if len(pos.partials_taken) == 0 and gain_pct >= FIRST_SCALE_GAIN_PCT:
                    partial_qty = int(pos.qty * FIRST_SCALE_PERCENT)
                    if partial_qty > 0:
                        alpaca.close_position(symbol, partial_qty)
                        position_manager.record_partial(symbol, partial_qty, current_price, "First scale +15%")

                # Second partial (25% at +30%)
                elif len(pos.partials_taken) == 1 and gain_pct >= SECOND_SCALE_GAIN_PCT:
                    partial_qty = int(pos.qty * SECOND_SCALE_PERCENT)
                    if partial_qty > 0:
                        alpaca.close_position(symbol, partial_qty)
                        position_manager.record_partial(symbol, partial_qty, current_price, "Second scale +30%")

                # Check momentum quality
                bars = polygon.get_recent_bars(symbol, "minute", limit=10)
                if not bars.empty:
                    momentum = momentum_analyzer.check_momentum_quality(symbol, bars)
                    logger.info(f"[MOMENTUM] {symbol}: quality={momentum['quality']} signals={momentum['signals']}")

                    # Exit on momentum break
                    if MOMENTUM_BREAK_EXIT and momentum["quality"] == "FAILING":
                        logger.warning(f"[EXIT] {symbol}: MOMENTUM BREAK - closing position")
                        alpaca.close_position(symbol)
                        position_manager.remove_position(symbol)
                        continue

                    # Exit on volume climax
                    if "VOLUME_CLIMAX" in str(momentum["signals"]):
                        logger.warning(f"[EXIT] {symbol}: VOLUME CLIMAX - closing remaining position")
                        alpaca.close_position(symbol)
                        position_manager.remove_position(symbol)
                        continue
                else:
                    logger.debug(f"[MOMENTUM] {symbol}: No bar data available")

            except Exception as e:
                logger.error(f"[MANAGE] {symbol}: Error: {e}")


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    bot = CatalystTradingBot()

    # Set mode here
    # bot.mode = "AUTO_TRADE"  # Uncomment for auto-trading (HIGH RISK)
    bot.mode = "SCAN_ONLY"     # Safe mode - alerts only

    bot.run()
