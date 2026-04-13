#!/usr/bin/env python3
"""
Master Trading Bot - Unified Multi-Strategy System
Combines best elements from all analyzed trading bots:
- Momentum Breakout (40% capital)
- VWAP Scalping (40% capital)
- Trend Following (20% capital)

Author: Generated from comprehensive bot analysis
Date: 2025-12-31
"""

import os
import sys
import json
import time
import yaml
import logging
import argparse
import requests
import threading
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple, Any, Set
from dataclasses import dataclass, asdict, field
from enum import Enum
from pathlib import Path
import traceback

# ============================================================================
# CONFIGURATION & CONSTANTS
# ============================================================================

class StrategyType(Enum):
    """Trading strategy types"""
    MOMENTUM_BREAKOUT = "momentum_breakout"
    VWAP_SCALPING = "vwap_scalping"
    TREND_FOLLOWING = "trend_following"

class PositionState(Enum):
    """Position lifecycle states"""
    IDLE = "idle"
    ENTRY_SUBMITTED = "entry_submitted"
    HOLDING = "holding"
    EXIT_SUBMITTED = "exit_submitted"
    CLOSED = "closed"

class MarketSession(Enum):
    """Market session types"""
    CLOSED = "closed"
    PRE = "pre_market"
    RTH = "regular_hours"
    AFTER = "after_hours"

class MarketRegime(Enum):
    """Market regime types for adaptive strategy selection"""
    TREND = "trend"              # Directional, strong follow-through
    RANGE_CHOP = "range_chop"    # Oscillating/choppy, no clear direction
    UNKNOWN = "unknown"          # Unable to determine or error state

@dataclass
class VirtualBracket:
    """Virtual bracket for extended hours positions (broker doesn't support bracket orders)"""
    symbol: str
    side: str  # "buy" or "sell"
    quantity: int
    entry_price: float
    stop_loss: float
    take_profit: float
    entry_time: datetime
    session: MarketSession
    active: bool = True
    moved_breakeven: bool = False

@dataclass
class RegimeMetrics:
    """Market regime detection metrics (for logging and analysis)"""
    regime: MarketRegime
    adx: float
    vwap_slope: float
    vwap_crossovers: int
    timestamp: datetime

@dataclass
class Position:
    """Active position tracking"""
    symbol: str
    strategy: StrategyType
    entry_time: datetime
    entry_price: float
    quantity: int
    stop_loss: float
    take_profit: float
    state: PositionState
    order_id: Optional[str] = None
    bracket_ids: Dict[str, str] = field(default_factory=dict)
    max_price: float = 0.0
    trail_stop: Optional[float] = None
    session: MarketSession = MarketSession.RTH  # Track which session position was opened in
    regime: MarketRegime = MarketRegime.UNKNOWN  # Regime when position was opened

@dataclass
class TradingState:
    """Persistent bot state"""
    positions: Dict[str, Position]
    daily_pnl: float
    daily_trades: int
    total_trades: int
    consecutive_losses: int
    last_reset_date: str
    circuit_breaker_active: bool
    cooldown_until: Optional[datetime]
    api_failures: int
    virtual_brackets: Dict[str, VirtualBracket] = field(default_factory=dict)  # Extended hours brackets
    current_regime: MarketRegime = MarketRegime.UNKNOWN  # Current market regime
    last_regime_check: Optional[datetime] = None  # Last time regime was updated

@dataclass
class RiskMetrics:
    """Real-time risk tracking"""
    total_exposure: float
    position_count: int
    daily_loss: float
    max_daily_loss_hit: bool
    drawdown_pct: float

# ============================================================================
# STATE MANAGEMENT
# ============================================================================

def _get_trading_day_str() -> str:
    """Get current trading day as YYYY-MM-DD string in ET timezone"""
    try:
        from zoneinfo import ZoneInfo
        return datetime.now(ZoneInfo("America/New_York")).strftime("%Y-%m-%d")
    except ImportError:
        import pytz
        return datetime.now(pytz.timezone("America/New_York")).strftime("%Y-%m-%d")

class StateManager:
    """Thread-safe atomic state persistence with crash recovery"""

    def __init__(self, state_file: str):
        self.state_file = Path(state_file)
        self.lock = threading.RLock()  # FIXED: Use RLock to prevent deadlock on recursive calls
        self.state = self._load_state()

    def _load_state(self) -> TradingState:
        """Load state from disk with automatic recovery"""
        if not self.state_file.exists():
            return self._create_default_state()

        try:
            with open(self.state_file, 'r') as f:
                data = json.load(f)

            # Deserialize positions
            positions = {}
            for sym, pos_data in data.get('positions', {}).items():
                # Convert string enums back to Enum types
                pos_data['strategy'] = StrategyType(pos_data['strategy'])
                pos_data['state'] = PositionState(pos_data['state'])
                # FIXED: Restore MarketSession enum
                pos_data['session'] = MarketSession(pos_data.get('session', 'regular_hours'))
                # Restore MarketRegime enum (default to UNKNOWN for old state files)
                pos_data['regime'] = MarketRegime(pos_data.get('regime', 'unknown'))
                # Convert ISO datetime back to datetime object
                pos_data['entry_time'] = datetime.fromisoformat(pos_data['entry_time'])
                if pos_data.get('cooldown_until'):
                    pos_data['cooldown_until'] = datetime.fromisoformat(pos_data['cooldown_until'])
                positions[sym] = Position(**pos_data)

            # FIXED: Deserialize virtual brackets
            virtual_brackets = {}
            for sym, bracket_data in data.get('virtual_brackets', {}).items():
                # Convert string enum back to MarketSession
                bracket_data['session'] = MarketSession(bracket_data['session'])
                # Convert ISO datetime back to datetime object
                bracket_data['entry_time'] = datetime.fromisoformat(bracket_data['entry_time'])
                virtual_brackets[sym] = VirtualBracket(**bracket_data)

            state = TradingState(
                positions=positions,
                virtual_brackets=virtual_brackets,  # FIXED: Now restored
                daily_pnl=data.get('daily_pnl', 0.0),
                daily_trades=data.get('daily_trades', 0),
                total_trades=data.get('total_trades', 0),
                consecutive_losses=data.get('consecutive_losses', 0),
                last_reset_date=data.get('last_reset_date', datetime.now().strftime('%Y-%m-%d')),
                circuit_breaker_active=data.get('circuit_breaker_active', False),
                cooldown_until=datetime.fromisoformat(data['cooldown_until']) if data.get('cooldown_until') else None,
                api_failures=data.get('api_failures', 0),
                current_regime=MarketRegime(data.get('current_regime', 'unknown')),
                last_regime_check=datetime.fromisoformat(data['last_regime_check']) if data.get('last_regime_check') else None
            )

            logging.info(f"[STATE] Loaded state: {len(positions)} positions, {len(virtual_brackets)} virtual brackets, Daily P&L: ${state.daily_pnl:.2f}")
            return state

        except Exception as e:
            logging.error(f"[STATE] Error loading state: {e}, using defaults")
            import traceback
            logging.error(traceback.format_exc())
            return self._create_default_state()

    def _create_default_state(self) -> TradingState:
        """Create fresh state"""
        return TradingState(
            positions={},
            daily_pnl=0.0,
            daily_trades=0,
            total_trades=0,
            consecutive_losses=0,
            last_reset_date=_get_trading_day_str(),
            circuit_breaker_active=False,
            cooldown_until=None,
            api_failures=0
        )

    def save_state(self) -> bool:
        """
        Atomic state persistence with internal locking (thread-safe).
        FIXED: Now locks internally using RLock, so safe to call from anywhere.
        """
        with self.lock:
            try:
                # Serialize to temporary file first
                temp_file = self.state_file.with_suffix('.tmp')

                # FIXED: Properly serialize positions with Enum types
                serialized_positions = {}
                for sym, pos in self.state.positions.items():
                    pos_dict = asdict(pos)
                    # Convert Enums to values
                    pos_dict['strategy'] = pos.strategy.value
                    pos_dict['state'] = pos.state.value
                    pos_dict['session'] = pos.session.value  # FIXED: MarketSession enum
                    pos_dict['regime'] = pos.regime.value  # Serialize MarketRegime enum
                    # Convert datetime to ISO format
                    pos_dict['entry_time'] = pos.entry_time.isoformat()
                    serialized_positions[sym] = pos_dict

                # FIXED: Serialize virtual brackets
                serialized_brackets = {}
                for sym, bracket in self.state.virtual_brackets.items():
                    bracket_dict = asdict(bracket)
                    # Convert Enum and datetime
                    bracket_dict['session'] = bracket.session.value
                    bracket_dict['entry_time'] = bracket.entry_time.isoformat()
                    serialized_brackets[sym] = bracket_dict

                data = {
                    'positions': serialized_positions,
                    'virtual_brackets': serialized_brackets,  # FIXED: Now persisted
                    'daily_pnl': self.state.daily_pnl,
                    'daily_trades': self.state.daily_trades,
                    'total_trades': self.state.total_trades,
                    'consecutive_losses': self.state.consecutive_losses,
                    'last_reset_date': self.state.last_reset_date,
                    'circuit_breaker_active': self.state.circuit_breaker_active,
                    'cooldown_until': self.state.cooldown_until.isoformat() if self.state.cooldown_until else None,
                    'api_failures': self.state.api_failures,
                    'current_regime': self.state.current_regime.value,
                    'last_regime_check': self.state.last_regime_check.isoformat() if self.state.last_regime_check else None
                }

                with open(temp_file, 'w') as f:
                    json.dump(data, f, indent=2)

                # Atomic rename
                temp_file.replace(self.state_file)
                return True

            except Exception as e:
                logging.error(f"[STATE] Save failed: {e}")
                import traceback
                logging.error(traceback.format_exc())
                return False

    def reset_daily_metrics(self):
        """Reset daily counters at market open (thread-safe with RLock)"""
        with self.lock:
            today = _get_trading_day_str()  # Get trading day in ET timezone
            if self.state.last_reset_date != today:
                logging.info(f"[STATE] Daily reset: Previous P&L ${self.state.daily_pnl:.2f}, {self.state.daily_trades} trades")
                self.state.daily_pnl = 0.0
                self.state.daily_trades = 0
                self.state.last_reset_date = today
                # save_state() also uses self.lock, but RLock allows reentrant locking
                self.save_state()

# ============================================================================
# ALPACA API CLIENT
# ============================================================================

class AlpacaClient:
    """Alpaca REST API with automatic retries and error handling"""

    def __init__(self, api_key: str, api_secret: str, paper: bool = True):
        self.api_key = api_key
        self.api_secret = api_secret
        self.base_url = "https://paper-api.alpaca.markets" if paper else "https://api.alpaca.markets"
        self.data_url = "https://data.alpaca.markets"
        self.headers = {
            "APCA-API-KEY-ID": api_key,
            "APCA-API-SECRET-KEY": api_secret
        }
        self.session = requests.Session()
        self.session.headers.update(self.headers)

    def _request(self, method: str, url: str, **kwargs) -> Optional[Dict]:
        """Make API request with retry logic"""
        max_retries = 3

        # Set default timeout if not provided (CRITICAL: prevents infinite hangs)
        if 'timeout' not in kwargs:
            kwargs['timeout'] = 30  # 30 second timeout

        for attempt in range(max_retries):
            try:
                resp = self.session.request(method, url, **kwargs)

                if resp.status_code == 429:
                    retry_after = int(resp.headers.get('Retry-After', 60))
                    logging.warning(f"[API] Rate limited, waiting {retry_after}s")
                    time.sleep(retry_after)
                    continue

                resp.raise_for_status()
                return resp.json() if resp.text else {}

            except Exception as e:
                logging.error(f"[API] Request failed (attempt {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                else:
                    return None

        return None

    def get_account(self) -> Optional[Dict]:
        """Get account information"""
        return self._request("GET", f"{self.base_url}/v2/account")

    def get_position(self, symbol: str) -> Optional[Dict]:
        """Get position for symbol"""
        return self._request("GET", f"{self.base_url}/v2/positions/{symbol}")

    def get_positions(self) -> List[Dict]:
        """Get all open positions"""
        result = self._request("GET", f"{self.base_url}/v2/positions")
        return result if result else []

    def get_orders(
        self,
        status: str = "open",
        limit: int = 100,
        after: Optional[str] = None,
        until: Optional[str] = None,
        direction: str = "desc"
    ) -> List[Dict]:
        """Get orders by status with optional filters"""
        params = {"status": status, "limit": limit, "direction": direction}
        if after:
            params["after"] = after
        if until:
            params["until"] = until

        result = self._request("GET", f"{self.base_url}/v2/orders", params=params)
        return result if result else []

    def submit_order(self, symbol: str, qty: int, side: str, order_type: str = "market",
                     time_in_force: str = "day", **kwargs) -> Optional[Dict]:
        """Submit order to Alpaca"""
        payload = {
            "symbol": symbol,
            "qty": qty,
            "side": side,
            "type": order_type,
            "time_in_force": time_in_force,
            **kwargs
        }
        return self._request("POST", f"{self.base_url}/v2/orders", json=payload)

    def submit_bracket_order(self, symbol: str, qty: int, side: str,
                            stop_loss: float, take_profit: float,
                            session: 'MarketSession' = None) -> Optional[Dict]:
        """
        Submit bracket order (entry + stop + target).
        For RTH: Uses broker bracket orders
        For extended hours: Uses limit order only (virtual brackets managed separately)
        """
        # Import MarketSession enum for comparison
        from enum import Enum

        # RTH: Use bracket orders with limit entry for better tracking
        if session is None or (hasattr(session, 'value') and session.value == 'regular_hours'):
            # Get aggressive limit price for fast fill
            quote = self.get_latest_quote(symbol)
            if quote and quote.get('ap', 0) > 0 and quote.get('bp', 0) > 0:
                if side == 'buy':
                    limit_price = quote['ap'] * 1.002  # 0.2% above ask
                else:
                    limit_price = quote['bp'] * 0.998  # 0.2% below bid
                limit_price = round(limit_price, 2)
            else:
                # Fallback to midpoint of stop/target
                limit_price = round((stop_loss + take_profit) / 2, 2)

            payload = {
                "symbol": symbol,
                "qty": qty,
                "side": side,
                "type": "limit",
                "limit_price": f"{limit_price:.2f}",
                "time_in_force": "day",
                "order_class": "bracket",
                "stop_loss": {"stop_price": f"{round(stop_loss, 2):.2f}"},
                "take_profit": {"limit_price": f"{round(take_profit, 2):.2f}"}
            }

        # Extended hours: Limit order only (no brackets)
        else:
            # Get aggressive limit price
            quote = self.get_latest_quote(symbol)
            if quote and quote.get('ap', 0) > 0 and quote.get('bp', 0) > 0:
                if side == 'buy':
                    limit_price = quote['ap'] * 1.002  # 0.2% above ask
                else:
                    limit_price = quote['bp'] * 0.998  # 0.2% below bid
                limit_price = round(limit_price, 2)
            else:
                # Fallback: use midpoint
                limit_price = round((stop_loss + take_profit) / 2, 2)

            payload = {
                "symbol": symbol,
                "qty": qty,
                "side": side,
                "type": "limit",
                "limit_price": f"{limit_price:.2f}",
                "time_in_force": "day",
                "extended_hours": True  # Critical for extended hours
            }

        return self._request("POST", f"{self.base_url}/v2/orders", json=payload)

    def cancel_order(self, order_id: str) -> bool:
        """Cancel order"""
        result = self._request("DELETE", f"{self.base_url}/v2/orders/{order_id}")
        return result is not None

    def close_position(self, symbol: str) -> Optional[Dict]:
        """Close position at market"""
        return self._request("DELETE", f"{self.base_url}/v2/positions/{symbol}")

    def get_bars(self, symbol: str, timeframe: str, start: str, end: str = None, limit: int = None) -> Optional[Dict]:
        """Get historical bars"""
        params = {"start": start, "timeframe": timeframe}
        if end:
            params["end"] = end
        if limit:
            params["limit"] = limit
        return self._request("GET", f"{self.data_url}/v2/stocks/{symbol}/bars", params=params)

    def get_latest_quote(self, symbol: str) -> Optional[Dict]:
        """Get latest quote"""
        result = self._request("GET", f"{self.data_url}/v2/stocks/{symbol}/quotes/latest")
        return result.get('quote') if result else None

    def get_latest_trade(self, symbol: str) -> Optional[Dict]:
        """Get latest trade"""
        result = self._request("GET", f"{self.data_url}/v2/stocks/{symbol}/trades/latest")
        return result.get('trade') if result else None

    def get_clock(self) -> Optional[Dict]:
        """Get market clock (open/closed status)"""
        return self._request("GET", f"{self.base_url}/v2/clock")

    def get_order(self, order_id: str) -> Optional[Dict]:
        """Get order by ID"""
        return self._request("GET", f"{self.base_url}/v2/orders/{order_id}")

# ============================================================================
# RISK MANAGEMENT
# ============================================================================

class RiskManager:
    """Multi-layered risk management system"""

    def __init__(self, config: Dict):
        self.max_daily_loss_pct = config.get('max_daily_loss_pct', 0.05)
        self.max_position_risk_pct = config.get('max_position_risk_pct', 0.02)
        self.max_concurrent_positions = config.get('max_concurrent_positions', 8)
        self.circuit_breaker_drawdown = config.get('circuit_breaker_drawdown', 0.10)
        self.max_consecutive_losses = config.get('max_consecutive_losses', 5)

    def check_daily_loss_limit(self, account_value: float, daily_pnl: float) -> bool:
        """Check if daily loss limit exceeded"""
        max_loss = account_value * self.max_daily_loss_pct
        if daily_pnl <= -max_loss:
            logging.error(f"[RISK] DAILY LOSS LIMIT HIT: ${daily_pnl:.2f} <= -${max_loss:.2f}")
            return True
        return False

    def check_circuit_breaker(self, account_value: float, daily_pnl: float) -> bool:
        """
        Check if circuit breaker should trigger.
        FIXED: Only triggers on losses, not profits (was using abs() which triggered on big wins too).
        """
        # Only consider losses for drawdown calculation
        loss_amount = max(0, -daily_pnl)  # Positive number representing loss
        drawdown_pct = loss_amount / account_value if account_value > 0 else 0.0

        if drawdown_pct >= self.circuit_breaker_drawdown:
            logging.error(f"[RISK] CIRCUIT BREAKER TRIGGERED: {drawdown_pct*100:.1f}% drawdown (${-daily_pnl:.2f} loss)")
            return True
        return False

    def calculate_position_size(self, account_value: float, entry_price: float,
                               stop_price: float, quality_score: float = 1.0,
                               buying_power: float = None) -> int:
        """Calculate position size using adaptive risk with buying power validation"""
        # Base risk: 0.5x-1.5x of max based on setup quality
        base_risk_pct = self.max_position_risk_pct
        adjusted_risk_pct = base_risk_pct * (0.5 + quality_score)

        # FIXED: Cap at max_position_risk_pct to prevent oversizing on high-quality setups
        # With quality_score > 1.0, adjusted could exceed max - prevent that
        adjusted_risk_pct = min(adjusted_risk_pct, self.max_position_risk_pct)

        risk_amount = account_value * adjusted_risk_pct
        price_risk = abs(entry_price - stop_price)

        if price_risk == 0:
            return 0

        qty = int(risk_amount / price_risk)
        qty = max(1, qty)

        # FIXED: Validate against buying power
        if buying_power is not None:
            position_cost = qty * entry_price

            # Ensure we don't exceed buying power
            if position_cost > buying_power:
                # Reduce to 95% of buying power to leave buffer
                max_affordable_qty = int((buying_power * 0.95) / entry_price)
                qty = max(1, max_affordable_qty)
                logging.warning(f"[RISK] Position size reduced from risk-based to buying power limit: {qty} shares")

        return qty

    def can_open_position(self, current_positions: int, daily_trades: int,
                         consecutive_losses: int) -> Tuple[bool, str]:
        """Check if new position allowed"""
        if current_positions >= self.max_concurrent_positions:
            return False, f"Max concurrent positions reached ({self.max_concurrent_positions})"

        if consecutive_losses >= self.max_consecutive_losses:
            return False, f"Max consecutive losses reached ({self.max_consecutive_losses})"

        return True, "OK"

    def get_risk_metrics(self, positions: Dict[str, Position], account_value: float,
                        daily_pnl: float) -> RiskMetrics:
        """Calculate current risk exposure"""
        total_exposure = sum(
            pos.entry_price * pos.quantity
            for pos in positions.values()
            if pos.state == PositionState.HOLDING
        )

        # FIXED: Only consider losses for drawdown calculation (not profits)
        loss_amount = max(0, -daily_pnl)
        drawdown_pct = loss_amount / account_value if account_value > 0 else 0.0

        return RiskMetrics(
            total_exposure=total_exposure,
            position_count=len([p for p in positions.values() if p.state == PositionState.HOLDING]),
            daily_loss=daily_pnl,
            max_daily_loss_hit=self.check_daily_loss_limit(account_value, daily_pnl),
            drawdown_pct=drawdown_pct
        )

# ============================================================================
# ALERTING SYSTEM
# ============================================================================

class AlertManager:
    """Multi-channel alerting (Slack, Email, Sound)"""

    def __init__(self, config: Dict):
        self.slack_webhook = config.get('slack_webhook')
        self.email_config = config.get('email', {})
        self.sound_enabled = config.get('sound_enabled', False)

    def send_alert(self, level: str, title: str, message: str):
        """Send alert through all configured channels"""
        full_message = f"[{level.upper()}] {title}\n{message}"
        logging.log(getattr(logging, level.upper(), logging.INFO), full_message)

        # Slack
        if self.slack_webhook:
            self._send_slack(level, title, message)

        # Email (critical only)
        if level in ['critical', 'error'] and self.email_config.get('enabled'):
            self._send_email(title, message)

        # Sound (critical only)
        if level == 'critical' and self.sound_enabled:
            self._play_sound()

    def _send_slack(self, level: str, title: str, message: str):
        """Send Slack notification with delivery verification"""
        try:
            emoji_map = {
                'critical': ':rotating_light:',
                'error': ':x:',
                'warning': ':warning:',
                'info': ':information_source:',
                'success': ':white_check_mark:'
            }

            payload = {
                "text": f"{emoji_map.get(level, ':bell:')} *{title}*\n{message}"
            }
            # FIXED: Check response status to verify delivery
            response = requests.post(self.slack_webhook, json=payload, timeout=5)
            if response.status_code != 200:
                logging.error(f"[ALERT] Slack delivery failed: HTTP {response.status_code} - {response.text}")
            elif response.text != 'ok':
                logging.warning(f"[ALERT] Slack unexpected response: {response.text}")
        except Exception as e:
            logging.error(f"[ALERT] Slack failed: {e}")

    def _send_email(self, subject: str, body: str):
        """
        Send email alert via SMTP.
        FIXED: Added warning that email is not implemented to prevent false sense of security.
        """
        # CRITICAL: Email is NOT implemented! If you enabled email alerts in config,
        # they will NOT be sent. Disable email.enabled in config or implement SMTP below.
        logging.error("[ALERT] Email alert requested but NOT IMPLEMENTED! "
                     f"Subject: {subject} | Body: {body[:100]}...")
        logging.error("[ALERT] To fix: Either disable email.enabled in config or implement SMTP")

        # TODO: Implement SMTP email sending
        # Example implementation:
        # import smtplib
        # from email.mime.text import MIMEText
        # msg = MIMEText(body)
        # msg['Subject'] = subject
        # msg['From'] = self.email_config['from_email']
        # msg['To'] = self.email_config['to_email']
        # with smtplib.SMTP(self.email_config['smtp_server'], self.email_config['smtp_port']) as server:
        #     server.starttls()
        #     server.login(self.email_config['from_email'], self.email_config['password'])
        #     server.send_message(msg)

    def _play_sound(self):
        """Play alert sound"""
        # TODO: Implement sound alert
        pass

# ============================================================================
# POLYGON API CLIENT (NEWS & MARKET DATA)
# ============================================================================

class PolygonClient:
    """Polygon API for news and supplemental market data"""

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api.polygon.io"
        self.session = requests.Session()

    def get_news(self, symbol: str, limit: int = 20) -> Optional[Dict]:
        """Get news for symbol (with 3x limit for post-filtering)"""
        try:
            resp = self.session.get(
                f"{self.base_url}/v2/reference/news",
                params={
                    "ticker": symbol,
                    "limit": limit * 3,
                    "order": "desc",
                    "sort": "published_utc",
                    "apiKey": self.api_key
                },
                timeout=10
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logging.error(f"[POLYGON] News fetch failed for {symbol}: {e}")
            return None

    def get_ticker_details(self, symbol: str) -> Optional[Dict]:
        """Get ticker details including float"""
        try:
            resp = self.session.get(
                f"{self.base_url}/v3/reference/tickers/{symbol}",
                params={"apiKey": self.api_key},
                timeout=10
            )
            resp.raise_for_status()
            return resp.json().get('results')
        except Exception as e:
            logging.error(f"[POLYGON] Ticker details failed for {symbol}: {e}")
            return None

# ============================================================================
# MOMENTUM BREAKOUT STRATEGY
# ============================================================================

class MomentumBreakoutStrategy:
    """
    Explosive gap-up momentum with multi-timeframe confirmation
    Entry: Gap > 3%, clean candle, multi-TF confirmation
    Exit: Trailing stop (2x ATR) or profit target (2R)
    """

    def __init__(self, config: Dict, alpaca: AlpacaClient, polygon: PolygonClient):
        self.config = config
        self.alpaca = alpaca
        self.polygon = polygon

        # Strategy parameters
        self.min_gap_pct = config.get('min_gap_pct', 3.0)
        self.min_volume_ratio = config.get('min_volume_ratio', 2.0)
        self.max_float = config.get('max_float', 50_000_000)
        self.min_price = config.get('min_price', 3.0)
        self.max_price = config.get('max_price', 100.0)
        self.atr_stop_multiplier = config.get('atr_stop_multiplier', 2.0)
        self.profit_target_r = config.get('profit_target_r', 2.0)

    def scan_for_signals(self, universe: List[str]) -> List[Dict]:
        """Scan universe for momentum breakout setups"""
        signals = []

        for symbol in universe:
            try:
                # Get premarket gap
                gap_pct = self._calculate_gap(symbol)
                if gap_pct is None or gap_pct < self.min_gap_pct:
                    continue

                # Get current price and volume
                quote = self.alpaca.get_latest_quote(symbol)
                if not quote:
                    continue

                price = (quote['ap'] + quote['bp']) / 2
                if price < self.min_price or price > self.max_price:
                    continue

                # Check volume ratio
                volume_ratio = self._get_volume_ratio(symbol)
                if volume_ratio is None or volume_ratio < self.min_volume_ratio:
                    continue

                # Check float (if available)
                if not self._check_float(symbol):
                    continue

                # Multi-timeframe confirmation
                if not self._check_multi_tf_confirmation(symbol):
                    continue

                # Check for clean candle structure
                if not self._check_clean_candle(symbol):
                    continue

                # Calculate ATR for stop placement
                atr = self._calculate_atr(symbol, period=14)
                if atr is None:
                    continue

                # Calculate quality score (0.5 - 1.5)
                quality = self._calculate_quality_score(gap_pct, volume_ratio, atr)

                signals.append({
                    'symbol': symbol,
                    'gap_pct': gap_pct,
                    'price': price,
                    'volume_ratio': volume_ratio,
                    'atr': atr,
                    'quality_score': quality,
                    'entry_price': price,
                    'stop_loss': price - (atr * self.atr_stop_multiplier),
                    'take_profit': price + (atr * self.atr_stop_multiplier * self.profit_target_r)
                })

                logging.info(f"[MOMENTUM] Signal: {symbol} gap={gap_pct:.1f}% vol={volume_ratio:.1f}x quality={quality:.2f}")

            except Exception as e:
                logging.error(f"[MOMENTUM] Scan error for {symbol}: {e}")
                continue

        return signals

    def _calculate_gap(self, symbol: str) -> Optional[float]:
        """Calculate gap percentage from previous day's close"""
        try:
            # FIXED: Get yesterday's close correctly - bars[-1] could be today's partial bar during market hours
            # Fetch enough history to ensure we have a completed previous day bar
            end_time = datetime.now(timezone.utc)
            start_time = end_time - timedelta(days=10)

            bars_data = self.alpaca.get_bars(
                symbol,
                timeframe="1Day",
                start=start_time.isoformat(),
                limit=10
            )

            # FIXED: Check bars_data is not None before accessing dict keys
            if bars_data is None:
                return None

            if 'bars' not in bars_data:
                return None

            bars = bars_data['bars']

            if not bars or len(bars) < 2:
                return None

            # Use bars[-2] as yesterday's close (bars[-1] might be today's partial bar)
            # This is safer during market hours
            prev_close = bars[-2]['c'] if len(bars) >= 2 else bars[-1]['c']

            # Get current price
            quote = self.alpaca.get_latest_quote(symbol)
            if not quote:
                return None

            current_price = (quote['ap'] + quote['bp']) / 2
            gap_pct = ((current_price - prev_close) / prev_close) * 100

            return gap_pct

        except Exception as e:
            logging.error(f"[MOMENTUM] Gap calculation failed for {symbol}: {e}")
            return None

    def _get_volume_ratio(self, symbol: str) -> Optional[float]:
        """Calculate current volume vs 20-day average"""
        try:
            end_time = datetime.now(timezone.utc)
            start_time = end_time - timedelta(days=30)

            bars_data = self.alpaca.get_bars(
                symbol,
                timeframe="1Day",
                start=start_time.isoformat(),
                limit=20
            )

            if not bars_data or 'bars' not in bars_data or len(bars_data['bars']) < 10:
                return None

            bars = bars_data['bars']
            avg_daily_volume = sum(b['v'] for b in bars[-20:]) / min(20, len(bars))

            # FIXED: Get today's volume from market open (handle DST correctly)
            # Market opens at 9:30 AM ET, which is 14:30 UTC (EST) or 13:30 UTC (EDT)
            try:
                from zoneinfo import ZoneInfo
                now_et = datetime.now(ZoneInfo("America/New_York"))
            except ImportError:
                import pytz
                now_et = datetime.now(pytz.timezone("America/New_York"))

            # Today's market open in ET
            market_open_et = now_et.replace(hour=9, minute=30, second=0, microsecond=0)
            # Convert to UTC for API call
            today_start = market_open_et.astimezone(timezone.utc)
            today_bars = self.alpaca.get_bars(
                symbol,
                timeframe="1Min",
                start=today_start.isoformat(),
                limit=1000
            )

            if not today_bars or 'bars' not in today_bars:
                return None

            current_volume = sum(b['v'] for b in today_bars['bars'])

            if avg_daily_volume == 0:
                return None

            # FIXED: Time-normalize the comparison using ET timezone (not hardcoded UTC)
            # Calculate minutes elapsed since market open
            minutes_elapsed = (now_et - market_open_et).total_seconds() / 60

            # Trading day is 390 minutes (9:30 AM - 4:00 PM)
            total_minutes = 390

            # Ensure we don't divide by zero or negative values
            if minutes_elapsed <= 0:
                return None

            # Extrapolate expected full-day volume based on current pace
            expected_full_day_volume = (current_volume / minutes_elapsed) * total_minutes

            # Return relative volume (current pace vs historical average)
            return expected_full_day_volume / avg_daily_volume

        except Exception as e:
            logging.error(f"[MOMENTUM] Volume ratio failed for {symbol}: {e}")
            return None

    def _check_float(self, symbol: str) -> bool:
        """Check if float is within acceptable range"""
        try:
            details = self.polygon.get_ticker_details(symbol)
            if not details:
                return True  # Allow if can't determine

            shares_outstanding = details.get('share_class_shares_outstanding')
            if shares_outstanding and shares_outstanding > self.max_float:
                return False

            return True

        except Exception:
            return True  # Allow if check fails

    def _check_multi_tf_confirmation(self, symbol: str) -> bool:
        """Check 5min and 15min trends align"""
        try:
            now = datetime.now(timezone.utc)
            start = now - timedelta(days=5)

            # 5-minute trend
            bars_5m = self.alpaca.get_bars(symbol, "5Min", start.isoformat(), limit=20)
            if not bars_5m or 'bars' not in bars_5m or len(bars_5m['bars']) < 10:
                return False

            ema_5m = self._calculate_ema([b['c'] for b in bars_5m['bars']], 9)

            # 15-minute trend
            bars_15m = self.alpaca.get_bars(symbol, "15Min", start.isoformat(), limit=20)
            if not bars_15m or 'bars' not in bars_15m or len(bars_15m['bars']) < 10:
                return False

            ema_15m = self._calculate_ema([b['c'] for b in bars_15m['bars']], 9)

            # FIXED: Add bounds checking before array access
            # Need at least 6 elements to safely access [-5] (index -5 means 5th from end)
            if len(ema_5m) < 6 or len(ema_15m) < 6:
                return False

            # Both should be in uptrend (current EMA > EMA from 5 periods ago)
            return ema_5m[-1] > ema_5m[-5] and ema_15m[-1] > ema_15m[-5]

        except Exception as e:
            logging.error(f"[MOMENTUM] Multi-TF check failed for {symbol}: {e}")
            return False

    def _check_clean_candle(self, symbol: str) -> bool:
        """Check last candle has small wicks (body > 60% of range)"""
        try:
            now = datetime.now(timezone.utc)
            start = now - timedelta(hours=2)

            bars = self.alpaca.get_bars(symbol, "5Min", start.isoformat(), limit=5)
            if not bars or 'bars' not in bars or len(bars['bars']) < 1:
                return False

            last_bar = bars['bars'][-1]
            body = abs(last_bar['c'] - last_bar['o'])
            total_range = last_bar['h'] - last_bar['l']

            if total_range == 0:
                return False

            body_pct = body / total_range
            return body_pct >= 0.60

        except Exception as e:
            logging.error(f"[MOMENTUM] Clean candle check failed for {symbol}: {e}")
            return False

    def _calculate_atr(self, symbol: str, period: int = 14) -> Optional[float]:
        """Calculate Average True Range"""
        try:
            now = datetime.now(timezone.utc)
            start = now - timedelta(days=30)

            bars_data = self.alpaca.get_bars(symbol, "1Day", start.isoformat(), limit=period + 10)
            if not bars_data or 'bars' not in bars_data or len(bars_data['bars']) < period:
                return None

            bars = bars_data['bars']
            trs = []

            for i in range(1, len(bars)):
                high = bars[i]['h']
                low = bars[i]['l']
                prev_close = bars[i-1]['c']

                tr = max(
                    high - low,
                    abs(high - prev_close),
                    abs(low - prev_close)
                )
                trs.append(tr)

            if len(trs) < period:
                return None

            atr = sum(trs[-period:]) / period
            return atr

        except Exception as e:
            logging.error(f"[MOMENTUM] ATR calculation failed for {symbol}: {e}")
            return None

    def _calculate_ema(self, prices: List[float], period: int) -> List[float]:
        """Calculate Exponential Moving Average"""
        if len(prices) < period:
            return []

        multiplier = 2 / (period + 1)
        ema = [sum(prices[:period]) / period]

        for price in prices[period:]:
            ema.append((price - ema[-1]) * multiplier + ema[-1])

        return ema

    def _calculate_quality_score(self, gap_pct: float, volume_ratio: float, atr: float) -> float:
        """Calculate setup quality score (0.5 - 1.5)"""
        score = 0.5

        # Gap contribution (0 - 0.3)
        if gap_pct >= 10:
            score += 0.3
        elif gap_pct >= 5:
            score += 0.15

        # Volume contribution (0 - 0.4)
        if volume_ratio >= 5:
            score += 0.4
        elif volume_ratio >= 3:
            score += 0.2

        # ATR contribution (0 - 0.3)
        # Higher ATR = higher volatility = higher score
        if atr >= 2.0:
            score += 0.3
        elif atr >= 1.0:
            score += 0.15

        return min(1.5, score)

# ============================================================================
# VWAP SCALPING STRATEGY
# ============================================================================

class VWAPScalpingStrategy:
    """
    VWAP reclaim scalping on high-volume liquid stocks
    Entry: Price reclaims VWAP after dip, volume confirmation
    Exit: Fixed profit target (0.5-1.5% or VWAP rejection)
    """

    def __init__(self, config: Dict, alpaca: AlpacaClient):
        self.config = config
        self.alpaca = alpaca

        self.min_price = config.get('min_price', 20.0)
        self.max_price = config.get('max_price', 500.0)
        self.min_avg_volume = config.get('min_avg_volume', 5_000_000)
        self.max_vwap_distance_pct = config.get('max_vwap_distance_pct', 0.25)  # Tighter
        self.profit_target_pct = config.get('profit_target_pct', 0.65)  # Optimized R:R
        self.stop_loss_pct = config.get('stop_loss_pct', 0.50)
        self.max_adx = config.get('max_adx', 23.0)  # ADX filter for ranging markets
        self.cooldown_bars = config.get('cooldown_bars', 10)
        self._last_trade_time: Dict[str, datetime] = {}  # Cooldown tracking

        # Trailing stop settings (OPTIMIZED for higher win rate)
        self.trailing_stop_enabled = config.get('trailing_stop_enabled', True)
        self.trailing_stop_activation_pct = config.get('trailing_stop_activation_pct', 0.30)
        self.trailing_stop_distance_pct = config.get('trailing_stop_distance_pct', 0.25)
        self.max_hold_minutes = config.get('max_hold_minutes', 180)

    def scan_for_signals(self, universe: List[str]) -> List[Dict]:
        """Scan for VWAP reclaim setups"""
        signals = []

        for symbol in universe:
            try:
                # Get current quote
                quote = self.alpaca.get_latest_quote(symbol)
                if not quote:
                    continue

                price = (quote['ap'] + quote['bp']) / 2
                if price < self.min_price or price > self.max_price:
                    continue

                # Calculate VWAP
                vwap = self._calculate_vwap(symbol)
                if vwap is None:
                    continue

                # Check if price is reclaiming VWAP
                distance_pct = ((price - vwap) / vwap) * 100

                # Look for reclaim: price slightly above VWAP (0 to 0.5%)
                if distance_pct < 0 or distance_pct > self.max_vwap_distance_pct:
                    continue

                # Check volume (require 2x for higher quality signals)
                volume_ratio = self._get_volume_ratio(symbol)
                if volume_ratio is None or volume_ratio < 2.0:
                    continue

                # Cooldown check - avoid overtrading same symbol
                if symbol in self._last_trade_time:
                    elapsed = (datetime.now() - self._last_trade_time[symbol]).total_seconds()
                    if elapsed < self.cooldown_bars * 60:  # cooldown_bars in minutes
                        continue

                # Check recent trend (was below VWAP recently)
                if not self._check_reclaim_pattern(symbol, vwap):
                    continue

                # ADX filter - avoid trending markets (VWAP works better in ranges)
                adx = self._calculate_adx_simple(symbol)
                if adx is not None and adx > self.max_adx:
                    continue

                # Calculate quality score
                quality = self._calculate_quality_score(distance_pct, volume_ratio)

                signals.append({
                    'symbol': symbol,
                    'price': price,
                    'vwap': vwap,
                    'distance_pct': distance_pct,
                    'volume_ratio': volume_ratio,
                    'quality_score': quality,
                    'entry_price': price,
                    'stop_loss': price * (1 - self.stop_loss_pct / 100),
                    'take_profit': price * (1 + self.profit_target_pct / 100)
                })

                logging.info(f"[VWAP] Signal: {symbol} price=${price:.2f} vwap=${vwap:.2f} vol={volume_ratio:.1f}x")

            except Exception as e:
                logging.error(f"[VWAP] Scan error for {symbol}: {e}")
                continue

        return signals

    def _calculate_vwap(self, symbol: str) -> Optional[float]:
        """Calculate VWAP from market open"""
        try:
            # FIXED: Get bars from market open (handle DST correctly)
            # Market opens at 9:30 AM ET, which is 14:30 UTC (EST) or 13:30 UTC (EDT)
            try:
                from zoneinfo import ZoneInfo
                now_et = datetime.now(ZoneInfo("America/New_York"))
            except ImportError:
                import pytz
                now_et = datetime.now(pytz.timezone("America/New_York"))

            # Today's market open in ET
            market_open_et = now_et.replace(hour=9, minute=30, second=0, microsecond=0)
            # Convert to UTC for API call
            today_start = market_open_et.astimezone(timezone.utc)

            bars_data = self.alpaca.get_bars(symbol, "1Min", today_start.isoformat(), limit=1000)
            if not bars_data or 'bars' not in bars_data or len(bars_data['bars']) == 0:
                return None

            bars = bars_data['bars']

            cum_volume = 0
            cum_vwap = 0

            for bar in bars:
                typical_price = (bar['h'] + bar['l'] + bar['c']) / 3
                volume = bar['v']

                cum_vwap += typical_price * volume
                cum_volume += volume

            if cum_volume == 0:
                return None

            return cum_vwap / cum_volume

        except Exception as e:
            logging.error(f"[VWAP] Calculation failed for {symbol}: {e}")
            return None

    def _get_volume_ratio(self, symbol: str) -> Optional[float]:
        """Get current volume ratio vs average"""
        try:
            # Get 20-day average volume
            end_time = datetime.now(timezone.utc)
            start_time = end_time - timedelta(days=30)

            bars_data = self.alpaca.get_bars(symbol, "1Day", start_time.isoformat(), limit=20)
            if not bars_data or 'bars' not in bars_data or len(bars_data['bars']) < 10:
                return None

            bars = bars_data['bars']
            avg_volume = sum(b['v'] for b in bars[-20:]) / min(20, len(bars))

            if avg_volume < self.min_avg_volume:
                return None

            # FIXED: Get today's volume from market open (handle DST correctly)
            # Market opens at 9:30 AM ET, which is 14:30 UTC (EST) or 13:30 UTC (EDT)
            try:
                from zoneinfo import ZoneInfo
                now_et = datetime.now(ZoneInfo("America/New_York"))
            except ImportError:
                import pytz
                now_et = datetime.now(pytz.timezone("America/New_York"))

            # Today's market open in ET
            market_open_et = now_et.replace(hour=9, minute=30, second=0, microsecond=0)
            # Convert to UTC for API call
            today_start = market_open_et.astimezone(timezone.utc)

            today_bars = self.alpaca.get_bars(symbol, "1Min", today_start.isoformat(), limit=1000)

            if not today_bars or 'bars' not in today_bars:
                return None

            current_volume = sum(b['v'] for b in today_bars['bars'])

            return current_volume / avg_volume if avg_volume > 0 else None

        except Exception as e:
            logging.error(f"[VWAP] Volume ratio failed for {symbol}: {e}")
            return None

    def _check_reclaim_pattern(self, symbol: str, vwap: float) -> bool:
        """Check if price was recently below VWAP (reclaim pattern)"""
        try:
            # Get last 10 minutes of 1min bars
            now = datetime.now(timezone.utc)
            start = now - timedelta(minutes=15)

            bars_data = self.alpaca.get_bars(symbol, "1Min", start.isoformat(), limit=15)
            if not bars_data or 'bars' not in bars_data or len(bars_data['bars']) < 5:
                return False

            bars = bars_data['bars']

            # Check if at least one recent bar was below VWAP
            was_below = any(bar['c'] < vwap for bar in bars[-10:-2])

            # Current bar should be above VWAP
            is_above = bars[-1]['c'] > vwap

            return was_below and is_above

        except Exception as e:
            logging.error(f"[VWAP] Reclaim pattern check failed for {symbol}: {e}")
            return False

    def _calculate_adx_simple(self, symbol: str, period: int = 14) -> Optional[float]:
        """Calculate ADX for trend strength filter"""
        try:
            now = datetime.now(timezone.utc)
            start = now - timedelta(minutes=period * 5)  # Get enough bars for ADX

            bars_data = self.alpaca.get_bars(symbol, "1Min", start.isoformat(), limit=100)
            if not bars_data or 'bars' not in bars_data or len(bars_data['bars']) < period * 2:
                return None

            bars = bars_data['bars']

            # Calculate +DM, -DM, TR
            plus_dm_list = []
            minus_dm_list = []
            tr_list = []

            for i in range(1, len(bars)):
                high = bars[i]['h']
                low = bars[i]['l']
                prev_high = bars[i-1]['h']
                prev_low = bars[i-1]['l']
                prev_close = bars[i-1]['c']

                plus_dm = max(0, high - prev_high) if high - prev_high > prev_low - low else 0
                minus_dm = max(0, prev_low - low) if prev_low - low > high - prev_high else 0
                tr = max(high - low, abs(high - prev_close), abs(low - prev_close))

                plus_dm_list.append(plus_dm)
                minus_dm_list.append(minus_dm)
                tr_list.append(tr)

            if len(tr_list) < period:
                return None

            # Smoothed averages (simple moving average)
            atr = sum(tr_list[-period:]) / period
            plus_di = 100 * (sum(plus_dm_list[-period:]) / period) / atr if atr > 0 else 0
            minus_di = 100 * (sum(minus_dm_list[-period:]) / period) / atr if atr > 0 else 0

            # DX and ADX
            di_sum = plus_di + minus_di
            if di_sum <= 0:
                return 0.0

            dx = 100 * abs(plus_di - minus_di) / di_sum
            return dx  # For simplicity, return DX as ADX approximation

        except Exception as e:
            logging.debug(f"[VWAP] ADX calculation failed for {symbol}: {e}")
            return None

    def record_trade(self, symbol: str):
        """Record trade time for cooldown tracking"""
        self._last_trade_time[symbol] = datetime.now()

    def _calculate_quality_score(self, distance_pct: float, volume_ratio: float) -> float:
        """Calculate setup quality (0.5 - 1.5)"""
        score = 0.5

        # Distance from VWAP (closer = better)
        if distance_pct < 0.2:
            score += 0.5
        elif distance_pct < 0.4:
            score += 0.3

        # Volume
        if volume_ratio >= 3:
            score += 0.5
        elif volume_ratio >= 2:
            score += 0.3

        return min(1.5, score)

    def update_trailing_stop(self, position: Position, current_price: float) -> Optional[float]:
        """
        Update trailing stop for VWAP positions.
        Returns new stop price if it should be updated, None otherwise.
        """
        if not self.trailing_stop_enabled:
            return None

        if position.strategy != StrategyType.VWAP_SCALPING:
            return None

        # Calculate profit percentage
        profit_pct = (current_price - position.entry_price) / position.entry_price * 100

        # Check if trailing stop should be activated
        if profit_pct < self.trailing_stop_activation_pct:
            return None

        # Update max price tracking
        if current_price > position.max_price:
            position.max_price = current_price

            # Calculate new trailing stop
            new_stop = current_price * (1 - self.trailing_stop_distance_pct / 100)

            # Only move stop up, never down
            if new_stop > position.stop_loss:
                position.trail_stop = new_stop
                return new_stop

        return None

    def should_time_exit(self, position: Position) -> bool:
        """Check if position should be closed due to max hold time."""
        if position.strategy != StrategyType.VWAP_SCALPING:
            return False

        elapsed_minutes = (datetime.now(timezone.utc) - position.entry_time).total_seconds() / 60
        return elapsed_minutes > self.max_hold_minutes


# ============================================================================
# VIRTUAL BRACKET MANAGER (Extended Hours)
# ============================================================================

class VirtualBracketManager:
    """
    Manages virtual stop loss and take profit for extended hours positions.
    Broker doesn't support bracket orders in extended hours, so we manage them in software.
    """

    def __init__(self, alpaca_client: 'AlpacaClient', state_manager: 'StateManager'):
        self.alpaca = alpaca_client
        self.state_manager = state_manager

    def add_bracket(self, symbol: str, position: Position):
        """Create virtual bracket for an extended hours position"""
        side = "buy"  # Assume buy for now (can extend for shorts later)

        bracket = VirtualBracket(
            symbol=symbol,
            side=side,
            quantity=position.quantity,
            entry_price=position.entry_price,
            stop_loss=position.stop_loss,
            take_profit=position.take_profit,
            entry_time=position.entry_time,
            session=position.session,
            active=True,
            moved_breakeven=False
        )

        with self.state_manager.lock:
            self.state_manager.state.virtual_brackets[symbol] = bracket
            self.state_manager.save_state()

        logging.info(f"[VIRTUAL BRACKET] Created for {symbol}: SL=${position.stop_loss:.2f}, TP=${position.take_profit:.2f}")

    def check_and_execute_brackets(self):
        """Check all virtual brackets and execute if stop/target hit"""
        for symbol in list(self.state_manager.state.virtual_brackets.keys()):
            bracket = self.state_manager.state.virtual_brackets.get(symbol)

            if not bracket or not bracket.active:
                continue

            try:
                # Get current position to check if it still exists
                alpaca_position = self.alpaca.get_position(symbol)
                if not alpaca_position:
                    # Position closed elsewhere - clean up bracket
                    logging.info(f"[VIRTUAL BRACKET] {symbol} position closed, removing bracket")
                    with self.state_manager.lock:
                        del self.state_manager.state.virtual_brackets[symbol]
                        self.state_manager.save_state()
                    continue

                # Get current price
                current_price = float(alpaca_position.get('current_price', 0))
                if current_price <= 0:
                    continue

                # Check stop loss (long position)
                if current_price <= bracket.stop_loss:
                    logging.warning(f"[VIRTUAL BRACKET] {symbol} hit stop loss: ${current_price:.2f} <= ${bracket.stop_loss:.2f}")
                    self._execute_exit(symbol, bracket, current_price, 'STOP_LOSS')

                # Check take profit (long position)
                elif current_price >= bracket.take_profit:
                    logging.info(f"[VIRTUAL BRACKET] {symbol} hit take profit: ${current_price:.2f} >= ${bracket.take_profit:.2f}")
                    self._execute_exit(symbol, bracket, current_price, 'TAKE_PROFIT')

            except Exception as e:
                logging.error(f"[VIRTUAL BRACKET] Error checking {symbol}: {e}")

    def _execute_exit(self, symbol: str, bracket: VirtualBracket, current_price: float, reason: str):
        """Execute limit exit order for virtual bracket"""
        try:
            # Get fresh quote for aggressive limit pricing
            quote = self.alpaca.get_latest_quote(symbol)

            if quote and quote.get('bp', 0) > 0 and quote.get('ap', 0) > 0:
                # Aggressive limit: 0.5% below bid for sells (ensure fill)
                limit_price = quote['bp'] * 0.995
            else:
                # Fallback: 0.5% below current price
                limit_price = current_price * 0.995

            # Round to 2 decimals
            limit_price = round(limit_price, 2)

            # Submit limit order with extended_hours flag
            payload = {
                "symbol": symbol,
                "qty": bracket.quantity,
                "side": "sell",  # Closing long position
                "type": "limit",
                "limit_price": f"{limit_price:.2f}",
                "time_in_force": "day",
                "extended_hours": True
            }

            order = self.alpaca._request("POST", f"{self.alpaca.base_url}/v2/orders", json=payload)

            if order:
                logging.info(f"[VIRTUAL BRACKET] {symbol} exit order submitted: {reason} @ ${limit_price:.2f} (order_id={order.get('id')})")

                # Mark bracket as inactive
                with self.state_manager.lock:
                    bracket.active = False
                    self.state_manager.save_state()
            else:
                logging.error(f"[VIRTUAL BRACKET] Failed to submit exit order for {symbol}")

        except Exception as e:
            logging.error(f"[VIRTUAL BRACKET] Error executing exit for {symbol}: {e}")

    def remove_bracket(self, symbol: str):
        """Remove virtual bracket (position closed)"""
        if symbol in self.state_manager.state.virtual_brackets:
            with self.state_manager.lock:
                del self.state_manager.state.virtual_brackets[symbol]
                self.state_manager.save_state()
            logging.info(f"[VIRTUAL BRACKET] Removed bracket for {symbol}")

# ============================================================================
# MAIN BOT CLASS
# ============================================================================

class MasterTradingBot:
    """Master trading bot combining all strategies"""

    def __init__(self, config_path: str):
        self.config = self._load_config(config_path)
        self.running = False
        self.kill_switch_file = Path(self.config.get('kill_switch_file', 'KILL_SWITCH.txt'))

        # Initialize components
        self.state_manager = StateManager(self.config.get('state_file', 'master_bot_state.json'))
        self.alpaca = AlpacaClient(
            self.config['alpaca']['api_key'],
            self.config['alpaca']['api_secret'],
            self.config['alpaca'].get('paper', True)
        )
        self.polygon = PolygonClient(self.config.get('polygon_api_key', ''))
        self.risk_manager = RiskManager(self.config.get('risk', {}))
        self.alert_manager = AlertManager(self.config.get('alerts', {}))
        self.virtual_bracket_manager = VirtualBracketManager(self.alpaca, self.state_manager)

        # Initialize strategies
        momentum_config = self.config.get('strategies', {}).get('momentum_breakout', {})
        vwap_config = self.config.get('strategies', {}).get('vwap_scalping', {})

        self.momentum_strategy = MomentumBreakoutStrategy(momentum_config, self.alpaca, self.polygon)
        self.vwap_strategy = VWAPScalpingStrategy(vwap_config, self.alpaca)

        # Trading universe
        self.core_universe = self.config.get('universe', [])
        self.dynamic_universe = set()  # Discovered high-volume movers
        self.last_universe_scan = None

        # Dynamic universe settings
        self.enable_dynamic_universe = self.config.get('dynamic_universe', {}).get('enabled', True)
        self.dynamic_scan_interval = self.config.get('dynamic_universe', {}).get('scan_interval_sec', 300)
        self.dynamic_min_rvol = self.config.get('dynamic_universe', {}).get('min_rvol', 2.5)
        self.dynamic_min_price = self.config.get('dynamic_universe', {}).get('min_price', 10.0)
        self.dynamic_max_price = self.config.get('dynamic_universe', {}).get('max_price', 1000.0)
        self.dynamic_min_volume_usd = self.config.get('dynamic_universe', {}).get('min_volume_usd', 50_000_000)
        self.dynamic_max_size = self.config.get('dynamic_universe', {}).get('max_size', 20)

        # Market regime filters
        self.min_spy_rvol = self.config.get('market_filters', {}).get('min_spy_rvol', 0.8)

        # Performance optimization - caching
        self._cache = {}
        self._cache_timestamps = {}
        self._cache_ttl = 60  # Cache TTL in seconds

        # Setup logging
        self._setup_logging()

        logging.info("[MASTER BOT] Initialized successfully")

    def _load_config(self, config_path: str) -> Dict:
        """Load and validate YAML configuration"""
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)

        # Validate configuration
        self._validate_config(config)
        return config

    def _validate_config(self, config: Dict):
        """Validate configuration parameters for safety"""
        errors = []
        warnings = []

        # Required fields
        if 'alpaca' not in config:
            errors.append("Missing 'alpaca' section")
        else:
            if not config['alpaca'].get('api_key') or config['alpaca']['api_key'] == 'YOUR_ALPACA_API_KEY':
                errors.append("Invalid or missing alpaca.api_key")
            if not config['alpaca'].get('api_secret') or config['alpaca']['api_secret'] == 'YOUR_ALPACA_SECRET_KEY':
                errors.append("Invalid or missing alpaca.api_secret")

        if not config.get('polygon_api_key') or config['polygon_api_key'] == 'YOUR_POLYGON_API_KEY':
            errors.append("Invalid or missing polygon_api_key")

        if not config.get('universe') or len(config['universe']) == 0:
            errors.append("Empty universe - add at least one symbol")

        # Risk management validation
        risk_config = config.get('risk', {})

        max_daily_loss = risk_config.get('max_daily_loss_pct', 0.05)
        if max_daily_loss > 0.20:  # > 20%
            warnings.append(f"max_daily_loss_pct is {max_daily_loss*100}% - very high risk! Recommended: 2-5%")
        elif max_daily_loss < 0.01:  # < 1%
            warnings.append(f"max_daily_loss_pct is {max_daily_loss*100}% - very conservative, may limit opportunities")

        max_position_risk = risk_config.get('max_position_risk_pct', 0.02)
        if max_position_risk > 0.05:  # > 5%
            warnings.append(f"max_position_risk_pct is {max_position_risk*100}% - very high per-trade risk! Recommended: 1-3%")

        max_concurrent = risk_config.get('max_concurrent_positions', 8)
        if max_concurrent > 15:
            warnings.append(f"max_concurrent_positions is {max_concurrent} - difficult to monitor, recommended: 5-10")

        circuit_breaker = risk_config.get('circuit_breaker_drawdown', 0.10)
        if circuit_breaker > 0.25:  # > 25%
            warnings.append(f"circuit_breaker_drawdown is {circuit_breaker*100}% - very high! Recommended: 10-15%")

        # Strategy validation
        momentum_config = config.get('strategies', {}).get('momentum_breakout', {})
        if momentum_config.get('min_gap_pct', 3.0) < 1.0:
            warnings.append("momentum_breakout.min_gap_pct < 1% - may generate too many false signals")

        vwap_config = config.get('strategies', {}).get('vwap_scalping', {})
        vwap_stop = vwap_config.get('stop_loss_pct', 0.5)
        vwap_target = vwap_config.get('profit_target_pct', 1.0)
        if vwap_stop >= vwap_target:
            errors.append(f"vwap_scalping stop_loss_pct ({vwap_stop}%) >= profit_target_pct ({vwap_target}%) - negative risk/reward!")

        # Log validation results
        if errors:
            logging.error("[CONFIG] VALIDATION ERRORS:")
            for error in errors:
                logging.error(f"  - {error}")
            raise ValueError(f"Configuration validation failed with {len(errors)} errors")

        if warnings:
            logging.warning("[CONFIG] VALIDATION WARNINGS:")
            for warning in warnings:
                logging.warning(f"  - {warning}")
            logging.warning("[CONFIG] Review warnings before trading with real money!")

        logging.info("[CONFIG] Validation passed")

    def _setup_logging(self):
        """Configure logging with rotation"""
        log_dir = Path(self.config.get('log_dir', 'logs'))
        log_dir.mkdir(exist_ok=True)

        log_file = log_dir / f"master_bot_{datetime.now().strftime('%Y%m%d')}.log"

        logging.basicConfig(
            level=logging.INFO,
            format='[%(asctime)s] %(levelname)s: %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler(sys.stdout)
            ]
        )

    def _get_cached(self, key: str):
        """Get cached value if not expired"""
        if key not in self._cache:
            return None

        timestamp = self._cache_timestamps.get(key)
        if not timestamp:
            return None

        # Check if cache expired
        age = (datetime.now(timezone.utc) - timestamp).total_seconds()
        if age > self._cache_ttl:
            # Cache expired, remove it
            del self._cache[key]
            del self._cache_timestamps[key]
            return None

        return self._cache[key]

    def _set_cache(self, key: str, value: any):
        """Set cached value with timestamp"""
        self._cache[key] = value
        self._cache_timestamps[key] = datetime.now(timezone.utc)

    def reconcile_positions_on_startup(self):
        """
        Reconcile bot state with actual Alpaca positions on startup.
        Prevents state corruption from crashes/restarts.
        """
        try:
            logging.info("[RECONCILE] Starting position reconciliation...")
            print(f"[{datetime.now().strftime('%H:%M:%S')}] [RECONCILE] Fetching broker positions...")

            # Get all actual positions from Alpaca
            alpaca_positions = self.alpaca.get_positions()
            alpaca_symbols = set()

            if alpaca_positions:
                for pos in alpaca_positions:
                    symbol = pos.get('symbol')
                    alpaca_symbols.add(symbol)

            # Get bot's tracked positions
            bot_symbols = set(self.state_manager.state.positions.keys())

            # Find discrepancies
            missing_from_bot = alpaca_symbols - bot_symbols
            missing_from_alpaca = bot_symbols - alpaca_symbols

            # Handle positions in Alpaca but not in bot state
            if missing_from_bot:
                logging.warning(f"[RECONCILE] Found {len(missing_from_bot)} Alpaca positions not tracked by bot: {missing_from_bot}")
                self.alert_manager.send_alert(
                    'warning',
                    'Position Reconciliation Warning',
                    f"Found {len(missing_from_bot)} untracked positions in Alpaca: {', '.join(missing_from_bot)}\n"
                    f"These may be from previous sessions or manual trades. "
                    f"Bot will not manage these positions automatically."
                )

            # Handle positions in bot state but not in Alpaca (ghost positions)
            if missing_from_alpaca:
                logging.warning(f"[RECONCILE] Found {len(missing_from_alpaca)} ghost positions in bot state: {missing_from_alpaca}")

                for symbol in missing_from_alpaca:
                    position = self.state_manager.state.positions[symbol]

                    # Check if this was a recent order that may still be pending
                    age_seconds = (datetime.now(timezone.utc) - position.entry_time).total_seconds()

                    # FIXED: Always verify order status for ghost positions, even recent ones
                    should_remove = True

                    if age_seconds < 600:  # Less than 10 minutes old - verify order status
                        logging.info(f"[RECONCILE] Recent ghost {symbol} (age: {age_seconds:.0f}s), verifying order status")

                        # FIXED: Guard against None order_id
                        if not position.order_id:
                            logging.warning(f"[RECONCILE] {symbol} has no order_id, removing ghost position")
                            should_remove = True
                        else:
                            try:
                                # Check if order actually exists and its status
                                order = self.alpaca.get_order(position.order_id)
                                if order:
                                    status = order.get('status')
                                    if status == 'filled':
                                        # Order filled but not showing in positions yet - might be API lag
                                        logging.info(f"[RECONCILE] Order filled for {symbol}, keeping position for sync")
                                        should_remove = False
                                    elif status in ['pending_new', 'accepted', 'new', 'partially_filled']:
                                        # Order still pending - keep for now
                                        logging.info(f"[RECONCILE] Order pending for {symbol}, keeping position")
                                        should_remove = False
                                    else:
                                        # Order cancelled/rejected/expired - remove position
                                        logging.warning(f"[RECONCILE] Order {status} for {symbol}, removing position")
                                else:
                                    logging.warning(f"[RECONCILE] Order not found for {symbol}, removing position")
                            except Exception as e:
                                logging.error(f"[RECONCILE] Error checking order for {symbol}: {e}")
                                # On error, remove old positions but keep very recent ones
                                if age_seconds < 120:  # Less than 2 minutes
                                    should_remove = False
                    else:
                        logging.error(f"[RECONCILE] Removing stale ghost position {symbol} (age: {age_seconds/3600:.1f} hours)")

                    if should_remove:
                        del self.state_manager.state.positions[symbol]

            # Verify position quantities match
            for symbol in bot_symbols & alpaca_symbols:
                bot_pos = self.state_manager.state.positions[symbol]
                alpaca_pos = next((p for p in alpaca_positions if p.get('symbol') == symbol), None)

                if alpaca_pos:
                    alpaca_qty = int(alpaca_pos.get('qty', 0))
                    bot_qty = bot_pos.quantity

                    if alpaca_qty != bot_qty:
                        logging.warning(f"[RECONCILE] Quantity mismatch for {symbol}: "
                                      f"Alpaca={alpaca_qty}, Bot={bot_qty}, updating bot state")
                        bot_pos.quantity = alpaca_qty

                    # Update position to HOLDING if it was stuck in ENTRY_SUBMITTED
                    if bot_pos.state == PositionState.ENTRY_SUBMITTED:
                        logging.info(f"[RECONCILE] Updating {symbol} state from ENTRY_SUBMITTED to HOLDING")
                        bot_pos.state = PositionState.HOLDING

            # Save reconciled state
            self.state_manager.save_state()

            reconcile_summary = (
                f"Bot positions: {len(bot_symbols)}, "
                f"Alpaca positions: {len(alpaca_symbols)}, "
                f"Untracked: {len(missing_from_bot)}, "
                f"Ghost: {len(missing_from_alpaca)}"
            )
            logging.info(f"[RECONCILE] Completed - {reconcile_summary}")
            print(f"[{datetime.now().strftime('%H:%M:%S')}] [RECONCILE] Completed - {reconcile_summary}")

            return True

        except Exception as e:
            logging.error(f"[RECONCILE] Failed: {e}", exc_info=True)
            print(f"[{datetime.now().strftime('%H:%M:%S')}] [RECONCILE] ERROR: {e}")
            import traceback
            traceback.print_exc()
            self.alert_manager.send_alert(
                'critical',
                'Position Reconciliation Failed',
                f"Error during startup reconciliation: {e}\n"
                f"Bot state may be inconsistent with actual positions!"
            )
            return False

    def is_market_open(self) -> bool:
        """Check if market is currently open for trading"""
        try:
            clock = self.alpaca.get_clock()

            if not clock:
                logging.warning("[MARKET] Unable to get market clock")
                return False

            is_open = clock.get('is_open', False)
            next_open = clock.get('next_open')
            next_close = clock.get('next_close')

            if not is_open:
                if next_open:
                    logging.info(f"[MARKET] Market closed. Next open: {next_open}")
                return False

            # Additional check - verify we're within market hours (9:30 AM - 4:00 PM ET)
            # FIXED: Use zoneinfo for proper DST handling instead of fixed UTC offset
            try:
                from zoneinfo import ZoneInfo
                now_et = datetime.now(ZoneInfo("America/New_York"))
            except ImportError:
                # Fallback for Python < 3.9 or missing tzdata
                try:
                    import pytz
                    now_et = datetime.now(pytz.timezone("America/New_York"))
                except ImportError:
                    # Last resort - use UTC offset (won't handle DST correctly but won't crash)
                    now_et = datetime.now(timezone.utc) - timedelta(hours=5)

            market_open_time = now_et.replace(hour=9, minute=30, second=0, microsecond=0)
            market_close_time = now_et.replace(hour=16, minute=0, second=0, microsecond=0)

            if now_et < market_open_time or now_et >= market_close_time:
                logging.info(f"[MARKET] Outside market hours: {now_et.strftime('%H:%M:%S ET')}")
                return False

            return True

        except Exception as e:
            logging.error(f"[MARKET] Error checking market status: {e}")
            # Fail safe - don't trade if we can't confirm market is open
            return False

    def get_market_session(self) -> MarketSession:
        """
        Determine current market session: PRE, RTH, AFTER, or CLOSED.
        Extended hours support for VWAP strategy only.
        """
        try:
            # Get current time in ET
            try:
                from zoneinfo import ZoneInfo
                now_et = datetime.now(ZoneInfo("America/New_York"))
            except ImportError:
                import pytz
                now_et = datetime.now(pytz.timezone("America/New_York"))

            hour = now_et.hour
            minute = now_et.minute

            # Check if extended hours is enabled
            extended_config = self.config.get('extended_hours', {})
            extended_enabled = extended_config.get('enabled', False)

            # Get market clock to check if market is open at all today
            clock = self.alpaca.get_clock()
            if not clock:
                return MarketSession.CLOSED

            is_open = clock.get('is_open', False)

            # RTH (Regular Trading Hours): 9:30 AM - 4:00 PM ET
            if (hour == 9 and minute >= 30) or (hour >= 10 and hour < 16):
                return MarketSession.RTH

            # If not RTH and extended hours not enabled, market is closed
            if not extended_enabled:
                return MarketSession.CLOSED

            # Extended hours sessions (only if enabled)
            pre_start = extended_config.get('pre_market', {}).get('start_hour', 7)
            pre_end_hour = 9
            pre_end_minute = 30

            after_start_hour = 16
            after_start_minute = 0
            after_end = extended_config.get('after_hours', {}).get('end_hour', 18)

            # PRE-MARKET: Configured start time (default 7:00 AM) - 9:30 AM ET
            if (hour == pre_start) or (hour > pre_start and hour < pre_end_hour) or (hour == pre_end_hour and minute < pre_end_minute):
                return MarketSession.PRE

            # AFTER-HOURS: 4:00 PM - Configured end time (default 6:00 PM) ET
            if (hour == after_start_hour and minute >= after_start_minute) or (hour > after_start_hour and hour < after_end):
                return MarketSession.AFTER

            # Outside all trading windows
            return MarketSession.CLOSED

        except Exception as e:
            logging.error(f"[MARKET] Error determining market session: {e}")
            # Fail safe - return CLOSED if we can't determine session
            return MarketSession.CLOSED

    def check_kill_switch(self) -> bool:
        """Check if kill switch is activated"""
        if self.kill_switch_file.exists():
            logging.critical("[KILL SWITCH] File detected - stopping all trading")
            return True

        if os.getenv('TRADING_KILL_SWITCH') == '1':
            logging.critical("[KILL SWITCH] Environment variable set - stopping all trading")
            return True

        return False

    def check_market_regime(self) -> Tuple[bool, str]:
        """Check if market conditions are suitable for trading"""
        try:
            # Check SPY relative volume
            spy_rvol = self._get_spy_rvol()
            if spy_rvol is None:
                return False, "Unable to determine SPY volume"

            if spy_rvol < self.min_spy_rvol:
                return False, f"SPY RVOL {spy_rvol:.2f}x < {self.min_spy_rvol}x minimum"

            return True, "Market regime OK"

        except Exception as e:
            logging.error(f"[REGIME] Check failed: {e}")
            return False, f"Regime check error: {e}"

    def _get_spy_rvol(self) -> Optional[float]:
        """Calculate SPY relative volume (cached for performance)"""
        # FIXED: Check cache first (60 second TTL)
        cached_rvol = self._get_cached('spy_rvol')
        if cached_rvol is not None:
            return cached_rvol

        try:
            # Get 20-day average volume
            end_time = datetime.now(timezone.utc)
            start_time = end_time - timedelta(days=30)

            bars_data = self.alpaca.get_bars("SPY", "1Day", start_time.isoformat(), limit=20)
            if not bars_data or 'bars' not in bars_data or len(bars_data['bars']) < 10:
                return None

            bars = bars_data['bars']
            avg_volume = sum(b['v'] for b in bars[-20:]) / min(20, len(bars))

            # FIXED: Get today's volume from market open (handle DST correctly)
            # Market opens at 9:30 AM ET, which is 14:30 UTC (EST) or 13:30 UTC (EDT)
            try:
                from zoneinfo import ZoneInfo
                now_et = datetime.now(ZoneInfo("America/New_York"))
            except ImportError:
                import pytz
                now_et = datetime.now(pytz.timezone("America/New_York"))

            # Today's market open in ET
            market_open_et = now_et.replace(hour=9, minute=30, second=0, microsecond=0)
            # Convert to UTC for API call
            today_start = market_open_et.astimezone(timezone.utc)

            today_bars = self.alpaca.get_bars("SPY", "1Min", today_start.isoformat(), limit=1000)

            if not today_bars or 'bars' not in today_bars:
                return None

            current_volume = sum(b['v'] for b in today_bars['bars'])

            rvol = current_volume / avg_volume if avg_volume > 0 else None

            # Cache the result
            if rvol is not None:
                self._set_cache('spy_rvol', rvol)

            return rvol

        except Exception as e:
            logging.error(f"[REGIME] SPY RVOL calculation failed: {e}")
            return None

    def _calculate_adx(self, bars: List[Dict], period: int = 14) -> Optional[float]:
        """
        Calculate Average Directional Index (ADX) for trend strength.

        ADX measures trend strength on 0-100 scale:
        - <20: Weak/no trend (ranging)
        - 20-25: Emerging trend
        - 25-50: Strong trend
        - 50+: Very strong trend

        Args:
            bars: List of OHLC bars (must have 'h', 'l', 'c' keys)
            period: ADX period (default 14)

        Returns:
            ADX value or None if calculation fails
        """
        try:
            if len(bars) < period * 2:
                return None

            # Calculate True Range (TR) and Directional Movement (+DM, -DM)
            tr_values = []
            plus_dm = []
            minus_dm = []

            for i in range(1, len(bars)):
                high = bars[i]['h']
                low = bars[i]['l']
                prev_close = bars[i-1]['c']

                # True Range = max(high-low, abs(high-prev_close), abs(low-prev_close))
                tr = max(
                    high - low,
                    abs(high - prev_close),
                    abs(low - prev_close)
                )
                tr_values.append(tr)

                # Directional Movement
                high_diff = high - bars[i-1]['h']
                low_diff = bars[i-1]['l'] - low

                if high_diff > low_diff and high_diff > 0:
                    plus_dm.append(high_diff)
                    minus_dm.append(0)
                elif low_diff > high_diff and low_diff > 0:
                    plus_dm.append(0)
                    minus_dm.append(low_diff)
                else:
                    plus_dm.append(0)
                    minus_dm.append(0)

            if len(tr_values) < period:
                return None

            # Smooth TR, +DM, -DM using Wilder's smoothing (exponential moving average)
            def wilder_smooth(values, period):
                """Wilder's smoothing = EMA with alpha = 1/period"""
                smoothed = [sum(values[:period]) / period]  # First value is simple average
                for i in range(period, len(values)):
                    smoothed.append((smoothed[-1] * (period - 1) + values[i]) / period)
                return smoothed

            smoothed_tr = wilder_smooth(tr_values, period)
            smoothed_plus_dm = wilder_smooth(plus_dm, period)
            smoothed_minus_dm = wilder_smooth(minus_dm, period)

            # Calculate +DI and -DI (Directional Indicators)
            plus_di = [(dm / tr * 100) if tr > 0 else 0 for dm, tr in zip(smoothed_plus_dm, smoothed_tr)]
            minus_di = [(dm / tr * 100) if tr > 0 else 0 for dm, tr in zip(smoothed_minus_dm, smoothed_tr)]

            # Calculate DX (Directional Index)
            dx = []
            for plus, minus in zip(plus_di, minus_di):
                di_sum = plus + minus
                if di_sum > 0:
                    dx.append(abs(plus - minus) / di_sum * 100)
                else:
                    dx.append(0)

            # Smooth DX to get ADX
            if len(dx) < period:
                return None

            adx_values = wilder_smooth(dx, period)

            # Return the most recent ADX value
            return adx_values[-1] if adx_values else None

        except Exception as e:
            logging.error(f"[REGIME] ADX calculation failed: {e}")
            return None

    def _calculate_vwap_slope(self, symbol: str = "SPY", lookback_minutes: int = 30) -> Optional[float]:
        """
        Calculate VWAP slope to determine directional bias.

        Positive slope = bullish, Negative slope = bearish, Flat = range-bound

        Args:
            symbol: Symbol to calculate VWAP for (default SPY)
            lookback_minutes: How many minutes to look back for slope calculation

        Returns:
            VWAP slope (% change per minute) or None if calculation fails
        """
        try:
            # Get market open time
            try:
                from zoneinfo import ZoneInfo
                now_et = datetime.now(ZoneInfo("America/New_York"))
            except ImportError:
                import pytz
                now_et = datetime.now(pytz.timezone("America/New_York"))

            market_open_et = now_et.replace(hour=9, minute=30, second=0, microsecond=0)
            today_start = market_open_et.astimezone(timezone.utc)

            # Get 1-minute bars from market open
            bars_data = self.alpaca.get_bars(symbol, "1Min", today_start.isoformat(), limit=1000)
            if not bars_data or 'bars' not in bars_data or len(bars_data['bars']) < lookback_minutes:
                return None

            bars = bars_data['bars']

            # Calculate VWAP for last N minutes
            recent_bars = bars[-lookback_minutes:]
            vwap_values = []
            cum_volume = 0
            cum_vwap = 0

            for bar in recent_bars:
                typical_price = (bar['h'] + bar['l'] + bar['c']) / 3
                volume = bar['v']
                cum_vwap += typical_price * volume
                cum_volume += volume
                if cum_volume > 0:
                    vwap_values.append(cum_vwap / cum_volume)

            if len(vwap_values) < 2:
                return None

            # Calculate slope (linear regression would be more robust, but simple difference works)
            vwap_start = vwap_values[0]
            vwap_end = vwap_values[-1]

            if vwap_start == 0:
                return None

            # Slope as percentage change per minute
            slope = ((vwap_end - vwap_start) / vwap_start) / lookback_minutes * 100

            return slope

        except Exception as e:
            logging.error(f"[REGIME] VWAP slope calculation failed: {e}")
            return None

    def _count_vwap_crossovers(self, symbol: str = "SPY", lookback_minutes: int = 30) -> int:
        """
        Count how many times price crossed VWAP in the last N minutes.

        High crossover count = choppy/ranging market
        Low crossover count = trending market

        Args:
            symbol: Symbol to analyze (default SPY)
            lookback_minutes: How many minutes to look back

        Returns:
            Number of VWAP crossovers
        """
        try:
            # Get market open time
            try:
                from zoneinfo import ZoneInfo
                now_et = datetime.now(ZoneInfo("America/New_York"))
            except ImportError:
                import pytz
                now_et = datetime.now(pytz.timezone("America/New_York"))

            market_open_et = now_et.replace(hour=9, minute=30, second=0, microsecond=0)
            today_start = market_open_et.astimezone(timezone.utc)

            # Get 1-minute bars
            bars_data = self.alpaca.get_bars(symbol, "1Min", today_start.isoformat(), limit=1000)
            if not bars_data or 'bars' not in bars_data or len(bars_data['bars']) < lookback_minutes:
                return 0

            bars = bars_data['bars']
            recent_bars = bars[-lookback_minutes:]

            # Calculate cumulative VWAP and track crossovers
            cum_volume = 0
            cum_vwap_sum = 0
            crossovers = 0
            above_vwap = None  # Track previous state

            for bar in recent_bars:
                typical_price = (bar['h'] + bar['l'] + bar['c']) / 3
                volume = bar['v']
                cum_vwap_sum += typical_price * volume
                cum_volume += volume

                if cum_volume == 0:
                    continue

                vwap = cum_vwap_sum / cum_volume
                price = bar['c']

                # Check if price crossed VWAP
                current_above = price > vwap

                if above_vwap is not None and current_above != above_vwap:
                    crossovers += 1

                above_vwap = current_above

            return crossovers

        except Exception as e:
            logging.error(f"[REGIME] VWAP crossover count failed: {e}")
            return 0

    def detect_market_regime(self) -> RegimeMetrics:
        """
        Detect current market regime based on SPY characteristics.

        Regime Classification:
        - TREND: ADX > 25, VWAP slope strong, <2 crossovers in 30 min
        - RANGE_CHOP: ADX < 20 OR 3+ crossovers in 30 min
        - UNKNOWN: Unable to calculate or error state

        This function is called every 5 minutes to update regime state.

        Returns:
            RegimeMetrics with current regime and supporting metrics
        """
        try:
            # Get SPY 5-minute bars for ADX calculation
            end_time = datetime.now(timezone.utc)
            start_time = end_time - timedelta(hours=3)  # Get ~36 5-min bars

            bars_data = self.alpaca.get_bars("SPY", "5Min", start_time.isoformat(), limit=50)
            if not bars_data or 'bars' not in bars_data or len(bars_data['bars']) < 30:
                logging.warning("[REGIME] Insufficient data for regime detection")
                return RegimeMetrics(
                    regime=MarketRegime.UNKNOWN,
                    adx=0.0,
                    vwap_slope=0.0,
                    vwap_crossovers=0,
                    timestamp=datetime.now(timezone.utc)
                )

            bars = bars_data['bars']

            # Calculate ADX (trend strength)
            adx = self._calculate_adx(bars, period=14)
            if adx is None:
                adx = 0.0

            # Calculate VWAP slope (directional bias)
            vwap_slope = self._calculate_vwap_slope(symbol="SPY", lookback_minutes=30)
            if vwap_slope is None:
                vwap_slope = 0.0

            # Count VWAP crossovers (chop indicator)
            vwap_crossovers = self._count_vwap_crossovers(symbol="SPY", lookback_minutes=30)

            # Determine regime
            # TREND: Strong directional movement with follow-through
            if adx > 25 and vwap_crossovers <= 2:
                regime = MarketRegime.TREND
            # RANGE_CHOP: Weak trend or excessive back-and-forth
            elif adx < 20 or vwap_crossovers >= 3:
                regime = MarketRegime.RANGE_CHOP
            # Borderline: Default to RANGE_CHOP (conservative approach)
            else:
                regime = MarketRegime.RANGE_CHOP

            metrics = RegimeMetrics(
                regime=regime,
                adx=adx,
                vwap_slope=vwap_slope,
                vwap_crossovers=vwap_crossovers,
                timestamp=datetime.now(timezone.utc)
            )

            logging.info(f"[REGIME] Detected: {regime.value.upper()} | "
                        f"ADX={adx:.1f}, VWAP_slope={vwap_slope:.4f}%/min, "
                        f"Crossovers={vwap_crossovers}")

            return metrics

        except Exception as e:
            logging.error(f"[REGIME] Detection failed: {e}")
            import traceback
            logging.error(traceback.format_exc())
            return RegimeMetrics(
                regime=MarketRegime.UNKNOWN,
                adx=0.0,
                vwap_slope=0.0,
                vwap_crossovers=0,
                timestamp=datetime.now(timezone.utc)
            )

    def discover_dynamic_universe(self):
        """
        Discover high relative volume stocks beyond core universe.

        Scans for stocks meeting criteria:
        - High relative volume (2.5x+)
        - Adequate price range ($10-$1000)
        - High dollar volume ($50M+ daily)
        - Not already in core universe

        NOTE: This implementation does NOT handle pagination. Polygon snapshots API
        may return partial results if there are >250 tickers. For most use cases this
        is acceptable as we only need top movers, but be aware the scan may miss some
        high-RVOL stocks if they're not in the first page of results.

        To fix: Follow response.next_url in a loop until None, accumulating all tickers.
        """
        if not self.enable_dynamic_universe:
            return

        # Check if we should scan (throttle to avoid excessive API calls)
        now = datetime.now(timezone.utc)
        if self.last_universe_scan:
            elapsed = (now - self.last_universe_scan).total_seconds()
            if elapsed < self.dynamic_scan_interval:
                return

        self.last_universe_scan = now

        try:
            logging.info("[DYNAMIC] Scanning for high RVOL movers...")

            # Use Polygon snapshots API to get all active stocks
            # WARNING: Does not handle pagination - may miss tickers beyond first page
            url = f"{self.polygon.base_url}/v2/snapshot/locale/us/markets/stocks/tickers"

            response = self.polygon.session.get(
                url,
                params={"apiKey": self.polygon.api_key},
                timeout=15
            )

            if response.status_code != 200:
                logging.warning(f"[DYNAMIC] Snapshot fetch failed: {response.status_code}")
                return

            data = response.json()
            tickers = data.get("tickers", [])

            # Log if pagination exists (indicates we may be missing data)
            next_url = data.get("next_url")
            if next_url:
                logging.warning(f"[DYNAMIC] Pagination detected - results may be incomplete. "
                              f"Found {len(tickers)} tickers in first page, but more exist.")

            discovered = []
            for ticker_data in tickers:
                try:
                    symbol = ticker_data.get("ticker")
                    if not symbol:
                        continue

                    # FIXED: Filter out invalid/problematic symbols
                    # Only accept simple ticker symbols (letters only, 1-5 chars)
                    # This excludes: Class shares (BRK.A), warrants (ABCW), preferred (ABCpA), etc.
                    if not symbol.isalpha() or len(symbol) > 5:
                        continue

                    # Skip if already in core universe
                    if symbol in self.core_universe:
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
                    if last_price < self.dynamic_min_price or last_price > self.dynamic_max_price:
                        continue

                    # Volume check
                    current_volume = day_data.get("v", 0)
                    prev_volume = prev_day.get("v", 1)

                    if prev_volume == 0:
                        continue

                    rel_vol = current_volume / prev_volume

                    # RVOL filter
                    if rel_vol < self.dynamic_min_rvol:
                        continue

                    # Dollar volume filter (liquidity)
                    dollar_volume = current_volume * last_price
                    if dollar_volume < self.dynamic_min_volume_usd:
                        continue

                    discovered.append({
                        "symbol": symbol,
                        "price": last_price,
                        "rvol": rel_vol,
                        "dollar_volume": dollar_volume
                    })

                except Exception as e:
                    logging.debug(f"[DYNAMIC] Error processing ticker: {e}")
                    continue

            # Sort by RVOL descending, take top N
            discovered.sort(key=lambda x: x["rvol"], reverse=True)
            top_movers = discovered[:self.dynamic_max_size]

            # Update dynamic universe
            new_symbols = set([m["symbol"] for m in top_movers])
            added = new_symbols - self.dynamic_universe
            removed = self.dynamic_universe - new_symbols

            self.dynamic_universe = new_symbols

            if added:
                logging.info(f"[DYNAMIC] Added {len(added)} movers: {sorted(added)}")
            if removed:
                logging.info(f"[DYNAMIC] Removed {len(removed)} movers: {sorted(removed)}")

            # Log top 5 movers
            for mover in top_movers[:5]:
                logging.info(f"[DYNAMIC] {mover['symbol']}: ${mover['price']:.2f} "
                           f"RVOL={mover['rvol']:.1f}x vol=${mover['dollar_volume']/1e6:.1f}M")

        except Exception as e:
            logging.error(f"[DYNAMIC] Universe discovery failed: {e}")

    def get_active_universe(self) -> List[str]:
        """Get combined universe (core + dynamic)"""
        if self.enable_dynamic_universe:
            combined = list(set(self.core_universe) | self.dynamic_universe)
            return combined
        return self.core_universe

    def scan_for_setups(self) -> List[Dict]:
        """Scan all strategies for trading setups (session-aware)"""
        all_signals = []

        # Get active universe (core + dynamic)
        universe = self.get_active_universe()

        if not universe:
            logging.warning("[SCAN] No symbols in universe")
            return all_signals

        # Get current market session
        session = self.get_market_session()

        logging.info(f"[SCAN] Scanning {len(universe)} symbols ({len(self.core_universe)} core + {len(self.dynamic_universe)} dynamic) | Session: {session.value}")

        # Momentum breakout signals (RTH ONLY - requires gap data, volume ratios, multi-timeframe)
        if session == MarketSession.RTH:
            try:
                momentum_signals = self.momentum_strategy.scan_for_signals(universe)
                for sig in momentum_signals:
                    sig['strategy'] = StrategyType.MOMENTUM_BREAKOUT
                    sig['session'] = session  # Tag with session
                    all_signals.append(sig)
            except Exception as e:
                logging.error(f"[SCAN] Momentum scan failed: {e}")
        else:
            logging.debug(f"[SCAN] Skipping Momentum strategy during {session.value} (RTH only)")

        # VWAP scalping signals (ALL SESSIONS if extended hours enabled)
        extended_config = self.config.get('extended_hours', {})
        extended_enabled = extended_config.get('enabled', False)

        # Allow VWAP during extended hours if enabled
        if session == MarketSession.RTH or (extended_enabled and session in [MarketSession.PRE, MarketSession.AFTER]):
            try:
                vwap_signals = self.vwap_strategy.scan_for_signals(universe)
                for sig in vwap_signals:
                    sig['strategy'] = StrategyType.VWAP_SCALPING
                    sig['session'] = session  # Tag with session
                    all_signals.append(sig)
            except Exception as e:
                logging.error(f"[SCAN] VWAP scan failed: {e}")
        else:
            logging.debug(f"[SCAN] Skipping VWAP strategy during {session.value} (extended hours not enabled)")

        # Sort by quality score (highest first)
        all_signals.sort(key=lambda x: x['quality_score'], reverse=True)

        return all_signals

    def execute_entry(self, signal: Dict) -> bool:
        """Execute entry order with bracket"""
        symbol = signal['symbol']
        strategy = signal['strategy']

        try:
            # Get account value for position sizing
            account = self.alpaca.get_account()
            if not account:
                logging.error(f"[ENTRY] Failed to get account info for {symbol}")
                return False

            account_value = float(account['equity'])
            buying_power = float(account.get('buying_power', account_value))

            # FIXED: Calculate position size with buying power validation
            qty = self.risk_manager.calculate_position_size(
                account_value,
                signal['entry_price'],
                signal['stop_loss'],
                signal['quality_score'],
                buying_power
            )

            if qty == 0:
                logging.warning(f"[ENTRY] Position size = 0 for {symbol}, skipping")
                return False

            # Additional safety check - verify position cost doesn't exceed buying power
            position_cost = qty * signal['entry_price']
            if position_cost > buying_power:
                logging.error(f"[ENTRY] Position cost ${position_cost:.2f} exceeds buying power ${buying_power:.2f}, skipping {symbol}")
                return False

            # FIXED: Validate stop loss and take profit values before submitting order
            if signal['stop_loss'] >= signal['entry_price']:
                logging.error(f"[ENTRY] Invalid stop loss for {symbol}: ${signal['stop_loss']:.2f} >= ${signal['entry_price']:.2f}")
                return False

            if signal['take_profit'] <= signal['entry_price']:
                logging.error(f"[ENTRY] Invalid take profit for {symbol}: ${signal['take_profit']:.2f} <= ${signal['entry_price']:.2f}")
                return False

            # Validate minimum risk/reward ratio (at least 1:1)
            risk = signal['entry_price'] - signal['stop_loss']
            reward = signal['take_profit'] - signal['entry_price']
            if risk <= 0 or reward <= 0:
                logging.error(f"[ENTRY] Invalid risk/reward for {symbol}: risk=${risk:.2f}, reward=${reward:.2f}")
                return False

            risk_reward_ratio = reward / risk
            if risk_reward_ratio < 1.0:
                logging.warning(f"[ENTRY] Low risk/reward for {symbol}: {risk_reward_ratio:.2f}:1 (< 1:1)")
                # Don't block, just warn - strategy may have valid reason

            # Get current session for order submission
            session = signal.get('session', MarketSession.RTH)

            # Adjust position size for extended hours (50% of RTH size)
            if session in [MarketSession.PRE, MarketSession.AFTER]:
                original_qty = qty
                qty = max(1, qty // 2)  # 50% size for extended hours
                logging.info(f"[ENTRY] Extended hours position size reduced: {original_qty} -> {qty} shares")

            # Submit bracket order (RTH) or limit order (extended hours)
            order = self.alpaca.submit_bracket_order(
                symbol,
                qty,
                "buy",
                signal['stop_loss'],
                signal['take_profit'],
                session=session
            )

            if not order:
                logging.error(f"[ENTRY] Order failed for {symbol}")
                return False

            # Create position tracking
            position = Position(
                symbol=symbol,
                strategy=strategy,
                entry_time=datetime.now(timezone.utc),
                entry_price=signal['entry_price'],
                quantity=qty,
                stop_loss=signal['stop_loss'],
                take_profit=signal['take_profit'],
                state=PositionState.ENTRY_SUBMITTED,
                order_id=order['id'],
                max_price=signal['entry_price'],
                session=session,  # Track session
                regime=self.state_manager.state.current_regime  # Track market regime when opened
            )

            # For extended hours, create virtual bracket after position filled
            # (will be added in _verify_order_fill when order fills)

            self.state_manager.state.positions[symbol] = position
            self.state_manager.save_state()

            self.alert_manager.send_alert(
                'info',
                f"Entry Order Submitted: {symbol}",
                f"Strategy: {strategy.value}\n"
                f"Regime: {self.state_manager.state.current_regime.value.upper()}\n"
                f"Qty: {qty}\n"
                f"Entry: ${signal['entry_price']:.2f}\n"
                f"Stop: ${signal['stop_loss']:.2f}\n"
                f"Target: ${signal['take_profit']:.2f}\n"
                f"Quality: {signal['quality_score']:.2f}"
            )

            logging.info(f"[ENTRY] {symbol} order submitted: {qty} @ ${signal['entry_price']:.2f}")
            return True

        except Exception as e:
            logging.error(f"[ENTRY] Exception for {symbol}: {e}")
            return False

    def manage_positions(self):
        """Monitor and manage active positions"""
        state = self.state_manager.state

        # Check and execute virtual brackets for extended hours positions
        try:
            self.virtual_bracket_manager.check_and_execute_brackets()
        except Exception as e:
            logging.error(f"[VIRTUAL BRACKET] Error checking brackets: {e}")

        for symbol, position in list(state.positions.items()):
            try:
                # Handle positions in ENTRY_SUBMITTED state - verify fill
                if position.state == PositionState.ENTRY_SUBMITTED:
                    self._verify_order_fill(symbol, position)
                    continue

                # Update position state from Alpaca
                alpaca_position = self.alpaca.get_position(symbol)

                if alpaca_position:
                    # Position still open
                    current_price = float(alpaca_position['current_price'])
                    unrealized_pl = float(alpaca_position.get('unrealized_pl', 0))
                    unrealized_plpc = float(alpaca_position.get('unrealized_plpc', 0))

                    # FIXED: Emergency stop loss - force close if loss exceeds 5% (safety net)
                    # Only trigger if not already exiting to prevent duplicate close attempts
                    if position.state != PositionState.EXIT_SUBMITTED and unrealized_plpc < -0.05:  # -5% loss
                        print(f"\n[{datetime.now().strftime('%H:%M:%S')}] 🚨 EMERGENCY STOP: {symbol} at {unrealized_plpc*100:.2f}% loss!")
                        logging.critical(f"[EMERGENCY] {symbol} hit emergency stop loss: {unrealized_plpc*100:.2f}% loss, forcing close")

                        # Set state BEFORE making API call to prevent duplicate attempts
                        with self.state_manager.lock:
                            position.state = PositionState.EXIT_SUBMITTED
                            self.state_manager.save_state()

                        try:
                            self.alpaca.close_position(symbol)
                            self.alert_manager.send_alert(
                                'critical',
                                f'Emergency Stop Loss: {symbol}',
                                f'Position hit -5% emergency stop\n'
                                f'Unrealized P&L: ${unrealized_pl:.2f} ({unrealized_plpc*100:.2f}%)\n'
                                f'Entry: ${position.entry_price:.2f}, Current: ${current_price:.2f}'
                            )
                        except Exception as close_err:
                            logging.error(f"[EMERGENCY] Failed to close {symbol}: {close_err}")

                    # Update max price for tracking
                    if current_price > position.max_price:
                        position.max_price = current_price

                    # TRAILING STOP: Update and check trailing stop for VWAP positions
                    if position.strategy == StrategyType.VWAP_SCALPING and position.state == PositionState.HOLDING:
                        # Update trailing stop (moves stop up as price rises)
                        new_stop = self.vwap_strategy.update_trailing_stop(position, current_price)
                        if new_stop:
                            logging.debug(f"[TRAILING STOP] {symbol} trail updated to ${new_stop:.2f}")

                        # Check if trailing stop was hit
                        if position.trail_stop and current_price <= position.trail_stop:
                            logging.info(f"[TRAILING STOP] {symbol} hit trailing stop at ${position.trail_stop:.2f}, current: ${current_price:.2f}")
                            try:
                                with self.state_manager.lock:
                                    position.state = PositionState.EXIT_SUBMITTED
                                    self.state_manager.save_state()
                                self.alpaca.close_position(symbol)
                                continue  # Skip further processing for this position
                            except Exception as trail_err:
                                logging.error(f"[TRAILING STOP] Failed to close {symbol}: {trail_err}")

                        # TIME EXIT: Check max hold time for VWAP positions
                        if self.vwap_strategy.should_time_exit(position):
                            logging.info(f"[TIME EXIT] {symbol} exceeded max hold time ({self.vwap_strategy.max_hold_minutes} min)")
                            try:
                                with self.state_manager.lock:
                                    position.state = PositionState.EXIT_SUBMITTED
                                    self.state_manager.save_state()
                                self.alpaca.close_position(symbol)
                                continue  # Skip further processing for this position
                            except Exception as time_err:
                                logging.error(f"[TIME EXIT] Failed to close {symbol}: {time_err}")

                    # FIXED: Thread-safe state update
                    if position.state != PositionState.HOLDING:
                        with self.state_manager.lock:
                            position.state = PositionState.HOLDING
                        logging.info(f"[POSITION] {symbol} confirmed HOLDING")

                else:
                    # Position closed
                    if position.state == PositionState.HOLDING or position.state == PositionState.EXIT_SUBMITTED:
                        self._handle_position_close(symbol, position)
                    else:
                        # Order was cancelled or rejected before fill
                        logging.warning(f"[POSITION] {symbol} order never filled, removing")
                        del state.positions[symbol]

                self.state_manager.save_state()

            except Exception as e:
                logging.error(f"[POSITION] Management error for {symbol}: {e}")

    def _verify_order_fill(self, symbol: str, position: Position):
        """Verify if entry order has been filled and update position state"""
        try:
            # Check order status
            order = self.alpaca.get_order(position.order_id)

            if not order:
                logging.warning(f"[POSITION] Order {position.order_id} not found for {symbol}")
                # Clean up orphaned position after 5 minutes
                elapsed = (datetime.now(timezone.utc) - position.entry_time).total_seconds()
                if elapsed > 300:  # 5 minutes
                    logging.error(f"[POSITION] Removing orphaned position {symbol} after 5 minutes")
                    del self.state_manager.state.positions[symbol]
                    self.state_manager.save_state()
                return

            status = order.get('status')

            if status == 'filled':
                # Order filled - update position with actual fill data
                filled_price = float(order.get('filled_avg_price', position.entry_price))
                filled_qty = int(order.get('filled_qty', position.quantity))

                # Update position with actual fill data
                position.entry_price = filled_price
                position.quantity = filled_qty
                position.state = PositionState.HOLDING

                logging.info(f"[POSITION] {symbol} ORDER FILLED | "
                           f"Qty: {filled_qty} @ ${filled_price:.2f} | "
                           f"Strategy: {position.strategy.value}")

                # Check if bracket orders were created (RTH only)
                if 'legs' in order and order['legs']:
                    for leg in order['legs']:
                        leg_id = leg.get('id')
                        leg_type = leg.get('type')
                        if leg_type == 'stop_loss':
                            position.bracket_ids['stop_loss'] = leg_id
                        elif leg_type == 'limit':
                            position.bracket_ids['take_profit'] = leg_id

                # For extended hours positions, create virtual bracket (broker doesn't support bracket orders)
                if position.session in [MarketSession.PRE, MarketSession.AFTER]:
                    self.virtual_bracket_manager.add_bracket(symbol, position)
                    logging.info(f"[VIRTUAL BRACKET] Created for {symbol} {position.session.value} position | "
                               f"Stop: ${position.stop_loss:.2f} Target: ${position.take_profit:.2f}")

                self.state_manager.save_state()

            elif status in ['cancelled', 'rejected', 'expired']:
                # Order failed - remove position
                logging.warning(f"[POSITION] {symbol} order {status}, removing position")
                del self.state_manager.state.positions[symbol]
                self.state_manager.save_state()

            elif status in ['pending_new', 'accepted', 'new', 'partially_filled']:
                # FIXED: Handle partial fills - update position with actual filled quantity
                if status == 'partially_filled':
                    filled_qty = int(order.get('filled_qty', 0))
                    if filled_qty > 0:
                        # Update position with partial fill
                        with self.state_manager.lock:
                            position.quantity = filled_qty
                            position.state = PositionState.HOLDING
                            filled_price = float(order.get('filled_avg_price', position.entry_price))
                            position.entry_price = filled_price
                            self.state_manager.save_state()

                        logging.info(f"[POSITION] {symbol} PARTIALLY FILLED | "
                                   f"Qty: {filled_qty} @ ${filled_price:.2f} (requested: {order.get('qty', position.quantity)})")

                        # Check if bracket orders were created for partial fill (RTH only)
                        if 'legs' in order and order['legs']:
                            for leg in order['legs']:
                                leg_id = leg.get('id')
                                leg_type = leg.get('type')
                                if leg_type == 'stop_loss':
                                    position.bracket_ids['stop_loss'] = leg_id
                                elif leg_type == 'limit':
                                    position.bracket_ids['take_profit'] = leg_id

                        # For extended hours positions, create virtual bracket
                        if position.session in [MarketSession.PRE, MarketSession.AFTER]:
                            self.virtual_bracket_manager.add_bracket(symbol, position)
                            logging.info(f"[VIRTUAL BRACKET] Created for {symbol} {position.session.value} partial fill | "
                                       f"Stop: ${position.stop_loss:.2f} Target: ${position.take_profit:.2f}")

                # Still waiting for full fill or in pending state
                elapsed = (datetime.now(timezone.utc) - position.entry_time).total_seconds()

                # Cancel if order pending too long (2 minutes for market orders, 5 for limit)
                # Don't cancel partial fills - let them complete or manage as HOLDING position
                timeout = 120 if order.get('type') == 'market' else 300

                if status != 'partially_filled' and elapsed > timeout:
                    logging.warning(f"[POSITION] {symbol} order timeout after {elapsed:.0f}s, cancelling")
                    try:
                        self.alpaca.cancel_order(position.order_id)
                        del self.state_manager.state.positions[symbol]
                        self.state_manager.save_state()
                    except Exception as cancel_err:
                        logging.error(f"[POSITION] Failed to cancel order {position.order_id}: {cancel_err}")

            else:
                logging.debug(f"[POSITION] {symbol} order status: {status}")

        except Exception as e:
            logging.error(f"[POSITION] Error verifying order fill for {symbol}: {e}", exc_info=True)

    def _handle_position_close(self, symbol: str, position: Position):
        """Handle position closure and P&L calculation"""
        try:
            # Get filled orders for this position
            # FIXED: Use 'after' parameter to only fetch orders from today's market open
            # This prevents missing orders on active days when >100 fills occur
            try:
                from zoneinfo import ZoneInfo
                now_et = datetime.now(ZoneInfo("America/New_York"))
            except ImportError:
                import pytz
                now_et = datetime.now(pytz.timezone("America/New_York"))

            # Today's market open in ET (or earlier if before open)
            market_open_et = now_et.replace(hour=0, minute=0, second=0, microsecond=0)
            after_time = market_open_et.astimezone(timezone.utc).isoformat()

            all_filled_orders = self.alpaca.get_orders(
                status="filled",
                limit=500,  # Increased limit for safety
                after=after_time  # Only fetch orders from today
            )

            if not all_filled_orders:
                logging.warning(f"[POSITION] No filled orders found for {symbol}")
                # Still remove position to prevent stuck state
                with self.state_manager.lock:
                    del self.state_manager.state.positions[symbol]
                    self.state_manager.save_state()
                return

            # Filter for this symbol only
            filled_orders = [o for o in all_filled_orders if o.get('symbol') == symbol]

            if not filled_orders:
                logging.warning(f"[POSITION] No filled orders found for {symbol} after filtering")
                # Still remove position to prevent stuck state
                with self.state_manager.lock:
                    del self.state_manager.state.positions[symbol]
                    self.state_manager.save_state()
                return

            # Find entry and exit orders
            entry_order = None
            exit_order = None

            for order in filled_orders:
                order_id = order.get('id')
                side = order.get('side')

                # Match entry order
                if order_id == position.order_id or order_id in position.bracket_ids.values():
                    if side == 'buy':
                        entry_order = order
                    elif side == 'sell':
                        exit_order = order

            if not entry_order or not exit_order:
                logging.warning(f"[POSITION] Incomplete order data for {symbol} - entry: {entry_order is not None}, exit: {exit_order is not None}")
                # Still remove position to prevent stuck state
                del self.state_manager.state.positions[symbol]
                self.state_manager.save_state()
                return

            # Calculate realized P&L
            entry_price = float(entry_order.get('filled_avg_price', position.entry_price))
            exit_price = float(exit_order.get('filled_avg_price', 0))
            qty = int(exit_order.get('filled_qty', position.quantity))

            realized_pnl = (exit_price - entry_price) * qty
            pnl_pct = ((exit_price - entry_price) / entry_price) * 100

            # Determine exit reason
            # Check within 1% tolerance for take profit/stop loss (accounts for slippage)
            tp_tolerance = position.take_profit * 0.01
            sl_tolerance = position.stop_loss * 0.01

            if exit_price >= (position.take_profit - tp_tolerance):
                exit_reason = "Take Profit"
            elif exit_price <= (position.stop_loss + sl_tolerance):
                exit_reason = "Stop Loss"
            else:
                exit_reason = "Manual Close"

            # Update state metrics (thread-safe)
            with self.state_manager.lock:
                self.state_manager.state.daily_pnl += realized_pnl
                self.state_manager.state.daily_trades += 1
                self.state_manager.state.total_trades += 1

                # Track consecutive losses
                if realized_pnl < 0:
                    self.state_manager.state.consecutive_losses += 1
                else:
                    self.state_manager.state.consecutive_losses = 0

                # Remove position
                del self.state_manager.state.positions[symbol]

            # Save state atomically
            self.state_manager.save_state()

            # Log detailed exit
            logging.info(f"[POSITION] {symbol} CLOSED | "
                        f"Strategy: {position.strategy.value} | "
                        f"Entry: ${entry_price:.2f} | "
                        f"Exit: ${exit_price:.2f} | "
                        f"Qty: {qty} | "
                        f"P&L: ${realized_pnl:.2f} ({pnl_pct:+.2f}%) | "
                        f"Reason: {exit_reason} | "
                        f"Daily P&L: ${self.state_manager.state.daily_pnl:.2f}")

            # Print to console with color-coded result
            result_emoji = "✅" if realized_pnl > 0 else "❌"
            print(f"\n[{datetime.now().strftime('%H:%M:%S')}] {result_emoji} {symbol} CLOSED | "
                  f"{exit_reason} | ${entry_price:.2f} → ${exit_price:.2f} | "
                  f"P&L: ${realized_pnl:+.2f} ({pnl_pct:+.2f}%) | "
                  f"Daily: ${self.state_manager.state.daily_pnl:+.2f}")

            # Send alert for significant P&L
            if abs(realized_pnl) >= 100:  # Alert on $100+ moves
                alert_type = 'success' if realized_pnl > 0 else 'warning'
                self.alert_manager.send_alert(
                    alert_type,
                    f"Position Closed: {symbol}",
                    f"Strategy: {position.strategy.value}\n"
                    f"P&L: ${realized_pnl:.2f} ({pnl_pct:+.2f}%)\n"
                    f"Exit Reason: {exit_reason}\n"
                    f"Daily P&L: ${self.state_manager.state.daily_pnl:.2f}\n"
                    f"Daily Trades: {self.state_manager.state.daily_trades}"
                )

            # Check consecutive losses
            if self.state_manager.state.consecutive_losses >= self.risk_manager.max_consecutive_losses:
                self.alert_manager.send_alert(
                    'critical',
                    f"Max Consecutive Losses Hit: {self.state_manager.state.consecutive_losses}",
                    "Trading paused. Review strategy and conditions before resuming."
                )
                logging.error(f"[RISK] Max consecutive losses hit: {self.state_manager.state.consecutive_losses}")

        except Exception as e:
            logging.error(f"[POSITION] Error handling close for {symbol}: {e}", exc_info=True)
            # Emergency cleanup - remove position to prevent stuck state
            try:
                if symbol in self.state_manager.state.positions:
                    del self.state_manager.state.positions[symbol]
                    self.state_manager.save_state()
                    logging.warning(f"[POSITION] Emergency cleanup completed for {symbol}")
            except Exception as cleanup_error:
                logging.error(f"[POSITION] Emergency cleanup failed for {symbol}: {cleanup_error}")

    def start(self):
        """Start the trading bot"""
        self.running = True

        # Print startup banner
        print("=" * 70)
        print("MASTER TRADING BOT - STARTING")
        print("=" * 70)
        print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Mode: {'PAPER TRADING' if self.config['alpaca'].get('paper', True) else 'LIVE TRADING'}")
        print(f"Strategies: Momentum Breakout, VWAP Scalping")
        print(f"Core Universe: {len(self.core_universe)} symbols")
        print(f"Dynamic Discovery: {'Enabled' if self.enable_dynamic_universe else 'Disabled'}")
        print("=" * 70)
        print()

        logging.info("[MASTER BOT] Starting main trading loop...")
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Initializing bot systems...")

        # Reconcile positions on startup - critical for preventing state corruption
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Reconciling positions with broker...")
        if not self.reconcile_positions_on_startup():
            logging.error("[MASTER BOT] Position reconciliation failed - review state before continuing")
            print(f"[{datetime.now().strftime('%H:%M:%S')}] WARNING: Position reconciliation failed!")

        print(f"[{datetime.now().strftime('%H:%M:%S')}] Sending startup alert...")
        self.alert_manager.send_alert('success', 'Master Bot Started',
                                     f"Strategies: Momentum Breakout, VWAP Scalping\n"
                                     f"Core Universe: {len(self.core_universe)} symbols\n"
                                     f"Dynamic Discovery: {'Enabled' if self.enable_dynamic_universe else 'Disabled'}\n"
                                     f"Tracked Positions: {len(self.state_manager.state.positions)}")

        print(f"[{datetime.now().strftime('%H:%M:%S')}] Entering main trading loop...")
        import sys
        sys.stdout.flush()  # Ensure output appears immediately

        scan_interval = self.config.get('scan_interval_seconds', 60)

        # Track last market check message time to avoid spam
        last_market_msg_time = None

        while self.running:
            try:
                # Check kill switch
                if self.check_kill_switch():
                    self.alert_manager.send_alert('critical', 'KILL SWITCH ACTIVATED',
                                                  'Bot stopped immediately')
                    break

                # Check if market is open
                if not self.is_market_open():
                    # Print message every minute when market is closed
                    now = datetime.now(timezone.utc)
                    if last_market_msg_time is None or (now - last_market_msg_time).total_seconds() >= 60:
                        print(f"[{datetime.now().strftime('%H:%M:%S')}] Market closed - waiting for market to open...")
                        logging.info("[MARKET] Market closed, waiting 60 seconds before next check...")
                        last_market_msg_time = now
                    time.sleep(60)  # Check every minute when market is closed
                    continue

                # Market is open - reset message timer and print status
                if last_market_msg_time is not None:
                    # First time market opened since we started waiting
                    print(f"[{datetime.now().strftime('%H:%M:%S')}] Market is now open - starting to scan for setups...")
                    logging.info("[MARKET] Market opened, beginning trading operations")
                last_market_msg_time = None

                # Reset daily metrics
                self.state_manager.reset_daily_metrics()

                # Check market regime (SPY RVOL filter)
                regime_ok, regime_msg = self.check_market_regime()
                if not regime_ok:
                    print(f"[{datetime.now().strftime('%H:%M:%S')}] ⚠️  TRADING PAUSED: {regime_msg}")
                    logging.warning(f"[REGIME] Trading paused: {regime_msg}")
                    time.sleep(scan_interval)
                    continue

                # Detect market regime (TREND vs RANGE_CHOP) - updated every 5 minutes
                now_utc = datetime.now(timezone.utc)
                should_update_regime = (
                    state.last_regime_check is None or
                    (now_utc - state.last_regime_check).total_seconds() >= 300  # 5 minutes
                )

                if should_update_regime:
                    regime_metrics = self.detect_market_regime()
                    previous_regime = state.current_regime
                    state.current_regime = regime_metrics.regime
                    state.last_regime_check = now_utc
                    self.state_manager.save_state()

                    # Log regime change
                    if previous_regime != regime_metrics.regime:
                        logging.info(f"[REGIME] CHANGED: {previous_regime.value.upper()} → {regime_metrics.regime.value.upper()}")
                        print(f"[{datetime.now().strftime('%H:%M:%S')}] Regime changed: {previous_regime.value} → {regime_metrics.regime.value}")
                else:
                    logging.debug(f"[REGIME] Current: {state.current_regime.value.upper()} (last check {(now_utc - state.last_regime_check).total_seconds():.0f}s ago)")

                # Get account info
                account = self.alpaca.get_account()
                if not account:
                    logging.error("[ACCOUNT] Failed to get account info")
                    time.sleep(scan_interval)
                    continue

                account_value = float(account['equity'])
                state = self.state_manager.state

                # Check risk limits
                risk_metrics = self.risk_manager.get_risk_metrics(
                    state.positions,
                    account_value,
                    state.daily_pnl
                )

                if risk_metrics.max_daily_loss_hit:
                    self.alert_manager.send_alert('critical', 'MAX DAILY LOSS HIT',
                                                  f"Daily loss: ${state.daily_pnl:.2f}\n"
                                                  f"Trading stopped for today")
                    time.sleep(300)  # Wait 5 minutes before checking again
                    continue

                # Check circuit breaker
                if self.risk_manager.check_circuit_breaker(account_value, state.daily_pnl):
                    if not state.circuit_breaker_active:
                        state.circuit_breaker_active = True
                        state.cooldown_until = datetime.now(timezone.utc) + timedelta(hours=1)
                        self.state_manager.save_state()

                        self.alert_manager.send_alert('critical', 'CIRCUIT BREAKER TRIGGERED',
                                                      f"Drawdown: {risk_metrics.drawdown_pct*100:.1f}%\n"
                                                      f"Cooldown: 1 hour")

                if state.circuit_breaker_active:
                    if state.cooldown_until and datetime.now(timezone.utc) < state.cooldown_until:
                        logging.warning("[CIRCUIT BREAKER] In cooldown period")
                        time.sleep(scan_interval)
                        continue
                    else:
                        state.circuit_breaker_active = False
                        state.cooldown_until = None
                        self.state_manager.save_state()
                        logging.info("[CIRCUIT BREAKER] Cooldown ended, resuming trading")

                # Discover dynamic universe (throttled to every 5 minutes)
                self.discover_dynamic_universe()

                # Manage existing positions
                self.manage_positions()

                # Check if we can open new positions
                current_positions = len([p for p in state.positions.values()
                                       if p.state == PositionState.HOLDING])

                can_trade, trade_msg = self.risk_manager.can_open_position(
                    current_positions,
                    state.daily_trades,
                    state.consecutive_losses
                )

                if not can_trade:
                    logging.info(f"[RISK] Cannot open new positions: {trade_msg}")
                    time.sleep(scan_interval)
                    continue

                # Scan for new setups
                signals = self.scan_for_setups()

                if signals:
                    logging.info(f"[SCAN] Found {len(signals)} signals")
                    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] 🎯 Found {len(signals)} trading signal{'s' if len(signals) > 1 else ''}")

                    # Show signal details
                    for i, sig in enumerate(signals[:3], 1):  # Show top 3
                        print(f"  {i}. {sig['symbol']:6s} | {sig['strategy'].value:20s} | Quality: {sig['quality_score']:.2f}")

                    # Execute top signal (if not already in position)
                    for signal in signals:
                        if signal['symbol'] not in state.positions:
                            logging.info(f"[SIGNAL] {signal['symbol']}: {signal['strategy'].value} "
                                       f"quality={signal['quality_score']:.2f}")
                            print(f"[{datetime.now().strftime('%H:%M:%S')}] 📈 Entering {signal['symbol']} ({signal['strategy'].value})...")

                            # Execute entry
                            # FIXED: daily_trades is incremented in _handle_position_close() only (count completed round-trips)
                            if self.execute_entry(signal):
                                print(f"[{datetime.now().strftime('%H:%M:%S')}] ✅ Entry order submitted for {signal['symbol']}")
                                break  # Only enter one position per cycle
                else:
                    # No signals - print status so user knows bot is working
                    session = self.get_market_session()
                    session_str = f"[{session.value.upper()}]" if session != MarketSession.RTH else ""

                    print(f"[{datetime.now().strftime('%H:%M:%S')}] {session_str} "
                          f"Scanning... | Positions: {current_positions}/{self.config['risk']['max_concurrent_positions']} | "
                          f"Daily: {state.daily_trades} trades, ${state.daily_pnl:+.2f} P&L")

                # Sleep until next scan
                time.sleep(scan_interval)

            except KeyboardInterrupt:
                logging.info("[MAIN LOOP] Keyboard interrupt")
                break
            except Exception as e:
                logging.error(f"[MAIN LOOP] Error: {e}")
                logging.error(traceback.format_exc())
                time.sleep(scan_interval)

    def stop(self):
        """Graceful shutdown"""
        logging.info("[MASTER BOT] Shutting down...")
        self.running = False

        # Close all positions if configured
        if self.config.get('close_all_on_exit', False):
            logging.info("[SHUTDOWN] Closing all positions...")
            for symbol in list(self.state_manager.state.positions.keys()):
                try:
                    self.alpaca.close_position(symbol)
                    logging.info(f"[SHUTDOWN] Closed {symbol}")
                except Exception as e:
                    logging.error(f"[SHUTDOWN] Failed to close {symbol}: {e}")

        self.state_manager.save_state()
        self.alert_manager.send_alert('info', 'Master Bot Stopped', 'Graceful shutdown complete')
        logging.info("[MASTER BOT] Stopped")

# ============================================================================
# ENTRY POINT
# ============================================================================

def main():
    parser = argparse.ArgumentParser(description='Master Trading Bot')
    parser.add_argument('--config', required=True, help='Path to config YAML file')
    parser.add_argument('--paper', action='store_true', help='Use paper trading')
    args = parser.parse_args()

    print("\n" + "=" * 70)
    print("INITIALIZING MASTER TRADING BOT")
    print("=" * 70)
    print(f"Config: {args.config}")
    print(f"Loading configuration...")
    print()

    try:
        bot = MasterTradingBot(args.config)
        print("✓ Bot initialized successfully")
        print()
    except Exception as e:
        print(f"\n✗ INITIALIZATION FAILED: {e}")
        print("\nFull error traceback:")
        traceback.print_exc()
        return

    try:
        bot.start()
    except KeyboardInterrupt:
        print("\n\n[MAIN] Keyboard interrupt received - shutting down...")
        logging.info("[MAIN] Keyboard interrupt received")
    except Exception as e:
        print(f"\n✗ FATAL ERROR: {e}")
        logging.error(f"[MAIN] Fatal error: {e}")
        logging.error(traceback.format_exc())
        traceback.print_exc()
    finally:
        bot.stop()

if __name__ == "__main__":
    main()
