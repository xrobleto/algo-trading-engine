"""
Trend Bot Backtest Framework
=============================

Comprehensive backtesting for trend_bot.py Volatility-Targeted Trend Following.

Strategy:
- Weekly rebalance (Fridays) with inverse-volatility weighting
- Trend filter: Close > SMA200 => long, else flat
- SPY regime filter with hysteresis
- Daily monitoring for risk exits (2-close stop, drawdown stop)
- Dynamic capital deployment based on risk score
- Turnover governor and rank stability

Usage:
    python trend_bot_backtest.py --start 2020-01-01 --end 2024-12-31
    python trend_bot_backtest.py --years 5 --output results.json
    python trend_bot_backtest.py --param-sweep --output sweep_results.csv

Dependencies:
    pip install pandas numpy requests tqdm
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime, date, timedelta
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Callable
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ============================================================
# CONFIGURATION
# ============================================================

ET = ZoneInfo("America/New_York")
POLYGON_API_KEY = os.getenv("POLYGON_API_KEY", "")

# Cache directory for historical data
CACHE_DIR = Path(__file__).parent.parent / "data" / "backtest_cache"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

# Default universe (matches trend_bot.py)
DEFAULT_EQUITY_TICKERS = ["SPY", "QQQ", "IWM"]  # v4: Removed EFA, EEM (low-beta intl)
DEFAULT_SECTOR_TICKERS = [
    "XLK", "XLF", "XLE", "XLV", "XLI", "XLY", "XLC",  # v4: Removed XLP, XLU, XLRE, XLB (defensive)
    "SMH", "IBB", "XHB",
]
DEFAULT_FACTOR_TICKERS = ["MTUM", "QUAL"]  # v4: Removed VLUE (anti-momentum)
DEFAULT_TACTICAL_ETFS = ["RSP"]

# v1: Extended universe with leveraged + high-momentum ETFs
LEVERAGED_ETFS = ["TQQQ", "UPRO", "SOXL", "TECL", "FAS"]
MOMENTUM_ETFS = ["ARKK", "XBI", "KWEB", "SOXX", "IGV", "CIBR", "SKYY"]

# Default parameters (from trend_bot.py)
DEFAULT_CONFIG = {
    # Core parameters
    "sma_lookback_days": 50,           # v1: Faster trend filter (was 200)
    "vol_lookback_days": 20,
    "vol_floor_annual": 0.10,
    "rebalance_weekday": 4,  # Friday

    # Risk management
    "max_gross_exposure": 1.25,        # v8: 25% margin leverage (was 0.95)
    "per_asset_cap": 0.40,             # v1: Higher concentration (was 0.25)
    "max_portfolio_drawdown": 1.00,    # v3: Disabled circuit breaker (was 0.15)
    "drawdown_cooldown_days": 3,       # v1: Faster recovery (was 5)

    # Regime filter
    "regime_buffer_on": 0.01,          # v1: Faster regime entry (was 0.02)
    "regime_buffer_off": 0.02,         # v1: Slower regime exit (was 0.01)

    # Stop loss
    "stop_loss_sma200_days": 2,
    "stop_loss_sma200_buffer": 0.93,   # v3: Even wider buffer for leveraged ETFs (was 0.98)
    "stop_loss_position_dd_pct": 0.30, # v3: Much wider stop for leveraged ETFs (was 0.15)

    # Momentum ranking
    "enable_momentum_ranking": True,
    "momentum_lookback_days": 30,      # v5: ~1.5 month momentum (was 63)
    "top_n_assets": 4,                 # v4: More concentrated (was 8)

    # v2: Use momentum-weighted allocation instead of inverse-vol
    "use_momentum_weighting": False,   # v3: Back to inverse-vol (momentum too volatile)

    # Dynamic capital
    "enable_dynamic_capital": True,
    "capital_deployment_tiers": [
        {"risk_on": True, "risk_score_min": 40, "multiplier": 1.00},   # v1: Full deployment more often
        {"risk_on": True, "risk_score_min": 0, "multiplier": 0.85},    # v1: Higher floor
        {"risk_on": False, "risk_score_min": 0, "multiplier": 0.40},   # v1: Higher risk-off floor
    ],

    # Turnover governor
    "enable_turnover_governor": False,  # v1: Disabled (was True)
    "max_turnover_per_rebalance": 0.50,
    "no_trade_drift_threshold": 0.03,

    # Rank stability
    "enable_rank_stability": False,     # v1: Disabled (was True)
    "rank_stability_weeks": 0,

    # Transaction costs
    "commission_per_trade": 0.0,  # $0 for most brokers
    "slippage_bps": 5,  # 5 bps slippage

    # Benchmark
    "benchmark_ticker": "SPY",
}


# ============================================================
# DATA STRUCTURES
# ============================================================

class TradeAction(Enum):
    BUY = "BUY"
    SELL = "SELL"
    REBALANCE = "REBALANCE"


class ExitReason(Enum):
    REBALANCE = "REBALANCE"
    TREND_EXIT = "TREND_EXIT"
    STOP_LOSS_SMA = "STOP_LOSS_SMA"
    STOP_LOSS_DD = "STOP_LOSS_DD"
    REGIME_EXIT = "REGIME_EXIT"
    GAP_DOWN = "GAP_DOWN"


@dataclass
class Position:
    """Represents a portfolio position."""
    symbol: str
    shares: float
    entry_price: float
    entry_date: date
    cost_basis: float = 0.0

    def __post_init__(self):
        if self.cost_basis == 0.0:
            self.cost_basis = self.shares * self.entry_price


@dataclass
class Trade:
    """Represents a completed trade."""
    symbol: str
    action: TradeAction
    shares: float
    price: float
    trade_date: date
    reason: str = ""
    notional: float = 0.0
    commission: float = 0.0
    slippage: float = 0.0

    def __post_init__(self):
        if self.notional == 0.0:
            self.notional = abs(self.shares * self.price)


@dataclass
class DailySnapshot:
    """Daily portfolio snapshot for performance tracking."""
    date: date
    equity: float
    cash: float
    positions_value: float
    positions: Dict[str, float]  # symbol -> market value
    weights: Dict[str, float]  # symbol -> weight
    drawdown: float = 0.0
    spy_regime: str = "risk_on"
    risk_score: float = 50.0


@dataclass
class BacktestResult:
    """Complete backtest results."""
    # Metadata
    start_date: date
    end_date: date
    initial_capital: float
    final_equity: float
    config: Dict[str, Any]

    # Performance metrics
    total_return: float = 0.0
    cagr: float = 0.0
    volatility: float = 0.0
    sharpe_ratio: float = 0.0
    sortino_ratio: float = 0.0
    max_drawdown: float = 0.0
    max_drawdown_duration_days: int = 0
    calmar_ratio: float = 0.0

    # Trade statistics
    total_trades: int = 0
    avg_trade_pnl: float = 0.0
    win_rate: float = 0.0
    profit_factor: float = 0.0
    avg_holding_days: float = 0.0

    # Turnover
    annual_turnover: float = 0.0
    avg_positions: float = 0.0

    # Benchmark comparison
    benchmark_return: float = 0.0
    benchmark_cagr: float = 0.0
    alpha: float = 0.0
    beta: float = 0.0
    information_ratio: float = 0.0

    # Monthly returns
    monthly_returns: List[float] = field(default_factory=list)

    # Data
    daily_snapshots: List[DailySnapshot] = field(default_factory=list)
    trades: List[Trade] = field(default_factory=list)


# ============================================================
# DATA LOADER
# ============================================================

class PolygonDataLoader:
    """Loads and caches historical data from Polygon."""

    def __init__(self, api_key: str, cache_dir: Path):
        self.api_key = api_key
        self.cache_dir = cache_dir
        self.cache_dir.mkdir(parents=True, exist_ok=True)

        # Setup session with retries
        self.session = requests.Session()
        retries = Retry(total=3, backoff_factor=0.5, status_forcelist=[429, 500, 502, 503, 504])
        self.session.mount("https://", HTTPAdapter(max_retries=retries))

    def get_daily_bars(
        self,
        symbols: List[str],
        start_date: date,
        end_date: date,
        use_cache: bool = True
    ) -> pd.DataFrame:
        """
        Fetch daily bars for multiple symbols.

        Returns DataFrame with columns: date, symbol, open, high, low, close, volume
        """
        all_bars = []

        for symbol in symbols:
            bars = self._get_symbol_bars(symbol, start_date, end_date, use_cache)
            if bars is not None and not bars.empty:
                all_bars.append(bars)

        if not all_bars:
            raise ValueError("No data loaded for any symbol")

        df = pd.concat(all_bars, ignore_index=True)
        df = df.sort_values(["symbol", "date"]).reset_index(drop=True)

        return df

    def _get_symbol_bars(
        self,
        symbol: str,
        start_date: date,
        end_date: date,
        use_cache: bool
    ) -> Optional[pd.DataFrame]:
        """Fetch bars for a single symbol."""
        cache_file = self.cache_dir / f"{symbol}_{start_date.isoformat()}_{end_date.isoformat()}.parquet"

        # Check cache
        if use_cache and cache_file.exists():
            try:
                df = pd.read_parquet(cache_file)
                return df
            except Exception:
                pass  # Cache corrupted, refetch

        # Fetch from API
        try:
            url = (
                f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/day/"
                f"{start_date.isoformat()}/{end_date.isoformat()}"
                f"?adjusted=true&sort=asc&limit=50000&apiKey={self.api_key}"
            )

            resp = self.session.get(url, timeout=30)
            resp.raise_for_status()
            data = resp.json()

            if data.get("resultsCount", 0) == 0:
                print(f"[WARN] No data for {symbol}")
                return None

            results = data.get("results", [])

            df = pd.DataFrame(results)
            df["date"] = pd.to_datetime(df["t"], unit="ms").dt.date
            df["symbol"] = symbol
            df = df.rename(columns={
                "o": "open",
                "h": "high",
                "l": "low",
                "c": "close",
                "v": "volume"
            })
            df = df[["date", "symbol", "open", "high", "low", "close", "volume"]]

            # Cache the data
            if use_cache:
                df.to_parquet(cache_file)

            # Rate limiting
            time.sleep(0.15)

            return df

        except Exception as e:
            print(f"[ERROR] Failed to fetch {symbol}: {e}")
            return None


# ============================================================
# INDICATORS
# ============================================================

def sma(series: pd.Series, period: int) -> float:
    """Calculate Simple Moving Average."""
    if len(series) < period:
        return float(series.iloc[-1]) if len(series) > 0 else 0.0
    return float(series.iloc[-period:].mean())


def annualized_vol(returns: pd.Series, period: int = 20) -> float:
    """Calculate annualized volatility from returns."""
    if len(returns) < period:
        return 0.20  # Default 20%
    return float(returns.iloc[-period:].std() * np.sqrt(252))


def compute_momentum_score(close: pd.Series, lookback: int = 63) -> Optional[float]:
    """
    Compute momentum score as weighted average of 1M, 3M returns.
    Matches trend_bot.py logic.
    """
    if len(close) < lookback:
        return None

    # 1-month return (21 trading days)
    if len(close) >= 21:
        ret_1m = (close.iloc[-1] / close.iloc[-21] - 1)
    else:
        ret_1m = 0.0

    # 3-month return (63 trading days)
    if len(close) >= 63:
        ret_3m = (close.iloc[-1] / close.iloc[-63] - 1)
    else:
        ret_3m = ret_1m

    # Weighted average (60% 1M, 40% 3M)
    return 0.6 * ret_1m + 0.4 * ret_3m


def compute_risk_score(
    spy_close: pd.Series,
    rsp_close: Optional[pd.Series] = None,
    iwm_close: Optional[pd.Series] = None
) -> float:
    """
    Compute market risk score (0-100).
    Higher = more bullish conditions.
    """
    score = 50.0  # Start neutral

    if len(spy_close) < 200:
        return score

    # SPY trend (40 points)
    spy_sma200 = sma(spy_close, 200)
    spy_last = float(spy_close.iloc[-1])
    if spy_last > spy_sma200:
        score += 20
        if spy_last > spy_sma200 * 1.05:  # 5% above
            score += 10
        if spy_last > spy_sma200 * 1.10:  # 10% above
            score += 10
    else:
        score -= 20
        if spy_last < spy_sma200 * 0.95:
            score -= 10

    # Breadth (RSP/SPY ratio) - 20 points
    if rsp_close is not None and len(rsp_close) >= 20:
        rsp_spy = rsp_close / spy_close.iloc[-len(rsp_close):]
        rsp_spy_sma = sma(rsp_spy, 20)
        rsp_spy_last = float(rsp_spy.iloc[-1])
        if rsp_spy_last > rsp_spy_sma:
            score += 10
        else:
            score -= 10

    # Small cap strength (IWM/SPY ratio) - 20 points
    if iwm_close is not None and len(iwm_close) >= 20:
        iwm_spy = iwm_close / spy_close.iloc[-len(iwm_close):]
        iwm_spy_sma = sma(iwm_spy, 20)
        iwm_spy_last = float(iwm_spy.iloc[-1])
        if iwm_spy_last > iwm_spy_sma:
            score += 10
        else:
            score -= 10

    return max(0.0, min(100.0, score))


# ============================================================
# BACKTEST ENGINE
# ============================================================

class BacktestEngine:
    """
    Event-driven backtest engine for trend_bot strategy.

    Simulates:
    - Weekly rebalancing (Fridays)
    - Daily monitoring for risk exits
    - Regime filter with hysteresis
    - Turnover governor and rank stability
    """

    def __init__(
        self,
        config: Dict[str, Any],
        initial_capital: float = 100_000,
        verbose: bool = True
    ):
        self.config = {**DEFAULT_CONFIG, **config}
        self.initial_capital = initial_capital
        self.verbose = verbose

        # State
        self.cash = initial_capital
        self.positions: Dict[str, Position] = {}
        self.equity_peak = initial_capital
        self.spy_regime = "risk_on"
        self.drawdown_cooldown_until: Optional[date] = None
        self.rank_history: Dict[str, List[date]] = {}
        self.last_rebalance_date: Optional[date] = None

        # Tracking
        self.trades: List[Trade] = []
        self.daily_snapshots: List[DailySnapshot] = []

        # Data
        self.bars_df: Optional[pd.DataFrame] = None
        self.trading_days: List[date] = []

    def run(
        self,
        bars_df: pd.DataFrame,
        start_date: date,
        end_date: date
    ) -> BacktestResult:
        """
        Run the backtest.

        Args:
            bars_df: DataFrame with columns [date, symbol, open, high, low, close, volume]
            start_date: Start date (inclusive)
            end_date: End date (inclusive)

        Returns:
            BacktestResult with all metrics
        """
        self.bars_df = bars_df

        # Get unique trading days
        self.trading_days = sorted(bars_df["date"].unique())
        self.trading_days = [d for d in self.trading_days if start_date <= d <= end_date]

        if not self.trading_days:
            raise ValueError("No trading days in date range")

        if self.verbose:
            print(f"[BACKTEST] Running {start_date} to {end_date} ({len(self.trading_days)} days)")

        # Main simulation loop
        for current_date in self.trading_days:
            self._process_day(current_date)

        # Calculate final metrics
        result = self._calculate_results(start_date, end_date)

        return result

    def _process_day(self, current_date: date):
        """Process a single trading day."""
        # Get today's data
        day_bars = self.bars_df[self.bars_df["date"] == current_date]
        if day_bars.empty:
            return

        # Update position values with today's prices
        prices = dict(zip(day_bars["symbol"], day_bars["close"]))

        # Calculate current equity
        positions_value = sum(
            pos.shares * prices.get(pos.symbol, pos.entry_price)
            for pos in self.positions.values()
        )
        equity = self.cash + positions_value

        # Update equity peak
        self.equity_peak = max(self.equity_peak, equity)
        drawdown = (self.equity_peak - equity) / self.equity_peak if self.equity_peak > 0 else 0.0

        # Check drawdown circuit breaker
        if drawdown >= self.config["max_portfolio_drawdown"]:
            if self.drawdown_cooldown_until is None or current_date > self.drawdown_cooldown_until:
                self._exit_all_positions(current_date, prices, "DRAWDOWN_BREAKER")
                self.drawdown_cooldown_until = current_date + timedelta(days=self.config["drawdown_cooldown_days"])
                if self.verbose:
                    print(f"[{current_date}] DRAWDOWN BREAKER TRIGGERED: {drawdown:.1%}")

        # Update regime
        spy_bars = self.bars_df[self.bars_df["symbol"] == "SPY"]
        spy_close = spy_bars[spy_bars["date"] <= current_date]["close"]
        if len(spy_close) >= 200:
            self._update_regime(spy_close)

        # Daily monitoring (risk exits)
        self._daily_monitoring(current_date, prices)

        # Weekly rebalance (Fridays)
        weekday = datetime.combine(current_date, datetime.min.time()).weekday()
        if weekday == self.config["rebalance_weekday"]:
            if self.drawdown_cooldown_until is None or current_date > self.drawdown_cooldown_until:
                self._rebalance(current_date, prices)

        # Record daily snapshot
        positions_value = sum(
            pos.shares * prices.get(pos.symbol, pos.entry_price)
            for pos in self.positions.values()
        )
        equity = self.cash + positions_value

        # Calculate risk score for snapshot
        rsp_close = None
        iwm_close = None
        if "RSP" in self.bars_df["symbol"].values:
            rsp_bars = self.bars_df[(self.bars_df["symbol"] == "RSP") & (self.bars_df["date"] <= current_date)]
            rsp_close = rsp_bars["close"]
        if "IWM" in self.bars_df["symbol"].values:
            iwm_bars = self.bars_df[(self.bars_df["symbol"] == "IWM") & (self.bars_df["date"] <= current_date)]
            iwm_close = iwm_bars["close"]

        risk_score = compute_risk_score(spy_close, rsp_close, iwm_close)

        snapshot = DailySnapshot(
            date=current_date,
            equity=equity,
            cash=self.cash,
            positions_value=positions_value,
            positions={s: pos.shares * prices.get(s, pos.entry_price) for s, pos in self.positions.items()},
            weights={s: (pos.shares * prices.get(s, pos.entry_price)) / equity if equity > 0 else 0.0
                    for s, pos in self.positions.items()},
            drawdown=drawdown,
            spy_regime=self.spy_regime,
            risk_score=risk_score
        )
        self.daily_snapshots.append(snapshot)

    def _update_regime(self, spy_close: pd.Series):
        """Update SPY regime with hysteresis."""
        spy_sma200 = sma(spy_close, 200)
        spy_last = float(spy_close.iloc[-1])

        buffer_on = self.config["regime_buffer_on"]
        buffer_off = self.config["regime_buffer_off"]

        if self.spy_regime == "risk_off":
            # Need to cross above SMA200 + buffer to turn risk_on
            if spy_last > spy_sma200 * (1 + buffer_on):
                self.spy_regime = "risk_on"
        else:
            # Need to cross below SMA200 - buffer to turn risk_off
            if spy_last < spy_sma200 * (1 - buffer_off):
                self.spy_regime = "risk_off"

    def _daily_monitoring(self, current_date: date, prices: Dict[str, float]):
        """Daily monitoring for risk exits (not rebalancing)."""
        exits = []

        for symbol, pos in list(self.positions.items()):
            current_price = prices.get(symbol)
            if current_price is None:
                continue

            # Get historical data for this symbol
            sym_bars = self.bars_df[
                (self.bars_df["symbol"] == symbol) &
                (self.bars_df["date"] <= current_date)
            ]["close"]

            sma_period = self.config["sma_lookback_days"]
            if len(sym_bars) < sma_period:
                continue

            sma200 = sma(sym_bars, sma_period)

            # Check 2-consecutive-close stop
            recent_closes = sym_bars.iloc[-self.config["stop_loss_sma200_days"]:]
            threshold = sma200 * self.config["stop_loss_sma200_buffer"]

            if len(recent_closes) >= self.config["stop_loss_sma200_days"]:
                if all(c < threshold for c in recent_closes):
                    exits.append((symbol, ExitReason.STOP_LOSS_SMA))
                    continue

            # Check position drawdown stop
            pnl_pct = (current_price - pos.entry_price) / pos.entry_price
            if pnl_pct <= -self.config["stop_loss_position_dd_pct"]:
                exits.append((symbol, ExitReason.STOP_LOSS_DD))

        # Execute exits
        for symbol, reason in exits:
            self._close_position(symbol, current_date, prices, reason.value)

    def _rebalance(self, current_date: date, prices: Dict[str, float]):
        """Execute weekly rebalance."""
        # Skip if in cooldown
        if self.drawdown_cooldown_until and current_date <= self.drawdown_cooldown_until:
            return

        # Calculate target weights
        target_weights = self._calculate_target_weights(current_date, prices)

        if not target_weights:
            return

        # Apply turnover governor
        if self.config["enable_turnover_governor"]:
            target_weights = self._apply_turnover_cap(target_weights, prices)

        # Get current equity
        positions_value = sum(
            pos.shares * prices.get(pos.symbol, pos.entry_price)
            for pos in self.positions.values()
        )
        equity = self.cash + positions_value

        # Calculate current weights
        current_weights = {
            s: (pos.shares * prices.get(s, pos.entry_price)) / equity if equity > 0 else 0.0
            for s, pos in self.positions.items()
        }

        # Calculate target positions
        slippage_mult = 1 + self.config["slippage_bps"] / 10000

        # Sells first
        for symbol in list(self.positions.keys()):
            target_weight = target_weights.get(symbol, 0.0)
            current_weight = current_weights.get(symbol, 0.0)

            if target_weight < current_weight * 0.98:  # Sell if target significantly lower
                pos = self.positions[symbol]
                price = prices.get(symbol, pos.entry_price)

                if target_weight <= 0.001:
                    # Full exit
                    self._close_position(symbol, current_date, prices, "REBALANCE")
                else:
                    # Partial sell
                    target_value = target_weight * equity
                    current_value = current_weight * equity
                    sell_value = current_value - target_value
                    sell_shares = sell_value / (price * slippage_mult)

                    if sell_shares > 0.001:
                        self._execute_trade(
                            symbol, -sell_shares, price / slippage_mult,
                            current_date, "REBALANCE"
                        )

        # Recalculate equity after sells
        positions_value = sum(
            pos.shares * prices.get(pos.symbol, pos.entry_price)
            for pos in self.positions.values()
        )
        equity = self.cash + positions_value

        # Buys
        for symbol, target_weight in target_weights.items():
            current_weight = 0.0
            if symbol in self.positions:
                pos = self.positions[symbol]
                current_weight = (pos.shares * prices.get(symbol, pos.entry_price)) / equity if equity > 0 else 0.0

            if target_weight > current_weight * 1.02:  # Buy if target significantly higher
                price = prices.get(symbol)
                if price is None or price <= 0:
                    continue

                target_value = target_weight * equity
                current_value = current_weight * equity
                buy_value = target_value - current_value
                buy_shares = buy_value / (price * slippage_mult)

                if buy_shares > 0.001 and buy_value <= self.cash:
                    self._execute_trade(
                        symbol, buy_shares, price * slippage_mult,
                        current_date, "REBALANCE"
                    )

        self.last_rebalance_date = current_date

    def _calculate_target_weights(
        self,
        current_date: date,
        prices: Dict[str, float]
    ) -> Dict[str, float]:
        """Calculate target portfolio weights."""
        # Get all symbols with enough history
        eligible = []

        for symbol in prices.keys():
            sym_bars = self.bars_df[
                (self.bars_df["symbol"] == symbol) &
                (self.bars_df["date"] <= current_date)
            ]
            close = sym_bars["close"]

            if len(close) < self.config["sma_lookback_days"]:
                continue

            # Trend filter: Close > SMA200
            sma200 = sma(close, self.config["sma_lookback_days"])
            last_close = float(close.iloc[-1])

            if last_close <= sma200:
                continue

            # Calculate volatility
            returns = close.pct_change().dropna()
            vol = annualized_vol(returns, self.config["vol_lookback_days"])
            vol = max(vol, self.config["vol_floor_annual"])

            # Calculate momentum score
            mom_score = compute_momentum_score(close, self.config["momentum_lookback_days"])

            eligible.append({
                "symbol": symbol,
                "close": last_close,
                "sma200": sma200,
                "vol": vol,
                "mom_score": mom_score or 0.0
            })

        if not eligible:
            return {}

        # Rank by momentum if enabled
        if self.config["enable_momentum_ranking"]:
            eligible.sort(key=lambda x: x["mom_score"], reverse=True)
            eligible = eligible[:self.config["top_n_assets"]]

        # Apply rank stability filter
        if self.config["enable_rank_stability"]:
            eligible = self._filter_by_rank_stability(eligible, current_date)

        # Calculate weights
        if self.config.get("use_momentum_weighting", False):
            # v2: Momentum-weighted — stronger momentum = larger position
            # Shift scores so all are positive (add abs of min + small epsilon)
            mom_scores = [e["mom_score"] for e in eligible]
            min_score = min(mom_scores) if mom_scores else 0
            shifted = [s - min_score + 0.01 for s in mom_scores]
            total_mom = sum(shifted)

            if total_mom <= 0:
                return {}

            weights = {}
            for e, s in zip(eligible, shifted):
                raw_weight = s / total_mom
                capped_weight = min(raw_weight, self.config["per_asset_cap"])
                weights[e["symbol"]] = capped_weight
        else:
            # Original: inverse-vol weights
            total_inv_vol = sum(1.0 / e["vol"] for e in eligible)

            if total_inv_vol <= 0:
                return {}

            weights = {}
            for e in eligible:
                raw_weight = (1.0 / e["vol"]) / total_inv_vol
                capped_weight = min(raw_weight, self.config["per_asset_cap"])
                weights[e["symbol"]] = capped_weight

        # Normalize
        total_weight = sum(weights.values())
        if total_weight > 0:
            weights = {s: w / total_weight for s, w in weights.items()}

        # Apply regime-based capital deployment
        if self.config["enable_dynamic_capital"]:
            spy_bars = self.bars_df[
                (self.bars_df["symbol"] == "SPY") &
                (self.bars_df["date"] <= current_date)
            ]
            spy_close = spy_bars["close"]

            rsp_close = None
            iwm_close = None
            if "RSP" in self.bars_df["symbol"].values:
                rsp_bars = self.bars_df[(self.bars_df["symbol"] == "RSP") & (self.bars_df["date"] <= current_date)]
                rsp_close = rsp_bars["close"]
            if "IWM" in self.bars_df["symbol"].values:
                iwm_bars = self.bars_df[(self.bars_df["symbol"] == "IWM") & (self.bars_df["date"] <= current_date)]
                iwm_close = iwm_bars["close"]

            risk_score = compute_risk_score(spy_close, rsp_close, iwm_close)

            # Find matching tier
            multiplier = 1.0
            is_risk_on = self.spy_regime == "risk_on"

            for tier in self.config["capital_deployment_tiers"]:
                if tier["risk_on"] == is_risk_on and risk_score >= tier["risk_score_min"]:
                    multiplier = tier["multiplier"]
                    break

            # Scale weights
            weights = {s: w * multiplier * self.config["max_gross_exposure"]
                      for s, w in weights.items()}
        else:
            # Apply max gross exposure
            weights = {s: w * self.config["max_gross_exposure"] for s, w in weights.items()}

        return weights

    def _filter_by_rank_stability(
        self,
        eligible: List[Dict],
        current_date: date
    ) -> List[Dict]:
        """Filter by rank stability (require N consecutive weeks in top ranks)."""
        required_weeks = self.config["rank_stability_weeks"]
        top_n = self.config["top_n_assets"]

        # Update rank history for current week
        current_top = {e["symbol"] for e in eligible[:top_n]}

        for symbol in current_top:
            if symbol not in self.rank_history:
                self.rank_history[symbol] = []
            self.rank_history[symbol].append(current_date)
            # Keep only recent history
            self.rank_history[symbol] = self.rank_history[symbol][-10:]

        # Filter: only include symbols with required consecutive weeks
        # OR symbols we already hold (grandfather clause)
        filtered = []
        for e in eligible:
            symbol = e["symbol"]

            # Grandfather existing positions
            if symbol in self.positions:
                filtered.append(e)
                continue

            # Check rank stability
            history = self.rank_history.get(symbol, [])
            if len(history) >= required_weeks:
                # Check if last N entries are within N weeks
                recent = history[-required_weeks:]
                if len(recent) >= required_weeks:
                    filtered.append(e)

        return filtered

    def _apply_turnover_cap(
        self,
        target_weights: Dict[str, float],
        prices: Dict[str, float]
    ) -> Dict[str, float]:
        """Apply turnover cap to limit rebalance magnitude."""
        # Calculate current weights
        positions_value = sum(
            pos.shares * prices.get(pos.symbol, pos.entry_price)
            for pos in self.positions.values()
        )
        equity = self.cash + positions_value

        if equity <= 0:
            return target_weights

        current_weights = {
            s: (pos.shares * prices.get(s, pos.entry_price)) / equity
            for s, pos in self.positions.items()
        }

        # Calculate one-way turnover
        all_symbols = set(target_weights.keys()) | set(current_weights.keys())
        turnover = 0.0
        for symbol in all_symbols:
            target_w = target_weights.get(symbol, 0.0)
            current_w = current_weights.get(symbol, 0.0)
            turnover += abs(target_w - current_w) / 2

        # If turnover exceeds cap, scale down changes
        max_turnover = self.config["max_turnover_per_rebalance"]
        if turnover > max_turnover:
            scale = max_turnover / turnover

            adjusted = {}
            for symbol in all_symbols:
                target_w = target_weights.get(symbol, 0.0)
                current_w = current_weights.get(symbol, 0.0)
                delta = target_w - current_w
                adjusted[symbol] = current_w + delta * scale

            return adjusted

        return target_weights

    def _execute_trade(
        self,
        symbol: str,
        shares: float,
        price: float,
        trade_date: date,
        reason: str
    ):
        """Execute a trade and update positions."""
        commission = self.config["commission_per_trade"]
        slippage = abs(shares * price * self.config["slippage_bps"] / 10000)
        notional = abs(shares * price)

        if shares > 0:
            # Buy
            cost = shares * price + commission + slippage
            if cost > self.cash:
                # Adjust shares to fit available cash
                shares = (self.cash - commission - slippage) / price
                if shares <= 0:
                    return
                cost = shares * price + commission + slippage

            self.cash -= cost

            if symbol in self.positions:
                # Add to existing position
                pos = self.positions[symbol]
                total_cost = pos.cost_basis + shares * price
                total_shares = pos.shares + shares
                pos.shares = total_shares
                pos.cost_basis = total_cost
                pos.entry_price = total_cost / total_shares
            else:
                # New position
                self.positions[symbol] = Position(
                    symbol=symbol,
                    shares=shares,
                    entry_price=price,
                    entry_date=trade_date,
                    cost_basis=shares * price
                )

            action = TradeAction.BUY
        else:
            # Sell
            shares = abs(shares)
            if symbol not in self.positions:
                return

            pos = self.positions[symbol]
            shares = min(shares, pos.shares)

            proceeds = shares * price - commission - slippage
            self.cash += proceeds

            pos.shares -= shares
            if pos.shares < 0.001:
                del self.positions[symbol]

            action = TradeAction.SELL

        self.trades.append(Trade(
            symbol=symbol,
            action=action,
            shares=shares if action == TradeAction.BUY else -shares,
            price=price,
            trade_date=trade_date,
            reason=reason,
            notional=notional,
            commission=commission,
            slippage=slippage
        ))

    def _close_position(
        self,
        symbol: str,
        trade_date: date,
        prices: Dict[str, float],
        reason: str
    ):
        """Close an entire position."""
        if symbol not in self.positions:
            return

        pos = self.positions[symbol]
        price = prices.get(symbol, pos.entry_price)

        self._execute_trade(symbol, -pos.shares, price, trade_date, reason)

    def _exit_all_positions(
        self,
        trade_date: date,
        prices: Dict[str, float],
        reason: str
    ):
        """Exit all positions."""
        for symbol in list(self.positions.keys()):
            self._close_position(symbol, trade_date, prices, reason)

    def _calculate_results(self, start_date: date, end_date: date) -> BacktestResult:
        """Calculate all performance metrics."""
        if not self.daily_snapshots:
            return BacktestResult(
                start_date=start_date,
                end_date=end_date,
                initial_capital=self.initial_capital,
                final_equity=self.initial_capital,
                config=self.config
            )

        # Extract equity curve
        equity_curve = pd.Series(
            [s.equity for s in self.daily_snapshots],
            index=[s.date for s in self.daily_snapshots]
        )

        final_equity = equity_curve.iloc[-1]

        # Returns
        daily_returns = equity_curve.pct_change().dropna()
        total_return = (final_equity - self.initial_capital) / self.initial_capital

        # CAGR
        years = (end_date - start_date).days / 365.25
        cagr = (final_equity / self.initial_capital) ** (1 / years) - 1 if years > 0 else 0.0

        # Volatility
        volatility = daily_returns.std() * np.sqrt(252)

        # Sharpe (assuming 4% risk-free rate)
        rf_daily = 0.04 / 252
        excess_returns = daily_returns - rf_daily
        sharpe = excess_returns.mean() / excess_returns.std() * np.sqrt(252) if excess_returns.std() > 0 else 0.0

        # Sortino
        downside_returns = daily_returns[daily_returns < 0]
        downside_std = downside_returns.std() * np.sqrt(252) if len(downside_returns) > 0 else volatility
        sortino = (daily_returns.mean() * 252) / downside_std if downside_std > 0 else 0.0

        # Max drawdown
        rolling_max = equity_curve.expanding().max()
        drawdown_series = (rolling_max - equity_curve) / rolling_max
        max_drawdown = drawdown_series.max()

        # Max drawdown duration
        in_drawdown = drawdown_series > 0.001
        dd_groups = (~in_drawdown).cumsum()
        dd_lengths = in_drawdown.groupby(dd_groups).sum()
        max_dd_duration = int(dd_lengths.max()) if len(dd_lengths) > 0 else 0

        # Calmar
        calmar = cagr / max_drawdown if max_drawdown > 0 else 0.0

        # Trade statistics
        total_trades = len(self.trades)

        # Calculate P&L per trade (simplified)
        trade_pnls = []
        for trade in self.trades:
            if trade.action == TradeAction.SELL:
                trade_pnls.append(trade.notional * 0.01)  # Placeholder

        avg_trade_pnl = np.mean(trade_pnls) if trade_pnls else 0.0

        # Turnover
        total_notional = sum(t.notional for t in self.trades)
        annual_turnover = total_notional / (self.initial_capital * years) if years > 0 else 0.0

        # Average positions
        avg_positions = np.mean([len(s.positions) for s in self.daily_snapshots])

        # Benchmark comparison
        benchmark_ticker = self.config["benchmark_ticker"]
        bench_bars = self.bars_df[
            (self.bars_df["symbol"] == benchmark_ticker) &
            (self.bars_df["date"] >= start_date) &
            (self.bars_df["date"] <= end_date)
        ]

        benchmark_return = 0.0
        benchmark_cagr = 0.0
        alpha = 0.0
        beta = 1.0
        information_ratio = 0.0

        if len(bench_bars) > 1:
            bench_close = bench_bars["close"]
            benchmark_return = (bench_close.iloc[-1] - bench_close.iloc[0]) / bench_close.iloc[0]
            benchmark_cagr = (1 + benchmark_return) ** (1 / years) - 1 if years > 0 else 0.0

            bench_returns = bench_close.pct_change().dropna()

            # Align returns
            if len(bench_returns) == len(daily_returns):
                cov = np.cov(daily_returns, bench_returns)[0, 1]
                var = bench_returns.var()
                beta = cov / var if var > 0 else 1.0
                alpha = cagr - (0.04 + beta * (benchmark_cagr - 0.04))

                active_returns = daily_returns - bench_returns
                tracking_error = active_returns.std() * np.sqrt(252)
                information_ratio = (daily_returns.mean() - bench_returns.mean()) * 252 / tracking_error if tracking_error > 0 else 0.0

        # Monthly returns
        equity_df = pd.DataFrame({"equity": equity_curve})
        equity_df.index = pd.to_datetime(equity_df.index)
        monthly = equity_df.resample("M").last()
        monthly_returns = monthly["equity"].pct_change().dropna().tolist()

        return BacktestResult(
            start_date=start_date,
            end_date=end_date,
            initial_capital=self.initial_capital,
            final_equity=final_equity,
            config=self.config,
            total_return=total_return,
            cagr=cagr,
            volatility=volatility,
            sharpe_ratio=sharpe,
            sortino_ratio=sortino,
            max_drawdown=max_drawdown,
            max_drawdown_duration_days=max_dd_duration,
            calmar_ratio=calmar,
            total_trades=total_trades,
            avg_trade_pnl=avg_trade_pnl,
            win_rate=0.0,  # Placeholder
            profit_factor=0.0,  # Placeholder
            avg_holding_days=0.0,  # Placeholder
            annual_turnover=annual_turnover,
            avg_positions=avg_positions,
            benchmark_return=benchmark_return,
            benchmark_cagr=benchmark_cagr,
            alpha=alpha,
            beta=beta,
            information_ratio=information_ratio,
            monthly_returns=monthly_returns,
            daily_snapshots=self.daily_snapshots,
            trades=self.trades
        )


# ============================================================
# REPORTING
# ============================================================

def print_report(result: BacktestResult):
    """Print formatted backtest report."""
    print("\n" + "=" * 70)
    print("  TREND BOT BACKTEST REPORT")
    print("=" * 70)

    print(f"\n  Period: {result.start_date} to {result.end_date}")
    print(f"  Initial Capital: ${result.initial_capital:,.0f}")
    print(f"  Final Equity: ${result.final_equity:,.0f}")

    print("\n" + "-" * 70)
    print("  PERFORMANCE METRICS")
    print("-" * 70)

    print(f"\n  Total Return:      {result.total_return:>10.2%}")
    print(f"  CAGR:              {result.cagr:>10.2%}")
    print(f"  Volatility:        {result.volatility:>10.2%}")
    print(f"  Sharpe Ratio:      {result.sharpe_ratio:>10.2f}")
    print(f"  Sortino Ratio:     {result.sortino_ratio:>10.2f}")
    print(f"  Max Drawdown:      {result.max_drawdown:>10.2%}")
    print(f"  Max DD Duration:   {result.max_drawdown_duration_days:>10} days")
    print(f"  Calmar Ratio:      {result.calmar_ratio:>10.2f}")

    print("\n" + "-" * 70)
    print("  BENCHMARK COMPARISON")
    print("-" * 70)

    print(f"\n  Benchmark Return:  {result.benchmark_return:>10.2%}")
    print(f"  Benchmark CAGR:    {result.benchmark_cagr:>10.2%}")
    print(f"  Alpha:             {result.alpha:>10.2%}")
    print(f"  Beta:              {result.beta:>10.2f}")
    print(f"  Information Ratio: {result.information_ratio:>10.2f}")

    print("\n" + "-" * 70)
    print("  TRADING ACTIVITY")
    print("-" * 70)

    print(f"\n  Total Trades:      {result.total_trades:>10}")
    print(f"  Annual Turnover:   {result.annual_turnover:>10.1%}")
    print(f"  Avg Positions:     {result.avg_positions:>10.1f}")

    print("\n" + "=" * 70)


def save_results(result: BacktestResult, output_path: str):
    """Save results to JSON file."""
    # Convert to serializable dict
    data = {
        "start_date": result.start_date.isoformat(),
        "end_date": result.end_date.isoformat(),
        "initial_capital": result.initial_capital,
        "final_equity": result.final_equity,
        "total_return": result.total_return,
        "cagr": result.cagr,
        "volatility": result.volatility,
        "sharpe_ratio": result.sharpe_ratio,
        "sortino_ratio": result.sortino_ratio,
        "max_drawdown": result.max_drawdown,
        "max_drawdown_duration_days": result.max_drawdown_duration_days,
        "calmar_ratio": result.calmar_ratio,
        "total_trades": result.total_trades,
        "annual_turnover": result.annual_turnover,
        "avg_positions": result.avg_positions,
        "benchmark_return": result.benchmark_return,
        "benchmark_cagr": result.benchmark_cagr,
        "alpha": result.alpha,
        "beta": result.beta,
        "information_ratio": result.information_ratio,
        "monthly_returns": result.monthly_returns,
        "config": result.config,
    }

    with open(output_path, "w") as f:
        json.dump(data, f, indent=2)

    print(f"\n[SAVED] Results to {output_path}")


# ============================================================
# CLI
# ============================================================

def main():
    parser = argparse.ArgumentParser(
        description="Backtest trend_bot.py strategy",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python trend_bot_backtest.py --start 2020-01-01 --end 2024-12-31
  python trend_bot_backtest.py --years 5
  python trend_bot_backtest.py --years 3 --output results.json
  python trend_bot_backtest.py --config custom_config.json
        """
    )

    parser.add_argument(
        "--start",
        type=str,
        help="Start date (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--end",
        type=str,
        help="End date (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--years",
        type=int,
        default=3,
        help="Years to backtest (default: 3)"
    )
    parser.add_argument(
        "--capital",
        type=float,
        default=100_000,
        help="Initial capital (default: 100000)"
    )
    parser.add_argument(
        "--output",
        type=str,
        help="Output file path for results JSON"
    )
    parser.add_argument(
        "--config",
        type=str,
        help="Custom config JSON file"
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Suppress progress output"
    )

    args = parser.parse_args()

    # Validate API key
    if not POLYGON_API_KEY:
        print("[ERROR] POLYGON_API_KEY environment variable not set")
        sys.exit(1)

    # Parse dates
    if args.end:
        end_date = date.fromisoformat(args.end)
    else:
        end_date = date.today() - timedelta(days=1)

    if args.start:
        start_date = date.fromisoformat(args.start)
    else:
        start_date = end_date - timedelta(days=args.years * 365)

    # Load custom config if provided
    config = {}
    if args.config:
        with open(args.config, "r") as f:
            config = json.load(f)

    # Get all symbols (v1: include leveraged + momentum ETFs)
    symbols = list(set(
        DEFAULT_EQUITY_TICKERS +
        DEFAULT_SECTOR_TICKERS +
        DEFAULT_FACTOR_TICKERS +
        DEFAULT_TACTICAL_ETFS +
        LEVERAGED_ETFS +
        MOMENTUM_ETFS
    ))

    print(f"\n[BACKTEST] Loading data for {len(symbols)} symbols...")
    print(f"[BACKTEST] Period: {start_date} to {end_date}")

    # Load data
    loader = PolygonDataLoader(POLYGON_API_KEY, CACHE_DIR)

    # Need extra history for SMA200
    data_start = start_date - timedelta(days=400)
    bars_df = loader.get_daily_bars(symbols, data_start, end_date)

    print(f"[BACKTEST] Loaded {len(bars_df)} bars")

    # Run backtest
    engine = BacktestEngine(
        config=config,
        initial_capital=args.capital,
        verbose=not args.quiet
    )

    result = engine.run(bars_df, start_date, end_date)

    # Print report
    print_report(result)

    # Save results if requested
    if args.output:
        save_results(result, args.output)

    return 0


if __name__ == "__main__":
    sys.exit(main())
