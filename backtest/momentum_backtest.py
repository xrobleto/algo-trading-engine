"""
Momentum Trading Bot - Comprehensive Backtest
==============================================
Tests the momentum strategy against historical data to identify:
1. Win rate and profit factor
2. Average trade duration
3. Drawdown analysis
4. Parameter sensitivity
5. Issues/improvements needed
"""

import os
import sys
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass, field
import json

import pandas as pd
import numpy as np
import requests
from pathlib import Path

# Load config
try:
    from dotenv import load_dotenv
    _env_path = Path(__file__).parent / "vwap_bot.env"
    if _env_path.exists():
        load_dotenv(_env_path)
except ImportError:
    pass

POLYGON_API_KEY = os.getenv("POLYGON_API_KEY", "")
if not POLYGON_API_KEY:
    print("ERROR: POLYGON_API_KEY not set")
    sys.exit(1)

# ============================================================
# STRATEGY PARAMETERS (from Momentum_Trading_Bot_Fixed.py)
# ============================================================

RELATIVE_VOLUME_THRESHOLD = 1.5
DAY_PRICE_CHANGE_THRESHOLD = 0.20  # 20% day change
PRICE_MIN = 2.00
PRICE_MAX = 20.00
PRICE_CHANGE_15MIN_THRESHOLD = 0.15  # 15% in 15 mins
PRICE_CHANGE_5MIN_THRESHOLD = 0.05   # 5% in 5 mins
PRICE_CHANGE_1MIN_THRESHOLD = 0.01   # 1% in 1 min

TRAILING_STOP_FALLBACK_PCT = 2.0
RISK_PER_TRADE_PCT = 0.25
STARTING_CAPITAL = 100000

# Test universe - liquid momentum stocks
TEST_UNIVERSE = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "META", "NVDA", "TSLA", "AMD",
    "PLTR", "COIN", "RIVN", "LCID", "NIO", "SOFI", "HOOD",
    "SOXL", "TQQQ", "GME", "AMC", "BBBY", "MARA", "RIOT",
    "SPY", "QQQ", "IWM"
]


# ============================================================
# DATA CLASSES
# ============================================================

@dataclass
class BacktestTrade:
    symbol: str
    entry_time: datetime
    entry_price: float
    exit_time: Optional[datetime] = None
    exit_price: Optional[float] = None
    qty: int = 0
    pnl: float = 0.0
    pnl_pct: float = 0.0
    exit_reason: str = ""
    highest_price: float = 0.0
    hold_duration_mins: int = 0


@dataclass
class BacktestResults:
    total_trades: int = 0
    winning_trades: int = 0
    losing_trades: int = 0
    win_rate: float = 0.0
    total_pnl: float = 0.0
    total_pnl_pct: float = 0.0
    avg_win: float = 0.0
    avg_loss: float = 0.0
    profit_factor: float = 0.0
    max_drawdown: float = 0.0
    max_drawdown_pct: float = 0.0
    avg_hold_mins: float = 0.0
    sharpe_ratio: float = 0.0
    trades: List[BacktestTrade] = field(default_factory=list)
    equity_curve: List[float] = field(default_factory=list)
    issues: List[str] = field(default_factory=list)


# ============================================================
# POLYGON DATA FETCHER
# ============================================================

def fetch_minute_bars(symbol: str, from_date: str, to_date: str) -> pd.DataFrame:
    """Fetch minute bars from Polygon."""
    url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/minute/{from_date}/{to_date}"
    params = {
        "apiKey": POLYGON_API_KEY,
        "adjusted": "true",
        "sort": "asc",
        "limit": 50000
    }

    try:
        resp = requests.get(url, params=params, timeout=30)
        data = resp.json()

        if data.get("status") != "OK" or not data.get("results"):
            return pd.DataFrame()

        df = pd.DataFrame(data["results"])
        df["timestamp"] = pd.to_datetime(df["t"], unit="ms", utc=True)
        df["timestamp"] = df["timestamp"].dt.tz_convert("America/New_York")
        df = df.rename(columns={"o": "open", "h": "high", "l": "low", "c": "close", "v": "volume"})
        df = df[["timestamp", "open", "high", "low", "close", "volume"]]
        return df
    except Exception as e:
        print(f"Error fetching {symbol}: {e}")
        return pd.DataFrame()


def fetch_daily_bars(symbol: str, from_date: str, to_date: str) -> pd.DataFrame:
    """Fetch daily bars for ATR calculation."""
    url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/day/{from_date}/{to_date}"
    params = {
        "apiKey": POLYGON_API_KEY,
        "adjusted": "true",
        "sort": "asc",
        "limit": 50000
    }

    try:
        resp = requests.get(url, params=params, timeout=30)
        data = resp.json()

        if data.get("status") != "OK" or not data.get("results"):
            return pd.DataFrame()

        df = pd.DataFrame(data["results"])
        df["date"] = pd.to_datetime(df["t"], unit="ms", utc=True).dt.date
        df = df.rename(columns={"o": "open", "h": "high", "l": "low", "c": "close", "v": "volume"})
        return df
    except Exception as e:
        print(f"Error fetching daily {symbol}: {e}")
        return pd.DataFrame()


# ============================================================
# SIGNAL DETECTION
# ============================================================

def check_momentum_signal(df: pd.DataFrame, idx: int) -> Tuple[bool, Dict]:
    """
    Check if momentum entry conditions are met at given bar index.
    Returns (signal_triggered, details_dict)
    """
    if idx < 16:
        return False, {}

    current_bar = df.iloc[idx]
    current_price = current_bar["close"]
    current_time = current_bar["timestamp"]

    # Price range filter
    if not (PRICE_MIN <= current_price <= PRICE_MAX):
        return False, {"reject": "price_range"}

    # Get day's open (first bar of trading day)
    day_date = current_time.date()
    day_bars = df[df["timestamp"].dt.date == day_date]
    if len(day_bars) == 0:
        return False, {"reject": "no_day_bars"}

    day_open = day_bars.iloc[0]["open"]
    day_pc = (current_price - day_open) / day_open if day_open > 0 else 0

    # Day price change threshold (20%)
    if day_pc < DAY_PRICE_CHANGE_THRESHOLD:
        return False, {"reject": "day_change", "day_pc": day_pc}

    # 15-minute change
    bar_15m_ago = df.iloc[idx - 15]
    pc_15m = (current_price - bar_15m_ago["close"]) / bar_15m_ago["close"]
    if pc_15m < PRICE_CHANGE_15MIN_THRESHOLD:
        return False, {"reject": "15m_change", "pc_15m": pc_15m}

    # 5-minute change
    if idx >= 5:
        bar_5m_ago = df.iloc[idx - 5]
        pc_5m = (current_price - bar_5m_ago["close"]) / bar_5m_ago["close"]
        if pc_5m < PRICE_CHANGE_5MIN_THRESHOLD:
            return False, {"reject": "5m_change", "pc_5m": pc_5m}

    # 1-minute change
    if idx >= 1:
        bar_1m_ago = df.iloc[idx - 1]
        pc_1m = (current_price - bar_1m_ago["close"]) / bar_1m_ago["close"]
        if pc_1m < PRICE_CHANGE_1MIN_THRESHOLD:
            return False, {"reject": "1m_change", "pc_1m": pc_1m}

    # Relative volume
    if idx > 30:
        prev_vol_mean = df["volume"].iloc[idx-30:idx].mean()
    else:
        prev_vol_mean = df["volume"].iloc[:idx].mean()

    current_vol = df["volume"].iloc[idx]
    rel_vol = current_vol / prev_vol_mean if prev_vol_mean > 0 else 0

    if rel_vol < RELATIVE_VOLUME_THRESHOLD:
        return False, {"reject": "rel_volume", "rel_vol": rel_vol}

    # All conditions met!
    return True, {
        "day_pc": day_pc,
        "pc_15m": pc_15m,
        "pc_5m": pc_5m if idx >= 5 else None,
        "pc_1m": pc_1m if idx >= 1 else None,
        "rel_vol": rel_vol
    }


# ============================================================
# BACKTEST ENGINE
# ============================================================

def run_backtest(
    symbol: str,
    df: pd.DataFrame,
    starting_capital: float = STARTING_CAPITAL
) -> List[BacktestTrade]:
    """Run backtest on single symbol."""
    trades = []
    capital = starting_capital
    position = None
    cooldown_until = None

    for idx in range(16, len(df)):
        current_bar = df.iloc[idx]
        current_price = current_bar["close"]
        current_high = current_bar["high"]
        current_low = current_bar["low"]
        current_time = current_bar["timestamp"]

        # Skip if in cooldown
        if cooldown_until and current_time < cooldown_until:
            continue

        # Manage existing position
        if position:
            # Update highest price
            if current_high > position.highest_price:
                position.highest_price = current_high

            # Calculate trailing stop
            trailing_stop = position.highest_price * (1 - TRAILING_STOP_FALLBACK_PCT / 100)

            # Check stop hit (use low of bar)
            if current_low <= trailing_stop:
                # Exit at trailing stop price
                exit_price = trailing_stop
                position.exit_time = current_time
                position.exit_price = exit_price
                position.pnl = (exit_price - position.entry_price) * position.qty
                position.pnl_pct = (exit_price - position.entry_price) / position.entry_price * 100
                position.exit_reason = "trailing_stop"
                position.hold_duration_mins = int((current_time - position.entry_time).total_seconds() / 60)

                trades.append(position)
                capital += position.pnl

                # Set cooldown (1 minute)
                cooldown_until = current_time + timedelta(minutes=1)
                position = None
                continue

        # Check for new entry signal (only if not in position)
        if position is None:
            signal, details = check_momentum_signal(df, idx)

            if signal:
                # Calculate position size
                stop_price = current_price * (1 - TRAILING_STOP_FALLBACK_PCT / 100)
                risk_per_share = current_price - stop_price
                risk_dollars = capital * (RISK_PER_TRADE_PCT / 100)
                qty = int(risk_dollars / risk_per_share) if risk_per_share > 0 else 0

                if qty > 0:
                    position = BacktestTrade(
                        symbol=symbol,
                        entry_time=current_time,
                        entry_price=current_price,
                        qty=qty,
                        highest_price=current_price
                    )

    # Close any open position at end
    if position:
        last_bar = df.iloc[-1]
        position.exit_time = last_bar["timestamp"]
        position.exit_price = last_bar["close"]
        position.pnl = (position.exit_price - position.entry_price) * position.qty
        position.pnl_pct = (position.exit_price - position.entry_price) / position.entry_price * 100
        position.exit_reason = "end_of_data"
        position.hold_duration_mins = int((position.exit_time - position.entry_time).total_seconds() / 60)
        trades.append(position)

    return trades


def analyze_results(trades: List[BacktestTrade], starting_capital: float) -> BacktestResults:
    """Analyze backtest results."""
    results = BacktestResults()
    results.trades = trades
    results.total_trades = len(trades)

    if not trades:
        results.issues.append("NO TRADES GENERATED - thresholds may be too strict")
        return results

    # Win/loss stats
    winners = [t for t in trades if t.pnl > 0]
    losers = [t for t in trades if t.pnl <= 0]

    results.winning_trades = len(winners)
    results.losing_trades = len(losers)
    results.win_rate = len(winners) / len(trades) * 100 if trades else 0

    # P&L stats
    results.total_pnl = sum(t.pnl for t in trades)
    results.total_pnl_pct = results.total_pnl / starting_capital * 100

    if winners:
        results.avg_win = sum(t.pnl for t in winners) / len(winners)
    if losers:
        results.avg_loss = abs(sum(t.pnl for t in losers) / len(losers))

    # Profit factor
    gross_profit = sum(t.pnl for t in winners) if winners else 0
    gross_loss = abs(sum(t.pnl for t in losers)) if losers else 1
    results.profit_factor = gross_profit / gross_loss if gross_loss > 0 else 0

    # Drawdown
    equity = starting_capital
    peak = equity
    max_dd = 0
    equity_curve = [equity]

    for t in trades:
        equity += t.pnl
        equity_curve.append(equity)
        peak = max(peak, equity)
        dd = peak - equity
        max_dd = max(max_dd, dd)

    results.max_drawdown = max_dd
    results.max_drawdown_pct = max_dd / starting_capital * 100
    results.equity_curve = equity_curve

    # Hold duration
    if trades:
        results.avg_hold_mins = sum(t.hold_duration_mins for t in trades) / len(trades)

    # Sharpe ratio (simplified)
    if len(trades) > 1:
        returns = [t.pnl_pct for t in trades]
        if np.std(returns) > 0:
            results.sharpe_ratio = np.mean(returns) / np.std(returns) * np.sqrt(252)

    # Identify issues
    if results.win_rate < 40:
        results.issues.append(f"LOW WIN RATE: {results.win_rate:.1f}% - entry criteria may be chasing tops")

    if results.profit_factor < 1.0:
        results.issues.append(f"PROFIT FACTOR < 1: {results.profit_factor:.2f} - strategy is losing money")

    if results.avg_hold_mins < 5:
        results.issues.append(f"VERY SHORT HOLDS: {results.avg_hold_mins:.1f} mins - likely getting stopped out quickly")

    if results.max_drawdown_pct > 20:
        results.issues.append(f"LARGE DRAWDOWN: {results.max_drawdown_pct:.1f}% - risk management needs work")

    # Check exit reasons
    stop_exits = len([t for t in trades if t.exit_reason == "trailing_stop"])
    if stop_exits == len(trades):
        results.issues.append("ALL EXITS BY STOP - no profit targets being hit, consider adding TPs")

    # Check for consecutive losses
    max_consecutive_losses = 0
    current_losses = 0
    for t in trades:
        if t.pnl <= 0:
            current_losses += 1
            max_consecutive_losses = max(max_consecutive_losses, current_losses)
        else:
            current_losses = 0

    if max_consecutive_losses >= 5:
        results.issues.append(f"MAX {max_consecutive_losses} CONSECUTIVE LOSSES - may need better filters")

    return results


# ============================================================
# MAIN BACKTEST
# ============================================================

def main():
    print("="*80)
    print("MOMENTUM TRADING BOT - COMPREHENSIVE BACKTEST")
    print("="*80)

    # Date range (last 30 days)
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)
    from_str = start_date.strftime("%Y-%m-%d")
    to_str = end_date.strftime("%Y-%m-%d")

    print(f"\nBacktest Period: {from_str} to {to_str}")
    print(f"Starting Capital: ${STARTING_CAPITAL:,.2f}")
    print(f"\nStrategy Parameters:")
    print(f"  - Day Price Change Threshold: {DAY_PRICE_CHANGE_THRESHOLD*100:.0f}%")
    print(f"  - 15min Change Threshold: {PRICE_CHANGE_15MIN_THRESHOLD*100:.0f}%")
    print(f"  - 5min Change Threshold: {PRICE_CHANGE_5MIN_THRESHOLD*100:.0f}%")
    print(f"  - 1min Change Threshold: {PRICE_CHANGE_1MIN_THRESHOLD*100:.0f}%")
    print(f"  - Relative Volume Threshold: {RELATIVE_VOLUME_THRESHOLD}x")
    print(f"  - Trailing Stop: {TRAILING_STOP_FALLBACK_PCT}%")
    print(f"  - Price Range: ${PRICE_MIN} - ${PRICE_MAX}")

    all_trades = []
    symbols_with_signals = []
    symbols_no_signals = []

    print(f"\nScanning {len(TEST_UNIVERSE)} symbols...")
    print("-"*80)

    for i, symbol in enumerate(TEST_UNIVERSE):
        print(f"[{i+1}/{len(TEST_UNIVERSE)}] {symbol}...", end=" ")

        df = fetch_minute_bars(symbol, from_str, to_str)

        if df.empty or len(df) < 100:
            print("insufficient data")
            continue

        trades = run_backtest(symbol, df)

        if trades:
            all_trades.extend(trades)
            symbols_with_signals.append(symbol)
            print(f"{len(trades)} trades")
        else:
            symbols_no_signals.append(symbol)
            print("0 signals")

    print("-"*80)

    # Analyze combined results
    results = analyze_results(all_trades, STARTING_CAPITAL)

    print("\n" + "="*80)
    print("BACKTEST RESULTS")
    print("="*80)

    print(f"\nOverview:")
    print(f"  Symbols with signals: {len(symbols_with_signals)}")
    print(f"  Symbols without signals: {len(symbols_no_signals)}")
    print(f"  Total trades: {results.total_trades}")

    print(f"\nPerformance:")
    print(f"  Win Rate: {results.win_rate:.1f}% ({results.winning_trades}W / {results.losing_trades}L)")
    print(f"  Total P&L: ${results.total_pnl:+,.2f} ({results.total_pnl_pct:+.2f}%)")
    print(f"  Avg Win: ${results.avg_win:,.2f}")
    print(f"  Avg Loss: ${results.avg_loss:,.2f}")
    print(f"  Profit Factor: {results.profit_factor:.2f}")
    print(f"  Max Drawdown: ${results.max_drawdown:,.2f} ({results.max_drawdown_pct:.1f}%)")
    print(f"  Avg Hold Time: {results.avg_hold_mins:.1f} minutes")
    print(f"  Sharpe Ratio: {results.sharpe_ratio:.2f}")

    # Exit reason breakdown
    exit_reasons = {}
    for t in results.trades:
        exit_reasons[t.exit_reason] = exit_reasons.get(t.exit_reason, 0) + 1

    print(f"\nExit Reasons:")
    for reason, count in sorted(exit_reasons.items(), key=lambda x: -x[1]):
        print(f"  {reason}: {count} ({count/len(results.trades)*100:.1f}%)")

    # Trade duration distribution
    if results.trades:
        durations = [t.hold_duration_mins for t in results.trades]
        print(f"\nHold Duration Distribution:")
        print(f"  < 5 mins: {len([d for d in durations if d < 5])} trades")
        print(f"  5-15 mins: {len([d for d in durations if 5 <= d < 15])} trades")
        print(f"  15-60 mins: {len([d for d in durations if 15 <= d < 60])} trades")
        print(f"  > 60 mins: {len([d for d in durations if d >= 60])} trades")

    # P&L distribution
    if results.trades:
        pnls = [t.pnl_pct for t in results.trades]
        print(f"\nP&L Distribution:")
        print(f"  Big wins (>5%): {len([p for p in pnls if p > 5])}")
        print(f"  Small wins (0-5%): {len([p for p in pnls if 0 < p <= 5])}")
        print(f"  Small losses (0 to -2%): {len([p for p in pnls if -2 <= p <= 0])}")
        print(f"  Big losses (<-2%): {len([p for p in pnls if p < -2])}")

    # Issues identified
    print("\n" + "="*80)
    print("ISSUES & IMPROVEMENT AREAS")
    print("="*80)

    if results.issues:
        for i, issue in enumerate(results.issues, 1):
            print(f"\n{i}. {issue}")
    else:
        print("\nNo major issues identified.")

    # Additional analysis
    print("\n" + "-"*80)
    print("ADDITIONAL OBSERVATIONS:")
    print("-"*80)

    # Check if thresholds are too strict
    if len(symbols_no_signals) > len(symbols_with_signals):
        print(f"\n- {len(symbols_no_signals)}/{len(TEST_UNIVERSE)} symbols had NO signals")
        print("  -> Entry thresholds (20% day change, 15% 15min change) are VERY strict")
        print("  -> This catches only extreme momentum spikes (often near tops)")

    # Check trailing stop effectiveness
    if results.avg_hold_mins < 10 and results.win_rate < 50:
        print(f"\n- Short hold times ({results.avg_hold_mins:.1f} min) + low win rate ({results.win_rate:.1f}%)")
        print("  -> 2% trailing stop is likely too tight for volatile momentum stocks")
        print("  -> Consider: ATR-based stops, or wider initial stop with tighter after profit")

    # Entry timing
    if results.profit_factor < 1.2:
        print(f"\n- Low profit factor ({results.profit_factor:.2f})")
        print("  -> Strategy may be entering AFTER the move (chasing)")
        print("  -> Consider: Lower thresholds to enter earlier, or add pullback requirement")

    # Sample trades
    if results.trades:
        print("\n" + "-"*80)
        print("SAMPLE TRADES (Last 10):")
        print("-"*80)
        for t in results.trades[-10:]:
            print(f"  {t.symbol} | Entry: {t.entry_time.strftime('%m/%d %H:%M')} @ ${t.entry_price:.2f} | "
                  f"Exit: ${t.exit_price:.2f} | P&L: {t.pnl_pct:+.2f}% | "
                  f"Hold: {t.hold_duration_mins}min | {t.exit_reason}")

    print("\n" + "="*80)
    print("RECOMMENDATIONS:")
    print("="*80)

    recommendations = []

    if results.win_rate < 45:
        recommendations.append("1. REDUCE ENTRY THRESHOLDS - Current 20%/15%/5%/1% cascading thresholds only catch extreme moves that often reverse quickly. Consider: 10%/8%/3%/0.5%")

    if results.avg_hold_mins < 10:
        recommendations.append("2. WIDEN INITIAL STOP - 2% trailing stop is too tight for momentum stocks. Consider: 3-4% initial stop, tighten to 2% after 2% profit")

    if "trailing_stop" in exit_reasons and exit_reasons.get("trailing_stop", 0) == len(results.trades):
        recommendations.append("3. ADD PROFIT TARGETS - All exits are by stop. Add staged TPs at +2%, +4%, +6% to lock in gains")

    if len(symbols_no_signals) > 15:
        recommendations.append("4. EXPAND PRICE RANGE - $2-$20 filter excludes many liquid momentum stocks (NVDA, TSLA, etc). Consider $5-$100")

    recommendations.append("5. ADD TIME FILTER - Avoid entries in first 15 mins (noisy) and last 30 mins (low liquidity)")
    recommendations.append("6. ADD PULLBACK REQUIREMENT - Instead of chasing, wait for small pullback after initial surge")

    for rec in recommendations:
        print(f"\n{rec}")

    print("\n" + "="*80)

    # Save results to file
    output = {
        "period": {"from": from_str, "to": to_str},
        "parameters": {
            "day_change_threshold": DAY_PRICE_CHANGE_THRESHOLD,
            "15min_threshold": PRICE_CHANGE_15MIN_THRESHOLD,
            "5min_threshold": PRICE_CHANGE_5MIN_THRESHOLD,
            "1min_threshold": PRICE_CHANGE_1MIN_THRESHOLD,
            "rel_volume_threshold": RELATIVE_VOLUME_THRESHOLD,
            "trailing_stop_pct": TRAILING_STOP_FALLBACK_PCT,
            "price_range": [PRICE_MIN, PRICE_MAX]
        },
        "results": {
            "total_trades": results.total_trades,
            "win_rate": results.win_rate,
            "total_pnl": results.total_pnl,
            "profit_factor": results.profit_factor,
            "max_drawdown_pct": results.max_drawdown_pct,
            "avg_hold_mins": results.avg_hold_mins
        },
        "issues": results.issues,
        "trades": [
            {
                "symbol": t.symbol,
                "entry_time": t.entry_time.isoformat(),
                "exit_time": t.exit_time.isoformat() if t.exit_time else None,
                "entry_price": t.entry_price,
                "exit_price": t.exit_price,
                "pnl_pct": t.pnl_pct,
                "hold_mins": t.hold_duration_mins,
                "exit_reason": t.exit_reason
            }
            for t in results.trades
        ]
    }

    output_file = "momentum_backtest_results.json"
    with open(output_file, "w") as f:
        json.dump(output, f, indent=2)
    print(f"\nResults saved to: {output_file}")


if __name__ == "__main__":
    main()
