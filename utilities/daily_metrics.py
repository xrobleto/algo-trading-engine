#!/usr/bin/env python3
"""
Daily Metrics Tracker for Trading Bots

Analyzes trade journals and generates daily performance summaries.
Run at end of each trading day to track metrics before go-live.

Usage:
    python daily_metrics.py                  # Today's metrics
    python daily_metrics.py --date 2026-01-27  # Specific date
    python daily_metrics.py --week           # Full week summary
    python daily_metrics.py --export csv     # Export to CSV
"""

import argparse
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, field
import sys

# --- Paths ---
SCRIPT_DIR = Path(__file__).parent
ALGO_ROOT = SCRIPT_DIR.parent
_output_root = Path(os.getenv("ALGO_OUTPUT_DIR", "")) if os.getenv("ALGO_OUTPUT_DIR") else ALGO_ROOT
DATA_DIR = _output_root / "data"

# Trade journals
SIMPLE_BOT_JOURNAL = DATA_DIR / "momentum_bot_trades.jsonl"
TREND_BOT_JOURNAL = DATA_DIR / "trend_bot_trades.jsonl"

# Output
METRICS_OUTPUT_DIR = DATA_DIR / "metrics"


@dataclass
class TradeMetrics:
    """Metrics for a single trading day."""
    date: str
    bot_name: str

    # Trade counts
    total_trades: int = 0
    winning_trades: int = 0
    losing_trades: int = 0
    scratch_trades: int = 0  # Breakeven or tiny P&L

    # P&L
    gross_pnl: float = 0.0
    net_pnl: float = 0.0  # After commissions if tracked
    largest_win: float = 0.0
    largest_loss: float = 0.0

    # R-multiples (if available)
    total_r: float = 0.0
    avg_r: float = 0.0

    # Risk metrics
    max_drawdown_pct: float = 0.0
    max_concurrent_positions: int = 0

    # Execution
    avg_hold_time_minutes: float = 0.0
    timeout_count: int = 0
    partial_fill_count: int = 0

    # Errors
    error_count: int = 0
    alert_count: int = 0

    # Individual trades for detail view
    trades: List[Dict] = field(default_factory=list)

    @property
    def win_rate(self) -> float:
        if self.total_trades == 0:
            return 0.0
        return (self.winning_trades / self.total_trades) * 100

    @property
    def profit_factor(self) -> float:
        total_wins = sum(t.get("pnl", 0) for t in self.trades if t.get("pnl", 0) > 0)
        total_losses = abs(sum(t.get("pnl", 0) for t in self.trades if t.get("pnl", 0) < 0))
        if total_losses == 0:
            return float('inf') if total_wins > 0 else 0.0
        return total_wins / total_losses


def load_trades_for_date(journal_path: Path, target_date: str) -> List[Dict]:
    """Load trades from JSONL journal for a specific date."""
    trades = []

    if not journal_path.exists():
        return trades

    try:
        with open(journal_path, 'r') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    trade = json.loads(line)
                    # Check if trade is from target date
                    trade_time = trade.get("exit_time") or trade.get("entry_time") or trade.get("timestamp")
                    if trade_time:
                        # Handle various timestamp formats
                        if isinstance(trade_time, (int, float)):
                            trade_date = datetime.fromtimestamp(trade_time).strftime("%Y-%m-%d")
                        else:
                            trade_date = trade_time[:10]  # "2026-01-27T..."

                        if trade_date == target_date:
                            trades.append(trade)
                except json.JSONDecodeError:
                    continue
    except Exception as e:
        print(f"Error reading {journal_path}: {e}")

    return trades


def calculate_metrics(trades: List[Dict], date: str, bot_name: str) -> TradeMetrics:
    """Calculate metrics from a list of trades."""
    metrics = TradeMetrics(date=date, bot_name=bot_name)
    metrics.trades = trades
    metrics.total_trades = len(trades)

    if not trades:
        return metrics

    for trade in trades:
        pnl = trade.get("pnl", 0) or trade.get("realized_pnl", 0) or 0
        r_multiple = trade.get("r_multiple", 0) or 0
        outcome = trade.get("outcome", "").upper()
        reason = trade.get("reason", "").upper()

        # Classify trade
        if pnl > 0.50:  # More than $0.50 profit
            metrics.winning_trades += 1
            metrics.largest_win = max(metrics.largest_win, pnl)
        elif pnl < -0.50:  # More than $0.50 loss
            metrics.losing_trades += 1
            metrics.largest_loss = min(metrics.largest_loss, pnl)
        else:
            metrics.scratch_trades += 1

        # P&L
        metrics.gross_pnl += pnl
        metrics.total_r += r_multiple

        # Check for issues
        if "TIMEOUT" in reason:
            metrics.timeout_count += 1
        if "PARTIAL" in reason:
            metrics.partial_fill_count += 1
        if outcome == "ERROR" or "ERROR" in reason:
            metrics.error_count += 1

        # Hold time
        entry_time = trade.get("entry_time")
        exit_time = trade.get("exit_time")
        if entry_time and exit_time:
            try:
                if isinstance(entry_time, (int, float)):
                    entry_dt = datetime.fromtimestamp(entry_time)
                else:
                    entry_dt = datetime.fromisoformat(entry_time.replace("Z", "+00:00"))

                if isinstance(exit_time, (int, float)):
                    exit_dt = datetime.fromtimestamp(exit_time)
                else:
                    exit_dt = datetime.fromisoformat(exit_time.replace("Z", "+00:00"))

                hold_minutes = (exit_dt - entry_dt).total_seconds() / 60
                metrics.avg_hold_time_minutes += hold_minutes
            except Exception:
                pass

    # Averages
    if metrics.total_trades > 0:
        metrics.avg_r = metrics.total_r / metrics.total_trades
        metrics.avg_hold_time_minutes /= metrics.total_trades

    metrics.net_pnl = metrics.gross_pnl  # No commission tracking yet

    return metrics


def print_daily_report(metrics: TradeMetrics):
    """Print formatted daily metrics report."""
    print()
    print("=" * 60)
    print(f"  {metrics.bot_name.upper()} - Daily Metrics for {metrics.date}")
    print("=" * 60)

    if metrics.total_trades == 0:
        print("  No trades recorded for this date.")
        print()
        return

    # Trade Summary
    print()
    print("  TRADE SUMMARY")
    print("  " + "-" * 40)
    print(f"  Total Trades:      {metrics.total_trades}")
    print(f"  Winners:           {metrics.winning_trades} ({metrics.win_rate:.1f}%)")
    print(f"  Losers:            {metrics.losing_trades}")
    print(f"  Scratch:           {metrics.scratch_trades}")

    # P&L
    print()
    print("  P&L SUMMARY")
    print("  " + "-" * 40)
    pnl_color = "\033[92m" if metrics.net_pnl >= 0 else "\033[91m"
    reset = "\033[0m"
    print(f"  Net P&L:           {pnl_color}${metrics.net_pnl:,.2f}{reset}")
    print(f"  Largest Win:       ${metrics.largest_win:,.2f}")
    print(f"  Largest Loss:      ${metrics.largest_loss:,.2f}")
    print(f"  Profit Factor:     {metrics.profit_factor:.2f}")

    # R-Multiples
    if metrics.total_r != 0:
        print()
        print("  R-MULTIPLE ANALYSIS")
        print("  " + "-" * 40)
        print(f"  Total R:           {metrics.total_r:+.2f}R")
        print(f"  Average R:         {metrics.avg_r:+.2f}R")

    # Execution
    print()
    print("  EXECUTION QUALITY")
    print("  " + "-" * 40)
    print(f"  Avg Hold Time:     {metrics.avg_hold_time_minutes:.1f} minutes")
    print(f"  Timeouts:          {metrics.timeout_count}")
    print(f"  Partial Fills:     {metrics.partial_fill_count}")
    print(f"  Errors:            {metrics.error_count}")

    print()
    print("=" * 60)
    print()


def print_week_summary(all_metrics: List[TradeMetrics], bot_name: str):
    """Print weekly summary across multiple days."""
    print()
    print("=" * 70)
    print(f"  {bot_name.upper()} - Weekly Summary")
    print("=" * 70)
    print()

    # Header
    print(f"  {'Date':<12} {'Trades':>7} {'Win%':>7} {'P&L':>10} {'Avg R':>8} {'Errors':>7}")
    print("  " + "-" * 58)

    total_trades = 0
    total_pnl = 0.0
    total_wins = 0
    total_errors = 0

    for m in all_metrics:
        pnl_str = f"${m.net_pnl:+,.2f}"
        print(f"  {m.date:<12} {m.total_trades:>7} {m.win_rate:>6.1f}% {pnl_str:>10} {m.avg_r:>+7.2f}R {m.error_count:>7}")
        total_trades += m.total_trades
        total_pnl += m.net_pnl
        total_wins += m.winning_trades
        total_errors += m.error_count

    print("  " + "-" * 58)

    overall_win_rate = (total_wins / total_trades * 100) if total_trades > 0 else 0
    pnl_str = f"${total_pnl:+,.2f}"
    print(f"  {'TOTAL':<12} {total_trades:>7} {overall_win_rate:>6.1f}% {pnl_str:>10} {'':>8} {total_errors:>7}")
    print()


def export_to_csv(all_metrics: List[TradeMetrics], output_path: Path):
    """Export metrics to CSV file."""
    import csv

    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow([
            'date', 'bot', 'total_trades', 'winners', 'losers', 'scratch',
            'win_rate', 'gross_pnl', 'net_pnl', 'largest_win', 'largest_loss',
            'profit_factor', 'total_r', 'avg_r', 'avg_hold_minutes',
            'timeouts', 'partial_fills', 'errors'
        ])

        for m in all_metrics:
            writer.writerow([
                m.date, m.bot_name, m.total_trades, m.winning_trades, m.losing_trades,
                m.scratch_trades, f"{m.win_rate:.2f}", f"{m.gross_pnl:.2f}",
                f"{m.net_pnl:.2f}", f"{m.largest_win:.2f}", f"{m.largest_loss:.2f}",
                f"{m.profit_factor:.2f}", f"{m.total_r:.2f}", f"{m.avg_r:.2f}",
                f"{m.avg_hold_time_minutes:.1f}", m.timeout_count, m.partial_fill_count,
                m.error_count
            ])

    print(f"Exported to: {output_path}")


def main():
    parser = argparse.ArgumentParser(description="Daily Metrics Tracker for Trading Bots")
    parser.add_argument("--date", type=str, help="Date to analyze (YYYY-MM-DD), default: today")
    parser.add_argument("--week", action="store_true", help="Show full week summary")
    parser.add_argument("--export", type=str, choices=["csv"], help="Export format")
    parser.add_argument("--bot", type=str, choices=["simple", "trend", "all"], default="all",
                       help="Which bot to analyze")
    args = parser.parse_args()

    # Determine date(s) to analyze
    if args.week:
        # Last 5 trading days
        today = datetime.now()
        dates = []
        current = today
        while len(dates) < 5:
            if current.weekday() < 5:  # Mon-Fri
                dates.append(current.strftime("%Y-%m-%d"))
            current -= timedelta(days=1)
        dates.reverse()
    elif args.date:
        dates = [args.date]
    else:
        dates = [datetime.now().strftime("%Y-%m-%d")]

    # Collect metrics
    all_simple_metrics = []
    all_trend_metrics = []

    for date in dates:
        if args.bot in ("simple", "all"):
            trades = load_trades_for_date(SIMPLE_BOT_JOURNAL, date)
            metrics = calculate_metrics(trades, date, "Simple Bot")
            all_simple_metrics.append(metrics)

        if args.bot in ("trend", "all"):
            trades = load_trades_for_date(TREND_BOT_JOURNAL, date)
            metrics = calculate_metrics(trades, date, "Trend Bot")
            all_trend_metrics.append(metrics)

    # Display
    if args.week:
        if all_simple_metrics:
            print_week_summary(all_simple_metrics, "Simple Bot")
        if all_trend_metrics:
            print_week_summary(all_trend_metrics, "Trend Bot")
    else:
        for m in all_simple_metrics:
            print_daily_report(m)
        for m in all_trend_metrics:
            print_daily_report(m)

    # Export
    if args.export == "csv":
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        if all_simple_metrics:
            export_to_csv(all_simple_metrics, METRICS_OUTPUT_DIR / f"simple_bot_metrics_{timestamp}.csv")
        if all_trend_metrics:
            export_to_csv(all_trend_metrics, METRICS_OUTPUT_DIR / f"trend_bot_metrics_{timestamp}.csv")


if __name__ == "__main__":
    main()
