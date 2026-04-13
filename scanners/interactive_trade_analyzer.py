"""
Interactive Trade Analyzer & Optimizer
=======================================

An interactive tool to analyze historical trades and optimize parameters.

Features:
1. Load trade logs from any bot backtest
2. Replay trades with visual price action context
3. Adjust stops/targets interactively and see impact
4. Identify patterns in winning vs losing trades
5. Generate optimized parameter recommendations

Usage:
    python interactive_trade_analyzer.py

Commands:
    load <file>     - Load a trade log CSV
    analyze         - Run full analysis on loaded trades
    replay [n]      - Replay trade #n with price context
    optimize        - Find optimal stop/target parameters
    compare         - Compare winners vs losers patterns
    export          - Export optimized parameters
    help            - Show all commands
    quit            - Exit

Author: Trading Bot Framework
Version: 1.0.0
"""

import os
import sys
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo
from collections import defaultdict

# ============================================================
# CONFIGURATION
# ============================================================

ET = ZoneInfo("America/New_York")
POLYGON_API_KEY = os.getenv("POLYGON_API_KEY", "eskzZ5O4QQyYpS5TsA3pe5WMeepeaxmW")

# Analysis parameters
STOP_TEST_RANGE = [0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]  # Stop distances to test (%)
TARGET_TEST_RANGE = [0.5, 0.75, 1.0, 1.25, 1.5, 1.75, 2.0]  # Target R multiples to test
TIME_BUCKETS = ["9:30-10:00", "10:00-11:00", "11:00-12:00", "12:00-13:00", "13:00-14:00", "14:00-15:00", "15:00-16:00"]


# ============================================================
# DATA STRUCTURES
# ============================================================

@dataclass
class TradeRecord:
    """A single trade from the backtest."""
    symbol: str
    entry_time: datetime
    exit_time: datetime
    entry_price: float
    exit_price: float
    qty: int
    bracket: str
    exit_reason: str
    pnl: float
    r_multiple: float
    hold_minutes: int

    # Extended analysis fields (populated later)
    max_favorable: float = 0.0  # Maximum favorable excursion (MFE)
    max_adverse: float = 0.0    # Maximum adverse excursion (MAE)
    time_bucket: str = ""
    volatility_regime: str = ""
    could_have_won: bool = False  # If different stop/target would have won


@dataclass
class OptimizationResult:
    """Result of parameter optimization."""
    stop_pct: float
    target_r: float
    win_rate: float
    profit_factor: float
    avg_r: float
    total_pnl: float
    sample_size: int


# ============================================================
# DATA FETCHING
# ============================================================

def fetch_minute_bars(symbol: str, date: datetime.date) -> Optional[pd.DataFrame]:
    """Fetch minute bars for a specific date."""
    start_date = date
    end_date = date + timedelta(days=1)

    url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/minute/{start_date}/{end_date}"
    params = {
        "adjusted": "true",
        "sort": "asc",
        "limit": 500,
        "apiKey": POLYGON_API_KEY
    }

    try:
        resp = requests.get(url, params=params, timeout=30)
        if resp.status_code != 200:
            return None

        data = resp.json()
        results = data.get("results", [])
        if not results:
            return None

        df = pd.DataFrame(results)
        df["timestamp"] = pd.to_datetime(df["t"], unit="ms", utc=True).dt.tz_convert(ET)
        df = df.rename(columns={"o": "open", "h": "high", "l": "low", "c": "close", "v": "volume"})
        df = df[["timestamp", "open", "high", "low", "close", "volume"]]

        # Filter RTH only
        df["hour"] = df["timestamp"].dt.hour
        df["minute"] = df["timestamp"].dt.minute
        df = df[
            ((df["hour"] == 9) & (df["minute"] >= 30)) |
            ((df["hour"] > 9) & (df["hour"] < 16))
        ].copy()
        df = df.drop(columns=["hour", "minute"])

        return df
    except:
        return None


# ============================================================
# TRADE ANALYSIS
# ============================================================

def calculate_excursions(trade: TradeRecord, price_data: pd.DataFrame) -> Tuple[float, float]:
    """
    Calculate Maximum Favorable Excursion (MFE) and Maximum Adverse Excursion (MAE).

    MFE = How far price moved in our favor before exit
    MAE = How far price moved against us before exit

    These are CRITICAL for understanding if our stops/targets are optimal.
    """
    # Filter to trade duration
    mask = (price_data["timestamp"] >= trade.entry_time) & (price_data["timestamp"] <= trade.exit_time)
    trade_bars = price_data[mask]

    if trade_bars.empty:
        return 0.0, 0.0

    # For long trades
    max_high = trade_bars["high"].max()
    min_low = trade_bars["low"].min()

    mfe = ((max_high - trade.entry_price) / trade.entry_price) * 100  # % favorable
    mae = ((trade.entry_price - min_low) / trade.entry_price) * 100   # % adverse

    return mfe, mae


def get_time_bucket(ts: datetime) -> str:
    """Categorize trade entry time into buckets."""
    hour = ts.hour
    if hour == 9:
        return "9:30-10:00"
    elif hour == 10:
        return "10:00-11:00"
    elif hour == 11:
        return "11:00-12:00"
    elif hour == 12:
        return "12:00-13:00"
    elif hour == 13:
        return "13:00-14:00"
    elif hour == 14:
        return "14:00-15:00"
    else:
        return "15:00-16:00"


def simulate_trade_with_params(trade: TradeRecord, price_data: pd.DataFrame,
                               stop_pct: float, target_r: float) -> Tuple[float, str]:
    """
    Simulate what would have happened with different stop/target.

    Returns: (pnl, exit_reason)
    """
    mask = (price_data["timestamp"] >= trade.entry_time)
    future_bars = price_data[mask]

    if future_bars.empty:
        return 0.0, "NO_DATA"

    entry = trade.entry_price
    stop_price = entry * (1 - stop_pct / 100)
    risk_per_share = entry - stop_price
    target_price = entry + (risk_per_share * target_r)

    # Walk through bars
    for _, bar in future_bars.iterrows():
        # Check stop first (conservative)
        if bar["low"] <= stop_price:
            return (stop_price - entry) / entry * 100, "STOP_LOSS"

        # Check target
        if bar["high"] >= target_price:
            return (target_price - entry) / entry * 100, "TAKE_PROFIT"

        # EOD close at 3:55 PM
        if bar["timestamp"].hour == 15 and bar["timestamp"].minute >= 55:
            return (bar["close"] - entry) / entry * 100, "EOD_CLOSE"

    # Didn't hit stop or target by end of data
    last_close = future_bars.iloc[-1]["close"]
    return (last_close - entry) / entry * 100, "HOLD"


# ============================================================
# INTERACTIVE ANALYZER
# ============================================================

class InteractiveAnalyzer:
    """Main interactive analysis tool."""

    def __init__(self):
        self.trades: List[TradeRecord] = []
        self.price_cache: Dict[str, pd.DataFrame] = {}
        self.current_file: str = ""

    def load_trades(self, filepath: str) -> bool:
        """Load trades from CSV file."""
        try:
            df = pd.read_csv(filepath)
            self.trades = []

            for _, row in df.iterrows():
                trade = TradeRecord(
                    symbol=row["symbol"],
                    entry_time=pd.to_datetime(row["entry_time"]),
                    exit_time=pd.to_datetime(row["exit_time"]),
                    entry_price=row["entry_price"],
                    exit_price=row["exit_price"],
                    qty=row["qty"],
                    bracket=row.get("bracket", "scalp"),
                    exit_reason=row["exit_reason"],
                    pnl=row["pnl"],
                    r_multiple=row["r_multiple"],
                    hold_minutes=row["hold_minutes"]
                )
                trade.time_bucket = get_time_bucket(trade.entry_time)
                self.trades.append(trade)

            self.current_file = filepath
            print(f"\n  Loaded {len(self.trades)} trades from {filepath}")
            return True

        except Exception as e:
            print(f"\n  Error loading file: {e}")
            return False

    def analyze_trades(self):
        """Run comprehensive analysis on loaded trades."""
        if not self.trades:
            print("\n  No trades loaded. Use 'load <file>' first.")
            return

        print(f"\n{'='*60}")
        print("COMPREHENSIVE TRADE ANALYSIS")
        print(f"{'='*60}")

        # Fetch price data and calculate excursions
        print("\nFetching price data for excursion analysis...")
        self._calculate_all_excursions()

        # Basic stats
        self._print_basic_stats()

        # Time analysis
        self._print_time_analysis()

        # Excursion analysis
        self._print_excursion_analysis()

        # Exit reason analysis
        self._print_exit_analysis()

        # Actionable insights
        self._print_insights()

    def _calculate_all_excursions(self):
        """Calculate MFE/MAE for all trades."""
        dates_needed = set()
        for trade in self.trades:
            dates_needed.add((trade.symbol, trade.entry_time.date()))

        # Fetch price data
        for symbol, date in dates_needed:
            cache_key = f"{symbol}_{date}"
            if cache_key not in self.price_cache:
                print(f"  Fetching {symbol} {date}...", end=" ")
                df = fetch_minute_bars(symbol, date)
                if df is not None:
                    self.price_cache[cache_key] = df
                    print("OK")
                else:
                    print("SKIP")

        # Calculate excursions
        for trade in self.trades:
            cache_key = f"{trade.symbol}_{trade.entry_time.date()}"
            if cache_key in self.price_cache:
                mfe, mae = calculate_excursions(trade, self.price_cache[cache_key])
                trade.max_favorable = mfe
                trade.max_adverse = mae

    def _print_basic_stats(self):
        """Print basic trade statistics."""
        print(f"\n[BASIC STATISTICS]")

        total = len(self.trades)
        winners = [t for t in self.trades if t.pnl > 0]
        losers = [t for t in self.trades if t.pnl <= 0]

        win_rate = len(winners) / total * 100 if total > 0 else 0
        total_pnl = sum(t.pnl for t in self.trades)
        avg_winner = sum(t.pnl for t in winners) / len(winners) if winners else 0
        avg_loser = sum(t.pnl for t in losers) / len(losers) if losers else 0

        print(f"   Total Trades: {total}")
        print(f"   Winners: {len(winners)} ({win_rate:.1f}%)")
        print(f"   Losers: {len(losers)} ({100-win_rate:.1f}%)")
        print(f"   Total P&L: ${total_pnl:,.2f}")
        print(f"   Avg Winner: ${avg_winner:,.2f}")
        print(f"   Avg Loser: ${avg_loser:,.2f}")
        print(f"   Avg R-Multiple: {sum(t.r_multiple for t in self.trades)/total:.2f}R")

    def _print_time_analysis(self):
        """Analyze performance by time of day."""
        print(f"\n[TIME OF DAY ANALYSIS]")

        by_time = defaultdict(list)
        for trade in self.trades:
            by_time[trade.time_bucket].append(trade)

        print(f"   {'Time Bucket':<15} {'Trades':>7} {'Win%':>7} {'Avg R':>8} {'P&L':>10}")
        print(f"   {'-'*50}")

        best_time = None
        best_wr = 0
        worst_time = None
        worst_wr = 100

        for bucket in TIME_BUCKETS:
            trades = by_time.get(bucket, [])
            if not trades:
                continue

            n = len(trades)
            wr = sum(1 for t in trades if t.pnl > 0) / n * 100
            avg_r = sum(t.r_multiple for t in trades) / n
            pnl = sum(t.pnl for t in trades)

            print(f"   {bucket:<15} {n:>7} {wr:>6.1f}% {avg_r:>7.2f}R ${pnl:>9,.2f}")

            if wr > best_wr:
                best_wr = wr
                best_time = bucket
            if wr < worst_wr:
                worst_wr = wr
                worst_time = bucket

        print(f"\n   Best time: {best_time} ({best_wr:.1f}% WR)")
        print(f"   Worst time: {worst_time} ({worst_wr:.1f}% WR)")

    def _print_excursion_analysis(self):
        """Analyze MFE/MAE to understand stop/target efficiency."""
        print(f"\n[EXCURSION ANALYSIS (MFE/MAE)]")

        winners = [t for t in self.trades if t.pnl > 0 and t.max_favorable > 0]
        losers = [t for t in self.trades if t.pnl <= 0 and t.max_adverse > 0]

        if winners:
            avg_mfe_win = sum(t.max_favorable for t in winners) / len(winners)
            avg_mae_win = sum(t.max_adverse for t in winners) / len(winners)
            print(f"\n   WINNERS ({len(winners)} trades):")
            print(f"      Avg Max Favorable Excursion: {avg_mfe_win:.2f}%")
            print(f"      Avg Max Adverse Excursion: {avg_mae_win:.2f}%")
            print(f"      Insight: Winners went {avg_mfe_win:.2f}% in our favor before exiting")

        if losers:
            avg_mfe_lose = sum(t.max_favorable for t in losers) / len(losers)
            avg_mae_lose = sum(t.max_adverse for t in losers) / len(losers)
            print(f"\n   LOSERS ({len(losers)} trades):")
            print(f"      Avg Max Favorable Excursion: {avg_mfe_lose:.2f}%")
            print(f"      Avg Max Adverse Excursion: {avg_mae_lose:.2f}%")

            # Key insight
            if avg_mfe_lose > 0.3:
                print(f"      *** INSIGHT: Losers averaged {avg_mfe_lose:.2f}% MFE before hitting stop!")
                print(f"          Consider taking partial profits at {avg_mfe_lose*0.7:.2f}%")

        # Stop analysis
        all_maes = [t.max_adverse for t in self.trades if t.max_adverse > 0]
        if all_maes:
            mae_median = np.median(all_maes)
            mae_75 = np.percentile(all_maes, 75)
            mae_90 = np.percentile(all_maes, 90)

            print(f"\n   STOP PLACEMENT ANALYSIS:")
            print(f"      Median MAE: {mae_median:.2f}%")
            print(f"      75th percentile MAE: {mae_75:.2f}%")
            print(f"      90th percentile MAE: {mae_90:.2f}%")
            print(f"      Recommended stop: {mae_75:.2f}% - {mae_90:.2f}%")

    def _print_exit_analysis(self):
        """Analyze exit reasons."""
        print(f"\n[EXIT REASON ANALYSIS]")

        by_reason = defaultdict(list)
        for trade in self.trades:
            by_reason[trade.exit_reason].append(trade)

        print(f"   {'Exit Reason':<20} {'Count':>7} {'Win%':>7} {'Avg R':>8} {'P&L':>10}")
        print(f"   {'-'*55}")

        for reason, trades in sorted(by_reason.items(), key=lambda x: -len(x[1])):
            n = len(trades)
            wr = sum(1 for t in trades if t.pnl > 0) / n * 100
            avg_r = sum(t.r_multiple for t in trades) / n
            pnl = sum(t.pnl for t in trades)

            print(f"   {reason:<20} {n:>7} {wr:>6.1f}% {avg_r:>7.2f}R ${pnl:>9,.2f}")

    def _print_insights(self):
        """Print actionable insights based on analysis."""
        print(f"\n{'='*60}")
        print("ACTIONABLE INSIGHTS")
        print(f"{'='*60}")

        # Calculate key metrics
        losers = [t for t in self.trades if t.pnl <= 0]
        winners = [t for t in self.trades if t.pnl > 0]

        insights = []

        # Insight 1: Stop too tight?
        stopped_out = [t for t in losers if "STOP" in t.exit_reason]
        if stopped_out:
            avg_mae_stopped = sum(t.max_adverse for t in stopped_out if t.max_adverse > 0) / len(stopped_out) if stopped_out else 0
            avg_mfe_stopped = sum(t.max_favorable for t in stopped_out if t.max_favorable > 0) / len(stopped_out) if stopped_out else 0

            if avg_mfe_stopped > 0.3:
                insights.append(
                    f"1. STOPS TOO TIGHT: {len(stopped_out)} stop-outs had avg MFE of {avg_mfe_stopped:.2f}%.\n"
                    f"   Many would have been winners with patience. Consider wider stops."
                )

        # Insight 2: Taking profit too early?
        if winners:
            high_mfe_winners = [t for t in winners if t.max_favorable > t.r_multiple * 1.5]
            if len(high_mfe_winners) > len(winners) * 0.3:
                avg_left = sum(t.max_favorable - t.r_multiple for t in high_mfe_winners) / len(high_mfe_winners)
                insights.append(
                    f"2. LEAVING MONEY ON TABLE: {len(high_mfe_winners)} winners ({len(high_mfe_winners)/len(winners)*100:.0f}%) "
                    f"had room to run.\n   Avg additional {avg_left:.2f}R left unrealized. Consider trailing stops."
                )

        # Insight 3: Time filter
        by_time = defaultdict(list)
        for trade in self.trades:
            by_time[trade.time_bucket].append(trade)

        for bucket, trades in by_time.items():
            if len(trades) >= 5:
                wr = sum(1 for t in trades if t.pnl > 0) / len(trades) * 100
                if wr < 40:
                    insights.append(
                        f"3. AVOID {bucket}: Only {wr:.0f}% win rate ({len(trades)} trades).\n"
                        f"   Consider skipping trades during this time."
                    )
                    break

        # Insight 4: Symbol performance
        by_symbol = defaultdict(list)
        for trade in self.trades:
            by_symbol[trade.symbol].append(trade)

        worst_symbols = []
        for symbol, trades in by_symbol.items():
            if len(trades) >= 3:
                wr = sum(1 for t in trades if t.pnl > 0) / len(trades) * 100
                if wr < 35:
                    worst_symbols.append((symbol, wr, len(trades)))

        if worst_symbols:
            worst_symbols.sort(key=lambda x: x[1])
            symbols_str = ", ".join(f"{s[0]} ({s[1]:.0f}%)" for s in worst_symbols[:3])
            insights.append(
                f"4. PROBLEM SYMBOLS: {symbols_str}\n"
                f"   Consider removing from universe or adjusting parameters."
            )

        # Print insights
        if insights:
            for insight in insights:
                print(f"\n{insight}")
        else:
            print("\n   No major issues detected. Strategy looks well-calibrated.")

    def optimize_parameters(self):
        """Find optimal stop/target parameters through simulation."""
        if not self.trades:
            print("\n  No trades loaded. Use 'load <file>' first.")
            return

        print(f"\n{'='*60}")
        print("PARAMETER OPTIMIZATION")
        print(f"{'='*60}")
        print("\nTesting stop/target combinations...")

        results: List[OptimizationResult] = []

        total_combos = len(STOP_TEST_RANGE) * len(TARGET_TEST_RANGE)
        tested = 0

        for stop_pct in STOP_TEST_RANGE:
            for target_r in TARGET_TEST_RANGE:
                tested += 1
                print(f"\r  Testing {tested}/{total_combos}: stop={stop_pct}%, target={target_r}R...", end="")

                # Simulate all trades with these parameters
                sim_results = []

                for trade in self.trades:
                    cache_key = f"{trade.symbol}_{trade.entry_time.date()}"
                    if cache_key not in self.price_cache:
                        continue

                    pnl_pct, exit_reason = simulate_trade_with_params(
                        trade, self.price_cache[cache_key], stop_pct, target_r
                    )

                    # Convert to R-multiple
                    r_mult = pnl_pct / stop_pct if stop_pct > 0 else 0
                    sim_results.append({
                        "pnl_pct": pnl_pct,
                        "r_mult": r_mult,
                        "exit": exit_reason,
                        "won": pnl_pct > 0
                    })

                if not sim_results:
                    continue

                # Calculate metrics
                n = len(sim_results)
                wins = sum(1 for r in sim_results if r["won"])
                wr = wins / n * 100

                gross_profit = sum(r["pnl_pct"] for r in sim_results if r["pnl_pct"] > 0)
                gross_loss = abs(sum(r["pnl_pct"] for r in sim_results if r["pnl_pct"] <= 0))
                pf = gross_profit / gross_loss if gross_loss > 0 else 999

                avg_r = sum(r["r_mult"] for r in sim_results) / n
                total_pnl = sum(r["pnl_pct"] for r in sim_results)

                results.append(OptimizationResult(
                    stop_pct=stop_pct,
                    target_r=target_r,
                    win_rate=wr,
                    profit_factor=pf,
                    avg_r=avg_r,
                    total_pnl=total_pnl,
                    sample_size=n
                ))

        print("\n")

        if not results:
            print("  No valid results. Need price data for simulation.")
            return

        # Sort by profit factor (most important metric)
        results.sort(key=lambda x: x.profit_factor, reverse=True)

        print(f"\n[TOP 10 PARAMETER COMBINATIONS]")
        print(f"   {'Stop%':>6} {'Target':>7} {'Win%':>7} {'PF':>6} {'Avg R':>7} {'Total%':>8}")
        print(f"   {'-'*45}")

        for r in results[:10]:
            print(f"   {r.stop_pct:>5.1f}% {r.target_r:>6.2f}R {r.win_rate:>6.1f}% "
                  f"{r.profit_factor:>5.2f} {r.avg_r:>6.2f}R {r.total_pnl:>7.2f}%")

        # Best by each metric
        print(f"\n[BEST BY METRIC]")

        best_wr = max(results, key=lambda x: x.win_rate)
        print(f"   Highest Win Rate: stop={best_wr.stop_pct}%, target={best_wr.target_r}R -> {best_wr.win_rate:.1f}% WR")

        best_pf = max(results, key=lambda x: x.profit_factor)
        print(f"   Highest Profit Factor: stop={best_pf.stop_pct}%, target={best_pf.target_r}R -> {best_pf.profit_factor:.2f} PF")

        best_r = max(results, key=lambda x: x.avg_r)
        print(f"   Highest Avg R: stop={best_r.stop_pct}%, target={best_r.target_r}R -> {best_r.avg_r:.2f}R")

        # Recommendation
        print(f"\n[RECOMMENDATION]")
        # Weight: 40% PF, 30% WR, 30% Avg R
        for r in results:
            r.score = (r.profit_factor / 3) * 0.4 + (r.win_rate / 100) * 0.3 + (r.avg_r + 1) * 0.3

        results.sort(key=lambda x: x.score, reverse=True)
        best = results[0]

        print(f"   Optimal Parameters:")
        print(f"      Stop Loss: {best.stop_pct}%")
        print(f"      Take Profit: {best.target_r}R ({best.target_r * best.stop_pct:.2f}%)")
        print(f"      Expected Win Rate: {best.win_rate:.1f}%")
        print(f"      Expected Profit Factor: {best.profit_factor:.2f}")
        print(f"      Expected Avg R: {best.avg_r:.2f}")

    def replay_trade(self, trade_num: int):
        """Replay a specific trade with visual context."""
        if not self.trades:
            print("\n  No trades loaded.")
            return

        if trade_num < 1 or trade_num > len(self.trades):
            print(f"\n  Invalid trade number. Use 1-{len(self.trades)}")
            return

        trade = self.trades[trade_num - 1]

        print(f"\n{'='*60}")
        print(f"TRADE #{trade_num} REPLAY")
        print(f"{'='*60}")

        print(f"\n   Symbol: {trade.symbol}")
        print(f"   Entry: {trade.entry_time} @ ${trade.entry_price:.2f}")
        print(f"   Exit: {trade.exit_time} @ ${trade.exit_price:.2f}")
        print(f"   Qty: {trade.qty} shares")
        print(f"   P&L: ${trade.pnl:.2f} ({trade.r_multiple:.2f}R)")
        print(f"   Exit Reason: {trade.exit_reason}")
        print(f"   Hold Time: {trade.hold_minutes} minutes")

        # Get price data
        cache_key = f"{trade.symbol}_{trade.entry_time.date()}"
        if cache_key not in self.price_cache:
            df = fetch_minute_bars(trade.symbol, trade.entry_time.date())
            if df is not None:
                self.price_cache[cache_key] = df

        if cache_key not in self.price_cache:
            print("\n   Could not fetch price data for visualization.")
            return

        df = self.price_cache[cache_key]

        # Excursion analysis
        mfe, mae = calculate_excursions(trade, df)
        print(f"\n   Max Favorable Excursion: {mfe:.2f}%")
        print(f"   Max Adverse Excursion: {mae:.2f}%")

        # Visual chart (ASCII)
        print(f"\n   PRICE ACTION:")
        self._draw_ascii_chart(trade, df)

        # What-if analysis
        print(f"\n   WHAT-IF ANALYSIS:")
        for target_r in [0.5, 1.0, 1.5, 2.0]:
            for stop_pct in [0.5, 0.75, 1.0]:
                pnl_pct, exit = simulate_trade_with_params(trade, df, stop_pct, target_r)
                r_mult = pnl_pct / stop_pct if stop_pct > 0 else 0
                result = "WIN" if pnl_pct > 0 else "LOSS"
                print(f"      stop={stop_pct}%, target={target_r}R -> {exit:12} | {result} | {r_mult:+.2f}R")

    def _draw_ascii_chart(self, trade: TradeRecord, df: pd.DataFrame):
        """Draw simple ASCII chart of the trade."""
        # Filter to trade window + buffer
        start = trade.entry_time - timedelta(minutes=10)
        end = trade.exit_time + timedelta(minutes=10)

        mask = (df["timestamp"] >= start) & (df["timestamp"] <= end)
        trade_df = df[mask].copy()

        if trade_df.empty:
            return

        # Normalize prices to 0-20 range for display
        min_price = trade_df["low"].min()
        max_price = trade_df["high"].max()
        price_range = max_price - min_price

        if price_range <= 0:
            return

        def normalize(price):
            return int((price - min_price) / price_range * 20)

        entry_y = normalize(trade.entry_price)
        exit_y = normalize(trade.exit_price)

        # Build chart
        width = min(len(trade_df), 60)
        chart = [[' ' for _ in range(width)] for _ in range(22)]

        # Plot price bars
        step = max(1, len(trade_df) // width)
        for i, (_, row) in enumerate(trade_df.iloc[::step].iterrows()):
            if i >= width:
                break

            high_y = normalize(row["high"])
            low_y = normalize(row["low"])
            close_y = normalize(row["close"])

            for y in range(low_y, high_y + 1):
                chart[20 - y][i] = '|'
            chart[20 - close_y][i] = '*'

        # Mark entry and exit
        entry_idx = 0
        exit_idx = min(width - 1, int(trade.hold_minutes / step))

        chart[20 - entry_y][entry_idx] = 'E'
        chart[20 - exit_y][exit_idx] = 'X'

        # Print chart
        print(f"\n      ${max_price:.2f} |", end="")
        for row in chart:
            print(''.join(row))
            print("             |", end="")
        print(f"\n      ${min_price:.2f} |")
        print(f"             E=Entry  X=Exit  *=Close  |=Range")

    def compare_winners_losers(self):
        """Compare characteristics of winners vs losers."""
        if not self.trades:
            print("\n  No trades loaded.")
            return

        winners = [t for t in self.trades if t.pnl > 0]
        losers = [t for t in self.trades if t.pnl <= 0]

        print(f"\n{'='*60}")
        print("WINNERS vs LOSERS COMPARISON")
        print(f"{'='*60}")

        print(f"\n   {'Metric':<25} {'Winners':>15} {'Losers':>15}")
        print(f"   {'-'*55}")

        # Count
        print(f"   {'Count':<25} {len(winners):>15} {len(losers):>15}")

        # Avg hold time
        avg_hold_win = sum(t.hold_minutes for t in winners) / len(winners) if winners else 0
        avg_hold_lose = sum(t.hold_minutes for t in losers) / len(losers) if losers else 0
        print(f"   {'Avg Hold (mins)':<25} {avg_hold_win:>15.1f} {avg_hold_lose:>15.1f}")

        # MFE
        avg_mfe_win = sum(t.max_favorable for t in winners if t.max_favorable > 0) / len(winners) if winners else 0
        avg_mfe_lose = sum(t.max_favorable for t in losers if t.max_favorable > 0) / len(losers) if losers else 0
        print(f"   {'Avg MFE (%)':<25} {avg_mfe_win:>15.2f} {avg_mfe_lose:>15.2f}")

        # MAE
        avg_mae_win = sum(t.max_adverse for t in winners if t.max_adverse > 0) / len(winners) if winners else 0
        avg_mae_lose = sum(t.max_adverse for t in losers if t.max_adverse > 0) / len(losers) if losers else 0
        print(f"   {'Avg MAE (%)':<25} {avg_mae_win:>15.2f} {avg_mae_lose:>15.2f}")

        # Time of day
        def best_time(trades):
            by_time = defaultdict(int)
            for t in trades:
                by_time[t.time_bucket] += 1
            return max(by_time.items(), key=lambda x: x[1])[0] if by_time else "N/A"

        print(f"   {'Most Common Time':<25} {best_time(winners):>15} {best_time(losers):>15}")

        # Avg R
        avg_r_win = sum(t.r_multiple for t in winners) / len(winners) if winners else 0
        avg_r_lose = sum(t.r_multiple for t in losers) / len(losers) if losers else 0
        print(f"   {'Avg R-Multiple':<25} {avg_r_win:>15.2f} {avg_r_lose:>15.2f}")

    def run(self):
        """Main interactive loop."""
        print(f"\n{'='*60}")
        print("INTERACTIVE TRADE ANALYZER")
        print(f"{'='*60}")
        print("\nCommands: load, analyze, replay, optimize, compare, help, quit")
        print("Example: load simple_bot_backtest_trades.csv")

        while True:
            try:
                cmd = input("\n> ").strip().lower()

                if not cmd:
                    continue

                parts = cmd.split()
                action = parts[0]

                if action == "quit" or action == "exit" or action == "q":
                    print("\nGoodbye!")
                    break

                elif action == "help" or action == "h":
                    self._print_help()

                elif action == "load":
                    if len(parts) < 2:
                        # List available files
                        csv_files = [f for f in os.listdir('.') if f.endswith('_trades.csv')]
                        if csv_files:
                            print("\n  Available trade logs:")
                            for i, f in enumerate(csv_files, 1):
                                print(f"    {i}. {f}")
                            print("\n  Usage: load <filename> or load <number>")
                        else:
                            print("\n  No trade logs found. Run a backtest first.")
                    else:
                        filename = parts[1]
                        # Check if user entered a number
                        try:
                            idx = int(filename) - 1
                            csv_files = [f for f in os.listdir('.') if f.endswith('_trades.csv')]
                            if 0 <= idx < len(csv_files):
                                filename = csv_files[idx]
                        except ValueError:
                            pass
                        self.load_trades(filename)

                elif action == "analyze" or action == "a":
                    self.analyze_trades()

                elif action == "replay" or action == "r":
                    if len(parts) < 2:
                        print("\n  Usage: replay <trade_number>")
                        print(f"  Available: 1-{len(self.trades)}")
                    else:
                        try:
                            trade_num = int(parts[1])
                            self.replay_trade(trade_num)
                        except ValueError:
                            print("\n  Invalid trade number.")

                elif action == "optimize" or action == "o":
                    self.optimize_parameters()

                elif action == "compare" or action == "c":
                    self.compare_winners_losers()

                elif action == "list" or action == "l":
                    if not self.trades:
                        print("\n  No trades loaded.")
                    else:
                        print(f"\n  Loaded {len(self.trades)} trades from {self.current_file}")
                        print(f"\n  {'#':>4} {'Symbol':>8} {'Entry Time':>20} {'P&L':>10} {'R':>6} {'Exit':>15}")
                        print(f"  {'-'*65}")
                        for i, t in enumerate(self.trades[:20], 1):
                            print(f"  {i:>4} {t.symbol:>8} {t.entry_time.strftime('%Y-%m-%d %H:%M'):>20} "
                                  f"${t.pnl:>9.2f} {t.r_multiple:>5.2f}R {t.exit_reason:>15}")
                        if len(self.trades) > 20:
                            print(f"  ... and {len(self.trades) - 20} more trades")

                else:
                    print(f"\n  Unknown command: {action}. Type 'help' for commands.")

            except KeyboardInterrupt:
                print("\n\nUse 'quit' to exit.")
            except Exception as e:
                print(f"\n  Error: {e}")

    def _print_help(self):
        """Print help message."""
        print(f"""
  COMMANDS:
  ---------
  load [file]     Load trade log CSV (or list available files)
  analyze         Run comprehensive analysis on loaded trades
  replay <n>      Replay trade #n with price context and what-if
  optimize        Find optimal stop/target parameters
  compare         Compare winners vs losers characteristics
  list            Show loaded trades
  help            Show this help message
  quit            Exit the analyzer

  WORKFLOW:
  ---------
  1. Run a backtest to generate a _trades.csv file
  2. load <filename> to load the trades
  3. analyze to see comprehensive statistics
  4. optimize to find better stop/target parameters
  5. replay <n> to deep-dive into specific trades
  6. compare to understand winner vs loser patterns
""")


# ============================================================
# MAIN
# ============================================================

def main():
    """Run the interactive analyzer."""
    analyzer = InteractiveAnalyzer()
    analyzer.run()


if __name__ == "__main__":
    main()
