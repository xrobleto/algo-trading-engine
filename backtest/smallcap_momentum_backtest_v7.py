"""
Small Cap Momentum Backtest - V7 COMPREHENSIVE APPROACH TEST
=============================================================

Testing 5 different approaches to fix the loser-to-winner ratio:

1. BASELINE - V5 settings (3x ATR stops, 0.75R/1.5R TPs)
2. TIGHT_STOPS - Tighter stops (2x ATR), wider targets (1R/2R)
3. TIME_EXIT - Exit if underwater after 8 bars
4. VOL_FILTER - Skip high volatility setups (ATR > 4%)
5. COMBINED - Best settings from all approaches

Version: 7.0.0
"""

import os
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple
from enum import Enum
from zoneinfo import ZoneInfo
from collections import defaultdict

ET = ZoneInfo("America/New_York")
POLYGON_API_KEY = os.getenv("POLYGON_API_KEY", "eskzZ5O4QQyYpS5TsA3pe5WMeepeaxmW")

# Base settings
BACKTEST_DAYS = 180
INITIAL_CAPITAL = 35_000
MIN_PRICE = 2.00
MAX_PRICE = 25.00
MIN_PCT_CHANGE = 5.0
MIN_RELATIVE_VOLUME = 2.0
MIN_ABSOLUTE_VOLUME = 200_000
TRADING_START_MINUTES = 10
TRADING_END_HOUR = 11
EOD_CLOSE_HOUR = 15
EOD_CLOSE_MINUTE = 55
FLAG_POLE_MIN_PCT = 3.0
FLAG_MAX_RETRACE_PCT = 50.0
FLAG_MIN_BARS = 2
FLAG_MAX_BARS = 15
ATR_PERIOD = 14
MAX_RISK_PER_TRADE = 150.00
MAX_POSITIONS = 1


@dataclass
class ApproachConfig:
    """Configuration for each approach."""
    name: str
    atr_mult: float = 3.0
    min_stop_pct: float = 2.5
    tp1_r: float = 0.75
    tp2_r: float = 1.5
    use_time_exit: bool = False
    time_exit_bars: int = 8
    use_vol_filter: bool = False
    max_atr_pct: float = 5.0
    trail_pct: float = 2.0


# Define approach configs
APPROACHES = {
    "BASELINE": ApproachConfig(
        name="BASELINE",
        atr_mult=3.0,
        min_stop_pct=2.5,
        tp1_r=0.75,
        tp2_r=1.5,
    ),
    "QUICK_SCALP": ApproachConfig(
        name="QUICK_SCALP",
        atr_mult=3.0,
        min_stop_pct=2.5,
        tp1_r=0.5,       # Very quick first take
        tp2_r=1.0,       # 1:1 R/R for second
        trail_pct=1.5,   # Tighter trail
    ),
    "HIGH_RR": ApproachConfig(
        name="HIGH_RR",
        atr_mult=3.0,
        min_stop_pct=2.5,
        tp1_r=1.0,       # Wait for 1R before first take
        tp2_r=2.5,       # Let runners run to 2.5R
        trail_pct=2.0,
    ),
    "FAST_EXIT": ApproachConfig(
        name="FAST_EXIT",
        atr_mult=3.0,
        min_stop_pct=2.5,
        tp1_r=0.5,
        tp2_r=1.0,
        use_time_exit=True,
        time_exit_bars=5,  # Very fast exit if underwater
    ),
    "WIDE_STOP": ApproachConfig(
        name="WIDE_STOP",
        atr_mult=4.0,     # Wider stop
        min_stop_pct=3.5, # Wider min
        tp1_r=0.5,        # But take profits quick
        tp2_r=1.0,
    ),
    "OPTIMAL": ApproachConfig(
        name="OPTIMAL",
        atr_mult=3.5,     # Slightly wider stops
        min_stop_pct=3.0,
        tp1_r=0.5,        # Quick first take
        tp2_r=1.0,        # 1:1 for second
        use_time_exit=True,
        time_exit_bars=6, # Exit quick if not working
        trail_pct=1.5,    # Tight trail for protection
    ),
    "BEST_MIX": ApproachConfig(
        name="BEST_MIX",
        atr_mult=4.0,     # Wide stop from WIDE_STOP (best WR)
        min_stop_pct=3.5,
        tp1_r=1.0,        # From HIGH_RR (best PF)
        tp2_r=2.5,
        trail_pct=2.0,
    ),
}


class ExitReason(Enum):
    STOP_LOSS = "STOP_LOSS"
    TP1 = "TP1"
    TP2 = "TP2"
    TRAILING_STOP = "TRAILING_STOP"
    BREAKEVEN = "BREAKEVEN"
    EOD_CLOSE = "EOD_CLOSE"
    TIME_EXIT = "TIME_EXIT"


@dataclass
class DailyGainer:
    symbol: str
    date: datetime
    gap_pct: float
    open_price: float
    prev_close: float
    volume: int
    rel_volume: float


@dataclass
class TradeSetup:
    symbol: str
    timestamp: datetime
    entry_price: float
    stop_price: float
    risk_per_share: float
    atr_pct: float = 0.0


@dataclass
class Position:
    symbol: str
    entry_time: datetime
    entry_price: float
    stop_price: float
    risk_per_share: float
    total_qty: int
    remaining_qty: int
    highest_price: float = 0.0
    trail_active: bool = False
    trail_stop: float = 0.0
    be_active: bool = False
    tp1_taken: bool = False
    tp2_taken: bool = False
    tp1_qty: int = 0
    tp2_qty: int = 0
    bars_since_entry: int = 0


@dataclass
class BacktestTrade:
    symbol: str
    date: str
    entry_time: datetime
    exit_time: datetime
    entry_price: float
    exit_price: float
    qty: int
    exit_reason: str
    pnl: float
    r_multiple: float
    hold_minutes: int


@dataclass
class BacktestResult:
    approach_name: str = ""
    total_trades: int = 0
    winners: int = 0
    losers: int = 0
    win_rate: float = 0.0
    gross_profit: float = 0.0
    gross_loss: float = 0.0
    net_pnl: float = 0.0
    profit_factor: float = 0.0
    avg_winner: float = 0.0
    avg_loser: float = 0.0
    avg_r: float = 0.0
    max_drawdown: float = 0.0
    final_capital: float = 0.0
    exit_stats: Dict[str, dict] = field(default_factory=dict)
    trades: List[BacktestTrade] = field(default_factory=list)


def fetch_daily_gainers_historical(days: int = 90) -> Dict[str, List[DailyGainer]]:
    """Fetch historical gap-up data."""
    print("Building historical gainer database...")

    SCAN_UNIVERSE = [
        "SNDL", "TLRY", "ACB", "CGC", "CRON", "VFF",
        "WKHS", "LCID", "RIVN", "BLNK", "CHPT", "QS", "EVGO", "XPEV", "NIO",
        "MARA", "RIOT", "BITF", "CLSK", "CIFR",
        "SPCE", "RKLB", "LUNR", "RDW", "MNTS",
        "AMC", "SOFI", "HOOD",
        "PLTR", "IONQ", "QUBT", "SOUN", "BBAI",
        "CRSP", "EDIT", "NTLA", "ARCT", "NVAX", "INO",
        "PATH", "DKNG", "PENN", "FUBO", "OPEN",
        "CLF", "BKKT", "IMPP", "OPAD", "TMC", "LIDR", "MLGO",
    ]

    gainers_by_date: Dict[str, List[DailyGainer]] = defaultdict(list)
    end_date = datetime.now(ET).date()
    start_date = end_date - timedelta(days=days + 10)

    for symbol in SCAN_UNIVERSE:
        print(f"  {symbol}...", end=" ")
        try:
            url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/day/{start_date}/{end_date}"
            params = {"adjusted": "true", "sort": "asc", "limit": 200, "apiKey": POLYGON_API_KEY}
            resp = requests.get(url, params=params, timeout=15)
            if resp.status_code != 200:
                print("SKIP")
                continue
            results = resp.json().get("results", [])
            if len(results) < 25:
                print("SKIP")
                continue

            df = pd.DataFrame(results)
            df["date"] = pd.to_datetime(df["t"], unit="ms", utc=True).dt.tz_convert(ET).dt.date
            df = df.rename(columns={"o": "open", "h": "high", "l": "low", "c": "close", "v": "volume"})
            df["avg_vol_20"] = df["volume"].rolling(20).mean()
            df["prev_close"] = df["close"].shift(1)
            df["gap_pct"] = ((df["open"] - df["prev_close"]) / df["prev_close"]) * 100
            df["rel_vol"] = df["volume"] / df["avg_vol_20"]

            gap_days = df[
                (df["gap_pct"] >= MIN_PCT_CHANGE) &
                (df["open"] >= MIN_PRICE) &
                (df["open"] <= MAX_PRICE) &
                (df["volume"] >= MIN_ABSOLUTE_VOLUME) &
                (df["rel_vol"] >= MIN_RELATIVE_VOLUME)
            ]

            if len(gap_days) > 0:
                print(f"OK ({len(gap_days)})")
                for _, row in gap_days.iterrows():
                    gainers_by_date[str(row["date"])].append(DailyGainer(
                        symbol=symbol, date=row["date"], gap_pct=row["gap_pct"],
                        open_price=row["open"], prev_close=row["prev_close"],
                        volume=int(row["volume"]), rel_volume=row["rel_vol"]
                    ))
            else:
                print("OK (0)")
        except Exception as e:
            print(f"ERR")

    for d in gainers_by_date:
        gainers_by_date[d].sort(key=lambda x: x.gap_pct, reverse=True)

    print(f"\nFound {sum(len(v) for v in gainers_by_date.values())} gap events")
    return dict(gainers_by_date)


def fetch_minute_bars(symbol: str, date: datetime) -> Optional[pd.DataFrame]:
    """Fetch minute bars."""
    date_str = date.strftime("%Y-%m-%d")
    url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/minute/{date_str}/{date_str}"
    params = {"adjusted": "true", "sort": "asc", "limit": 50000, "apiKey": POLYGON_API_KEY}
    try:
        resp = requests.get(url, params=params, timeout=15)
        if resp.status_code != 200:
            return None
        results = resp.json().get("results", [])
        if not results:
            return None
        df = pd.DataFrame(results)
        df["timestamp"] = pd.to_datetime(df["t"], unit="ms", utc=True).dt.tz_convert(ET)
        df = df.rename(columns={"o": "open", "h": "high", "l": "low", "c": "close", "v": "volume"})
        df = df[["timestamp", "open", "high", "low", "close", "volume"]]
        df["hour"] = df["timestamp"].dt.hour
        df["minute"] = df["timestamp"].dt.minute
        df = df[((df["hour"] == 9) & (df["minute"] >= 30)) | ((df["hour"] > 9) & (df["hour"] < 16))].copy()
        df = df.drop(columns=["hour", "minute"]).reset_index(drop=True)
        return df
    except:
        return None


def calculate_atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    """Calculate ATR."""
    tr1 = df["high"] - df["low"]
    tr2 = abs(df["high"] - df["close"].shift(1))
    tr3 = abs(df["low"] - df["close"].shift(1))
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    return tr.rolling(window=period).mean()


def detect_bull_flag(df: pd.DataFrame, idx: int, gainer: DailyGainer,
                     config: ApproachConfig) -> Optional[TradeSetup]:
    """Detect bull flag with config-based stops."""
    if idx < 15:
        return None

    ts = df["timestamp"].iloc[idx]
    current_price = df["close"].iloc[idx]
    lookback = df.iloc[max(0, idx-20):idx+1]

    # Find pole
    best_pole = None
    best_pole_move = 0
    for i in range(len(lookback) - 10):
        segment = lookback.iloc[i:i+8]
        pole_low = segment["low"].min()
        pole_high = segment["high"].max()
        pole_move = ((pole_high - pole_low) / pole_low) * 100
        if pole_move > best_pole_move and pole_move >= FLAG_POLE_MIN_PCT:
            best_pole = (i, pole_low, pole_high, segment["high"].idxmax())
            best_pole_move = pole_move

    if best_pole is None:
        return None

    _, pole_low, pole_high, pole_high_idx = best_pole
    pole_end_loc = lookback.index.get_loc(pole_high_idx)
    flag_bars = lookback.iloc[pole_end_loc:]

    if len(flag_bars) < FLAG_MIN_BARS or len(flag_bars) > FLAG_MAX_BARS:
        return None

    flag_high = flag_bars["high"].max()
    flag_low = flag_bars["low"].min()
    pole_range = pole_high - pole_low
    if pole_range <= 0:
        return None
    retracement = (pole_high - flag_low) / pole_range * 100
    if retracement > FLAG_MAX_RETRACE_PCT or flag_high > pole_high * 1.01:
        return None

    entry_price = flag_high + 0.02

    # Calculate stop using config
    atr_pct = 0.0
    if "atr" in df.columns:
        atr = df.iloc[idx]["atr"]
        if pd.notna(atr) and atr > 0:
            atr_stop = atr * config.atr_mult
            min_stop = entry_price * (config.min_stop_pct / 100)
            stop_distance = max(atr_stop, min_stop)
            stop_price = entry_price - stop_distance
            atr_pct = (atr / current_price) * 100
        else:
            stop_price = flag_low - 0.02
    else:
        stop_price = flag_low - 0.02

    risk = entry_price - stop_price
    if risk <= 0 or risk > entry_price * 0.10:
        return None

    return TradeSetup(
        symbol=gainer.symbol, timestamp=ts, entry_price=entry_price,
        stop_price=stop_price, risk_per_share=risk, atr_pct=atr_pct
    )


class BacktestEngine:
    """Backtest engine with configurable approach."""

    def __init__(self, config: ApproachConfig):
        self.config = config
        self.capital = INITIAL_CAPITAL
        self.positions: Dict[str, Position] = {}
        self.trades: List[BacktestTrade] = []
        self.equity_curve: List[float] = [INITIAL_CAPITAL]

    def run(self, gainers_by_date: Dict[str, List[DailyGainer]]) -> BacktestResult:
        """Run backtest."""
        print(f"\n{'='*50}")
        print(f"APPROACH: {self.config.name}")
        print(f"  ATR: {self.config.atr_mult}x | Stop: {self.config.min_stop_pct}%")
        print(f"  TP1: {self.config.tp1_r}R | TP2: {self.config.tp2_r}R")
        if self.config.use_time_exit:
            print(f"  Time Exit: {self.config.time_exit_bars} bars")
        if self.config.use_vol_filter:
            print(f"  Vol Filter: ATR < {self.config.max_atr_pct}%")
        print(f"{'='*50}")

        for date_str in sorted(gainers_by_date.keys()):
            gainers = gainers_by_date[date_str]
            if gainers:
                self._process_day(date_str, gainers)

        return self._calc_results()

    def _process_day(self, date_str: str, gainers: List[DailyGainer]):
        """Process one day."""
        self.positions = {}
        gainers = sorted(gainers, key=lambda x: x.gap_pct, reverse=True)[:5]

        minute_data = {}
        for g in gainers:
            df = fetch_minute_bars(g.symbol, g.date)
            if df is not None and len(df) > 30:
                df["atr"] = calculate_atr(df, ATR_PERIOD)
                minute_data[g.symbol] = df

        if not minute_data:
            return

        timestamps = sorted(set(ts for df in minute_data.values() for ts in df["timestamp"]))

        for ts in timestamps:
            hour, minute = ts.hour, ts.minute
            if hour == 9 and minute < 30 + TRADING_START_MINUTES:
                continue

            self._manage_positions(ts, minute_data)

            if hour >= EOD_CLOSE_HOUR and minute >= EOD_CLOSE_MINUTE:
                self._close_all(ts, minute_data, ExitReason.EOD_CLOSE)
                continue

            if hour >= TRADING_END_HOUR:
                continue

            if len(self.positions) < MAX_POSITIONS:
                self._scan_entries(ts, minute_data, gainers)

    def _manage_positions(self, ts: datetime, data: Dict[str, pd.DataFrame]):
        """Manage open positions."""
        to_remove = []

        for sym, pos in self.positions.items():
            df = data.get(sym)
            if df is None:
                continue

            bars = df[df["timestamp"] == ts]
            if bars.empty:
                continue

            row = bars.iloc[0]
            high, low, close = row["high"], row["low"], row["close"]

            pos.bars_since_entry += 1
            if high > pos.highest_price:
                pos.highest_price = high

            # Time exit check
            if self.config.use_time_exit and not pos.tp1_taken:
                if pos.bars_since_entry >= self.config.time_exit_bars:
                    if close < pos.entry_price:
                        self._exit(pos, ts, close, ExitReason.TIME_EXIT)
                        to_remove.append(sym)
                        continue

            # Stop check
            stop = pos.trail_stop if pos.trail_active else pos.stop_price
            if low <= stop:
                reason = ExitReason.TRAILING_STOP if pos.trail_active else (
                    ExitReason.BREAKEVEN if pos.be_active else ExitReason.STOP_LOSS)
                self._exit(pos, ts, stop, reason)
                to_remove.append(sym)
                continue

            # TP1
            if not pos.tp1_taken:
                tp1 = pos.entry_price + pos.risk_per_share * self.config.tp1_r
                if high >= tp1:
                    self._partial_exit(pos, ts, tp1, ExitReason.TP1)
                    pos.tp1_taken = True
                    pos.be_active = True
                    pos.stop_price = pos.entry_price + 0.01
                    continue

            # TP2
            if pos.tp1_taken and not pos.tp2_taken:
                tp2 = pos.entry_price + pos.risk_per_share * self.config.tp2_r
                if high >= tp2:
                    self._partial_exit(pos, ts, tp2, ExitReason.TP2)
                    pos.tp2_taken = True
                    pos.trail_active = True
                    pos.trail_stop = high * (1 - self.config.trail_pct / 100)
                    continue

            # Update trail
            if pos.trail_active:
                new_trail = pos.highest_price * (1 - self.config.trail_pct / 100)
                if new_trail > pos.trail_stop:
                    pos.trail_stop = new_trail

        for sym in to_remove:
            del self.positions[sym]

    def _scan_entries(self, ts: datetime, data: Dict[str, pd.DataFrame],
                      gainers: List[DailyGainer]):
        """Scan for entry."""
        for g in gainers:
            if g.symbol in self.positions:
                continue

            df = data.get(g.symbol)
            if df is None:
                continue

            bars = df[df["timestamp"] == ts]
            if bars.empty:
                continue

            idx = bars.index[0]
            setup = detect_bull_flag(df, idx, g, self.config)

            if setup:
                # Vol filter
                if self.config.use_vol_filter and setup.atr_pct > self.config.max_atr_pct:
                    continue

                qty = int(MAX_RISK_PER_TRADE / setup.risk_per_share)
                if qty <= 0:
                    continue

                pos_value = qty * setup.entry_price
                if pos_value > self.capital * 0.5:
                    qty = int(self.capital * 0.5 / setup.entry_price)
                if qty <= 0:
                    continue

                tp1_qty = int(qty * 0.33)
                tp2_qty = int(qty * 0.33)

                self.positions[g.symbol] = Position(
                    symbol=g.symbol, entry_time=ts, entry_price=setup.entry_price,
                    stop_price=setup.stop_price, risk_per_share=setup.risk_per_share,
                    total_qty=qty, remaining_qty=qty, highest_price=setup.entry_price,
                    tp1_qty=tp1_qty, tp2_qty=tp2_qty
                )
                break

    def _partial_exit(self, pos: Position, ts: datetime, price: float, reason: ExitReason):
        """Take partial exit."""
        qty = pos.tp1_qty if reason == ExitReason.TP1 else pos.tp2_qty
        if qty <= 0:
            return

        pnl = (price - pos.entry_price) * qty
        r = (price - pos.entry_price) / pos.risk_per_share
        pos.remaining_qty -= qty
        self.capital += pnl
        self.equity_curve.append(self.capital)

        self.trades.append(BacktestTrade(
            symbol=pos.symbol, date=ts.strftime("%Y-%m-%d"),
            entry_time=pos.entry_time, exit_time=ts,
            entry_price=pos.entry_price, exit_price=price, qty=qty,
            exit_reason=reason.value, pnl=pnl, r_multiple=r,
            hold_minutes=int((ts - pos.entry_time).total_seconds() / 60)
        ))

    def _exit(self, pos: Position, ts: datetime, price: float, reason: ExitReason):
        """Full exit."""
        pnl = (price - pos.entry_price) * pos.remaining_qty
        r = (price - pos.entry_price) / pos.risk_per_share
        self.capital += pnl
        self.equity_curve.append(self.capital)

        self.trades.append(BacktestTrade(
            symbol=pos.symbol, date=ts.strftime("%Y-%m-%d"),
            entry_time=pos.entry_time, exit_time=ts,
            entry_price=pos.entry_price, exit_price=price, qty=pos.remaining_qty,
            exit_reason=reason.value, pnl=pnl, r_multiple=r,
            hold_minutes=int((ts - pos.entry_time).total_seconds() / 60)
        ))

    def _close_all(self, ts: datetime, data: Dict[str, pd.DataFrame], reason: ExitReason):
        """Close all positions."""
        for sym, pos in list(self.positions.items()):
            df = data.get(sym)
            if df is None:
                continue
            bars = df[df["timestamp"] <= ts].tail(1)
            if bars.empty:
                continue
            self._exit(pos, ts, bars.iloc[0]["close"], reason)
        self.positions = {}

    def _calc_results(self) -> BacktestResult:
        """Calculate results."""
        r = BacktestResult(approach_name=self.config.name)
        r.trades = self.trades
        r.total_trades = len(self.trades)
        r.final_capital = self.capital

        if r.total_trades == 0:
            return r

        pnls = [t.pnl for t in self.trades]
        winners = [p for p in pnls if p > 0]
        losers = [p for p in pnls if p <= 0]

        r.winners = len(winners)
        r.losers = len(losers)
        r.win_rate = r.winners / r.total_trades * 100
        r.gross_profit = sum(winners)
        r.gross_loss = abs(sum(losers))
        r.net_pnl = sum(pnls)
        r.profit_factor = r.gross_profit / r.gross_loss if r.gross_loss > 0 else 999
        r.avg_winner = sum(winners) / len(winners) if winners else 0
        r.avg_loser = abs(sum(losers)) / len(losers) if losers else 0
        r.avg_r = sum(t.r_multiple for t in self.trades) / r.total_trades

        peak = INITIAL_CAPITAL
        max_dd = 0
        for eq in self.equity_curve:
            if eq > peak:
                peak = eq
            dd = (peak - eq) / peak
            if dd > max_dd:
                max_dd = dd
        r.max_drawdown = max_dd * 100

        for reason in ExitReason:
            trades = [t for t in self.trades if t.exit_reason == reason.value]
            if trades:
                w = sum(1 for t in trades if t.pnl > 0)
                r.exit_stats[reason.value] = {
                    "trades": len(trades),
                    "wins": w,
                    "win_rate": w / len(trades) * 100,
                    "pnl": sum(t.pnl for t in trades)
                }

        return r


def main():
    """Run all approaches and compare."""
    gainers = fetch_daily_gainers_historical(BACKTEST_DAYS)
    if not gainers:
        print("No data")
        return

    results = []
    for name, config in APPROACHES.items():
        engine = BacktestEngine(config)
        result = engine.run(gainers)
        results.append(result)

        # Print quick summary
        lw = result.avg_loser / result.avg_winner if result.avg_winner > 0 else 0
        print(f"  -> {result.total_trades} trades | {result.win_rate:.1f}% WR | "
              f"PF: {result.profit_factor:.2f} | L/W: {lw:.1f}x | P&L: ${result.net_pnl:,.0f}")

    # Final comparison
    print("\n" + "="*90)
    print("COMPARISON SUMMARY")
    print("="*90)
    print(f"{'Approach':<12} {'Trades':>7} {'WR%':>6} {'PF':>6} {'Net P&L':>11} "
          f"{'Avg W':>9} {'Avg L':>9} {'L/W':>5} {'MaxDD':>7}")
    print("-"*90)

    for r in results:
        lw = r.avg_loser / r.avg_winner if r.avg_winner > 0 else 0
        print(f"{r.approach_name:<12} {r.total_trades:>7} {r.win_rate:>5.1f}% {r.profit_factor:>6.2f} "
              f"${r.net_pnl:>9,.0f} ${r.avg_winner:>8,.0f} ${r.avg_loser:>8,.0f} "
              f"{lw:>4.1f}x {r.max_drawdown:>6.1f}%")

    print("="*90)

    # Find best
    best = max(results, key=lambda x: x.profit_factor if x.profit_factor < 900 else 0)
    print(f"\nBEST: {best.approach_name} (PF: {best.profit_factor:.2f})")

    # Exit breakdown for best
    print(f"\n{best.approach_name} Exit Breakdown:")
    for reason, stats in sorted(best.exit_stats.items(), key=lambda x: -x[1]["trades"]):
        pct = stats["trades"] / best.total_trades * 100
        print(f"  {reason}: {stats['trades']} ({pct:.0f}%) | WR: {stats['win_rate']:.0f}% | P&L: ${stats['pnl']:,.0f}")


if __name__ == "__main__":
    main()
