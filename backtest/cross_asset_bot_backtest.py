"""
Cross-Asset Bot Backtest Framework
===================================

Backtests cross_asset_bot.py (dual-momentum diversified ETF basket with inverse-vol
risk parity sizing) on daily bars.

Reuses the production signal + sizing functions directly from strategies/cross_asset_bot.py:
- compute_signals:       12-1 mo absolute momentum + Donchian channel + 200d SMA
- compute_target_weights: inverse-vol (risk parity) with caps + MAX_GROSS_EXPOSURE

Simulates:
- Weekly Wednesday rebalance at daily close
- Fractional shares
- Position-level stop-loss (POSITION_STOP_LOSS_PCT from entry)
- Portfolio trailing stop (PORTFOLIO_TRAILING_STOP_PCT from equity peak => flatten all)

Outputs:
- Trades CSV
- Daily equity CSV (needed by engine_composite_report.py)

Polygon bars cached to parquet under %LOCALAPPDATA%/AlgoTrading/data/cache/ for reruns.
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd

# --- Polygon key fallback (matches simple_bot_backtest.py / directional_bot_backtest.py) ---
# cross_asset_bot reads POLYGON_API_KEY from env at call time, so set it before import.
if not os.getenv("POLYGON_API_KEY"):
    os.environ["POLYGON_API_KEY"] = "eskzZ5O4QQyYpS5TsA3pe5WMeepeaxmW"

# --- Make strategies/cross_asset_bot importable ---
_THIS_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _THIS_DIR.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

# Reuse production strategy logic (single source of truth)
from strategies.cross_asset_bot import (  # noqa: E402
    ALL_SYMBOLS,
    INVERSE_MAP,
    INVERSE_REVERSE_MAP,
    LONG_UNIVERSE,
    MAX_GROSS_EXPOSURE,
    MAX_WEIGHT_PER_ASSET,
    MOMENTUM_LOOKBACK_DAYS,
    MOMENTUM_SKIP_DAYS,
    DONCHIAN_CHANNEL_DAYS,
    TREND_SMA_DAYS,
    PORTFOLIO_TRAILING_STOP_PCT,
    POSITION_STOP_LOSS_PCT,
    REBALANCE_WEEKDAY,
    TARGET_VOL_PER_POSITION,
    VOL_LOOKBACK_DAYS,
    compute_signals,
    compute_target_weights,
    fetch_daily_bars_polygon,
)


# ============================================================
# CONFIG
# ============================================================

INITIAL_CAPITAL_DEFAULT = 100_000
# Calendar-day warmup before window start so momentum/SMA/vol indicators are primed.
# MOMENTUM_LOOKBACK_DAYS + MOMENTUM_SKIP_DAYS = 273 trading days ≈ 395 calendar days.
WARMUP_CALENDAR_DAYS = 420

# Cache location
_ALGO_OUT = os.getenv("ALGO_OUTPUT_DIR")
if _ALGO_OUT:
    CACHE_DIR = Path(_ALGO_OUT) / "data" / "cache" / "xasset_backtest"
else:
    CACHE_DIR = _REPO_ROOT / "backtest" / "_cache_xasset"
CACHE_DIR.mkdir(parents=True, exist_ok=True)


# ============================================================
# DATA LOADING (cached)
# ============================================================

def load_bars(
    tickers: List[str],
    fetch_start: date,
    fetch_end: date,
    use_cache: bool = True,
) -> pd.DataFrame:
    """Fetch daily OHLC bars for all tickers, with parquet cache."""
    cache_key = f"{fetch_start.isoformat()}_{fetch_end.isoformat()}.parquet"
    cache_path = CACHE_DIR / cache_key

    if use_cache and cache_path.exists():
        print(f"[CACHE] Loading {cache_path.name}")
        df = pd.read_parquet(cache_path)
        # Ensure timestamp is date
        if df["timestamp"].dtype != object:
            df["timestamp"] = pd.to_datetime(df["timestamp"]).dt.date
        missing = sorted(set(tickers) - set(df["symbol"].unique()))
        if missing:
            print(f"[CACHE] Missing tickers {missing}, re-fetching union")
            use_cache = False

    if not use_cache or not cache_path.exists():
        print(f"[FETCH] Polygon daily bars: {len(tickers)} tickers, {fetch_start} -> {fetch_end}")
        df = fetch_daily_bars_polygon(tickers, fetch_start, fetch_end)
        # Normalize timestamp to date for caching
        df = df.copy()
        df["timestamp"] = pd.to_datetime(df["timestamp"]).dt.date
        df.to_parquet(cache_path, index=False)
        print(f"[CACHE] Wrote {cache_path.name}")

    return df


# ============================================================
# BACKTEST ENGINE
# ============================================================

class XAssetBacktest:
    """
    Daily-bar backtest for cross_asset_bot.

    - Iterates over trading days inside the requested window.
    - On each Wednesday, recompute signals + target weights from bars up to T,
      then rebalance at the day's close price.
    - Between rebalances, check position stop-loss and portfolio trailing stop daily at close.
    """

    def __init__(
        self,
        bars: pd.DataFrame,
        window_start: date,
        window_end: date,
        initial_capital: float = INITIAL_CAPITAL_DEFAULT,
        verbose: bool = True,
    ):
        self.bars = bars
        self.window_start = window_start
        self.window_end = window_end
        self.cash = initial_capital
        self.initial_capital = initial_capital
        self.verbose = verbose

        # Portfolio state
        self.positions: Dict[str, Dict] = {}  # symbol -> {qty, entry_price, entry_date}
        self.equity_peak = initial_capital
        self.portfolio_halted = False

        # Output records
        self.trades: List[Dict] = []
        self.daily_equity: List[Dict] = []

    # ------------------------------------------------------------
    # Price utilities
    # ------------------------------------------------------------

    def _close_on(self, symbol: str, d: date) -> Optional[float]:
        sub = self.bars[(self.bars["symbol"] == symbol) & (self.bars["timestamp"] == d)]
        if sub.empty:
            return None
        return float(sub["close"].iloc[0])

    def _trading_days_in_window(self) -> List[date]:
        """All calendar days for which *any* symbol has a bar inside [window_start, window_end]."""
        in_window = (self.bars["timestamp"] >= self.window_start) & (self.bars["timestamp"] <= self.window_end)
        dates = sorted(set(self.bars.loc[in_window, "timestamp"].tolist()))
        return dates

    def _mark_to_market(self, d: date) -> float:
        equity = self.cash
        for sym, pos in self.positions.items():
            px = self._close_on(sym, d)
            if px is not None:
                equity += pos["qty"] * px
            else:
                # Missing bar — carry previous mark
                equity += pos["qty"] * pos["last_mark"]
        return equity

    # ------------------------------------------------------------
    # Trade execution
    # ------------------------------------------------------------

    def _exit_position(self, symbol: str, exit_price: float, exit_date: date, reason: str):
        pos = self.positions.pop(symbol, None)
        if pos is None:
            return
        qty = pos["qty"]
        pnl = (exit_price - pos["entry_price"]) * qty
        self.cash += qty * exit_price
        self.trades.append({
            "symbol": symbol,
            "side": "LONG",                 # (inverse ETFs are still held LONG on the book)
            "underlying_signal": pos.get("underlying_signal", ""),
            "entry_date": pos["entry_date"].isoformat(),
            "exit_date": exit_date.isoformat(),
            "entry_price": round(pos["entry_price"], 4),
            "exit_price": round(exit_price, 4),
            "qty": round(qty, 4),
            "pnl": round(pnl, 2),
            "return_pct": round((exit_price / pos["entry_price"] - 1.0) * 100, 3),
            "exit_reason": reason,
            "hold_days": (exit_date - pos["entry_date"]).days,
        })

    def _enter_position(self, symbol: str, target_notional: float, price: float, entry_date: date,
                        underlying_signal: str):
        if price <= 0 or target_notional <= 0:
            return
        qty = target_notional / price
        cost = qty * price
        if cost > self.cash + 1e-6:
            qty = max(0.0, self.cash / price)
            cost = qty * price
        if qty <= 0:
            return
        self.cash -= cost
        self.positions[symbol] = {
            "qty": qty,
            "entry_price": price,
            "entry_date": entry_date,
            "last_mark": price,
            "underlying_signal": underlying_signal,
        }

    def _resize_position(self, symbol: str, target_qty: float, price: float, trade_date: date,
                        underlying_signal: str):
        """Adjust existing position to target qty. Records a trade for any exit portion."""
        existing = self.positions.get(symbol)
        if existing is None:
            self._enter_position(symbol, target_qty * price, price, trade_date, underlying_signal)
            return
        delta = target_qty - existing["qty"]
        if abs(delta) < 1e-6:
            return
        if delta > 0:
            # Buy more — use weighted avg cost
            cost = delta * price
            if cost > self.cash + 1e-6:
                cost = max(0.0, self.cash)
                delta = cost / price
            self.cash -= delta * price
            new_qty = existing["qty"] + delta
            new_avg = ((existing["qty"] * existing["entry_price"]) + (delta * price)) / new_qty if new_qty > 0 else price
            existing["qty"] = new_qty
            existing["entry_price"] = new_avg
            existing["last_mark"] = price
        else:
            # Sell part — record as a separate trade slice
            sell_qty = -delta
            pnl = (price - existing["entry_price"]) * sell_qty
            self.cash += sell_qty * price
            self.trades.append({
                "symbol": symbol,
                "side": "LONG",
                "underlying_signal": existing.get("underlying_signal", ""),
                "entry_date": existing["entry_date"].isoformat(),
                "exit_date": trade_date.isoformat(),
                "entry_price": round(existing["entry_price"], 4),
                "exit_price": round(price, 4),
                "qty": round(sell_qty, 4),
                "pnl": round(pnl, 2),
                "return_pct": round((price / existing["entry_price"] - 1.0) * 100, 3),
                "exit_reason": "REBALANCE_TRIM",
                "hold_days": (trade_date - existing["entry_date"]).days,
            })
            existing["qty"] = existing["qty"] - sell_qty
            existing["last_mark"] = price
            if existing["qty"] < 1e-6:
                self.positions.pop(symbol, None)

    # ------------------------------------------------------------
    # Risk checks (daily at close)
    # ------------------------------------------------------------

    def _check_daily_risk(self, d: date):
        """Apply position stop-loss and portfolio trailing stop at close of d."""
        # Mark to market
        equity = self._mark_to_market(d)
        self.equity_peak = max(self.equity_peak, equity)

        # Portfolio trailing stop
        drawdown = (self.equity_peak - equity) / self.equity_peak if self.equity_peak > 0 else 0.0
        if drawdown >= PORTFOLIO_TRAILING_STOP_PCT and self.positions:
            if self.verbose:
                print(f"[{d}] PORTFOLIO TRAILING STOP HIT: dd={drawdown:.1%}, flattening all")
            for sym in list(self.positions.keys()):
                px = self._close_on(sym, d) or self.positions[sym]["last_mark"]
                self._exit_position(sym, px, d, "PORTFOLIO_TRAILING_STOP")
            self.portfolio_halted = True
            return

        # Per-position stop-loss
        for sym in list(self.positions.keys()):
            pos = self.positions[sym]
            px = self._close_on(sym, d)
            if px is None:
                continue
            pos["last_mark"] = px
            loss_pct = (pos["entry_price"] - px) / pos["entry_price"] if pos["entry_price"] > 0 else 0.0
            if loss_pct >= POSITION_STOP_LOSS_PCT:
                if self.verbose:
                    print(f"[{d}] STOP-LOSS {sym}: -{loss_pct:.1%}")
                self._exit_position(sym, px, d, "POSITION_STOP_LOSS")

    # ------------------------------------------------------------
    # Rebalance (Wednesday close)
    # ------------------------------------------------------------

    def _rebalance(self, d: date):
        if self.portfolio_halted:
            return

        # Slice bars up to and including d (signals must not peek ahead)
        bars_up_to_d = self.bars[self.bars["timestamp"] <= d]

        # compute_signals + compute_target_weights expect MultiIndex or long-form with 'timestamp','symbol','close','high','low'
        signals = compute_signals(bars_up_to_d)
        target_weights, _diag = compute_target_weights(bars_up_to_d, signals)

        equity = self._mark_to_market(d)
        if equity <= 0:
            return

        # Build trade-symbol -> (target_notional, underlying_signal)
        targets: Dict[str, Tuple[float, str]] = {}
        for trade_sym, w in target_weights.items():
            underlying = INVERSE_REVERSE_MAP.get(trade_sym, trade_sym)
            sig = signals.get(underlying, "LONG")
            targets[trade_sym] = (equity * w, sig)

        # Close positions not in targets
        for sym in list(self.positions.keys()):
            if sym not in targets:
                px = self._close_on(sym, d) or self.positions[sym]["last_mark"]
                self._exit_position(sym, px, d, "REBALANCE_EXIT")

        # Resize / enter targets
        for sym, (target_notional, underlying_signal) in targets.items():
            px = self._close_on(sym, d)
            if px is None or px <= 0:
                continue
            target_qty = target_notional / px
            self._resize_position(sym, target_qty, px, d, underlying_signal)

        if self.verbose:
            print(f"[{d}] REBALANCE: {len(self.positions)} positions, equity=${equity:,.0f}, cash=${self.cash:,.0f}")

    # ------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------

    def run(self):
        days = self._trading_days_in_window()
        if not days:
            raise RuntimeError("No trading days in window — check data coverage")

        print(f"[XASSET] Running backtest over {len(days)} trading days ({days[0]} -> {days[-1]})")
        print(f"[XASSET] Initial capital: ${self.initial_capital:,.0f}")
        print(f"[XASSET] Universe: {len(LONG_UNIVERSE)} long + {len(INVERSE_MAP)} inverse pairs")

        for d in days:
            # Daily risk checks FIRST (applies even on rebalance days: indicators use close)
            self._check_daily_risk(d)

            # Rebalance on Wednesdays (REBALANCE_WEEKDAY = 2)
            # datetime.weekday(): Mon=0, Tue=1, Wed=2, ...
            if d.weekday() == REBALANCE_WEEKDAY:
                self._rebalance(d)

            # Record equity after any trades today
            equity = self._mark_to_market(d)
            self.daily_equity.append({
                "date": d.isoformat(),
                "equity": round(equity, 2),
                "cash": round(self.cash, 2),
                "positions": len(self.positions),
            })

        # Print summary
        final_equity = self._mark_to_market(days[-1])
        total_return = (final_equity / self.initial_capital - 1.0) * 100
        n_trades = len(self.trades)
        print("\n" + "=" * 60)
        print("XASSET BACKTEST RESULTS")
        print("=" * 60)
        print(f"Period:       {days[0]} -> {days[-1]} ({len(days)} trading days)")
        print(f"Final equity: ${final_equity:,.2f}")
        print(f"Total return: {total_return:+.2f}%")
        print(f"Trades:       {n_trades}")
        print(f"Portfolio halted: {self.portfolio_halted}")
        print("=" * 60)


# ============================================================
# CLI
# ============================================================

def main():
    parser = argparse.ArgumentParser(
        description="Backtest cross_asset_bot.py on daily bars",
        epilog=(
            "Examples:\n"
            "  python cross_asset_bot_backtest.py --start 2023-06-01 --end 2023-12-29\n"
            "  python cross_asset_bot_backtest.py --start 2025-10-01 --end 2026-04-01 \\\n"
            "      --output-trades trades_P6.csv --output-equity equity_P6.csv\n"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--start", type=str, required=True, help="Backtest window start (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, required=True, help="Backtest window end (YYYY-MM-DD)")
    parser.add_argument("--capital", type=float, default=INITIAL_CAPITAL_DEFAULT,
                        help=f"Initial capital (default: {INITIAL_CAPITAL_DEFAULT:,.0f})")
    parser.add_argument("--output-trades", type=str, default="cross_asset_backtest_trades.csv",
                        help="Trades CSV output path")
    parser.add_argument("--output-equity", type=str, default="cross_asset_backtest_equity.csv",
                        help="Daily equity CSV output path")
    parser.add_argument("--no-cache", action="store_true", help="Bypass parquet cache, force Polygon fetch")

    args = parser.parse_args()

    window_start = date.fromisoformat(args.start)
    window_end = date.fromisoformat(args.end)
    fetch_start = window_start - timedelta(days=WARMUP_CALENDAR_DAYS)
    fetch_end = window_end

    if not os.getenv("POLYGON_API_KEY"):
        print("[ERROR] POLYGON_API_KEY not set")
        sys.exit(1)

    print(f"[XASSET] Window: {window_start} -> {window_end}")
    print(f"[XASSET] Data fetch (with warmup): {fetch_start} -> {fetch_end}")

    bars = load_bars(ALL_SYMBOLS, fetch_start, fetch_end, use_cache=not args.no_cache)

    bt = XAssetBacktest(
        bars=bars,
        window_start=window_start,
        window_end=window_end,
        initial_capital=args.capital,
        verbose=True,
    )
    bt.run()

    # Persist outputs
    if bt.trades:
        pd.DataFrame(bt.trades).to_csv(args.output_trades, index=False)
        print(f"Trades saved to {args.output_trades} ({len(bt.trades)} rows)")
    else:
        # Still write an empty CSV with headers so downstream readers don't choke
        pd.DataFrame(columns=[
            "symbol", "side", "underlying_signal", "entry_date", "exit_date",
            "entry_price", "exit_price", "qty", "pnl", "return_pct",
            "exit_reason", "hold_days",
        ]).to_csv(args.output_trades, index=False)
        print(f"No trades — wrote empty {args.output_trades}")

    pd.DataFrame(bt.daily_equity).to_csv(args.output_equity, index=False)
    print(f"Daily equity saved to {args.output_equity} ({len(bt.daily_equity)} rows)")


if __name__ == "__main__":
    main()
