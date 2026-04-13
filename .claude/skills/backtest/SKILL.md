---
name: backtest
description: Run, analyze, or create backtests for trading strategies. Use when the user wants to test a strategy, compare results, optimize parameters, or review backtest output.
---

# Backtest — Algo Trading

Run and analyze backtests for the algorithmic trading strategies.

## Backtest scripts (in `backtest/`)

| Script | Strategy |
|--------|----------|
| `simple_bot_backtest.py` | Intraday momentum (VWAP/EMA confluence) |
| `simple_bot_optimizer.py` | Parameter optimization for simple bot |
| `directional_bot_backtest.py` | Directional trend following |
| `trend_bot_backtest.py` | Weekly ETF rotation (SMA200 filter) |
| `momentum_backtest.py` | Momentum factor trading |
| `vwap_backtest.py` / `vwap_bot_backtest.py` | VWAP reversion |
| `smallcap_momentum_backtest.py` (v5-v7) | Small cap gap-up momentum |
| `master_bot_backtest.py` | Multi-strategy orchestrator |
| `signal_scorer_backtest.py` | Signal scoring validation |
| `param_sweep.py` | Parameter grid search optimization |

## Corresponding live strategies (in `strategies/`)

`trend_bot.py`, `simple_bot.py`, `directional_bot.py`, `swing_trader.py`, `momentum_bot.py`, `vwap_bot.py`, `smallcap_momentum_bot.py`, `master_bot.py`, `market_scanner.py`

## How to run

```bash
cd backtest
python <backtest_script>.py
```

Most backtests output a CSV of trades (e.g., `directional_bot_backtest_trades.csv`). Root-level CSV files are previous backtest results:
- `directional_bot_backtest_baseline.csv`
- `directional_bot_backtest_scanner_v2.csv`
- `simple_bot_backtest_trades.csv`

## When the user asks to backtest

1. Identify which strategy to backtest from `$ARGUMENTS` (or ask if unclear)
2. Read the corresponding backtest script to understand its parameters
3. Read the corresponding live strategy to understand the trading logic
4. Run the backtest and capture output
5. Analyze results — focus on:
   - Win rate and profit factor
   - Max drawdown
   - Sharpe ratio (if available)
   - Number of trades and average hold time
   - Compare to previous results if CSV baselines exist
6. Suggest parameter tweaks if performance is poor

## When creating a new backtest

Follow the patterns in existing backtest scripts:
- Use Polygon API for historical data (check `config/` for API keys)
- Output trades to CSV for analysis
- Include key metrics: win rate, P&L, max drawdown, Sharpe
- Match the entry/exit logic from the corresponding live strategy exactly
