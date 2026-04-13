---
name: new-strategy
description: Scaffold a new trading bot or strategy following existing project patterns. Use when the user wants to create a new trading strategy, bot, or scanner.
---

# New Strategy — Algo Trading

Create a new trading bot following the established project architecture.

## Existing strategies for reference (in `strategies/`)

| Bot | Type | Key Patterns |
|-----|------|-------------|
| `trend_bot.py` | Weekly rebalancer | SMA200 filter, volatility targeting, Alpaca portfolio API |
| `simple_bot.py` | Intraday momentum | VWAP/EMA confluence, WebSocket streaming, state machine |
| `directional_bot.py` | Trend follower | Entry/exit mechanics, bracket orders |
| `swing_trader.py` | Multi-day swing | Bracket orders, daily analysis |
| `momentum_bot.py` | Intraday scalping | Momentum factors |
| `vwap_bot.py` | VWAP reversion | Mean reversion, tight stops |
| `smallcap_momentum_bot.py` | Gap-up trading | Premarket scanning, float rotation |
| `master_bot.py` | Orchestrator | Multi-strategy management |
| `market_scanner.py` | Scanner | Opportunity detection |

## Required components for a new strategy

1. **Strategy file** → `strategies/<name>_bot.py`
   - Import patterns: `alpaca-py` for trading, `requests` for Polygon API, `python-dotenv` for config
   - State management: JSON state files in `data/state/`
   - Logging: Python `logging` module, output to `logs/`
   - Error handling: graceful reconnection, circuit breakers

2. **Config file** → `config/<name>.env` or add section to `config/master_bot_config.yaml`
   - API keys loaded via `python-dotenv`
   - Strategy parameters (thresholds, intervals, position sizes)

3. **Launcher** → `launchers/Start <Name> Bot.bat`
   ```bat
   @echo off
   cd /d "%~dp0.."
   python strategies/<name>_bot.py
   pause
   ```

4. **Backtest** → `backtest/<name>_backtest.py`
   - Use Polygon historical data
   - Output CSV with trade log
   - Report: win rate, P&L, max drawdown, Sharpe

5. **Documentation** → `project_notes/<name>.md`
   - Strategy description and logic
   - Parameters and configuration
   - Session log for changes

## Architecture patterns to follow

- **State machine** for order lifecycle (SCANNING → ENTRY_PENDING → IN_POSITION → EXIT_PENDING)
- **Bracket orders** via Alpaca for automated stop-loss and take-profit
- **Multi-timeframe analysis** (1m/5m/15m confluences)
- **Risk controls**: per-trade max loss, daily max loss, drawdown circuit breaker
- **Market hours awareness**: use `pytz` with `America/New_York`, check RTH vs extended hours
- **Alpaca WebSocket** for real-time data where needed

## Steps

1. Ask what type of strategy from `$ARGUMENTS` (or clarify: momentum, mean reversion, trend following, etc.)
2. Identify the closest existing bot to use as a template
3. Create all 5 components listed above
4. Wire up the config with sensible defaults
5. Suggest initial backtest parameters
