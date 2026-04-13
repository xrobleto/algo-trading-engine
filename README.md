# Algo Trading System

A consolidated algorithmic trading system for stocks and ETFs using Alpaca and Polygon APIs.

## Directory Structure

```
Algo_Trading/
├── strategies/       # Trading bots (automated trading)
├── scanners/         # Market scanners and trade executors
├── alerts/           # Email alert systems
├── utilities/        # Shared helper modules
├── ai_manager/       # AI-powered investment analysis
├── backtest/         # Backtesting scripts
├── config/           # Configuration files (.yaml, .env)
├── data/             # State, inputs, and cache
│   ├── state/        # JSON state files
│   ├── inputs/       # Watchlists and activity files
│   └── cache/        # API response caches
├── logs/             # Log files
├── project_notes/    # Documentation and session logs
└── launchers/        # Windows batch files to run scripts
```

## Quick Start

All scripts can be run via batch files in the `launchers/` folder:

| Launcher | Description |
|----------|-------------|
| `Start Trend Bot.bat` | Weekly ETF rebalancer (runs continuously) |
| `Start Simple Bot.bat` | Intraday momentum bot (runs continuously) |
| `Start Smallcap Scanner.bat` | Scans for small cap setups |
| `Execute Trade.bat` | Execute trades from scanner signals |
| `Run Buy Alerts.bat` | One-time buy alert scan |
| `Run Sell Alerts.bat` | One-time sell alert scan |
| `Run All Alerts.bat` | Run buy, sell, and newsletter together |

## Components

### Strategies (Trading Bots)

| Bot | Description | Schedule |
|-----|-------------|----------|
| `trend_bot.py` | Volatility-targeted ETF rotation using SMA200 trend filter | Weekly (Fridays) |
| `simple_bot.py` | Intraday momentum with VWAP/EMA confluence | Market hours |
| `swing_trader.py` | Multi-day swing trades with bracket orders | Daily |
| `vwap_bot.py` | VWAP reversion strategy | Market hours |
| `momentum_bot.py` | Momentum factor trading | Market hours |

### Scanners

| Scanner | Description |
|---------|-------------|
| `smallcap_scanner.py` | Finds small cap gap-up setups with flag patterns |
| `smallcap_executor.py` | Executes trades with automated exit management |
| `interactive_trade_analyzer.py` | Analyzes trade history |

### Alerts

| Alert | Description |
|-------|-------------|
| `buy_alerts.py` | Technical buy signals (RSI oversold, support tests) |
| `sell_alerts.py` | Sell signals for portfolio management |
| `swing_newsletter.py` | Daily market summary email |

### Utilities

| Utility | Description |
|---------|-------------|
| `polymarket_client.py` | Prediction market sentiment |
| `polymarket_monitor.py` | Continuous Polymarket monitoring |
| `reddit_sentiment.py` | Reddit stock sentiment analysis |
| `enhanced_news_filter.py` | News sentiment scoring |
| `apply_risk_level.py` | Adjust config risk levels (1-3) |

## Configuration

### Environment Variables

Create `.env` files in `config/` or set environment variables:

```
ALPACA_API_KEY=your_key
ALPACA_SECRET_KEY=your_secret
POLYGON_API_KEY=your_polygon_key
```

### Config Files

- `alerts_config.yaml` - Buy/sell alert settings, risk levels, watchlist
- `trend_bot.env` - Trend bot specific settings
- `smallcap_scanner.env` - Scanner settings

### Risk Levels (alerts_config.yaml)

```yaml
risk_level: 2  # 1=Conservative, 2=Moderate, 3=Aggressive
```

| Level | Alert Frequency | Quality Threshold |
|-------|-----------------|-------------------|
| 1 | ~1-3/week | WOW-only, highest quality |
| 2 | ~3-8/week | Balanced (recommended) |
| 3 | ~8-15/week | More frequent, lower bar |

## Data Files

### Inputs (`data/inputs/`)

- `buy_universe.txt` - Watchlist tickers (one per line)
- `robinhood_activity.csv` - Portfolio activity export

### State (`data/state/`)

State files persist bot state across restarts. Do not edit manually.

## Project Notes

Each bot has a corresponding markdown file in `project_notes/` documenting:
- Session logs with changes made
- Known issues and TODOs
- Configuration reference

## Performance Tracking

- `project_notes/trend_bot_rebalances.csv` - Trend bot rebalance history
- `data/state/*_trades.jsonl` - Trade journals (JSONL format)

## Scheduled Tasks (Windows Task Scheduler)

| Task | Schedule | Launcher |
|------|----------|----------|
| Swing Newsletter | 8:30 AM ET weekdays | `Scheduled Swing Newsletter.bat` |
| AI Investment Manager | Fridays 6:00 AM CT | `Scheduled AI Investment Manager.bat` |

## Troubleshooting

1. **API errors**: Check `.env` files in `config/` have valid keys
2. **State issues**: Delete state file in `data/state/` to reset
3. **Path errors**: Ensure running from `launchers/` folder

## Requirements

- Python 3.10+
- Packages: alpaca-py, pandas, numpy, requests, python-dotenv, pytz
- For AI manager: separate venv in `ai_manager/`
