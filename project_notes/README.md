# Project Notes

This directory contains session notes and change logs for each trading script/bot.

---

## Directory Consolidation (2026-01-24)

All trading scripts were consolidated from multiple locations into a single `Algo_Trading` directory:

**Previous locations:**
- `Documents/Alpaca Scripts/Stocks/bots/` - Trading strategies and scanners
- `Documents/Alpaca Scripts/hysa_phase1/` - Trend bot and swing alerts
- `Desktop/Trading Bots/` - Launcher batch files

**New structure:** `Documents/Algo_Trading/`
```
Algo_Trading/
├── strategies/      # Trading bots (simple_bot, vwap_bot, momentum_bot, swing_trader)
├── scanners/        # Scanners and executors (smallcap_scanner, smallcap_executor)
├── alerts/          # Alert scripts (buy_alerts, sell_alerts, swing_newsletter)
├── utilities/       # Shared modules (alpaca_helpers, indicators)
├── ai_manager/      # AI Investment Manager
├── backtest/        # Backtesting framework
├── config/          # All configuration files
├── data/            # State files, cache, inputs
├── logs/            # Log files
├── launchers/       # Batch files to run scripts
└── project_notes/   # This directory
```

See the main [README.md](../README.md) for full documentation.

---

## How to Use

1. **Starting a session**: Tell Claude Code to read the relevant notes file first
   - Example: "Read project_notes/ai_investment_manager.md and continue where we left off"

2. **During work**: Ask Claude to update the notes as work progresses

3. **Ending a session**: Ask Claude to summarize what was accomplished

## Files

| File | Script Location | Description |
|------|-----------------|-------------|
| [ai_investment_manager.md](ai_investment_manager.md) | `ai_manager/` | Portfolio monitoring & email alerts |
| [simple_bot.md](simple_bot.md) | `strategies/simple_bot.py` | Momentum trading bot with bracket orders |
| [smallcap_executor.md](smallcap_executor.md) | `scanners/smallcap_executor.py` | Small cap trade executor with tiered exits |
| [smallcap_scanner.md](smallcap_scanner.md) | `scanners/smallcap_scanner.py` | Small cap flag/pennant scanner |
| [trend_bot.md](trend_bot.md) | `strategies/trend_bot.py` | ETF trend-following bot |
| [buy_alerts.md](buy_alerts.md) | `alerts/buy_alerts.py` | Buy signal alerts |
| [sell_alerts.md](sell_alerts.md) | `alerts/sell_alerts.py` | Sell signal alerts |

## Adding New Notes

When working on a new script, create a notes file with this template:

```markdown
# [Script Name] - Project Notes

> **Purpose**: Track progress and changes for [script].
> **Location**: `[path to script]`

---

## Overview

[Brief description of what the script does]

---

## Session Log

### Session: [DATE]

**What was worked on:**
- [Item 1]

**Decisions made:**
- [Decision 1]

**Issues encountered:**
- [Issue 1]

---

## Known Issues / TODOs

- [ ] [Item 1]

---

## Key Configuration

- [Config item 1]
```
