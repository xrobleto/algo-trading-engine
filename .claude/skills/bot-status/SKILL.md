---
name: bot-status
description: Check the status of trading bots, review state files, logs, and recent trade activity. Use when the user asks about bot health, positions, or recent trades.
---

# Bot Status — Algo Trading

Check the health and activity of running trading bots.

## State files (`data/state/`)

Each bot persists state as JSON. Check these for:
- Current positions and entry prices
- Open orders and pending fills
- Last scan/rebalance timestamp
- Daily P&L tracking
- Circuit breaker status

## Log files (`logs/`)

Bot logs are written here. Check for:
- Recent errors or warnings
- Connection issues (WebSocket disconnects, API rate limits)
- Trade execution confirmations
- Missed signals or skipped trades

## Trade journals (`data/state/*_trades.jsonl`)

JSONL files with historical trade records. Analyze for:
- Recent trade count and frequency
- Win/loss ratio over last N trades
- Average hold time
- Largest winner/loser

## Performance tracking

- `project_notes/trend_bot_rebalances.csv` — Trend bot rebalance history

## Steps

1. Determine which bot(s) to check from `$ARGUMENTS` (or check all if not specified)
2. Read the relevant state file(s) in `data/state/`
3. Read recent log entries in `logs/`
4. Summarize for the user:
   - Is the bot running? (check last log timestamp)
   - Current positions and unrealized P&L
   - Recent trades (last 5-10)
   - Any errors or warnings
   - Circuit breaker / drawdown status
5. If the bot appears stalled or unhealthy, suggest troubleshooting:
   - Check if the launcher is running
   - Verify API keys in `config/` are valid
   - Check for stale state files that may need resetting

## Available bots

| Bot | State file pattern | Launcher |
|-----|-------------------|----------|
| Trend Bot | `trend_bot_state.json` | `Start Trend Bot.bat` |
| Simple Bot | `simple_bot_state.json` | `Start Simple Bot.bat` |
| Directional Bot | `directional_bot_state.json` | `Start Directional Bot.bat` |
| Swing Trader | `swing_trader_state.json` | `Start Swing Trader.bat` |
| Momentum Bot | `momentum_bot_state.json` | `Start Momentum Bot.bat` |
| VWAP Bot | `vwap_bot_state.json` | `Start VWAP Bot.bat` |
| Smallcap Momentum | `smallcap_momentum_state.json` | `Start Smallcap Bot.bat` |
| Master Bot | `master_bot_state.json` | `Start Master Bot.bat` |
| Market Scanner | `scanner_state.json` | `Start Scanner + Monitor.bat` |
