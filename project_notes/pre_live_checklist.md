# Pre-Live Checklist

> **Purpose**: Final validation checklist before transitioning simple_bot and trend_bot from paper to live trading.
> **Target Go-Live Date**: Week of 2026-02-03

---

## Week of 2026-01-27: Final Testing Week

### Daily Testing Tasks

| Day | Focus Area | Simple Bot | Trend Bot |
|-----|------------|------------|-----------|
| Mon | Normal operation | [ ] | [ ] |
| Tue | Stress test (volatility) | [ ] | [ ] |
| Wed | Restart recovery test | [ ] | [ ] |
| Thu | EOD reduction test | [ ] | [ ] |
| Fri | Full day monitoring | [ ] | [ ] |

---

## Configuration Review

### Simple Bot (`strategies/simple_bot.py`)

- [ ] **API Mode**: Verify `paper: true` in config (will change to `false` for live)
- [ ] **Position Sizing**:
  - [ ] `MAX_POSITION_RISK_PCT` is appropriate for live account
  - [ ] `MAX_CONCURRENT_POSITIONS` limit is set
  - [ ] Buying power check is working
- [ ] **Circuit Breakers**:
  - [ ] `MAX_DAILY_LOSS_PCT` (currently 5%) - acceptable?
  - [ ] `DRAWDOWN_PAUSE_THRESHOLD` (currently 8%) - acceptable?
  - [ ] `MAX_CONSECUTIVE_LOSSES` (currently 5) - acceptable?
- [ ] **EOD Handling**:
  - [ ] `GRADUAL_EOD_REDUCTION = True` (new feature)
  - [ ] `EOD_CLOSE_TIME_ET = (15, 55)` - positions flat by 3:55 PM
- [ ] **Kill Switch**:
  - [ ] Know location of `HALT_TRADING` file
  - [ ] Test that creating file stops bot

### Trend Bot (`strategies/trend_bot.py`)

- [ ] **API Mode**: Verify paper mode in config
- [ ] **Position Sizing**:
  - [ ] Target volatility settings appropriate
  - [ ] Max position per ETF is reasonable
- [ ] **Rebalance Settings**:
  - [ ] Rebalance frequency is set correctly
  - [ ] Drift threshold is appropriate
- [ ] **Regime Detection**:
  - [ ] SPY regime logic working correctly
  - [ ] Risk-on/risk-off transitions smooth

---

## Alerting & Monitoring

- [ ] **Slack Alerts**: Test CRITICAL alert delivery
- [ ] **Email Alerts**: Test email delivery (if enabled)
- [ ] **Log Files**:
  - [ ] Rotation working (not growing unbounded)
  - [ ] Location: `logs/` directory
- [ ] **Trade Journal**:
  - [ ] `momentum_bot_trades.jsonl` capturing all trades
  - [ ] Fields are complete and accurate

---

## Stress Testing

### Restart Recovery Test
1. [ ] Start bot with positions open
2. [ ] Kill bot process (Ctrl+C or Task Manager)
3. [ ] Restart bot
4. [ ] Verify:
   - [ ] Positions are detected and recovered
   - [ ] Stop/TP orders are preserved or recreated
   - [ ] No duplicate orders placed
   - [ ] Alert sent about recovered positions

### WebSocket Disconnect Test
1. [ ] Disconnect network briefly (5-10 seconds)
2. [ ] Reconnect network
3. [ ] Verify:
   - [ ] Bot reconnects automatically
   - [ ] Exponential backoff works
   - [ ] No missed fills during disconnect

### EOD Reduction Test (Simple Bot)
1. [ ] Have position open at 3:20 PM
2. [ ] Watch 3:25 PM - first reduction should occur
3. [ ] Verify:
   - [ ] 10% reduction every 5 minutes
   - [ ] Logs show reduction numbers (#1, #2, etc.)
   - [ ] Full flatten at 3:55 PM
   - [ ] Position is flat by market close

---

## Metrics to Collect This Week

Run `python utilities/daily_metrics.py` at end of each day.

| Metric | Mon | Tue | Wed | Thu | Fri | Target |
|--------|-----|-----|-----|-----|-----|--------|
| Trades (simple_bot) | | | | | | 3-8/day |
| Win Rate | | | | | | >50% |
| Avg R-Multiple | | | | | | >0.3R |
| Max Drawdown | | | | | | <5% |
| Errors/Alerts | | | | | | 0 |
| Trades (trend_bot) | | | | | | 0-2/day |
| Rebalances | | | | | | As needed |

---

## Go-Live Day Checklist (2026-02-03)

### Before Market Open (9:00 AM)

- [ ] Switch API keys from paper to live
- [ ] Set position size to **50%** of target (conservative start)
- [ ] Verify account has sufficient buying power
- [ ] Clear any stale state files if needed
- [ ] Start bots and verify they connect

### During First Hour (9:30-10:30 AM)

- [ ] Monitor actively - do not walk away
- [ ] Verify first trade executes correctly (if any)
- [ ] Check that stops/TPs are placed at broker
- [ ] Compare signals to paper instance (if running parallel)

### End of Day 1

- [ ] Review all trades in journal
- [ ] Check P&L matches broker statement
- [ ] Note any issues for adjustment
- [ ] Decide: continue at 50% or increase to 75%?

### First Week Live

- [ ] Day 1-2: 50% position size
- [ ] Day 3-4: 75% position size (if Day 1-2 clean)
- [ ] Day 5: 100% position size (if all clean)

---

## Emergency Procedures

### Kill Switch
```bash
# Create kill switch file to halt immediately
echo "HALT" > C:\Users\xrobl\Documents\Algo_Trading\data\state\HALT_TRADING
```

### Manual Flatten All
```python
# Via Alpaca dashboard or:
# 1. Go to https://app.alpaca.markets
# 2. Click "Close All Positions"
```

### Contact
- Alpaca Support: support@alpaca.markets
- Polygon Support: support@polygon.io

---

## Sign-Off

| Role | Name | Date | Signature |
|------|------|------|-----------|
| Developer | | | |
| Reviewer | | | |

---

## Notes

_Add any observations or concerns here during final testing week:_

-

