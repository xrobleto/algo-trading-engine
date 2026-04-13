# Directional Bot - Project Notes

> **Purpose**: Track progress and changes for the directional_bot.py script.
> **Location**: `C:\Users\xrobl\Documents\GitHub\Algo_Trading\strategies\directional_bot.py`

---

## Overview

A SHORT-ONLY intraday momentum bot that sells short stocks showing downward momentum during favorable market regimes. Uses a regime-gated approach based on SPY SMA200 and VWAP to determine when shorting conditions are favorable.

Key Architecture:
- Short-only intraday strategy (no overnight exposure)
- Software-managed trailing stops (primary profit engine)
- Broker-native safety net stop orders (crash protection)
- 5-state regime detection (SPY SMA200 + VWAP proximity)
- Shared-account safety via `dir_` order prefix tagging
- Conservative production sizing (35% position vs 75% backtest)

---

## Strategy Summary

### Entry Signal (9 filters, all must pass)
1. Price >= 0.30% BELOW VWAP (confirms downward pressure)
2. EMA9 < EMA20 with >= 0.20% separation (short-term downtrend)
3. 3 of 5 bars must be lower closes (price action confirmation)
4. Momentum <= -0.15% over 5 minutes (negative momentum)
5. RVOL >= 1.5x (volume confirmation, stricter than simple_bot's 1.2x)
6. ADX 15-35 (trend strength, narrower range for cleaner signals)
7. 30-minute opening blackout (no trades before 10:00 AM ET)
8. Late cutoff 1:30 PM ET (no new entries after, earlier than simple_bot's 2:30 PM)
9. Reject if stock up > 2% on the day (squeeze protection)

### Exit Strategy
- **Trailing stop** (primary profit engine): Activates at 0.60R, trails at 0.40R distance
  - ~54% of exits, 100% win rate, ~75% of gross profit
- **Scalp take-profit**: Full exit at 1.0R (100% of position)
  - ~14% of exits, 100% win rate
- **Stop loss**: ATR-based, 4.5x ATR multiplier, min 0.80% distance
  - ~30% of exits, 0% win rate (by definition)
- **EOD close**: Flatten any remaining positions before market close
  - ~3% of exits

### Regime Detection (5-state)
| Regime | SPY vs SMA200 | Volatility | Action |
|--------|--------------|------------|--------|
| BULL_TREND | Above | Low | Shorts allowed (reduced) |
| BULL_VOLATILE | Above | High | Shorts allowed (reduced) |
| NEUTRAL | Near | Any | Shorts allowed (full size) |
| BEAR_VOLATILE | Below | High | Shorts allowed (full size) |
| BEAR_TREND | Below | Low | Shorts allowed (full size) |

Note: In v20 backtest, NEUTRAL regime accounted for 92.8% of bars and 90% of trades. The regime system primarily acts as a safety gate during strong bull trends.

---

## Backtest History

### v20 Final Results (252 trading days, $111,000 initial capital)
| Metric | Value |
|--------|-------|
| Total Trades | 133 |
| Win Rate | 66.9% |
| Profit Factor | 1.50 |
| Net P&L | +$24,192 |
| Return | +21.8% |
| Max Drawdown | 7.2% |
| Avg Winner | $315 |
| Avg Loser | $453 |

### Key Version Milestones
| Version | Return | Key Change |
|---------|--------|------------|
| v1 | +6.2% | Original long/short baseline |
| v13 | +7.9% | SHORT-ONLY breakthrough (removed longs) |
| v15 | — | Proved 100% exit at 1.0R beats partial exits |
| v17 | +8.3% | 1.0x size multiplier + compounding + v13 trailing |
| v19 | +18.7% | Position size 65% of equity |
| v20 | +21.8% | Position size 75% of equity (beats NASDAQ) |

### Production vs Backtest Sizing
| Parameter | Backtest (v20) | Production | Reasoning |
|-----------|---------------|------------|-----------|
| POSITION_SIZE_PCT | 0.75 | 0.35 | Conservative start, real slippage |
| MAX_SHORT_POSITIONS | 5 | 3 | Reduce concentration risk |
| MAX_RISK_PER_TRADE_PCT | 0.10 | 0.07 | Tighter risk per trade |

---

## Shared-Account Safety

This bot runs on the same Alpaca account as simple_bot and trend_bot. Safety mechanisms prevent interference:

### Directional Bot Safety
- Tags all orders with `dir_` prefix in `client_order_id`
- Only manages positions it has explicitly tracked in its internal state
- Checks existing positions before entering (won't short a stock another bot owns)
- Excludes TREND_BOT_SYMBOLS from its trading universe
- Shutdown only cancels `dir_` prefixed orders
- FLATTEN_ALL only closes its own tracked short positions

### Simple Bot Safety (fixes applied 2026-01-30)
- Reconcile skips short positions (directional_bot is the only short seller)
- Shutdown skips `dir_` and `TBOT_` prefixed orders when cancelling
- FLATTEN_ALL skips TREND_BOT_SYMBOLS and short positions
- EOD close has broker-level check to skip short positions
- Kill switch uses same filtered approach

### Trend Bot Safety (fixes applied 2026-01-30)
- Kill switch only cancels `TBOT_` prefixed orders
- FLATTEN_ALL only closes positions in its own ALL_TICKERS universe
- Added `_cancel_our_orders()` helper for filtered cancellation

### Safety Matrix
| Action | simple_bot | trend_bot | directional_bot |
|--------|-----------|-----------|-----------------|
| Order prefix | (none) | TBOT_ | dir_ |
| Position side | LONG only | LONG only | SHORT only |
| Cancel on shutdown | Skips dir_, TBOT_ | Only TBOT_ | Only dir_ |
| Flatten on shutdown | Skips shorts, TREND_BOT | Only ALL_TICKERS | Only own tracked |

---

## Configuration

### Environment File
`config/directional_bot.env`

Required settings:
- `ALPACA_API_KEY` - Same account as other bots
- `ALPACA_SECRET_KEY` - Same account as other bots
- `POLYGON_API_KEY` - For historical data

Safety settings:
- `KILL_SWITCH_DIRECTIONAL=0` - Set to 1 to halt all trading
- `SHUTDOWN_POLICY=CANCEL_ORDERS_ONLY` - Or FLATTEN_ALL
- `LIVE_TRADING=0` - Set to 1 + I_UNDERSTAND_LIVE_TRADING=YES for live

### Launcher
`launchers/Start Directional Bot.bat`

---

## Session Log

### Session: 2026-01-30

**Initial creation of directional_bot.py**

Created production bot from v20 backtest findings:
- 1,976 lines
- All 9 entry signal filters preserved from v20
- Software trailing stops as primary profit engine
- Conservative production sizing (35% position, 3 max shorts, 7% risk per trade)
- Shared-account safety mechanisms (dir_ prefix, position checking)

Classes implemented:
- `AlpacaClient` - REST API wrapper with retry logic
- `PolygonClient` - Historical data fetching
- `SoftwarePosition` - Tracks positions with software trailing stops
- `RegimeDetector` - 5-state SPY regime classification
- `RiskManager` - Daily loss limits, position limits
- `KillSwitch` - File/env-based emergency halt
- `TradeJournal` - JSONL trade logging
- `MarketData` / `MarketScanner` - Indicator calculation with caching
- `DirectionalBot` - Main orchestrator (scan, enter, manage, exit loop)

**Cross-bot safety audit and fixes**

Audited simple_bot.py and trend_bot.py for shared-account conflicts:
- simple_bot: 4 fixes (reconcile, shutdown, EOD close, kill switch)
- trend_bot: 2 fixes (kill switch, shutdown handler)

See "Shared-Account Safety" section above for details.

### Session: 2026-02-02

**Long/Short Directional Backtest (`backtest/directional_bot_backtest.py`)**

Created a new backtest script that tests BOTH long and short trading with regime-gated directional logic. Based on the simple_bot_backtest.py template with added regime detection and short-side entry logic.

**Results (252 days, $111K initial capital):**

| Metric | Overall | Long | Short |
|--------|---------|------|-------|
| Total Trades | 251 | 243 | 8 |
| Win Rate | 62.2% | 62.6% | 50.0% |
| Profit Factor | 1.16 | 1.18 | 0.68 |
| Net P&L | $45,168 | $49,505 | -$4,338 |
| Return | 40.7% | — | — |
| Max Drawdown | 17.1% | — | — |

**Regime Distribution:**
| Regime | Bars | Trades | WR | P&L |
|--------|------|--------|----|-----|
| BULL_TREND | 76.2% | 161 (161L/0S) | 62.1% | $38,266 |
| BULL_VOLATILE | 8.5% | 37 (37L/0S) | 64.9% | $5,776 |
| NEUTRAL | 15.3% | 53 (45L/8S) | 60.4% | $1,125 |
| BEAR_VOLATILE | 0% | 0 | — | — |
| BEAR_TREND | 0% | 0 | — | — |

**Exit Reasons:**
| Exit | Count | WR | P&L |
|------|-------|----|-----|
| TRAILING_STOP | 143 (57%) | 100% | $278,795 |
| STOP_LOSS | 78 (31%) | 0% | -$261,037 |
| EOD_CLOSE | 27 (11%) | 37% | $20,963 |
| SCALP_TP | 2 (1%) | 100% | $5,912 |

**Key Observations:**
1. 40.7% return is strong — long side carried the strategy
2. Only 8 short trades total — 2025 was overwhelmingly bullish (76.2% BULL_TREND)
3. No BEAR regime detected at all — shorts only triggered in NEUTRAL periods
4. Short PF 0.68 is unprofitable but sample size too small to draw conclusions (8 trades)
5. Regime gating worked correctly: no shorts in bull, no longs in bear
6. Trailing stop remains the dominant profit engine (57% of exits, 100% WR, $278K gross)
7. Long side alone: 243 trades at 62.6% WR is consistent with simple_bot v48c results

**Implication for Production:**
- The long side confirms simple_bot's edge with regime awareness
- Short side needs a bear market period to properly evaluate
- Current production directional_bot (short-only) is correct for its design — this backtest validates that adding longs via regime gating has merit but the short side needs more data

**Scanner v2 Integration & Backtest Comparison**

Integrated `market_scanner.py` into production `directional_bot.py` for dynamic universe discovery, then added scanner-equivalent features to the backtest for A/B comparison.

**Production Changes (directional_bot.py):**
1. Import enhanced MarketScanner with fallback
2. Updated TREND_BOT_SYMBOLS to v8 universe (added leveraged/momentum ETFs)
3. Added scanner config: `USE_ENHANCED_SCANNER`, `ENABLE_DYNAMIC_UNIVERSE`, `DYNAMIC_SCAN_INTERVAL_SEC`
4. Scanner initialization in `__init__()` with dynamic_universe tracking
5. Pre-market gap scan before main loop (if running before market open)
6. Periodic intraday scan every 2 minutes in main loop
7. `_scan_for_setups()` now iterates scanner-scored symbols first, then core
8. RVOL baselines include dynamic universe symbols

**Backtest v2 Scanner Features:**
1. Expanded universe: +19 high-beta stocks (MARA, RIOT, MSTR, AFRM, SMCI, ARM, etc.)
2. SPY VWAP intraday gate: Skip longs when SPY < VWAP, skip shorts when SPY > VWAP
3. RVOL quality boost: +10 score points for RVOL >= 2.0x candidates
4. SPY alignment bonus: +5 score points when trade direction matches SPY intraday momentum

**A/B Comparison Results (252 days, $111K initial):**

| Metric | Baseline (v1) | Scanner (v2) | Delta |
|--------|:---:|:---:|:---:|
| Total Trades | 252 | 406 | +154 |
| Win Rate | 61.9% | 63.8% | +1.9% |
| Profit Factor | 1.14 | 1.12 | -0.02 |
| Net P&L | $40,793 | $67,449 | +$26,656 |
| Return | 36.8% | 60.8% | +24.0% |
| Max Drawdown | 17.1% | 34.5% | +17.4% |

| Direction | Baseline | Scanner v2 |
|-----------|----------|------------|
| Long trades | 244, WR 62.3%, P&L $45,131 | 395, WR 63.3%, P&L $56,025 |
| Short trades | 8, WR 50.0%, P&L -$4,337 | 11, WR 81.8%, P&L $11,424 |

**Key Observations:**
1. +24% return improvement from scanner features (36.8% → 60.8%)
2. Short side flipped from unprofitable (-$4.3K) to profitable (+$11.4K) with PF 2.13
3. SPY VWAP gate dramatically improved short quality: 81.8% WR vs 50.0% baseline
4. Expanded universe added 154 more trades — more high-beta opportunities discovered
5. NEUTRAL regime dominated profits ($55.7K) — both longs and shorts profitable there
6. Max drawdown doubled (17.1% → 34.5%) — high-beta stocks are more volatile
7. Profit factor slightly lower (1.14 → 1.12) — more trades, but losers are larger ($3.9K vs $3.0K avg)

**Production Implications:**
- Scanner integration is clearly beneficial for returns (+65% improvement)
- SPY VWAP gate is the most impactful single feature (transformed short side profitability)
- Higher drawdown is expected with high-beta universe — production sizing (35%) should keep actual DD manageable
- May want to tighten position sizing for scanner-discovered symbols vs core symbols

### Session: 2026-01-31

**Robustness improvements based on cross-bot analysis**

Reviewed directional_bot against patterns found in simple_bot and trend_bot. Trailing stop implementation and state persistence are excellent. Three robustness gaps found and fixed:

**1. Exit Fill Verification** (lines ~1835-1945)

- **Problem**: When trailing stop, scalp TP, or EOD close triggered a buy-to-cover, the bot only waited 10 seconds for fill confirmation. If not confirmed, it used an estimated exit price and removed the position from tracking — even though the actual short position might still be open at the broker.
- **Fix**: Increased exit fill timeout from 10s to 30s. After timeout, now checks broker position directly: if position still exists, keeps it in tracker for retry on next cycle. Only removes from tracker when position is confirmed closed.

**2. Safety Stop Restoration on Failed Cover** (lines ~1935-1955)

- **Problem**: When closing a position, the safety net stop order was cancelled first (necessary to prevent double-buy). But if the cover order then failed, the position was left with no protection — safety stop cancelled, cover failed, position exposed.
- **Fix**: Added `_restore_safety_stop()` helper. If cover order fails and fallback also fails, the bot attempts to re-place the safety stop at the original stop price. Logs critical warning if restoration also fails, indicating manual intervention needed.

**3. Kill Switch Shared-Account Safety** (line ~942)

- **Problem**: `execute_emergency_shutdown()` called `cancel_all_orders()` which cancels ALL orders on the account, including simple_bot and trend_bot orders.
- **Fix**: Now uses filtered cancellation — only cancels orders with `dir_` prefix. Logs how many other bot orders were preserved.

---

## Known Issues / TODOs

- [ ] Monitor real slippage vs backtest assumptions before increasing position size
- [ ] Consider adding gradual EOD reduction (like simple_bot) instead of immediate flatten
- [ ] May want to add 5-min multi-timeframe confluence (like simple_bot v41)
- [ ] Consider adding Opening Range Breakdown filter (inverse of simple_bot's ORB)
- [ ] Test with live paper trading for 2-4 weeks before going live
- [ ] Position sizing ramp-up plan: 35% → 50% → 65% based on live performance
- [ ] Consider tighter sizing for scanner-discovered (non-core) symbols to manage DD
- [x] ~~Exit fill verification and safety stop restoration~~ (implemented 2026-01-31)
- [x] ~~Kill switch shared-account safety~~ (implemented 2026-01-31)
- [x] ~~Dynamic universe via MarketScanner integration~~ (implemented 2026-02-02)
- [x] ~~Scanner v2 backtest A/B comparison~~ (verified 2026-02-02: +24% return improvement)

---

## Symbol Universe

**Core (static):** Same 32 symbols as simple_bot (minus SQ which has no short data), plus DIA. Excludes TREND_BOT_SYMBOLS (ETFs managed by trend_bot).

**Dynamic (scanner-discovered):** MarketScanner adds high-beta/momentum stocks in real-time:
- Pre-market: Gap-up/down stocks with unusual volume
- Intraday: High-RVOL movers, volume acceleration candidates
- Scored and ranked by composite quality score (0-100)

**Backtest scanner symbols (v2):** MARA, RIOT, MSTR, AFRM, SOFI, UPST, RBLX, MRNA, ARM, SMCI, CRWD, MDB, ZS, SHOP, ROKU, SNAP, PINS, LYFT, UBER

Key symbols from backtest (highest frequency):
- DDOG, COIN, HOOD, PANW (tech/fintech - high RVOL movers)
- AVGO, UNH, BA (larger caps - occasional short setups)
- SMCI, MSTR, UPST, AFRM (scanner-discovered high-beta movers)
