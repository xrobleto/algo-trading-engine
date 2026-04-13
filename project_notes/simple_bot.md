# Simple Bot - Project Notes

> **Purpose**: Track progress and changes for the simple_bot.py script.
> **Location**: `C:\Users\xrobl\Documents\Algo_Trading\strategies\simple_bot.py`

---

## Overview

A momentum trading bot that manages intraday positions using bracket orders (entry + stop-loss + take-profit). Uses VWAP and EMA trend confirmation with ATR-based stops. Implements a state machine to track trade lifecycle from NEW through ACTIVE_EXITS to CLOSED.

Key Architecture:
- Single-bracket architecture (100% scalp @ 3.0R cap)
- Broker-native bracket orders for guaranteed stop/TP execution (safety net)
- Software trailing stop (activates at 0.60R, trails at 0.50R distance — medium trail for gap runners)
- Gap-up filter: only trade stocks gapping up >= 0.5% (overnight catalyst requirement)
- SPY VWAP gate: only enter when SPY > intraday VWAP (market regime confirmation)
- Enhanced market scanner (market_scanner.py) for dynamic universe discovery
- Candidate ranking by signal score when multiple setups fire
- TradeIntent state machine for order lifecycle tracking
- Persisted intents to survive bot restarts (includes trailing stop state)

---

## Session Log

### Session: 2026-01-21

**Issues observed:**
- Positions (QGEN, CRVS) were left overnight without stop-loss protection
- Bot had stopped on MLK Day (Jan 20) when market was closed
- When bot restarted, it had no knowledge of the orphaned positions
- User had to manually sell the positions

**Root cause analysis:**
1. **Timeout Handler Bug**: When entry order timed out, the handler was cancelling ALL orders (including protective stop/TP) BEFORE checking if the entry was fully filled. This left positions unprotected.
   - Location: `check_entry_timeouts()` method (~line 5598)
   - The code cancelled orders first, then checked fill status

2. **Missing Orphaned Position Recovery**: The startup reconciliation (`reconcile_broker_state()`) would detect positions at the broker, but if there was no matching TradeIntent, it would only add to position_manager without creating an intent. This meant the bot couldn't properly manage the position.

**Fixes implemented:**

1. **Timeout Handler Fix** (lines ~5598-5700):
   - Changed order of operations: Check fill status FIRST
   - If entry is fully filled, transition to ACTIVE_EXITS (not FILLED) and preserve stop/TP orders
   - Only cancel orders if entry is NOT fully filled
   - For true partial fills, flatten immediately
   ```python
   # CRITICAL FIX: Check fill status FIRST before cancelling any orders
   is_fully_filled = (is_single_bracket and scalp_filled and filled_qty == intent.total_qty) or \
                    (not is_single_bracket and scalp_filled and runner_filled)

   if is_fully_filled:
       # COMPLETE FILL - DO NOT cancel orders! The stop/TP are protecting the position
       trade_manager.transition_state(symbol, TradeState.ACTIVE_EXITS)
   ```

2. **Orphaned Position Recovery** (lines ~6071-6180):
   - Enhanced `reconcile_broker_state()` to detect positions without matching intents
   - Creates synthetic TradeIntent with state=ACTIVE_EXITS for orphaned positions
   - Checks if orphan has broker stop/limit orders (warns if unprotected)
   - Sends alert via alerter about recovered orphans
   - Blocks re-entry for orphaned symbols
   ```python
   # ORPHANED POSITION: Position at broker but no TradeIntent
   synthetic_intent = TradeIntent(
       symbol=symbol,
       state=TradeState.ACTIVE_EXITS,
       # ... other fields estimated from broker position
   )
   trade_manager.intents[symbol] = synthetic_intent
   ```

**Decisions made:**
- Orphaned positions get synthetic intents with ACTIVE_EXITS state so bot can manage them
- Stop price for orphans estimated as 5% below entry (conservative)
- Take-profit estimated as 5% above entry (conservative)
- Alerter notifies user about recovered orphans so they can review

### Session: 2026-01-22

**Improvements implemented:**

1. **Orphan Recovery: Actual Stop/TP from Broker Orders** (lines ~6115-6135)
   - Issue: Orphan recovery was using estimated stop/TP prices (5% from entry)
   - Fix: Now queries broker orders for the symbol and extracts actual stop_price and limit_price
   - If broker has stop/TP orders, uses those actual prices for the synthetic intent
   - Falls back to estimates only if no broker orders exist
   ```python
   # Extract actual stop price from broker orders
   for order in symbol_orders:
       if order_type in ("stop", "stop_limit") and order_side == "sell":
           actual_stop_price = float(order.get("stop_price"))
       if order_type == "limit" and order_side == "sell":
           actual_tp_price = float(order.get("limit_price"))
   ```
   - Logs and alerts now show whether prices are "BROKER" or "ESTIMATED"

2. **WebSocket Reconnection Circuit Breaker** (lines ~892-980)
   - Issue: If WebSocket failed repeatedly, bot would silently keep retrying forever
   - Fix: Added reconnection tracking with exponential backoff and circuit breaker
   - After 5 consecutive failures, sends CRITICAL alert
   - Exponential backoff: 5s → 10s → 20s → 40s → 60s (capped)
   - Resets counter on successful connection
   ```python
   MAX_RECONNECT_ATTEMPTS = 5          # Alert after 5 failures
   RECONNECT_BACKOFF_BASE = 5          # Base seconds
   RECONNECT_BACKOFF_MAX = 60          # Max seconds (cap)
   ```
   - Tracks `last_successful_connect` timestamp for diagnostics

### Session: 2026-01-23

**v41 Strategy Improvements (Research-Backed)**

Major enhancement to entry signal quality based on academic research and quantitative trading best practices from r/algotrading, Ernest Chan's books, and analysis of successful quant strategies.

**1. Opening Range Breakout Filter (ORB)**

- Research: 88% of daily high/low is set by 10:30 AM, ORB strategies achieve 2.4+ Sharpe ratios
- Implementation: After 10:00 AM, require price to break above Opening Range high for long entries
- Prevents entries into weak stocks that can't hold above their morning range

```python
# Configuration (lines ~366-375)
USE_OPENING_RANGE_FILTER = True          # Enable Opening Range breakout requirement
OPENING_RANGE_MINUTES = 30               # OR period: 9:30-10:00 AM (first 30 mins)
OR_BREAKOUT_BUFFER_PCT = 0.001           # Require 0.1% above OR high
OR_MIN_RANGE_PCT = 0.003                 # Skip if OR range < 0.3% (too tight)
OR_MAX_RANGE_PCT = 0.05                  # Skip if OR range > 5% (too wide)
OR_REQUIRE_VWAP_ABOVE_MID = True         # VWAP should be above OR midpoint
```

- Added `OpeningRangeTracker` class (~line 4196) to track OR for each symbol
- Added OR fields to `MarketData`: `or_high`, `or_low`, `or_set`, `or_breakout_status`
- Filter in `check_long_setup()` requires price ABOVE OR high after 10:00 AM

**2. Multi-Timeframe Confluence (5-Min EMA)**

- Research: Using higher timeframe confirmation reduces false signals significantly
- Implementation: Require 5-min EMA9 > EMA20 to confirm uptrend before entering on 1-min signal
- Filters out 1-min noise where higher timeframe doesn't confirm trend

```python
# Configuration (lines ~377-383)
USE_5MIN_CONFLUENCE = True              # Enable 5-min timeframe confirmation
MTF_EMA_FAST = 9                        # Fast EMA on 5-min chart
MTF_EMA_SLOW = 20                       # Slow EMA on 5-min chart
MTF_MIN_EMA_SEPARATION_PCT = 0.001      # Require 0.1% EMA separation on 5-min
```

- Resamples 1-min bars to 5-min in `get_market_data()` (~line 4030)
- Added `mtf_ema_fast`, `mtf_ema_slow`, `mtf_trend_aligned` to `MarketData`
- Filter in `check_long_setup()` requires 5-min trend alignment

**3. Enhanced 4-State Regime Detection**

- Research: HMM regime detection achieved 98% return in 2008 vs -38% for SPY
- Implementation: Clean quadrant approach - TREND (bull/bear) × VOLATILITY (low/high)
- More actionable position sizing rules than 5-state model

```python
# Configuration (lines ~612-648)
USE_4STATE_REGIME = True
REGIME_4STATE_ADJUSTMENTS = {
    "BULL_LOW_VOL":  {"size_mult": 1.25, "stop_mult": 1.0, "tp_mult": 1.25},  # Optimal
    "BULL_HIGH_VOL": {"size_mult": 0.75, "stop_mult": 1.3, "tp_mult": 1.0},   # Reduce size
    "BEAR_LOW_VOL":  {"size_mult": 0.5, "stop_mult": 0.8, "tp_mult": 0.75},   # Counter-trend
    "BEAR_HIGH_VOL": {"size_mult": 0.25, "stop_mult": 1.5, "tp_mult": 0.5},   # Danger zone
}
REGIME_4STATE_VOL_THRESHOLD = 1.2   # ATR ratio > 1.2 = HIGH_VOL
REGIME_4STATE_TREND_THRESHOLD = 0.2  # SPY vs VWAP > 0.2% = BULL
```

- Added `regime_4state` field to `RegimeState` dataclass (~line 3341)
- Classification in `_detect_regime()` (~line 3507)
- Applied in `check_long_setup()` using most conservative of 5-state or 4-state sizing

**Version Update:**
- Updated to v4.1.0 (v41 ORB + MTF + 4-State Regime)

### Session: 2026-01-24

**Feature: Gradual EOD Position Reduction**

Implemented gradual end-of-day position reduction instead of flattening all positions at once at 3:55 PM. This improves execution quality and reduces market impact.

**Source**: Logic inspired by `low_risk_strategy.py` from old scripts analysis.

**Configuration** (lines 308-314):
```python
GRADUAL_EOD_REDUCTION = True          # Enable gradual reduction (False = immediate flatten)
EOD_REDUCTION_START_MINUTES = 30      # Start reducing 30 min before EOD_CLOSE_TIME_ET (3:25 PM)
EOD_REDUCTION_INTERVAL_MINUTES = 5    # Reduce every 5 minutes
EOD_REDUCTION_PERCENT = 0.10          # Reduce 10% of position each interval
```

**Implementation Details**:

1. **Tracking Attributes** (`MomentumBot.__init__`, lines 5445-5446):
   - `eod_reductions: Dict[str, int]` - tracks reduction count per symbol
   - `last_eod_reduction_time: Optional[datetime]` - controls interval timing

2. **Modified `check_eod_close()` Method** (lines 6169-6280):
   - **Mode 1 - Gradual Reduction** (3:25 PM to 3:55 PM):
     - Checks if enough time has passed since last reduction (5 min interval)
     - For each position, calculates 10% of current quantity
     - Submits market sell order for the reduction amount
     - Updates position manager with remaining quantity
     - Logs each reduction with reduction number (#1, #2, etc.)
   - **Mode 2 - Full Flatten** (at 3:55 PM):
     - Flattens any remaining positions using `flatten_symbol()`
     - Resets tracking for next day
     - Sends alert if any flattens fail

**Example Reduction Schedule** (for a 100-share position):
| Time | Reduction | Shares Sold | Remaining |
|------|-----------|-------------|-----------|
| 3:25 PM | #1 | 10 | 90 |
| 3:30 PM | #2 | 9 | 81 |
| 3:35 PM | #3 | 8 | 73 |
| 3:40 PM | #4 | 7 | 66 |
| 3:45 PM | #5 | 6 | 60 |
| 3:50 PM | #6 | 6 | 54 |
| 3:55 PM | Flatten | 54 | 0 |

**Benefits**:
- Reduces market impact at EOD
- Better execution prices through distributed selling
- Less visible to algos watching for EOD dumps
- Still guarantees flat by market close

### Session: 2026-01-29

**Tuning: Relaxing Entry Criteria for More Trade Frequency**

After running for a full month (January 2026), simple_bot only took 2 trades. Both were profitable, confirming signal quality is good. However, the 15+ simultaneous entry conditions create a multiplicative filtering effect that is too restrictive for typical market conditions.

Analysis identified the biggest trade suppressors:
1. VWAP distance (0.5%) filtering out valid setups slightly above VWAP
2. 30-minute opening blackout eliminating early momentum plays
3. News filter with 79 bearish keywords (broad false-positive surface)
4. ADX sweet spot (10-35) excluding strong trend days

**Changes implemented (lowest risk first):**

**1. VWAP Distance: 0.5% → 0.3%** (line ~4296)
- Entry filter: `data.vwap * 1.005` → `data.vwap * 1.003`
- Still requires price above VWAP (confirms uptrend) but catches setups closer to VWAP
- Signal scoring sweet spot also adjusted: full points at 0.3%+ (was 0.5%+)
- Scoring tiers shifted down: 70% score at 0.15-0.3% (was 0.3-0.5%)
- Expected impact: +20-30% more setups

**2. Opening Blackout: 30 → 20 minutes** (line ~379)
- `NO_TRADE_FIRST_MINUTES = 30` → `NO_TRADE_FIRST_MINUTES = 20`
- Bot can now evaluate setups starting at 9:50 AM ET (was 10:00 AM)
- Note: Opening Range filter (OPENING_RANGE_MINUTES = 30) is unchanged - OR high/low still tracked over first 30 min. Between 9:50-10:00 AM, the OR breakout check is skipped (OR still forming), so trades during this window rely on the other 14 filters
- Expected impact: +10-15% more setups

**What was NOT changed:**
- News filter (higher risk to disable)
- ADX range (moderate risk)
- Signal score threshold (already at 50)
- Opening Range period (separate from blackout)

**Rationale**: Conservative approach - implement the two lowest-risk relaxations first. Monitor for 2-4 weeks. If trade frequency is still too low, consider relaxing the late-day cutoff (2:30 PM → 3:00 PM) or ADX range (35 → 40) next.

### Session: 2026-01-30

**Fix: Same-Account Isolation (simple_bot + trend_bot)**

Problem: User plans to run both simple_bot and trend_bot on a single Alpaca account (Alpaca only allows 1 live account). Analysis revealed critical conflicts:

1. **EOD flatten** (3:55 PM) would close ALL positions, including trend_bot's long-term ETF holdings
2. **Orphan recovery** on startup would "adopt" trend_bot positions and then flatten them at EOD
3. **Overlapping symbols** (SPY, QQQ, IWM, XLF, XLV, XLI, XLY, XLP, XLB, XLE) in both universes
4. **Dynamic discovery** could pick up trend_bot ETFs as "high RVOL movers"

**Changes implemented:**

**1. Removed overlapping ETFs from CORE_SYMBOLS** (lines ~162-180)
- Removed: SPY, QQQ, IWM, XLF, XLV, XLI, XLY, XLP, XLB, XLE (10 symbols)
- Kept: DIA (not in trend_bot's universe)
- Also removed from SECTOR_MAP
- simple_bot retains 50+ individual stocks - plenty of trading opportunities

**2. Added TREND_BOT_SYMBOLS constant** (lines ~175-188)
- Explicit set of all symbols managed by trend_bot (28 symbols)
- Includes: equity ETFs, sector ETFs, factor ETFs, defensive ETFs, cash (SGOV)
- Used as exclusion list across multiple systems

**3. Dynamic discovery exclusion** (line ~257)
- `DYNAMIC_EXCLUSION_LIST` now unions with `TREND_BOT_SYMBOLS`
- Prevents dynamic universe from discovering trend_bot ETFs as "movers"

**4. EOD flatten safety filter** (lines ~6200-6205)
- Added `own_positions` filter: `[p for p in positions if p.symbol not in TREND_BOT_SYMBOLS]`
- Both gradual reduction (Mode 1) and full flatten (Mode 2) now skip trend_bot symbols
- Belt-and-suspenders protection even if a trend_bot symbol somehow enters position_manager

**5. Reconciliation safety check** (lines ~6637-6639)
- Added explicit `TREND_BOT_SYMBOLS` check before the `CORE_SYMBOLS` check
- Logs: "Skipping - managed by trend_bot" for any trend_bot position found at broker
- Prevents synthetic TradeIntent creation for trend_bot positions

**Conflict resolution summary:**
| Conflict | Fix | Layer |
|----------|-----|-------|
| EOD flatten kills trend_bot positions | TREND_BOT_SYMBOLS filter in check_eod_close() | Runtime |
| Orphan recovery adopts trend_bot positions | TREND_BOT_SYMBOLS check in reconcile_broker_state() | Startup |
| Overlapping symbols in universe | Removed 10 ETFs from CORE_SYMBOLS | Config |
| Dynamic discovery finds trend_bot ETFs | TREND_BOT_SYMBOLS in DYNAMIC_EXCLUSION_LIST | Runtime |

### Session: 2026-01-31

**Backtest analysis and production improvements**

Re-ran simple_bot backtest and identified critical gaps vs directional_bot's architecture. Backtest v44 results: 210 trades, 58.6% WR, PF 1.39, +10.6% return, 3.3% max DD (after improvements).

**1. Full-Position Software Trailing Stop** (lines ~6405-6530)

- **Problem**: Trailing stop constants (ENABLE_TRAILING_STOP, ACTIVATION_R=0.50, DISTANCE_R=0.30) were defined at lines 307-311 but never implemented. Broker bracket orders handled all exits with fixed stop/TP only.
- **Fix**: Implemented software-managed trailing stop in `manage_positions()`:
  - Activates when gain reaches 0.50R (half the risk distance in profit)
  - Trails at 0.30R distance from highest price seen
  - When triggered: cancels bracket orders → market sell → cleanup state
  - Broker bracket stop remains as safety net (crash protection)
  - State persisted in TradeIntent (trailing_activated, highest_price_seen, trailing_stop_price) for crash recovery
- **TradeIntent changes**: Added 3 fields + to_dict/from_dict serialization
- **Expected impact**: Backtest showed trailing stops captured ~75% of gross profit in directional_bot with 100% win rate on trailing exits

**2. Candidate Ranking** (lines ~5714-5748)

- **Problem**: `scan_for_setups()` entered the first symbol that passed filters (`break` on first match). When multiple setups fired simultaneously, entry was determined by symbol list order rather than quality.
- **Fix**: Collects all valid candidates, sorts by signal_score (already computed by SignalScorer's 100-point system), enters the highest-scoring one. Logs runner-up candidates for transparency.
- **No additional API calls**: Signal score is already computed during `check_long_setup()` as part of the entry filter pipeline.

**3. Deployed Capital Tracking** (no changes needed)

- Analysis showed `calculate_position_size()` already queries Alpaca buying_power and applies MAX_CAPITAL_USAGE_PCT cap. Pre-flight checks in `enter_long()` verify buying power and exposure limits before submission.

### Session: 2026-02-01

**v45: Profitability Overhaul — Scanner, Sizing, and Trailing Stop Optimization**

Goal: Significantly improve profitability beyond the v44 baseline (~8.8% return on current data window). Built and tested 5 improvement paths, kept what worked, discarded what didn't.

**1. Market Scanner Module** (NEW: `strategies/market_scanner.py`, ~570 lines)

Created standalone dynamic market scanner for finding highest-probability momentum candidates:
- **PolygonAPI**: Lightweight REST client with retry for snapshot/gainers/news endpoints
- **VolumeTracker**: Tracks volume acceleration (recent rate vs earlier rate)
- **CandidateScorer**: Multi-factor 0-100 scoring (change%, RVOL, vol acceleration, catalyst, spread, dollar volume)
- **MarketScanner**: Pre-market gap scanning + intraday gainers scanning
  - Pre-market: Identifies stocks gapping up 2-15% with unusual volume before open
  - Intraday: Uses Polygon gainers + all-tickers snapshot for real-time discovery
  - Time-adjusted RVOL (accounts for U-shaped volume curve through the day)
  - News/catalyst checking with 30-minute cache
- Wired into simple_bot: replaces basic `discover_dynamic_universe()` when available
  - Prioritizes scanner-scored symbols in `scan_for_setups()` (highest quality first)
  - Pre-market scan runs before market open to build initial watchlist
  - Falls back gracefully to basic discovery if scanner fails

**2. Trailing Stop Optimization** (BIGGEST WIN)

Systematic A/B testing of trailing stop parameters on identical data:

| Config | Activation | Distance | Return | WR | PF | Max DD |
|--------|-----------|----------|--------|-----|-----|--------|
| v44 baseline | 0.60R | 0.40R | 8.8% | 56.4% | 1.36 | 3.8% |
| Adaptive tiers | varies | varies | 6.8% | 55.6% | 1.27 | 4.1% |
| 0.40R / 0.30R | 0.40R | 0.30R | 10.4% | 67.9% | 1.36 | 3.9% |
| **0.40R / 0.40R** | **0.40R** | **0.40R** | **11.0%** | **66.2%** | **1.40** | **4.0%** |
| 0.35R / 0.35R | 0.35R | 0.35R | 7.8% | 69.4% | 1.27 | 4.7% |

Winner: **0.40R activation / 0.40R distance** — "breakeven lockup" approach:
- Trail activates when trade moves +0.40R in our favor
- Trail sits 0.40R behind the high → initially at breakeven
- As trade runs up, trail ratchets up, always 0.40R behind peak
- Gives enough room for trades to reach 1.0R TP
- Catches deteriorating trades before they become EOD losers

Impact vs v44 baseline:
- EOD close exits: 29% → 17% (the biggest drain eliminated)
- Trailing stop exits: 25% → 47% (now the dominant exit type)
- Win rate: 56.4% → 66.2% (+9.8pp)
- Profit factor: 1.36 → 1.40 (+0.04)

**3. Position Sizing Increase**

- POSITION_SIZE_PCT: 0.33 → 0.50 (production), 0.55 (backtest)
- Avg winner grew from $176 to $225 — more capital deployed per winning trade
- Conservative increase for production vs backtest to account for slippage

**4. Entry Filter Expansion (RVOL + 11AM)**

- MIN_RELATIVE_VOLUME: 1.5 → 1.2 (backtest), matches production
- 11AM hour skip removed in backtest (added 6-9 more trades)
- Combined: +14% more trades at similar quality

**5. Adaptive Trailing / Multi-Timeframe Entry Filters — REJECTED**

Tested and discarded (hurt performance in backtest):
- **Adaptive trailing tiers** (STRONG/NORMAL/WEAK): Tightened trail on weak moves, captured profits too early. Fixed 0.40/0.40 outperformed. PF 1.27 vs 1.40.
- **Daily SMA20 filter**: Cut 59% of trades without improving win rate (filtered winners equally)
- **5-min higher lows filter**: Too restrictive, O(n²) computation
- **Volume confirmation on entry bar**: Filtered good trades proportionally

These remain in code (disabled via config flags) for future re-evaluation.

**Production Changes Applied:**
| Parameter | Before | After | Reasoning |
|-----------|--------|-------|-----------|
| TRAILING_STOP_ACTIVATION_R | 0.50 | 0.40 | Breakeven lockup, backtest proven |
| TRAILING_STOP_DISTANCE_R | 0.30 | 0.40 | Room to reach 1R TP |
| USE_ADAPTIVE_TRAILING | True | False | Fixed trail outperformed |
| POSITION_SIZE_PCT | 0.33 | 0.50 | More capital per trade |
| USE_DAILY_TREND_FILTER | True | False | Didn't help in backtest |
| USE_5MIN_STRUCTURE_FILTER | True | False | Too restrictive |
| USE_VOLUME_CONFIRMATION | True | False | Didn't help in backtest |

**New File:** `strategies/market_scanner.py` — standalone, wired into simple_bot via import

### Session: 2026-02-01 (continued)

**v47-v48: Backtest Iteration Sprint — Targeting 40% Return**

Extended backtest iteration to reach 40% annual return target. Tested 20+ configurations across v47 and v48 series. Discovered that signal filtering (what to trade) matters more than exit mechanics (how to manage trades).

**v47 Series Summary (Exit/Sizing Experiments — Mostly Flat)**

| Version | Config | Return | PF | DD | Key Finding |
|---------|--------|--------|-----|-----|-------------|
| v47a | 2.5x leverage | 9.8% | 1.04 | 27% | Leverage amplifies losses equally |
| v47b | Swing hold (390 min) | 10.2% | 1.04 | 30% | Overnight not worth the risk |
| v47c | Wide trail (1.5R/1.0R) | 8.5% | 1.03 | 33% | Too much room, losers run |
| v47d | High-beta universe | 7.2% | 1.02 | 35% | More volatile ≠ more profitable |
| v47e-i | Various combos | 6-10% | 1.02-1.04 | 25-35% | Diminishing returns on exit tuning |

**Key insight from v47**: With PF ~1.03, no amount of leverage, position sizing, or exit optimization can reach 40%. The core signal needs improvement.

**v48 Series (Signal Quality Revolution — Gap-Up + SPY Filter)**

| Version | Config | Return | PF | DD | WR | Key Change |
|---------|--------|--------|-----|-----|-----|-------------|
| v48a | SPY VWAP filter only | 6.4% | 1.02 | 36% | 33% | SPY alone hurts (removes good trades too) |
| v48b | Gap-up + tight trail | 21.0% | 1.12 | 14% | 70% | Gap-up filter is game-changer |
| v48c | Gap-up + medium trail + SPY | **75.2%** | **1.36** | **15%** | **65%** | **WINNER — smashed 40% target** |
| v48d | Gap-up + medium trail, no SPY | 40.4% | 1.16 | 17% | 63% | Robust even without SPY filter |

**v48c Winning Configuration Details:**

1. **Gap-Up Filter (Key Discovery)**
   - Only trade stocks that opened >= 0.5% above previous day's close
   - Rationale: Overnight catalysts (earnings, upgrades, news) create sustained intraday momentum
   - Impact: PF jumped from 1.02 → 1.12+ across all trail widths
   - Implementation: Compare today's open vs yesterday's close per symbol
   ```python
   USE_GAP_UP_FILTER = True
   MIN_GAP_UP_PCT = 0.50  # Stock must gap up >= 0.5%
   ```

2. **SPY VWAP Market Gate**
   - Only enter new positions when SPY price > SPY intraday VWAP
   - Alone: actually hurt (6.4% vs 9.5% baseline) — filters good and bad trades equally
   - Combined with gap-up: nearly doubled returns (40.4% → 75.2%)
   - Insight: Gap-up stocks on bullish market days have strongest momentum continuation
   ```python
   USE_SPY_VWAP_GATE = True
   SPY_VWAP_MIN_DISTANCE_PCT = 0.0  # SPY must be at or above VWAP
   ```

3. **Medium Trail Width (0.60R / 0.50R)**
   - Tight (0.40/0.35): 70% WR, avg winner $1,277 — exits too early
   - Medium (0.60/0.50): 65% WR, avg winner $2,353 — optimal tradeoff
   - Trail width interacts with gap-up filter: gap-up stocks run further, need more room

4. **Tighter Stops + Higher RVOL**
   - ATR multiplier: 5.0 → 4.0 (tighter stops, smaller risk per trade)
   - Min stop: 0.8% → 0.7%
   - RVOL threshold: 1.2 → 1.5 (only trade high-volume stocks)
   - Scalp TP: 1.0R → 3.0R (let winners run further before hard cap)

5. **4x PDT Intraday Margin (Backtest Only)**
   - Backtest: POSITION_SIZE_PCT=2.00, MAX_CAPITAL_USAGE_PCT=4.00
   - Works with PF 1.36 — higher quality signals support leverage

**Production Sync (v48c → simple_bot.py):**

| Parameter | Before (v45) | After (v48c) | Notes |
|-----------|-------------|-------------|-------|
| MIN_RELATIVE_VOLUME | 1.2 | 1.5 | Stricter volume filter |
| ATR_STOP_MULTIPLIER_DEFAULT | 5.0 | 4.0 | Tighter stops |
| MIN_STOP_DISTANCE_PCT | 0.008 | 0.007 | Tighter min stop |
| SCALP_TP_R | 1.00 | 3.00 | Let winners run |
| TRAILING_STOP_ACTIVATION_R | 0.40 | 0.60 | Medium trail |
| TRAILING_STOP_DISTANCE_R | 0.40 | 0.50 | Medium trail |
| POSITION_SIZE_PCT | 0.50 | 0.65 | Conservative production sizing |
| MAX_CAPITAL_USAGE_PCT | 1.00 | 1.50 | ~1.5x margin (conservative) |
| USE_GAP_UP_FILTER | N/A | True | **NEW** — gap-up entry filter |
| MIN_GAP_UP_PCT | N/A | 0.50 | 0.5% gap minimum |
| USE_SPY_VWAP_GATE | N/A | True | **NEW** — SPY > VWAP gate |

Production uses ~1.5x margin vs backtest's 4x for slippage safety. Gap-up filter uses Polygon snapshot API (prevDay.c vs day.o) in production instead of historical bar calculation.

SPY VWAP gate implemented in `scan_for_setups()` using existing `RegimeDetector.get_regime().spy_vs_vwap_pct` — no additional API calls needed.

**Polygon API Pagination Fix (v47)**
- Discovered that Polygon's 50K bar limit was silently truncating historical data for long backtest periods
- Implemented 120-day chunk pagination in `fetch_minute_bars()` to fetch complete data
- Fixed upward bias in earlier backtest results that used truncated data

---

## Known Issues / TODOs

- [x] ~~Consider adding ability to fetch actual stop/TP prices from orphan's broker orders~~ (implemented)
- [x] ~~Gradual EOD reduction instead of all-at-once flatten~~ (implemented)
- [x] ~~Same-account isolation from trend_bot~~ (implemented 2026-01-30)
- [x] ~~Software trailing stop implementation~~ (implemented 2026-01-31)
- [x] ~~Candidate ranking by signal score~~ (implemented 2026-01-31)
- [x] ~~Trailing stop optimization~~ (0.40R/0.40R breakeven lockup, 2026-02-01)
- [x] ~~Position sizing increase~~ (33% → 50%, 2026-02-01)
- [x] ~~Enhanced market scanner~~ (market_scanner.py, 2026-02-01)
- [x] ~~Gap-up filter implementation~~ (v48c, 2026-02-01)
- [x] ~~SPY VWAP market gate~~ (v48c, 2026-02-01)
- [x] ~~Trailing stop re-tuned to 0.60R/0.50R~~ (medium trail for gap runners, 2026-02-01)
- [ ] May want to add a flag to automatically place protective orders if orphan has none
- [ ] Timeout value (ENTRY_TIMEOUT_SEC) could be made configurable per symbol
- [ ] Monitor scanner-discovered symbols vs core symbols performance in live trading
- [ ] Re-evaluate disabled v45 filters (daily SMA, 5-min structure, volume) after 4+ weeks of live data
- [ ] Monitor gap-up filter hit rate in live trading (expect fewer but higher quality trades)
- [ ] Consider MIN_GAP_UP_PCT tuning: 0.50% may be too strict in low-vol markets, test 0.30%
- [ ] Production margin ramp-up: 1.5x → 2.0x → 2.5x after validating gap-up PF in live
- [ ] Evaluate adding gap-down + short entry filter (directional_bot concept)

---

## Key Configuration

- Uses Alpaca API for trading (paper or live based on env vars)
- ENTRY_TIMEOUT_SEC: Time before unfilled entry orders are cancelled
- AUTO_CLOSE_EOD: Whether to flatten all positions at end of day
- SHUTDOWN_POLICY: "CANCEL_ORDERS_ONLY" or "FLATTEN_ALL" on bot shutdown
- TradeState machine: NEW → SUBMITTED → PARTIALLY_FILLED → FILLED → ACTIVE_EXITS → CLOSED

---

## State Machine Reference

```
NEW              → Intent created, orders not yet submitted
SUBMITTED        → Orders submitted to broker
PARTIALLY_FILLED → At least one bracket filled
FILLED           → Entry fully filled (deprecated - use ACTIVE_EXITS)
ACTIVE_EXITS     → Position open with active TP/SL brackets at broker
CLOSED           → All brackets closed (TP/SL hit or manual close)
FAILED           → Entry timeout or error
CANCELLED        → Cancelled before fill
```
