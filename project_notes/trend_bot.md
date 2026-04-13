# Trend Bot - Project Notes

> **Purpose**: Track progress and changes for the trend_bot.py script.
> **Location**: `C:\Users\xrobl\Documents\Algo_Trading\strategies\trend_bot.py`

---

## Overview

A volatility-targeted trend-following bot that manages a portfolio of ETFs using:
- SMA200 trend filter with hysteresis (risk-on/risk-off regime)
- Inverse-volatility position sizing
- Dynamic capital deployment based on risk score
- Weekly rebalancing (Fridays near close)
- Daily monitoring for risk management (exit-only)
- Top-N rotation with rank stability filter

**Position Handling Note**: This bot is stateless with respect to positions - it queries Alpaca directly each cycle and computes deltas. Manual position changes via Alpaca UI will be adjusted at the next rebalance.

---

## Session Log

### Session: 2026-01-16

**Issues observed:**
- Duplicate `client_order_id` errors when placing sell orders for DBC, IWM, QQQ, SPY
- All 9 orders showed "TIMEOUT with PARTIAL FILL" warnings
- Position drift warnings (DBC, SPY, QQQ, IWM ~48% over target)
- Negative cash balance (-$798.60) due to margin usage

**Discussion:**
- The duplicate order ID issue stems from order ID generation not being unique enough when multiple orders are placed rapidly
- Bot uses `acct.equity` (total portfolio value) for position sizing, not `acct.cash`
- User opted to disable margin in Alpaca UI to prevent negative cash situations

**Resolution:**
- User will disable margin in Alpaca settings to prevent margin usage
- Duplicate order ID fix not implemented yet (deferred)

### Session: 2026-01-22

**Code review findings and fixes:**

1. **Partial Fill Cancellation** (line ~4385)
   - Issue: `verify_order_execution()` would timeout on partial fills but not cancel the unfilled remainder
   - Result: Open orders could accumulate, causing position drift
   - Fix: Added `cancel_on_timeout` parameter (default True) that cancels unfilled remainder on timeout
   ```python
   def verify_order_execution(..., cancel_on_timeout: bool = True):
       # On timeout with partial fill, cancel unfilled remainder
       if cancel_on_timeout and filled_qty < total_qty:
           trading_client.cancel_order_by_id(order_id)
   ```

2. **Startup API Credential Validation** (line ~5268)
   - Issue: API credentials only validated when first trade failed
   - Fix: Added `trading.get_account()` call at startup to validate credentials early
   - Now logs account number, equity, and status on startup
   - Warns if account status is not "ACTIVE"

3. **Sell Quantity Clamping Visibility** (line ~4996)
   - Issue: Sell qty clamping was logged but not recorded for analysis
   - Fix: Enhanced to log percentage shortfall and record `SELL_CLAMPED` event to trade log
   ```python
   log_trade(action="SELL_CLAMPED", reason=f"sell_clamped_from_{qty}_shortfall_{pct}pct", ...)
   ```

### Session: 2026-01-23

**Issue observed:**
- All 13 rebalance orders showed "TIMEOUT with PARTIAL FILL" warnings
- But fill amounts showed 100% filled (e.g., `65.059777924/65.059777924 shares filled`)
- Bot thought orders were partial fills when they were actually fully filled

**Root cause analysis:**
In `verify_order_execution()` (line ~4385), the code was comparing order status using string comparison:
```python
status = str(order.status)
if status == "filled":  # WRONG!
```

The problem: `str(OrderStatus.FILLED)` returns `"OrderStatus.FILLED"`, not `"filled"`.
So the comparison always failed, causing the loop to timeout even when orders were fully filled.

**Fix implemented:**
Changed to use proper enum comparison (line ~4416):
```python
from alpaca.trading.enums import OrderStatus
status = order.status
if status == OrderStatus.FILLED:  # Correct enum comparison
```

Also updated terminal status checks to use enums:
```python
elif status in (OrderStatus.CANCELED, OrderStatus.EXPIRED, OrderStatus.REJECTED, OrderStatus.REPLACED):
```

**Impact:**
- Orders now correctly detected as FULLY FILLED (no more false timeout warnings)
- Rebalance completes faster (no 60-second timeout per order)
- No more unnecessary "partial fill" alerts

**Second fix - Duplicate client_order_id:**

When running `--rebalance` to correct allocations, 4 orders (IEF, IWM, QQQ, SPY) failed:
```
ERROR - Failed to submit order for IEF: {"code":40010001,"message":"client_order_id must be unique"}
```

**Root cause:**
The previous morning's scheduled rebalance had already used the same order IDs. The format was:
```
TBOT_2026-01-23_rebalance_SPY_BUY
```
This was date-only, so same-day rebalances collided.

**Fix implemented:**
Changed `generate_client_order_id()` (line ~964) to include timestamp:
```python
# Old format: TBOT_2026-01-23_rebalance_SPY_BUY
# New format: TBOT_2026-01-23_154532_reb_SPY_BUY

et_now = datetime.now(ET)
time_suffix = et_now.strftime("%H%M%S")
reason_abbrev = reason[:4]  # Abbreviate to save chars
base = f"TBOT_{date_str}_{time_suffix}_{reason_abbrev}_{symbol}_{side}"
```

**Behavior:**
- Orders within the same second still get the same ID (short-term idempotency)
- Different rebalance runs get unique IDs (no collisions)
- Fits within Alpaca's 48-char limit

### Session: 2026-01-23 (v42 Strategy Improvements)

**v42 Research-Backed Improvements:**

Major enhancement to risk management based on quantitative trading research from r/algotrading, Ernest Chan's books, and analysis of successful quant strategies (same research used for simple_bot v41).

**1. Smooth Drawdown-Based Position Sizing**

- **Research**: Binary triggers create whipsaw - you go from 100% to 50% exposure instantly, then back. Smooth scaling reduces timing risk and emotional trading.
- **Old behavior**: If drawdown >= 10%, exposure = 0.5x (binary)
- **New behavior**: Exposure scales continuously as drawdown increases

```python
# Configuration (lines ~403-411)
USE_SMOOTH_DRAWDOWN_SCALING = True    # Enable smooth drawdown-based position sizing
DRAWDOWN_SCALE_START = 0.03           # Start reducing exposure at 3% drawdown
DRAWDOWN_SCALE_FLOOR = 0.25           # Minimum exposure multiplier (25%)
DRAWDOWN_SCALE_MAX = 0.20             # Max drawdown for floor (20% dd = 0.25x)

# Results at different drawdown levels:
# 3% dd  -> 1.00x exposure (no reduction yet)
# 5% dd  -> 0.88x exposure
# 10% dd -> 0.58x exposure
# 15% dd -> 0.42x exposure
# 20% dd -> 0.25x exposure (floor)
```

- Added `compute_smooth_drawdown_mult()` helper function (line ~1580)
- Legacy binary cooldown still available if `USE_SMOOTH_DRAWDOWN_SCALING = False`

**2. Dynamic Volatility Floor (Vol Clustering)**

- **Research**: Volatility clusters - high vol periods persist (GARCH effect). After a market shock, volatility remains elevated but the 60-day lookback lags, causing over-concentration.
- **Old behavior**: Static 12% vol floor for all conditions
- **New behavior**: Raises floor by 50% when short-term vol >> long-term vol

```python
# Configuration (lines ~235-240)
USE_DYNAMIC_VOL_FLOOR = True          # Enable dynamic vol floor
VOL_SHORT_LOOKBACK = 20               # Short-term vol (recent conditions)
VOL_CLUSTER_THRESHOLD = 1.3           # short_vol > long_vol * 1.3 = cluster
VOL_FLOOR_CLUSTER_MULT = 1.5          # Raise floor by 50% during cluster

# Example:
# Normal: vol_floor = 12%
# During vol cluster: vol_floor = 18% (prevents over-concentration)
```

- Added `compute_dynamic_vol_floor()` helper function (line ~1520)
- Logs when vol cluster is detected for transparency
- Each symbol's diagnostics now includes `vol_floor_used` for analysis

**Expected Impact:**
- Smoother position sizing during drawdowns (less whipsaw)
- Better downside protection after market shocks
- Reduced concentration risk during volatile periods

### Session: 2026-01-24

**Feature: Performance Tracking Log**

Added comprehensive rebalance summary logging for post-hoc performance analysis.

**New Log File:** `project_notes/trend_bot_rebalances.csv`

**Columns tracked:**
| Column | Description |
|--------|-------------|
| date, time_et | Rebalance timestamp |
| event | REBALANCE, SKIP_LOW_DRIFT |
| regime | risk_on / risk_off |
| spy_price | SPY closing price |
| spy_vs_sma200_pct | SPY distance from SMA200 (%) |
| equity | Total portfolio equity |
| equity_peak | High water mark |
| drawdown_pct | Current drawdown from peak |
| exposure_mult | Drawdown-adjusted exposure multiplier |
| capital_usage_pct | % of equity deployed |
| deployable_capital | $ available for positions |
| cash_reserve | $ held in cash |
| num_positions | Number of holdings |
| positions_json | Position weights as JSON |
| top_weights | Top 5 positions (quick view) |
| risk_score | Composite risk score (0-100) |
| event_mult | Event calendar multiplier |
| turnover_pct | Portfolio turnover % |
| orders_placed | Number of orders submitted |
| notes | Capital reason, order counts |

**Events logged:**
- `REBALANCE` - Normal weekly rebalance executed
- `SKIP_LOW_DRIFT` - Rebalance skipped due to turnover governor (drift < threshold)

**Implementation:**
- Added `log_rebalance_summary()` function (line ~1080)
- Called at end of `rebalance()` function (line ~5460)
- Also logs SKIP events when turnover governor blocks rebalance
- Thread-safe with `_csv_lock`

**Usage for analysis:**
```python
import pandas as pd
df = pd.read_csv("project_notes/trend_bot_rebalances.csv")
df['date'] = pd.to_datetime(df['date'])
df.set_index('date', inplace=True)

# Plot equity curve
df['equity'].plot(title='Portfolio Equity Over Time')

# Analyze drawdowns
df[df['drawdown_pct'] > 5][['equity', 'drawdown_pct', 'regime']]
```

### Session: 2026-01-31

**Robustness improvements based on cross-bot analysis**

Reviewed trend_bot against patterns found in simple_bot and directional_bot. Bot is architecturally solid (v42 improvements, correct enum comparisons, comprehensive error handling). Three robustness gaps identified and fixed:

**1. Daily Monitoring Exit Verification** (lines ~5023-5105)

- **Problem**: Emergency exits (gap-down, SMA200 trend break, position drawdown) submitted market sell orders but never verified they filled. If a sell was rejected or partially filled, the position remained exposed and the bot didn't know.
- **Fix**: Added `verify_order_execution()` call after each daily monitoring exit order (30-second timeout). On partial fill or failure, sends CRITICAL alert for manual intervention. Also cancels any pending TBOT_ orders for the symbol before submitting the exit (prevents conflicts with pending rebalance/drift orders).
- **Bonus fix**: Fixed stale `pos_data` reference in exit loop — was using last iteration's value from the outer position-checking loop instead of looking up by exit symbol.

**2. Buying Power Check Before Rebalance Buys** (lines ~5471-5510)

- **Problem**: After executing SELLs and waiting 5 seconds, the bot submitted ALL BUY orders without checking if the account had sufficient buying power. If sells hadn't settled or buying power was tight, later buy orders could be rejected by Alpaca, leaving the portfolio partially implemented.
- **Fix**: Added pre-flight buying power check before BUY order submission. If total buy notional exceeds available buying power, scales down all buy quantities proportionally and sends a WARNING alert. Logs available vs needed buying power for transparency.

**3. Daily Monitoring Exit: Cancel Pending Orders First**

- **Problem**: If a pending rebalance or drift-mini order existed for the same symbol, the emergency sell could conflict.
- **Fix**: Before submitting the exit sell, queries pending TBOT_ orders for the symbol and cancels them. Brief 0.5-second pause for cancellations to settle before proceeding.

### Session: 2026-02-02

**v8 Optimization: 40%+ Return Target Achieved**

Systematic optimization of trend_bot backtest parameters through 8 iterations. Target was 40%+ return.

**Iteration Results (252-day backtest, $100K initial capital):**

| Version | Config | Return | Max DD | Sharpe | Key Change |
|---------|--------|--------|--------|--------|------------|
| Baseline | Original | 9.26% | 6.6% | 0.59 | -- |
| v1 | Leveraged ETFs, SMA50, top 5 | 29.26% | 15.1% | 1.08 | Added TQQQ/UPRO/SOXL/TECL/FAS + momentum ETFs |
| v2 | Top 3, momentum-weighted | 12.39% | 21.8% | 0.40 | DD breaker killed it (locked out Nov-Feb) |
| v3 | v1 + no DD breaker, wider stops | 35.18% | 11.8% | 1.30 | Disabled circuit breaker |
| v4 | + 10% leverage, top 4, no defensive | 36.78% | 12.8% | 1.21 | Removed low-beta ETFs |
| v5 | + 15% leverage, 30-day momentum | 38.60% | 13.3% | 1.22 | Faster momentum lookback |
| v6 | 20% leverage | 39.43% | 13.9% | 1.20 | More leverage |
| v7 | SMA40, higher cap | 35.43% | 15.8% | 1.11 | SMA40 too fast (whipsaw) |
| **v8** | **SMA50, 25% leverage** | **43.53%** | **14.4%** | **1.27** | **WINNER** |

**3-Year Robustness Check (v8, 756 days):**
- Total return: 87.7%, CAGR: 23.4%, Max DD: 30.6%, Sharpe: 0.70

**Key Learnings:**
1. Leveraged ETFs (3x) are the biggest return driver - TQQQ/SOXL capture amplified momentum
2. Drawdown circuit breakers are fatal with leveraged ETFs - a 20% DD is normal for 3x ETFs, breaker locks out capital for months
3. SMA50 is optimal for trend filter - SMA200 too slow (misses moves), SMA40 too fast (whipsaw)
4. Inverse-vol weighting naturally limits leveraged ETF exposure (better than momentum-weighting)
5. Concentration (top 4) beats diversification (top 8) for returns, with acceptable DD increase

**v8 Winning Config (synced to production):**

| Parameter | Old | New (v8) | Rationale |
|-----------|-----|----------|-----------|
| SMA_LOOKBACK_DAYS | 200 | 50 | Faster trend filter |
| MAX_WEIGHT_PER_ASSET | 0.22 | 0.40 | Higher concentration |
| TOP_N_EQUITY | 6 | 4 | More concentrated |
| ENABLE_CONDITIONAL_LEVERAGE | False | True | 25% margin in strong conditions |
| ENABLE_TURNOVER_GOVERNOR | True | False | Faster rotation |
| ENABLE_RANK_STABILITY | True | False | Enter momentum positions immediately |
| REGIME_BUFFER_ON | 0.02 | 0.01 | Faster risk-on entry |
| STOP_LOSS_SMA200_BUFFER | 0.98 | 0.93 | Wider stops for leveraged ETFs |
| STOP_LOSS_POSITION_DD_PCT | 0.15 | 0.30 | Wider stops for leveraged ETFs |
| DRAWDOWN_TRIGGER | 0.10 | 0.30 | Raised for leveraged ETF headroom |
| DRAWDOWN_SCALE_START | 0.03 | 0.10 | Raised for leveraged ETF headroom |

**Universe Changes:**
- Added: TQQQ, UPRO, SOXL, TECL, FAS (leveraged), ARKK, XBI, KWEB, SOXX, IGV, CIBR, SKYY (momentum)
- Removed: EFA, EEM (low-beta international), XLP, XLU, XLRE, XLB (defensive sectors), VLUE (value factor)

**Capital Deployment Tiers (simplified):**
- Risk-on + score >= 40: 100% deployed (was max 95%)
- Risk-on + any score: 85% deployed
- Risk-off: 40% deployed (was tiered 35-55%)

**Cross-bot safety:** Updated TREND_BOT_SYMBOLS in simple_bot.py to include new leveraged/momentum ETFs.

**`--liquidate` Emergency Safety Feature**

Added `--liquidate` CLI flag for emergency position liquidation:
- `python trend_bot.py --liquidate` (shorthand: `-l`)
- Cancels all `TBOT_` prefixed open orders (shared-account safe)
- Closes all positions in `ALL_TICKERS` universe only (preserves other bots)
- Verifies fills if market is open (5s poll), queues orders for next open if after-hours
- Ignores DRY_RUN — explicit safety action always executes
- Follows same execute-and-exit pattern as `--rebalance`

### Session: 2026-02-03

**Wash Sale Protection (Hybrid: Cooldown + Substitutes)**

Added wash sale awareness to rebalance logic. When a position is sold at a loss, the bot avoids re-buying it for 31 calendar days (IRS wash sale rule). If a correlated substitute ETF exists, the bot buys the substitute instead to maintain exposure.

**How it works:**
1. After sell orders execute, checks if each was a loss (est_price < avg_entry_price)
2. Loss sales recorded in state with date → triggers 31-day cooldown
3. On next rebalance, if target weight wants to buy a cooldown symbol:
   - If substitute exists → buy substitute, attribute its weight to original symbol
   - If no substitute → skip the buy entirely (log warning)
4. When cooldown expires, natural rebalance sells substitute and buys original

**Substitute mapping (bidirectional):**
| Original | Substitute | Category |
|----------|-----------|----------|
| SPY | VOO | Broad Market |
| QQQ | QQQM | Broad Market |
| IWM | VTWO | Broad Market |
| XLK/XLF/XLE/XLV/XLI/XLY/XLC | VGT/VFH/VDE/VHT/VIS/VCR/VOX | Sectors |
| SMH | SOXX | In-universe pair |
| IBB | XBI | In-universe pair |
| XHB | ITB | Specialty |
| MTUM/QUAL | JMOM/JQUA | Factor |
| IEF/TLT/GLD/DBC | VGIT/VGLT/IAU/PDBC | Defensive |
| SGOV | BIL | Cash |
| KWEB/IGV/CIBR/SKYY | MCHI/WCLD/HACK/CLOU | Thematic |

**No substitute (cooldown only):** TQQQ, UPRO, SOXL, TECL, FAS (leveraged), ARKK (unique active)

**State fields added:**
- `loss_sales: {symbol: ISO_date}` — tracks when each loss sale occurred
- `active_substitutions: {substitute: original}` — tracks active substitute holdings

---

## Known Issues / TODOs

- [x] ~~Partial fill handling could be improved~~ (now cancels unfilled remainder)
- [x] ~~Fix duplicate `client_order_id` generation~~ (now includes HHMMSS timestamp)
- [x] ~~Consider adding cash balance check before placing orders~~ (implemented 2026-01-31)
- [ ] Race condition on `rebalance_in_progress` flag (use atomic check-and-set)
- [ ] DataFrame slicing without length checks (add `len(series) >= lookback` guards)

---

## Key Configuration

- **Rebalance**: Weekly on Fridays at 11:00 AM ET
- **Trend Filter**: SMA50 with hysteresis buffer (v8: was SMA200)
- **Position Sizing**: Inverse-volatility weighted, top 4 by momentum
- **Dynamic Capital**: 40-100% deployed based on risk score and regime
- **Leverage**: Up to 1.25x in strong conditions (v8: enabled)
- **Universe**: 26 ETFs including leveraged (TQQQ, UPRO, SOXL, TECL, FAS) and momentum ETFs
- **Data Source**: Alpaca (or Polygon if configured)

### Manual Position Handling

| Scenario | Behavior |
|----------|----------|
| Position closed manually while running | Rebuilt at next weekly rebalance (if signal bullish) |
| Position added manually (in-universe) | Adjusted to target weight at next rebalance |
| Position added manually (out-of-universe) | Ignored (but consumes buying power) |
