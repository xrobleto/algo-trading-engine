# Small Cap Executor - Project Notes

> **Purpose**: Track progress and changes for the smallcap_executor.py script.
> **Location**: `C:\Users\xrobl\Documents\Algo_Trading\scanners\smallcap_executor.py`

---

## Overview

A trade execution script for small cap momentum plays. Takes a ticker symbol, validates the setup, calculates position size based on risk, and executes with automated exit management (tiered exits with trailing stop).

Key Features:
- Quick setup validation with execution quality gates (spread, liquidity, volume)
- Position sizing based on % risk of available cash
- Tiered exits: TP1 at 1.0R (33%), TP2 at 2.5R (33%), Trail remaining 34%
- Real-time position monitoring
- Automatic stop management (breakeven after TP1, trail after TP2)
- Trade journaling

---

## Session Log

### Session: 2026-01-21

**Issue observed:**
- BOXL order: 701 shares requested, 662 filled (partial fill)
- Executor showed "Order timed out - cancelling" but order was actually partially filled
- Position was not tracked - user had to manually sell via Alpaca UI

**Root cause analysis:**
The `execute_entry()` method (lines 975-1077) had two critical bugs:

1. **Only checked for `FILLED` status** - The code looked for `OrderStatus.FILLED` but NOT `OrderStatus.PARTIALLY_FILLED`. When an order is 94% filled (662/701), Alpaca reports it as `PARTIALLY_FILLED`, not `FILLED`.

2. **Cancelled on timeout without checking fill quantity** - When the 10-second timeout hit, the code immediately tried to cancel the order without first checking if any shares had been filled. The partial fill already existed but was ignored.

**Fix implemented:**

1. **Extended wait time** from 10 seconds to 15 seconds (small caps can be slow to fill)

2. **Added `PARTIALLY_FILLED` status handling** - During the wait loop, partial fills are now tracked and displayed: `[662/701]`

3. **Check fill quantity before cancelling** - On timeout, now queries the order one more time to check `filled_qty`. If shares exist:
   - Accept the partial fill and create a position
   - Cancel only the unfilled remainder
   - Log that it was a partial fill

4. **Created helper method `_create_position_from_fill()`** - Consolidates position creation logic for both full and partial fills, ensuring consistent handling and proper TP recalculation based on actual fill price.

```python
# On timeout, check for partial fills FIRST
order_status = self.trading_client.get_order_by_id(order_id)
filled_qty = int(order_status.filled_qty) if order_status.filled_qty else 0

if filled_qty > 0:
    # PARTIAL FILL EXISTS - we have shares!
    filled_price = float(order_status.filled_avg_price)
    # Cancel remaining unfilled portion
    self.trading_client.cancel_order_by_id(order_id)
    # Create position with partial fill
    pos = self._create_position_from_fill(symbol, filled_qty, filled_price, setup, order_id)
    return pos
```

**Additional improvement:**
- TP1/TP2 prices are now recalculated based on actual fill price (not limit price), which provides more accurate R-multiple tracking when fill price differs from the limit.

### Session: 2026-01-22

**Issue observed:**
- SXTP position showed `[EXIT PENDING]` with 1/219 shares remaining
- When exit order filled, executor said "Position closed externally - removing from tracking"
- But it was NOT closed externally - the executor's own exit order filled

**Root cause analysis:**
In `check_positions()` method (line ~1136), the code:
1. Syncs with Alpaca to get actual position qty
2. If qty=0, assumes "closed externally"
3. Only AFTER that check does it look at `exit_pending` flag

The problem: When our exit order fills, Alpaca shows qty=0. The code saw qty=0 before checking `exit_pending` and incorrectly concluded it was closed externally.

**Fix implemented:**
Changed the logic to check `exit_pending` flag WHEN qty=0:
- If `exit_pending=True` and qty=0 → Our exit filled (expected)
- If `exit_pending=False` and qty=0 → Actually closed externally

```python
if actual_qty <= 0:
    if pos.exit_pending:
        # Our exit order filled - this is expected behavior
        exit_reason = "OUR_EXIT_FILLED"
        print_colored(f"  [{symbol}] Exit order filled - position closed", "green")
    else:
        # No pending exit = closed externally
        exit_reason = "EXTERNAL"
        print_colored(f"  [{symbol}] Position closed externally", "cyan")
```

Also fixed P&L calculation to use `remaining_qty` instead of `total_qty` (since partial profits were already taken).

### Session: 2026-01-23

**Issue observed:**
- User noticed price data appearing delayed compared to Alpaca UI
- Concern that order decisions might be based on stale data

**Root cause analysis:**

1. **`display_positions()` used wrong price field** (line ~1690)
   - Was using `snapshot["price"]` which is `day.c` (day's close - can be stale during trading)
   - Should use `last_trade_price` like the monitoring logic does

2. **Alpaca data client was initialized but never used** (line ~526)
   - `StockHistoricalDataClient` was created but no methods called it
   - Alpaca provides real-time quotes that could serve as backup/comparison

3. **Quote data relied solely on Polygon**
   - Polygon snapshot `lastQuote` can lag a few hundred ms behind actual market
   - For order execution, Alpaca's real-time quote is often more current

**Fixes implemented:**

1. **Fixed `display_positions()` to use `last_trade_price`** (line ~1690)
   ```python
   # Before: current_price = snapshot["price"]
   # After:
   current_price = snapshot.get("last_trade_price") or snapshot.get("price", pos.entry_price)
   ```

2. **Added `get_alpaca_quote()` method to MarketDataFetcher** (lines ~297-320)
   - Fetches real-time bid/ask from Alpaca data API
   - Serves as backup when Polygon quote seems stale
   ```python
   def get_alpaca_quote(self, symbol: str) -> Optional[dict]:
       request = StockLatestQuoteRequest(symbol_or_symbols=symbol)
       quotes = self.alpaca_client.get_stock_latest_quote(request)
       # Returns bid, ask, bid_size, ask_size
   ```

3. **Updated `get_snapshot()` to cross-check with Alpaca** (lines ~343-410)
   - Still uses Polygon for OHLCV data (volume, open, high, low)
   - Now uses Alpaca quote for bid/ask (more accurate for order execution)
   - Falls back to Polygon quote if Alpaca unavailable
   ```python
   alpaca_quote = self.get_alpaca_quote(symbol)
   if alpaca_quote and alpaca_quote.get("bid", 0) > 0:
       bid, ask = alpaca_quote["bid"], alpaca_quote["ask"]
   else:
       bid, ask = polygon_bid, polygon_ask
   ```

4. **MarketDataFetcher now accepts Alpaca client** (line ~288)
   - Constructor: `def __init__(self, alpaca_data_client=None)`
   - TradeExecutor passes its data client to enable Alpaca quotes

**Data Source Summary After Fix:**

| Data Type | Primary Source | Backup Source | Latency |
|-----------|---------------|---------------|---------|
| Last Trade Price | Polygon `lastTrade.p` | - | ~20ms |
| Bid/Ask Quote | Alpaca `get_stock_latest_quote` | Polygon `lastQuote` | ~10-20ms |
| OHLCV | Polygon snapshot `day` | Polygon daily bars | ~20ms |
| 1-min Bars | Polygon aggregates | - | 1-2 sec |

---

## Known Issues / TODOs

- [ ] Consider adding WebSocket streaming for real-time fill updates (vs polling)
- [ ] May want to add option to reject partial fills below a threshold (e.g., <50% filled)
- [ ] Trade journal now tracks `partial_fill` and `requested_qty` fields for analysis

---

## Key Configuration

- `RISK_PCT_PER_TRADE`: 0.50% of cash per trade (1R)
- `MAX_POSITION_PCT`: 15% max of cash in single position
- `MAX_SPREAD_BPS`: 80 bps maximum spread
- `TP1_R_MULTIPLE`: 1.0R (sell 33%)
- `TP2_R_MULTIPLE`: 2.5R (sell 33%)
- `TRAIL_DISTANCE_PCT`: 2.0% trailing stop on remaining 34%
- Entry timeout: 15 seconds (was 10 seconds)

---

## Exit Strategy Reference

```
Entry Fill
    ↓
Price hits TP1 (1.0R) → Sell 33%, move stop to breakeven
    ↓
Price hits TP2 (2.5R) → Sell 33%, activate trailing stop (2% behind high)
    ↓
Trail stop hit OR EOD close → Sell remaining 34%
```
