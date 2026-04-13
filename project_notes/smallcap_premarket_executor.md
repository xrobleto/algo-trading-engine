# Small Cap Pre-Market Executor - Project Notes

> **Purpose**: Track progress and changes for the smallcap_premarket_executor.py script.
> **Location**: `C:\Users\xrobl\Documents\Algo_Trading\scanners\smallcap_premarket_executor.py`
> **Launcher**: `C:\Users\xrobl\Documents\Algo_Trading\launchers\Premarket Execute.bat`

---

## Overview

A specialized trade execution script for **PRE-MARKET HOURS** (4:00 AM - 9:30 AM ET). Pre-market trading has fundamentally different characteristics than regular hours, requiring adapted parameters, order handling, and risk management.

Key Features:
- Pre-market specific execution quality gates (wider spread tolerance, lower volume thresholds)
- Conservative position sizing (35% reduced from regular hours)
- Limit orders ONLY with `extended_hours=True` (no market orders)
- Scale-out exit strategy (50% at 0.75R, 50% at 1.5R)
- Gap trend detection (EXPANDING, HOLDING, FADING)
- Auto-close positions before market open (9:25 AM)
- Separate state/journal files from regular session

---

## Pre-Market vs Regular Hours: Key Differences

| Parameter | Regular Hours | Pre-Market | Reason |
|-----------|--------------|------------|--------|
| **Risk per trade** | 0.50% | 0.35% | Lower liquidity = harder exits |
| **Max spread** | 80 bps | 150 bps | Spreads 2-5x wider pre-market |
| **Max positions** | 3 | 2 | Concentration risk |
| **Min volume** | $100K | $25K | Volume is 5-10x lower |
| **Daily loss limit** | 1.5% | 1.0% | More conservative |
| **Order types** | IOC/Market OK | Limit ONLY | Market orders not allowed pre-market |
| **Extended hours flag** | Optional | ALWAYS True | Required for PM execution |
| **TP1 target** | 1.0R | 0.75R | Take profits faster (thin liquidity) |
| **TP2 target** | 2.5R | 1.5R | More conservative targets |

---

## Pre-Market Phases

| Phase | Time (ET) | Trading Allowed | Notes |
|-------|-----------|-----------------|-------|
| **EARLY** | 4:00-7:00 AM | ⚠️ Warning | Very thin liquidity - avoid unless necessary |
| **PRIME** | 7:00-8:00 AM | ✅ Yes | Volume starting to pick up |
| **BEST** | 8:00-9:25 AM | ✅ Yes | Best liquidity window |
| **CLOSE_ONLY** | 9:25-9:30 AM | ❌ Close only | Auto-closes all positions |

---

## Order Handling Rules

### Entry Orders
```python
order = LimitOrderRequest(
    symbol=symbol,
    qty=qty,
    side=OrderSide.BUY,
    type=OrderType.LIMIT,
    time_in_force=TimeInForce.DAY,
    limit_price=round(entry_price, 2),
    extended_hours=True  # CRITICAL - must be True for pre-market
)
```

### Exit Orders
```python
order = LimitOrderRequest(
    symbol=symbol,
    qty=exit_qty,
    side=OrderSide.SELL,
    type=OrderType.LIMIT,
    time_in_force=TimeInForce.DAY,
    limit_price=round(bid * 0.997, 2),  # 0.3% below bid for quick fills
    extended_hours=True  # CRITICAL
)
```

**Important**: IOC (Immediate or Cancel) orders don't work well pre-market due to thin liquidity. Use DAY orders instead.

---

## Exit Strategies

### 1. SCALE_OUT (Default)
- Take 50% at 0.75R (quick profit)
- Take remaining 50% at 1.5R
- Stop moves to breakeven after first scale-out

### 2. CLOSE_BEFORE_OPEN
- Exit all positions by 9:25 AM
- Avoids volatility spike at market open
- Best for uncertain setups

### 3. HOLD_THROUGH_OPEN
- Keep position into regular hours
- Higher risk - gap can expand OR collapse at open
- Only for highest conviction plays

---

## Gap Trend Detection

The executor monitors whether the gap is:

| Trend | Meaning | Action |
|-------|---------|--------|
| **EXPANDING** | Gap getting bigger | Good - momentum continuing |
| **HOLDING** | Gap stable | Neutral - watch closely |
| **FADING** | Gap closing | Warning - consider early exit |

Detection logic compares average price of first 5 bars vs last 5 bars in pre-market.

---

## Spread-Adjusted Sizing

Wide spreads reduce position size to account for execution slippage:

```python
if spread_bps <= 80:      # Ideal spread
    spread_penalty = 1.0  # Full size
elif spread_bps <= 150:   # Acceptable spread
    spread_penalty = 0.5  # 50% size
else:                     # Too wide
    spread_penalty = 0    # Reject trade
```

---

## Files & Persistence

| File | Location | Purpose |
|------|----------|---------|
| **State** | `data/premarket_executor_state.json` | Daily stats, size factor |
| **Journal** | `data/premarket_executor_trades.jsonl` | Trade log (separate from RTH) |
| **Launcher** | `launchers/Premarket Execute.bat` | Easy startup |

---

## Usage

```bash
# Interactive mode
python smallcap_premarket_executor.py

# Direct ticker entry
python smallcap_premarket_executor.py GLUE

# Monitor existing positions
python smallcap_premarket_executor.py --monitor
```

### Keyboard Controls (during monitoring)
- `[Q]` - Quit monitor
- `[X]` - Close all positions
- `[S]` - Show status report

---

## Session Log

### Session: 2026-01-28

**Created**: Initial implementation of pre-market executor based on analysis of RTH executor.

**Key design decisions:**

1. **Conservative by default** - Pre-market is inherently riskier due to liquidity constraints. All parameters are more conservative than the regular hours executor.

2. **No market orders** - Most brokers don't allow market orders pre-market. Even if they did, slippage would be severe. All orders are limit with `extended_hours=True`.

3. **Faster profit-taking** - TP1 at 0.75R instead of 1.0R because exits are harder in thin liquidity. Better to lock in profits quickly.

4. **Gap fade detection** - Pre-market gaps can fade quickly when regular traders come online. The executor warns when the gap is fading so you can exit early.

5. **Auto-close before open** - The 9:25-9:30 AM window is "CLOSE_ONLY" phase. Avoids the chaotic first minutes of market open.

6. **Separate state files** - Pre-market trades don't mix with regular session stats. This keeps P&L tracking clean and allows different risk parameters.

---

## Known Issues / TODOs

- [ ] Add support for pre-market specific news catalyst validation
- [ ] Consider adding "hold through open" position tracking (hand-off to regular executor)
- [ ] May want to add earnings calendar check (avoid holding through earnings release)
- [ ] WebSocket streaming for faster gap trend updates

---

## Key Configuration Reference

```python
# Risk Management
RISK_PCT_PER_TRADE = 0.35        # 35% less than RTH
MAX_DAILY_LOSS_PCT = 1.00        # Tighter than RTH (1.5%)
MAX_POSITIONS = 2                 # Lower than RTH (3)
MAX_POSITION_PCT = 10.0          # Lower than RTH (15%)

# Execution Quality Gates
MAX_SPREAD_BPS = 150             # Wider than RTH (80 bps)
MIN_BID_SIZE = 50                # Lower than RTH (100)
MIN_ASK_SIZE = 50                # Lower than RTH (100)
MIN_DOLLAR_VOLUME = 25_000       # Lower than RTH ($100K)

# Pre-market Volume Thresholds
PM_VOLUME_MINIMUM = 5_000        # Need at least 5K shares
PM_VOLUME_IDEAL = 50_000         # 50K+ for good liquidity

# Exit Targets
SCALE_OUT_1_R = 0.75             # First exit (faster than RTH 1.0R)
SCALE_OUT_2_R = 1.5              # Second exit (conservative)
SCALE_OUT_1_PCT = 0.50           # Take 50% at first target

# Timing
CLOSE_POSITIONS_BY = 9:25 AM     # Close before this time
MIN_TIME_TO_OPEN_MINUTES = 10    # Don't enter within 10 min of open
```

---

## Pre-Market Trading Best Practices

1. **Focus on A+ setups only** - Pre-market should be reserved for highest conviction plays

2. **Wait for volume** - Early pre-market (4-7 AM) is extremely thin. Best liquidity is 8:00-9:25 AM

3. **Always have a catalyst** - Pre-market moves are news-driven. No catalyst = no trade

4. **Use midpoint pricing** - Entry at mid + 0.2%, exit at mid - 0.3%

5. **Have exit plan before entry** - Decide upfront: close before open or hold through

6. **Watch for gap fade** - If gap starts closing, exit immediately regardless of targets

7. **Size conservatively** - Better to miss some upside than get stuck in an illiquid position
