# Small Cap Scanner - Project Notes

> **Purpose**: Track progress and changes for the smallcap_scanner.py script.
> **Location**: `C:\Users\xrobl\Documents\Algo_Trading\scanners\smallcap_scanner.py`

---

## Overview

A scanner that identifies small cap stocks with bullish flag/pennant setups during market hours. Grades setups from A+ to C based on gap percentage, relative volume, pole strength, and retracement depth.

---

## Session Log

### Session: 2026-01-16

**What was worked on:**
- Added 1-minute % change column (`chg_1m`) to track fresh buying pressure
- Implemented new A+ grading based on multi-timeframe momentum alignment (1m/5m/15m)
- Shifted all previous grades down (A+ → A, A → B, B → C)

**New A+ Criteria (Momentum Alignment):**
```python
GRADE_A_PLUS_MOMENTUM = {
    "min_chg_1m": 0.5,           # 1m change > +0.5% (fresh buying pressure)
    "min_chg_5m": 1.5,           # 5m change > +1.5% (short-term trend)
    "min_chg_15m": 2.5,          # 15m change > +2.5% (intermediate trend)
    "min_1m_contribution": 0.25, # 1m must be at least 25% of 5m (not stalling)
}
# Plus base criteria (slightly relaxed since momentum alignment is key):
GRADE_A_PLUS = {
    "min_gap_pct": 12.0,
    "min_rvol": 6.0,
    "min_pole_pct": 6.0,
    "max_retrace_pct": 30.0,
}
```

**Grade Hierarchy (after changes):**
- **A+**: Multi-timeframe momentum alignment + base criteria
- **A**: Previous A+ criteria (strong setup but missing momentum alignment)
- **B**: Previous A criteria
- **C**: Previous B criteria (marginal setups)

**Changes made:**
- Added `chg_1m` field to `SetupSignal` dataclass
- Updated `_grade_setup()` method signature and logic
- Added 1m column to display output
- Modified grade thresholds

### Session: 2026-01-22

**What was worked on:**
- Added execution quality factors to grading and confidence scoring
- Scanner now considers tradability, not just chart pattern quality

**Execution Quality Improvements:**

1. **Spread Gates for Grades** (prevents trading wide-spread stocks):
   ```python
   MAX_SPREAD_BPS_A_PLUS = 50   # Max 50bps (0.50%) for A+
   MAX_SPREAD_BPS_A = 80        # Max 80bps (0.80%) for A
   MAX_SPREAD_BPS_B = 120       # Max 120bps (1.20%) for B
   ```
   - A+ now requires spread ≤ 0.50%
   - A requires spread ≤ 0.80%
   - B requires spread ≤ 1.20%
   - Wider spread = downgrade to C (or lower confidence)

2. **Volume Acceleration for A+** (confirms breakout momentum):
   ```python
   MIN_VOLUME_ACCEL_A_PLUS = 1.3  # Volume must be 1.3x prior 5-bar avg
   ```
   - A+ now requires volume surging (1.3x+ vs prior 5 bars)
   - Ensures you're catching active breakouts, not stale patterns

3. **Float Rotation Confidence Boost** (squeeze potential):
   ```python
   FLOAT_ROTATION_BOOST_THRESHOLD = 0.5  # 50% turnover = boost
   FLOAT_ROTATION_MAX_BOOST = 1.0        # 100%+ turnover = max boost
   ```
   - 50% float rotation: +0.05 confidence
   - 100%+ float rotation: +0.15 confidence (major squeeze signal)

4. **Updated Confidence Score** now includes:
   - Float rotation boost (+0.05 to +0.15)
   - Volume acceleration boost (+0.05 to +0.10)
   - Tight spread bonus (+0.05 for ≤30bps)
   - Wide spread penalty (-0.05 for >120bps)

**Updated Grade Requirements:**

| Grade | Gap% | RVOL | Pole% | Retrace% | Momentum | Spread | Vol Accel |
|-------|------|------|-------|----------|----------|--------|-----------|
| A+ | 12%+ | 6x+ | 6%+ | ≤30% | **Yes** | ≤50bps | ≥1.3x |
| A | 15%+ | 8x+ | 8%+ | ≤25% | No | ≤80bps | - |
| B | 10%+ | 5x+ | 6%+ | ≤30% | No | ≤120bps | - |
| C | 7%+ | 3x+ | 5%+ | ≤40% | No | Any | - |

2. **Hot List Mode** - Monitor A+/A setups for optimal entry timing

   **Problem**: Scanner identifies good setups but entry timing is often off - stocks become better entries later after consolidating or pulling back.

   **Solution**: Hot List mode tracks qualifying A+/A setups and monitors them for specific entry triggers:

   ```python
   # Hot List Configuration
   HOT_LIST_ENABLED = True                 # Enable Hot List tracking
   HOT_LIST_RESCAN_SEC = 30                # Re-scan hot list every 30 seconds
   HOT_LIST_MAX_AGE_MINUTES = 60           # Remove stale entries after 60 minutes
   HOT_LIST_MIN_GRADE = "A"                # Minimum grade to track ("A+" or "A")
   HOT_LIST_PERSIST_FILE = "hot_list.json" # Persist across restarts
   ```

   **Entry Triggers Detected**:
   - **HOD_BREAK**: Price breaks above high of day (by ≥0.2%)
   - **KEY_LEVEL**: Price breaks above psychological level ($0.50/$1.00)
   - **MICRO_PB**: Micro pullback bounce (1-3% pullback, 0.5% bounce)
   - **SPREAD_TIGHT**: Spread tightens to ≤30bps (execution quality improving)
   - **VOLUME_SURGE**: Volume surges to 2x+ prior 5-bar average

   **Stage Tracking**:
   ```
   WATCHING → CONSOLIDATING → SETUP → TRIGGER → COOLING → REMOVED
   ```

   **Features**:
   - Auto-adds A+/A setups to hot list
   - Rescans hot list symbols every 30 seconds
   - Plays distinct sound alert when triggers fire
   - Persists to JSON file (survives restarts, resets daily)
   - Shows price change vs entry price when added
   - Tracks total trigger count per symbol

### Session: 2026-01-24

**Feature: ADX Filter for A+ Setups**

Added ADX (Average Directional Index) trend strength filter as a requirement for A+ grade. This filters out choppy, non-trending stocks that may have high volume but lack directional conviction.

**Source**: Logic inspired by `scanner_v17.py` from old scripts analysis which used ADX > 25 for quality filtering.

**Configuration** (lines 175-179):
```python
ADX_PERIOD = 14                 # Standard ADX calculation period
MIN_ADX_A_PLUS = 25.0           # Minimum ADX for A+ grade (strong trend)
```

**Implementation Details**:

1. **New `_calculate_adx()` Method** (lines 2309-2355):
   - Calculates ADX using Wilder's smoothing method
   - Uses True Range, +DM (positive directional movement), -DM (negative directional movement)
   - Computes +DI and -DI (directional indicators)
   - Derives DX and smooths to ADX
   - Returns 0.0 if insufficient data

   ```python
   def _calculate_adx(self, df: pd.DataFrame, period: int = ADX_PERIOD) -> float:
       """Calculate Average Directional Index (ADX) for trend strength."""
       # True Range calculation
       # +DM and -DM directional movement
       # Wilder's smoothing (EWM with alpha=1/period)
       # +DI and -DI from smoothed values
       # DX = 100 * abs(+DI - -DI) / (+DI + -DI)
       # ADX = smoothed DX
   ```

2. **Updated `_grade_setup()` Method** (lines 2357-2412):
   - Added `adx: float = 0.0` parameter
   - A+ now requires `adx >= MIN_ADX_A_PLUS` (>25) in addition to existing criteria
   - Updated docstring to document ADX requirement

   ```python
   # Execution quality for A+: tight spread + volume accelerating + strong trend
   execution_quality_a_plus = (
       spread_bps <= MAX_SPREAD_BPS_A_PLUS and
       volume_accel >= MIN_VOLUME_ACCEL_A_PLUS and
       adx >= MIN_ADX_A_PLUS  # ADX > 25 confirms strong directional trend
   )
   ```

3. **Updated Grading Call Site** (lines 2001-2018):
   - Calculates ADX from price dataframe before grading
   - Passes ADX value to `_grade_setup()`

**ADX Interpretation**:
| ADX Value | Interpretation |
|-----------|----------------|
| 0-20 | Weak/no trend (choppy market) |
| 20-25 | Emerging trend |
| 25-50 | Strong trend |
| 50-75 | Very strong trend |
| 75-100 | Extremely strong trend (rare) |

**Updated Grade Requirements:**

| Grade | Gap% | RVOL | Pole% | Retrace% | Momentum | Spread | Vol Accel | **ADX** |
|-------|------|------|-------|----------|----------|--------|-----------|---------|
| A+ | 12%+ | 6x+ | 6%+ | ≤30% | **Yes** | ≤50bps | ≥1.3x | **≥25** |
| A | 15%+ | 8x+ | 8%+ | ≤25% | No | ≤80bps | - | - |
| B | 10%+ | 5x+ | 6%+ | ≤30% | No | ≤120bps | - | - |
| C | 7%+ | 3x+ | 5%+ | ≤40% | No | Any | - | - |

**Benefits**:
- Filters out high-volume stocks that are chopping sideways
- Confirms directional conviction before giving A+ grade
- Improves win rate by only taking trades with strong trend backing
- ADX is direction-agnostic, measuring strength not direction

---

## Known Issues / TODOs

- [x] ~~Consider adding 1m change to the executor integration~~
- [x] ~~Add Hot List mode to monitor setups for optimal entry timing~~
- [x] ~~Add ADX filter for A+ setups~~ (implemented)
- [ ] May want to tune momentum thresholds based on market conditions
- [ ] Consider adding pre-market volume as a boost factor
- [ ] Hot List: Consider adding volume surge trigger detection
- [ ] Hot List: May want configurable trigger thresholds per symbol

---

## Key Configuration

- Runs during market hours (9:30 AM - 4:00 PM ET)
- Scans for gap ups with bullish consolidation patterns
- Integrates with smallcap_executor for automated trading

### Execution Quality Thresholds
```python
MAX_SPREAD_BPS_A_PLUS = 50      # Max spread for A+ grade
MAX_SPREAD_BPS_A = 80           # Max spread for A grade
MAX_SPREAD_BPS_B = 120          # Max spread for B grade
MIN_VOLUME_ACCEL_A_PLUS = 1.3   # Min volume acceleration for A+
FLOAT_ROTATION_BOOST_THRESHOLD = 0.5  # Float rotation for confidence boost
```

### Sound Alert Configuration
```python
SOUND_ALERT_ENABLED = True      # Play sound when new A+ setup appears
SOUND_ALERT_FOR_A = False       # Also play sound for A setups (not just A+)
```

### Hot List Configuration
```python
HOT_LIST_ENABLED = True                 # Enable Hot List tracking mode
HOT_LIST_RESCAN_SEC = 30                # Re-scan hot list symbols every 30 seconds
HOT_LIST_MAX_AGE_MINUTES = 60           # Remove from hot list after 60 minutes
HOT_LIST_MIN_GRADE = "A+"               # Minimum grade to add to hot list (changed from "A")

# Entry Trigger Thresholds
HOT_LIST_HOD_BREAK_BUFFER_PCT = 0.2     # Break HOD by at least 0.2%
HOT_LIST_KEY_LEVEL_BUFFER_PCT = 0.3     # Break key level by at least 0.3%
HOT_LIST_MICRO_PB_MIN_PCT = 1.0         # Minimum pullback %
HOT_LIST_MICRO_PB_MAX_PCT = 3.0         # Maximum pullback %
HOT_LIST_MICRO_PB_BOUNCE_PCT = 0.5      # Bounce at least 0.5% from low
HOT_LIST_SPREAD_TIGHT_BPS = 30          # Spread tightens to 30bps
HOT_LIST_VOLUME_SURGE_MULT = 2.0        # 2x volume = surge trigger
```
