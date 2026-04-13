# Dashboard v4.0 Changelog - Professional Trader Edition

## Summary
Major upgrade integrating advanced features from the CLI scanner while maintaining
a clean, single-screen professional interface.

## How to Revert
```bash
cp app_v3.4_backup.py app.py
```
Or simply rename `app_v3.4_backup.py` back to `app.py`.

---

## New Features Added

### 1. ADX (Average Directional Index) Trend Strength
- **What**: Measures trend strength (not direction)
- **Purpose**: Filters out choppy/consolidating stocks lacking directional conviction
- **Display**: Shows as "ADX" column with visual indicator
- **Thresholds**:
  - ADX > 25: Strong trend (required for A+ grade)
  - ADX 20-25: Emerging trend
  - ADX < 20: Weak/choppy

### 2. Multi-Timeframe Momentum Alignment
- **What**: Analyzes price momentum across 1m, 5m, and 15m timeframes
- **Purpose**: Confirms all timeframes are aligned for higher probability setups
- **Display**: Shows as "MTF" column with arrow indicators
- **A+ Requirements**:
  - 1m change > +0.5%
  - 5m change > +1.5%
  - 15m change > +2.5%
  - 1m must be at least 25% of 5m (not stalling)

### 3. Spread Quality Gates
- **What**: Bid-ask spread in basis points (bps)
- **Purpose**: Ensures execution quality - wide spreads = poor fills
- **Display**: Shows as "Spread" column
- **Grade Requirements**:
  - A+ requires ≤50bps (≤0.50%)
  - A requires ≤80bps (≤0.80%)
  - B requires ≤120bps (≤1.20%)

### 4. Volume Acceleration
- **What**: Current volume vs recent average (last 5 bars)
- **Purpose**: Detects surging volume into breakout (institutional buying)
- **Display**: Shows as "VAcc" (Volume Acceleration) column
- **A+ Requirement**: ≥1.3x (30% above recent average)

### 5. Float Rotation
- **What**: Volume as percentage of float shares traded
- **Purpose**: High rotation = squeeze potential (same shares changing hands multiple times)
- **Display**: Shows as "Rot" column with percentage
- **Signal Levels**:
  - 50%+ = High squeeze potential
  - 100%+ = Maximum boost (entire float traded)

### 6. Enhanced Grading System
The new grading system now considers ALL factors:

**A+ Grade Requirements** (all must be met):
- Gap ≥12%
- RVOL ≥6x
- ADX ≥25 (strong trend)
- Spread ≤50bps
- Volume Acceleration ≥1.3x
- Multi-timeframe momentum aligned

**A Grade Requirements**:
- Gap ≥15%
- RVOL ≥8x
- Spread ≤80bps

**B Grade Requirements**:
- Gap ≥10%
- RVOL ≥5x
- Spread ≤120bps

**C Grade**: Marginal setups or poor execution quality

### 7. Confidence Score
- **What**: 0-100% probability score combining all factors
- **Purpose**: Quick assessment of overall setup quality
- **Display**: Shows as "Conf" column with percentage

### 8. Compact Professional UI
- Single-screen design for experienced traders
- Color-coded metrics (green=good, yellow=caution, red=warning)
- Tooltips explaining each metric
- Sortable by any column
- Keyboard shortcuts reference in sidebar

---

## Columns Reference (for experienced traders)

| Column | Meaning | Good | Caution | Warning |
|--------|---------|------|---------|---------|
| Status | Breakout state | BREAKING/HOD | NEAR KEY | EXTENDED |
| Grade | Overall quality | A+/A | B | C |
| MTF | Multi-TF momentum | All aligned | Mixed | Diverging |
| ADX | Trend strength | >25 | 20-25 | <20 |
| Spread | Bid-ask bps | <50 | 50-100 | >100 |
| VAcc | Volume accel | >1.3x | 1.0-1.3x | <1.0x |
| Rot | Float rotation | >50% | 25-50% | <25% |
| Conf | Confidence | >70% | 50-70% | <50% |

---

## Files Changed
- `app.py` - Main dashboard application (v3.4 → v4.0)

## Files Created
- `app_v3.4_backup.py` - Backup of previous version
- `CHANGELOG_v4.md` - This file

## Dependencies
No new dependencies required. Uses existing:
- streamlit
- pandas
- numpy (already imported via pandas)
- polygon API
