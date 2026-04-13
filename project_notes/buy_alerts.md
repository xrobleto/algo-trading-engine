# Buy Alerts - Project Notes

> **Purpose**: Track progress and changes for the buy_alerts.py script.
> **Location**: `C:\Users\xrobl\Documents\Algo_Trading\alerts\buy_alerts.py`

---

## Overview

A portfolio buy alert engine that monitors watchlist stocks for technical buy signals. Sends email alerts when oversold, support test, or value conditions are detected. Uses hash-based deduplication to avoid duplicate alerts.

Key Features:
- Technical analysis: RSI oversold, Bollinger bands, EMA support tests
- Trend context: EMA alignment, 200 EMA slope, falling knife detection
- Volume confirmation: elevated volume on pullback = accumulation signal
- News sentiment via Polygon API
- AI conviction assessment via Claude API (optional)
- Hash-based deduplication with time cooldown

---

## Session Log

### Session: 2026-01-23

**Major Enhancement: AI-Powered Quick Rundown Emails**

Redesigned the entire email system to provide comprehensive AI analysis with mobile-friendly "quick rundown" format.

**New AIBuyAssessment Fields:**
```python
@dataclass
class AIBuyAssessment:
    conviction: str              # HIGH, MEDIUM, LOW
    conviction_score: int        # 1-100 numeric score
    headline: str                # One-line hook for quick scanning
    reasoning: str               # 2-3 sentence summary
    bull_case: str               # Why this could work
    bear_case: str               # What could go wrong
    risk_factors: List[str]      # Key risks as bullet points
    catalysts: List[str]         # Upcoming catalysts/events
    position_sizing: str         # "Full size", "Half size", "Small starter"
    entry_strategy: str          # "Enter now", "Wait", "Scale in"
    company_context: str         # Brief company description
    recent_developments: str     # Key recent news summary
    analyst_sentiment: str       # General analyst sentiment
    earnings_context: str        # Upcoming/recent earnings
    sector_trend: str            # Sector performance context
```

**New MarketContext (gathered for each analysis):**
```python
@dataclass
class MarketContext:
    spy_price: float
    spy_change_pct: float
    spy_trend: str               # "bullish", "bearish", "neutral"
    vix_level: float
    vix_context: str             # "low", "normal", "elevated", "high"
    market_regime: str           # "risk-on", "risk-off", "mixed"
```

**Enhanced AI Prompt:**
- Sends full technical data (RSI, EMAs, ATR, volume, trend alignment)
- Includes complete news content (not just headlines)
- Provides market context (SPY, VIX, regime)
- Asks for structured response with all new fields

**Mobile-Friendly Email Redesign:**
1. **Visual Conviction Meter** - Color-coded progress bar (green/orange/red)
2. **AI Headline** - Compelling one-liner at top of each card
3. **Bull/Bear Cases** - Side-by-side boxes (stack on mobile)
4. **Catalysts & Risks** - Pill badges for quick scanning
5. **Risk/Reward Bar** - Visual bar showing stop vs target percentages
6. **Entry Strategy Badges** - Color-coded "Enter Now" / "Wait" / "Scale In"
7. **Collapsible Technical Details** - Keeps main view clean

**Key Design Decisions:**
- Max width 500px for mobile readability
- System font stack (-apple-system, Segoe UI, etc.)
- Viewport meta tag for proper mobile scaling
- Flexbox with wrap for responsive layouts
- Color coding throughout for at-a-glance assessment

### Session: 2026-01-23 (Enhanced Email Content - Part 1)

**User request:**
Email felt too sparse - wanted "a little more" content like the previous version but improved.

**Enhancements implemented:**

1. **52-Week Context Section** - Shows distance from 52-week high/low with color-coded status (Near Low/Mid-Range/Near High)

2. **EMA Levels Section** - Displays EMA20, EMA50, EMA200 with current price distance percentage. EMA200 color-coded based on above/below.

3. **Bollinger Band Context** - Shows position within bands (Near Lower/Mid/Near Upper) with band range values

4. **ATR Context** - Shows expected daily move in dollars and percentage for volatility context

5. **All Take Profit Targets** - Expanded from just TP1 to show all three TP levels (TP1, TP2, TP3) with percentages

6. **Recent News Section** - Shows up to 3 recent news headlines with sentiment indicator (Positive/Neutral/Negative)

7. **Expanded Technical Reasons** - When no AI assessment, reasons now shown expanded by default (not collapsed) with "Why This Signal" header

### Session: 2026-01-23 (User-Friendly Improvements - Part 2)

**User request:**
Make email more user-friendly for people who don't know technical analysis.

**Enhancements implemented:**

1. **Renamed "Quick Rundown" to "Buy Alerts"** - Clearer, more descriptive title

2. **Added Company Name** - Shows full company name below ticker symbol (fetched from Polygon API)
   - Added `get_ticker_details()` method to PolygonClient
   - Added `company_name` field to TickerAnalysis dataclass

3. **Score Format Changed** - Now shows "60/100" instead of just "60"
   - Color-coded: Green (70+), Blue (50-69), Orange (<50)

4. **Risk/Reward Bar Explained** - Added legend below the bar:
   - "◀ Risk to Stop | Reward to Target ▶"
   - Red = downside risk %, Green = upside reward %

5. **News Articles with Summaries** - Each article now shows:
   - Full headline
   - Description/summary (up to 150 chars)
   - Sentiment emoji (📈 Positive, 📉 Negative, 📰 Neutral)

6. **Technical Levels with Explanations** - Each section now explains what it means:
   - 52-Week Range: "Stock is near its lowest price in the past year - could be oversold or in trouble"
   - Moving Averages: "Price above 200-day average = long-term uptrend"
   - Bollinger Bands: "Price near lower band often signals oversold - potential bounce opportunity"
   - ATR: "High/Moderate/Low volatility - this is how much the stock typically moves in a day"
   - Take Profit: "TP1 = Conservative · TP2 = Moderate · TP3 = Aggressive"

7. **Quick Reference Guide** - Collapsible glossary at bottom of email explaining:
   - Signal types (OVERSOLD, SUPPORT TEST, VALUE)
   - Key terms (RSI, EMA, Stop Loss, Take Profit, Risk:Reward)
   - Score meaning (70+ = Strong, 50-69 = Moderate, <50 = Weak)

**Card Order (HTML):**
```
Header (Symbol + Company Name, Signal Badge, Trend Badge)
AI Conviction Meter (if AI)
AI Headline (if AI)
Company Context (if AI)
Bull/Bear Cases (if AI)
Catalysts (if AI)
Risks (if AI)
Entry Strategy (if AI)
Price Section (Entry Price, Score/100, Risk/Reward Bar with legend)
Position Sizing
Key Metrics (RSI, Vol, EMA Aligned)
52-Week Context (with explanation)
EMA Levels (with explanation)
Bollinger Bands (with explanation)
ATR Context (with explanation)
Take Profit Targets (with explanation)
Recent News (with summaries)
Technical Reasons
---
Quick Reference Guide (collapsible)
Footer
```

**Plain Text Version:**
- Also updated with company name, score/100 format

---

### Session: 2026-01-22

**Issue observed:**
- Getting repeated buy alerts for AAPL almost back-to-back
- User thought there was a no-repeat period, but alerts kept coming

**Root cause analysis:**
The script uses hash-based deduplication (`compute_ticker_hash()`) to detect "material changes" in signal conditions. However, the hash buckets were too narrow:

1. **Score buckets**: 10-point (50-59, 60-69, etc.)
   - Problem: Score bouncing 59 → 61 triggers new alert
2. **RSI buckets**: 5-point (25, 30, 35, etc.)
   - Problem: RSI 29.9 → 30.1 triggers new alert
3. **No time cooldown**: Even with hash change, could alert within minutes

**Fixes implemented:**

1. **Widened hash buckets** to reduce sensitivity:
   - Score: 10-point → **20-point buckets** (40-59, 60-79, 80-100)
   - RSI: 5-point → **10-point buckets** (20-29, 30-39, etc.)
   - Near-EMA/Bollinger: 2-3% buffer → **5% buffer**

2. **Added time-based cooldown** (default 8 hours):
   - Even if hash changes, won't re-alert within cooldown period
   - Configurable via `alert_cooldown_hours` in config
   - Prevents rapid-fire alerts from indicator noise

```python
def should_alert(state, symbol, ticker_hash, min_cooldown_hours=8):
    # 1. If never alerted before -> alert
    # 2. If hash unchanged -> never alert again
    # 3. If hash changed BUT within cooldown -> wait
    # 4. If hash changed AND cooldown expired -> alert
```

**Configuration:**
Add to config.yaml:
```yaml
buy:
  alert_cooldown_hours: 8  # Minimum hours between alerts for same ticker
```

---

## Known Issues / TODOs

- [ ] Consider making cooldown configurable per-ticker (high-conviction tickers could have shorter cooldown)
- [ ] May want to add "force alert" flag for manual override
- [ ] Could add logging to show why alert was skipped (hash match vs cooldown)

---

## Key Configuration

- `min_score_to_alert`: 50 (minimum score to generate alert)
- `alert_cooldown_hours`: 8 (minimum hours between alerts for same ticker)
- `rsi_oversold`: 30 (RSI threshold for oversold signal)
- `rsi_very_oversold`: 25 (RSI threshold for very oversold)
- `counter_trend_penalty`: 15 (score penalty for below 200 EMA)
- `falling_knife_max_score`: 49 (hard cap for falling knife setups)

---

## Deduplication Logic

```
New ticker → Alert immediately

Same ticker, hash unchanged → Never re-alert
  (Conditions haven't materially changed)

Same ticker, hash changed, within cooldown → Wait
  (Conditions changed but too soon, may be noise)

Same ticker, hash changed, cooldown expired → Alert
  (Genuine new signal)
```

Hash includes:
- Score bucket (20-point)
- RSI bucket (10-point)
- Signal type (oversold, support_test, value)
- Boolean flags: at_lower_boll, near_200ema, above_200ema, ema_bullish/bearish, volume_confirmed
- 52-week zone (near_low, middle, near_high)
