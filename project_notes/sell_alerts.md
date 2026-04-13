# Sell Alerts - Project Notes

> **Purpose**: Track progress and changes for the sell_alerts.py script.
> **Location**: `C:\Users\xrobl\Documents\Algo_Trading\alerts\sell_alerts.py`

---

## Overview

A HYSA (High Yield Savings Account) funding sell alert engine that monitors portfolio positions and sends email alerts when sell conditions are met. Helps fund a HYSA target by identifying optimal sell opportunities.

Key Features:
- Separate thresholds for "strength" (sell into gains) vs "riskoff" (cut losses)
- Tax awareness: warns when positions are near long-term capital gains threshold
- AI conviction assessment via Claude API with comprehensive research
- HTML + plain-text multipart email with mobile-friendly design
- Per-ticker cooldown to prevent alert spam
- Robinhood activity CSV support for position tracking

---

## Session Log

### Session: 2026-01-23

**Major Enhancement: AI-Powered Quick Rundown Emails**

Redesigned the entire AI analysis and email system to match buy_alerts.py sophistication, plus sell-specific features.

**New AISellAssessment Fields:**
```python
@dataclass
class AISellAssessment:
    # Core assessment
    conviction: str              # HIGH, MEDIUM, LOW
    conviction_score: int        # 1-100 numeric score
    headline: str                # One-line hook for quick scanning
    urgency: str                 # "Sell Now", "Can Wait", "Watch Closely"
    urgency_reason: str          # Brief explanation of timing
    reasoning: str               # 2-3 sentence summary

    # Risk analysis
    risk_if_hold: str            # What could happen if you don't sell
    downside_scenario: str       # "If drops to support at $X, you lose $Y"
    downside_pct: float          # Estimated downside percentage

    # Action recommendation
    position_action: str         # "Full exit", "Trim 50%", "Small trim 25%"
    suggested_exit_price: float  # Suggested limit price

    # Opportunity cost
    opportunity_cost: str        # "This capital could earn X in HYSA"

    # Tax considerations
    tax_note: str                # Tax-aware recommendation
    tax_impact: str              # "Short-term gains taxed at income rate"

    # Company/Market context
    company_context: str         # Brief company description
    recent_developments: str     # Key recent news summary
    sector_trend: str            # Sector performance
    analyst_sentiment: str       # General analyst sentiment

    # Timing factors
    upcoming_events: List[str]   # Earnings, ex-div dates, etc.
    hold_factors: List[str]      # Reasons you might want to hold
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
- Full technical data (RSI, EMAs, Bollinger, drawdown)
- Complete news content (not just headlines)
- Market context (SPY, VIX, regime)
- Tax status and holding period
- Opportunity cost calculation (HYSA APY)
- Downside risk to support levels
- HYSA funding context and shortfall

**Mobile-Friendly Email Redesign:**
1. **Visual Urgency Meter** - Color-coded progress bar (red=Sell Now, orange=Can Wait, blue=Watch)
2. **AI Headline** - Compelling one-liner at top of each card
3. **Risk vs Opportunity** - Side-by-side boxes comparing hold risk vs HYSA earnings
4. **Downside Bar** - Visual showing potential loss percentage
5. **Upcoming Events** - Pill badges for earnings, ex-div dates
6. **Hold Factors** - Reasons to potentially wait (yellow badges)
7. **Tax Impact** - Yellow banner for tax considerations
8. **HYSA Progress Header** - Visual progress bar showing funding goal

**Key Design Decisions:**
- Max width 500px for mobile readability
- System font stack (-apple-system, Segoe UI, etc.)
- Viewport meta tag for proper mobile scaling
- Flexbox with wrap for responsive layouts
- Gradient header with funding progress
- Color coding: red=urgent/risk, green=opportunity/profit, orange=caution, blue=info

**Sell-Specific Features (vs buy_alerts.py):**
- Urgency levels instead of conviction levels as primary metric
- Downside scenario with quantified loss potential
- Opportunity cost showing HYSA alternative earnings
- Hold factors (reasons NOT to sell) prominently displayed
- HYSA funding progress in header
- Tax impact warnings (STCG vs LTCG, days until LTCG)

---

## Known Issues / TODOs

- [ ] Consider adding dividend ex-date awareness (don't sell right before ex-div)
- [ ] May want to add position concentration warnings
- [ ] Could show historical P&L for position

---

## Key Configuration

- Uses Polygon API for market data
- Uses Claude API for AI analysis (optional)
- Per-ticker cooldown prevents alert spam
- Risk levels 1-3 control alert sensitivity
- Tax lot awareness from Robinhood activity exports

### AI Analysis Parameters
```python
hysa_rate = 0.045  # Current HYSA APY for opportunity cost calculation
```

---

## Email Subject Logic

```
Urgent count > 0    → "[URGENT] X Sell Now | Need $Y"
Risk level 1        → "[Sell Alert] X Opportunities | Need $Y"
Risk level 2        → "[Sell Alert] X Windows | Need $Y"
Risk level 3        → "[Sell Ideas] X Potential | Need $Y"
```
