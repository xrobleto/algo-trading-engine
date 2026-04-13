# AI Investment Manager - Project Notes

> **Purpose**: This file maintains project context between Claude Code sessions.
> Update this file at the end of each session with progress, decisions, and next steps.

---

## Project Overview

A production-grade investment monitoring system that:
- Parses Robinhood CSV exports to reconstruct portfolio holdings
- Enriches data with Polygon, Alpaca, and FRED APIs
- Generates risk/opportunity scores using deterministic rules
- Sends email alerts only when thresholds are met
- Uses Claude API only for generating narrative text (not scoring)

---

## Current Architecture

```
src/
├── main.py                 # CLI entrypoint
├── ingestion/              # CSV parsing, holdings reconstruction
├── providers/              # API clients (Polygon, Alpaca, FRED, TradingView)
├── signals/                # Technical, news, macro, risk analysis
├── engine/                 # Scoring and recommendations
├── llm/                    # Claude API client
├── reporting/              # Email rendering, charts
├── scheduling/             # APScheduler runner
├── storage/                # SQLite state/deduplication
└── utils/                  # Logging, time, retry helpers
```

---

## Session Log

### Session: 2026-01-16

**What was worked on:**
- Fixed critical bug in holdings reconstruction where incomplete CSV history caused inflated share counts
- SOXL was showing 7,575 shares instead of correct 75 shares (100x inflation)
- Portfolio value was ~$1.8M instead of correct ~$1.03M
- Made `--once` flag always send email (force_email=True)
- Added multi-timeframe momentum alignment (1m/5m/15m) to smallcap_scanner A+ grading

**Decisions made:**
- Allow negative share counts during transaction processing to handle incomplete CSV history
- When CSV is missing historical BUY transactions, SELLs create negative balances that are correctly offset by later BUYs
- This produces accurate final share counts even with incomplete data

**Issues encountered:**
- Bug: `reconstruct_holdings()` was clipping SELL quantities to 0 when shares=0, which prevented shares from going negative
- This caused BUYs to accumulate without being offset by the SELLs that happened before CSV history began

**Fix applied:**
- Modified `src/ingestion/robinhood_csv.py` lines 479-509:
  - Removed the qty clipping that prevented negative shares
  - Added `state.shares -= qty` outside the `if state.shares > 0` block
  - Cost basis calculation only applies to shares actually held (uses `min(qty, state.shares)`)

**Next steps:**
- [ ] Consider suppressing warnings for known incomplete history (or make them INFO level)
- [ ] Test with full portfolio run and verify email report accuracy

---

## Known Issues / TODOs

- [x] SOXL (and other tickers) showing inflated share counts - FIXED 2026-01-16
- [ ] Many "SELL X shares but only Y held" warnings due to incomplete CSV history (cosmetic, not affecting accuracy)

---

## Key Configuration

- Portfolio CSV: `C:/Users/xrobl/Documents/Robinhood/activity.csv`
- State DB: `data/state.db`
- Alert thresholds: action_score_min=55, risk_score_min=55
- LLM model: claude-sonnet-4-20250514

---

## How to Use This File

1. **Starting a new session**: Tell Claude Code to read `PROJECT_NOTES.md` first
2. **During work**: Ask Claude to update the session log as you go
3. **Ending a session**: Ask Claude to summarize progress in this file

Example prompts:
- "Read PROJECT_NOTES.md and continue where we left off"
- "Update PROJECT_NOTES.md with what we accomplished today"
- "Add [issue] to the Known Issues section"
