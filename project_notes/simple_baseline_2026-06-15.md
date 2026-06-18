# SIMPLE — Diagnosis & Quantified Baseline (2026-06-15)

The bar the redesign must beat. All numbers verified against code + live Alpaca data.

## Diagnosis (verified)
- **Identity contradiction** (`strategies/simple_bot.py:3499-3615`): scanner hunts high-RVOL movers; the
  8-feature scorer penalizes their extension — VWAP-dist>3%→30%, 5min-momentum>1.5%→30%, ADX>35→40%,
  EMA-sep>2%→40%. A +8–9% runner scores ~46.5 (below `MIN_SIGNAL_SCORE=50`); a tame near-VWAP name ~68.
  It scans momentum and buys mean-reversion. (RVOL itself is NOT penalized — correction to the brief.)
- **Capital starvation:** live account equity $7,778.82 → SIMPLE 0.10 ≈ $780 sleeve; on probation
  (`MAX_POSITIONS=1`, `POSITION_SIZE_PCT=0.02`). Positions can't move the account.
- **Fill mechanics:** passive limit + ~45s timeout (`simple_bot.py:442`). COIN was attempted 2026-06-15
  and never filled (TIMEOUT_NO_FILL) while slower names filled.
- **Tuning drift:** `MIN_RELATIVE_VOLUME` relaxed 1.5→1.0, `min_score` lowered — activity over edge.
- **Overnight drift:** EOD flatten not reliably containing positions (see live data below).
- **Leveraged ETFs in universe** via dynamic discovery (AMZU/NVDL/USD live).

## Baseline 1 — LIVE realized P&L (2026-04-20 → 2026-06-15, real money)
Pulled read-only from Alpaca (`railway run`), attributed by SIMPLE's symbol set (bracket exit legs carry
Alpaca-generated client_order_ids, not `ENG_SIMPLE_`):
- **Realized P&L: −$53.45** over 18 FIFO round-trips
- **Win rate 27.8% (5W/13L), profit factor 0.19, expectancy −$2.97/trade**
- **6/18 round-trips held >16h** (AMZU "scalp" held ~3 days) — overnight drift confirmed
- Symbols traded incl. leveraged ETFs AMZU/NVDL/USD; COIN attempted, no fill
→ No edge — a steady small bleed.

## Baseline 2 — Existing (UNFAITHFUL) backtest, same window
`backtest/simple_bot_backtest.py --start 2026-04-20 --end 2026-06-15` (0% slippage, no commissions, no
timeout, 4-feature scorer — flatters reality):
- Return −5.0%, net −$5,594, **PF 0.93, win rate 68.4%** but **avg loser $3,202 = 2.3× avg winner $1,379**,
  avg 0.07R, maxDD 17.7%. Trailing-stop exits 100% WR (small), stop-loss exits 0% WR (−$68k).
→ Even flattered, it loses; classic "pennies in front of a steamroller" mean-reversion signature.

## Benchmark — buy-and-hold, same window (40 trading days)
- **QQQ +15.03%** (ann. Sharpe 4.06, maxDD −7.0%)
- **SPY +6.51%** (ann. Sharpe 3.14, maxDD −4.5%)
→ SIMPLE *lost* money while passive exposure compounded double digits in a bull tape.

## The bar to beat
A redesign must, on a FAITHFUL harness, clear the pre-registered B1–B7 bar AND beat both
(a) this SIMPLE baseline (trivially: be positive) and (b) buy-hold QQQ/SPY over the same window
(hard). If it can't, retiring SIMPLE and redeploying its 0.10 allocation is the honest outcome.
