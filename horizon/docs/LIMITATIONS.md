# Horizon Engine — Known Limitations & Fidelity Gaps

Stated plainly. The brief's instruction was to ship a truthfully-validated
engine, not an optimistic one — so this document is deliberately unflattering.

## Result limitations

1. **The +7pp-over-QQQ goal is not met.** The validated engine delivers QQQ + 0
   to +4.4pp CAGR (full period 2008-2026), +6.3pp out-of-sample. Reaching +7pp
   would require leverage beyond the ~2.0x Reg-T limit and a drawdown the
   gating bar (E3) rejects. This is the honest ceiling of faithfully-validated,
   retail-tradeable strategies here. The engine's real value is risk-adjusted:
   higher Sharpe and a much lower drawdown than QQQ.

2. **Two of four candidate strategies failed.** REVERT (mean reversion, Sharpe
   0.06 — decayed edge) and DRIFT (overnight seasonality, −4% CAGR — costs
   exceed the edge) did not clear the bar and are excluded. The engine is two
   sleeves, PULSE + ROTATION — genuine diversification, but thinner than the
   four-source ideal.

3. **The engine is substantially correlated to the Nasdaq.** PULSE's
   correlation to QQQ is ~0.90; it is the dominant sleeve. Horizon is not
   market-neutral. In a prolonged, tech-specific decline it will lose money —
   less than QQQ, but it will lose.

## Data limitations

4. **Options, Reddit/social sentiment, and prediction markets are excluded**
   from the validated engine — none has backtestable history (DATA_PIPELINE.md).
   The brief's vision of options-flow / sentiment-driven strategies could not
   be *validated*, so it was not shipped as validated. These remain candidate
   live-only enrichments for future research.

5. **Validation window is 2008-2026** (~18 years, one post-GFC macro era). The
   2008 GFC is the worst in-sample stress. A worse or structurally different
   event is possible and is not in the sample.

## Backtest fidelity gaps

6. **DRIFT's overnight fills** are modeled conservatively (4 bps/leg); the true
   auction slippage is uncertain. DRIFT failed anyway, so this does not affect
   the engine.

7. **No intraday modeling.** All strategies are daily; fills are at the next
   day's open. Intraday gaps, halts, and limit-up/down are not modeled.

8. **Sleeve allocation in the portfolio** rebalances monthly with a small
   modeled cost; within-month weight drift is not separately tracked (a
   second-order effect).

## Live-engine limitations

9. **The live order layer is v1.** It now has email alerting (failures,
   kill-switch trips, daily heartbeat), an emergency `--flatten` command,
   orphaned-position recovery in the reconciler, and Railway deployment config
   (Dockerfile + railway.json). What remains v1: partial-fill handling is
   minimal — fine for liquid-ETF market orders but unproven under stress — and
   orders are tagged holistically rather than per-sleeve (per-sleeve accounting
   is approximate). Still: test on small stakes first.

10. **Withdrawals are not reliably monthly.** The high-water-mark mechanism
    pays out only on genuine new highs; on a volatile growth account those
    cluster in bull runs and pause for years in drawdowns (≈2 withdrawals/year
    on the validated run, not ~12). A fixed-percentage monthly withdrawal would
    pay every month but erodes principal in drawdowns — a different trade-off
    the user can choose.

11. **Stale Alpaca keys.** The paper keys found in the project config returned
    "unauthorized" during testing. The user must supply current Alpaca keys in
    `horizon/.env` before any live or paper run.

## Design decisions worth knowing

12. **The binary trend filter was tested and rejected** — it cut return more
    than risk (STRATEGIES.md). PULSE's only risk control is volatility
    targeting. This means PULSE has no "go fully to cash" state; in a slow
    grinding bear (e.g. 2022) it stays invested at reduced leverage and rides
    the decline down. The portfolio risk overlay and ROTATION cushion this at
    the book level, but the engine is always at least partly long equities.
