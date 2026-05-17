# Horizon Strategies — as built and as validated

Four candidate strategies were built. Two cleared the pre-registered gating bar
(DESIGN.md section 9) and form the engine; two failed and are excluded. Each
strategy is a pure `decide(MarketView, state) -> Decision` — the same object the
backtest and the live engine both call.

---

## PULSE — adaptive vol-targeted leveraged growth core  *(ADMITTED)*

`strategies/pulse.py`

**Logic.** Hold QQQ with volatility-targeted leverage: `leverage =
clip(target_vol / realized_vol_20d, 0.5, 2.0)`. When QQQ's realized volatility
rises — which it does in every crash — leverage falls automatically. This is
the risk control: continuous, with no whipsaw. Borrowed dollars accrue margin
interest daily.

**Parameters.** `target_vol = 0.22` (the per-sleeve leverage dial — set so
standalone drawdown stays within the A3 limit), `vol_days = 20`,
`max_leverage = 2.0`.

**Validated (2008-2026, standalone):** CAGR 18.9%, Sharpe 0.68 (vs QQQ 0.61),
MaxDD −38.5% (vs QQQ −49%), +2.4pp vs QQQ. Passes A1-A7 and the A6 ±25%
robustness test.

**A rejected component, reported honestly.** PULSE was first designed with a
binary EMA-105 trend filter (cash out when the trend breaks). It was built
(`use_trend_filter`, default off) and tested. The trend filter **cut return
more than it cut risk** in *both* the in-sample and out-of-sample windows — a
trend-timed PULSE trailed QQQ while no-trend PULSE beat it. The whipsaw cost
(selling dips, rebuying rips) exceeded the drawdown protection. So PULSE ships
without it. Volatility targeting alone is the risk control. This negative
finding is itself a validated result.

---

## ROTATION — cross-asset dual momentum  *(ADMITTED)*

`strategies/rotation.py`

**Logic.** Each month, score five low-correlation assets (QQQ, EFA, TLT, GLD,
DBC) by blended 3/6/12-month total-return momentum. Hold the top 2 — but only
those whose absolute momentum also beats T-bills (BIL); otherwise hold BIL.
Long-only, unleveraged, monthly rebalance.

**Parameters.** `lookbacks = (63, 126, 252)`, `top_n = 2`.

**Validated (2008-2026, standalone):** CAGR 13.1%, Sharpe 0.65, MaxDD −30.1%,
correlation to QQQ **+0.38**. It does not beat QQQ standalone — it is not meant
to. It is the diversifier: it earns in bonds/gold/commodities exactly when
equities are weak, which is why the engine's drawdown (−27%) is far below
QQQ's (−49%). Passes A1-A7 and A6.

---

## REVERT — mean-reversion swing  *(REJECTED — failed A2)*

`strategies/revert.py`

**Logic.** In a healthy market (SPY > 200-day SMA), buy liquid index/sector
ETFs that are sharply oversold within their own uptrend (`RSI(2) < 10`, price
above its 200-day SMA); exit on the bounce. Up to 4 positions, 2-8 day holds.

**Validated (2008-2026, standalone):** CAGR 4.3%, **Sharpe 0.06**, MaxDD −19%.
The short-horizon mean-reversion edge — well documented but heavily crowded —
has decayed to near-nothing on liquid ETFs over this sample. REVERT fails
gating bar **A2 (Sharpe ≥ 0.40)** decisively. It is excluded. A near-zero-Sharpe
sleeve adds no return to a levered book; admitting it would be levering noise.

---

## DRIFT — overnight seasonality  *(REJECTED — failed A1, A2, A3)*

`strategies/drift.py`

**Logic.** Hold QQQ over the close→open session when the market is not
risk-off; skip the weekend hold. Decision strictly precedes both fills.

**Validated (2008-2026, standalone):** CAGR −4.1%, Sharpe −1.10. The overnight
drift is real *gross*, but a daily close/open round-trip pays slippage on both
legs every session — ~10-20%/year of drag — which more than consumes the edge.
This is exactly the outcome independent research predicted ("the overnight edge
shrinks at realistic fills"). DRIFT was kept as a deliberate honesty test; it
fails, and the failure is reported.

---

## The regime layer  (`engine/intelligence.py`)

A pure function of market data classifies the regime — RISK_ON / NEUTRAL /
RISK_OFF / CRISIS — from four backtestable signals: trend (SPY/QQQ vs 200-day
SMA), volatility (SPY realized-vol percentile), breadth (equal-weight RSP vs
cap-weight SPY), and credit (HYG vs IEF). It applies a bounded monthly
allocation tilt between sleeves. No Reddit or prediction-market inputs — those
have no backtestable history (see DATA_PIPELINE.md).

## Risk overlay  (`engine/risk.py`)

A portfolio drawdown overlay de-levers the whole book when it draws down,
measured against a **rolling 252-day high** — never an all-time peak. Every
de-risk state has a guaranteed recovery path; `risk.recovery_is_guaranteed()`
is an asserted invariant. This is the explicit fix for the Unified Engine's
drawdown "death-spiral" bug.

## Withdrawals  (`engine/withdrawal.py`)

A high-water-mark monthly skim: when gross value sets a new high, a fraction of
the gain is withdrawn; in down months, nothing. Honest caveat — on a volatile
growth account, genuine new highs cluster in bull runs and pause for years in
drawdowns, so withdrawals are *not* reliably monthly (≈2/year on the validated
run). See LIMITATIONS.md.
