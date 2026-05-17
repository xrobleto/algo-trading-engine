# Horizon Engine — Design Document

> Status: design locked 2026-05-17. This document is written **before** any
> backtesting. The gating bar in §9 is pre-registered: it is the standard the
> work is judged against, decided in advance so results cannot be rationalized
> after the fact.

> **Implementation note (post-validation).** This document is the
> pre-registration and is left unchanged. Where the as-built engine differs
> from this plan — most notably PULSE's binary trend filter, which was built,
> tested, and rejected because it cut return more than risk — see
> `STRATEGIES.md` and `VALIDATION.md`. The §9 gating bar was applied exactly as
> written; the honest result (the +7pp goal is not met) is in `VALIDATION.md`.

---

## 1. Mission & success bar

Build a new, forward-looking, multi-strategy trading engine — "Horizon Engine" —
that **beats QQQ by at least 7 percentage points of CAGR per year**, measured on
faithfully-backtested, walk-forward, out-of-sample data, net of realistic costs.

- Capital modeled: **$7,400** (the live account size). Leverage permitted.
- Risk tolerance: **high**. Drawdowns are acceptable if the probability-weighted
  payoff justifies them. Ruin is not acceptable.
- Broker: **Alpaca**. Data: **Polygon** (deep history) + Alpaca (execution).
- Runtime target: **Railway**, alongside (not replacing) the existing Unified
  Engine. Code lives on GitHub and syncs to Google Drive.
- The user wants to **withdraw gains roughly monthly during good periods** —
  Horizon models this with a high-water-mark profit-skim.

This is an extreme bar. QQQ compounded ~16–19%/yr over the last decade; +7pp
means a sustained ~23–26% CAGR through 2008, 2020 and 2022. Most candidate
strategies will not clear it. **That is expected and is reported honestly.** The
deliverable is a *truthfully validated* engine, not an optimistic one.

---

## 2. Why Horizon exists

The existing Unified Engine runs three momentum-family strategies (TREND,
SIMPLE, CROSSASSET). A faithful rebuild of its backtest *measurement* showed the
flagship strategy earns ~3.7% CAGR — its old 17–31% figures were inflated by four
stacked errors: a one-bar look-ahead, a phantom 1.25× leverage, a crude momentum
formula, and omitted risk-scaling. A separate drawdown "death-spiral" bug was
also found.

The engine **framework** (orchestration, ownership ledger, sleeve/adapter
pattern, reconciliation, regime layer) is sound. The **strategy layer** — the
source of returns — failed, and all three sleeves were the same directional bet
(long-equity momentum), so the book could not diversify into an edge.

Horizon reuses the framework pattern and replaces the strategy layer with a
roster of **genuinely uncorrelated, individually-validated** strategies.

---

## 3. The data-fidelity reality (read this first)

Every hard requirement in the brief — single source of truth, no look-ahead,
walk-forward validation — means **a strategy that cannot be faithfully
backtested cannot be validated, and therefore cannot enter the engine.** This
constraint, not aesthetic preference, drives the design.

What the Polygon key can deliver, verified by direct probing on 2026-05-17:

| Data | Coverage | Backtestable? |
|---|---|---|
| Daily stock/ETF/index OHLCV | ~2003 → present | **Yes — deep** |
| Intraday (1/5-min) bars | recent years | Yes (recent) |
| VIX / SPX / NDX index daily | ~2003 → present | **Yes — deep** |
| Tick trades & NBBO quotes | recent | Partial |
| Fundamentals (`/vX/reference/financials`) | with `filing_date` | Yes (point-in-time) |
| News + per-ticker sentiment `insights` | ~2018 → present | Shallow / partial |
| Short interest (`/stocks/v1/short-interest`) | 2017 → present, bi-weekly | Shallow |
| Short volume (`/stocks/v1/short-volume`) | ~2024 → present | **Too shallow** |
| Options chain snapshot (greeks/IV/OI) | live snapshot only | **No history** |
| Option contract aggregates | ~last ~2yr only (older = 403) | **No** |
| Reddit / ApeWisdom sentiment | live only | **No** |
| Polymarket prediction markets | live only | **No** |

**Consequences, applied honestly:**

1. **Options-flow / gamma strategies cannot be validated.** There is no
   historical option-chain depth. Per independent market-structure research,
   naive options-flow following is also only ~55–60% directional anyway. Options
   are **excluded** from the validated engine and noted as future live-only
   research.
2. **Reddit and Polymarket are live-only.** They may appear as optional,
   clearly-flagged *enrichments* in the live engine, but they are **never
   load-bearing** in a validated strategy — you cannot walk-forward what has no
   history.
3. **The validated engine rests on deeply-backtestable data:** price, volume,
   volatility, cross-sectional breadth, index/VIX, and (for the regime layer)
   macro proxies built from liquid ETFs. This is not a retreat to "price-only" —
   breadth, dispersion, cross-asset behavior and volatility regime are genuine
   market-structure signals. It is a retreat to *honesty*: only signals whose
   history exists get to claim a validated edge.

ATD (alphatraderdaily.com) was studied as instructed. It is a thin caching layer
over the same upstreams (Polygon, Finnhub, FINRA, ApeWisdom, Polymarket) and
stores no historical market-data warehouse. Horizon therefore goes **directly to
Polygon** — replicating ATD's derivation methods (RVOL, breadth, regime-from-MA),
not depending on ATD's uptime. ATD's database holds nothing Horizon needs.

---

## 4. Architecture

Horizon keeps the Unified Engine's proven spine and fixes its documented design
debts. The package (`horizon/`) is fully self-contained and independent of the
old engine.

```
horizon/
  config.py            EngineConfig / SleeveConfig
  platform.py          state/log paths (ALGO_OUTPUT_DIR aware)
  data/
    polygon_client.py  Polygon REST client (retry, paging)
    cache.py           on-disk parquet cache of daily bars
    calendar.py        NYSE trading calendar
    universe.py        tradable universe definitions
  strategies/
    base.py            Strategy ABC — PURE functions; MarketView; TargetBook
    indicators.py      shared TA (sma/ema/rsi/atr/realized-vol/momentum)
    pulse.py           PULSE   — trend-timed leveraged growth core
    rotation.py        ROTATION— cross-asset dual momentum
    revert.py          REVERT  — mean-reversion swing
    drift.py           DRIFT   — overnight seasonality
  engine/
    ledger.py          ownership ledger (prefix-tagged, single source of truth)
    sleeves.py         capital allocation across strategies
    broker.py          Alpaca facade (read + guarded submit)
    reconciler.py      ledger <-> broker convergence
    intelligence.py    regime layer (backtestable inputs only)
    risk.py            rolling-reference drawdown control (no death-spiral)
    withdrawal.py      high-water-mark monthly profit-skim
    killswitch.py      portfolio kill switch
    main.py            orchestration loop
  backtest/
    costs.py           slippage + commission + borrow model
    metrics.py         honest performance metrics
    harness.py         the faithful single-source-of-truth harness
    walkforward.py     rolling out-of-sample driver
    run_validation.py  end-to-end validation entry point
  docs/  tests/  results/
```

### 4.1 The single-source-of-truth contract

This is the most important design rule, and it is structural — not a discipline
the harness has to remember.

- Every strategy in `strategies/*.py` exposes one pure method:
  `decide(view: MarketView, state: StrategyState) -> Decision`.
- `MarketView` exposes market data **only up to the current simulated/real
  date** — it physically cannot return a future bar. No look-ahead is possible
  because future data is not in the object.
- `decide()` takes no wall clock and does no network I/O. It is a pure function
  of its arguments.
- `backtest/harness.py` and `engine/main.py` **both import and call the exact
  same `decide()`**. There is no "backtest copy" of any strategy. A unit test
  asserts both import paths resolve to the same module object.

This removes the Unified Engine's monkey-patching entirely. The old engine had
to patch `now_et`, `fetch_real_vix`, etc., because its strategies were live
programs; the four ways its backtest diverged from reality all leaked through
those seams. Horizon strategies are pure from day one, so there is nothing to
patch and nothing to diverge.

### 4.2 Execution model (no look-ahead, T→T+1)

- A strategy `decide()`s using data through the close of day **T**.
- The engine/harness executes the resulting orders at day **T+1's open**, at a
  fill price including modeled slippage. Never same-bar signal-and-fill.
- DRIFT is the one declared exception (`execution = OVERNIGHT`): it decides from
  data through T-1 and is credited the close(T)→open(T+1) return — the decision
  strictly precedes both fills. This is faithful, just a different window.

### 4.3 Sleeves, ledger, reconciler, broker, kill switch

Reused conceptually from the Unified Engine, rebuilt clean:

- **SleeveManager** — splits account equity into per-strategy dollar budgets.
  Static base allocations, scaled each cycle by the regime layer.
- **OwnershipLedger** — every order tagged by `client_order_id` prefix
  (`HZN_PULSE_`, `HZN_ROT_`, `HZN_REV_`, `HZN_DRIFT_`). The ledger is the single
  source of truth for "which strategy owns what." Atomic JSON persistence.
- **Reconciler** — converges ledger against the Alpaca account each cycle:
  resolves pending→filled via order history, closes entries whose position is
  gone, creates synthetic entries for unrecognized positions. Conflicts trip the
  kill switch.
- **BrokerFacade** — Alpaca wrapper. Live-vs-paper is cross-checked against the
  base URL; live trading requires explicit confirmation env vars.
- **PortfolioKillSwitch** — file/env/programmatic halt; blocks new entries, never
  blocks exits, never kills the process.

Design debts from the Unified Engine that Horizon fixes: the `_current_ctx`
implicit contract is hoisted into the base class; the `qty*100` notional
fallback is replaced with a real quote; T+1 execution and a commission/borrow
model are in the harness from the start; session classification consults the
broker market clock; no secret is ever committed.

---

## 5. The strategy roster

Four candidate strategies, chosen for **uncorrelated payoff profiles** — the
property the old engine lacked. Each is faithfully backtestable.

### 5.1 PULSE — adaptive trend-timed growth core *(the return engine)*

**Thesis.** The single most reliable way to beat QQQ by a wide margin is to hold
leveraged Nasdaq exposure during confirmed uptrends and step entirely aside
during downtrends — capturing the up-years with leverage while dodging the
−30%-to-−50% drawdowns that destroy compounding. This is the only sleeve that
can plausibly deliver the +7pp on its own.

**Universe.** `QQQ` for growth exposure; `BIL` (T-bills) when risk-off.

**Signal (data through T).**
- Trend ON when QQQ close is above its `EMA(105)` (a faster lookback than the
  pre-2015 200-day norm — markets cycle faster now) **and** 6-month absolute
  momentum is positive. Trend OFF otherwise. Hysteresis band of ±1.5% on the EMA
  cross prevents whipsaw.
- When trend is OFF → 0% QQQ, 100% BIL.

**Leverage (volatility targeting).** When trend is ON, target a constant sleeve
volatility of **28%** annualized: `leverage = clip(0.28 / realized_vol_20d, 0.5,
2.0)`. Leverage is capped at **2.0×** (Alpaca Reg-T overnight max). Borrowed
dollars accrue margin interest every day in the backtest — leverage is explicit
and *costed*, never phantom.

**Rebalance.** Evaluated daily; acts only on a state change or a >10% leverage
drift. Positions held for weeks/months. PDT-safe (no intraday round-trips).

**Why it diversifies.** It is highly correlated to QQQ *when on* and completely
uncorrelated *when off* (in T-bills). Its edge is conditional exposure, not
stock selection.

### 5.2 ROTATION — cross-asset dual momentum *(the all-weather diversifier)*

**Thesis.** When equities are not trending, capital should still earn — in
bonds, gold, or commodities. Dual momentum (Antonacci-style) is one of the most
robust, highest-capacity systematic strategies and trades the most liquid ETFs
in the world.

**Universe.** `QQQ`, `EFA` (intl equity), `TLT` (long Treasuries), `GLD` (gold),
`DBC` (commodities), plus `BIL` (cash floor).

**Signal.** Each month, score every asset by blended absolute momentum =
average of its 3-, 6-, and 12-month total return. Hold the **top 2** assets
(equal weight) whose absolute momentum also exceeds BIL's return. If fewer than 2
qualify, the remainder goes to BIL.

**Rebalance.** Monthly. Long-only, unleveraged.

**Why it diversifies.** It is *designed* to hold non-equity assets exactly when
PULSE is in cash. Its return stream is structurally decorrelated from PULSE and
from QQQ.

### 5.3 REVERT — mean-reversion swing *(the anti-momentum diversifier)*

**Thesis.** Short-horizon mean reversion is the textbook diversifier to trend
following — it earns when trend chops. It also realizes gains every few days,
which directly serves the monthly-withdrawal goal.

**Universe.** Liquid index/sector ETFs: `SPY, QQQ, IWM, DIA, XLK, XLF, XLV,
XLY, XLE, XLI, SMH`.

**Signal.** Only active when the broad market is healthy (`SPY > SMA(200)`).
Enter an ETF when it is sharply oversold within its own uptrend:
`RSI(2) < 10` **and** ETF close `> SMA(200)`. Equal-weight across signals, max
**4** concurrent positions.

**Exit.** `RSI(2) > 65`, or close back above `SMA(5)`, or a 7-trading-day time
stop, or a −8% hard stop — whichever first. Holding period 2–8 days.

**Why it diversifies.** Mean reversion is negatively correlated with trend
following by construction; it is the sleeve that earns in the choppy regimes
where PULSE whipsaws.

### 5.4 DRIFT — overnight seasonality *(the orthogonal stream — and an honest test case)*

**Thesis.** US index ETFs have historically earned the large majority of their
return in the overnight (close→open) session. Independent research confirms the
effect is real but **shrinks at realistic fills** — making DRIFT a deliberate
honesty test: if it fails the gating bar once costs are modeled, that failure is
reported plainly.

**Universe.** `QQQ`, overnight only.

**Signal (`execution = OVERNIGHT`).** Decide from data through T-1: hold QQQ over
the close(T)→open(T+1) session when the market is not risk-off (`SPY > SMA(200)`
as of T-1). Skip Friday→Monday weekend holds (research shows the weakest window).

**Costs.** Both legs modeled with conservative slippage; the harness also runs a
sensitivity that fills at "open + a few minutes" to quantify the OHLC-artifact
risk the research flagged.

**Why it diversifies.** It is a *time-of-day* exposure, not a directional bet —
near-orthogonal to PULSE, ROTATION and REVERT.

### 5.5 Why this is genuine diversification (not "four flavors of momentum")

The old engine failed because TREND, SIMPLE and CROSSASSET were all the same
bet: long US equity, momentum-continuation. Horizon's four sleeves have
deliberately different payoff profiles:

| Sleeve | Edge type | Wins when… | Asset class |
|---|---|---|---|
| PULSE | Trend / leveraged beta | equities trend up | US growth equity |
| ROTATION | Cross-asset momentum | *something* trends | bonds/gold/cmdty/equity |
| REVERT | **Mean reversion** | equities chop / dip-buy | US equity ETFs |
| DRIFT | Seasonality | structural overnight drift | US equity (overnight) |

REVERT is explicitly *anti*-correlated to PULSE. ROTATION earns in the regimes
PULSE sits out. DRIFT is orthogonal in time. The pairwise-correlation gate (§9,
A5) enforces this quantitatively — a candidate that turns out to be just another
copy of PULSE is rejected.

---

## 6. Intelligence / regime layer

A single `MarketContext`, refreshed each cycle, classifies the regime and scales
the sleeves. **Every input is backtestable** (no Reddit, no Polymarket):

- **Trend** — SPY/QQQ vs long moving averages.
- **Volatility** — VIX level and its 1-year percentile.
- **Breadth** — % of S&P 500 members above their 50- and 200-day moving averages,
  computed from Polygon grouped-daily bars (this is the TCAF "narrow market"
  signal, and it is fully reconstructable historically).
- **Credit / risk appetite** — HYG-vs-IEF and LQD-vs-IEF relative trend, a
  liquid-ETF proxy for credit spreads.

Regimes: `RISK_ON / NEUTRAL / RISK_OFF / CRISIS`. Levers (all bounded, all
logged):
1. **Allocation** — shift sleeve weights within ±10% of base.
2. **Risk** — scale each sleeve's effective equity 0.5×–1.2×; PULSE's leverage
   cap is lowered in RISK_OFF/CRISIS.
3. **Gating** — block new REVERT/DRIFT entries in CRISIS.

If the layer fails entirely, it falls back to all-1.0 multipliers — zero
behavioral change, never a hard failure.

---

## 7. Risk management & withdrawals

### 7.1 No death-spiral — ever

The Unified Engine's worst bug measured drawdown from a never-resetting
all-time-high, so a deep drawdown permanently suppressed position sizing with no
recovery path. Horizon's `engine/risk.py`:

- Measures drawdown against a **rolling 252-day high**, not the all-time peak.
- De-risking is **continuous and bounded** — it scales exposure down smoothly and
  has a floor; it never drives exposure to zero from a drawdown signal alone.
- Every risk state has an explicit **recovery path**: when the rolling reference
  is matched again, exposure returns to normal. A unit test asserts that from any
  drawdown state, a recovering equity curve restores full exposure.

### 7.2 Leverage realism

PULSE leverage is capped at 2.0× (Alpaca Reg-T overnight). Borrowed dollars
accrue a configurable margin rate (default 6.5%/yr) debited daily in the
backtest. The backtest models the actual $7,400 account, fractional shares, and
cash earning the T-bill rate (held as `BIL`). No phantom leverage, no free cash.

### 7.3 High-water-mark withdrawals

`engine/withdrawal.py` tracks total deposited capital and an all-time equity
high-water-mark. Monthly, if equity exceeds the HWM, it skims a configurable
fraction (default 50%) of the excess as a "withdrawal"; in down months it skims
nothing. The engine **signals** the withdrawal — it never moves money itself.

The backtest reports **two** equity curves: fully-compounded (for apples-to-apples
comparison with QQQ and for the gating bar) and net-of-withdrawals (showing the
realistic cash the user takes out plus the residual account).

---

## 8. The faithful backtest harness

`backtest/harness.py` enforces every non-negotiable in the brief:

- **Single source of truth** — imports the production `Strategy` objects and
  calls their real `decide()`; never reimplements logic.
- **No look-ahead** — `MarketView` cannot expose a bar dated after the simulated
  day; signal on T, fill on T+1 open.
- **Realistic costs** — `backtest/costs.py`: per-trade slippage (2 bps for
  SPY/QQQ-class ETFs, 5 bps for sector ETFs), $0 equity commission (Alpaca), and
  daily margin-borrow interest on leveraged dollars.
- **Account realism** — models $7,400, fractional shares, a $1 minimum order,
  cash drag.
- **Honest metrics** — `backtest/metrics.py`: CAGR, Sharpe, Sortino, max
  drawdown + duration, Calmar, win rate, profit factor, turnover, beta and alpha
  vs QQQ. No stubs.

---

## 9. PRE-REGISTERED GATING BAR

Decided **now, before any test is run.** A result that misses a threshold is
reported as a miss; thresholds are not moved afterward.

### 9.1 Per-strategy admission bar

A candidate strategy is admitted to the engine **only if it passes ALL** of the
following, on out-of-sample walk-forward data, net of modeled costs, aggregated
across every OOS window:

- **A1 — Beats cash.** OOS net CAGR > 0 and ≥ the T-bill return over the period.
- **A2 — Risk-adjusted.** OOS Sharpe ≥ 0.40.
- **A3 — Survivable drawdown.** OOS max drawdown ≤ 40% (≤ 45% for PULSE, which is
  leveraged by design).
- **A4 — Diversifies or outperforms.** |corr(daily returns, QQQ)| ≤ 0.85, OR the
  strategy's standalone OOS CAGR exceeds QQQ's. A sleeve that is merely "QQQ with
  extra steps" is rejected.
- **A5 — Genuinely uncorrelated.** Pairwise daily-return correlation to every
  already-admitted strategy ≤ 0.75.
- **A6 — Not overfit.** Under a ±25% perturbation of each primary parameter, OOS
  CAGR stays positive and Sharpe stays ≥ 0.25. No knife-edge.
- **A7 — Regime survival.** Across the 2008, 2020 and 2022 OOS windows, drawdown
  stays within A3, risk controls demonstrably engage, and a recovery is observed.

### 9.2 Engine (portfolio) bar

The assembled book of *admitted* strategies, on OOS walk-forward data, net of
costs, must:

- **E1 — Beat QQQ CAGR by ≥ 7.0 percentage points.** *(The user's firm bar.)*
- **E2 — Win risk-adjusted.** Portfolio Sharpe ≥ QQQ's Sharpe.
- **E3 — Control drawdown.** Max drawdown ≤ QQQ's max drawdown over the same
  period — the diversification and regime overlay must pay for the leverage.
- **E4 — Be consistent.** Positive alpha vs QQQ in ≥ 70% of individual OOS
  windows — not one lucky window.
- **E5 — Pass reconciliation.** No look-ahead detected; the backtest↔live
  reconciliation gate (RECONCILIATION.md) passes within tolerance.

### 9.3 What happens on a miss

If a strategy fails its bar, it is **excluded** and the failure is reported
plainly. If the engine misses **E1**, the achieved margin (e.g. "QQQ + 3.4 pp")
becomes the **headline result** of the project. The engine still ships — runnable
and testable — but with its honest validated edge stated, not an aspirational
one. No hope is shipped.

---

## 10. Walk-forward validation methodology

- **History:** ~2004-01 → 2026-05 daily bars (Polygon depth ~2003).
- **Walk-forward windows:** rolling, non-overlapping ~2.5-year OOS test windows
  preceded by their training history, rolled across the full sample so every
  regime is an OOS test at least once: 2008 GFC, 2011, 2015–16, 2018Q4, 2020
  COVID crash, 2022 bear, 2023–24 bull, 2025–26.
- **Parameters:** primary parameters are fixed a priori at literature-standard,
  round-number values (listed in §5). Walk-forward's job is to test *stability*
  across regimes, not to optimize. Any parameter selection that is done happens
  only on in-sample data; OOS results are reported untouched. A6 sensitivity
  analysis quantifies overfit risk.
- **Benchmark:** QQQ total return (dividends reinvested) over each identical
  window.
- **Reporting:** per-strategy and portfolio metrics per window and aggregated;
  every number is the honest OOS figure. Results land in `docs/VALIDATION.md`.

---

## 11. Known limitations & fidelity gaps (stated up front)

1. **Options, Reddit, Polymarket are excluded** from the validated engine — no
   backtestable history. They are candidate live-only enrichments only.
2. **DRIFT's overnight edge is fill-sensitive** — modeled conservatively; a
   sensitivity case quantifies the OHLC-artifact risk.
3. **Leveraged-ETF path dependency is avoided** by getting PULSE leverage from
   modeled margin on QQQ rather than holding TQQQ — cleaner and backtestable to
   2004.
4. **Survivorship:** the ETF universe is fixed and liquid; ETF survivorship bias
   is negligible. REVERT uses ETFs, not delisting-prone single stocks, partly for
   this reason.
5. **Backtest ≠ live.** The faithful harness is the best obtainable estimate, not
   a guarantee. Real fills, halts, and corporate actions will differ. The
   reconciliation gate (E5) and a cautious small-stakes live rollout — never a
   paper account, per user preference — are how that gap is closed.
