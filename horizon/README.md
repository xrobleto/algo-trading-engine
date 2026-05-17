# Horizon Engine

A forward-looking, data-driven, multi-strategy trading engine — a clean-room
rebuild of the strategy layer benchmarked against **QQQ**.

Horizon was built from scratch (it does not modify the existing Unified
Engine). It reuses the proven framework pattern — sleeves, an ownership ledger,
a regime layer, broker reconciliation — and replaces the strategy layer with
strategies that are **faithfully backtestable** and **walk-forward validated**.

## The honest headline result

Validated on 2008-2026 daily data, net of modeled slippage and borrow cost,
out-of-sample across every major regime (2008 GFC, 2020 crash, 2022 bear):

| Configuration | CAGR | Sharpe | Max drawdown | vs QQQ |
|---|--:|--:|--:|--:|
| QQQ buy & hold | 16.4% | 0.61 | −49% | — |
| Horizon, book-leverage 1.0 | 16.7% | **0.72** | **−27%** | +0.2pp |
| Horizon, book-leverage 1.4 | 19.1% | 0.67 | −33% | +2.7pp |
| Horizon, book-leverage 1.8 | 20.8% | 0.64 | −38% | +4.4pp |

**The engine does not clear the +7pp-over-QQQ stretch goal** — that target sits
beyond what faithfully-validated, retail-tradeable strategies deliver within
Reg-T leverage without ruinous drawdown. What it *does* deliver is a genuine,
robust improvement: **QQQ-equivalent return at roughly half the drawdown** (un-
levered), or **QQQ + ~3-4pp** at a still-shallower drawdown (levered) — and
+6.3pp out-of-sample. Two of the four candidate strategies (REVERT, DRIFT)
failed their gating bar and are excluded; that is reported plainly.

Full results: [docs/VALIDATION.md](docs/VALIDATION.md). The reasoning, the
architecture and the *pre-registered* gating bar: [docs/DESIGN.md](docs/DESIGN.md).

## Quickstart

```bash
pip install -r horizon/requirements.txt          # pandas, numpy, requests, alpaca-py
cp horizon/.env.example horizon/.env             # then fill in POLYGON_API_KEY etc.
```

Run the full validation (fetches/caches data, backtests, gates, writes the report):

```bash
python -m horizon.backtest.run_validation        # -> horizon/docs/VALIDATION.md
```

Run the live engine in dry-run (logs the orders it *would* place, submits nothing):

```bash
python -m horizon.engine.main --once             # one cycle
python -m horizon.engine.main --interval 900     # loop every 15 minutes
```

Live trading is the user's explicit decision and requires `LIVE_TRADING=1` plus
`I_UNDERSTAND_LIVE_TRADING=YES` in `.env`. **Horizon is delivered for testing,
not deployment** — see [docs/TEST_PLAN.md](docs/TEST_PLAN.md).

## Repository layout

```
horizon/
  config.py            engine + sleeve + cost configuration
  data/                Polygon ingestion, cache, calendar, universe
  strategies/          PULSE, ROTATION, REVERT, DRIFT — pure decide() functions
  engine/              ledger, sleeves, broker, reconciler, intelligence,
                       risk, withdrawal, kill switch, live main loop
  backtest/            costs, metrics, faithful harness, portfolio,
                       walk-forward, run_validation
  docs/                DESIGN, STRATEGIES, DATA_PIPELINE, VALIDATION,
                       RECONCILIATION, TEST_PLAN, LIMITATIONS
  tests/               unit tests (no-look-ahead, risk recovery, ledger)
```

## How it is faithful

- **Single source of truth.** The backtest harness imports and calls the exact
  production `Strategy.decide()` — the live engine calls the identical objects.
  No reimplementation, so the backtest cannot diverge from live behavior.
- **No look-ahead.** A strategy only ever sees a `MarketView` sliced at the
  simulated day; future bars are physically absent. Signal on T, fill at T+1.
- **Realistic costs.** Slippage on every fill, daily borrow interest on
  leveraged dollars, the real $7,400 account size.
- **Honest gating.** A strategy enters the engine only if it clears a bar that
  was written down *before* any test was run. Most candidates fail — and the
  failures are reported.
