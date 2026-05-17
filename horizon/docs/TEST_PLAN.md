# Horizon Engine — Test Plan

Horizon is delivered **validated and runnable, not deployed**. Deployment is
your decision. You have indicated a preference against paper accounts, so the
plan centers on the faithful backtest, the walk-forward results, and a cautious
small-stakes live rollout you control.

## Stage 0 — Understand the honest result  *(no code)*

Read [DESIGN.md](DESIGN.md) section 9 (the pre-registered gating bar) and
[VALIDATION.md](VALIDATION.md). Key things to internalize:

- The engine delivers **QQQ + 0 to +4.4pp CAGR** (full period) at a **lower
  drawdown and higher Sharpe than QQQ** — and +6.3pp out-of-sample. It does
  **not** clear the +7pp stretch goal; that is reported honestly.
- `book_leverage` is the dial: 1.0 = QQQ-return at ~half QQQ's drawdown;
  ~1.8 = QQQ +4.4pp at a drawdown still below QQQ's. Higher leverage = more
  return and more drawdown. **You choose the point on the frontier.**
- REVERT and DRIFT failed their bar and are not in the engine.

## Stage 1 — Reproduce the validation  *(~10 min, no money)*

```bash
python -m horizon.backtest.run_validation
```

Confirm `docs/VALIDATION.md` regenerates with the same numbers. Inspect the
walk-forward table — every regime window (2008 GFC, 2020, 2022) should show the
engine surviving within its drawdown limits.

## Stage 2 — Dry-run the live engine  *(2-4 weeks, no money)*

```bash
python -m horizon.engine.main --interval 900     # logs orders, submits nothing
```

Each cycle logs the regime and the orders the engine *would* place. Daily,
sanity-check those against your own read of the market. Run the
decision-equivalence check in [RECONCILIATION.md](RECONCILIATION.md) section 2.

## Stage 3 — Cautious small-stakes live  *(your decision; 30+ days)*

Only if Stages 1-2 satisfy you:

1. Use a **small slice** of capital first — not the full $7,400, and not the
   $30k. A few hundred dollars is enough to validate execution.
2. Set a **conservative `book_leverage` (1.0-1.2)** in `config.py` to start.
3. Put working Alpaca keys in `.env`; set `LIVE_TRADING=1` and
   `I_UNDERSTAND_LIVE_TRADING=YES`.
4. Run `python -m horizon.engine.main --interval 900 --live`.
5. Each week, run the execution-fidelity check (RECONCILIATION.md section 3):
   do live fills and the equity path track the backtest expectation?
6. The kill switch: create the file `state/HALT_ALL_TRADING` or set
   `HORIZON_KILL_SWITCH=1` to block all new entries immediately.

## Stage 4 — Scale  *(only after reconciliation holds 30+ days)*

If live reconciles with the backtest, scale capital and/or `book_leverage`
toward your chosen frontier point. Re-read the drawdown column before raising
leverage — the deep-drawdown scenarios in VALIDATION.md are real and will
happen.

## What NOT to do

- Do not skip Stage 2-3. The live engine's order layer is v1 (LIMITATIONS.md).
- Do not deploy the $30k before a 30+ day live track record reconciles.
- Do not raise `book_leverage` to chase the +7pp number — the validation shows
  that costs a drawdown the gating bar deliberately rejects.
