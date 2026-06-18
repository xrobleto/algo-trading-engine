# Unified Engine — Verified Architecture Map (2026-06-15)

Code-verified against `/Users/xrobleto/Projects/algo-trading-engine` (the live repo). This supersedes
prior summaries; every claim below was checked against source.

## 1. Entry point & main loop
- Live: `python -m engine.main` from `strategies/` (Railway `railway.toml` + `Dockerfile`; service
  `algo-trading-engine`, project `inspiring-benevolence`, `ALGO_OUTPUT_DIR=/data`, LIVE_TRADING=1).
- `strategies/engine/main.py`: session-aware loop — MARKET_OPEN ticks ~5s, PRE_MARKET ~60s (intel/equity
  refresh, no strategy ticks), OFF_HOURS ~300s. Per open tick: kill-switch check → intel refresh (~10min)
  → equity + sleeve allocation → conflict detection → sequential `adapter.tick()` → ledger persist →
  periodic reconcile (~60s) → heartbeat.

## 2. The three sleeves (adapters monkey-patch the underlying bots; zero reimplementation)
- **TREND** — `adapters/trend_adapter.py` → `strategies/trend_bot.py`. Vol-targeted ETF rotation, SMA200
  regime, weekly rebalance. Order prefix `ENG_TREND_`.
- **SIMPLE** — `adapters/simple_adapter.py` imports `simple_bot` and instantiates `MomentumBot`; patches
  module globals + `submit_order`/`generate_client_order_id`. Intraday momentum. Prefix `ENG_SIMPLE_`.
- **CROSSASSET** — `adapters/cross_asset_adapter.py` → `cross_asset_bot.py`. Rates/commodities/FX ETFs.
  Prefix `ENG_XASSET_`.

## 3. Allocation & intelligence (`engine/config.py`, `engine/intelligence.py`)
- Composite allocation (config.py): **TREND 0.75 / SIMPLE 0.10 / CROSSASSET 0.12 / Cash 0.03** (commit
  `05faf6b` shifted TREND 0.65→0.75, SIMPLE 0.20→0.10 on 06-02).
- **SIMPLE is on PROBATION** (config.py): `MAX_POSITIONS=1`, `POSITION_SIZE_PCT=0.02`, daily-loss 2%,
  `auto_halt_on_anomaly=True` (`max_fill_deviation_pct=0.02`).
- Intelligence: 5 sources fused — macro 0.35 / polymarket 0.20 / event_calendar 0.20 / news 0.15 /
  reddit 0.10 → composite (0–100) → 4 regimes (RISK_ON/CAUTIOUS/RISK_OFF/CRISIS) with ±3 hysteresis.
  **All four regimes are acted upon** (allocation + risk multipliers; CRISIS hard-blocks SIMPLE entries).
- Market-structure gate (WS3 breadth) and chop dampener (WS4) exist but are **feature-flagged OFF by
  default** (`INTEL_MARKET_STRUCTURE_GATE=0`, `INTEL_CHOP_DAMPENER=0`). Guardrails: ±10% allocation swing
  clamp, 2%/refresh velocity cap, 2% cash floor.

## 4. Ownership ledger & reconciler (`engine/ownership.py`, `engine/reconciler.py`)
- Ledger keyed by `client_order_id`; statuses pending→filled/partial→closed/cancelled; atomic JSON persist
  after each tick (`engine_ownership_{live|paper}.json`).
- Reconciler (startup + ~60s): classifies broker positions/orders by prefix, resolves pending→filled via
  Alpaca order history (fallback: infer from position), closes vanished fills, adds synthetic entries for
  unrecognized positions, kills on cross-strategy conflict. Phantom-drift patches in trend_adapter.

## 5. Order/execution
- Each adapter patches its bot's broker client: validates sleeve capacity, registers ownership, prefixes
  client_order_id. Bots place bracket/`oto` orders. **GOTCHA (verified live): bracket TP/SL child legs get
  Alpaca-generated client_order_ids, NOT the `ENG_SIMPLE_` prefix** — so prefix-only P&L attribution
  undercounts exits; attribute by symbol set instead.
- Slippage/commission: not modeled in engine (lives in broker fills; Alpaca equities commission-free).

## 6. Risk controls
- Global kill switch (`engine/portfolio_kill_switch.py`): file `${state}/HALT_ALL_TRADING` / env
  `KILL_SWITCH=1` / programmatic — blocks entries, always allows exits.
- Per-sleeve daily-loss: TREND 10% / SIMPLE 4% (2% on probation) / CROSSASSET 15%. Per-regime risk
  multipliers 0.5–1.2. Consecutive-error auto-halt (default 5). SIMPLE fill-deviation anomaly halt.

## 7. State / logs (ALGO_OUTPUT_DIR=/data on Railway; local `~/Library/Application Support/AlgoTrading`)
- `data/state/`: ownership ledger, kill switch, per-bot state (`momentum_bot_state.json` etc.), heartbeats.
- `data/logs/`: `engine.log`, `intelligence.log`+`.jsonl`, per-bot logs. Live journals are on the Railway
  volume (local `data/state` empty here). `railway ssh` needs an SSH key; `railway run` injects live env.

## 8. Backtest harness faithfulness (CRITICAL)
- `backtest/simple_bot_backtest.py` — **REIMPLEMENTATION**, no import of `simple_bot`; 4-feature vs live
  8-feature scoring; 0% slippage; no commissions; no 45s timeout. Unfaithful (flatters).
- `backtest/trend_bot_backtest.py` — also reimplements.
- `backtest/cross_asset_bot_backtest.py` — **FAITHFUL**: imports `compute_signals`/`compute_target_weights`
  from the production bot. This is the in-repo template for single-source-of-truth.

## 9. Deployment
- Git → Railway (Dockerfile builds; `python -m engine.main`). Config from `config/*.env` (gitignored;
  keys live on Railway + Google Drive copy). NOTE: a Polygon key is **hardcoded** in
  `backtest/simple_bot_backtest.py:30` (committed secret — should be rotated/removed).

## 10. Horizon inventory (`horizon/`)
- Daily-bar, no-deploy sandbox. Faithful harness (`horizon/backtest/harness.py`: signal@T/fill@T+1,
  slippage, borrow), walk-forward across 6 regimes 2008–2026, pre-registered gating bar A1–A7/E1–E5
  (`docs/DESIGN.md §9`), rolling-252d drawdown risk that fixes the death-spiral (`engine/risk.py`),
  single-source-of-truth via `strategies/registry.build_all()`. Admitted PULSE+ROTATION; rejected
  REVERT+DRIFT (honestly reported in `docs/VALIDATION.md`/`LIMITATIONS.md`).
- **Key tension for SIMPLE:** Horizon is daily-only with NO intraday capability and NO parabolic strategy
  built (Section 5 names it as a vision). It deliberately EXCLUDED options-flow/dark-pool/short-vol/
  sentiment as non-backtestable (`docs/LIMITATIONS.md`). So Horizon is the *standard to inherit*, not the
  host. Build brief `horizon_engine_prompt.md` lives only in the Google Drive copy.

## Surprises / looks-wrong / inconsistent
1. **SIMPLE identity contradiction (confirmed):** `simple_bot.py:3499-3615` scorer penalizes extension
   (VWAP>3%→30%, 5min-mom>1.5%→30%, ADX>35→40%, EMA-sep>2%→40%); scanner hunts the opposite. Correction
   to the brief: **RVOL has no exhaustion penalty** (>5x scores full).
2. **SIMPLE is on probation** (smaller than the brief implies): ~$780 sleeve, 1 position, 2% sizing.
3. **WS3/WS4 gates default OFF** — the chop/breadth protections aren't active.
4. **Bracket-leg attribution gotcha** (see §5) — affects any prefix-based accounting.
5. **Committed Polygon key** in `simple_bot_backtest.py:30`.
6. **Brief vs reality:** Horizon can't currently backtest the intraday parabolic strategy the brief wants
   built on it; resolved by "backtestable core + live overlay" in the Unified Engine sleeve.
