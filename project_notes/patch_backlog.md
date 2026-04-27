# Unified Engine — Patch Backlog

Patches 1–7 are live. Numbered items below are the pending queue, roughly ordered
by impact × confidence. Each entry: **trigger** (the real event that exposed it),
**symptom**, **root cause**, **proposed fix**, **test plan**.

---

## Patch 8 — SIMPLE fill-status reconciler (entry-timeout false-flatten)

**Status**: PROPOSED — queued after 2026-04-20 MP incident.

**Trigger event**
- 2026-04-20 14:19:09 UTC: SIMPLE opened MP (18 shares @ $63.91, A+ long, score 67.5).
- Entry filled in full within 50 ms (Alpaca `fill` event, `filled_qty=18/18`).
- 14:20:57 UTC: 45-second entry-timeout fired and flattened the position at
  $63.82 IOC limit. Realized PnL −$1.62 (≈ −0.14%).
- No stop hit, no TP hit, no regime flip, no kill switch — purely a state-machine
  misfire.

**Symptom** (`logs/momentum_bot.log`)
```
14:19:09  [TRADE_MGR] MP: State transition | NEW -> SUBMITTED
14:19:09  [fill event, filled_qty=18, status=filled]
14:19:15  [STATE_SYNC] MP: Scalp bracket FILLED @ $63.91
14:19:15  [STATE_SYNC] MP: State transition | SUBMITTED -> PARTIALLY_FILLED (18/18 shares)
14:19:24  [STATE_SYNC] MP: Scalp bracket FILLED @ $63.91        ← no transition
14:19:29  [STATE_SYNC] MP: Scalp bracket FILLED @ $63.91        ← no transition
...
14:20:57  [TIMEOUT] MP: Entry timeout (45s) - checking fill status
14:20:57  [TIMEOUT] MP: PARTIAL FILL on timeout | filled_qty=18/18 avg_fill=$63.91 | FLATTENING IMMEDIATELY
```

The fourth line is the bug in miniature: intent is tagged `PARTIALLY_FILLED` even
though 18 of 18 ordered shares have filled. The timeout handler's predicate
checks the *intent state*, not `filled_qty vs. ordered_qty`, so it takes the
flatten branch.

**Root cause** — two linked defects:
1. `STATE_SYNC` detects the fill every tick but only transitions once and lands
   on `PARTIALLY_FILLED` — the state machine lacks a `PARTIALLY_FILLED → FILLED`
   edge when `filled_qty >= ordered_qty`.
2. `check_entry_timeout()` branches on state alone and treats
   `PARTIALLY_FILLED` at the 45 s mark as "scratch the remainder". It never
   consults the qty fields that are sitting in the same log line.

**Proposed fix** (belt + suspenders)
1. In the state-sync handler (see `strategies/simple_bot.py`, grep
   `"Scalp bracket FILLED"`): once `filled_qty >= ordered_qty`, promote
   `PARTIALLY_FILLED → FILLED` (or whatever the OPEN-position terminal state is
   named) so downstream timeouts no longer see partial.
2. In `check_entry_timeout()`: before invoking the flatten path, re-read
   `filled_qty` / `ordered_qty` from the live order; if the order is fully
   filled, **promote state and return** rather than flattening. Log a WARNING
   with the two quantities so we can audit every case it catches.
3. Either guard alone fixes MP; shipping both makes the system robust to the
   other failing in the future.

**Test plan**
- Unit: construct a TradeIntent, inject a sequence of Alpaca trade-update
  messages (pending → new → fill@full_qty), advance simulated clock past the
  45 s timeout, assert state == FILLED/OPEN, assert no cancel/flatten issued.
- Unit: same sequence but with a true partial (e.g. 12/18 filled), assert the
  timeout DOES flatten (don't regress the intended safety path).
- Regression replay: point a test harness at the 2026-04-20 MP trade update
  stream; verify the patched code keeps the position open.

**Files likely touched**
- `strategies/simple_bot.py` — `check_entry_timeout`, `STATE_SYNC` handler,
  TradeIntent state machine.
- `strategies/adapters/simple_adapter.py` — only if the fix belongs in the
  adapter layer. Prefer fixing in `simple_bot.py` so paper and live both benefit.

**Blast radius** — SIMPLE only. TREND/CROSSASSET unaffected. Cosmetic risk:
over-promoting state on a race between fill event and order update; mitigate by
re-reading the order object (not cached state) inside the guard.

---

## Patch 9 — Ownership ledger: pending→filled transition reconciler

**Status**: SHIPPED 2026-04-21 via config-only fix (Option A). The original
websocket proposal was oversized — the core reconciler had already gained a
pending-resolution block between 2026-04-15 (when this entry was written) and
2026-04-17 promotion, so the remaining gap was purely *latency*, not a missing
code path.

**What was actually happening (2026-04-21 re-analysis)**

`reconciler.py` (the loop at lines 244–279) already walks every `pending`
ledger entry on each cycle, calls `broker.get_order_by_client_id(coid)`, and
promotes to `filled` / `partially_filled` / `cancelled` based on Alpaca's
authoritative REST response. The *only* exposure was that this ran every
**5 minutes** (`reconcile_interval_sec=300.0`) so `get_filled_entries()` /
`get_deployed_notional()` / `count_active_positions()` could undercount fills
for up to 5 minutes after submission. `is_active` still returned True during
that window (pending counts as active), so the ownership filter and
`is_symbol_owned_by_other()` were always correct — the risk was sizing
undercounts, not double-ownership.

**Fix shipped**: lowered `reconcile_interval_sec` from `300.0` → `60.0` in
`strategies/engine/config.py`. Cuts worst-case staleness by 5×. Cost is
~2 extra REST calls/min + N_pending `get_order_by_client_id` calls; trivial
versus Alpaca's 200/min rate limit.

**Not shipped (deferred)**: the websocket/TradingStream proposal. Introducing
a new background thread + reconnect lifecycle in `broker.py` is a lot of
surface area for a problem the 1-min cadence already addresses. Revisit only
if we observe a real sizing miss in sleeve-allocation logs — grep for
allocation decisions that re-submitted because `count_active_positions` read
low, or a `get_deployed_notional` that lagged actual deployment.

**Files touched**: `strategies/engine/config.py` (one line). No changes to
`broker.py`, `ownership.py`, or `reconciler.py`.

---

## Patch 10 — TREND correlation cap

**Trigger**: SMH + SOXX + SOXL all in TREND sleeve concurrently during 2026-04-13
rebalance. Three semi-conductor ETFs is effectively one position.

**Proposed fix**: in `strategies/trend_bot.py` rebalance builder, cluster
candidates by ETF category (semis / energy / rates / broad equity …) and cap
aggregate sleeve weight per cluster at e.g. 35%.

**Priority**: lower than 8/9 — this is a sizing improvement, not a bug. Revisit
after the first live TREND rebalance (Friday 2026-04-24) so the fix is informed
by real live behavior, not paper.

---

## Patch 11 — Ownership ledger `notional_at_entry` fallback corrupts sleeve allocation

**Status**: DRAFTED 2026-04-27 — code patched in working tree, pending pytest +
Railway deploy + ledger backfill. Time-sensitive: must land before the
2026-04-29 CROSSASSET rebalance.

**Trigger event** — daily review, 2026-04-27 post-close (Day 6 live)

Engine tick log was reporting wildly wrong sleeve allocations:

```
[ENGINE] tick=60 | equity=$7,357 | TREND: $414/$4,782 (9%, 2pos)
                                  | SIMPLE: $0/$1,471 (0%, 0pos)
                                  | CROSSASSET: $2,009/$883 (228%, 4pos)
```

Ground truth from Alpaca live positions (TREND owns SMH+SOXX, CROSSASSET owns
DBA+DBC+TBT+USO):

| Sleeve | Engine reported | Actual market value | Target |
|---|---|---|---|
| TREND | $414 (9%) | $1,993 (27.1%) | $4,785 (65%) |
| CROSSASSET | $2,009 (228%) | $664 (9.0%) | $883 (12%) |
| SIMPLE | $0 (0%) | $0 (0%) | $1,472 (20%) |

If Wednesday's CROSSASSET rebalance had run on these numbers, it would have
dumped nearly the entire commodity basket trying to reduce a phantom 228%
allocation back to 12%. Real allocation is just under-target, not over.

**Symptom** — `engine_ownership_live.json` had a uniform pattern across every
TREND and CROSSASSET filled entry: `notional_at_entry == fill_qty * 100`,
not `fill_qty * fill_price`. The SIMPLE entry (AMZU) was correct.

```
SMH:  fill_qty=2.121, fill_price=$505.83  → notional=$212.10  (should be $1,073)
SOXX: fill_qty=2.022, fill_price=$459.92  → notional=$202.17  (should be $930)
DBA:  fill_qty=8.090, fill_price=$27.29   → notional=$809.04  (should be $221)
DBC:  fill_qty=5.169, fill_price=$29.38   → notional=$516.86  (should be $152)
TBT:  fill_qty=6.357, fill_price=$34.73   → notional=$635.74  (should be $221)
USO:  fill_qty=0.478, fill_price=$127.78  → notional=$47.84   (should be $61)
AMZU: fill_qty=28.0,  fill_price=$41.83   → notional=$1,171.52 (correct)
```

The engine's `get_deployed_notional(strategy_id)` (`ownership.py:188-190`) sums
`notional_at_entry` across filled entries and that sum drives the sleeve %
shown in tick logs. So the bug surfaces as catastrophically wrong allocation
reporting.

**Root cause** — adapter pre-submit notional estimate has a `qty * 100`
fallback that fires for new positions:

```python
# strategies/adapters/trend_adapter.py:328-336 (and identical in cross_asset_adapter.py:284-291)
notional = 0.0
try:
    pos = self._trading.get_open_position(symbol)
    price = float(pos.current_price)
    notional = qty * price
except Exception:
    notional = qty * 100  # conservative fallback
```

On a rebalance opening a *fresh* position, `get_open_position()` raises
because no position exists yet, so the bare `qty * 100` fallback fires. That
estimate is what gets persisted in `register_order(notional=...)`.

The `100` is a stale placeholder — likely intended as an arbitrary positive
default for the pre-submit `validate_order()` sleeve-budget check. It works
acceptably for that ephemeral check (worst-case it underestimates and lets a
slightly too-large order through), but persisting it in the ledger as
`notional_at_entry` was unintended.

The SIMPLE adapter avoids the bug because the SIMPLE/scalp pathway passes
`limit_price` into `submit_order`, so `price` is populated and the fallback
is skipped. AMZU (entered via that path) was correct.

**Fix shipped to working tree (not yet deployed)**

1. `strategies/engine/ownership.py` — `update_status()` now accepts a
   `notional` kwarg and writes through to `entry.notional_at_entry`. Pure
   additive change; existing callers unaffected.
2. `strategies/adapters/trend_adapter.py` — the existing post-fill
   `update_status` call (line 414) now passes
   `notional=qty * _order_price`. `_order_price` is sourced from
   `result.filled_avg_price` (preferred) or `get_open_position().current_price`
   (fallback) — both reflect the actual fill, not the pre-submit estimate.
3. `strategies/adapters/cross_asset_adapter.py` — added a post-submit price
   lookup mirroring trend's pattern, then a `update_status(..., notional=...)`
   call to overwrite the placeholder. CROSSASSET previously relied on the
   reconciler to advance pending → filled; with this patch it advances
   immediately like TREND. Reconciler will run as a no-op confirm.
4. `strategies/engine/backfill_ledger_notional.py` (new) — one-shot script
   that walks `engine_ownership_live.json`, recomputes
   `notional_at_entry = fill_qty * fill_price` for filled entries, and writes
   back with a timestamped `.bak`. Supports `--dry-run`, `--path`,
   `--tolerance`. Required because the patched code only fixes new orders;
   the six currently-open entries need a one-time correction.

**Pre-submit estimate left as-is.** The `qty * 100` is still used to feed
`validate_order()` for sleeve-budget gating. In theory this could let a
too-large order through (estimate $200 vs real $1,000) — but in practice
TREND's 65% sleeve has plenty of headroom, and Alpaca's buying-power check
gates the order at the broker level. If we ever see a sleeve-overrun
incident, revisit by replacing the fallback with a Polygon snapshot or
Alpaca latest-trade call.

**Backfill validation** — dry-run against the live ledger produces the
expected sleeve totals:

```
TREND:      old=$414.27   -> new=$2,002.71   (matches market value $1,993)
CROSSASSET: old=$2,009.48 -> new=$654.49     (matches market value $664)
```

Small residual gap is just price drift since fill — `notional_at_entry` is
cost basis, which is the correct semantic for sleeve sizing.

**Test plan**

- Unit: extend `test_trend_adapter_patch7.py` (or add a new file) with a
  test that submits a fresh BUY for a symbol with no prior position, asserts
  the resulting ledger entry's `notional_at_entry` equals `fill_qty * fill_price`,
  not `fill_qty * 100`.
- Mirror the same test in a `test_cross_asset_adapter` file (does not yet
  exist — first test for that adapter).
- Regression: `test_ownership_update_status` should add a case asserting
  the new `notional` kwarg overwrites `notional_at_entry` and is opt-in
  (None → no change).
- Smoke test post-deploy: tail one engine tick after the next TREND or
  CROSSASSET rebalance, confirm reported sleeve % matches Alpaca market
  value within ~5pp drift.

**Deploy sequence (user-driven)**

1. `pytest strategies/adapters/` locally to confirm nothing broke.
2. Commit + push to trigger Railway deploy.
3. `railway ssh "python3 /app/strategies/engine/backfill_ledger_notional.py --dry-run"`
   to preview, then re-run without `--dry-run`.
4. Restart engine (or wait for hot-reload if wired).
5. Verify next `[ENGINE] tick=...` line shows TREND ~27% and CROSSASSET ~9%.

**Blast radius** — sleeve allocation reporting + sleeve drift sizing for
TREND and CROSSASSET. SIMPLE unaffected (entered via correct codepath).
Worst-case if patch is wrong: rebalance behavior identical to current state
(broken). The backfill script is reversible via the `.bak` file it writes.

**Files touched**
- `strategies/engine/ownership.py` (added `notional` kwarg to `update_status`)
- `strategies/adapters/trend_adapter.py` (added kwarg to existing call)
- `strategies/adapters/cross_asset_adapter.py` (added post-submit price lookup
  + `update_status` call)
- `strategies/engine/backfill_ledger_notional.py` (new)

**Follow-up** — separately investigate why SIMPLE has produced zero entries
in 6 trading days despite scanner consistently surfacing 5+ qualifying
tickers (scores 53–63, RVOL 1.5–6×). May be unrelated, but the broken
deployed-notional reporting could plausibly have made SIMPLE's gating logic
read state incorrectly. Re-evaluate once Patch 11 is live and the ledger
shows correct numbers.

---

## Watch items (not yet a patch)

- **Tradestie Reddit source 404** (since 2026-04-18 intermittent). If persistent
  past the 2026-04-22 daily review, either (a) swap source or (b) drop its
  weight to 0 in the composite. Not urgent — falls back cleanly.
- **Margin enable** once equity > ~$10k. Requires re-checking
  `daytrading_buying_power` logic and SIMPLE probation math.
