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

**Trigger**: flagged in the 2026-04-15 post-mortem. No production miss yet on
live (promoted 2026-04-17), but the race is latent in the ledger.

**Symptom**: entries created at order submission stay `status="pending"` in the
ledger even after Alpaca reports the fill. `is_active` still returns True so the
ownership filter keeps working, but the audit trail is wrong and
`is_symbol_owned_by_other()` can't distinguish "pending that may cancel" from
"real position".

**Proposed fix**: subscribe ledger to the trade-updates stream in
`strategies/engine/broker.py` and call `ledger.update_status(client_order_id,
"filled" | "partially_filled" | "closed" | "canceled")` off the authoritative
Alpaca events instead of inferring from initial submission.

**Blocked by**: nothing. Safe to ship after Patch 8 lands; same general area.

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

## Watch items (not yet a patch)

- **Tradestie Reddit source 404** (since 2026-04-18 intermittent). If persistent
  past the 2026-04-22 daily review, either (a) swap source or (b) drop its
  weight to 0 in the composite. Not urgent — falls back cleanly.
- **Margin enable** once equity > ~$10k. Requires re-checking
  `daytrading_buying_power` logic and SIMPLE probation math.
