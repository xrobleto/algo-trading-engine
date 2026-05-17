# Horizon Data Pipeline

## Sources

Horizon's validated engine uses **one** data source: **Polygon.io** (daily
aggregates, dividends, indices). The Polygon key is read from `horizon/.env` (or
the project's `config/*.env` as a fallback) — never hardcoded.

The live engine additionally uses **Alpaca** for execution and account state.

`alphatraderdaily.com` (the user's market-intelligence platform) was studied as
instructed. It is a thin caching layer over the same upstreams (Polygon,
Finnhub, FINRA, ApeWisdom, Polymarket) and stores no historical market-data
warehouse. Horizon therefore goes **directly to Polygon**, replicating the
derivation methods rather than depending on ATD's uptime. ATD's database holds
nothing Horizon needs.

## Ingestion (`data/`)

- `polygon_client.py` — REST client. Daily aggregates (`adjusted=true`,
  `limit=50000` — one call covers 22 years) and cash dividends. Retries
  transient errors; fails fast on 4xx.
- `cache.py` — one pickle per symbol under `data/cache/`. Each frame carries
  split-adjusted OHLCV, a `dividend` column (cash on its ex-date), and
  `tr_close` — a **total-return index**. Total return matters: BIL and TLT earn
  most of their return as dividends, so a price-only backtest of them would be
  badly wrong.
- `calendar.py` — trading days are derived from the data itself (Polygon emits
  a bar only on trading days), so the calendar cannot drift from the prices.
- `universe.py` — the fixed, liquid ETF universe (~20 symbols). ETF
  survivorship bias is negligible.

### Ticker stitching

The Nasdaq-100 ETF traded as `QQQQ` from 2004-12 to 2011-03. `cache.py` stitches
`QQQ` + `QQQQ` into one continuous series (verified: clean price join at the
2011-03-22→23 boundary, 5,525 continuous bars).

## What is backtestable vs. what is not

This distinction drove the entire design (DESIGN.md section 3). A strategy that
cannot be faithfully backtested cannot be validated and cannot enter the engine.

**Deeply backtestable (used by the validated engine):**
- Daily price / volume / OHLCV for ETFs and indices, ~2004 → present.
- Total-return series (price + reinvested dividends).
- Realized volatility, trend, breadth, credit-spread proxies.

**NOT backtestable — excluded from the validated engine:**
- **Options chains / flow / gamma** — Polygon has live option snapshots but no
  usable historical option-chain depth (contract aggregates 403 beyond ~2yr).
- **Reddit / social sentiment** (ApeWisdom) — live snapshot only, no history.
- **Prediction markets** (Polymarket) — live only.
- **Index VIX history** — Polygon's `I:VIX` only reaches ~2023 on this plan;
  the volatility regime instead uses SPY realized volatility (deep history,
  also computable live).

These are candidate **live-only enrichments** for future work — they may inform
the live engine, but they are never load-bearing in a validated strategy,
because a walk-forward test of something with no history is impossible.

## Cost model (`backtest/costs.py`)

- Slippage: 2 bps on broad ETFs (SPY/QQQ-class), 5 bps on sector ETFs, 4 bps
  per leg for DRIFT's auction fills.
- Commissions: $0 (Alpaca equities).
- Borrow: 6.5%/yr on leveraged dollars, debited daily.
- Account realism: the real $7,400 size, fractional shares, a $1 minimum order,
  idle cash earning 0% (strategies hold BIL explicitly for T-bill yield).
