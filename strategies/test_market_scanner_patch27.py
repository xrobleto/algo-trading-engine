"""
Regression tests for Patch 27 — decouple news enrichment from the timed scan.

Before: `_score_candidates` (which runs inside the SIMPLE adapter's scan
timeout) called `_check_catalyst` for the top 15 candidates, each doing
`time.sleep(0.5)` + a synchronous Polygon news GET. That's a 7.5s sleep floor
alone, on top of two full-market snapshot fetches — so the scan routinely blew
the 10s timeout and SIMPLE produced zero candidates (133 timeouts logged).

After: the hot path reads news cache-only (no sleep, no network) and hands
cache-misses to a single background warmer thread. News stays a score bonus
without ever stalling a scan.

These tests pin down:
  1. `_check_catalyst(allow_fetch=False)` never hits the network (hot path).
  2. A fresh cache hit is returned on the hot path.
  3. A stale cache entry is treated as a miss on the hot path (still no fetch).
  4. `_news_is_fresh` honors the 30-min TTL.
  5. The background warmer populates the cache off the hot path.
  6. `_score_candidates` makes no synchronous news call.

Run:
    python strategies/test_market_scanner_patch27.py
    pytest strategies/test_market_scanner_patch27.py
"""

from __future__ import annotations

import sys
import time
from datetime import datetime, timezone
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from strategies import market_scanner as ms_mod
from strategies.market_scanner import MarketScanner, ScanCandidate, NEWS_CACHE_MINUTES


def _scanner():
    # PolygonAPI.__init__ just stores the key — no network on construction.
    return MarketScanner("test-key")


def test_hot_path_never_fetches_on_cache_miss():
    sc = _scanner()
    calls = []
    sc.api.get_news = lambda *a, **k: calls.append(1) or []
    has_news, headline = sc._check_catalyst("FOO", allow_fetch=False)
    assert has_news is False and headline == ""
    assert calls == [], "hot path must not call the news API"


def test_hot_path_returns_fresh_cache():
    sc = _scanner()
    sc.api.get_news = lambda *a, **k: (_ for _ in ()).throw(AssertionError("should not fetch"))
    sc._news_cache["BAR"] = (time.time(), True, "Big catalyst")
    has_news, headline = sc._check_catalyst("BAR", allow_fetch=False)
    assert has_news is True and headline == "Big catalyst"


def test_stale_cache_is_miss_without_fetch():
    sc = _scanner()
    calls = []
    sc.api.get_news = lambda *a, **k: calls.append(1) or []
    sc._news_cache["OLD"] = (time.time() - (NEWS_CACHE_MINUTES * 60 + 10), True, "stale")
    has_news, headline = sc._check_catalyst("OLD", allow_fetch=False)
    assert has_news is False and headline == ""
    assert calls == []


def test_news_is_fresh():
    sc = _scanner()
    assert sc._news_is_fresh("MISSING") is False
    sc._news_cache["FRESH"] = (time.time(), False, "")
    assert sc._news_is_fresh("FRESH") is True
    sc._news_cache["STALE"] = (time.time() - (NEWS_CACHE_MINUTES * 60 + 5), True, "x")
    assert sc._news_is_fresh("STALE") is False


def test_warmer_populates_cache_off_hot_path():
    saved_delay = ms_mod.API_RATE_LIMIT_DELAY
    try:
        ms_mod.API_RATE_LIMIT_DELAY = 0  # don't actually sleep in the test warmer
        sc = _scanner()
        now_iso = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        sc.api.get_news = lambda symbol, limit=5: [{"published_utc": now_iso, "title": "Catalyst!"}]

        sc._warm_news_async(["FOO"])

        # Wait for the background warmer to finish (bounded).
        deadline = time.time() + 3.0
        while sc._news_warming and time.time() < deadline:
            time.sleep(0.01)

        assert "FOO" in sc._news_cache, "warmer should populate the cache"
        _ts, has_news, headline = sc._news_cache["FOO"]
        assert has_news is True and headline == "Catalyst!"
    finally:
        ms_mod.API_RATE_LIMIT_DELAY = saved_delay


def test_score_candidates_makes_no_synchronous_news_call():
    sc = _scanner()
    # Pretend a warmer is already running so _score_candidates won't spawn one,
    # isolating the synchronous hot-path behavior.
    sc._news_warming = True
    sc.api.get_news = lambda *a, **k: (_ for _ in ()).throw(AssertionError("synchronous fetch!"))
    sc.scorer.score = lambda *a, **k: 50.0
    sc.vol_tracker.get_acceleration = lambda s: 0.0

    cand = ScanCandidate(
        symbol="FOO", price=10.0, prev_close=9.0, change_pct=11.1,
        day_volume=1_000_000, prev_day_volume=400_000, rvol=2.5, dollar_volume=10_000_000,
    )
    watchlist = sc._score_candidates([cand])
    assert len(watchlist) == 1
    assert watchlist[0].symbol == "FOO"
    assert watchlist[0].has_catalyst is False  # no cached news → no bonus, no fetch


if __name__ == "__main__":
    tests = [
        test_hot_path_never_fetches_on_cache_miss,
        test_hot_path_returns_fresh_cache,
        test_stale_cache_is_miss_without_fetch,
        test_news_is_fresh,
        test_warmer_populates_cache_off_hot_path,
        test_score_candidates_makes_no_synchronous_news_call,
    ]
    failed = 0
    for t in tests:
        try:
            t()
            print(f"  PASS  {t.__name__}")
        except Exception as e:  # noqa: BLE001
            failed += 1
            print(f"  FAIL  {t.__name__}: {e}")
    print(f"\n{len(tests) - failed}/{len(tests)} passed")
    sys.exit(1 if failed else 0)
