"""Polygon.io REST client.

Horizon only needs daily aggregates and cash dividends — everything else
(intraday, options, news) is excluded from the validated engine per DESIGN.md
section 3. The client retries transient errors and fails fast on 4xx.
"""

from __future__ import annotations

import time
from typing import Optional

import pandas as pd
import requests

from ..paths import require_secret

POLYGON_BASE = "https://api.polygon.io"


class PolygonClient:
    def __init__(self, api_key: Optional[str] = None, timeout: float = 30.0,
                 max_retries: int = 4):
        self.api_key = api_key or require_secret("POLYGON_API_KEY")
        self.timeout = timeout
        self.max_retries = max_retries
        self._session = requests.Session()

    def _get(self, path: str, params: Optional[dict] = None) -> dict:
        params = dict(params or {})
        params["apiKey"] = self.api_key
        url = POLYGON_BASE + path
        delay = 1.0
        for attempt in range(self.max_retries):
            try:
                resp = self._session.get(url, params=params, timeout=self.timeout)
            except requests.RequestException:
                if attempt == self.max_retries - 1:
                    raise
                time.sleep(delay)
                delay *= 2
                continue
            if resp.status_code == 200:
                return resp.json()
            if resp.status_code == 429 or resp.status_code >= 500:
                if attempt == self.max_retries - 1:
                    resp.raise_for_status()
                time.sleep(delay)
                delay *= 2
                continue
            # Other 4xx — not authorized, bad request, etc. Fail fast.
            resp.raise_for_status()
        raise RuntimeError("unreachable")

    def daily_bars(self, symbol: str, start: str, end: str) -> pd.DataFrame:
        """Split-adjusted daily OHLCV. `symbol` may be an index like 'I:VIX'."""
        path = f"/v2/aggs/ticker/{symbol}/range/1/day/{start}/{end}"
        data = self._get(path, {"adjusted": "true", "sort": "asc",
                                "limit": 50000})
        results = data.get("results") or []
        if not results:
            return pd.DataFrame(
                columns=["open", "high", "low", "close", "volume"]
            ).rename_axis("date")
        rows = []
        for r in results:
            ts = (pd.to_datetime(r["t"], unit="ms", utc=True)
                  .tz_convert("America/New_York").normalize().tz_localize(None))
            rows.append({
                "date": ts,
                "open": float(r["o"]), "high": float(r["h"]),
                "low": float(r["l"]), "close": float(r["c"]),
                "volume": float(r.get("v") or 0.0),
            })
        df = pd.DataFrame(rows).set_index("date").sort_index()
        return df[~df.index.duplicated(keep="last")]

    def dividends(self, symbol: str) -> pd.DataFrame:
        """Cash dividends keyed by ex-dividend date. Empty for index symbols."""
        if symbol.startswith("I:"):
            return pd.DataFrame(columns=["dividend"]).rename_axis("date")
        data = self._get("/v3/reference/dividends",
                         {"ticker": symbol, "limit": 1000})
        rows = []
        for r in data.get("results") or []:
            ex = r.get("ex_dividend_date")
            amt = r.get("cash_amount")
            if ex and amt:
                rows.append({"date": pd.to_datetime(ex), "dividend": float(amt)})
        if not rows:
            return pd.DataFrame(columns=["dividend"]).rename_axis("date")
        df = pd.DataFrame(rows).set_index("date").sort_index()
        return df.groupby(level=0)["dividend"].sum().to_frame()
