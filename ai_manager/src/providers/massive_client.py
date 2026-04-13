"""
Polygon (Massive) API Client

Provides market data, reference data, and news from Polygon.io.
Implements rate limiting, caching, and graceful degradation.
"""

import os
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Any

import requests

from ..utils.logging import get_logger
from ..utils.time import now_utc, now_et, format_timestamp, ET
from ..utils.retry import retry_with_backoff, RateLimiter, TTLCache
from ..utils.typing import NewsItem, NewsSentiment, CatalystType

logger = get_logger(__name__)


# Sentiment keywords for classification
POSITIVE_KEYWORDS = [
    "surge", "soar", "jump", "rally", "gain", "rise", "beat", "exceed",
    "strong", "bullish", "upgrade", "buy", "outperform", "growth",
    "profit", "revenue beat", "record", "breakthrough", "approval",
    "partnership", "acquisition", "deal", "contract"
]

NEGATIVE_KEYWORDS = [
    "fall", "drop", "plunge", "crash", "decline", "miss", "weak",
    "bearish", "downgrade", "sell", "underperform", "loss", "layoff",
    "lawsuit", "investigation", "recall", "warning", "fraud",
    "bankruptcy", "default", "delisting", "offering", "dilution"
]

# Catalyst type keywords
CATALYST_KEYWORDS = {
    CatalystType.EARNINGS: ["earnings", "eps", "revenue", "quarterly", "q1", "q2", "q3", "q4", "results"],
    CatalystType.GUIDANCE: ["guidance", "outlook", "forecast", "projection"],
    CatalystType.FDA: ["fda", "approval", "clinical", "trial", "drug"],
    CatalystType.ACQUISITION: ["acquisition", "acquire", "buyout", "takeover"],
    CatalystType.MERGER: ["merger", "merge", "combination"],
    CatalystType.PARTNERSHIP: ["partnership", "collaboration", "agreement", "deal"],
    CatalystType.CONTRACT: ["contract", "award", "government", "defense"],
    CatalystType.LAWSUIT: ["lawsuit", "litigation", "sued", "legal"],
    CatalystType.REGULATORY: ["regulatory", "sec", "investigation", "probe", "fine"],
    CatalystType.PRODUCT: ["launch", "product", "release", "unveil", "announce"],
}


class MassiveClient:
    """
    Client for Polygon.io (Massive) API.

    Features:
        - Rate limiting (configurable calls per minute)
        - Response caching with TTL
        - Retry with exponential backoff
        - Graceful degradation on errors
    """

    BASE_URL = "https://api.polygon.io"

    def __init__(
        self,
        api_key: Optional[str] = None,
        rate_limit_per_min: int = 5,
        cache_ttl_seconds: int = 300
    ):
        """
        Initialize Polygon client.

        Args:
            api_key: Polygon API key (or from POLYGON_API_KEY env var)
            rate_limit_per_min: Max API calls per minute
            cache_ttl_seconds: Cache TTL (default 5 minutes)
        """
        self.api_key = api_key or os.environ.get("POLYGON_API_KEY")
        if not self.api_key:
            logger.warning("No Polygon API key configured - client will be disabled")

        self.rate_limiter = RateLimiter(rate_limit_per_min, name="polygon")
        self.cache = TTLCache(cache_ttl_seconds)
        self.last_successful_call: Optional[datetime] = None
        self._enabled = bool(self.api_key)

    @property
    def is_available(self) -> bool:
        """Check if client is available and configured."""
        return self._enabled

    def _make_request(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        cache_key: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Make an API request with rate limiting and caching.

        Args:
            endpoint: API endpoint path
            params: Query parameters
            cache_key: Optional cache key (None = don't cache)

        Returns:
            JSON response or None on error
        """
        if not self._enabled:
            return None

        # Check cache first
        if cache_key:
            cached = self.cache.get(cache_key)
            if cached is not None:
                logger.debug(f"Cache hit for {cache_key}")
                return cached

        # Rate limit
        self.rate_limiter.acquire()

        url = f"{self.BASE_URL}{endpoint}"
        params = params or {}
        params["apiKey"] = self.api_key

        try:
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()

            data = response.json()
            self.last_successful_call = now_utc()

            # Cache successful response
            if cache_key:
                self.cache.set(cache_key, data)

            return data

        except requests.exceptions.HTTPError as e:
            if response.status_code == 429:
                logger.warning("Polygon rate limit exceeded")
            elif response.status_code == 403:
                logger.error("Polygon API key invalid or insufficient permissions")
                self._enabled = False
            else:
                logger.error(f"Polygon HTTP error: {e}")
            return None

        except requests.exceptions.RequestException as e:
            logger.error(f"Polygon request failed: {e}")
            return None

    # =========================================================
    # QUOTES & PRICES
    # =========================================================

    def get_snapshot(self, symbol: str) -> Optional[Dict[str, Any]]:
        """
        Get real-time snapshot for a ticker.

        Args:
            symbol: Stock symbol

        Returns:
            Dict with price, volume, etc. or None
        """
        cache_key = f"snapshot:{symbol}"
        data = self._make_request(
            f"/v2/snapshot/locale/us/markets/stocks/tickers/{symbol}",
            cache_key=cache_key
        )

        if not data or "ticker" not in data:
            return None

        ticker = data["ticker"]
        day = ticker.get("day", {})
        prev_day = ticker.get("prevDay", {})
        last_trade = ticker.get("lastTrade", {})
        last_quote = ticker.get("lastQuote", {})

        # Use best available price: today's close > last trade > prev close (for pre-market)
        price = day.get("c") or last_trade.get("p") or prev_day.get("c")

        return {
            "symbol": symbol,
            "price": price,
            "open": day.get("o"),
            "high": day.get("h"),
            "low": day.get("l"),
            "close": day.get("c"),
            "volume": day.get("v"),
            "vwap": day.get("vw"),
            "prev_close": prev_day.get("c"),
            "prev_volume": prev_day.get("v"),
            "change": (price - prev_day.get("c", 0)) if price and prev_day.get("c") else None,
            "change_pct": ((price - prev_day.get("c", 1)) / prev_day.get("c", 1)) * 100
                          if price and prev_day.get("c") else None,
            "bid": last_quote.get("p") or last_quote.get("P"),
            "ask": last_quote.get("P") or last_quote.get("p"),
            "timestamp": now_utc().isoformat(),
        }

    def get_multiple_snapshots(self, symbols: List[str]) -> Dict[str, Dict[str, Any]]:
        """
        Get snapshots for multiple tickers.

        Args:
            symbols: List of stock symbols

        Returns:
            Dict mapping symbol to snapshot data
        """
        # Polygon has a tickers endpoint for multiple symbols
        # Note: This endpoint requires a paid plan. Fall back to individual requests if it fails.
        cache_key = f"snapshots:{','.join(sorted(symbols[:20]))}"  # Only cache first 20 for key
        data = self._make_request(
            "/v2/snapshot/locale/us/markets/stocks/tickers",
            params={"tickers": ",".join(symbols)},
            cache_key=cache_key
        )

        if not data or "tickers" not in data:
            # Fall back to individual requests
            logger.warning(f"Multi-ticker snapshot failed or empty, falling back to individual requests for {len(symbols)} symbols")
            result = {}
            for symbol in symbols:
                snapshot = self.get_snapshot(symbol)
                if snapshot and snapshot.get("price"):
                    result[symbol] = snapshot
            logger.info(f"Got {len(result)}/{len(symbols)} prices via individual requests")
            return result

        logger.info(f"Multi-ticker snapshot returned {len(data.get('tickers', []))} tickers")

        result = {}
        for ticker in data.get("tickers", []):
            symbol = ticker.get("ticker")
            if symbol:
                day = ticker.get("day", {})
                prev_day = ticker.get("prevDay", {})
                last_trade = ticker.get("lastTrade", {})

                # Use best available price: today's close > last trade > prev close
                price = day.get("c") or last_trade.get("p") or prev_day.get("c")

                result[symbol] = {
                    "symbol": symbol,
                    "price": price,
                    "open": day.get("o"),
                    "high": day.get("h"),
                    "low": day.get("l"),
                    "volume": day.get("v"),
                    "prev_close": prev_day.get("c"),
                    "change_pct": ((price - prev_day.get("c", 1)) / prev_day.get("c", 1)) * 100
                                  if price and prev_day.get("c") else None,
                    "timestamp": now_utc().isoformat(),
                }

        prices_found = sum(1 for v in result.values() if v.get("price"))
        logger.info(f"Extracted {prices_found}/{len(result)} prices from multi-ticker response")
        return result

    # =========================================================
    # HISTORICAL BARS
    # =========================================================

    @retry_with_backoff(max_retries=2, base_delay=1.0, exceptions=(requests.exceptions.RequestException,))
    def get_bars(
        self,
        symbol: str,
        timespan: str = "day",
        multiplier: int = 1,
        from_date: Optional[date] = None,
        to_date: Optional[date] = None,
        limit: int = 100
    ) -> Optional[List[Dict[str, Any]]]:
        """
        Get historical price bars.

        Args:
            symbol: Stock symbol
            timespan: minute, hour, day, week, month
            multiplier: Size of timespan (e.g., 5 for 5-minute bars)
            from_date: Start date
            to_date: End date
            limit: Max number of bars

        Returns:
            List of bar dictionaries or None
        """
        if to_date is None:
            to_date = now_et().date()
        if from_date is None:
            from_date = to_date - timedelta(days=365)

        cache_key = f"bars:{symbol}:{timespan}:{multiplier}:{from_date}:{to_date}"

        data = self._make_request(
            f"/v2/aggs/ticker/{symbol}/range/{multiplier}/{timespan}/{from_date}/{to_date}",
            params={"adjusted": "true", "sort": "asc", "limit": limit},
            cache_key=cache_key
        )

        if not data or "results" not in data:
            return None

        bars = []
        for r in data["results"]:
            bars.append({
                "timestamp": datetime.fromtimestamp(r["t"] / 1000, tz=ET),
                "open": r.get("o"),
                "high": r.get("h"),
                "low": r.get("l"),
                "close": r.get("c"),
                "volume": r.get("v"),
                "vwap": r.get("vw"),
                "transactions": r.get("n"),
            })

        return bars

    # =========================================================
    # NEWS
    # =========================================================

    def get_news(
        self,
        symbol: Optional[str] = None,
        limit: int = 10,
        published_utc_gte: Optional[str] = None
    ) -> List[NewsItem]:
        """
        Get news articles.

        Args:
            symbol: Filter by ticker symbol
            limit: Max articles to return
            published_utc_gte: Filter to articles after this timestamp

        Returns:
            List of NewsItem objects
        """
        params = {"limit": limit, "order": "desc", "sort": "published_utc"}

        if symbol:
            params["ticker"] = symbol

        if published_utc_gte:
            params["published_utc.gte"] = published_utc_gte

        cache_key = f"news:{symbol}:{limit}:{published_utc_gte}"
        data = self._make_request("/v2/reference/news", params=params, cache_key=cache_key)

        if not data or "results" not in data:
            return []

        news_items = []
        for article in data["results"]:
            title = article.get("title", "")
            published_str = article.get("published_utc", "")

            # Parse published date
            try:
                if published_str.endswith("Z"):
                    published_str = published_str[:-1] + "+00:00"
                published_at = datetime.fromisoformat(published_str)
            except (ValueError, TypeError):
                published_at = now_utc()

            # Analyze sentiment
            sentiment = self._analyze_sentiment(title)

            # Classify catalyst type
            catalyst_type = self._classify_catalyst(title)

            news_items.append(NewsItem(
                title=title,
                url=article.get("article_url", ""),
                published_at=published_at,
                source=article.get("publisher", {}).get("name", "Unknown"),
                tickers=article.get("tickers", []),
                sentiment=sentiment,
                catalyst_type=catalyst_type,
            ))

        return news_items

    def _analyze_sentiment(self, text: str) -> NewsSentiment:
        """Analyze sentiment from text."""
        text_lower = text.lower()

        positive_count = sum(1 for kw in POSITIVE_KEYWORDS if kw in text_lower)
        negative_count = sum(1 for kw in NEGATIVE_KEYWORDS if kw in text_lower)

        if positive_count > negative_count + 1:
            return NewsSentiment.VERY_POSITIVE if positive_count >= 3 else NewsSentiment.POSITIVE
        elif negative_count > positive_count + 1:
            return NewsSentiment.VERY_NEGATIVE if negative_count >= 3 else NewsSentiment.NEGATIVE
        else:
            return NewsSentiment.NEUTRAL

    def _classify_catalyst(self, text: str) -> CatalystType:
        """Classify catalyst type from text."""
        text_lower = text.lower()

        for catalyst_type, keywords in CATALYST_KEYWORDS.items():
            for keyword in keywords:
                if keyword in text_lower:
                    return catalyst_type

        return CatalystType.UNKNOWN

    # =========================================================
    # REFERENCE DATA
    # =========================================================

    def get_ticker_details(self, symbol: str) -> Optional[Dict[str, Any]]:
        """
        Get ticker reference data.

        Args:
            symbol: Stock symbol

        Returns:
            Dict with company info or None
        """
        cache_key = f"ticker_details:{symbol}"
        data = self._make_request(
            f"/v3/reference/tickers/{symbol}",
            cache_key=cache_key
        )

        if not data or "results" not in data:
            return None

        results = data["results"]
        return {
            "symbol": results.get("ticker"),
            "name": results.get("name"),
            "market_cap": results.get("market_cap"),
            "shares_outstanding": results.get("share_class_shares_outstanding"),
            "sector": results.get("sic_description"),
            "exchange": results.get("primary_exchange"),
            "type": results.get("type"),
            "locale": results.get("locale"),
        }

    # =========================================================
    # DIVIDENDS & CORPORATE ACTIONS
    # =========================================================

    def get_dividends(
        self,
        symbol: str,
        ex_dividend_date_gte: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Get dividend information.

        Args:
            symbol: Stock symbol
            ex_dividend_date_gte: Filter to dividends after this date

        Returns:
            List of dividend records
        """
        params = {"ticker": symbol, "limit": 20}
        if ex_dividend_date_gte:
            params["ex_dividend_date.gte"] = ex_dividend_date_gte

        data = self._make_request("/v3/reference/dividends", params=params)

        if not data or "results" not in data:
            return []

        return [
            {
                "ticker": d.get("ticker"),
                "ex_dividend_date": d.get("ex_dividend_date"),
                "pay_date": d.get("pay_date"),
                "cash_amount": d.get("cash_amount"),
                "frequency": d.get("frequency"),
            }
            for d in data["results"]
        ]

    # =========================================================
    # STATUS
    # =========================================================

    def get_status(self) -> Dict[str, Any]:
        """Get client status for monitoring."""
        return {
            "provider": "polygon",
            "enabled": self._enabled,
            "last_successful_call": format_timestamp(self.last_successful_call) if self.last_successful_call else None,
            "rate_limiter_usage": f"{self.rate_limiter.current_usage}/{self.rate_limiter.calls_per_minute}",
            "cache_status": "active",
        }
