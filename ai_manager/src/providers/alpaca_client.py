"""
Alpaca API Client

Provides market data and news from Alpaca Markets.
Complements Polygon for redundancy and additional features.
"""

import os
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Any

from ..utils.logging import get_logger
from ..utils.time import now_utc, now_et, format_timestamp, ET
from ..utils.retry import RateLimiter, TTLCache
from ..utils.typing import NewsItem, NewsSentiment, CatalystType

logger = get_logger(__name__)

# Attempt to import alpaca-py
try:
    from alpaca.data.historical import StockHistoricalDataClient
    from alpaca.data.requests import (
        StockLatestQuoteRequest,
        StockBarsRequest,
        StockSnapshotRequest,
    )
    from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
    from alpaca.data.models import Quote, Bar, Snapshot
    ALPACA_AVAILABLE = True
except ImportError:
    ALPACA_AVAILABLE = False
    logger.warning("alpaca-py not installed - Alpaca client will be disabled")

# News client
try:
    from alpaca.data.historical.news import NewsClient
    from alpaca.data.requests import NewsRequest
    ALPACA_NEWS_AVAILABLE = True
except ImportError:
    ALPACA_NEWS_AVAILABLE = False


# Sentiment keywords (same as Polygon for consistency)
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


class AlpacaClient:
    """
    Client for Alpaca Markets API.

    Features:
        - Market data (quotes, bars, snapshots)
        - News with sentiment analysis
        - Rate limiting and caching
        - Graceful degradation
    """

    def __init__(
        self,
        api_key: Optional[str] = None,
        secret_key: Optional[str] = None,
        paper: bool = True,
        cache_ttl_seconds: int = 300
    ):
        """
        Initialize Alpaca client.

        Args:
            api_key: Alpaca API key (or from env)
            secret_key: Alpaca secret key (or from env)
            paper: Use paper trading environment
            cache_ttl_seconds: Cache TTL
        """
        self.api_key = api_key or os.environ.get("ALPACA_API_KEY")
        self.secret_key = secret_key or os.environ.get("ALPACA_SECRET_KEY")
        self.paper = paper

        self.cache = TTLCache(cache_ttl_seconds)
        self.last_successful_call: Optional[datetime] = None

        self._data_client = None
        self._news_client = None
        self._news_disabled = False  # Set True if 401 received (subscription required)
        self._enabled = False

        if not ALPACA_AVAILABLE:
            logger.warning("Alpaca client disabled - alpaca-py not installed")
            return

        if not self.api_key or not self.secret_key:
            logger.warning("No Alpaca API keys configured - client will be disabled")
            return

        try:
            # Initialize data client (no auth needed for free data)
            self._data_client = StockHistoricalDataClient(
                api_key=self.api_key,
                secret_key=self.secret_key
            )

            # Initialize news client if available
            if ALPACA_NEWS_AVAILABLE:
                self._news_client = NewsClient(
                    api_key=self.api_key,
                    secret_key=self.secret_key
                )

            self._enabled = True
            logger.info("Alpaca client initialized successfully")

        except Exception as e:
            logger.error(f"Failed to initialize Alpaca client: {e}")

    @property
    def is_available(self) -> bool:
        """Check if client is available and configured."""
        return self._enabled and self._data_client is not None

    # =========================================================
    # QUOTES & SNAPSHOTS
    # =========================================================

    def get_latest_quote(self, symbol: str) -> Optional[Dict[str, Any]]:
        """
        Get latest quote for a symbol.

        Args:
            symbol: Stock symbol

        Returns:
            Quote data dict or None
        """
        if not self.is_available:
            return None

        cache_key = f"alpaca_quote:{symbol}"
        cached = self.cache.get(cache_key)
        if cached:
            return cached

        try:
            request = StockLatestQuoteRequest(symbol_or_symbols=symbol)
            response = self._data_client.get_stock_latest_quote(request)

            if symbol not in response:
                return None

            quote = response[symbol]
            result = {
                "symbol": symbol,
                "bid": quote.bid_price,
                "ask": quote.ask_price,
                "bid_size": quote.bid_size,
                "ask_size": quote.ask_size,
                "timestamp": quote.timestamp.isoformat() if quote.timestamp else None,
            }

            self.cache.set(cache_key, result, ttl_seconds=60)  # Short TTL for quotes
            self.last_successful_call = now_utc()
            return result

        except Exception as e:
            logger.error(f"Alpaca get_latest_quote failed for {symbol}: {e}")
            return None

    def get_snapshot(self, symbol: str) -> Optional[Dict[str, Any]]:
        """
        Get snapshot for a symbol.

        Args:
            symbol: Stock symbol

        Returns:
            Snapshot data dict or None
        """
        if not self.is_available:
            return None

        cache_key = f"alpaca_snapshot:{symbol}"
        cached = self.cache.get(cache_key)
        if cached:
            return cached

        try:
            request = StockSnapshotRequest(symbol_or_symbols=symbol)
            response = self._data_client.get_stock_snapshot(request)

            if symbol not in response:
                return None

            snap = response[symbol]
            daily_bar = snap.daily_bar
            prev_daily_bar = snap.previous_daily_bar
            latest_quote = snap.latest_quote
            latest_trade = snap.latest_trade

            price = latest_trade.price if latest_trade else (daily_bar.close if daily_bar else None)
            prev_close = prev_daily_bar.close if prev_daily_bar else None

            result = {
                "symbol": symbol,
                "price": price,
                "open": daily_bar.open if daily_bar else None,
                "high": daily_bar.high if daily_bar else None,
                "low": daily_bar.low if daily_bar else None,
                "close": daily_bar.close if daily_bar else None,
                "volume": daily_bar.volume if daily_bar else None,
                "vwap": daily_bar.vwap if daily_bar else None,
                "prev_close": prev_close,
                "change_pct": ((price - prev_close) / prev_close * 100) if price and prev_close else None,
                "bid": latest_quote.bid_price if latest_quote else None,
                "ask": latest_quote.ask_price if latest_quote else None,
                "timestamp": now_utc().isoformat(),
            }

            self.cache.set(cache_key, result)
            self.last_successful_call = now_utc()
            return result

        except Exception as e:
            logger.error(f"Alpaca get_snapshot failed for {symbol}: {e}")
            return None

    def get_multiple_snapshots(self, symbols: List[str]) -> Dict[str, Dict[str, Any]]:
        """
        Get snapshots for multiple symbols.

        Args:
            symbols: List of stock symbols

        Returns:
            Dict mapping symbol to snapshot data
        """
        if not self.is_available:
            return {}

        try:
            request = StockSnapshotRequest(symbol_or_symbols=symbols)
            response = self._data_client.get_stock_snapshot(request)

            result = {}
            for symbol, snap in response.items():
                daily_bar = snap.daily_bar
                prev_daily_bar = snap.previous_daily_bar
                latest_trade = snap.latest_trade

                price = latest_trade.price if latest_trade else (daily_bar.close if daily_bar else None)
                prev_close = prev_daily_bar.close if prev_daily_bar else None

                result[symbol] = {
                    "symbol": symbol,
                    "price": price,
                    "volume": daily_bar.volume if daily_bar else None,
                    "prev_close": prev_close,
                    "change_pct": ((price - prev_close) / prev_close * 100) if price and prev_close else None,
                    "timestamp": now_utc().isoformat(),
                }

            self.last_successful_call = now_utc()
            return result

        except Exception as e:
            logger.error(f"Alpaca get_multiple_snapshots failed: {e}")
            return {}

    # =========================================================
    # HISTORICAL BARS
    # =========================================================

    def get_bars(
        self,
        symbol: str,
        timeframe: str = "day",
        start: Optional[date] = None,
        end: Optional[date] = None,
        limit: int = 100
    ) -> Optional[List[Dict[str, Any]]]:
        """
        Get historical price bars.

        Args:
            symbol: Stock symbol
            timeframe: "minute", "hour", "day", "week", "month"
            start: Start date
            end: End date
            limit: Max bars to return

        Returns:
            List of bar dicts or None
        """
        if not self.is_available:
            return None

        if end is None:
            end = now_et().date()
        if start is None:
            start = end - timedelta(days=365)

        # Map timeframe string to TimeFrame object
        tf_map = {
            "minute": TimeFrame(1, TimeFrameUnit.Minute),
            "hour": TimeFrame(1, TimeFrameUnit.Hour),
            "day": TimeFrame(1, TimeFrameUnit.Day),
            "week": TimeFrame(1, TimeFrameUnit.Week),
            "month": TimeFrame(1, TimeFrameUnit.Month),
        }

        tf = tf_map.get(timeframe, TimeFrame(1, TimeFrameUnit.Day))

        cache_key = f"alpaca_bars:{symbol}:{timeframe}:{start}:{end}"
        cached = self.cache.get(cache_key)
        if cached:
            return cached

        try:
            request = StockBarsRequest(
                symbol_or_symbols=symbol,
                timeframe=tf,
                start=datetime.combine(start, datetime.min.time()),
                end=datetime.combine(end, datetime.max.time()),
                limit=limit
            )

            response = self._data_client.get_stock_bars(request)

            if symbol not in response:
                return None

            bars = []
            for bar in response[symbol]:
                bars.append({
                    "timestamp": bar.timestamp,
                    "open": bar.open,
                    "high": bar.high,
                    "low": bar.low,
                    "close": bar.close,
                    "volume": bar.volume,
                    "vwap": bar.vwap,
                    "trade_count": bar.trade_count,
                })

            self.cache.set(cache_key, bars)
            self.last_successful_call = now_utc()
            return bars

        except Exception as e:
            logger.error(f"Alpaca get_bars failed for {symbol}: {e}")
            return None

    # =========================================================
    # NEWS
    # =========================================================

    def get_news(
        self,
        symbols: Optional[List[str]] = None,
        limit: int = 10,
        start: Optional[datetime] = None
    ) -> List[NewsItem]:
        """
        Get news articles.

        Args:
            symbols: Filter by ticker symbols
            limit: Max articles to return
            start: Articles published after this time

        Returns:
            List of NewsItem objects
        """
        # Skip if news is disabled due to subscription issues
        if self._news_disabled:
            return []

        if not self._news_client:
            return []

        try:
            request_params = {
                "limit": limit,
                "sort": "desc",
            }

            if symbols:
                # Alpaca NewsRequest expects symbols as comma-separated string, not list
                request_params["symbols"] = ",".join(symbols) if isinstance(symbols, list) else symbols
            if start:
                request_params["start"] = start

            request = NewsRequest(**request_params)
            response = self._news_client.get_news(request)

            news_items = []
            for article in response.news:
                title = article.headline or ""
                published_at = article.created_at or now_utc()

                # Analyze sentiment
                sentiment = self._analyze_sentiment(title)
                catalyst_type = self._classify_catalyst(title)

                news_items.append(NewsItem(
                    title=title,
                    url=article.url or "",
                    published_at=published_at,
                    source=article.source or "Unknown",
                    tickers=article.symbols or [],
                    sentiment=sentiment,
                    catalyst_type=catalyst_type,
                    summary=article.summary,
                ))

            self.last_successful_call = now_utc()
            return news_items

        except Exception as e:
            error_str = str(e)
            # Check for 401/403 - subscription required for News API
            if "401" in error_str or "Authorization Required" in error_str or "403" in error_str:
                if not self._news_disabled:
                    logger.warning("Alpaca News API requires paid subscription - disabling news for this session")
                    self._news_disabled = True
                return []
            logger.error(f"Alpaca get_news failed: {e}")
            return []

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

        catalyst_keywords = {
            CatalystType.EARNINGS: ["earnings", "eps", "revenue", "quarterly", "results"],
            CatalystType.GUIDANCE: ["guidance", "outlook", "forecast"],
            CatalystType.FDA: ["fda", "approval", "clinical", "drug"],
            CatalystType.ACQUISITION: ["acquisition", "acquire", "buyout"],
            CatalystType.MERGER: ["merger", "merge"],
            CatalystType.PARTNERSHIP: ["partnership", "collaboration", "deal"],
            CatalystType.CONTRACT: ["contract", "award"],
            CatalystType.LAWSUIT: ["lawsuit", "litigation", "sued"],
            CatalystType.REGULATORY: ["regulatory", "sec", "investigation"],
            CatalystType.PRODUCT: ["launch", "product", "release"],
        }

        for catalyst_type, keywords in catalyst_keywords.items():
            for keyword in keywords:
                if keyword in text_lower:
                    return catalyst_type

        return CatalystType.UNKNOWN

    # =========================================================
    # STATUS
    # =========================================================

    def get_status(self) -> Dict[str, Any]:
        """Get client status for monitoring."""
        return {
            "provider": "alpaca",
            "enabled": self._enabled,
            "data_client_active": self._data_client is not None,
            "news_client_active": self._news_client is not None,
            "last_successful_call": format_timestamp(self.last_successful_call) if self.last_successful_call else None,
            "cache_status": "active",
        }
