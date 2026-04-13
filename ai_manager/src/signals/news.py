"""
News Analysis Module

Aggregates and analyzes news sentiment for portfolio holdings.
Uses deterministic keyword matching - LLM only for summarization.
Includes Reddit sentiment integration.
"""

import os
import sys
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any

from ..utils.logging import get_logger
from ..utils.time import now_utc, time_ago_str
from ..utils.typing import NewsItem, NewsSentiment, CatalystType, Holding
from ..providers.massive_client import MassiveClient
from ..providers.alpaca_client import AlpacaClient

logger = get_logger(__name__)

# Try to import Reddit sentiment provider
try:
    # Add utilities directory to path
    UTILITIES_PATH = os.path.join(os.path.dirname(__file__), "..", "..", "..", "utilities")
    if os.path.exists(UTILITIES_PATH):
        sys.path.insert(0, UTILITIES_PATH)
    from reddit_sentiment import RedditSentimentProvider
    REDDIT_AVAILABLE = True
except ImportError:
    REDDIT_AVAILABLE = False
    RedditSentimentProvider = None

# Reddit sentiment settings
REDDIT_WEIGHT = 0.20  # How much Reddit sentiment affects the final news score (20%)
REDDIT_BULLISH_BOOST = 10  # Points to add for bullish sentiment
REDDIT_BEARISH_PENALTY = 10  # Points to subtract for bearish sentiment


# Sentiment score mapping
SENTIMENT_SCORES = {
    NewsSentiment.VERY_POSITIVE: 90,
    NewsSentiment.POSITIVE: 70,
    NewsSentiment.NEUTRAL: 50,
    NewsSentiment.NEGATIVE: 30,
    NewsSentiment.VERY_NEGATIVE: 10,
}

# Catalyst weights (how much they impact scoring)
CATALYST_WEIGHTS = {
    CatalystType.EARNINGS: 1.5,
    CatalystType.GUIDANCE: 1.4,
    CatalystType.FDA: 1.6,
    CatalystType.ACQUISITION: 1.5,
    CatalystType.MERGER: 1.5,
    CatalystType.PARTNERSHIP: 1.2,
    CatalystType.CONTRACT: 1.2,
    CatalystType.LAWSUIT: 1.3,
    CatalystType.REGULATORY: 1.3,
    CatalystType.PRODUCT: 1.1,
    CatalystType.MACRO: 1.0,
    CatalystType.UNKNOWN: 0.8,
}


class NewsAnalyzer:
    """
    News analysis engine.

    Fetches and scores news for holdings using deterministic methods.
    """

    def __init__(
        self,
        massive_client: Optional[MassiveClient] = None,
        alpaca_client: Optional[AlpacaClient] = None,
        max_articles_per_ticker: int = 10,
        max_news_age_hours: int = 72,
        catalyst_tags: Optional[List[str]] = None
    ):
        """
        Initialize news analyzer.

        Args:
            massive_client: Polygon client for news
            alpaca_client: Alpaca client (fallback)
            max_articles_per_ticker: Max articles to fetch per ticker
            max_news_age_hours: Ignore news older than this
            catalyst_tags: Tags to look for in news
        """
        self.massive = massive_client
        self.alpaca = alpaca_client
        self.max_articles = max_articles_per_ticker
        self.max_age_hours = max_news_age_hours
        self.catalyst_tags = catalyst_tags or [
            "earnings", "guidance", "product", "acquisition",
            "merger", "lawsuit", "regulatory", "fda", "clinical",
            "contract", "partnership"
        ]

    def get_news_for_ticker(self, symbol: str) -> List[NewsItem]:
        """
        Fetch news for a single ticker.

        Args:
            symbol: Stock symbol

        Returns:
            List of NewsItem objects
        """
        cutoff = now_utc() - timedelta(hours=self.max_age_hours)
        cutoff_str = cutoff.strftime("%Y-%m-%dT%H:%M:%SZ")

        news_items = []

        # Try Polygon first (primary source)
        if self.massive and self.massive.is_available:
            polygon_news = self.massive.get_news(
                symbol=symbol,
                limit=self.max_articles,
                published_utc_gte=cutoff_str
            )
            news_items.extend(polygon_news)
            if polygon_news:
                logger.debug(f"{symbol}: Found {len(polygon_news)} news articles from Polygon")

        # Only try Alpaca if Polygon returned nothing
        # Note: Alpaca News requires paid subscription - may return 401
        if len(news_items) == 0 and self.alpaca and self.alpaca.is_available:
            try:
                alpaca_news = self.alpaca.get_news(
                    symbols=[symbol],
                    limit=self.max_articles,
                    start=cutoff
                )

                # Deduplicate by title similarity
                existing_titles = {n.title.lower()[:50] for n in news_items}
                for news in alpaca_news:
                    if news.title.lower()[:50] not in existing_titles:
                        news_items.append(news)
            except Exception as e:
                # Alpaca news often fails - don't spam logs
                pass

        # Sort by published date (most recent first)
        news_items.sort(key=lambda n: n.published_at, reverse=True)

        return news_items[:self.max_articles]

    def analyze_ticker(self, symbol: str) -> Dict[str, Any]:
        """
        Get news analysis for a ticker.

        Args:
            symbol: Stock symbol

        Returns:
            Dict with news items, sentiment, catalyst type, and score
        """
        news_items = self.get_news_for_ticker(symbol)

        if not news_items:
            return {
                "symbol": symbol,
                "news_items": [],
                "overall_sentiment": NewsSentiment.NEUTRAL,
                "primary_catalyst": CatalystType.UNKNOWN,
                "news_score": 50.0,
                "has_catalyst": False,
                "catalyst_count": 0,
            }

        # Aggregate sentiment
        sentiment_scores = []
        catalyst_types = []

        for news in news_items:
            # Weight by recency (more recent = higher weight)
            age_hours = (now_utc() - news.published_at).total_seconds() / 3600
            recency_weight = max(0.5, 1 - (age_hours / self.max_age_hours))

            # Weight by catalyst importance
            catalyst_weight = CATALYST_WEIGHTS.get(news.catalyst_type, 1.0)

            base_score = SENTIMENT_SCORES.get(news.sentiment, 50)
            weighted_score = base_score * recency_weight * catalyst_weight
            sentiment_scores.append(weighted_score)

            if news.catalyst_type != CatalystType.UNKNOWN:
                catalyst_types.append(news.catalyst_type)

        # Calculate overall sentiment score
        news_score = sum(sentiment_scores) / len(sentiment_scores) if sentiment_scores else 50.0

        # Determine overall sentiment from score
        if news_score >= 75:
            overall_sentiment = NewsSentiment.VERY_POSITIVE
        elif news_score >= 60:
            overall_sentiment = NewsSentiment.POSITIVE
        elif news_score <= 25:
            overall_sentiment = NewsSentiment.VERY_NEGATIVE
        elif news_score <= 40:
            overall_sentiment = NewsSentiment.NEGATIVE
        else:
            overall_sentiment = NewsSentiment.NEUTRAL

        # Determine primary catalyst (most frequent)
        primary_catalyst = CatalystType.UNKNOWN
        if catalyst_types:
            catalyst_counts = {}
            for ct in catalyst_types:
                catalyst_counts[ct] = catalyst_counts.get(ct, 0) + 1
            primary_catalyst = max(catalyst_counts.keys(), key=lambda k: catalyst_counts[k])

        return {
            "symbol": symbol,
            "news_items": news_items,
            "overall_sentiment": overall_sentiment,
            "primary_catalyst": primary_catalyst,
            "news_score": news_score,
            "has_catalyst": len(catalyst_types) > 0,
            "catalyst_count": len(catalyst_types),
        }


def aggregate_news_sentiment(
    holdings: List[Holding],
    massive_client: Optional[MassiveClient] = None,
    alpaca_client: Optional[AlpacaClient] = None,
    max_articles_per_ticker: int = 10,
    max_news_age_hours: int = 72,
    include_reddit: bool = True
) -> Dict[str, Dict[str, Any]]:
    """
    Aggregate news sentiment for all holdings.

    Args:
        holdings: List of holdings to analyze
        massive_client: Polygon client
        alpaca_client: Alpaca client
        max_articles_per_ticker: Max articles per ticker
        max_news_age_hours: Max news age to consider
        include_reddit: Whether to include Reddit sentiment

    Returns:
        Dict mapping symbol to news analysis
    """
    analyzer = NewsAnalyzer(
        massive_client=massive_client,
        alpaca_client=alpaca_client,
        max_articles_per_ticker=max_articles_per_ticker,
        max_news_age_hours=max_news_age_hours
    )

    # Initialize Reddit provider if available and enabled
    reddit_data = {}
    if include_reddit and REDDIT_AVAILABLE:
        try:
            reddit_provider = RedditSentimentProvider(cache_ttl_minutes=30, min_mentions=10)
            trending = reddit_provider.get_trending_tickers(limit=100)
            reddit_data = {stock.ticker: stock for stock in trending}
            logger.info(f"Loaded Reddit sentiment for {len(reddit_data)} tickers")
        except Exception as e:
            logger.warning(f"Failed to load Reddit sentiment: {e}")

    results = {}
    total_news_items = 0
    tickers_with_news = 0

    for holding in holdings:
        analysis = analyzer.analyze_ticker(holding.symbol)

        # Track news statistics
        news_count = len(analysis.get("news_items", []))
        total_news_items += news_count
        if news_count > 0:
            tickers_with_news += 1

        # Add Reddit sentiment if available
        reddit_stock = reddit_data.get(holding.symbol.upper())
        if reddit_stock:
            analysis["reddit_mentions"] = reddit_stock.mentions
            analysis["reddit_sentiment"] = reddit_stock.sentiment
            analysis["reddit_sentiment_label"] = reddit_stock.sentiment_label
            analysis["reddit_trending"] = True

            # Adjust news score based on Reddit sentiment
            base_score = analysis["news_score"]
            if reddit_stock.is_bullish:
                analysis["news_score"] = min(100, base_score + REDDIT_BULLISH_BOOST)
            elif reddit_stock.is_bearish:
                analysis["news_score"] = max(0, base_score - REDDIT_BEARISH_PENALTY)

            logger.debug(f"{holding.symbol}: Reddit mentions={reddit_stock.mentions}, "
                        f"sentiment={reddit_stock.sentiment:.2f}")
        else:
            analysis["reddit_mentions"] = 0
            analysis["reddit_sentiment"] = 0.0
            analysis["reddit_sentiment_label"] = ""
            analysis["reddit_trending"] = False

        results[holding.symbol] = analysis

        # Update holding with news data
        holding.news_items = analysis["news_items"]
        holding.news_sentiment = analysis["overall_sentiment"]
        holding.catalyst_type = analysis["primary_catalyst"]

    logger.info(f"Aggregated news for {len(results)} holdings: {total_news_items} articles across {tickers_with_news} tickers")

    return results


def get_top_news(
    news_analyses: Dict[str, Dict[str, Any]],
    limit: int = 10,
    sentiment_filter: Optional[NewsSentiment] = None
) -> List[Dict[str, Any]]:
    """
    Get top news items across all holdings.

    Args:
        news_analyses: Dict of news analyses by symbol
        limit: Max items to return
        sentiment_filter: Optional filter by sentiment

    Returns:
        List of top news items with metadata
    """
    all_news = []

    for symbol, analysis in news_analyses.items():
        for news_item in analysis.get("news_items", []):
            if sentiment_filter and news_item.sentiment != sentiment_filter:
                continue

            all_news.append({
                "symbol": symbol,
                "title": news_item.title,
                "url": news_item.url,
                "published_at": news_item.published_at.isoformat(),
                "source": news_item.source,
                "sentiment": news_item.sentiment.value,
                "catalyst_type": news_item.catalyst_type.value,
                "age": time_ago_str(news_item.published_at),
            })

    # Sort by published date (most recent first)
    all_news.sort(key=lambda n: n["published_at"], reverse=True)

    return all_news[:limit]


def get_catalyst_summary(
    news_analyses: Dict[str, Dict[str, Any]]
) -> Dict[str, List[str]]:
    """
    Get summary of catalysts by type across portfolio.

    Args:
        news_analyses: Dict of news analyses by symbol

    Returns:
        Dict mapping catalyst type to list of symbols
    """
    catalyst_map: Dict[str, List[str]] = {}

    for symbol, analysis in news_analyses.items():
        catalyst = analysis.get("primary_catalyst")
        if catalyst and catalyst != CatalystType.UNKNOWN:
            catalyst_name = catalyst.value
            if catalyst_name not in catalyst_map:
                catalyst_map[catalyst_name] = []
            if symbol not in catalyst_map[catalyst_name]:
                catalyst_map[catalyst_name].append(symbol)

    return catalyst_map
