"""
Reddit Sentiment Provider
=========================
Aggregates stock mentions and sentiment from Reddit via multiple free APIs.

Sources:
- ApeWisdom: Tracks mentions across r/wallstreetbets, r/stocks, r/investing, etc.
- Tradestie: WSB sentiment scores and comment counts (may be blocked)
- Heuristic: Upvote/mention ratio and momentum-based sentiment estimation

No Reddit API credentials required - uses aggregator APIs.
"""

import logging
import requests
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from pathlib import Path
import json

logger = logging.getLogger(__name__)


@dataclass
class RedditStock:
    """A stock mentioned on Reddit."""
    ticker: str
    mentions: int = 0
    mentions_24h: int = 0
    rank: int = 999
    sentiment: float = 0.0  # -1 to 1 scale
    sentiment_label: str = "neutral"  # bullish, bearish, neutral
    upvotes: int = 0
    sources: List[str] = field(default_factory=list)
    last_updated: Optional[datetime] = None

    @property
    def is_bullish(self) -> bool:
        return self.sentiment > 0.1 or self.sentiment_label == "bullish"

    @property
    def is_bearish(self) -> bool:
        return self.sentiment < -0.1 or self.sentiment_label == "bearish"


class RedditSentimentProvider:
    """
    Aggregates stock mentions from Reddit via multiple free APIs.

    Usage:
        provider = RedditSentimentProvider()
        trending = provider.get_trending_tickers(limit=50)

        for stock in trending:
            print(f"{stock.ticker}: {stock.mentions} mentions, sentiment={stock.sentiment:.2f}")
    """

    # Common words that look like tickers but aren't
    TICKER_BLACKLIST = {
        "CEO", "IPO", "ETF", "USA", "GDP", "FBI", "SEC", "FDA", "AI", "IT",
        "DD", "PM", "AM", "IV", "EPS", "ATH", "LOL", "IMO", "YOLO", "FUD",
        "HODL", "FOMO", "WSB", "RH", "TD", "API", "USD", "EUR", "GBP",
        "ATM", "ITM", "OTM", "DTE", "YTD", "QE", "FED", "CPI", "PPI",
        "NFT", "BTC", "ETH", "APE", "MOON", "BEAR", "BULL", "PUT", "CALL",
        "ALL", "ARE", "FOR", "THE", "AND", "NOT", "YOU", "CAN", "HAS",
        "WAS", "ONE", "OUR", "OUT", "NEW", "NOW", "HOW", "WHY", "WHO",
        "ANY", "MAY", "SAY", "WAY", "DAY", "BIG", "OLD", "TOP", "LOW",
        "HIGH", "NEXT", "BEST", "MOST", "VERY", "JUST", "BEEN", "SOME",
        "OVER", "INTO", "YEAR", "YOUR", "FROM", "THEY", "BEEN", "HAVE",
        "THIS", "WILL", "EACH", "MAKE", "LIKE", "BACK", "ONLY", "COME",
        "MADE", "FIND", "MORE", "LONG", "DOWN", "GOOD", "MUCH", "WELL"
    }

    # Minimum market cap symbols (helps filter penny stocks / noise)
    # These are known large/mid caps that are commonly discussed
    QUALITY_TICKERS = {
        "AAPL", "MSFT", "GOOGL", "GOOG", "AMZN", "META", "NVDA", "TSLA",
        "AMD", "INTC", "MU", "QCOM", "AVGO", "TXN", "AMAT", "LRCX", "KLAC",
        "ASML", "TSM", "SMCI", "ARM", "PLTR", "SNOW", "NET", "CRWD", "PANW",
        "CRM", "ORCL", "ADBE", "NOW", "SHOP", "SQ", "PYPL", "COIN", "HOOD",
        "NFLX", "DIS", "ROKU", "SPOT", "UBER", "LYFT", "ABNB", "DASH",
        "JPM", "BAC", "GS", "MS", "WFC", "C", "V", "MA", "AXP",
        "XOM", "CVX", "OXY", "SLB", "HAL",
        "LLY", "UNH", "JNJ", "PFE", "MRNA", "ABBV", "MRK", "BMY",
        "BA", "LMT", "RTX", "NOC", "GD",
        "CAT", "DE", "HON", "GE", "MMM",
        "WMT", "COST", "TGT", "HD", "LOW",
        "KO", "PEP", "MCD", "SBUX", "CMG",
        "SPY", "QQQ", "IWM", "DIA", "VTI",
        "SOXL", "SOXS", "TQQQ", "SQQQ", "UVXY",
        "GME", "AMC", "BBBY", "BB", "NOK", "WISH", "CLOV", "SOFI",
        "RIVN", "LCID", "NIO", "XPEV", "LI",
        "RKLB", "IONQ", "RGTI", "QUBT",
        "MARA", "RIOT", "CLSK", "HIVE", "BITF",
    }

    def __init__(
        self,
        cache_ttl_minutes: int = 30,
        cache_dir: Optional[str] = None,
        min_mentions: int = 10,
        filter_quality: bool = True
    ):
        """
        Initialize Reddit sentiment provider.

        Args:
            cache_ttl_minutes: How long to cache results
            cache_dir: Directory for persistent cache (optional)
            min_mentions: Minimum mentions to include a ticker
            filter_quality: If True, prefer known quality tickers
        """
        self.cache_ttl = timedelta(minutes=cache_ttl_minutes)
        self.cache_dir = Path(cache_dir) if cache_dir else None
        self.min_mentions = min_mentions
        self.filter_quality = filter_quality

        self._cache: Dict[str, RedditStock] = {}
        self._last_fetch: Optional[datetime] = None
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) StockScanner/1.0"
        })

    def get_trending_tickers(self, limit: int = 50) -> List[RedditStock]:
        """
        Get top trending tickers from Reddit.

        Args:
            limit: Maximum number of tickers to return

        Returns:
            List of RedditStock objects sorted by mentions
        """
        # Check cache
        if self._is_cache_valid():
            logger.debug("Using cached Reddit data")
            return self._get_sorted_cache()[:limit]

        logger.info("Fetching Reddit sentiment data...")
        results: Dict[str, RedditStock] = {}

        # Source 1: ApeWisdom (most comprehensive)
        try:
            ape_data = self._fetch_apewisdom()
            for item in ape_data:
                ticker = item.get("ticker", "").upper()
                if not self._is_valid_ticker(ticker):
                    continue

                if ticker not in results:
                    results[ticker] = RedditStock(ticker=ticker)

                results[ticker].mentions += item.get("mentions") or 0
                results[ticker].mentions_24h = item.get("mentions_24h_ago") or 0
                results[ticker].rank = min(results[ticker].rank, item.get("rank") or 999)
                results[ticker].upvotes += item.get("upvotes") or 0
                results[ticker].sources.append("apewisdom")

            logger.info(f"ApeWisdom: {len(ape_data)} tickers")
        except Exception as e:
            logger.warning(f"ApeWisdom fetch failed: {e}")

        # Source 2: Tradestie (has sentiment scores)
        try:
            tradestie_data = self._fetch_tradestie()
            for item in tradestie_data:
                ticker = item.get("ticker", "").upper()
                if not self._is_valid_ticker(ticker):
                    continue

                if ticker not in results:
                    results[ticker] = RedditStock(ticker=ticker)

                results[ticker].mentions += item.get("no_of_comments") or 0

                # Tradestie provides sentiment
                sentiment_score = item.get("sentiment_score") or 0
                results[ticker].sentiment = sentiment_score

                sentiment_label = item.get("sentiment", "neutral").lower()
                results[ticker].sentiment_label = sentiment_label

                if "tradestie" not in results[ticker].sources:
                    results[ticker].sources.append("tradestie")

            logger.info(f"Tradestie: {len(tradestie_data)} tickers")
        except Exception as e:
            logger.warning(f"Tradestie fetch failed: {e}")

        # Filter by minimum mentions
        results = {
            ticker: stock for ticker, stock in results.items()
            if stock.mentions >= self.min_mentions
        }

        # Apply heuristic sentiment if no API sentiment available
        self._apply_heuristic_sentiment(results)

        # Update cache
        self._cache = results
        self._last_fetch = datetime.now()

        # Update timestamps
        for stock in results.values():
            stock.last_updated = datetime.now()

        # Save to disk cache if configured
        if self.cache_dir:
            self._save_cache()

        sorted_results = self._get_sorted_cache()
        logger.info(f"Reddit trending: {len(sorted_results)} tickers after filtering")

        return sorted_results[:limit]

    def get_ticker_sentiment(self, ticker: str) -> Optional[RedditStock]:
        """
        Get sentiment data for a specific ticker.

        Args:
            ticker: Stock symbol

        Returns:
            RedditStock object or None if not found
        """
        ticker = ticker.upper()

        # Ensure cache is populated
        if not self._is_cache_valid():
            self.get_trending_tickers(limit=200)

        return self._cache.get(ticker)

    def get_tickers_list(self, limit: int = 50) -> List[str]:
        """
        Get just the ticker symbols (convenience method).

        Args:
            limit: Maximum number of tickers

        Returns:
            List of ticker symbols
        """
        trending = self.get_trending_tickers(limit=limit)
        return [stock.ticker for stock in trending]

    def get_bullish_tickers(self, limit: int = 20) -> List[RedditStock]:
        """Get tickers with bullish sentiment."""
        trending = self.get_trending_tickers(limit=100)
        bullish = [s for s in trending if s.is_bullish]
        return sorted(bullish, key=lambda x: x.sentiment, reverse=True)[:limit]

    def get_bearish_tickers(self, limit: int = 20) -> List[RedditStock]:
        """Get tickers with bearish sentiment."""
        trending = self.get_trending_tickers(limit=100)
        bearish = [s for s in trending if s.is_bearish]
        return sorted(bearish, key=lambda x: x.sentiment)[:limit]

    def _apply_heuristic_sentiment(self, results: Dict[str, RedditStock]):
        """
        Apply heuristic-based sentiment when API sentiment is unavailable.

        Uses:
        - Upvote/mention ratio (high ratio = positive engagement)
        - Rank (lower rank = more interest = slightly bullish bias)
        - Mention momentum (mentions vs 24h ago)

        This provides a rough sentiment estimate when Tradestie is blocked.
        """
        for ticker, stock in results.items():
            # Skip if we already have API sentiment
            if stock.sentiment != 0.0 or "tradestie" in stock.sources:
                continue

            sentiment_score = 0.0

            # Factor 1: Upvote ratio (engagement quality)
            # High upvotes per mention = positive sentiment
            if stock.mentions > 0 and stock.upvotes > 0:
                upvote_ratio = stock.upvotes / stock.mentions
                # Typical ratio is 5-20 upvotes per mention
                # Higher ratio = more bullish
                if upvote_ratio > 15:
                    sentiment_score += 0.3
                elif upvote_ratio > 10:
                    sentiment_score += 0.15
                elif upvote_ratio < 3:
                    sentiment_score -= 0.1

            # Factor 2: Rank momentum
            # Top 10 tickers get slight bullish bias (market interest)
            if stock.rank <= 5:
                sentiment_score += 0.2
            elif stock.rank <= 10:
                sentiment_score += 0.1
            elif stock.rank <= 20:
                sentiment_score += 0.05

            # Factor 3: Mention momentum (24h change)
            # More mentions than 24h ago = growing interest = bullish
            if stock.mentions_24h > 0 and stock.mentions > 0:
                momentum = (stock.mentions - stock.mentions_24h) / stock.mentions_24h
                if momentum > 0.5:  # 50%+ increase
                    sentiment_score += 0.25
                elif momentum > 0.2:  # 20%+ increase
                    sentiment_score += 0.1
                elif momentum < -0.3:  # 30%+ decrease
                    sentiment_score -= 0.15

            # Factor 4: Quality ticker bonus
            # Known quality tickers get slight neutral-to-bullish assumption
            if ticker in self.QUALITY_TICKERS:
                sentiment_score += 0.05

            # Clamp to [-1, 1]
            stock.sentiment = max(-1.0, min(1.0, sentiment_score))

            # Set label based on score
            if stock.sentiment > 0.1:
                stock.sentiment_label = "bullish"
            elif stock.sentiment < -0.1:
                stock.sentiment_label = "bearish"
            else:
                stock.sentiment_label = "neutral"

            # Mark source
            if "heuristic" not in stock.sources:
                stock.sources.append("heuristic")

    def _fetch_apewisdom(self) -> List[Dict]:
        """Fetch from ApeWisdom API."""
        url = "https://apewisdom.io/api/v1.0/filter/all-stocks"
        resp = self._session.get(url, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        return data.get("results", [])

    def _fetch_tradestie(self) -> List[Dict]:
        """Fetch from Tradestie API."""
        url = "https://tradestie.com/api/v1/apps/reddit"
        resp = self._session.get(url, timeout=15)
        if resp.status_code == 403:
            # Tradestie sometimes blocks automated requests - fail silently
            logger.debug("Tradestie returned 403 - skipping")
            return []
        resp.raise_for_status()
        return resp.json()

    def _is_valid_ticker(self, ticker: str) -> bool:
        """Check if ticker is valid (not a common word)."""
        if not ticker:
            return False
        if len(ticker) < 1 or len(ticker) > 5:
            return False
        if ticker in self.TICKER_BLACKLIST:
            return False
        if not ticker.isalpha():
            return False

        # If filtering for quality, prefer known tickers
        # But still allow unknown ones if they have high mentions
        return True

    def _is_cache_valid(self) -> bool:
        """Check if cache is still valid."""
        if not self._last_fetch:
            # Try loading from disk
            if self.cache_dir and self._load_cache():
                return True
            return False
        return datetime.now() - self._last_fetch < self.cache_ttl

    def _get_sorted_cache(self) -> List[RedditStock]:
        """Get cache sorted by mentions."""
        stocks = list(self._cache.values())

        # Sort by mentions, with quality tickers getting a boost
        def sort_key(s: RedditStock) -> Tuple[int, int]:
            quality_boost = 1000 if s.ticker in self.QUALITY_TICKERS else 0
            return (quality_boost + s.mentions, s.rank)

        return sorted(stocks, key=sort_key, reverse=True)

    def _save_cache(self):
        """Save cache to disk."""
        if not self.cache_dir:
            return

        self.cache_dir.mkdir(parents=True, exist_ok=True)
        cache_file = self.cache_dir / "reddit_sentiment_cache.json"

        try:
            data = {
                "last_fetch": self._last_fetch.isoformat() if self._last_fetch else None,
                "stocks": {
                    ticker: {
                        "ticker": s.ticker,
                        "mentions": s.mentions,
                        "mentions_24h": s.mentions_24h,
                        "rank": s.rank,
                        "sentiment": s.sentiment,
                        "sentiment_label": s.sentiment_label,
                        "upvotes": s.upvotes,
                        "sources": s.sources,
                    }
                    for ticker, s in self._cache.items()
                }
            }
            cache_file.write_text(json.dumps(data, indent=2))
        except Exception as e:
            logger.warning(f"Failed to save cache: {e}")

    def _load_cache(self) -> bool:
        """Load cache from disk."""
        if not self.cache_dir:
            return False

        cache_file = self.cache_dir / "reddit_sentiment_cache.json"
        if not cache_file.exists():
            return False

        try:
            data = json.loads(cache_file.read_text())
            last_fetch = data.get("last_fetch")
            if last_fetch:
                self._last_fetch = datetime.fromisoformat(last_fetch)

                # Check if disk cache is still valid
                if datetime.now() - self._last_fetch > self.cache_ttl:
                    return False

            self._cache = {
                ticker: RedditStock(
                    ticker=s["ticker"],
                    mentions=s["mentions"],
                    mentions_24h=s.get("mentions_24h", 0),
                    rank=s.get("rank", 999),
                    sentiment=s.get("sentiment", 0),
                    sentiment_label=s.get("sentiment_label", "neutral"),
                    upvotes=s.get("upvotes", 0),
                    sources=s.get("sources", []),
                )
                for ticker, s in data.get("stocks", {}).items()
            }

            logger.info(f"Loaded {len(self._cache)} tickers from cache")
            return True

        except Exception as e:
            logger.warning(f"Failed to load cache: {e}")
            return False


# =============================================================================
# CLI for testing
# =============================================================================

def main():
    """Test the Reddit sentiment provider."""
    import argparse

    parser = argparse.ArgumentParser(description="Reddit Stock Sentiment")
    parser.add_argument("--limit", type=int, default=30, help="Number of tickers")
    parser.add_argument("--bullish", action="store_true", help="Show only bullish")
    parser.add_argument("--ticker", type=str, help="Get sentiment for specific ticker")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(message)s")

    provider = RedditSentimentProvider(
        cache_ttl_minutes=30,
        min_mentions=10
    )

    if args.ticker:
        stock = provider.get_ticker_sentiment(args.ticker)
        if stock:
            print(f"\n{stock.ticker}:")
            print(f"  Mentions: {stock.mentions}")
            print(f"  Sentiment: {stock.sentiment:.2f} ({stock.sentiment_label})")
            print(f"  Rank: {stock.rank}")
            print(f"  Sources: {', '.join(stock.sources)}")
        else:
            print(f"{args.ticker} not found in Reddit data")
        return

    if args.bullish:
        stocks = provider.get_bullish_tickers(limit=args.limit)
        print(f"\nTop {len(stocks)} Bullish Tickers on Reddit:\n")
    else:
        stocks = provider.get_trending_tickers(limit=args.limit)
        print(f"\nTop {len(stocks)} Trending Tickers on Reddit:\n")

    print(f"{'Rank':<5} {'Ticker':<8} {'Mentions':<10} {'Sentiment':<12} {'Sources'}")
    print("-" * 55)

    for i, stock in enumerate(stocks, 1):
        sentiment_str = f"{stock.sentiment:+.2f}" if stock.sentiment else "N/A"
        sources = ", ".join(stock.sources[:2])
        print(f"{i:<5} {stock.ticker:<8} {stock.mentions:<10} {sentiment_str:<12} {sources}")


if __name__ == "__main__":
    main()
