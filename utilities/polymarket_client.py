"""
Polymarket API Client - Prediction Market Sentiment Integration
================================================================

Fetches prediction market data from Polymarket to use as sentiment indicators
for trading decisions. No authentication required for read-only data.

Key Markets to Monitor:
- Fed rate decisions (interest rate policy)
- Recession probability
- Major economic events
- Geopolitical events (tariffs, elections, etc.)

API Base URLs:
- Gamma API (markets/events): https://gamma-api.polymarket.com
- CLOB API (prices/orderbook): https://clob.polymarket.com

Author: Claude Code
Version: 1.0.0
"""

import os
import time
import logging
import requests
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta

# ============================================================
# CONFIGURATION
# ============================================================

# API Base URLs (no auth required for read-only)
GAMMA_API_BASE = "https://gamma-api.polymarket.com"
CLOB_API_BASE = "https://clob.polymarket.com"

# Cache settings
CACHE_TTL_SECONDS = 300  # 5 minutes

# Rate limiting
REQUEST_TIMEOUT = 10
MIN_REQUEST_INTERVAL = 0.5  # seconds between requests

# Logging
logger = logging.getLogger(__name__)

# ============================================================
# KEY MARKET SLUGS TO MONITOR
# ============================================================

# These are example slugs - they change frequently as markets resolve
# The client will search for active markets by keyword instead

MARKET_KEYWORDS = {
    # Fed/Monetary Policy
    "fed": ["fed chair", "federal reserve", "fomc", "interest rate", "rate cut", "rate hike", "kudlow"],

    # Economic Indicators
    "recession": ["recession", "gdp", "economic growth", "soft landing", "hard landing"],

    # Inflation
    "inflation": ["inflation", "cpi", "pce", "consumer prices"],

    # Market Events - crypto as proxy for risk appetite
    "market": ["bitcoin", "ethereum", "crypto", "btc"],

    # Geopolitical / Policy
    "tariffs": ["tariff", "trade war", "china", "deport"],

    # Volatility
    "volatility": ["vix", "volatility", "correction", "crash"],
}

# Specific high-value markets to track (update these as markets change)
TRACKED_MARKET_SLUGS = [
    # These need to be updated periodically as markets resolve/new ones open
    # Run polymarket_client.py --discover to find current active markets
]


# ============================================================
# DATA CLASSES
# ============================================================

@dataclass
class PolymarketPrice:
    """Current price/probability for a market outcome."""
    token_id: str
    outcome: str  # "Yes" or "No"
    price: float  # 0.00 to 1.00 (probability)
    timestamp: datetime = field(default_factory=datetime.now)


@dataclass
class PolymarketMarket:
    """A single prediction market."""
    id: str
    slug: str
    question: str
    description: str
    outcomes: List[str]  # e.g., ["Yes", "No"]
    outcome_prices: Dict[str, float]  # outcome -> price
    volume: float  # Total volume traded
    liquidity: float  # Current liquidity
    end_date: Optional[datetime]
    closed: bool
    resolved: bool
    tags: List[str]

    @property
    def yes_price(self) -> float:
        """Probability of 'Yes' outcome."""
        return self.outcome_prices.get("Yes", 0.0)

    @property
    def no_price(self) -> float:
        """Probability of 'No' outcome."""
        return self.outcome_prices.get("No", 0.0)

    def __str__(self) -> str:
        return f"{self.question[:60]}... | Yes: {self.yes_price*100:.1f}% | Vol: ${self.volume:,.0f}"


@dataclass
class PolymarketEvent:
    """An event containing multiple related markets."""
    id: str
    slug: str
    title: str
    description: str
    markets: List[PolymarketMarket]
    end_date: Optional[datetime]
    closed: bool

    def __str__(self) -> str:
        return f"{self.title} ({len(self.markets)} markets)"


@dataclass
class MarketSentiment:
    """Aggregated sentiment from prediction markets."""
    fed_dovish_prob: Optional[float] = None  # Probability of rate cut
    fed_hawkish_prob: Optional[float] = None  # Probability of rate hike
    recession_prob: Optional[float] = None  # Probability of recession
    market_bullish_prob: Optional[float] = None  # Probability of market up
    high_volatility_prob: Optional[float] = None  # Probability of VIX spike

    raw_markets: List[PolymarketMarket] = field(default_factory=list)
    fetch_time: datetime = field(default_factory=datetime.now)

    @property
    def overall_risk_level(self) -> str:
        """
        Calculate overall risk level from prediction markets.
        Returns: "LOW", "MEDIUM", "HIGH", "EXTREME"
        """
        risk_score = 0

        # Recession probability adds significant risk
        if self.recession_prob:
            if self.recession_prob > 0.5:
                risk_score += 3
            elif self.recession_prob > 0.3:
                risk_score += 2
            elif self.recession_prob > 0.15:
                risk_score += 1

        # Hawkish Fed adds risk (tightening = bad for stocks)
        if self.fed_hawkish_prob and self.fed_hawkish_prob > 0.5:
            risk_score += 1

        # High volatility expectation adds risk
        if self.high_volatility_prob and self.high_volatility_prob > 0.4:
            risk_score += 1

        if risk_score >= 4:
            return "EXTREME"
        elif risk_score >= 3:
            return "HIGH"
        elif risk_score >= 1:
            return "MEDIUM"
        return "LOW"

    def get_trading_adjustment(self) -> Dict:
        """
        Get trading parameter adjustments based on prediction market sentiment.
        """
        risk = self.overall_risk_level

        adjustments = {
            "LOW": {
                "size_mult": 1.0,
                "allow_trading": True,
                "note": "Prediction markets show low risk environment"
            },
            "MEDIUM": {
                "size_mult": 0.75,
                "allow_trading": True,
                "note": "Elevated uncertainty in prediction markets"
            },
            "HIGH": {
                "size_mult": 0.5,
                "allow_trading": True,
                "note": "High risk signals from prediction markets"
            },
            "EXTREME": {
                "size_mult": 0.25,
                "allow_trading": True,  # Still allow but very reduced
                "note": "Extreme risk - prediction markets signal caution"
            }
        }

        return adjustments.get(risk, adjustments["MEDIUM"])


# ============================================================
# API CLIENT
# ============================================================

class PolymarketClient:
    """
    Read-only client for Polymarket prediction market data.
    No authentication required.
    """

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "Accept": "application/json",
            "User-Agent": "TradingBot/1.0"
        })
        self._cache: Dict[str, Tuple[datetime, any]] = {}
        self._last_request_time = 0

    def _rate_limit(self):
        """Enforce minimum interval between requests."""
        elapsed = time.time() - self._last_request_time
        if elapsed < MIN_REQUEST_INTERVAL:
            time.sleep(MIN_REQUEST_INTERVAL - elapsed)
        self._last_request_time = time.time()

    def _get_cached(self, key: str) -> Optional[any]:
        """Get cached value if not expired."""
        if key in self._cache:
            cached_time, value = self._cache[key]
            if datetime.now() - cached_time < timedelta(seconds=CACHE_TTL_SECONDS):
                return value
        return None

    def _set_cache(self, key: str, value: any):
        """Cache a value."""
        self._cache[key] = (datetime.now(), value)

    def _gamma_get(self, endpoint: str, params: dict = None) -> Optional[dict]:
        """Make GET request to Gamma API."""
        self._rate_limit()
        try:
            url = f"{GAMMA_API_BASE}{endpoint}"
            resp = self.session.get(url, params=params, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            logger.warning(f"[POLYMARKET] Gamma API error: {endpoint} - {e}")
            return None

    def _clob_get(self, endpoint: str, params: dict = None) -> Optional[dict]:
        """Make GET request to CLOB API."""
        self._rate_limit()
        try:
            url = f"{CLOB_API_BASE}{endpoint}"
            resp = self.session.get(url, params=params, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            logger.warning(f"[POLYMARKET] CLOB API error: {endpoint} - {e}")
            return None

    # --------------------------------------------------------
    # Market Discovery
    # --------------------------------------------------------

    def get_active_events(self, limit: int = 50) -> List[PolymarketEvent]:
        """
        Get list of active (non-closed) events.
        """
        cache_key = f"events_active_{limit}"
        cached = self._get_cached(cache_key)
        if cached:
            return cached

        data = self._gamma_get("/events", params={
            "closed": "false",
            "limit": limit,
            "order": "volume",
            "ascending": "false"  # Highest volume first
        })

        if not data:
            return []

        events = []
        for item in data:
            try:
                event = self._parse_event(item)
                if event:
                    events.append(event)
            except Exception as e:
                logger.debug(f"[POLYMARKET] Failed to parse event: {e}")

        self._set_cache(cache_key, events)
        return events

    def get_active_markets(self, limit: int = 100) -> List[PolymarketMarket]:
        """
        Get list of active markets sorted by volume.
        """
        cache_key = f"markets_active_{limit}"
        cached = self._get_cached(cache_key)
        if cached:
            return cached

        data = self._gamma_get("/markets", params={
            "closed": "false",
            "limit": limit,
            "order": "volume",
            "ascending": "false"
        })

        if not data:
            return []

        markets = []
        for item in data:
            try:
                market = self._parse_market(item)
                if market:
                    markets.append(market)
            except Exception as e:
                logger.debug(f"[POLYMARKET] Failed to parse market: {e}")

        self._set_cache(cache_key, markets)
        return markets

    def search_markets(self, keywords: List[str], limit: int = 20) -> List[PolymarketMarket]:
        """
        Search for markets containing any of the keywords.
        """
        all_markets = self.get_active_markets(limit=200)

        matches = []
        keywords_lower = [k.lower() for k in keywords]

        for market in all_markets:
            text = f"{market.question} {market.description}".lower()
            if any(kw in text for kw in keywords_lower):
                matches.append(market)
                if len(matches) >= limit:
                    break

        return matches

    def get_market_by_slug(self, slug: str) -> Optional[PolymarketMarket]:
        """
        Get a specific market by its slug.
        """
        cache_key = f"market_slug_{slug}"
        cached = self._get_cached(cache_key)
        if cached:
            return cached

        data = self._gamma_get(f"/markets/slug/{slug}")
        if not data:
            return None

        market = self._parse_market(data)
        if market:
            self._set_cache(cache_key, market)
        return market

    def get_event_by_slug(self, slug: str) -> Optional[PolymarketEvent]:
        """
        Get a specific event by its slug.
        """
        cache_key = f"event_slug_{slug}"
        cached = self._get_cached(cache_key)
        if cached:
            return cached

        data = self._gamma_get(f"/events/slug/{slug}")
        if not data:
            return None

        event = self._parse_event(data)
        if event:
            self._set_cache(cache_key, event)
        return event

    # --------------------------------------------------------
    # Pricing
    # --------------------------------------------------------

    def get_market_price(self, token_id: str, side: str = "buy") -> Optional[float]:
        """
        Get current price for a token.

        Args:
            token_id: The token ID (from market.tokens)
            side: "buy" or "sell"

        Returns:
            Price as float (0.0 to 1.0)
        """
        data = self._clob_get("/price", params={
            "token_id": token_id,
            "side": side
        })

        if data and "price" in data:
            return float(data["price"])
        return None

    def get_midpoint_price(self, token_id: str) -> Optional[float]:
        """
        Get midpoint price for a token.
        """
        data = self._clob_get("/midpoint", params={
            "token_id": token_id
        })

        if data and "mid" in data:
            return float(data["mid"])
        return None

    def get_orderbook(self, token_id: str) -> Optional[dict]:
        """
        Get orderbook for a token.
        """
        return self._clob_get("/book", params={
            "token_id": token_id
        })

    # --------------------------------------------------------
    # Sentiment Aggregation
    # --------------------------------------------------------

    def get_market_sentiment(self) -> MarketSentiment:
        """
        Aggregate sentiment from key prediction markets.

        This searches for markets related to:
        - Fed policy (rate cuts/hikes)
        - Recession probability
        - Market direction (using crypto as risk appetite proxy)
        - Geopolitical/policy events
        """
        sentiment = MarketSentiment()

        # Search for Fed-related markets
        fed_markets = self.search_markets(MARKET_KEYWORDS["fed"], limit=10)
        for market in fed_markets:
            q_lower = market.question.lower()
            if "cut" in q_lower or "lower" in q_lower or "dovish" in q_lower:
                sentiment.fed_dovish_prob = market.yes_price
                sentiment.raw_markets.append(market)
            elif "hike" in q_lower or "raise" in q_lower or "increase" in q_lower or "hawkish" in q_lower:
                sentiment.fed_hawkish_prob = market.yes_price
                sentiment.raw_markets.append(market)
            # Kudlow as Fed chair would be dovish signal
            elif "kudlow" in q_lower:
                sentiment.fed_dovish_prob = market.yes_price
                sentiment.raw_markets.append(market)

        # Search for recession markets
        recession_markets = self.search_markets(MARKET_KEYWORDS["recession"], limit=5)
        for market in recession_markets:
            q_lower = market.question.lower()
            if "recession" in q_lower:
                sentiment.recession_prob = market.yes_price
                sentiment.raw_markets.append(market)
                break  # Take first match

        # Search for market direction - use crypto/bitcoin as risk appetite proxy
        market_markets = self.search_markets(MARKET_KEYWORDS["market"], limit=10)
        for market in market_markets:
            q_lower = market.question.lower()
            # Look for bitcoin/crypto price prediction markets
            if ("bitcoin" in q_lower or "btc" in q_lower or "ethereum" in q_lower) and \
               any(x in q_lower for x in ["above", "greater", "reach", "hit"]):
                # Higher crypto = risk-on sentiment = bullish for stocks
                sentiment.market_bullish_prob = market.yes_price
                sentiment.raw_markets.append(market)
                break

        # Search for geopolitical/policy risk
        tariff_markets = self.search_markets(MARKET_KEYWORDS["tariffs"], limit=5)
        for market in tariff_markets:
            q_lower = market.question.lower()
            # Tariff/trade war escalation = higher volatility/risk
            if "tariff" in q_lower or "trade war" in q_lower:
                # High probability of tariff escalation = bearish signal
                if market.yes_price > 0.5:
                    sentiment.high_volatility_prob = market.yes_price
                sentiment.raw_markets.append(market)
                break

        # Search for volatility-specific markets
        vol_markets = self.search_markets(MARKET_KEYWORDS["volatility"], limit=5)
        for market in vol_markets:
            if sentiment.high_volatility_prob is None:
                sentiment.high_volatility_prob = market.yes_price
            sentiment.raw_markets.append(market)
            break

        sentiment.fetch_time = datetime.now()
        return sentiment

    # --------------------------------------------------------
    # Parsing Helpers
    # --------------------------------------------------------

    def _parse_market(self, data: dict) -> Optional[PolymarketMarket]:
        """Parse raw API response into PolymarketMarket."""
        try:
            import json as json_module

            # Extract outcome prices
            outcome_prices = {}
            outcomes = []

            # Handle different response formats
            if "tokens" in data:
                for token in data["tokens"]:
                    outcome = token.get("outcome", "Unknown")
                    price = float(token.get("price", 0))
                    outcome_prices[outcome] = price
                    outcomes.append(outcome)
            elif "outcomePrices" in data:
                # outcomePrices can be a JSON string or a list
                prices = data["outcomePrices"]
                if isinstance(prices, str):
                    try:
                        prices = json_module.loads(prices)
                    except:
                        prices = []
                if isinstance(prices, list) and len(prices) >= 2:
                    outcome_prices["Yes"] = float(prices[0])
                    outcome_prices["No"] = float(prices[1])
                    outcomes = ["Yes", "No"]

            # Also parse outcomes if it's a JSON string
            if "outcomes" in data and not outcomes:
                raw_outcomes = data["outcomes"]
                if isinstance(raw_outcomes, str):
                    try:
                        outcomes = json_module.loads(raw_outcomes)
                    except:
                        outcomes = ["Yes", "No"]
                elif isinstance(raw_outcomes, list):
                    outcomes = raw_outcomes

            # Parse end date
            end_date = None
            if data.get("endDate"):
                try:
                    end_date = datetime.fromisoformat(data["endDate"].replace("Z", "+00:00"))
                except:
                    pass

            return PolymarketMarket(
                id=str(data.get("id", "")),
                slug=data.get("slug", ""),
                question=data.get("question", data.get("title", "")),
                description=data.get("description", ""),
                outcomes=outcomes or ["Yes", "No"],
                outcome_prices=outcome_prices,
                volume=float(data.get("volume", 0) or 0),
                liquidity=float(data.get("liquidity", 0) or 0),
                end_date=end_date,
                closed=data.get("closed", False),
                resolved=data.get("resolved", False),
                tags=[t.get("label", "") for t in data.get("tags", [])] if data.get("tags") else []
            )
        except Exception as e:
            logger.debug(f"[POLYMARKET] Parse error: {e}")
            return None

    def _parse_event(self, data: dict) -> Optional[PolymarketEvent]:
        """Parse raw API response into PolymarketEvent."""
        try:
            markets = []
            for m in data.get("markets", []):
                market = self._parse_market(m)
                if market:
                    markets.append(market)

            end_date = None
            if data.get("endDate"):
                try:
                    end_date = datetime.fromisoformat(data["endDate"].replace("Z", "+00:00"))
                except:
                    pass

            return PolymarketEvent(
                id=str(data.get("id", "")),
                slug=data.get("slug", ""),
                title=data.get("title", ""),
                description=data.get("description", ""),
                markets=markets,
                end_date=end_date,
                closed=data.get("closed", False)
            )
        except Exception as e:
            logger.debug(f"[POLYMARKET] Parse event error: {e}")
            return None


# ============================================================
# STANDALONE CLI
# ============================================================

def main():
    """CLI for testing and discovering markets."""
    import argparse
    import sys

    # Fix Windows console encoding for unicode characters
    if sys.platform == "win32":
        sys.stdout.reconfigure(encoding='utf-8', errors='replace')

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s"
    )

    parser = argparse.ArgumentParser(description="Polymarket API Client")
    parser.add_argument("--discover", action="store_true", help="Discover active markets")
    parser.add_argument("--sentiment", action="store_true", help="Get aggregated sentiment")
    parser.add_argument("--search", type=str, help="Search for markets by keyword")
    parser.add_argument("--slug", type=str, help="Get market by slug")
    parser.add_argument("--limit", type=int, default=20, help="Limit results")
    args = parser.parse_args()

    client = PolymarketClient()

    if args.discover:
        print("\n" + "="*70)
        print("TOP ACTIVE MARKETS BY VOLUME")
        print("="*70)

        markets = client.get_active_markets(limit=args.limit)
        for i, market in enumerate(markets, 1):
            print(f"\n{i}. {market.question[:70]}")
            print(f"   Slug: {market.slug}")
            print(f"   Yes: {market.yes_price*100:.1f}% | No: {market.no_price*100:.1f}%")
            print(f"   Volume: ${market.volume:,.0f} | Liquidity: ${market.liquidity:,.0f}")
            if market.tags:
                print(f"   Tags: {', '.join(market.tags[:5])}")

    elif args.sentiment:
        print("\n" + "="*70)
        print("PREDICTION MARKET SENTIMENT")
        print("="*70)

        sentiment = client.get_market_sentiment()

        print(f"\nFed Dovish (Rate Cut) Probability: {sentiment.fed_dovish_prob*100:.1f}%" if sentiment.fed_dovish_prob else "\nFed Dovish: N/A")
        print(f"Fed Hawkish (Rate Hike) Probability: {sentiment.fed_hawkish_prob*100:.1f}%" if sentiment.fed_hawkish_prob else "Fed Hawkish: N/A")
        print(f"Recession Probability: {sentiment.recession_prob*100:.1f}%" if sentiment.recession_prob else "Recession: N/A")
        print(f"Market Bullish Probability: {sentiment.market_bullish_prob*100:.1f}%" if sentiment.market_bullish_prob else "Market Bullish: N/A")
        print(f"High Volatility Probability: {sentiment.high_volatility_prob*100:.1f}%" if sentiment.high_volatility_prob else "High Volatility: N/A")

        print(f"\n>>> OVERALL RISK LEVEL: {sentiment.overall_risk_level}")

        adj = sentiment.get_trading_adjustment()
        print(f">>> Trading Adjustment: {adj['size_mult']:.0%} position size")
        print(f">>> Note: {adj['note']}")

        if sentiment.raw_markets:
            print(f"\nMarkets analyzed ({len(sentiment.raw_markets)}):")
            for m in sentiment.raw_markets[:10]:
                print(f"  - {m.question[:60]}... ({m.yes_price*100:.1f}%)")

    elif args.search:
        print(f"\n" + "="*70)
        print(f"SEARCH RESULTS: '{args.search}'")
        print("="*70)

        markets = client.search_markets([args.search], limit=args.limit)
        if not markets:
            print("\nNo markets found.")
        else:
            for i, market in enumerate(markets, 1):
                print(f"\n{i}. {market.question[:70]}")
                print(f"   Yes: {market.yes_price*100:.1f}% | Volume: ${market.volume:,.0f}")

    elif args.slug:
        market = client.get_market_by_slug(args.slug)
        if market:
            print(f"\n{market.question}")
            print(f"Yes: {market.yes_price*100:.1f}% | No: {market.no_price*100:.1f}%")
            print(f"Volume: ${market.volume:,.0f}")
            print(f"Description: {market.description[:200]}...")
        else:
            print(f"Market not found: {args.slug}")

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
