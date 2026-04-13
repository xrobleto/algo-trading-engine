"""
Enhanced News Filtering and Formatting for HYSA Phase 1

Improvements over basic headline filtering:
1. Better relevance filtering (checks all tickers, not just primary)
2. Shows article descriptions/summaries (not just titles)
3. Includes publish date for context
4. Scores relevance to filter out tangential mentions
5. Optionally fetches full article content if available

Usage:
    from enhanced_news_filter import enhanced_score_news

    news_json = polygon_client.get_news(symbol, limit=20)
    score, news_items = enhanced_score_news(news_json, lookback_hours=72, symbol=symbol)

    # news_items is list of dicts with:
    #   - title: str
    #   - description: str (summary)
    #   - published: datetime
    #   - relevance_score: float (0-1)
    #   - url: str
"""

import datetime as dt
from typing import Dict, Any, List, Tuple, Optional
import logging


def calculate_relevance_score(article: Dict[str, Any], symbol: str) -> float:
    """
    Calculate how relevant an article is to the given symbol.

    Returns:
        float: 0.0 (not relevant) to 1.0 (highly relevant)
    """
    symbol_upper = symbol.upper()
    title = (article.get("title") or "").upper()
    description = (article.get("description") or "").upper()
    tickers = article.get("tickers", []) or []

    score = 0.0

    # Strong signals (high relevance)
    if tickers and len(tickers) > 0:
        primary_ticker = tickers[0].upper()
        if primary_ticker == symbol_upper:
            score += 0.5  # Primary ticker match

    # Check if symbol appears in title
    if symbol_upper in title:
        # Strong signal if symbol is a standalone word (not part of another word)
        words_in_title = title.split()
        if symbol_upper in words_in_title:
            score += 0.4  # Standalone mention in title
        else:
            score += 0.2  # Mentioned in title but possibly part of another word

    # Check if symbol appears in description
    if symbol_upper in description:
        words_in_desc = description.split()
        if symbol_upper in words_in_desc:
            score += 0.2  # Standalone mention in description
        else:
            score += 0.1  # Mentioned in description

    # Check position in ticker list (earlier = more relevant)
    if tickers:
        try:
            ticker_index = next(i for i, t in enumerate(tickers) if t.upper() == symbol_upper)
            # First 3 tickers are most relevant
            if ticker_index == 0:
                score += 0.3
            elif ticker_index == 1:
                score += 0.2
            elif ticker_index == 2:
                score += 0.1
        except StopIteration:
            pass  # Symbol not in ticker list

    return min(score, 1.0)  # Cap at 1.0


def enhanced_score_news(
    news_json: Dict[str, Any],
    lookback_hours: int,
    symbol: str = "",
    min_relevance_score: float = 0.3,
    max_articles: int = 5,
    include_descriptions: bool = True
) -> Tuple[Optional[float], List[Dict[str, Any]]]:
    """
    Enhanced news scoring with better relevance filtering and detailed article info.

    Args:
        news_json: Polygon news API response
        lookback_hours: Only consider news from this many hours ago
        symbol: Ticker symbol to filter for
        min_relevance_score: Minimum relevance score to include (0.0-1.0)
                           0.3 = moderate relevance (recommended)
                           0.5 = high relevance only
                           0.1 = include tangential mentions
        max_articles: Maximum number of articles to return
        include_descriptions: If True, include article descriptions/summaries

    Returns:
        Tuple of (sentiment_score, news_items)

        news_items is a list of dicts with:
        - title: Article title
        - description: Article summary (if available and include_descriptions=True)
        - published: Datetime of publication
        - published_str: Human-readable publish time
        - relevance_score: How relevant (0.0-1.0)
        - url: Article URL
        - sentiment: "positive" | "negative" | "neutral" | None
    """
    results = news_json.get("results", []) or []
    if not results:
        return None, []

    cutoff = dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=lookback_hours)
    sentiments = []
    articles = []

    for r in results:
        title = (r.get("title") or "").strip()
        description = (r.get("description") or "").strip()
        article_url = r.get("article_url") or r.get("amp_url") or ""

        if not title:
            continue

        # Parse publish date
        published = r.get("published_utc")
        try:
            published_dt = dt.datetime.fromisoformat(published.replace("Z", "+00:00")) if published else None
        except Exception:
            published_dt = None

        # Skip old articles
        if published_dt and published_dt < cutoff:
            continue

        # Calculate relevance score
        relevance_score = 0.0
        if symbol:
            relevance_score = calculate_relevance_score(r, symbol)

            # Skip irrelevant articles
            if relevance_score < min_relevance_score:
                continue

        # Format publish time
        published_str = ""
        if published_dt:
            now = dt.datetime.now(dt.timezone.utc)
            delta = now - published_dt

            if delta.days > 0:
                published_str = f"{delta.days}d ago"
            elif delta.seconds >= 3600:
                hours = delta.seconds // 3600
                published_str = f"{hours}h ago"
            else:
                minutes = delta.seconds // 60
                published_str = f"{minutes}m ago"

        # Extract sentiment from insights
        article_sentiment = None
        insights = r.get("insights") or []
        for ins in insights:
            s = (ins.get("sentiment") or ins.get("sentiment_reasoning") or "").lower()
            if "positive" in s and "not" not in s:
                sentiments.append(1.0)
                article_sentiment = "positive"
                break  # Use first sentiment found
            elif "negative" in s:
                sentiments.append(-1.0)
                article_sentiment = "negative"
                break
            elif "neutral" in s:
                sentiments.append(0.0)
                article_sentiment = "neutral"
                break

        # Build article dict
        article_dict = {
            "title": title,
            "published": published_dt,
            "published_str": published_str,
            "relevance_score": relevance_score,
            "url": article_url,
            "sentiment": article_sentiment
        }

        # Add description if requested and available
        if include_descriptions and description:
            # Truncate very long descriptions
            if len(description) > 300:
                description = description[:297] + "..."
            article_dict["description"] = description

        articles.append(article_dict)

    # Sort by relevance score (descending), then by publish date (most recent first)
    articles.sort(key=lambda x: (x["relevance_score"], x["published"] or dt.datetime.min.replace(tzinfo=dt.timezone.utc)), reverse=True)

    # Limit to max_articles
    articles = articles[:max_articles]

    # Calculate sentiment score
    if sentiments:
        import numpy as np
        sentiment_score = float(np.clip(np.mean(sentiments), -1.0, 1.0))
        return sentiment_score, articles

    return None, articles


def format_news_for_text(news_items: List[Dict[str, Any]], include_descriptions: bool = True) -> str:
    """
    Format news items for plain text email.

    Args:
        news_items: List of news items from enhanced_score_news
        include_descriptions: If True, include article descriptions

    Returns:
        Formatted text string
    """
    if not news_items:
        return ""

    lines = []
    lines.append("  Recent News:")

    for i, article in enumerate(news_items, 1):
        title = article["title"]
        published_str = article.get("published_str", "")
        relevance = article.get("relevance_score", 0.0)
        sentiment = article.get("sentiment")

        # Format title line
        sentiment_indicator = ""
        if sentiment == "positive":
            sentiment_indicator = " [↗ Positive]"
        elif sentiment == "negative":
            sentiment_indicator = " [↘ Negative]"

        lines.append(f"   {i}. {title}{sentiment_indicator}")

        # Add metadata line
        metadata_parts = []
        if published_str:
            metadata_parts.append(published_str)
        if relevance > 0:
            metadata_parts.append(f"relevance: {relevance:.0%}")
        if metadata_parts:
            lines.append(f"      ({', '.join(metadata_parts)})")

        # Add description if available
        if include_descriptions and "description" in article:
            desc = article["description"]
            # Indent description
            lines.append(f"      {desc}")

        lines.append("")  # Blank line between articles

    return "\n".join(lines)


def format_news_for_html(news_items: List[Dict[str, Any]], include_descriptions: bool = True) -> str:
    """
    Format news items for HTML email.

    Args:
        news_items: List of news items from enhanced_score_news
        include_descriptions: If True, include article descriptions

    Returns:
        HTML string
    """
    if not news_items:
        return ""

    def esc(s):
        return str(s).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")

    html_parts = []
    html_parts.append("<div style='margin-top:10px;color:#555;font-size:13px;'>")
    html_parts.append("<b>Recent News:</b>")
    html_parts.append("<ul style='margin:6px 0 0 18px;padding:0;list-style-type:none;'>")

    for article in news_items:
        title = article["title"]
        url = article.get("url", "")
        published_str = article.get("published_str", "")
        relevance = article.get("relevance_score", 0.0)
        sentiment = article.get("sentiment")
        description = article.get("description", "")

        # Sentiment badge
        sentiment_badge = ""
        if sentiment == "positive":
            sentiment_badge = "<span style='display:inline-block;margin-left:6px;padding:1px 6px;border-radius:3px;background:#E8F5E9;color:#1B5E20;font-size:10px;font-weight:700;'>↗ Positive</span>"
        elif sentiment == "negative":
            sentiment_badge = "<span style='display:inline-block;margin-left:6px;padding:1px 6px;border-radius:3px;background:#FFEBEE;color:#B71C1C;font-size:10px;font-weight:700;'>↘ Negative</span>"

        # Build article HTML
        html_parts.append("<li style='margin:8px 0;padding-bottom:8px;border-bottom:1px solid #eee;'>")

        # Title (with link if available)
        if url:
            html_parts.append(f"<div style='font-weight:600;color:#222;'><a href='{esc(url)}' style='color:#1565C0;text-decoration:none;'>{esc(title)}</a>{sentiment_badge}</div>")
        else:
            html_parts.append(f"<div style='font-weight:600;color:#222;'>{esc(title)}{sentiment_badge}</div>")

        # Metadata
        metadata_parts = []
        if published_str:
            metadata_parts.append(f"<span style='color:#888;'>{esc(published_str)}</span>")
        if relevance > 0:
            metadata_parts.append(f"<span style='color:#888;'>relevance: {relevance:.0%}</span>")
        if metadata_parts:
            html_parts.append(f"<div style='font-size:11px;margin-top:2px;'>{' • '.join(metadata_parts)}</div>")

        # Description
        if include_descriptions and description:
            html_parts.append(f"<div style='margin-top:4px;color:#555;font-size:12px;line-height:1.4;'>{esc(description)}</div>")

        html_parts.append("</li>")

    html_parts.append("</ul>")
    html_parts.append("</div>")

    return "".join(html_parts)


# Example usage and testing
if __name__ == "__main__":
    # Test with sample Polygon news response
    sample_news = {
        "results": [
            {
                "title": "Apple Announces Record Q4 Earnings, Stock Surges",
                "description": "Apple Inc. reported better-than-expected quarterly earnings driven by strong iPhone sales and services revenue growth.",
                "published_utc": "2026-01-02T14:30:00Z",
                "tickers": ["AAPL", "GOOGL"],
                "article_url": "https://example.com/apple-earnings",
                "insights": [
                    {"sentiment": "positive", "sentiment_reasoning": "Strong earnings beat"}
                ]
            },
            {
                "title": "Tech Sector Rally Continues as FAANG Stocks Climb",
                "description": "Major technology stocks including Apple, Amazon, and Google saw gains today amid broader market optimism.",
                "published_utc": "2026-01-02T13:00:00Z",
                "tickers": ["AMZN", "GOOGL", "AAPL", "META"],
                "article_url": "https://example.com/tech-rally",
                "insights": [
                    {"sentiment": "neutral"}
                ]
            },
            {
                "title": "Federal Reserve Signals Rate Cuts Ahead",
                "description": "The Federal Reserve indicated potential interest rate cuts in 2026, boosting investor sentiment across markets.",
                "published_utc": "2026-01-02T10:00:00Z",
                "tickers": ["SPY", "QQQ"],
                "article_url": "https://example.com/fed-rates",
                "insights": []
            }
        ]
    }

    # Test AAPL relevance filtering
    print("=" * 80)
    print("Testing enhanced news filtering for AAPL")
    print("=" * 80)

    score, items = enhanced_score_news(
        sample_news,
        lookback_hours=72,
        symbol="AAPL",
        min_relevance_score=0.3,
        include_descriptions=True
    )

    print(f"\nSentiment score: {score}")
    print(f"Number of relevant articles: {len(items)}\n")

    for item in items:
        print(f"Title: {item['title']}")
        print(f"  Relevance: {item['relevance_score']:.1%}")
        print(f"  Published: {item['published_str']}")
        print(f"  Sentiment: {item['sentiment']}")
        if "description" in item:
            print(f"  Summary: {item['description']}")
        print()

    print("\n" + "=" * 80)
    print("Text format:")
    print("=" * 80)
    print(format_news_for_text(items))

    print("\n" + "=" * 80)
    print("HTML format:")
    print("=" * 80)
    print(format_news_for_html(items))
