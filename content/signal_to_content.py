"""
Signal to Content Engine
Converts trading signals to Instagram posts
Reads from: alert state files, newsletter outputs
Outputs: content_queue.json, generated images
"""

import json
import os
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, List, Dict, Any
from dotenv import load_dotenv
import anthropic

# Import image generator
from generate_image import (
    generate_trade_alert,
    generate_market_context,
    generate_results,
    generate_setup_teaser,
    generate_educational
)

# Load environment variables
load_dotenv()

# Paths
BASE_DIR = Path(__file__).parent.parent
CONTENT_DIR = Path(__file__).parent
_output_root = Path(os.getenv("ALGO_OUTPUT_DIR", "")) if os.getenv("ALGO_OUTPUT_DIR") else BASE_DIR
_content_output = _output_root / "content" if os.getenv("ALGO_OUTPUT_DIR") else CONTENT_DIR
STATE_DIR = _output_root / "data" / "state"
QUEUE_FILE = _content_output / "content_queue.json"
_content_output.mkdir(parents=True, exist_ok=True)

# Alert state files
BUY_ALERT_STATE = STATE_DIR / "buy_alert_state.json"
SELL_ALERT_STATE = STATE_DIR / "alert_state.json"
SWING_STATE = STATE_DIR / "swing_newsletter_state.json"

# Instagram Scraper insights (for educational content)
SCRAPER_INSIGHTS = Path(os.getenv("INSTAGRAM_SCRAPER_PATH", "C:/Users/xrobl/Documents/Instagram_Scraper")) / ".tmp" / "insights.json"

# Initialize Anthropic client
client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))

# Your Instagram handle
INSTAGRAM_HANDLE = os.getenv("INSTAGRAM_HANDLE", "@yourhandle")


# =============================================================================
# CONTENT QUEUE MANAGEMENT
# =============================================================================

def load_queue() -> Dict:
    """Load existing content queue"""
    if QUEUE_FILE.exists():
        with open(QUEUE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"posts": [], "last_updated": None}


def save_queue(queue: Dict):
    """Save content queue"""
    queue["last_updated"] = datetime.now().isoformat()
    with open(QUEUE_FILE, "w", encoding="utf-8") as f:
        json.dump(queue, f, indent=2, ensure_ascii=False)


def add_to_queue(post: Dict):
    """Add a post to the queue"""
    queue = load_queue()

    # Check for duplicates (same type + ticker within 24 hours)
    recent_cutoff = datetime.now() - timedelta(hours=24)
    for existing in queue["posts"]:
        if existing.get("status") == "pending":
            existing_time = datetime.fromisoformat(existing.get("created_at", "2000-01-01"))
            if existing_time > recent_cutoff:
                if (existing.get("type") == post.get("type") and
                    existing.get("ticker") == post.get("ticker")):
                    print(f"  [SKIP] Duplicate post: {post['type']} - {post.get('ticker', 'N/A')}")
                    return False

    post["id"] = str(uuid.uuid4())[:8]
    post["created_at"] = datetime.now().isoformat()
    post["status"] = "pending"

    queue["posts"].append(post)
    save_queue(queue)
    print(f"  [OK] Added to queue: {post['type']} - {post.get('ticker', 'N/A')}")
    return True


def get_pending_posts() -> List[Dict]:
    """Get all pending posts"""
    queue = load_queue()
    return [p for p in queue["posts"] if p.get("status") == "pending"]


def mark_post_status(post_id: str, status: str):
    """Update post status (pending, scheduled, posted, failed)"""
    queue = load_queue()
    for post in queue["posts"]:
        if post["id"] == post_id:
            post["status"] = status
            post["updated_at"] = datetime.now().isoformat()
            break
    save_queue(queue)


# =============================================================================
# SIGNAL READERS
# =============================================================================

def load_buy_alerts() -> List[Dict]:
    """Load recent buy alerts from state file"""
    if not BUY_ALERT_STATE.exists():
        return []

    with open(BUY_ALERT_STATE, "r", encoding="utf-8") as f:
        state = json.load(f)

    alerts = []
    recent_cutoff = datetime.now() - timedelta(hours=24)

    for ticker, data in state.get("sent", {}).items():
        ts = datetime.fromisoformat(data["ts_utc"].replace("+00:00", ""))
        if ts > recent_cutoff:
            alerts.append({
                "ticker": ticker,
                "timestamp": ts,
                "hash": data["hash"]
            })

    return sorted(alerts, key=lambda x: x["timestamp"], reverse=True)


def load_sell_alerts() -> List[Dict]:
    """Load recent sell alerts from state file"""
    if not SELL_ALERT_STATE.exists():
        return []

    with open(SELL_ALERT_STATE, "r", encoding="utf-8") as f:
        state = json.load(f)

    alerts = []
    recent_cutoff = datetime.now() - timedelta(hours=24)

    for ticker, data in state.get("sent", {}).items():
        ts = datetime.fromisoformat(data["ts_utc"].replace("+00:00", ""))
        if ts > recent_cutoff:
            alerts.append({
                "ticker": ticker,
                "timestamp": ts,
                "hash": data["hash"]
            })

    return sorted(alerts, key=lambda x: x["timestamp"], reverse=True)


def load_scraper_insights() -> List[Dict]:
    """Load educational insights from Instagram scraper"""
    if not SCRAPER_INSIGHTS.exists():
        return []

    with open(SCRAPER_INSIGHTS, "r", encoding="utf-8") as f:
        data = json.load(f)

    return data.get("insights", [])


# =============================================================================
# CONTENT GENERATORS
# =============================================================================

def generate_caption_with_ai(content_type: str, data: Dict) -> str:
    """Generate engaging caption using Claude"""

    prompts = {
        "trade_alert": f"""Write a short, punchy Instagram caption for a trade alert.
Ticker: {data.get('ticker')}
Signal: {data.get('signal_type', 'BUY ALERT')}
Conviction: {data.get('conviction', 'HIGH')}

Rules:
- 2-3 sentences max
- Start with a hook (question, bold statement, or pattern interrupt)
- Mention the ticker once
- End with "Link in bio for full analysis"
- Don't use emojis excessively (max 2-3)
- Sound confident but not arrogant

Return ONLY the caption text, nothing else.""",

        "market_context": f"""Write a short Instagram caption for a market open post.
SPY Trend: {data.get('spy_trend')}
VIX: {data.get('vix_level')}
Leaders: {data.get('sector_leaders')}

Rules:
- 2-3 sentences max
- Mention what you're watching today
- Tease that you have setups ready
- End with "Link in bio for today's plays"
- Keep it conversational

Return ONLY the caption text, nothing else.""",

        "results": f"""Write a short Instagram caption for a weekly results post.
P&L: {data.get('pnl_pct')}
Winners: {data.get('winners')}

Rules:
- Be humble but factual
- 2-3 sentences
- Mention that full breakdown is in newsletter
- End with "Link in bio for free newsletter"

Return ONLY the caption text, nothing else.""",

        "setup_teaser": f"""Write a short Instagram caption teasing a premium trade setup.
Ticker: {data.get('ticker')}
Pattern: {data.get('pattern')}
Days Ago: {data.get('days_ago')}

Rules:
- Create FOMO without being sleazy
- Mention that premium subscribers got this X days ago
- 2-3 sentences
- End with "Link in bio to join"

Return ONLY the caption text, nothing else.""",

        "educational": f"""Write a short Instagram caption for an educational trading tip.
Topic: {data.get('topic')}

Rules:
- Hook them with why this matters
- 2-3 sentences
- End with "More tips in my free newsletter - link in bio"
- Sound like a knowledgeable friend, not a guru

Return ONLY the caption text, nothing else."""
    }

    prompt = prompts.get(content_type, prompts["trade_alert"])

    try:
        message = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=300,
            messages=[{"role": "user", "content": prompt}]
        )
        return message.content[0].text.strip()
    except Exception as e:
        print(f"  [X] AI caption failed: {e}")
        return f"New trading insight. Link in bio for details. #{data.get('ticker', 'trading')}"


def generate_hashtags(content_type: str, ticker: str = None) -> List[str]:
    """Generate relevant hashtags"""
    base_tags = ["#trading", "#stockmarket", "#investing", "#trader", "#stocks"]

    type_tags = {
        "trade_alert": ["#buyalert", "#stockalert", "#tradingalert", "#stockpicks"],
        "market_context": ["#marketopen", "#premarket", "#marketanalysis", "#daytrading"],
        "results": ["#tradingresults", "#pnl", "#portfolio", "#winrate"],
        "setup_teaser": ["#swingtrading", "#technicalanalysis", "#chartpatterns", "#tradingsetup"],
        "educational": ["#tradingtips", "#learntoday", "#stockeducation", "#tradingwisdom"]
    }

    tags = base_tags + type_tags.get(content_type, [])

    if ticker:
        tags.append(f"#{ticker.lower()}")
        tags.append(f"${ticker.upper()}")

    return tags[:15]  # Instagram allows max 30, but 10-15 is optimal


# =============================================================================
# CONTENT TYPE HANDLERS
# =============================================================================

def create_trade_alert_post(ticker: str, conviction: str = "HIGH", score: int = 80,
                            headline: str = None, signal_type: str = "BUY ALERT") -> Optional[Dict]:
    """Create a trade alert post"""
    print(f"\n  Creating trade alert for ${ticker}...")

    if not headline:
        headline = f"Potential setup forming on ${ticker}"

    # Generate image
    image_path = generate_trade_alert(
        ticker=ticker,
        conviction=conviction,
        score=score,
        headline=headline,
        signal_type=signal_type,
        handle=INSTAGRAM_HANDLE
    )

    # Generate caption
    caption = generate_caption_with_ai("trade_alert", {
        "ticker": ticker,
        "conviction": conviction,
        "signal_type": signal_type
    })

    # Generate hashtags
    hashtags = generate_hashtags("trade_alert", ticker)

    return {
        "type": "trade_alert",
        "ticker": ticker,
        "image_path": str(image_path),
        "caption": caption,
        "hashtags": hashtags,
        "data": {
            "conviction": conviction,
            "score": score,
            "signal_type": signal_type
        }
    }


def create_market_context_post(spy_trend: str, spy_change: str, vix_level: float,
                               sector_leaders: List[str], sector_laggards: List[str],
                               setups_count: int = 3,
                               futures: Dict[str, str] = None,
                               key_levels: Dict[str, str] = None,
                               market_breadth: Dict[str, int] = None,
                               watchlist: List[str] = None,
                               catalyst: str = None,
                               insight: str = None) -> Optional[Dict]:
    """Create a market open/context post with enhanced data"""
    print(f"\n  Creating market context post...")

    # Generate image with all available data
    image_path = generate_market_context(
        spy_trend=spy_trend,
        spy_change=spy_change,
        vix_level=vix_level,
        sector_leaders=sector_leaders,
        sector_laggards=sector_laggards,
        futures=futures,
        key_levels=key_levels,
        market_breadth=market_breadth,
        watchlist=watchlist,
        catalyst=catalyst,
        insight=insight,
        setups_count=setups_count,
        handle=INSTAGRAM_HANDLE
    )

    # Generate caption
    caption = generate_caption_with_ai("market_context", {
        "spy_trend": spy_trend,
        "spy_change": spy_change,
        "vix_level": vix_level,
        "sector_leaders": ", ".join(sector_leaders)
    })

    # Generate hashtags
    hashtags = generate_hashtags("market_context")

    return {
        "type": "market_context",
        "ticker": None,
        "image_path": str(image_path),
        "caption": caption,
        "hashtags": hashtags,
        "data": {
            "spy_trend": spy_trend,
            "spy_change": spy_change,
            "vix_level": vix_level,
            "futures": futures,
            "key_levels": key_levels
        }
    }


def create_results_post(period: str, pnl_pct: str, winners: List[str],
                        losers: List[str], win_rate: int = None) -> Optional[Dict]:
    """Create a results/performance post"""
    print(f"\n  Creating {period} results post...")

    # Generate image
    image_path = generate_results(
        period=period,
        pnl_pct=pnl_pct,
        winners=winners,
        losers=losers,
        win_rate=win_rate,
        handle=INSTAGRAM_HANDLE
    )

    # Generate caption
    caption = generate_caption_with_ai("results", {
        "pnl_pct": pnl_pct,
        "winners": ", ".join(winners)
    })

    # Generate hashtags
    hashtags = generate_hashtags("results")

    return {
        "type": "results",
        "ticker": None,
        "image_path": str(image_path),
        "caption": caption,
        "hashtags": hashtags,
        "data": {
            "period": period,
            "pnl_pct": pnl_pct,
            "win_rate": win_rate
        }
    }


def create_setup_teaser_post(ticker: str, pattern: str, risk_reward: str,
                             days_ago: int = 3) -> Optional[Dict]:
    """Create a setup teaser post (premium content teaser)"""
    print(f"\n  Creating setup teaser for ${ticker}...")

    # Generate image
    image_path = generate_setup_teaser(
        ticker=ticker,
        pattern=pattern,
        risk_reward=risk_reward,
        days_ago=days_ago,
        handle=INSTAGRAM_HANDLE
    )

    # Generate caption
    caption = generate_caption_with_ai("setup_teaser", {
        "ticker": ticker,
        "pattern": pattern,
        "days_ago": days_ago
    })

    # Generate hashtags
    hashtags = generate_hashtags("setup_teaser", ticker)

    return {
        "type": "setup_teaser",
        "ticker": ticker,
        "image_path": str(image_path),
        "caption": caption,
        "hashtags": hashtags,
        "data": {
            "pattern": pattern,
            "risk_reward": risk_reward,
            "days_ago": days_ago
        }
    }


def create_educational_post(topic: str, tip_text: str, tip_number: int = None) -> Optional[Dict]:
    """Create an educational tip post"""
    print(f"\n  Creating educational post: {topic[:30]}...")

    # Generate image
    image_path = generate_educational(
        topic=topic,
        tip_text=tip_text,
        tip_number=tip_number,
        handle=INSTAGRAM_HANDLE
    )

    # Generate caption
    caption = generate_caption_with_ai("educational", {
        "topic": topic
    })

    # Generate hashtags
    hashtags = generate_hashtags("educational")

    return {
        "type": "educational",
        "ticker": None,
        "image_path": str(image_path),
        "caption": caption,
        "hashtags": hashtags,
        "data": {
            "topic": topic,
            "tip_number": tip_number
        }
    }


# =============================================================================
# SCHEDULED CONTENT GENERATION
# =============================================================================

def generate_morning_content():
    """Generate morning market context post (8:30 AM)"""
    print("\n" + "=" * 50)
    print("MORNING CONTENT GENERATION")
    print("=" * 50)

    # TODO: Read actual market data from your swing_newsletter output
    # For now, using placeholder data - replace with actual data source

    # Check if swing newsletter state has recent data
    market_data = {
        "spy_trend": "bullish",
        "spy_change": "+0.5%",
        "vix_level": 15.2,
        "sector_leaders": ["XLK", "XLY", "XLC", "XLI"],
        "sector_laggards": ["XLE", "XLF", "XLRE"],
        "setups_count": 3,
        "futures": {"ES": "+0.3%", "NQ": "+0.5%", "RTY": "+0.1%"},
        "key_levels": {"support": "590", "resistance": "600"},
        "market_breadth": {"advancing": 62, "declining": 38},
        "watchlist": ["NVDA", "AAPL", "META", "TSLA"],
        "catalyst": None,  # Set to "FOMC @ 2PM" or "NVDA Earnings" when relevant
        "insight": "Risk-on sentiment continues. Watch for breakouts in tech names."
    }

    post = create_market_context_post(**market_data)
    if post:
        # Schedule for 8:30 AM
        post["scheduled_time"] = datetime.now().replace(hour=8, minute=30).isoformat()
        add_to_queue(post)


def generate_midday_content():
    """Generate midday educational post (12:00 PM)"""
    print("\n" + "=" * 50)
    print("MIDDAY CONTENT GENERATION")
    print("=" * 50)

    # Try to get insights from Instagram scraper
    insights = load_scraper_insights()

    if insights:
        # Pick a random insight to base content on
        import random
        insight = random.choice(insights)
        topic = insight.get("insights", {}).get("main_topic", "Trading tip of the day")
        key_insights = insight.get("insights", {}).get("key_insights", [])
        tip_text = key_insights[0] if key_insights else "Always manage your risk before entering a trade."
    else:
        # Fallback educational content
        tips = [
            ("Why I never chase extended stocks",
             "When a stock is more than 8% above its 20 EMA, the risk/reward flips against you. Wait for a pullback to support, or find another setup."),
            ("The 2% rule that saved my account",
             "Never risk more than 2% of your account on a single trade. This means sizing your position based on your stop loss, not your conviction."),
            ("Volume tells the real story",
             "Price can lie, but volume doesn't. A breakout without volume is just a trap. Wait for confirmation before entering."),
        ]
        import random
        topic, tip_text = random.choice(tips)

    post = create_educational_post(
        topic=topic,
        tip_text=tip_text,
        tip_number=random.randint(1, 100)
    )
    if post:
        post["scheduled_time"] = datetime.now().replace(hour=12, minute=30).isoformat()
        add_to_queue(post)


def generate_afternoon_content():
    """Generate afternoon trade recap (4:30 PM)"""
    print("\n" + "=" * 50)
    print("AFTERNOON CONTENT GENERATION")
    print("=" * 50)

    # Check for buy/sell alerts from today
    buy_alerts = load_buy_alerts()
    sell_alerts = load_sell_alerts()

    if buy_alerts:
        # Create a trade alert post for the highest conviction alert
        alert = buy_alerts[0]  # Most recent
        post = create_trade_alert_post(
            ticker=alert["ticker"],
            conviction="HIGH",
            score=80,
            headline=f"Today's setup on ${alert['ticker']}",
            signal_type="BUY ALERT"
        )
        if post:
            post["scheduled_time"] = datetime.now().replace(hour=17, minute=0).isoformat()
            add_to_queue(post)
    elif sell_alerts:
        # Create content about sells
        print("  [INFO] Sell alerts found, but no buy alert content generated")
    else:
        print("  [INFO] No alerts from today")


def generate_evening_content():
    """Generate evening setup teaser (7:00 PM)"""
    print("\n" + "=" * 50)
    print("EVENING CONTENT GENERATION")
    print("=" * 50)

    # TODO: Read actual setup data from swing_newsletter
    # For now, using placeholder - replace with actual data source

    buy_alerts = load_buy_alerts()

    if buy_alerts and len(buy_alerts) > 1:
        # Use a previous alert as "teaser" content
        old_alert = buy_alerts[-1] if len(buy_alerts) > 1 else buy_alerts[0]

        post = create_setup_teaser_post(
            ticker=old_alert["ticker"],
            pattern="technical_setup",
            risk_reward="1:2.5",
            days_ago=3
        )
        if post:
            post["scheduled_time"] = datetime.now().replace(hour=19, minute=30).isoformat()
            add_to_queue(post)
    else:
        print("  [INFO] Not enough historical alerts for teaser content")


def generate_weekly_results():
    """Generate weekly results post (Fridays)"""
    print("\n" + "=" * 50)
    print("WEEKLY RESULTS GENERATION")
    print("=" * 50)

    # TODO: Calculate actual P&L from your tracking system
    # For now, using placeholder - replace with actual data

    post = create_results_post(
        period="WEEKLY",
        pnl_pct="+3.2%",
        winners=["NVDA", "META"],
        losers=["TSLA"],
        win_rate=67
    )
    if post:
        post["scheduled_time"] = datetime.now().replace(hour=18, minute=0).isoformat()
        add_to_queue(post)


# =============================================================================
# MAIN
# =============================================================================

def run_scheduled_generation(time_slot: str = None):
    """
    Run content generation based on time slot

    Time slots:
    - morning: 8:00 AM - Market context
    - midday: 12:00 PM - Educational
    - afternoon: 4:30 PM - Trade recap
    - evening: 7:00 PM - Setup teaser
    - weekly: Fridays - Results

    If no time_slot provided, determines automatically from current time.
    """

    if time_slot is None:
        # Auto-detect based on current time
        hour = datetime.now().hour
        day = datetime.now().weekday()

        if 7 <= hour < 10:
            time_slot = "morning"
        elif 11 <= hour < 14:
            time_slot = "midday"
        elif 16 <= hour < 18:
            time_slot = "afternoon"
        elif 18 <= hour < 21:
            time_slot = "evening"
            if day == 4:  # Friday
                time_slot = "weekly"
        else:
            print(f"[INFO] Current hour ({hour}) not in scheduled content window")
            return

    print(f"\n{'=' * 60}")
    print(f"SIGNAL TO CONTENT ENGINE")
    print(f"Time slot: {time_slot}")
    print(f"{'=' * 60}")

    generators = {
        "morning": generate_morning_content,
        "midday": generate_midday_content,
        "afternoon": generate_afternoon_content,
        "evening": generate_evening_content,
        "weekly": generate_weekly_results,
        "all": lambda: [f() for f in [
            generate_morning_content,
            generate_midday_content,
            generate_afternoon_content,
            generate_evening_content
        ]]
    }

    generator = generators.get(time_slot)
    if generator:
        generator()
    else:
        print(f"[X] Unknown time slot: {time_slot}")

    # Print queue summary
    pending = get_pending_posts()
    print(f"\n{'=' * 60}")
    print(f"QUEUE SUMMARY: {len(pending)} pending posts")
    print(f"{'=' * 60}")
    for post in pending[-5:]:  # Show last 5
        print(f"  [{post['type']}] {post.get('ticker', 'N/A')} - {post['status']}")


def main():
    """Main entry point"""
    import sys

    if len(sys.argv) > 1:
        time_slot = sys.argv[1]
    else:
        time_slot = None  # Auto-detect

    run_scheduled_generation(time_slot)


if __name__ == "__main__":
    main()
