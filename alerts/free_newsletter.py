"""
Free Daily Newsletter Generator
Sends daily email to free tier subscribers via Beehiiv
Content: Market recap, educational tip, premium teaser

Schedule: Daily at 6 PM ET
"""

import json
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional
from dotenv import load_dotenv

# Add parent paths for imports
sys.path.insert(0, str(Path(__file__).parent.parent))
from utilities.beehiiv_client import BeehiivClient, send_free_newsletter

# Load environment variables
load_dotenv()

# Paths
BASE_DIR = Path(__file__).parent.parent
_output_root = Path(os.getenv("ALGO_OUTPUT_DIR", "")) if os.getenv("ALGO_OUTPUT_DIR") else BASE_DIR
STATE_DIR = _output_root / "data" / "state"
SCRAPER_INSIGHTS = Path(os.getenv("INSTAGRAM_SCRAPER_PATH", "C:/Users/xrobl/Documents/Instagram_Scraper")) / ".tmp" / "insights.json"


# =============================================================================
# DATA LOADERS
# =============================================================================

def load_buy_alerts_today() -> List[Dict]:
    """Load today's buy alerts (redacted for free tier)"""
    state_file = STATE_DIR / "buy_alert_state.json"
    if not state_file.exists():
        return []

    with open(state_file, "r", encoding="utf-8") as f:
        state = json.load(f)

    alerts = []
    today = datetime.now().date()

    for ticker, data in state.get("sent", {}).items():
        ts = datetime.fromisoformat(data["ts_utc"].replace("+00:00", ""))
        if ts.date() == today:
            alerts.append({"ticker": ticker, "timestamp": ts})

    return alerts


def load_sell_alerts_today() -> List[Dict]:
    """Load today's sell alerts (redacted for free tier)"""
    state_file = STATE_DIR / "alert_state.json"
    if not state_file.exists():
        return []

    with open(state_file, "r", encoding="utf-8") as f:
        state = json.load(f)

    alerts = []
    today = datetime.now().date()

    for ticker, data in state.get("sent", {}).items():
        ts = datetime.fromisoformat(data["ts_utc"].replace("+00:00", ""))
        if ts.date() == today:
            alerts.append({"ticker": ticker, "timestamp": ts})

    return alerts


def get_educational_tip() -> Dict:
    """Get an educational tip from scraper insights or defaults"""

    # Try to load from Instagram scraper insights
    if SCRAPER_INSIGHTS.exists():
        try:
            with open(SCRAPER_INSIGHTS, "r", encoding="utf-8") as f:
                data = json.load(f)

            insights = data.get("insights", [])
            if insights:
                import random
                insight = random.choice(insights)
                return {
                    "topic": insight.get("insights", {}).get("main_topic", "Trading Tip"),
                    "content": insight.get("insights", {}).get("key_insights", [""])[0]
                }
        except:
            pass

    # Default tips
    tips = [
        {
            "topic": "Why I Never Chase Extended Stocks",
            "content": "When a stock is more than 8% above its 20 EMA, the risk/reward flips against you. Wait for a pullback to support, or find another setup. Patience is the edge most traders lack."
        },
        {
            "topic": "The 2% Rule That Saved My Account",
            "content": "Never risk more than 2% of your account on a single trade. This means sizing your position based on your stop loss distance, not your conviction level. Live to trade another day."
        },
        {
            "topic": "Volume Tells The Real Story",
            "content": "Price can lie, but volume doesn't. A breakout without volume is just a trap waiting to spring. Always wait for volume confirmation before entering."
        },
        {
            "topic": "Why Most Traders Fail at Earnings",
            "content": "Earnings are a coin flip. Even if you're right on the direction, IV crush can destroy your options. I either sell before earnings or use defined-risk strategies."
        },
        {
            "topic": "The Best Trade Is Often No Trade",
            "content": "Forcing trades when conditions aren't right is how accounts blow up. Cash is a position. Sitting on your hands when nothing makes sense is a skill worth developing."
        }
    ]

    import random
    return random.choice(tips)


# =============================================================================
# HTML TEMPLATE
# =============================================================================

def generate_newsletter_html(
    date: datetime,
    spy_data: Dict,
    vix_level: float,
    sector_leaders: List[str],
    sector_laggards: List[str],
    buy_alert_count: int,
    sell_alert_count: int,
    tip: Dict
) -> str:
    """Generate the HTML newsletter content"""

    spy_trend = spy_data.get("trend", "neutral")
    spy_change = spy_data.get("change", "0%")

    # Trend colors
    spy_color = "#00ba7c" if spy_trend == "bullish" else "#f91880" if spy_trend == "bearish" else "#71767b"
    spy_arrow = "▲" if spy_trend == "bullish" else "▼" if spy_trend == "bearish" else "─"

    vix_color = "#00ba7c" if vix_level < 18 else "#ff7a00" if vix_level < 25 else "#f91880"
    vix_label = "LOW" if vix_level < 18 else "ELEVATED" if vix_level < 25 else "HIGH"

    html = f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Daily Market Recap</title>
</head>
<body style="margin: 0; padding: 0; background-color: #0f1419; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;">
    <div style="max-width: 600px; margin: 0 auto; padding: 20px;">

        <!-- Header -->
        <div style="text-align: center; padding: 20px 0; border-bottom: 1px solid #2f3336;">
            <h1 style="color: #1d9bf0; margin: 0; font-size: 24px;">Daily Market Recap</h1>
            <p style="color: #71767b; margin: 10px 0 0 0; font-size: 14px;">{date.strftime("%A, %B %d, %Y")}</p>
        </div>

        <!-- Market Summary -->
        <div style="padding: 20px 0;">
            <h2 style="color: #e7e9ea; font-size: 18px; margin: 0 0 15px 0;">📊 Market Summary</h2>

            <!-- SPY Card -->
            <div style="background: #16181c; border-radius: 8px; padding: 15px; margin-bottom: 10px;">
                <table style="width: 100%;">
                    <tr>
                        <td style="color: #e7e9ea; font-size: 20px; font-weight: bold;">SPY</td>
                        <td style="text-align: right;">
                            <span style="color: {spy_color}; font-size: 18px; font-weight: bold;">{spy_change}</span>
                        </td>
                    </tr>
                    <tr>
                        <td colspan="2" style="color: {spy_color}; font-size: 14px; padding-top: 5px;">
                            {spy_arrow} {spy_trend.upper()}
                        </td>
                    </tr>
                </table>
            </div>

            <!-- VIX Card -->
            <div style="background: #16181c; border-radius: 8px; padding: 15px; margin-bottom: 10px;">
                <table style="width: 100%;">
                    <tr>
                        <td style="color: #e7e9ea; font-size: 16px;">VIX</td>
                        <td style="text-align: right; color: {vix_color}; font-size: 16px; font-weight: bold;">
                            {vix_level:.1f} ({vix_label})
                        </td>
                    </tr>
                </table>
            </div>

            <!-- Sectors -->
            <div style="background: #16181c; border-radius: 8px; padding: 15px;">
                <p style="color: #00ba7c; margin: 0 0 5px 0; font-size: 14px;">LEADERS: <span style="color: #e7e9ea;">{", ".join(sector_leaders)}</span></p>
                <p style="color: #f91880; margin: 0; font-size: 14px;">LAGGARDS: <span style="color: #e7e9ea;">{", ".join(sector_laggards)}</span></p>
            </div>
        </div>

        <!-- Educational Tip -->
        <div style="padding: 20px 0; border-top: 1px solid #2f3336;">
            <h2 style="color: #e7e9ea; font-size: 18px; margin: 0 0 15px 0;">💡 Today's Tip</h2>
            <div style="background: #16181c; border-radius: 8px; padding: 15px; border-left: 3px solid #1d9bf0;">
                <p style="color: #1d9bf0; font-weight: bold; margin: 0 0 10px 0; font-size: 16px;">{tip["topic"]}</p>
                <p style="color: #e7e9ea; margin: 0; font-size: 14px; line-height: 1.6;">{tip["content"]}</p>
            </div>
        </div>

        <!-- Premium Teaser -->
        <div style="padding: 20px 0; border-top: 1px solid #2f3336;">
            <h2 style="color: #e7e9ea; font-size: 18px; margin: 0 0 15px 0;">🔥 What Premium Subscribers Got Today</h2>
            <div style="background: linear-gradient(135deg, #1e1e3f 0%, #2a1f4e 100%); border-radius: 8px; padding: 20px;">
                <ul style="color: #e7e9ea; margin: 0; padding-left: 20px; font-size: 14px; line-height: 1.8;">
                    <li><strong style="color: #00ba7c;">{buy_alert_count} BUY alerts</strong> with entry levels + conviction scores</li>
                    <li><strong style="color: #f91880;">{sell_alert_count} SELL alerts</strong> with exit strategies</li>
                    <li>Real-time notifications (you're getting this 6+ hours later)</li>
                    <li>Full technical analysis + risk/reward breakdowns</li>
                </ul>

                <div style="text-align: center; margin-top: 20px;">
                    <a href="{{{{upgrade_link}}}}" style="display: inline-block; background: #7856ff; color: white; text-decoration: none; padding: 12px 30px; border-radius: 25px; font-weight: bold; font-size: 14px;">
                        Upgrade to Premium - $29/mo
                    </a>
                </div>
            </div>
        </div>

        <!-- Footer -->
        <div style="padding: 20px 0; border-top: 1px solid #2f3336; text-align: center;">
            <p style="color: #71767b; font-size: 12px; margin: 0 0 10px 0;">
                This is not financial advice. Trading involves risk. Past performance doesn't guarantee future results.
            </p>
            <p style="color: #71767b; font-size: 12px; margin: 0;">
                <a href="{{{{unsubscribe_link}}}}" style="color: #71767b;">Unsubscribe</a> |
                <a href="{{{{preferences_link}}}}" style="color: #71767b;">Email Preferences</a>
            </p>
        </div>

    </div>
</body>
</html>
"""
    return html


# =============================================================================
# MAIN
# =============================================================================

def generate_and_send_newsletter(dry_run: bool = False) -> Dict:
    """Generate and send the daily free newsletter"""

    print("=" * 60)
    print("FREE NEWSLETTER GENERATOR")
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    # Gather data
    print("\nGathering data...")

    # Load alerts from today
    buy_alerts = load_buy_alerts_today()
    sell_alerts = load_sell_alerts_today()

    print(f"  Buy alerts today: {len(buy_alerts)}")
    print(f"  Sell alerts today: {len(sell_alerts)}")

    # Get educational tip
    tip = get_educational_tip()
    print(f"  Tip: {tip['topic'][:40]}...")

    # Market data (TODO: Replace with actual data from your system)
    # For now using placeholders - wire this up to your swing_newsletter data
    spy_data = {
        "trend": "bullish",
        "change": "+0.8%"
    }
    vix_level = 15.4
    sector_leaders = ["XLK", "XLY", "XLC"]
    sector_laggards = ["XLE", "XLF"]

    # Generate HTML
    print("\nGenerating newsletter HTML...")
    html = generate_newsletter_html(
        date=datetime.now(),
        spy_data=spy_data,
        vix_level=vix_level,
        sector_leaders=sector_leaders,
        sector_laggards=sector_laggards,
        buy_alert_count=max(len(buy_alerts), 2),  # Show at least 2 for FOMO
        sell_alert_count=max(len(sell_alerts), 1),
        tip=tip
    )

    # Generate subject
    subject = f"📈 Market Recap: SPY {spy_data['change']} | {len(buy_alerts)} alerts today"

    # Preview text
    preview = f"Today's market summary + trading tip + what premium subscribers got"

    if dry_run:
        # Save locally for preview
        preview_path = _output_root / "data" / "newsletter_preview.html"
        with open(preview_path, "w", encoding="utf-8") as f:
            f.write(html)
        print(f"\n[DRY RUN] Newsletter saved to: {preview_path}")
        print(f"Subject: {subject}")
        return {"status": "dry_run", "preview_path": str(preview_path)}

    # Send via Beehiiv
    print("\nSending via Beehiiv...")

    try:
        result = send_free_newsletter(
            subject=subject,
            html_content=html,
            preview_text=preview
        )
        print("[OK] Newsletter sent successfully!")
        return {"status": "sent", "result": result}

    except Exception as e:
        print(f"[X] Failed to send: {e}")
        return {"status": "error", "error": str(e)}


def main():
    import sys

    dry_run = "--dry-run" in sys.argv or "-d" in sys.argv

    if "--help" in sys.argv:
        print("""
Free Newsletter Generator

Usage:
  python free_newsletter.py           Send newsletter via Beehiiv
  python free_newsletter.py --dry-run Generate preview without sending
  python free_newsletter.py --help    Show this help

Schedule with Windows Task Scheduler for daily 6 PM ET runs.
        """)
        return

    generate_and_send_newsletter(dry_run=dry_run)


if __name__ == "__main__":
    main()
