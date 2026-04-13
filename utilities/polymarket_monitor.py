"""
Polymarket Sentiment Monitor - Continuous Monitoring
=====================================================

Continuously monitors prediction markets and provides alerts/sentiment
for trading decisions. Can send email alerts on significant changes.

Usage:
    python polymarket_monitor.py              # Run continuously
    python polymarket_monitor.py --once       # Single check
    python polymarket_monitor.py --email      # Enable email alerts

Author: Claude Code
Version: 1.0.0
"""

import os
import sys
import time
import logging
import smtplib
import argparse
from datetime import datetime, timedelta
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import Optional, List, Dict
from zoneinfo import ZoneInfo
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from polymarket_client import PolymarketClient, MarketSentiment, PolymarketMarket

# ============================================================
# CONFIGURATION
# ============================================================

ET = ZoneInfo("America/New_York")

# Monitoring settings
CHECK_INTERVAL_SECONDS = 300  # Check every 5 minutes
ALERT_COOLDOWN_MINUTES = 60   # Don't repeat same alert for 60 minutes

# Alert thresholds (probability changes)
RECESSION_ALERT_THRESHOLD = 0.30     # Alert if recession prob > 30%
RECESSION_SPIKE_THRESHOLD = 0.10     # Alert if recession prob increases 10%+ in one check
FED_CHANGE_THRESHOLD = 0.15          # Alert if Fed odds change by 15%+
MARKET_CRASH_THRESHOLD = 0.25        # Alert if market crash prob > 25%

# Email settings (from environment)
SMTP_SERVER = os.getenv("SMTP_SERVER", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USERNAME = os.getenv("SMTP_USERNAME", "")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "")
ALERT_EMAIL_TO = os.getenv("ALERT_EMAIL_TO", "")
ALERT_EMAIL_FROM = os.getenv("ALERT_EMAIL_FROM", "")

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)


# ============================================================
# KEY MARKETS TO TRACK
# ============================================================

# High-priority keywords for market discovery
PRIORITY_KEYWORDS = [
    # Economic
    "recession", "gdp", "unemployment",
    # Fed
    "federal reserve", "fomc", "rate cut", "rate hike", "interest rate",
    # Markets
    "s&p 500", "spy", "nasdaq", "stock market", "correction", "crash",
    # Geopolitical
    "tariff", "china", "trade war",
    # Volatility
    "vix",
]


# ============================================================
# MONITOR CLASS
# ============================================================

class PolymarketMonitor:
    """
    Continuously monitors Polymarket for sentiment changes.
    """

    def __init__(self, enable_email: bool = False):
        self.client = PolymarketClient()
        self.enable_email = enable_email
        self.last_sentiment: Optional[MarketSentiment] = None
        self.alert_history: Dict[str, datetime] = {}  # alert_key -> last_alert_time
        self.tracked_markets: List[PolymarketMarket] = []

    def _should_alert(self, alert_key: str) -> bool:
        """Check if we should send this alert (cooldown check)."""
        if alert_key in self.alert_history:
            last_alert = self.alert_history[alert_key]
            if datetime.now() - last_alert < timedelta(minutes=ALERT_COOLDOWN_MINUTES):
                return False
        return True

    def _record_alert(self, alert_key: str):
        """Record that we sent an alert."""
        self.alert_history[alert_key] = datetime.now()

    def _send_email_alert(self, subject: str, body: str):
        """Send email alert."""
        if not self.enable_email:
            return

        if not all([SMTP_USERNAME, SMTP_PASSWORD, ALERT_EMAIL_TO]):
            logger.warning("[EMAIL] Email not configured - skipping alert")
            return

        try:
            msg = MIMEMultipart("alternative")
            msg["Subject"] = f"[Polymarket] {subject}"
            msg["From"] = ALERT_EMAIL_FROM or SMTP_USERNAME
            msg["To"] = ALERT_EMAIL_TO

            # Plain text
            msg.attach(MIMEText(body, "plain"))

            # HTML version
            html_body = f"""
            <html>
            <body style="font-family: Arial, sans-serif; padding: 20px;">
                <h2 style="color: #e74c3c;">⚠️ Polymarket Alert</h2>
                <h3>{subject}</h3>
                <pre style="background: #f5f5f5; padding: 15px; border-radius: 5px;">{body}</pre>
                <p style="color: #666; font-size: 12px;">
                    Generated at {datetime.now(ET).strftime('%Y-%m-%d %H:%M:%S ET')}
                </p>
            </body>
            </html>
            """
            msg.attach(MIMEText(html_body, "html"))

            with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
                server.starttls()
                server.login(SMTP_USERNAME, SMTP_PASSWORD)
                server.sendmail(ALERT_EMAIL_FROM or SMTP_USERNAME, ALERT_EMAIL_TO, msg.as_string())

            logger.info(f"[EMAIL] Alert sent: {subject}")

        except Exception as e:
            logger.error(f"[EMAIL] Failed to send alert: {e}")

    def discover_key_markets(self) -> List[PolymarketMarket]:
        """Discover and cache key markets to monitor."""
        logger.info("[DISCOVER] Searching for key markets...")

        all_markets = []
        seen_ids = set()

        for keyword in PRIORITY_KEYWORDS:
            markets = self.client.search_markets([keyword], limit=5)
            for m in markets:
                if m.id not in seen_ids and m.volume > 10000:  # Min $10k volume
                    all_markets.append(m)
                    seen_ids.add(m.id)

        # Sort by volume
        all_markets.sort(key=lambda x: x.volume, reverse=True)

        logger.info(f"[DISCOVER] Found {len(all_markets)} key markets")
        self.tracked_markets = all_markets[:30]  # Track top 30
        return self.tracked_markets

    def check_sentiment(self) -> MarketSentiment:
        """Check current sentiment and compare with previous."""
        sentiment = self.client.get_market_sentiment()

        # Log current state
        logger.info("="*60)
        logger.info("POLYMARKET SENTIMENT CHECK")
        logger.info("="*60)

        if sentiment.recession_prob:
            logger.info(f"  Recession Probability: {sentiment.recession_prob*100:.1f}%")
        if sentiment.fed_dovish_prob:
            logger.info(f"  Fed Dovish (Cut): {sentiment.fed_dovish_prob*100:.1f}%")
        if sentiment.fed_hawkish_prob:
            logger.info(f"  Fed Hawkish (Hike): {sentiment.fed_hawkish_prob*100:.1f}%")
        if sentiment.market_bullish_prob:
            logger.info(f"  Market Bullish: {sentiment.market_bullish_prob*100:.1f}%")

        logger.info(f"  >>> RISK LEVEL: {sentiment.overall_risk_level}")

        adj = sentiment.get_trading_adjustment()
        logger.info(f"  >>> Position Size Mult: {adj['size_mult']:.0%}")

        # Check for alerts
        self._check_alerts(sentiment)

        self.last_sentiment = sentiment
        return sentiment

    def _check_alerts(self, sentiment: MarketSentiment):
        """Check if any alert thresholds are breached."""
        alerts = []

        # Recession alert
        if sentiment.recession_prob and sentiment.recession_prob >= RECESSION_ALERT_THRESHOLD:
            if self._should_alert("recession_high"):
                alerts.append(f"⚠️ RECESSION PROBABILITY HIGH: {sentiment.recession_prob*100:.1f}%")
                self._record_alert("recession_high")

        # Recession spike (compared to last check)
        if sentiment.recession_prob and self.last_sentiment and self.last_sentiment.recession_prob:
            change = sentiment.recession_prob - self.last_sentiment.recession_prob
            if change >= RECESSION_SPIKE_THRESHOLD:
                if self._should_alert("recession_spike"):
                    alerts.append(f"📈 RECESSION PROB SPIKED: +{change*100:.1f}% (now {sentiment.recession_prob*100:.1f}%)")
                    self._record_alert("recession_spike")

        # Fed policy shift
        if sentiment.fed_hawkish_prob and self.last_sentiment and self.last_sentiment.fed_hawkish_prob:
            change = abs(sentiment.fed_hawkish_prob - self.last_sentiment.fed_hawkish_prob)
            if change >= FED_CHANGE_THRESHOLD:
                if self._should_alert("fed_shift"):
                    direction = "more hawkish" if sentiment.fed_hawkish_prob > self.last_sentiment.fed_hawkish_prob else "more dovish"
                    alerts.append(f"🏦 FED SENTIMENT SHIFT: {direction} ({change*100:.1f}% change)")
                    self._record_alert("fed_shift")

        # Send alerts
        for alert in alerts:
            logger.warning(alert)
            if self.enable_email:
                self._send_email_alert(
                    alert.split(":")[0].strip(),
                    self._format_sentiment_report(sentiment)
                )

    def _format_sentiment_report(self, sentiment: MarketSentiment) -> str:
        """Format sentiment for email/display."""
        lines = [
            "POLYMARKET SENTIMENT REPORT",
            "=" * 40,
            f"Time: {datetime.now(ET).strftime('%Y-%m-%d %H:%M:%S ET')}",
            "",
        ]

        if sentiment.recession_prob:
            lines.append(f"Recession Probability: {sentiment.recession_prob*100:.1f}%")
        if sentiment.fed_dovish_prob:
            lines.append(f"Fed Rate Cut Probability: {sentiment.fed_dovish_prob*100:.1f}%")
        if sentiment.fed_hawkish_prob:
            lines.append(f"Fed Rate Hike Probability: {sentiment.fed_hawkish_prob*100:.1f}%")
        if sentiment.market_bullish_prob:
            lines.append(f"Market Up Probability: {sentiment.market_bullish_prob*100:.1f}%")

        lines.extend([
            "",
            f"OVERALL RISK LEVEL: {sentiment.overall_risk_level}",
            "",
            "Trading Adjustment:",
            f"  Position Size: {sentiment.get_trading_adjustment()['size_mult']:.0%}",
            f"  Note: {sentiment.get_trading_adjustment()['note']}",
        ])

        if sentiment.raw_markets:
            lines.extend(["", "Key Markets:"])
            for m in sentiment.raw_markets[:5]:
                lines.append(f"  - {m.question[:50]}... ({m.yes_price*100:.1f}%)")

        return "\n".join(lines)

    def print_market_summary(self):
        """Print summary of tracked markets."""
        if not self.tracked_markets:
            self.discover_key_markets()

        print("\n" + "="*70)
        print("KEY PREDICTION MARKETS")
        print("="*70)

        for i, market in enumerate(self.tracked_markets[:15], 1):
            print(f"\n{i:2}. {market.question[:60]}")
            print(f"    Yes: {market.yes_price*100:5.1f}% | Vol: ${market.volume:>12,.0f}")

    def run_continuous(self):
        """Run continuous monitoring loop."""
        logger.info("="*60)
        logger.info("POLYMARKET MONITOR STARTED")
        logger.info(f"Check interval: {CHECK_INTERVAL_SECONDS} seconds")
        logger.info(f"Email alerts: {'ENABLED' if self.enable_email else 'DISABLED'}")
        logger.info("="*60)

        # Initial discovery
        self.discover_key_markets()

        check_count = 0
        while True:
            try:
                check_count += 1
                logger.info(f"\n[CHECK #{check_count}] {datetime.now(ET).strftime('%H:%M:%S ET')}")

                # Check sentiment
                sentiment = self.check_sentiment()

                # Rediscover markets periodically (every 12 checks = ~1 hour)
                if check_count % 12 == 0:
                    self.discover_key_markets()

                # Wait for next check
                logger.info(f"\nNext check in {CHECK_INTERVAL_SECONDS} seconds...")
                time.sleep(CHECK_INTERVAL_SECONDS)

            except KeyboardInterrupt:
                logger.info("\n[MONITOR] Stopped by user")
                break
            except Exception as e:
                logger.error(f"[MONITOR] Error: {e}")
                time.sleep(60)  # Wait a minute before retrying


# ============================================================
# CLI
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="Polymarket Sentiment Monitor")
    parser.add_argument("--once", action="store_true", help="Single check then exit")
    parser.add_argument("--email", action="store_true", help="Enable email alerts")
    parser.add_argument("--markets", action="store_true", help="Show tracked markets")
    parser.add_argument("--interval", type=int, default=300, help="Check interval in seconds")
    args = parser.parse_args()

    global CHECK_INTERVAL_SECONDS
    CHECK_INTERVAL_SECONDS = args.interval

    monitor = PolymarketMonitor(enable_email=args.email)

    if args.markets:
        monitor.print_market_summary()
    elif args.once:
        monitor.discover_key_markets()
        sentiment = monitor.check_sentiment()
        print("\n" + monitor._format_sentiment_report(sentiment))
    else:
        monitor.run_continuous()


if __name__ == "__main__":
    main()
