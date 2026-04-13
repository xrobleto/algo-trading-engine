"""
Beehiiv API Client
Handles newsletter operations: sending, subscriber management, analytics

Setup:
1. Create Beehiiv account: https://beehiiv.com
2. Get API key from Settings > Integrations > API
3. Add to .env: BEEHIIV_API_KEY and BEEHIIV_PUBLICATION_ID

API Docs: https://developers.beehiiv.com/docs/v2
"""

import json
import os
import requests
from datetime import datetime
from typing import Optional, Dict, List, Any
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Beehiiv API
BEEHIIV_API_BASE = "https://api.beehiiv.com/v2"
BEEHIIV_API_KEY = os.getenv("BEEHIIV_API_KEY")
BEEHIIV_PUBLICATION_ID = os.getenv("BEEHIIV_PUBLICATION_ID")


class BeehiivClient:
    """
    Beehiiv API client for newsletter management

    Features:
    - Send newsletters to segments
    - Manage subscribers (add, remove, update)
    - Get analytics (opens, clicks, growth)
    - Manage subscription tiers
    """

    def __init__(self, api_key: str = None, publication_id: str = None):
        self.api_key = api_key or BEEHIIV_API_KEY
        self.publication_id = publication_id or BEEHIIV_PUBLICATION_ID
        self.base_url = BEEHIIV_API_BASE

        if not self.api_key:
            raise ValueError("BEEHIIV_API_KEY not set")
        if not self.publication_id:
            raise ValueError("BEEHIIV_PUBLICATION_ID not set")

    def _request(self, method: str, endpoint: str, data: Dict = None, params: Dict = None) -> Dict:
        """Make authenticated request to Beehiiv API"""
        url = f"{self.base_url}{endpoint}"

        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }

        response = requests.request(
            method,
            url,
            headers=headers,
            json=data,
            params=params
        )

        if response.status_code not in [200, 201]:
            raise Exception(f"Beehiiv API error: {response.status_code} - {response.text}")

        return response.json()

    # =========================================================================
    # PUBLICATION
    # =========================================================================

    def get_publication(self) -> Dict:
        """Get publication details"""
        return self._request("GET", f"/publications/{self.publication_id}")

    def get_publication_stats(self) -> Dict:
        """Get publication statistics"""
        pub = self.get_publication()
        return pub.get("data", {}).get("stats", {})

    # =========================================================================
    # SUBSCRIBERS
    # =========================================================================

    def list_subscribers(self, limit: int = 100, page: int = 1,
                        status: str = None, tier: str = None) -> Dict:
        """
        List subscribers with optional filters

        Args:
            limit: Number of subscribers per page (max 100)
            page: Page number
            status: Filter by status (active, inactive, pending)
            tier: Filter by subscription tier

        Returns:
            Dict with subscribers list and pagination
        """
        params = {"limit": limit, "page": page}
        if status:
            params["status"] = status
        if tier:
            params["tier"] = tier

        return self._request(
            "GET",
            f"/publications/{self.publication_id}/subscriptions",
            params=params
        )

    def get_subscriber(self, subscriber_id: str) -> Dict:
        """Get a single subscriber by ID"""
        return self._request(
            "GET",
            f"/publications/{self.publication_id}/subscriptions/{subscriber_id}"
        )

    def get_subscriber_by_email(self, email: str) -> Optional[Dict]:
        """Find subscriber by email"""
        result = self._request(
            "GET",
            f"/publications/{self.publication_id}/subscriptions",
            params={"email": email}
        )
        subscribers = result.get("data", [])
        return subscribers[0] if subscribers else None

    def create_subscriber(self, email: str, reactivate_existing: bool = True,
                         send_welcome_email: bool = True,
                         utm_source: str = None, utm_medium: str = None,
                         utm_campaign: str = None,
                         referring_site: str = None,
                         custom_fields: Dict = None) -> Dict:
        """
        Add a new subscriber

        Args:
            email: Subscriber email address
            reactivate_existing: If True, reactivates unsubscribed users
            send_welcome_email: If True, sends the welcome email
            utm_source: Traffic source (e.g., "instagram")
            utm_medium: Traffic medium (e.g., "social")
            utm_campaign: Campaign name
            referring_site: URL where signup occurred
            custom_fields: Dict of custom field values

        Returns:
            Created subscriber object
        """
        data = {
            "email": email,
            "reactivate_existing": reactivate_existing,
            "send_welcome_email": send_welcome_email
        }

        if utm_source:
            data["utm_source"] = utm_source
        if utm_medium:
            data["utm_medium"] = utm_medium
        if utm_campaign:
            data["utm_campaign"] = utm_campaign
        if referring_site:
            data["referring_site"] = referring_site
        if custom_fields:
            data["custom_fields"] = custom_fields

        return self._request(
            "POST",
            f"/publications/{self.publication_id}/subscriptions",
            data=data
        )

    def update_subscriber(self, subscriber_id: str, custom_fields: Dict = None,
                         tier: str = None) -> Dict:
        """Update subscriber details"""
        data = {}
        if custom_fields:
            data["custom_fields"] = custom_fields
        if tier:
            data["tier"] = tier

        return self._request(
            "PATCH",
            f"/publications/{self.publication_id}/subscriptions/{subscriber_id}",
            data=data
        )

    def unsubscribe(self, subscriber_id: str) -> Dict:
        """Unsubscribe a subscriber"""
        return self._request(
            "DELETE",
            f"/publications/{self.publication_id}/subscriptions/{subscriber_id}"
        )

    def get_subscriber_count(self, status: str = "active") -> int:
        """Get total subscriber count"""
        result = self.list_subscribers(limit=1, status=status)
        return result.get("total_results", 0)

    # =========================================================================
    # POSTS / NEWSLETTERS
    # =========================================================================

    def list_posts(self, limit: int = 50, status: str = None) -> Dict:
        """
        List posts/newsletters

        Args:
            limit: Number of posts to return
            status: Filter by status (draft, confirmed, archived)
        """
        params = {"limit": limit}
        if status:
            params["status"] = status

        return self._request(
            "GET",
            f"/publications/{self.publication_id}/posts",
            params=params
        )

    def get_post(self, post_id: str) -> Dict:
        """Get a single post by ID"""
        return self._request(
            "GET",
            f"/publications/{self.publication_id}/posts/{post_id}"
        )

    def create_post(self, title: str, content_html: str,
                   subtitle: str = None,
                   authors: List[str] = None,
                   status: str = "draft",
                   content_json: Dict = None) -> Dict:
        """
        Create a new post/newsletter

        Args:
            title: Post title (email subject)
            content_html: HTML content of the post
            subtitle: Optional subtitle (preview text)
            authors: List of author IDs
            status: "draft" or "confirmed" (to send immediately)
            content_json: Alternative to content_html for structured content

        Returns:
            Created post object
        """
        data = {
            "title": title,
            "status": status
        }

        if content_html:
            data["content_html"] = content_html
        if content_json:
            data["content_json"] = content_json
        if subtitle:
            data["subtitle"] = subtitle
        if authors:
            data["authors"] = authors

        return self._request(
            "POST",
            f"/publications/{self.publication_id}/posts",
            data=data
        )

    def update_post(self, post_id: str, **kwargs) -> Dict:
        """Update an existing post"""
        return self._request(
            "PATCH",
            f"/publications/{self.publication_id}/posts/{post_id}",
            data=kwargs
        )

    def delete_post(self, post_id: str) -> Dict:
        """Delete a draft post"""
        return self._request(
            "DELETE",
            f"/publications/{self.publication_id}/posts/{post_id}"
        )

    # =========================================================================
    # SEGMENTS
    # =========================================================================

    def list_segments(self) -> Dict:
        """List all segments"""
        return self._request(
            "GET",
            f"/publications/{self.publication_id}/segments"
        )

    # =========================================================================
    # PREMIUM SUBSCRIPTIONS
    # =========================================================================

    def list_premium_tiers(self) -> Dict:
        """List premium subscription tiers"""
        return self._request(
            "GET",
            f"/publications/{self.publication_id}/premium_tiers"
        )

    def get_premium_subscribers(self, tier_id: str = None) -> Dict:
        """Get premium subscribers"""
        params = {}
        if tier_id:
            params["tier_id"] = tier_id

        return self._request(
            "GET",
            f"/publications/{self.publication_id}/premium_subscriptions",
            params=params
        )

    # =========================================================================
    # ANALYTICS
    # =========================================================================

    def get_post_analytics(self, post_id: str) -> Dict:
        """Get analytics for a specific post"""
        return self._request(
            "GET",
            f"/publications/{self.publication_id}/posts/{post_id}/stats"
        )

    def get_growth_stats(self, start_date: str = None, end_date: str = None) -> Dict:
        """
        Get subscriber growth statistics

        Args:
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
        """
        params = {}
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date

        return self._request(
            "GET",
            f"/publications/{self.publication_id}/stats/growth",
            params=params
        )


# =============================================================================
# NEWSLETTER SENDING UTILITIES
# =============================================================================

def send_free_newsletter(
    subject: str,
    html_content: str,
    preview_text: str = None,
    segment: str = "free"
) -> Dict:
    """
    Send a newsletter to free subscribers

    Args:
        subject: Email subject line
        html_content: Full HTML content
        preview_text: Email preview text
        segment: Segment to send to

    Returns:
        Post creation result
    """
    client = BeehiivClient()

    # Create the post
    result = client.create_post(
        title=subject,
        content_html=html_content,
        subtitle=preview_text,
        status="confirmed"  # Send immediately
    )

    return result


def add_subscriber_from_instagram(
    email: str,
    campaign: str = "instagram"
) -> Dict:
    """
    Add a subscriber who signed up via Instagram

    Args:
        email: Email address
        campaign: Campaign identifier

    Returns:
        Subscriber creation result
    """
    client = BeehiivClient()

    return client.create_subscriber(
        email=email,
        utm_source="instagram",
        utm_medium="social",
        utm_campaign=campaign,
        send_welcome_email=True
    )


def get_subscriber_stats() -> Dict:
    """Get quick subscriber statistics"""
    client = BeehiivClient()

    total = client.get_subscriber_count("active")

    # Try to get premium count
    try:
        premium = client.get_premium_subscribers()
        premium_count = len(premium.get("data", []))
    except:
        premium_count = 0

    return {
        "total_active": total,
        "premium": premium_count,
        "free": total - premium_count
    }


# =============================================================================
# MAIN / TEST
# =============================================================================

def main():
    """Test Beehiiv connection and show stats"""
    import sys

    if not BEEHIIV_API_KEY:
        print("[X] BEEHIIV_API_KEY not set in .env")
        print("\nSetup:")
        print("1. Create Beehiiv account: https://beehiiv.com")
        print("2. Go to Settings > Integrations > API")
        print("3. Create API key")
        print("4. Add to .env:")
        print("   BEEHIIV_API_KEY=your_key")
        print("   BEEHIIV_PUBLICATION_ID=your_pub_id")
        return

    try:
        client = BeehiivClient()

        # Get publication info
        pub = client.get_publication()
        pub_data = pub.get("data", {})

        print("=" * 60)
        print("BEEHIIV CONNECTION TEST")
        print("=" * 60)
        print(f"\nPublication: {pub_data.get('name', 'Unknown')}")
        print(f"URL: {pub_data.get('url', 'N/A')}")

        # Get stats
        stats = get_subscriber_stats()
        print(f"\nSubscriber Stats:")
        print(f"  Active: {stats['total_active']}")
        print(f"  Premium: {stats['premium']}")
        print(f"  Free: {stats['free']}")

        # List recent posts
        posts = client.list_posts(limit=5)
        print(f"\nRecent Posts:")
        for post in posts.get("data", [])[:5]:
            title = post.get("title", "Untitled")[:40]
            status = post.get("status", "unknown")
            print(f"  [{status}] {title}")

        print("\n[OK] Beehiiv connection successful!")

    except Exception as e:
        print(f"[X] Error: {e}")


if __name__ == "__main__":
    main()
