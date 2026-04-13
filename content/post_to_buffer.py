"""
Buffer API Integration for Instagram Posting
Schedules posts from content_queue.json to Buffer
Buffer then auto-posts to Instagram at scheduled times

Setup:
1. Create Buffer account: https://buffer.com
2. Connect your Instagram Business/Creator account
3. Get API access token from: https://buffer.com/developers/api
4. Add to .env: BUFFER_ACCESS_TOKEN and BUFFER_PROFILE_ID
"""

import json
import os
import requests
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Dict, List
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Paths
BASE_DIR = Path(__file__).parent
_content_output = Path(os.getenv("ALGO_OUTPUT_DIR", "")) / "content" if os.getenv("ALGO_OUTPUT_DIR") else BASE_DIR
QUEUE_FILE = _content_output / "content_queue.json"

# Buffer API
BUFFER_API_BASE = "https://api.bufferapp.com/1"
BUFFER_ACCESS_TOKEN = os.getenv("BUFFER_ACCESS_TOKEN")
BUFFER_PROFILE_ID = os.getenv("BUFFER_PROFILE_ID")  # Instagram profile ID


# =============================================================================
# QUEUE MANAGEMENT
# =============================================================================

def load_queue() -> Dict:
    """Load content queue"""
    if QUEUE_FILE.exists():
        with open(QUEUE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"posts": [], "last_updated": None}


def save_queue(queue: Dict):
    """Save content queue"""
    queue["last_updated"] = datetime.now().isoformat()
    with open(QUEUE_FILE, "w", encoding="utf-8") as f:
        json.dump(queue, f, indent=2, ensure_ascii=False)


def get_pending_posts() -> List[Dict]:
    """Get pending posts from queue"""
    queue = load_queue()
    return [p for p in queue["posts"] if p.get("status") == "pending"]


def update_post_status(post_id: str, status: str, buffer_id: str = None, error: str = None):
    """Update a post's status in the queue"""
    queue = load_queue()
    for post in queue["posts"]:
        if post["id"] == post_id:
            post["status"] = status
            post["updated_at"] = datetime.now().isoformat()
            if buffer_id:
                post["buffer_id"] = buffer_id
            if error:
                post["error"] = error
            break
    save_queue(queue)


# =============================================================================
# BUFFER API CLIENT
# =============================================================================

class BufferClient:
    """Buffer API client for scheduling Instagram posts"""

    def __init__(self, access_token: str, profile_id: str):
        self.access_token = access_token
        self.profile_id = profile_id
        self.base_url = BUFFER_API_BASE

    def _request(self, method: str, endpoint: str, **kwargs) -> Dict:
        """Make authenticated request to Buffer API"""
        url = f"{self.base_url}{endpoint}"

        # Add access token to params
        params = kwargs.pop("params", {})
        params["access_token"] = self.access_token

        response = requests.request(method, url, params=params, **kwargs)

        if response.status_code != 200:
            raise Exception(f"Buffer API error: {response.status_code} - {response.text}")

        return response.json()

    def get_profile(self) -> Dict:
        """Get profile information"""
        return self._request("GET", f"/profiles/{self.profile_id}.json")

    def get_pending_updates(self) -> List[Dict]:
        """Get pending scheduled updates"""
        result = self._request("GET", f"/profiles/{self.profile_id}/updates/pending.json")
        return result.get("updates", [])

    def create_update(self, text: str, media_url: str = None, scheduled_at: datetime = None) -> Dict:
        """
        Create a new update (scheduled post)

        Args:
            text: Post caption
            media_url: URL of image (must be publicly accessible)
            scheduled_at: When to post (None = add to queue)

        Returns:
            Buffer update object
        """
        data = {
            "profile_ids[]": self.profile_id,
            "text": text,
        }

        if media_url:
            data["media[photo]"] = media_url

        if scheduled_at:
            data["scheduled_at"] = int(scheduled_at.timestamp())

        return self._request("POST", "/updates/create.json", data=data)

    def upload_media(self, image_path: str) -> str:
        """
        Upload media to Buffer

        Note: Buffer requires images to be accessible via URL.
        For local files, we need to use a different approach:
        - Option 1: Upload to Imgur/Cloudinary first
        - Option 2: Use Buffer's media upload endpoint (if available)

        For simplicity, this implementation assumes you'll host images
        or use a service like Cloudinary.
        """
        # Buffer doesn't have a direct media upload endpoint
        # You need to host images somewhere accessible

        # Option: Upload to Imgur (free, no account needed for anonymous)
        try:
            with open(image_path, "rb") as f:
                response = requests.post(
                    "https://api.imgur.com/3/image",
                    headers={"Authorization": "Client-ID " + os.getenv("IMGUR_CLIENT_ID", "")},
                    files={"image": f}
                )

            if response.status_code == 200:
                return response.json()["data"]["link"]
            else:
                print(f"  [X] Imgur upload failed: {response.text}")
                return None

        except Exception as e:
            print(f"  [X] Media upload error: {e}")
            return None


# =============================================================================
# POSTING FUNCTIONS
# =============================================================================

def schedule_post_to_buffer(post: Dict, client: BufferClient) -> bool:
    """
    Schedule a single post to Buffer

    Args:
        post: Post dict from content_queue.json
        client: BufferClient instance

    Returns:
        True if successful, False otherwise
    """
    print(f"\n  Scheduling: [{post['type']}] {post.get('ticker', 'N/A')}")

    try:
        # Build caption with hashtags
        caption = post.get("caption", "")
        hashtags = post.get("hashtags", [])
        full_caption = f"{caption}\n\n{' '.join(hashtags)}"

        # Get image URL
        image_path = post.get("image_path")
        media_url = None

        if image_path and Path(image_path).exists():
            # Upload image to get URL
            media_url = client.upload_media(image_path)
            if not media_url:
                print(f"  [!] Could not upload image, posting without media")

        # Parse scheduled time
        scheduled_at = None
        if post.get("scheduled_time"):
            scheduled_at = datetime.fromisoformat(post["scheduled_time"])

            # If scheduled time is in the past, schedule for now + 5 minutes
            if scheduled_at < datetime.now():
                scheduled_at = datetime.now() + timedelta(minutes=5)

        # Create Buffer update
        result = client.create_update(
            text=full_caption,
            media_url=media_url,
            scheduled_at=scheduled_at
        )

        if result.get("success"):
            buffer_id = result.get("updates", [{}])[0].get("id")
            update_post_status(post["id"], "scheduled", buffer_id=buffer_id)
            print(f"  [OK] Scheduled to Buffer (ID: {buffer_id})")
            return True
        else:
            error = result.get("message", "Unknown error")
            update_post_status(post["id"], "failed", error=error)
            print(f"  [X] Buffer rejected: {error}")
            return False

    except Exception as e:
        update_post_status(post["id"], "failed", error=str(e))
        print(f"  [X] Error: {e}")
        return False


def schedule_all_pending():
    """Schedule all pending posts to Buffer"""
    print("=" * 60)
    print("BUFFER SCHEDULER")
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    # Check credentials
    if not BUFFER_ACCESS_TOKEN:
        print("[X] BUFFER_ACCESS_TOKEN not set in .env")
        print("  Get your token from: https://buffer.com/developers/api")
        return

    if not BUFFER_PROFILE_ID:
        print("[X] BUFFER_PROFILE_ID not set in .env")
        print("  Run 'python post_to_buffer.py --profiles' to list your profile IDs")
        return

    # Initialize client
    client = BufferClient(BUFFER_ACCESS_TOKEN, BUFFER_PROFILE_ID)

    # Verify connection
    try:
        profile = client.get_profile()
        print(f"\nConnected to: {profile.get('formatted_service', 'Unknown')} - @{profile.get('service_username', 'unknown')}")
    except Exception as e:
        print(f"[X] Could not connect to Buffer: {e}")
        return

    # Get pending posts
    pending = get_pending_posts()
    print(f"\nPending posts: {len(pending)}")

    if not pending:
        print("No posts to schedule.")
        return

    # Schedule each post
    success_count = 0
    for post in pending:
        if schedule_post_to_buffer(post, client):
            success_count += 1

    # Summary
    print(f"\n{'=' * 60}")
    print(f"RESULTS: {success_count}/{len(pending)} posts scheduled")
    print("=" * 60)


def list_profiles():
    """List all Buffer profiles (to get profile IDs)"""
    if not BUFFER_ACCESS_TOKEN:
        print("[X] BUFFER_ACCESS_TOKEN not set in .env")
        return

    response = requests.get(
        f"{BUFFER_API_BASE}/profiles.json",
        params={"access_token": BUFFER_ACCESS_TOKEN}
    )

    if response.status_code == 200:
        profiles = response.json()
        print("\nYour Buffer Profiles:")
        print("-" * 60)
        for p in profiles:
            print(f"  Service: {p.get('formatted_service')}")
            print(f"  Username: @{p.get('service_username')}")
            print(f"  Profile ID: {p.get('id')}")
            print(f"  ----")
    else:
        print(f"[X] Error: {response.text}")


def check_queue_status():
    """Print current queue status"""
    queue = load_queue()
    posts = queue.get("posts", [])

    print("\nContent Queue Status:")
    print("-" * 60)

    status_counts = {}
    for post in posts:
        status = post.get("status", "unknown")
        status_counts[status] = status_counts.get(status, 0) + 1

    for status, count in status_counts.items():
        print(f"  {status}: {count}")

    print(f"\nLast updated: {queue.get('last_updated', 'Never')}")

    # Show recent posts
    print("\nRecent posts:")
    for post in posts[-5:]:
        ticker = post.get("ticker", "N/A")
        print(f"  [{post['status']}] {post['type']} - {ticker}")


# =============================================================================
# MAIN
# =============================================================================

def main():
    import sys

    if len(sys.argv) > 1:
        command = sys.argv[1]

        if command == "--profiles":
            list_profiles()
        elif command == "--status":
            check_queue_status()
        elif command == "--help":
            print("""
Buffer Scheduler Commands:
  python post_to_buffer.py           Schedule all pending posts
  python post_to_buffer.py --profiles List your Buffer profile IDs
  python post_to_buffer.py --status   Show queue status
  python post_to_buffer.py --help     Show this help

Setup:
  1. Create Buffer account: https://buffer.com
  2. Connect Instagram Business account
  3. Get API token: https://buffer.com/developers/api
  4. Add to .env:
     BUFFER_ACCESS_TOKEN=your_token
     BUFFER_PROFILE_ID=your_profile_id
            """)
        else:
            print(f"Unknown command: {command}")
            print("Use --help for available commands")
    else:
        schedule_all_pending()


if __name__ == "__main__":
    main()
