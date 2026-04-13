"""
Post Ready - Prepares content for manual Instagram posting
Outputs: Ready-to-post folder with images + captions

Workflow:
1. Run signal_to_content.py → generates queue
2. Run post_ready.py → creates ready_to_post folder
3. Open ready_to_post folder
4. For each post: drag image to Meta Business Suite, paste caption
5. Mark as posted in Kanban

This replaces the Buffer integration with a manual-friendly output.
"""

import json
import os
import shutil
from datetime import datetime
from pathlib import Path
from typing import Dict, List

# Paths
BASE_DIR = Path(__file__).parent
_content_output = Path(os.getenv("ALGO_OUTPUT_DIR", "")) / "content" if os.getenv("ALGO_OUTPUT_DIR") else BASE_DIR
QUEUE_FILE = _content_output / "content_queue.json"
READY_DIR = _content_output / "ready_to_post"
ARCHIVE_DIR = _content_output / "archive"


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


def prepare_posts_for_manual():
    """
    Prepare all pending posts for manual Instagram posting

    Creates a folder structure:
    ready_to_post/
    ├── 01_trade_alert_AAPL/
    │   ├── image.png
    │   ├── caption.txt
    │   └── info.json
    ├── 02_market_context/
    │   ├── image.png
    │   ├── caption.txt
    │   └── info.json
    └── INSTRUCTIONS.txt
    """

    print("=" * 60)
    print("PREPARE POSTS FOR MANUAL POSTING")
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    # Create/clear ready directory
    if READY_DIR.exists():
        shutil.rmtree(READY_DIR)
    READY_DIR.mkdir(parents=True)

    # Create archive directory
    ARCHIVE_DIR.mkdir(exist_ok=True)

    # Load queue
    queue = load_queue()
    pending = [p for p in queue["posts"] if p.get("status") == "pending"]

    if not pending:
        print("\n[INFO] No pending posts in queue.")
        print("Run signal_to_content.py first to generate content.")
        return

    print(f"\nPending posts: {len(pending)}")

    # Process each post
    for i, post in enumerate(pending, 1):
        post_type = post.get("type", "post")
        ticker = post.get("ticker", "")
        post_id = post.get("id", "unknown")

        # Create folder name
        folder_name = f"{i:02d}_{post_type}"
        if ticker:
            folder_name += f"_{ticker}"

        post_dir = READY_DIR / folder_name
        post_dir.mkdir()

        print(f"\n[{i}/{len(pending)}] {folder_name}")

        # Copy image
        image_path = post.get("image_path")
        if image_path and Path(image_path).exists():
            dest_image = post_dir / "image.png"
            shutil.copy(image_path, dest_image)
            print(f"  [OK] Image copied")
        else:
            print(f"  [!] No image found")

        # Write caption
        caption = post.get("caption", "")
        hashtags = post.get("hashtags", [])
        full_caption = f"{caption}\n\n{' '.join(hashtags)}"

        caption_file = post_dir / "caption.txt"
        with open(caption_file, "w", encoding="utf-8") as f:
            f.write(full_caption)
        print(f"  [OK] Caption saved ({len(full_caption)} chars)")

        # Write info file
        info = {
            "id": post_id,
            "type": post_type,
            "ticker": ticker,
            "created_at": post.get("created_at"),
            "scheduled_time": post.get("scheduled_time"),
            "data": post.get("data", {})
        }
        info_file = post_dir / "info.json"
        with open(info_file, "w", encoding="utf-8") as f:
            json.dump(info, f, indent=2)

        # Update status to "ready"
        post["status"] = "ready"
        post["ready_at"] = datetime.now().isoformat()
        post["ready_folder"] = str(post_dir)

    # Save updated queue
    save_queue(queue)

    # Write instructions
    instructions = f"""
================================================================================
INSTAGRAM POSTING INSTRUCTIONS
Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Posts ready: {len(pending)}
================================================================================

QUICK WORKFLOW (5 minutes):

1. Open Meta Business Suite: https://business.facebook.com
   (or use the Meta Business Suite mobile app)

2. Click "Create Post" → Select Instagram

3. For each folder below:
   a. Drag the image.png into Meta Business Suite
   b. Open caption.txt, copy all text (Ctrl+A, Ctrl+C)
   c. Paste into the caption field
   d. Set schedule time (or post now)
   e. Click "Schedule" or "Post"

4. After posting, run: python post_ready.py --mark-posted

================================================================================
POSTS TO UPLOAD:
================================================================================
"""

    for i, post in enumerate(pending, 1):
        post_type = post.get("type", "post")
        ticker = post.get("ticker", "")
        scheduled = post.get("scheduled_time", "ASAP")
        if scheduled:
            try:
                scheduled = datetime.fromisoformat(scheduled).strftime("%I:%M %p")
            except:
                pass

        folder_name = f"{i:02d}_{post_type}"
        if ticker:
            folder_name += f"_{ticker}"

        instructions += f"""
{i}. {folder_name}/
   Type: {post_type}
   {'Ticker: ' + ticker if ticker else ''}
   Suggested time: {scheduled}
"""

    instructions += """
================================================================================
TIPS:
- Best posting times: 8:30 AM, 12:30 PM, 5:00 PM, 7:30 PM
- Space posts at least 2 hours apart
- Meta Business Suite lets you schedule up to 30 days ahead
================================================================================
"""

    instructions_file = READY_DIR / "INSTRUCTIONS.txt"
    with open(instructions_file, "w", encoding="utf-8") as f:
        f.write(instructions)

    print(f"\n{'=' * 60}")
    print(f"[OK] {len(pending)} posts ready in: {READY_DIR}")
    print(f"[OK] Open INSTRUCTIONS.txt for posting guide")
    print("=" * 60)

    # Open the folder
    os.startfile(READY_DIR)


def mark_all_posted():
    """Mark all ready posts as posted and archive them"""

    print("=" * 60)
    print("MARKING POSTS AS POSTED")
    print("=" * 60)

    queue = load_queue()
    ready_posts = [p for p in queue["posts"] if p.get("status") == "ready"]

    if not ready_posts:
        print("[INFO] No ready posts to mark as posted.")
        return

    # Archive the ready_to_post folder
    if READY_DIR.exists():
        archive_name = f"posted_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        archive_path = ARCHIVE_DIR / archive_name
        shutil.move(READY_DIR, archive_path)
        print(f"[OK] Archived to: {archive_path}")

    # Update status
    for post in ready_posts:
        post["status"] = "posted"
        post["posted_at"] = datetime.now().isoformat()

    save_queue(queue)
    print(f"[OK] Marked {len(ready_posts)} posts as posted")


def show_status():
    """Show current queue status"""

    queue = load_queue()
    posts = queue.get("posts", [])

    print("\n" + "=" * 60)
    print("CONTENT QUEUE STATUS")
    print("=" * 60)

    status_counts = {}
    for post in posts:
        status = post.get("status", "unknown")
        status_counts[status] = status_counts.get(status, 0) + 1

    for status, count in sorted(status_counts.items()):
        print(f"  {status}: {count}")

    print(f"\nTotal: {len(posts)}")
    print(f"Last updated: {queue.get('last_updated', 'Never')}")

    # Show pending
    pending = [p for p in posts if p.get("status") == "pending"]
    if pending:
        print(f"\nPending posts:")
        for p in pending[:5]:
            ticker = p.get("ticker", "N/A")
            print(f"  [{p['type']}] {ticker}")


def main():
    import sys

    if "--mark-posted" in sys.argv:
        mark_all_posted()
    elif "--status" in sys.argv:
        show_status()
    elif "--help" in sys.argv:
        print("""
Post Ready - Prepare content for manual Instagram posting

Usage:
  python post_ready.py              Prepare pending posts for manual upload
  python post_ready.py --mark-posted  Mark all ready posts as posted
  python post_ready.py --status       Show queue status
  python post_ready.py --help         Show this help

Workflow:
  1. python signal_to_content.py all   (generate content)
  2. python post_ready.py              (prepare for posting)
  3. Upload to Instagram via Meta Business Suite
  4. python post_ready.py --mark-posted (mark as done)
        """)
    else:
        prepare_posts_for_manual()


if __name__ == "__main__":
    main()
