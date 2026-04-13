"""
Daily Content Automation Runner
Orchestrates the full content pipeline

Can be run manually or via Windows Task Scheduler

Usage:
  python run_daily.py              # Auto-detect time slot and run
  python run_daily.py --full       # Generate all content types
  python run_daily.py --post       # Prepare posts for manual upload
  python run_daily.py --newsletter # Just send free newsletter

Workflow:
  1. Content is auto-generated based on time slot
  2. Posts prepared in ready_to_post/ folder
  3. Open folder, upload manually via Meta Business Suite
  4. Run: python post_ready.py --mark-posted
"""

import sys
import os
from datetime import datetime
from pathlib import Path

# Add paths
sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).parent.parent))

def run_content_generation(time_slot: str = None):
    """Run content generation"""
    print("\n" + "=" * 60)
    print("STEP 1: CONTENT GENERATION")
    print("=" * 60)

    from signal_to_content import run_scheduled_generation
    run_scheduled_generation(time_slot)


def run_prepare_posts():
    """Prepare posts for manual Instagram uploading"""
    print("\n" + "=" * 60)
    print("STEP 2: PREPARE POSTS")
    print("=" * 60)

    from post_ready import prepare_posts_for_manual
    prepare_posts_for_manual()


def run_free_newsletter():
    """Send free daily newsletter"""
    print("\n" + "=" * 60)
    print("STEP 3: FREE NEWSLETTER")
    print("=" * 60)

    from alerts.free_newsletter import generate_and_send_newsletter
    generate_and_send_newsletter()


def main():
    print("=" * 60)
    print("DAILY CONTENT AUTOMATION")
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    args = sys.argv[1:]

    if "--help" in args or "-h" in args:
        print(__doc__)
        return

    if "--full" in args:
        # Generate all content types
        run_content_generation("all")
        run_prepare_posts()

    elif "--post" in args:
        # Just prepare posts for manual upload
        run_prepare_posts()

    elif "--newsletter" in args:
        # Just send newsletter
        run_free_newsletter()

    else:
        # Auto-detect based on time
        hour = datetime.now().hour

        # Morning run (7-10 AM)
        if 7 <= hour < 10:
            run_content_generation("morning")
            run_prepare_posts()

        # Midday run (11 AM - 2 PM)
        elif 11 <= hour < 14:
            run_content_generation("midday")
            run_prepare_posts()

        # Afternoon run (4-6 PM)
        elif 16 <= hour < 18:
            run_content_generation("afternoon")
            run_prepare_posts()

        # Evening run (6-9 PM)
        elif 18 <= hour < 21:
            # Check if Friday for weekly results
            if datetime.now().weekday() == 4:
                run_content_generation("weekly")
            else:
                run_content_generation("evening")

            run_prepare_posts()
            run_free_newsletter()

        else:
            print(f"[INFO] Current hour ({hour}) outside scheduled windows.")
            print("Use --full to generate all content types.")

    print("\n" + "=" * 60)
    print("COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    main()
