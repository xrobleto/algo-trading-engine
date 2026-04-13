# Content Automation System

Automatically generates Instagram posts from your trading signals and sends daily newsletters.

## Overview

```
Trading Signals (buy_alerts, sell_alerts, swing_newsletter)
    │
    ▼
signal_to_content.py (Generate posts)
    │
    ▼
generate_image.py (Create images)
    │
    ▼
content_queue.json (Staging area)
    │
    ▼
post_to_buffer.py (Schedule to Instagram)
    │
    ▼
Buffer → Instagram (Auto-post at scheduled time)

Meanwhile:
    │
    ▼
free_newsletter.py → Beehiiv → Email subscribers
```

## Setup

### 1. Install dependencies

```bash
cd C:\Users\xrobl\Documents\Algo_Trading\content
pip install -r requirements.txt
```

### 2. Configure API keys

Copy `.env.example` to `.env` and fill in your credentials:

```bash
copy .env.example .env
```

Required keys:
- **ANTHROPIC_API_KEY** - For AI captions ([Get here](https://console.anthropic.com))
- **BUFFER_ACCESS_TOKEN** - For Instagram posting ([Get here](https://buffer.com/developers/api))
- **BUFFER_PROFILE_ID** - Run `python post_to_buffer.py --profiles`
- **BEEHIIV_API_KEY** - For newsletters ([Beehiiv Settings](https://app.beehiiv.com))
- **BEEHIIV_PUBLICATION_ID** - From Beehiiv settings

Optional:
- **IMGUR_CLIENT_ID** - For image hosting

### 3. Test image generation

```bash
python generate_image.py
```

Check `.tmp/images/` for sample images.

### 4. Test content generation

```bash
python signal_to_content.py all
```

Check `content_queue.json` for generated posts.

## Usage

### Generate content for a specific time slot

```bash
python signal_to_content.py morning    # Market context (8:30 AM)
python signal_to_content.py midday     # Educational tip (12:00 PM)
python signal_to_content.py afternoon  # Trade recap (4:30 PM)
python signal_to_content.py evening    # Setup teaser (7:00 PM)
python signal_to_content.py weekly     # Weekly results (Fridays)
python signal_to_content.py all        # Generate all types
```

### Prepare posts for Instagram

```bash
python post_ready.py              # Prepare pending posts for manual upload
python post_ready.py --mark-posted  # Mark all as posted after uploading
python post_ready.py --status       # Show queue status
```

The `ready_to_post/` folder will open automatically with:
- Each post in its own numbered folder
- `image.png` - drag to Meta Business Suite
- `caption.txt` - copy/paste as caption
- `INSTRUCTIONS.txt` - step-by-step guide

### Send free newsletter

```bash
python ../alerts/free_newsletter.py           # Send via Beehiiv
python ../alerts/free_newsletter.py --dry-run # Preview without sending
```

## Automation with Windows Task Scheduler

Create scheduled tasks for automated operation:

### Task 1: Morning Content (8:15 AM ET)
```
Program: python
Arguments: C:\Users\xrobl\Documents\Algo_Trading\content\signal_to_content.py morning
```

### Task 2: Midday Content (11:45 AM ET)
```
Program: python
Arguments: C:\Users\xrobl\Documents\Algo_Trading\content\signal_to_content.py midday
```

### Task 3: Afternoon Content (4:15 PM ET)
```
Program: python
Arguments: C:\Users\xrobl\Documents\Algo_Trading\content\signal_to_content.py afternoon
```

### Task 4: Evening Content (6:45 PM ET)
```
Program: python
Arguments: C:\Users\xrobl\Documents\Algo_Trading\content\signal_to_content.py evening
```

### Task 5: Free Newsletter (6:00 PM ET)
```
Program: python
Arguments: C:\Users\xrobl\Documents\Algo_Trading\alerts\free_newsletter.py
```

## File Structure

```
content/
├── .env.example         # API key template
├── .env                  # Your API keys (create this)
├── requirements.txt      # Python dependencies
├── README.md             # This file
│
├── run_daily.py          # Master automation orchestrator
├── signal_to_content.py  # Main content generation engine
├── generate_image.py     # Image template generator
├── post_ready.py         # Prepare posts for manual upload
│
├── content_queue.json    # Post staging queue (auto-created)
├── ready_to_post/        # Posts ready for upload (auto-created)
├── archive/              # Previously posted content
│
└── .tmp/
    └── images/           # Generated images
```

## Content Types

| Type | Schedule | Purpose |
|------|----------|---------|
| market_context | 8:30 AM | Daily market open overview |
| educational | 12:30 PM | Trading tip/insight |
| trade_alert | 5:00 PM | Teaser of today's alerts |
| setup_teaser | 7:30 PM | Premium content teaser |
| results | Fridays | Weekly performance recap |

## Troubleshooting

### "ANTHROPIC_API_KEY not set"
Add your Anthropic API key to `.env`

### "BUFFER_ACCESS_TOKEN not set"
1. Create Buffer account
2. Connect Instagram Business account
3. Get API token from developer settings
4. Add to `.env`

### "Could not upload image"
Either:
- Add IMGUR_CLIENT_ID for image hosting, or
- Buffer requires images to be publicly accessible URLs

### Images look wrong
Check that fonts are available. The system uses Windows system fonts as fallback.

### No content generated
Check that your alert state files exist in `data/state/`:
- `buy_alert_state.json`
- `alert_state.json`
