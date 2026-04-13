"""
Instagram Image Generator
Creates static images for Instagram posts using Pillow
Templates: trade_alert, market_context, results, setup_teaser, educational
"""

import os
from pathlib import Path
from datetime import datetime
from PIL import Image, ImageDraw, ImageFont
import textwrap

# Paths
BASE_DIR = Path(__file__).parent
_content_output = Path(os.getenv("ALGO_OUTPUT_DIR", "")) / "content" if os.getenv("ALGO_OUTPUT_DIR") else BASE_DIR
OUTPUT_DIR = _content_output / ".tmp" / "images"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Instagram dimensions (1080x1350 for portrait, 1080x1080 for square)
IMG_WIDTH = 1080
IMG_HEIGHT = 1350

# Colors (dark theme matching your brand)
COLORS = {
    "background": "#0f1419",
    "card_bg": "#16181c",
    "card_border": "#2f3336",
    "text_primary": "#e7e9ea",
    "text_secondary": "#71767b",
    "accent_blue": "#1d9bf0",
    "accent_green": "#00ba7c",
    "accent_red": "#f91880",
    "accent_orange": "#ff7a00",
    "accent_purple": "#7856ff",
    "accent_gold": "#d4a853",
    "accent_gold_dark": "#b8942e",
    "high_conviction": "#00ba7c",
    "medium_conviction": "#ff7a00",
    "low_conviction": "#f91880",
}

# Logo path
LOGO_PATH = BASE_DIR / "Logos" / "Alpha_Trader_Daily_Logo.jpg"

def hex_to_rgb(hex_color):
    """Convert hex color to RGB tuple"""
    hex_color = hex_color.lstrip('#')
    return tuple(int(hex_color[i:i+2], 16) for i in (0, 2, 4))


def get_font(size, bold=False):
    """Get font - uses system fonts, falls back gracefully"""
    font_paths = [
        # Windows
        "C:/Windows/Fonts/segoeui.ttf",
        "C:/Windows/Fonts/segoeuib.ttf",
        "C:/Windows/Fonts/arial.ttf",
        "C:/Windows/Fonts/arialbd.ttf",
        # Fallback
        "arial.ttf",
    ]

    if bold:
        font_paths = [p.replace('.ttf', 'b.ttf') if 'segoe' not in p.lower() else p.replace('segoeui', 'segoeuib') for p in font_paths] + font_paths

    for path in font_paths:
        try:
            return ImageFont.truetype(path, size)
        except (IOError, OSError):
            continue

    # Ultimate fallback
    return ImageFont.load_default()


def get_title_font(size, bold=False):
    """Get elegant title font - serif style for brand name"""
    font_paths = [
        # Elegant serif fonts (Windows)
        "C:/Windows/Fonts/georgia.ttf",
        "C:/Windows/Fonts/georgiab.ttf",
        "C:/Windows/Fonts/times.ttf",
        "C:/Windows/Fonts/timesbd.ttf",
        "C:/Windows/Fonts/garamond.ttf",
        "C:/Windows/Fonts/cambria.ttc",
        "C:/Windows/Fonts/palatino.ttf",
        # Fallback to regular
        "C:/Windows/Fonts/segoeui.ttf",
    ]

    if bold:
        # Prefer bold versions
        font_paths = [
            "C:/Windows/Fonts/georgiab.ttf",
            "C:/Windows/Fonts/timesbd.ttf",
            "C:/Windows/Fonts/georgia.ttf",
        ] + font_paths

    for path in font_paths:
        try:
            return ImageFont.truetype(path, size)
        except (IOError, OSError):
            continue

    return get_font(size, bold)


def draw_rounded_rect(draw, coords, radius, fill=None, outline=None, width=1):
    """Draw a rounded rectangle"""
    x1, y1, x2, y2 = coords

    # Draw the main rectangle body
    draw.rectangle([x1 + radius, y1, x2 - radius, y2], fill=fill)
    draw.rectangle([x1, y1 + radius, x2, y2 - radius], fill=fill)

    # Draw the four corners
    draw.pieslice([x1, y1, x1 + 2*radius, y1 + 2*radius], 180, 270, fill=fill)
    draw.pieslice([x2 - 2*radius, y1, x2, y1 + 2*radius], 270, 360, fill=fill)
    draw.pieslice([x1, y2 - 2*radius, x1 + 2*radius, y2], 90, 180, fill=fill)
    draw.pieslice([x2 - 2*radius, y2 - 2*radius, x2, y2], 0, 90, fill=fill)

    if outline:
        # Draw outline (simplified - just rectangle for now)
        draw.rectangle(coords, outline=outline, width=width)


def draw_progress_bar(draw, x, y, width, height, progress, color):
    """Draw a progress bar"""
    # Background
    draw.rectangle([x, y, x + width, y + height], fill=hex_to_rgb(COLORS["card_border"]))
    # Progress
    progress_width = int(width * (progress / 100))
    if progress_width > 0:
        draw.rectangle([x, y, x + progress_width, y + height], fill=hex_to_rgb(color))


def wrap_text(text, font, max_width, draw):
    """Wrap text to fit within max_width"""
    words = text.split()
    lines = []
    current_line = []

    for word in words:
        test_line = ' '.join(current_line + [word])
        bbox = draw.textbbox((0, 0), test_line, font=font)
        if bbox[2] <= max_width:
            current_line.append(word)
        else:
            if current_line:
                lines.append(' '.join(current_line))
            current_line = [word]

    if current_line:
        lines.append(' '.join(current_line))

    return lines


# =============================================================================
# TEMPLATE: TRADE ALERT
# =============================================================================

def generate_trade_alert(
    ticker: str,
    conviction: str,  # HIGH, MEDIUM, LOW
    score: int,
    headline: str,
    signal_type: str,  # OVERSOLD, SUPPORT_TEST, etc.
    risk_reward: str = "1:2",
    handle: str = "@yourhandle",
    output_name: str = None
) -> Path:
    """Generate a trade alert image"""

    img = Image.new('RGB', (IMG_WIDTH, IMG_HEIGHT), hex_to_rgb(COLORS["background"]))
    draw = ImageDraw.Draw(img)

    # Fonts
    font_large = get_font(72, bold=True)
    font_medium = get_font(48, bold=True)
    font_body = get_font(36)
    font_small = get_font(28)
    font_ticker = get_font(120, bold=True)

    # Conviction color
    conviction_colors = {
        "HIGH": COLORS["high_conviction"],
        "MEDIUM": COLORS["medium_conviction"],
        "LOW": COLORS["low_conviction"]
    }
    conv_color = conviction_colors.get(conviction.upper(), COLORS["accent_blue"])

    y_pos = 80

    # Conviction badge at top
    badge_text = f"{conviction.upper()} CONVICTION"
    draw.rectangle([60, y_pos, IMG_WIDTH - 60, y_pos + 80], fill=hex_to_rgb(conv_color))
    bbox = draw.textbbox((0, 0), badge_text, font=font_medium)
    text_x = (IMG_WIDTH - bbox[2]) // 2
    draw.text((text_x, y_pos + 15), badge_text, fill=hex_to_rgb(COLORS["background"]), font=font_medium)

    y_pos += 140

    # Ticker symbol (large, centered)
    ticker_text = f"${ticker.upper()}"
    bbox = draw.textbbox((0, 0), ticker_text, font=font_ticker)
    text_x = (IMG_WIDTH - bbox[2]) // 2
    draw.text((text_x, y_pos), ticker_text, fill=hex_to_rgb(COLORS["text_primary"]), font=font_ticker)

    y_pos += 160

    # Signal type badge
    signal_badge = signal_type.upper().replace("_", " ")
    bbox = draw.textbbox((0, 0), signal_badge, font=font_small)
    badge_width = bbox[2] + 40
    badge_x = (IMG_WIDTH - badge_width) // 2
    draw.rectangle([badge_x, y_pos, badge_x + badge_width, y_pos + 50], fill=hex_to_rgb(COLORS["accent_blue"]))
    draw.text((badge_x + 20, y_pos + 8), signal_badge, fill=hex_to_rgb(COLORS["text_primary"]), font=font_small)

    y_pos += 100

    # Headline (wrapped)
    headline_lines = wrap_text(f'"{headline}"', font_body, IMG_WIDTH - 120, draw)
    for line in headline_lines[:3]:  # Max 3 lines
        bbox = draw.textbbox((0, 0), line, font=font_body)
        text_x = (IMG_WIDTH - bbox[2]) // 2
        draw.text((text_x, y_pos), line, fill=hex_to_rgb(COLORS["text_secondary"]), font=font_body)
        y_pos += 50

    y_pos += 40

    # Conviction score bar
    draw.text((60, y_pos), "CONVICTION SCORE", fill=hex_to_rgb(COLORS["text_secondary"]), font=font_small)
    y_pos += 40
    draw_progress_bar(draw, 60, y_pos, IMG_WIDTH - 120, 30, score, conv_color)
    draw.text((IMG_WIDTH - 120, y_pos - 35), f"{score}/100", fill=hex_to_rgb(COLORS["text_primary"]), font=font_small)

    y_pos += 80

    # Risk/Reward
    draw.text((60, y_pos), f"RISK/REWARD: {risk_reward}", fill=hex_to_rgb(COLORS["accent_green"]), font=font_medium)

    y_pos += 100

    # Divider
    draw.rectangle([60, y_pos, IMG_WIDTH - 60, y_pos + 2], fill=hex_to_rgb(COLORS["card_border"]))

    y_pos += 50

    # CTA
    cta_lines = [
        "Full analysis + entry levels",
        "in the link in bio"
    ]
    for line in cta_lines:
        bbox = draw.textbbox((0, 0), line, font=font_body)
        text_x = (IMG_WIDTH - bbox[2]) // 2
        draw.text((text_x, y_pos), line, fill=hex_to_rgb(COLORS["text_primary"]), font=font_body)
        y_pos += 50

    # Handle at bottom
    draw.text((60, IMG_HEIGHT - 80), handle, fill=hex_to_rgb(COLORS["accent_blue"]), font=font_small)

    # Date
    date_text = datetime.now().strftime("%b %d, %Y")
    bbox = draw.textbbox((0, 0), date_text, font=font_small)
    draw.text((IMG_WIDTH - 60 - bbox[2], IMG_HEIGHT - 80), date_text, fill=hex_to_rgb(COLORS["text_secondary"]), font=font_small)

    # Save
    if output_name is None:
        output_name = f"trade_alert_{ticker}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
    output_path = OUTPUT_DIR / output_name
    img.save(output_path, "PNG", quality=95)

    return output_path


# =============================================================================
# TEMPLATE: MARKET CONTEXT (Clean Design with Background Logo)
# =============================================================================

def generate_market_context(
    spy_trend: str,  # bullish, bearish, neutral
    spy_change: str,  # "+1.2%"
    vix_level: float,
    sector_leaders: list,
    sector_laggards: list,
    futures: dict = None,  # {"ES": "+0.3%", "NQ": "+0.5%", "RTY": "-0.1%"}
    key_levels: dict = None,  # {"support": "585", "resistance": "595"}
    market_breadth: dict = None,  # {"advancing": 65, "declining": 35}
    watchlist: list = None,  # ["NVDA", "AAPL", "TSLA"] - tickers on radar
    catalyst: str = None,  # "FOMC Minutes 2PM" or "NVDA Earnings After Close"
    insight: str = None,  # "Risk-on rotation continues as tech leads..."
    setups_count: int = 3,
    handle: str = "@alphatraderdaily",
    output_name: str = None
) -> Path:
    """Generate a clean market context image - square format with card layout"""

    # Use square format (1080x1080) for cleaner look
    img_size = 1080
    img = Image.new('RGB', (img_size, img_size), hex_to_rgb(COLORS["background"]))
    draw = ImageDraw.Draw(img)

    # Fonts
    font_title = get_title_font(38, bold=True)
    font_hero = get_font(64, bold=True)
    font_large = get_font(42, bold=True)
    font_medium = get_font(32, bold=True)
    font_body = get_font(28)
    font_small = get_font(24)
    font_tiny = get_font(20)

    margin = 50
    card_padding = 20

    # === HEADER WITH LOGO ===
    y_pos = 40

    # Small logo on left
    logo_size = 70
    if LOGO_PATH.exists():
        try:
            logo = Image.open(LOGO_PATH).convert("RGBA")
            logo = logo.resize((logo_size, logo_size), Image.Resampling.LANCZOS)
            img.paste(logo, (margin, y_pos), logo)
        except Exception:
            pass

    # Brand name next to logo
    brand_text = "ALPHA TRADER DAILY"
    draw.text((margin + logo_size + 15, y_pos + 8), brand_text, fill=hex_to_rgb(COLORS["accent_gold"]), font=font_title)

    # Date below brand
    date_text = datetime.now().strftime("%B %d, %Y")
    draw.text((margin + logo_size + 15, y_pos + 45), date_text, fill=hex_to_rgb(COLORS["text_secondary"]), font=font_small)

    y_pos += 100

    # === MAIN HERO CARD (SPY) ===
    hero_card_height = 180
    draw.rectangle(
        [margin, y_pos, img_size - margin, y_pos + hero_card_height],
        fill=hex_to_rgb(COLORS["card_bg"]),
        outline=hex_to_rgb(COLORS["card_border"]),
        width=2
    )

    # SPY label and change inside card
    change_color = COLORS["accent_green"] if spy_change.startswith("+") else COLORS["accent_red"]

    # Left side: SPY info
    draw.text((margin + card_padding, y_pos + 20), "S&P 500 (SPY)", fill=hex_to_rgb(COLORS["text_secondary"]), font=font_small)
    draw.text((margin + card_padding, y_pos + 50), spy_change, fill=hex_to_rgb(change_color), font=font_hero)

    # Trend badge
    trend_color = COLORS["accent_green"] if spy_trend == "bullish" else COLORS["accent_red"] if spy_trend == "bearish" else COLORS["text_secondary"]
    trend_text = spy_trend.upper()
    bbox = draw.textbbox((0, 0), trend_text, font=font_medium)
    badge_width = bbox[2] - bbox[0] + 30
    badge_x = margin + card_padding
    badge_y = y_pos + 125
    draw.rectangle([badge_x, badge_y, badge_x + badge_width, badge_y + 38], fill=hex_to_rgb(trend_color))
    draw.text((badge_x + 15, badge_y + 6), trend_text, fill=hex_to_rgb(COLORS["background"]), font=font_medium)

    # Right side: VIX and Key Levels
    right_x = img_size - margin - card_padding
    vix_color = COLORS["accent_green"] if vix_level < 18 else COLORS["accent_orange"] if vix_level < 25 else COLORS["accent_red"]
    vix_label = "LOW" if vix_level < 18 else "ELEVATED" if vix_level < 25 else "HIGH"

    draw.text((right_x - 150, y_pos + 20), "VIX", fill=hex_to_rgb(COLORS["text_secondary"]), font=font_tiny)
    draw.text((right_x - 150, y_pos + 42), f"{vix_level:.1f}", fill=hex_to_rgb(vix_color), font=font_large)
    draw.text((right_x - 150, y_pos + 85), vix_label, fill=hex_to_rgb(vix_color), font=font_small)

    if key_levels:
        draw.text((right_x - 150, y_pos + 115), "KEY LEVELS", fill=hex_to_rgb(COLORS["text_secondary"]), font=font_tiny)
        levels_text = f"S: {key_levels.get('support', '')}  R: {key_levels.get('resistance', '')}"
        draw.text((right_x - 150, y_pos + 137), levels_text, fill=hex_to_rgb(COLORS["text_primary"]), font=font_small)

    y_pos += hero_card_height + 15

    # === SECTOR ROTATION CARD ===
    sector_card_height = 100
    draw.rectangle(
        [margin, y_pos, img_size - margin, y_pos + sector_card_height],
        fill=hex_to_rgb(COLORS["card_bg"]),
        outline=hex_to_rgb(COLORS["card_border"]),
        width=1
    )

    draw.text((margin + card_padding, y_pos + 12), "SECTOR ROTATION", fill=hex_to_rgb(COLORS["text_secondary"]), font=font_tiny)

    # Leaders row
    draw.text((margin + card_padding, y_pos + 38), "▲", fill=hex_to_rgb(COLORS["accent_green"]), font=font_body)
    leaders_text = "  ".join(sector_leaders[:4])
    draw.text((margin + card_padding + 25, y_pos + 38), leaders_text, fill=hex_to_rgb(COLORS["accent_green"]), font=font_body)

    # Laggards row
    draw.text((margin + card_padding, y_pos + 68), "▼", fill=hex_to_rgb(COLORS["accent_red"]), font=font_body)
    laggards_text = "  ".join(sector_laggards[:4])
    draw.text((margin + card_padding + 25, y_pos + 68), laggards_text, fill=hex_to_rgb(COLORS["accent_red"]), font=font_body)

    y_pos += sector_card_height + 15

    # === WATCHLIST CARD ===
    if watchlist and len(watchlist) > 0:
        watch_card_height = 75
        draw.rectangle(
            [margin, y_pos, img_size - margin, y_pos + watch_card_height],
            fill=hex_to_rgb(COLORS["card_bg"]),
            outline=hex_to_rgb(COLORS["accent_gold_dark"]),
            width=1
        )

        draw.text((margin + card_padding, y_pos + 12), "ON MY RADAR", fill=hex_to_rgb(COLORS["accent_gold"]), font=font_tiny)

        # Ticker badges
        ticker_x = margin + card_padding
        for ticker in watchlist[:5]:
            bbox = draw.textbbox((0, 0), ticker, font=font_medium)
            tw = bbox[2] - bbox[0]
            badge_w = tw + 20
            badge_y = y_pos + 38
            draw.rectangle(
                [ticker_x, badge_y, ticker_x + badge_w, badge_y + 30],
                fill=hex_to_rgb(COLORS["background"]),
                outline=hex_to_rgb(COLORS["accent_blue"]),
                width=1
            )
            draw.text((ticker_x + 10, badge_y + 3), ticker, fill=hex_to_rgb(COLORS["accent_blue"]), font=font_medium)
            ticker_x += badge_w + 10

        y_pos += watch_card_height + 15

    # === CATALYST CARD (if provided) ===
    if catalyst:
        cat_card_height = 65
        draw.rectangle(
            [margin, y_pos, img_size - margin, y_pos + cat_card_height],
            fill=hex_to_rgb(COLORS["card_bg"]),
            outline=hex_to_rgb(COLORS["accent_orange"]),
            width=1
        )

        draw.text((margin + card_padding, y_pos + 12), "⚡ TODAY'S CATALYST", fill=hex_to_rgb(COLORS["accent_orange"]), font=font_tiny)
        draw.text((margin + card_padding, y_pos + 35), catalyst, fill=hex_to_rgb(COLORS["text_primary"]), font=font_body)

        y_pos += cat_card_height + 15

    # === INSIGHT QUOTE ===
    if insight:
        y_pos += 5
        insight_text = f'"{insight}"'
        insight_lines = wrap_text(insight_text, font_body, img_size - margin * 2 - 20, draw)
        for line in insight_lines[:2]:
            bbox = draw.textbbox((0, 0), line, font=font_body)
            text_width = bbox[2] - bbox[0]
            text_x = (img_size - text_width) // 2
            draw.text((text_x, y_pos), line, fill=hex_to_rgb(COLORS["text_secondary"]), font=font_body)
            y_pos += 32
        y_pos += 10

    # === CTA SECTION ===
    cta_y = img_size - 140

    # Gold accent line
    draw.rectangle([margin, cta_y, img_size - margin, cta_y + 3], fill=hex_to_rgb(COLORS["accent_gold"]))
    cta_y += 25

    # Setups teaser
    setup_text = f"{setups_count} setups on my radar today"
    bbox = draw.textbbox((0, 0), setup_text, font=font_body)
    text_x = (img_size - (bbox[2] - bbox[0])) // 2
    draw.text((text_x, cta_y), setup_text, fill=hex_to_rgb(COLORS["text_primary"]), font=font_body)
    cta_y += 35

    # CTA
    cta_text = "Free newsletter → Link in bio"
    bbox = draw.textbbox((0, 0), cta_text, font=font_medium)
    text_x = (img_size - (bbox[2] - bbox[0])) // 2
    draw.text((text_x, cta_y), cta_text, fill=hex_to_rgb(COLORS["accent_gold"]), font=font_medium)

    # === FOOTER ===
    footer_y = img_size - 45
    draw.text((margin, footer_y), handle, fill=hex_to_rgb(COLORS["accent_gold"]), font=font_small)

    bbox = draw.textbbox((0, 0), "alphatraderdaily.com", font=font_small)
    draw.text((img_size - margin - bbox[2], footer_y), "alphatraderdaily.com", fill=hex_to_rgb(COLORS["text_secondary"]), font=font_small)

    # Save
    if output_name is None:
        output_name = f"market_context_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
    output_path = OUTPUT_DIR / output_name
    img.save(output_path, "PNG", quality=95)

    return output_path


# =============================================================================
# TEMPLATE: RESULTS
# =============================================================================

def generate_results(
    period: str,  # "WEEKLY", "MONTHLY"
    pnl_pct: str,  # "+4.2%"
    winners: list,
    losers: list,
    win_rate: int = None,
    handle: str = "@yourhandle",
    output_name: str = None
) -> Path:
    """Generate a results/performance image"""

    img = Image.new('RGB', (IMG_WIDTH, IMG_HEIGHT), hex_to_rgb(COLORS["background"]))
    draw = ImageDraw.Draw(img)

    # Fonts
    font_huge = get_font(120, bold=True)
    font_large = get_font(64, bold=True)
    font_medium = get_font(48, bold=True)
    font_body = get_font(36)
    font_small = get_font(28)

    y_pos = 80

    # Header
    header_text = f"{period.upper()} RESULTS"
    bbox = draw.textbbox((0, 0), header_text, font=font_large)
    text_x = (IMG_WIDTH - bbox[2]) // 2
    draw.text((text_x, y_pos), header_text, fill=hex_to_rgb(COLORS["accent_purple"]), font=font_large)

    y_pos += 150

    # P&L (big number)
    pnl_color = COLORS["accent_green"] if pnl_pct.startswith("+") else COLORS["accent_red"]
    bbox = draw.textbbox((0, 0), pnl_pct, font=font_huge)
    text_x = (IMG_WIDTH - bbox[2]) // 2
    draw.text((text_x, y_pos), pnl_pct, fill=hex_to_rgb(pnl_color), font=font_huge)

    y_pos += 180

    # Win rate if provided
    if win_rate:
        wr_text = f"Win Rate: {win_rate}%"
        bbox = draw.textbbox((0, 0), wr_text, font=font_medium)
        text_x = (IMG_WIDTH - bbox[2]) // 2
        draw.text((text_x, y_pos), wr_text, fill=hex_to_rgb(COLORS["text_secondary"]), font=font_medium)
        y_pos += 80

    y_pos += 40

    # Winners
    draw.text((60, y_pos), "WINNERS", fill=hex_to_rgb(COLORS["accent_green"]), font=font_medium)
    y_pos += 60
    winners_text = ", ".join(winners[:4]) if winners else "None this period"
    draw.text((60, y_pos), winners_text, fill=hex_to_rgb(COLORS["text_primary"]), font=font_body)

    y_pos += 100

    # Losers
    draw.text((60, y_pos), "LOSERS", fill=hex_to_rgb(COLORS["accent_red"]), font=font_medium)
    y_pos += 60
    losers_text = ", ".join(losers[:4]) if losers else "None this period"
    draw.text((60, y_pos), losers_text, fill=hex_to_rgb(COLORS["text_primary"]), font=font_body)

    y_pos += 120

    # Divider
    draw.rectangle([60, y_pos, IMG_WIDTH - 60, y_pos + 2], fill=hex_to_rgb(COLORS["card_border"]))

    y_pos += 50

    # CTA
    cta_lines = [
        "Full breakdown in my",
        "free newsletter"
    ]
    for line in cta_lines:
        bbox = draw.textbbox((0, 0), line, font=font_body)
        text_x = (IMG_WIDTH - bbox[2]) // 2
        draw.text((text_x, y_pos), line, fill=hex_to_rgb(COLORS["text_primary"]), font=font_body)
        y_pos += 50

    # Handle at bottom
    draw.text((60, IMG_HEIGHT - 80), handle, fill=hex_to_rgb(COLORS["accent_blue"]), font=font_small)

    # Save
    if output_name is None:
        output_name = f"results_{period.lower()}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
    output_path = OUTPUT_DIR / output_name
    img.save(output_path, "PNG", quality=95)

    return output_path


# =============================================================================
# TEMPLATE: SETUP TEASER
# =============================================================================

def generate_setup_teaser(
    ticker: str,
    pattern: str,  # "bull_flag", "oversold_bounce", etc.
    risk_reward: str,
    days_ago: int = 3,
    handle: str = "@yourhandle",
    output_name: str = None
) -> Path:
    """Generate a setup teaser image (premium content teaser)"""

    img = Image.new('RGB', (IMG_WIDTH, IMG_HEIGHT), hex_to_rgb(COLORS["background"]))
    draw = ImageDraw.Draw(img)

    # Fonts
    font_large = get_font(64, bold=True)
    font_medium = get_font(48, bold=True)
    font_body = get_font(36)
    font_small = get_font(28)
    font_ticker = get_font(100, bold=True)

    y_pos = 100

    # Premium badge
    badge_text = "PREMIUM ALERT"
    draw.rectangle([60, y_pos, IMG_WIDTH - 60, y_pos + 70], fill=hex_to_rgb(COLORS["accent_purple"]))
    bbox = draw.textbbox((0, 0), badge_text, font=font_medium)
    text_x = (IMG_WIDTH - bbox[2]) // 2
    draw.text((text_x, y_pos + 12), badge_text, fill=hex_to_rgb(COLORS["text_primary"]), font=font_medium)

    y_pos += 130

    # "X days ago..."
    timing_text = f"{days_ago} days ago, subscribers got this:"
    bbox = draw.textbbox((0, 0), timing_text, font=font_body)
    text_x = (IMG_WIDTH - bbox[2]) // 2
    draw.text((text_x, y_pos), timing_text, fill=hex_to_rgb(COLORS["text_secondary"]), font=font_body)

    y_pos += 100

    # Ticker
    ticker_text = f"${ticker.upper()}"
    bbox = draw.textbbox((0, 0), ticker_text, font=font_ticker)
    text_x = (IMG_WIDTH - bbox[2]) // 2
    draw.text((text_x, y_pos), ticker_text, fill=hex_to_rgb(COLORS["text_primary"]), font=font_ticker)

    y_pos += 140

    # Pattern badge
    pattern_text = pattern.upper().replace("_", " ")
    bbox = draw.textbbox((0, 0), pattern_text, font=font_small)
    badge_width = bbox[2] + 40
    badge_x = (IMG_WIDTH - badge_width) // 2
    draw.rectangle([badge_x, y_pos, badge_x + badge_width, y_pos + 50], fill=hex_to_rgb(COLORS["accent_blue"]))
    draw.text((badge_x + 20, y_pos + 8), pattern_text, fill=hex_to_rgb(COLORS["text_primary"]), font=font_small)

    y_pos += 100

    # Risk/Reward
    rr_text = f"Risk/Reward: {risk_reward}"
    bbox = draw.textbbox((0, 0), rr_text, font=font_medium)
    text_x = (IMG_WIDTH - bbox[2]) // 2
    draw.text((text_x, y_pos), rr_text, fill=hex_to_rgb(COLORS["accent_green"]), font=font_medium)

    y_pos += 150

    # Result teaser (blurred/hidden)
    draw.rectangle([60, y_pos, IMG_WIDTH - 60, y_pos + 100], fill=hex_to_rgb(COLORS["card_bg"]))
    result_text = "Current status: █████████"
    bbox = draw.textbbox((0, 0), result_text, font=font_body)
    text_x = (IMG_WIDTH - bbox[2]) // 2
    draw.text((text_x, y_pos + 30), result_text, fill=hex_to_rgb(COLORS["text_secondary"]), font=font_body)

    y_pos += 150

    # CTA
    cta_text = "Get alerts like this"
    bbox = draw.textbbox((0, 0), cta_text, font=font_body)
    text_x = (IMG_WIDTH - bbox[2]) // 2
    draw.text((text_x, y_pos), cta_text, fill=hex_to_rgb(COLORS["text_primary"]), font=font_body)

    y_pos += 50

    cta_text2 = "Link in bio"
    bbox = draw.textbbox((0, 0), cta_text2, font=font_medium)
    text_x = (IMG_WIDTH - bbox[2]) // 2
    draw.text((text_x, y_pos), cta_text2, fill=hex_to_rgb(COLORS["accent_blue"]), font=font_medium)

    # Handle at bottom
    draw.text((60, IMG_HEIGHT - 80), handle, fill=hex_to_rgb(COLORS["accent_blue"]), font=font_small)

    # Save
    if output_name is None:
        output_name = f"setup_teaser_{ticker}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
    output_path = OUTPUT_DIR / output_name
    img.save(output_path, "PNG", quality=95)

    return output_path


# =============================================================================
# TEMPLATE: EDUCATIONAL TIP
# =============================================================================

def generate_educational(
    topic: str,
    tip_text: str,
    tip_number: int = None,
    handle: str = "@yourhandle",
    output_name: str = None
) -> Path:
    """Generate an educational tip image"""

    img = Image.new('RGB', (IMG_WIDTH, IMG_HEIGHT), hex_to_rgb(COLORS["background"]))
    draw = ImageDraw.Draw(img)

    # Fonts
    font_large = get_font(64, bold=True)
    font_medium = get_font(48, bold=True)
    font_body = get_font(40)
    font_small = get_font(28)

    y_pos = 100

    # Tip number badge (if provided)
    if tip_number:
        badge_text = f"TIP #{tip_number}"
        draw.rectangle([60, y_pos, 250, y_pos + 60], fill=hex_to_rgb(COLORS["accent_orange"]))
        draw.text((80, y_pos + 10), badge_text, fill=hex_to_rgb(COLORS["background"]), font=font_small)
        y_pos += 100

    # Topic header
    topic_lines = wrap_text(topic.upper(), font_large, IMG_WIDTH - 120, draw)
    for line in topic_lines[:2]:
        draw.text((60, y_pos), line, fill=hex_to_rgb(COLORS["accent_blue"]), font=font_large)
        y_pos += 80

    y_pos += 40

    # Divider
    draw.rectangle([60, y_pos, 200, y_pos + 4], fill=hex_to_rgb(COLORS["accent_blue"]))

    y_pos += 60

    # Tip content (wrapped)
    tip_lines = wrap_text(tip_text, font_body, IMG_WIDTH - 120, draw)
    for line in tip_lines[:8]:  # Max 8 lines
        draw.text((60, y_pos), line, fill=hex_to_rgb(COLORS["text_primary"]), font=font_body)
        y_pos += 55

    y_pos += 60

    # CTA
    cta_text = "More tips in my free newsletter"
    bbox = draw.textbbox((0, 0), cta_text, font=font_body)
    text_x = (IMG_WIDTH - bbox[2]) // 2
    draw.text((text_x, y_pos), cta_text, fill=hex_to_rgb(COLORS["text_secondary"]), font=font_body)

    y_pos += 50

    cta_text2 = "(link in bio)"
    bbox = draw.textbbox((0, 0), cta_text2, font=font_body)
    text_x = (IMG_WIDTH - bbox[2]) // 2
    draw.text((text_x, y_pos), cta_text2, fill=hex_to_rgb(COLORS["accent_blue"]), font=font_body)

    # Handle at bottom
    draw.text((60, IMG_HEIGHT - 80), handle, fill=hex_to_rgb(COLORS["accent_blue"]), font=font_small)

    # Save
    if output_name is None:
        safe_topic = topic[:20].replace(" ", "_").lower()
        output_name = f"educational_{safe_topic}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
    output_path = OUTPUT_DIR / output_name
    img.save(output_path, "PNG", quality=95)

    return output_path


# =============================================================================
# TEST / DEMO
# =============================================================================

def generate_all_templates():
    """Generate sample images for all templates"""
    print("Generating sample images...")

    # Trade Alert
    path1 = generate_trade_alert(
        ticker="AAPL",
        conviction="HIGH",
        score=85,
        headline="Oversold bounce at major support level with volume confirmation",
        signal_type="OVERSOLD",
        risk_reward="1:2.5"
    )
    print(f"  [OK] Trade alert: {path1}")

    # Market Context (Enhanced with watchlist + catalyst + insight)
    path2 = generate_market_context(
        spy_trend="bullish",
        spy_change="+1.2%",
        vix_level=14.2,
        sector_leaders=["XLK", "XLY", "XLC", "XLI"],
        sector_laggards=["XLE", "XLF", "XLU", "XLRE"],
        futures={"ES": "+0.4%", "NQ": "+0.6%", "RTY": "-0.2%"},
        key_levels={"support": "585", "resistance": "595"},
        watchlist=["NVDA", "AAPL", "META", "TSLA"],
        catalyst="FOMC Minutes @ 2PM ET",
        insight="Risk-on rotation continues. Tech leading, energy lagging. Watch for breakout above 595 resistance.",
        setups_count=4,
        handle="@alphatraderdaily"
    )
    print(f"  [OK] Market context: {path2}")

    # Results
    path3 = generate_results(
        period="WEEKLY",
        pnl_pct="+4.2%",
        winners=["NVDA", "META", "GOOGL"],
        losers=["TSLA"],
        win_rate=75
    )
    print(f"  [OK] Results: {path3}")

    # Setup Teaser
    path4 = generate_setup_teaser(
        ticker="NVDA",
        pattern="bull_flag",
        risk_reward="1:3",
        days_ago=3
    )
    print(f"  [OK] Setup teaser: {path4}")

    # Educational
    path5 = generate_educational(
        topic="Why I never chase extended stocks",
        tip_text="When a stock is more than 8% above its 20 EMA, the risk/reward flips against you. Wait for a pullback to support, or find another setup. Chasing extended moves is how most traders blow up their accounts.",
        tip_number=47
    )
    print(f"  [OK] Educational: {path5}")

    print(f"\nAll images saved to: {OUTPUT_DIR}")


if __name__ == "__main__":
    generate_all_templates()
