"""
Smallcap Scanner Dashboard v3.4
Real-time web interface for the smallcap momentum scanner.

Features:
- Compact single-screen layout
- Active breakout detection with visual emphasis
- Grade-based color coding (A+, A, B, C)
- VIX indicator for market fear gauge
- Browser notifications for new A+ setups
- News/Catalyst integration
- Market hours awareness (different modes for after-hours/pre-market)

Run with: streamlit run app.py
"""

import sys
from pathlib import Path

# Add parent directory to path so we can import scanner modules
sys.path.insert(0, str(Path(__file__).parent.parent))
sys.path.insert(0, str(Path(__file__).parent.parent / "scanners"))

import streamlit as st
import streamlit.components.v1 as components
import streamlit_authenticator as stauth
import yaml
import pandas as pd
from datetime import datetime, time as dt_time
from zoneinfo import ZoneInfo
import requests
import os
import math

# Set page config first (must be first Streamlit command)
st.set_page_config(
    page_title="Smallcap Scanner",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Load environment variables
from dotenv import load_dotenv
env_path = Path(__file__).parent.parent / "config" / "smallcap_scanner.env"
if env_path.exists():
    load_dotenv(env_path)

POLYGON_API_KEY = os.getenv("POLYGON_API_KEY", "")
ET = ZoneInfo("America/New_York")

# TradingView URL
TRADINGVIEW_URL = "https://www.tradingview.com/chart/?symbol="

# Scanner criteria
MIN_PRICE = 1.00
MAX_PRICE = 20.00
MIN_GAP_PCT = 10.0
MIN_VOLUME = 500_000
MIN_RVOL = 5.0
MAX_FLOAT = 10_000_000

# ============================================================
# AUTHENTICATION
# ============================================================

def load_auth_config():
    """Load authentication configuration."""
    config_path = Path(__file__).parent / "config.yaml"
    with open(config_path) as f:
        return yaml.safe_load(f)

def setup_auth():
    """Set up authentication and return authenticator."""
    config = load_auth_config()
    authenticator = stauth.Authenticate(
        credentials=config['credentials'],
        cookie_name=config['cookie']['name'],
        cookie_key=config['cookie']['key'],
        cookie_expiry_days=config['cookie']['expiry_days']
    )
    return authenticator

# ============================================================
# GRADING LOGIC
# ============================================================

def calculate_grade(gap_pct: float, rvol: float, float_shares: int, volume: int, spread_pct: float = 0) -> str:
    """
    Calculate setup grade based on multiple factors.

    A+ = Perfect setup (gap 20%+, RVOL 10x+, float <5M, tight spread)
    A  = Strong setup (gap 15%+, RVOL 7x+, float <10M)
    B  = Decent setup (gap 10%+, RVOL 5x+)
    C  = Weak setup (meets minimum criteria only)
    """
    score = 0

    # Gap scoring (0-30 points)
    if gap_pct >= 30:
        score += 30
    elif gap_pct >= 20:
        score += 25
    elif gap_pct >= 15:
        score += 20
    elif gap_pct >= 10:
        score += 15
    else:
        score += 5

    # RVOL scoring (0-30 points)
    if rvol >= 15:
        score += 30
    elif rvol >= 10:
        score += 25
    elif rvol >= 7:
        score += 20
    elif rvol >= 5:
        score += 15
    else:
        score += 5

    # Float scoring (0-25 points) - lower is better
    if float_shares and float_shares > 0:
        if float_shares <= 2_000_000:
            score += 25
        elif float_shares <= 5_000_000:
            score += 20
        elif float_shares <= 10_000_000:
            score += 15
        else:
            score += 5
    else:
        score += 10  # Unknown float, neutral score

    # Volume scoring (0-15 points)
    if volume >= 5_000_000:
        score += 15
    elif volume >= 2_000_000:
        score += 12
    elif volume >= 1_000_000:
        score += 10
    else:
        score += 5

    # Determine grade
    if score >= 85:
        return "A+"
    elif score >= 70:
        return "A"
    elif score >= 55:
        return "B"
    else:
        return "C"

def get_grade_color(grade: str) -> str:
    """Get color for grade."""
    colors = {
        "A+": "#9933ff",  # Purple
        "A": "#00cc00",   # Green
        "B": "#ffcc00",   # Yellow
        "C": "#ff3333",   # Red
    }
    return colors.get(grade, "#ffffff")

def get_grade_emoji(grade: str) -> str:
    """Get emoji for grade."""
    emojis = {
        "A+": "🟣",
        "A": "🟢",
        "B": "🟡",
        "C": "🔴",
    }
    return emojis.get(grade, "⚪")

# ============================================================
# KEY LEVEL CALCULATION
# ============================================================

def get_key_level(price: float) -> tuple:
    """
    Calculate nearest key level ($0.50 or $1.00 increments).
    Returns (key_level, distance_pct, direction)
    """
    if price <= 5:
        increment = 0.50
    elif price <= 20:
        increment = 1.00
    else:
        increment = 5.00

    lower = math.floor(price / increment) * increment
    upper = lower + increment

    dist_to_lower = price - lower
    dist_to_upper = upper - price

    if dist_to_upper < dist_to_lower:
        key_level = upper
        distance = dist_to_upper
        direction = "below"
    else:
        key_level = lower
        distance = dist_to_lower
        direction = "above"

    distance_pct = (distance / price) * 100 if price > 0 else 0

    return key_level, distance_pct, direction

def detect_breakout_status(price: float, high: float, low: float, key_level: float, prev_close: float) -> tuple:
    """
    Detect if a stock is in an active breakout.
    Returns (status, emoji) tuple.

    Status types:
    - "BREAKING" = Price just crossed above key level (within 2% above)
    - "HOD" = At or near high of day
    - "EXTENDED" = Well above key level (>5%)
    - "" = Normal, no special status
    """
    if price <= 0 or key_level <= 0:
        return "", ""

    pct_above_key = ((price - key_level) / key_level) * 100
    pct_from_hod = ((high - price) / price) * 100 if price > 0 else 0

    # Check if at high of day (within 0.5%)
    at_hod = pct_from_hod < 0.5

    # Breaking out: price is 0-2% above key level and at/near HOD
    if 0 <= pct_above_key <= 2 and at_hod:
        return "BREAKING", "🔥"

    # At HOD but not necessarily at key level
    if at_hod and pct_above_key > 0:
        return "HOD", "⬆️"

    # Extended: well above key level (may be chasing)
    if pct_above_key > 5:
        return "EXTENDED", "⚡"

    # Near key level (within 2% below) - watching for breakout
    if -2 <= pct_above_key < 0:
        return "NEAR KEY", "👀"

    return "", ""

# ============================================================
# MARKET DATA
# ============================================================

@st.cache_data(ttl=30)
def fetch_market_momentum():
    """Fetch SPY/QQQ momentum data."""
    if not POLYGON_API_KEY:
        return None

    results = {}
    now = datetime.now(ET)
    today = now.date()

    for symbol in ["SPY", "QQQ"]:
        url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/minute/{today}/{today}"
        params = {
            "adjusted": "true",
            "sort": "asc",
            "limit": 500,
            "apiKey": POLYGON_API_KEY
        }

        try:
            resp = requests.get(url, params=params, timeout=10)
            if resp.status_code != 200:
                continue

            bars = resp.json().get("results", [])
            if not bars or len(bars) < 2:
                continue

            current_price = bars[-1]["c"]

            # 5-minute change
            price_5m_ago = None
            target_ts = bars[-1]["t"] - (5 * 60 * 1000)
            for bar in reversed(bars):
                if bar["t"] <= target_ts:
                    price_5m_ago = bar["c"]
                    break
            if not price_5m_ago:
                price_5m_ago = bars[0]["c"]

            change_5m = ((current_price - price_5m_ago) / price_5m_ago * 100)

            results[symbol] = {
                "price": current_price,
                "change_5m": change_5m,
                "direction": "UP" if change_5m > 0.05 else ("DOWN" if change_5m < -0.05 else "FLAT")
            }
        except Exception:
            continue

    return results

@st.cache_data(ttl=30)
def fetch_vix_data():
    """Fetch VIX data using VIXY as proxy."""
    if not POLYGON_API_KEY:
        return None

    now = datetime.now(ET)
    today = now.date()

    # Use VIXY (VIX ETF) as proxy since direct VIX may not be available
    url = f"https://api.polygon.io/v2/aggs/ticker/VIXY/range/1/minute/{today}/{today}"
    params = {
        "adjusted": "true",
        "sort": "asc",
        "limit": 500,
        "apiKey": POLYGON_API_KEY
    }

    try:
        resp = requests.get(url, params=params, timeout=10)
        if resp.status_code != 200:
            return None

        bars = resp.json().get("results", [])
        if not bars or len(bars) < 2:
            return None

        current_price = bars[-1]["c"]
        open_price = bars[0]["o"]

        # Calculate day change
        day_change_pct = ((current_price - open_price) / open_price * 100) if open_price > 0 else 0

        # Determine fear level based ONLY on day's percentage change
        # Absolute VIXY price is meaningless due to futures roll/decay over time
        if day_change_pct > 10:
            fear_level = "EXTREME FEAR"
            color = "🔴"
        elif day_change_pct > 5:
            fear_level = "HIGH FEAR"
            color = "🟠"
        elif day_change_pct > 2:
            fear_level = "ELEVATED"
            color = "🟡"
        elif day_change_pct < -2:
            fear_level = "LOW FEAR"
            color = "🟢"
        else:
            fear_level = "NORMAL"
            color = "⚪"

        return {
            "price": current_price,
            "day_change_pct": day_change_pct,
            "fear_level": fear_level,
            "color": color
        }
    except Exception:
        return None

@st.cache_data(ttl=60)
def fetch_avg_volume(symbol: str) -> float:
    """Fetch 20-day average volume for RVOL calculation."""
    if not POLYGON_API_KEY:
        return 0

    try:
        url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/prev"
        params = {"apiKey": POLYGON_API_KEY}
        resp = requests.get(url, params=params, timeout=10)
        if resp.status_code == 200:
            results = resp.json().get("results", [])
            if results:
                return results[0].get("v", 0)
    except Exception:
        pass
    return 0

@st.cache_data(ttl=300)
def fetch_ticker_details(symbol: str) -> dict:
    """Fetch ticker details including shares outstanding."""
    if not POLYGON_API_KEY:
        return {}

    try:
        url = f"https://api.polygon.io/v3/reference/tickers/{symbol}"
        params = {"apiKey": POLYGON_API_KEY}
        resp = requests.get(url, params=params, timeout=10)
        if resp.status_code == 200:
            return resp.json().get("results", {})
    except Exception:
        pass
    return {}

@st.cache_data(ttl=300)
def fetch_ticker_news(symbol: str) -> str:
    """Fetch recent news/catalyst for a ticker."""
    if not POLYGON_API_KEY:
        return ""

    try:
        # Get news from the last 3 days
        url = f"https://api.polygon.io/v2/reference/news"
        params = {
            "ticker": symbol,
            "limit": 3,
            "order": "desc",
            "apiKey": POLYGON_API_KEY
        }
        resp = requests.get(url, params=params, timeout=10)
        if resp.status_code == 200:
            results = resp.json().get("results", [])
            if results:
                # Get the most recent headline
                headline = results[0].get("title", "")
                # Truncate if too long
                if len(headline) > 50:
                    headline = headline[:47] + "..."
                return headline
    except Exception:
        pass
    return ""

def categorize_catalyst(headline: str) -> str:
    """Categorize a news headline into a catalyst type."""
    if not headline:
        return "Unknown"

    headline_lower = headline.lower()

    # Check for common catalyst patterns
    if any(word in headline_lower for word in ["fda", "approval", "drug", "trial", "phase"]):
        return "FDA/Biotech"
    elif any(word in headline_lower for word in ["earnings", "revenue", "profit", "q1", "q2", "q3", "q4", "quarter"]):
        return "Earnings"
    elif any(word in headline_lower for word in ["contract", "deal", "agreement", "partnership", "acquisition"]):
        return "Deal/Contract"
    elif any(word in headline_lower for word in ["sec", "filing", "13d", "13g", "insider"]):
        return "SEC Filing"
    elif any(word in headline_lower for word in ["upgrade", "downgrade", "target", "analyst", "rating"]):
        return "Analyst"
    elif any(word in headline_lower for word in ["offering", "dilution", "shares", "stock"]):
        return "Offering"
    elif any(word in headline_lower for word in ["short", "squeeze", "reddit", "wsb", "meme"]):
        return "Social/Squeeze"
    else:
        return "News"

@st.cache_data(ttl=15)
def fetch_enhanced_gainers():
    """Fetch top gainers with enhanced data for grading."""
    if not POLYGON_API_KEY:
        return [], []

    url = "https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/gainers"
    params = {"apiKey": POLYGON_API_KEY}

    try:
        resp = requests.get(url, params=params, timeout=15)
        if resp.status_code != 200:
            return [], []

        tickers = resp.json().get("tickers", [])

        setups = []
        hot_list = []

        for t in tickers[:50]:
            ticker_symbol = t.get("ticker", "")
            day = t.get("day", {})
            prev = t.get("prevDay", {})

            price = day.get("c", 0)
            prev_close = prev.get("c", 0)
            volume = day.get("v", 0)
            high = day.get("h", price)
            low = day.get("l", price)

            if prev_close <= 0 or price <= 0:
                continue

            gap_pct = ((price - prev_close) / prev_close) * 100

            # Get additional data
            details = fetch_ticker_details(ticker_symbol)
            float_shares = details.get("share_class_shares_outstanding", 0) or details.get("weighted_shares_outstanding", 0)

            # Estimate RVOL (using prev day volume as proxy for avg)
            prev_volume = prev.get("v", 0)
            rvol = (volume / prev_volume) if prev_volume > 0 else 1.0

            # Calculate key level
            key_level, key_dist_pct, key_direction = get_key_level(price)

            # Calculate entry/stop
            atr_estimate = (high - low) if high > low else price * 0.03
            entry = price  # Current price as entry
            stop = price - (atr_estimate * 2)  # 2 ATR stop
            stop_pct = ((price - stop) / price) * 100 if price > 0 else 0

            # Calculate grade
            grade = calculate_grade(gap_pct, rvol, float_shares, volume)

            # Fetch news/catalyst (only for A+/A grades to reduce API calls)
            catalyst = ""
            catalyst_type = ""
            if grade in ["A+", "A"] or gap_pct >= 20:
                news_headline = fetch_ticker_news(ticker_symbol)
                catalyst = news_headline
                catalyst_type = categorize_catalyst(news_headline)

            # Detect breakout status
            breakout_status, breakout_emoji = detect_breakout_status(
                price, high, low, key_level, prev_close
            )

            setup = {
                "symbol": ticker_symbol,
                "price": price,
                "gap_pct": gap_pct,
                "volume": volume,
                "rvol": rvol,
                "float": float_shares,
                "key_level": key_level,
                "key_dist_pct": key_dist_pct,
                "entry": entry,
                "stop": stop,
                "stop_pct": stop_pct,
                "grade": grade,
                "high": high,
                "low": low,
                "catalyst": catalyst,
                "catalyst_type": catalyst_type,
                "breakout_status": breakout_status,
                "breakout_emoji": breakout_emoji,
            }

            # Filter for main setups list
            if MIN_PRICE <= price <= MAX_PRICE and gap_pct >= MIN_GAP_PCT and volume >= MIN_VOLUME:
                setups.append(setup)

                # Add A+ and A setups to hot list
                if grade in ["A+", "A"]:
                    hot_list.append(setup)

        # Sort by grade then gap%
        grade_order = {"A+": 0, "A": 1, "B": 2, "C": 3}
        setups.sort(key=lambda x: (grade_order.get(x["grade"], 4), -x["gap_pct"]))
        hot_list.sort(key=lambda x: (grade_order.get(x["grade"], 4), -x["gap_pct"]))

        return setups, hot_list

    except Exception as e:
        st.error(f"Error fetching data: {e}")
        return [], []

# ============================================================
# BROWSER NOTIFICATIONS
# ============================================================

def init_notification_state():
    """Initialize session state for notifications."""
    if "seen_a_plus_setups" not in st.session_state:
        st.session_state.seen_a_plus_setups = set()
    if "notifications_enabled" not in st.session_state:
        st.session_state.notifications_enabled = False

def request_notification_permission():
    """Inject JavaScript to request browser notification permission."""
    js_code = """
    <script>
    if ("Notification" in window) {
        if (Notification.permission === "default") {
            Notification.requestPermission().then(function(permission) {
                if (permission === "granted") {
                    console.log("Notifications enabled");
                }
            });
        }
    }
    </script>
    """
    components.html(js_code, height=0)

def send_browser_notification(title: str, body: str, tag: str = "scanner"):
    """Send a browser notification via JavaScript injection."""
    # Escape special characters for JavaScript
    title = title.replace("'", "\\'").replace('"', '\\"')
    body = body.replace("'", "\\'").replace('"', '\\"')

    js_code = f"""
    <script>
    if ("Notification" in window && Notification.permission === "granted") {{
        var notification = new Notification("{title}", {{
            body: "{body}",
            icon: "https://em-content.zobj.net/source/apple/354/chart-increasing_1f4c8.png",
            tag: "{tag}",
            requireInteraction: true
        }});

        // Auto-close after 10 seconds
        setTimeout(function() {{
            notification.close();
        }}, 10000);

        // Play a sound (optional - uses system sound)
        try {{
            var audio = new Audio('data:audio/wav;base64,UklGRnoGAABXQVZFZm10IBAAAAABAAEAQB8AAEAfAAABAAgAZGF0YQoGAACBhYqFbF1fdJivrJBhNjVgodDbq2EcBj+a2teleQYAaLnujnkADluS4/+uawAAPZDm/8J1AAAsnOL/1IIAABOX3//kk');
            audio.volume = 0.3;
            audio.play().catch(function(e) {{}});
        }} catch(e) {{}}
    }}
    </script>
    """
    components.html(js_code, height=0)

def check_and_notify_new_setups(hot_list: list):
    """Check for new A+ setups and send notifications."""
    if not st.session_state.get("notifications_enabled", False):
        return

    current_a_plus = set()
    new_setups = []

    for setup in hot_list:
        if setup["grade"] == "A+":
            symbol = setup["symbol"]
            current_a_plus.add(symbol)

            # Check if this is a new A+ setup
            if symbol not in st.session_state.seen_a_plus_setups:
                new_setups.append(setup)

    # Send notifications for new A+ setups
    for setup in new_setups:
        catalyst = setup.get("catalyst_type", "Unknown")
        body = f"Gap: +{setup['gap_pct']:.1f}% | RVOL: {setup['rvol']:.1f}x | {catalyst}"
        send_browser_notification(
            title=f"🟣 NEW A+ SETUP: {setup['symbol']}",
            body=body,
            tag=f"setup_{setup['symbol']}"
        )

    # Update seen setups
    st.session_state.seen_a_plus_setups = current_a_plus

# ============================================================
# UI COMPONENTS
# ============================================================

def display_market_indicator():
    """Display the market direction indicator with VIX."""
    momentum = fetch_market_momentum()
    vix_data = fetch_vix_data()

    if not momentum:
        st.warning("Unable to fetch market data")
        return

    # Create columns: SPY, QQQ, VIX, Overall
    cols = st.columns(4)

    # SPY and QQQ
    for i, (symbol, data) in enumerate(momentum.items()):
        with cols[i]:
            direction = data["direction"]
            change = data["change_5m"]

            if direction == "UP":
                delta_color = "normal"
                arrow = "▲"
            elif direction == "DOWN":
                delta_color = "inverse"
                arrow = "▼"
            else:
                delta_color = "off"
                arrow = "►"

            st.metric(
                label=symbol,
                value=f"${data['price']:.2f}",
                delta=f"{arrow} {change:+.2f}% (5m)",
                delta_color=delta_color
            )

    # VIX indicator (using VIXY)
    with cols[2]:
        if vix_data:
            change = vix_data["day_change_pct"]
            # VIX going up is bad for bulls, so inverse the color
            if change > 0:
                delta_color = "inverse"  # Red when VIX up
                arrow = "▲"
            elif change < 0:
                delta_color = "normal"   # Green when VIX down
                arrow = "▼"
            else:
                delta_color = "off"
                arrow = "►"

            st.metric(
                label=f"VIX (VIXY) {vix_data['color']}",
                value=f"${vix_data['price']:.2f}",
                delta=f"{arrow} {change:+.1f}% today",
                delta_color=delta_color,
                help="VIXY ETF as VIX proxy. Rising = increasing fear/volatility."
            )
        else:
            st.metric(label="VIX", value="N/A", delta="No data")

    # Overall market sentiment
    with cols[3]:
        up_count = sum(1 for d in momentum.values() if d["direction"] == "UP")
        down_count = sum(1 for d in momentum.values() if d["direction"] == "DOWN")

        # Factor in VIX for overall sentiment (only if showing elevated/high fear)
        vix_fear = False
        if vix_data and vix_data["fear_level"] in ["HIGH FEAR", "EXTREME FEAR"]:
            vix_fear = True

        if up_count == len(momentum) and not vix_fear:
            st.success("🟢 BULLISH")
            st.caption("SPY/QQQ up, low fear")
        elif down_count == len(momentum) or vix_fear:
            if vix_fear and down_count < len(momentum):
                st.warning("⚠️ CAUTION")
                st.caption("VIX spiking - be careful")
            else:
                st.error("🔴 BEARISH")
                st.caption("Risk-off environment")
        else:
            st.warning("🟡 MIXED")
            st.caption("Conflicting signals")

def format_float(value: int) -> str:
    """Format float/shares for display."""
    if not value or value <= 0:
        return "N/A"
    if value >= 1_000_000:
        return f"{value/1_000_000:.1f}M"
    elif value >= 1_000:
        return f"{value/1_000:.0f}K"
    return str(value)

def format_volume(value: int) -> str:
    """Format volume for display."""
    if value >= 1_000_000:
        return f"{value/1_000_000:.1f}M"
    elif value >= 1_000:
        return f"{value/1_000:.0f}K"
    return str(value)

def get_catalyst_emoji(catalyst_type: str) -> str:
    """Get emoji for catalyst type."""
    emojis = {
        "FDA/Biotech": "💊",
        "Earnings": "📊",
        "Deal/Contract": "🤝",
        "SEC Filing": "📄",
        "Analyst": "📈",
        "Offering": "💰",
        "Social/Squeeze": "🚀",
        "News": "📰",
        "Unknown": "❓",
    }
    return emojis.get(catalyst_type, "❓")

def display_setups_table(setups: list, max_rows: int = 10):
    """Display setups in a compact table with breakout status."""
    if not setups:
        st.info("No setups currently meet all criteria. Watching for opportunities...")
        return

    # Build dataframe for display
    rows = []
    for setup in setups[:max_rows]:
        grade = setup["grade"]
        grade_emoji = get_grade_emoji(grade)
        symbol = setup["symbol"]
        tv_link = f"{TRADINGVIEW_URL}{symbol}"

        # Format catalyst
        catalyst_type = setup.get("catalyst_type", "")
        catalyst_emoji = get_catalyst_emoji(catalyst_type) if catalyst_type else ""
        catalyst_display = f"{catalyst_emoji} {catalyst_type}" if catalyst_type else "-"

        # Format breakout status
        breakout_status = setup.get("breakout_status", "")
        breakout_emoji = setup.get("breakout_emoji", "")
        status_display = f"{breakout_emoji} {breakout_status}" if breakout_status else ""

        rows.append({
            "Status": status_display,
            "Grade": f"{grade_emoji} {grade}",
            "Symbol": symbol,
            "Chart": tv_link,
            "Catalyst": catalyst_display,
            "Price": f"${setup['price']:.2f}",
            "Gap%": f"+{setup['gap_pct']:.1f}%",
            "RVOL": f"{setup['rvol']:.1f}x",
            "Float": format_float(setup["float"]),
            "Key Lvl": f"${setup['key_level']:.2f}",
            "Stop": f"${setup['stop']:.2f}",
        })

    df = pd.DataFrame(rows)

    # Calculate dynamic height: ~35px per row + ~40px for header
    # This ensures the full table is visible without scrolling
    table_height = (len(rows) * 35) + 40

    st.dataframe(
        df,
        use_container_width=True,
        hide_index=True,
        height=table_height,
        key=f"setups_table_{len(rows)}",
        column_config={
            "Status": st.column_config.TextColumn("Status", width="small"),
            "Grade": st.column_config.TextColumn("Grade", width="small"),
            "Symbol": st.column_config.TextColumn("Symbol", width="small"),
            "Chart": st.column_config.LinkColumn("Chart", display_text="📈", width="small"),
            "Catalyst": st.column_config.TextColumn("Catalyst", width="medium"),
            "Price": st.column_config.TextColumn("Price", width="small"),
            "Gap%": st.column_config.TextColumn("Gap%", width="small"),
            "RVOL": st.column_config.TextColumn("RVOL", width="small"),
            "Float": st.column_config.TextColumn("Float", width="small"),
            "Key Lvl": st.column_config.TextColumn("Key Lvl", width="small"),
            "Stop": st.column_config.TextColumn("Stop", width="small"),
        }
    )

# ============================================================
# MAIN APPLICATION
# ============================================================

def main():
    """Main application entry point."""

    # Authentication
    authenticator = setup_auth()

    try:
        authenticator.login(location='main')
    except TypeError:
        authenticator.login('Login', 'main')

    authentication_status = st.session_state.get("authentication_status")
    name = st.session_state.get("name")
    username = st.session_state.get("username")

    if authentication_status == False:
        st.error('Username/password is incorrect')
        return

    if authentication_status == None:
        st.warning('Please enter your username and password')
        st.info("Contact admin for access credentials")
        return

    # Initialize notification state
    init_notification_state()

    # User is authenticated - SIDEBAR with all reference info
    with st.sidebar:
        st.write(f"Welcome, **{name}**")
        try:
            authenticator.logout(location='sidebar')
        except TypeError:
            authenticator.logout('Logout', 'sidebar')
        st.divider()

        # Settings
        st.subheader("Settings")
        auto_refresh = st.checkbox("Auto-refresh", value=True)
        refresh_interval = st.slider("Refresh (sec)", 10, 60, 15)
        notifications_enabled = st.checkbox(
            "A+ Alerts",
            value=st.session_state.get("notifications_enabled", False),
            help="Browser notifications for new A+ setups"
        )
        st.session_state.notifications_enabled = notifications_enabled
        if notifications_enabled:
            request_notification_permission()

        min_grade = st.selectbox("Min Grade", ["All", "A+", "A", "B"], index=0)

        st.divider()

        # Reference: Grades
        st.subheader("Grade Guide")
        st.markdown("""
        🟣 **A+** Gap 20%+, RVOL 10x+, Float <5M
        🟢 **A** Gap 15%+, RVOL 7x+, Float <10M
        🟡 **B** Meets criteria, weaker metrics
        🔴 **C** Minimum criteria only
        """)

        st.divider()

        # Reference: Status
        st.subheader("Status Guide")
        st.markdown("""
        🔥 **BREAKING** At key level breakout
        ⬆️ **HOD** At high of day
        ⚡ **EXTENDED** Well above key level
        👀 **NEAR KEY** Approaching breakout
        """)

        st.divider()

        # Reference: Catalysts
        st.subheader("Catalyst Types")
        st.caption("💊 FDA | 📊 Earnings | 🤝 Deal")
        st.caption("📄 SEC | 📈 Analyst | 💰 Offering")
        st.caption("🚀 Squeeze | 📰 News")

        st.divider()

        # Scanner criteria
        st.subheader("Scanner Criteria")
        st.caption(f"Price: ${MIN_PRICE}-${MAX_PRICE}")
        st.caption(f"Gap: ≥{MIN_GAP_PCT:.0f}%")
        st.caption(f"Volume: ≥{MIN_VOLUME/1000:.0f}K")

        st.divider()
        st.caption("Smallcap Scanner v3.4")

    # =========================================================
    # MAIN CONTENT - Compact single-screen layout
    # =========================================================

    # Determine market session
    now = datetime.now(ET)
    current_time = now.time()

    is_premarket = current_time < dt_time(9, 30) and current_time >= dt_time(4, 0)
    is_market_hours = dt_time(9, 30) <= current_time < dt_time(16, 0)
    is_after_hours = current_time >= dt_time(16, 0) or current_time < dt_time(4, 0)

    if current_time < dt_time(4, 0):
        session, color = "OVERNIGHT", "⚫"
    elif current_time < dt_time(9, 30):
        session, color = "PRE-MARKET", "🟡"
    elif current_time < dt_time(11, 0):
        session, color = "PRIME TIME", "🟢"
    elif current_time < dt_time(16, 0):
        session, color = "INTRADAY", "🔵"
    else:
        session, color = "AFTER HOURS", "🔴"

    # Header row: Title + Session
    col_title, col_session = st.columns([3, 1])
    with col_title:
        st.markdown("## 📈 Smallcap Momentum Scanner")
    with col_session:
        st.markdown(f"**{color} {session}**")
        st.caption(now.strftime('%I:%M:%S %p ET'))

    # How-to guide (collapsed by default)
    with st.expander("📖 How to Use This Scanner", expanded=False):
        st.markdown("""
        **Quick Start:** Focus on 🟣A+ and 🟢A setups when market is BULLISH.
        Watch for 🔥BREAKING status = active breakout above key level.

        | Column | Meaning |
        |--------|---------|
        | **Status** | 🔥BREAKING=breakout, ⬆️HOD=high of day, 👀NEAR KEY=watch |
        | **Grade** | A+=best, A=good, B=ok, C=weak |
        | **RVOL** | Volume vs normal (10x = 10 times average) |
        | **Key Lvl** | Nearest round number - breakout level |

        ⚠️ Small-caps are volatile. Risk 1-2% max per trade.
        """)

    # Market Direction Row - compact
    st.markdown("##### Market Direction")
    display_market_indicator()

    # Fetch data once (cached)
    setups, hot_list = fetch_enhanced_gainers()

    # Apply grade filter
    if min_grade != "All":
        grade_order = {"A+": 0, "A": 1, "B": 2, "C": 3}
        min_grade_val = grade_order.get(min_grade, 3)
        setups = [s for s in setups if grade_order.get(s["grade"], 4) <= min_grade_val]

    # Different display based on market hours
    if is_after_hours:
        # After hours: Show end-of-day summary
        st.warning("📊 **Market Closed** - Showing today's top performers. Live scanning resumes at 9:30 AM ET.")

        # Sort by grade then gap (no live status tracking after hours)
        grade_order = {"A+": 0, "A": 1, "B": 2, "C": 3}
        setups.sort(key=lambda x: (
            grade_order.get(x.get("grade", "C"), 3),
            -x.get("gap_pct", 0)
        ))

        # Summary stats
        a_plus_count = sum(1 for s in setups if s.get("grade") == "A+")
        a_count = sum(1 for s in setups if s.get("grade") == "A")

        # Header with counts inline
        header_parts = [f"Today's Top Movers ({len(setups)} found)"]
        if a_plus_count > 0:
            header_parts.append(f"🟣 {a_plus_count} A+")
        if a_count > 0:
            header_parts.append(f"🟢 {a_count} A")
        st.markdown(f"##### {' | '.join(header_parts)}")

        # Show table - all rows
        display_setups_table(setups, max_rows=len(setups))

    elif is_premarket:
        # Pre-market: Show yesterday's data with note
        st.info("🌅 **Pre-Market** - Showing previous day's data. Live scanning begins at 9:30 AM ET.")

        grade_order = {"A+": 0, "A": 1, "B": 2, "C": 3}
        setups.sort(key=lambda x: (
            grade_order.get(x.get("grade", "C"), 3),
            -x.get("gap_pct", 0)
        ))

        st.markdown(f"##### Pre-Market Watch List ({len(setups)} found)")
        display_setups_table(setups, max_rows=len(setups))

    else:
        # Market hours: Full live scanning

        # Check for new A+ setups and notify
        check_and_notify_new_setups(hot_list)

        # Sort by status priority first, then by grade
        status_priority = {"BREAKING": 0, "HOD": 1, "NEAR KEY": 2, "EXTENDED": 3, "": 4}
        grade_order = {"A+": 0, "A": 1, "B": 2, "C": 3}
        setups.sort(key=lambda x: (
            status_priority.get(x.get("breakout_status", ""), 4),
            grade_order.get(x.get("grade", "C"), 3),
            -x.get("gap_pct", 0)
        ))

        # Main table header
        st.markdown(f"##### Momentum Setups ({len(setups)} found)")

        # Main setups table - show all rows
        display_setups_table(setups, max_rows=len(setups))

    # Auto-refresh - only during market hours
    # After hours/pre-market data is static, no need to refresh
    if auto_refresh and is_market_hours:
        import time
        time.sleep(refresh_interval)
        st.rerun()

if __name__ == "__main__":
    main()
