"""
Smallcap Scanner Dashboard v4.0 - Professional Trader Edition
=============================================================

Advanced features integrated from CLI scanner:
- ADX trend strength filter (>25 for A+ grade)
- Multi-timeframe momentum alignment (1m/5m/15m)
- Spread quality gates (<50bps for A+)
- Volume acceleration tracking (>1.3x for A+)
- Float rotation analysis (squeeze potential)
- Enhanced grading with execution quality factors

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
import numpy as np
from datetime import datetime, time as dt_time, timedelta
from zoneinfo import ZoneInfo
import requests
import os
import math

# Set page config first (must be first Streamlit command)
st.set_page_config(
    page_title="Smallcap Scanner Pro",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Hide the "Running..." status indicator for seamless experience
st.markdown("""
<style>
    /* Hide the running status in the top right */
    .stStatusWidget {
        display: none !important;
    }
    /* Also hide the spinner text */
    .stSpinner > div > div {
        display: none !important;
    }
</style>
""", unsafe_allow_html=True)

# Load environment variables
from dotenv import load_dotenv
env_path = Path(__file__).parent.parent / "config" / "smallcap_scanner.env"
if env_path.exists():
    load_dotenv(env_path)

POLYGON_API_KEY = os.getenv("POLYGON_API_KEY", "")
ET = ZoneInfo("America/New_York")

# Version
VERSION = "4.3"

# TradingView URL
TRADINGVIEW_URL = "https://www.tradingview.com/chart/?symbol="

# ============================================================
# CONFIGURATION - Advanced Scanner Criteria
# ============================================================

# Universe filters
MIN_PRICE = 1.00
MAX_PRICE = 20.00
MIN_GAP_PCT = 10.0
MIN_VOLUME = 500_000
MIN_RVOL = 5.0
MAX_FLOAT = 10_000_000

# ADX Trend Strength (required for A+ grade)
ADX_PERIOD = 14
MIN_ADX_A_PLUS = 25.0  # ADX > 25 = strong trend

# Multi-timeframe Momentum (required for A+ grade)
MTF_MIN_CHG_1M = 0.5   # 1m change > +0.5%
MTF_MIN_CHG_5M = 1.5   # 5m change > +1.5%
MTF_MIN_CHG_15M = 2.5  # 15m change > +2.5%
MTF_MIN_CONTRIBUTION = 0.25  # 1m must be 25% of 5m

# Spread Quality Gates (basis points)
MAX_SPREAD_BPS_A_PLUS = 50   # <=0.50%
MAX_SPREAD_BPS_A = 80        # <=0.80%
MAX_SPREAD_BPS_B = 120       # <=1.20%

# Volume Acceleration
MIN_VOL_ACCEL_A_PLUS = 1.3   # 1.3x prior 5-bar avg

# Float Rotation (squeeze potential)
FLOAT_ROT_HIGH = 0.5   # 50% = high squeeze potential
FLOAT_ROT_MAX = 1.0    # 100% = maximum boost

# Grade thresholds (base criteria)
GRADE_A_PLUS = {"min_gap": 12.0, "min_rvol": 6.0}
GRADE_A = {"min_gap": 15.0, "min_rvol": 8.0}
GRADE_B = {"min_gap": 10.0, "min_rvol": 5.0}

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
# ADX CALCULATION
# ============================================================

def calculate_adx(bars: list, period: int = ADX_PERIOD) -> float:
    """
    Calculate Average Directional Index (ADX) for trend strength.

    ADX measures trend strength (not direction):
    - ADX > 25: Strong trend
    - ADX 20-25: Emerging trend
    - ADX < 20: Weak/choppy
    """
    if not bars or len(bars) < period * 2:
        return 0.0

    df = pd.DataFrame(bars)
    if 'h' not in df.columns:
        return 0.0

    high = df['h']
    low = df['l']
    close = df['c']

    # True Range
    tr1 = high - low
    tr2 = abs(high - close.shift(1))
    tr3 = abs(low - close.shift(1))
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

    # Directional Movement
    up_move = high - high.shift(1)
    down_move = low.shift(1) - low

    # +DM and -DM
    plus_dm = up_move.where((up_move > down_move) & (up_move > 0), 0.0)
    minus_dm = down_move.where((down_move > up_move) & (down_move > 0), 0.0)

    # Smoothed using Wilder's method
    atr = tr.ewm(alpha=1/period, min_periods=period, adjust=False).mean()
    plus_dm_smooth = plus_dm.ewm(alpha=1/period, min_periods=period, adjust=False).mean()
    minus_dm_smooth = minus_dm.ewm(alpha=1/period, min_periods=period, adjust=False).mean()

    # +DI and -DI
    plus_di = 100 * (plus_dm_smooth / atr.replace(0, 1))
    minus_di = 100 * (minus_dm_smooth / atr.replace(0, 1))

    # DX and ADX
    di_sum = plus_di + minus_di
    dx = 100 * abs(plus_di - minus_di) / di_sum.replace(0, 1)
    adx = dx.ewm(alpha=1/period, min_periods=period, adjust=False).mean()

    result = adx.iloc[-1] if not pd.isna(adx.iloc[-1]) else 0.0
    return round(result, 1)

# ============================================================
# MULTI-TIMEFRAME MOMENTUM
# ============================================================

def calculate_mtf_momentum(bars: list) -> dict:
    """
    Calculate multi-timeframe momentum (1m, 5m, 15m changes).
    Returns dict with changes and alignment status.
    """
    if not bars or len(bars) < 15:
        return {"chg_1m": 0, "chg_5m": 0, "chg_15m": 0, "aligned": False, "stalling": True}

    current = bars[-1]['c']

    # 1-minute change
    price_1m_ago = bars[-2]['c'] if len(bars) >= 2 else current
    chg_1m = ((current - price_1m_ago) / price_1m_ago * 100) if price_1m_ago > 0 else 0

    # 5-minute change
    price_5m_ago = bars[-6]['c'] if len(bars) >= 6 else bars[0]['c']
    chg_5m = ((current - price_5m_ago) / price_5m_ago * 100) if price_5m_ago > 0 else 0

    # 15-minute change
    price_15m_ago = bars[-16]['c'] if len(bars) >= 16 else bars[0]['c']
    chg_15m = ((current - price_15m_ago) / price_15m_ago * 100) if price_15m_ago > 0 else 0

    # Check alignment
    aligned = (
        chg_1m >= MTF_MIN_CHG_1M and
        chg_5m >= MTF_MIN_CHG_5M and
        chg_15m >= MTF_MIN_CHG_15M
    )

    # Check if stalling (1m not contributing enough to 5m)
    stalling = chg_5m > 0 and chg_1m < chg_5m * MTF_MIN_CONTRIBUTION

    return {
        "chg_1m": round(chg_1m, 2),
        "chg_5m": round(chg_5m, 2),
        "chg_15m": round(chg_15m, 2),
        "aligned": aligned and not stalling,
        "stalling": stalling
    }

# ============================================================
# VOLUME ACCELERATION
# ============================================================

def calculate_volume_accel(bars: list) -> float:
    """
    Calculate volume acceleration (current bar vs prior 5-bar average).
    Returns multiplier (1.3 = 30% above average).
    """
    if not bars or len(bars) < 6:
        return 1.0

    current_vol = bars[-1].get('v', 0)
    prior_5_avg = sum(b.get('v', 0) for b in bars[-6:-1]) / 5

    if prior_5_avg <= 0:
        return 1.0

    return round(current_vol / prior_5_avg, 2)

# ============================================================
# SPREAD CALCULATION
# ============================================================

@st.cache_data(ttl=10)
def fetch_quote_data(symbol: str) -> dict:
    """Fetch real-time quote for spread calculation."""
    if not POLYGON_API_KEY:
        return {}

    try:
        url = f"https://api.polygon.io/v3/quotes/{symbol}"
        params = {"limit": 1, "apiKey": POLYGON_API_KEY}
        resp = requests.get(url, params=params, timeout=5)
        if resp.status_code == 200:
            results = resp.json().get("results", [])
            if results:
                return results[0]
    except Exception:
        pass
    return {}

def calculate_spread_bps(bid: float, ask: float, price: float) -> float:
    """Calculate bid-ask spread in basis points."""
    if bid <= 0 or ask <= 0 or price <= 0:
        return 0
    spread = ask - bid
    spread_pct = (spread / price) * 100
    return round(spread_pct * 100, 1)  # Convert to bps

# ============================================================
# ADVANCED GRADING
# ============================================================

def calculate_advanced_grade(
    gap_pct: float,
    rvol: float,
    float_shares: int,
    volume: int,
    adx: float = 0,
    mtf_aligned: bool = False,
    spread_bps: float = 0,
    vol_accel: float = 1.0
) -> tuple:
    """
    Calculate grade with advanced execution quality factors.

    Returns (grade, confidence, grade_factors)
    """
    # Initialize confidence score
    confidence = 0.0
    factors = []

    # === A+ CHECK (all criteria must be met) ===
    a_plus_criteria = {
        "gap": gap_pct >= GRADE_A_PLUS["min_gap"],
        "rvol": rvol >= GRADE_A_PLUS["min_rvol"],
        "adx": adx >= MIN_ADX_A_PLUS,
        "mtf": mtf_aligned,
        "spread": spread_bps <= MAX_SPREAD_BPS_A_PLUS or spread_bps == 0,
        "vol_accel": vol_accel >= MIN_VOL_ACCEL_A_PLUS
    }

    if all(a_plus_criteria.values()):
        grade = "A+"
        confidence = 85 + min(15, (adx - 25) + (vol_accel - 1.3) * 10)
        factors = ["Strong trend", "MTF aligned", "Tight spread", "Vol surge"]

    # === A CHECK ===
    elif (gap_pct >= GRADE_A["min_gap"] and
          rvol >= GRADE_A["min_rvol"] and
          (spread_bps <= MAX_SPREAD_BPS_A or spread_bps == 0)):
        grade = "A"
        confidence = 70
        if adx >= 20:
            confidence += 5
            factors.append("Emerging trend")
        if mtf_aligned:
            confidence += 5
            factors.append("MTF aligned")
        if vol_accel >= 1.2:
            confidence += 5
            factors.append("Vol rising")

    # === B CHECK ===
    elif (gap_pct >= GRADE_B["min_gap"] and
          rvol >= GRADE_B["min_rvol"] and
          (spread_bps <= MAX_SPREAD_BPS_B or spread_bps == 0)):
        grade = "B"
        confidence = 55
        if adx >= 20:
            confidence += 5

    # === C (Default) ===
    else:
        grade = "C"
        confidence = 40
        if spread_bps > MAX_SPREAD_BPS_B:
            factors.append("Wide spread")
        if adx < 20:
            factors.append("Choppy")

    # Float bonus
    if float_shares and float_shares > 0:
        if float_shares <= 2_000_000:
            confidence += 10
            factors.append("Tiny float")
        elif float_shares <= 5_000_000:
            confidence += 5
            factors.append("Low float")

    # Cap confidence
    confidence = min(99, max(30, confidence))

    return grade, round(confidence), factors

# ============================================================
# KEY LEVEL CALCULATION
# ============================================================

def get_key_level(price: float) -> tuple:
    """Calculate nearest key level ($0.50 or $1.00 increments)."""
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

def detect_breakout_status(price: float, high: float, key_level: float) -> tuple:
    """Detect breakout status relative to key level and HOD."""
    if price <= 0 or key_level <= 0:
        return "", ""

    pct_above_key = ((price - key_level) / key_level) * 100
    pct_from_hod = ((high - price) / price) * 100 if price > 0 else 0

    at_hod = pct_from_hod < 0.5

    if 0 <= pct_above_key <= 2 and at_hod:
        return "BREAKING", "🔥"
    if at_hod and pct_above_key > 0:
        return "HOD", "⬆️"
    if pct_above_key > 5:
        return "EXTENDED", "⚡"
    if -2 <= pct_above_key < 0:
        return "NEAR KEY", "👀"
    return "", ""

# ============================================================
# MARKET DATA FETCHERS
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
        params = {"adjusted": "true", "sort": "asc", "limit": 500, "apiKey": POLYGON_API_KEY}

        try:
            resp = requests.get(url, params=params, timeout=10)
            if resp.status_code != 200:
                continue

            bars = resp.json().get("results", [])
            if not bars or len(bars) < 2:
                continue

            current_price = bars[-1]["c"]
            price_5m_ago = bars[-6]["c"] if len(bars) >= 6 else bars[0]["c"]
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

    url = f"https://api.polygon.io/v2/aggs/ticker/VIXY/range/1/minute/{today}/{today}"
    params = {"adjusted": "true", "sort": "asc", "limit": 500, "apiKey": POLYGON_API_KEY}

    try:
        resp = requests.get(url, params=params, timeout=10)
        if resp.status_code != 200:
            return None

        bars = resp.json().get("results", [])
        if not bars or len(bars) < 2:
            return None

        current_price = bars[-1]["c"]
        open_price = bars[0]["o"]
        day_change_pct = ((current_price - open_price) / open_price * 100) if open_price > 0 else 0

        if day_change_pct > 10:
            fear_level, color = "EXTREME FEAR", "🔴"
        elif day_change_pct > 5:
            fear_level, color = "HIGH FEAR", "🟠"
        elif day_change_pct > 2:
            fear_level, color = "ELEVATED", "🟡"
        elif day_change_pct < -2:
            fear_level, color = "LOW FEAR", "🟢"
        else:
            fear_level, color = "NORMAL", "⚪"

        return {"price": current_price, "day_change_pct": day_change_pct, "fear_level": fear_level, "color": color}
    except Exception:
        return None

@st.cache_data(ttl=60)
def fetch_intraday_bars(symbol: str) -> list:
    """Fetch 1-minute bars for a symbol (for ADX, MTF momentum calculations)."""
    if not POLYGON_API_KEY:
        return []

    now = datetime.now(ET)
    today = now.date()

    url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/minute/{today}/{today}"
    params = {"adjusted": "true", "sort": "asc", "limit": 390, "apiKey": POLYGON_API_KEY}

    try:
        resp = requests.get(url, params=params, timeout=10)
        if resp.status_code == 200:
            return resp.json().get("results", [])
    except Exception:
        pass
    return []

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
        url = f"https://api.polygon.io/v2/reference/news"
        params = {"ticker": symbol, "limit": 3, "order": "desc", "apiKey": POLYGON_API_KEY}
        resp = requests.get(url, params=params, timeout=10)
        if resp.status_code == 200:
            results = resp.json().get("results", [])
            if results:
                headline = results[0].get("title", "")
                if len(headline) > 50:
                    headline = headline[:47] + "..."
                return headline
    except Exception:
        pass
    return ""

def categorize_catalyst(headline: str) -> str:
    """Categorize news headline into catalyst type."""
    if not headline:
        return ""

    headline_lower = headline.lower()

    if any(word in headline_lower for word in ["fda", "approval", "drug", "trial", "phase"]):
        return "FDA"
    elif any(word in headline_lower for word in ["earnings", "revenue", "profit", "quarter"]):
        return "EARN"
    elif any(word in headline_lower for word in ["contract", "deal", "partnership", "acquisition"]):
        return "DEAL"
    elif any(word in headline_lower for word in ["offering", "dilution", "shares"]):
        return "OFFR"
    elif any(word in headline_lower for word in ["short", "squeeze", "reddit"]):
        return "SQZ"
    return "NEWS"

# ============================================================
# MAIN DATA FETCHER
# ============================================================

@st.cache_data(ttl=15)
def fetch_enhanced_gainers():
    """Fetch top gainers with all advanced metrics."""
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

            # Get ticker details
            details = fetch_ticker_details(ticker_symbol)
            float_shares = details.get("share_class_shares_outstanding", 0) or details.get("weighted_shares_outstanding", 0)

            # RVOL calculation
            prev_volume = prev.get("v", 0)
            rvol = (volume / prev_volume) if prev_volume > 0 else 1.0

            # Float rotation
            float_rotation = (volume / float_shares * 100) if float_shares > 0 else 0

            # Key level and breakout status
            key_level, key_dist_pct, key_direction = get_key_level(price)
            breakout_status, breakout_emoji = detect_breakout_status(price, high, key_level)

            # ATR-based stop
            atr_estimate = (high - low) if high > low else price * 0.03
            stop = price - (atr_estimate * 2)
            stop_pct = ((price - stop) / price) * 100 if price > 0 else 0

            # Fetch intraday bars for advanced calculations
            bars = fetch_intraday_bars(ticker_symbol)

            # ADX calculation
            adx = calculate_adx(bars) if bars else 0

            # Multi-timeframe momentum
            mtf = calculate_mtf_momentum(bars) if bars else {"chg_1m": 0, "chg_5m": 0, "chg_15m": 0, "aligned": False}

            # Volume acceleration
            vol_accel = calculate_volume_accel(bars) if bars else 1.0

            # Spread (estimate from last bar if no quote data)
            spread_bps = 0
            if bars and len(bars) > 0:
                last_bar = bars[-1]
                bar_spread = (last_bar.get('h', 0) - last_bar.get('l', 0)) / price * 100 * 100 if price > 0 else 0
                spread_bps = min(bar_spread / 2, 200)  # Estimate spread as half of bar range, cap at 200bps

            # Advanced grading
            grade, confidence, factors = calculate_advanced_grade(
                gap_pct=gap_pct,
                rvol=rvol,
                float_shares=float_shares,
                volume=volume,
                adx=adx,
                mtf_aligned=mtf["aligned"],
                spread_bps=spread_bps,
                vol_accel=vol_accel
            )

            # Fetch catalyst for A+/A grades
            catalyst = ""
            catalyst_type = ""
            if grade in ["A+", "A"] or gap_pct >= 20:
                news_headline = fetch_ticker_news(ticker_symbol)
                catalyst = news_headline
                catalyst_type = categorize_catalyst(news_headline)

            setup = {
                "symbol": ticker_symbol,
                "price": price,
                "gap_pct": gap_pct,
                "volume": volume,
                "rvol": rvol,
                "float": float_shares,
                "float_rot": float_rotation,
                "key_level": key_level,
                "stop": stop,
                "grade": grade,
                "confidence": confidence,
                "factors": factors,
                "high": high,
                "low": low,
                "catalyst": catalyst,
                "catalyst_type": catalyst_type,
                "breakout_status": breakout_status,
                "breakout_emoji": breakout_emoji,
                # Advanced metrics
                "adx": adx,
                "mtf_1m": mtf["chg_1m"],
                "mtf_5m": mtf["chg_5m"],
                "mtf_15m": mtf["chg_15m"],
                "mtf_aligned": mtf["aligned"],
                "vol_accel": vol_accel,
                "spread_bps": spread_bps,
            }

            # Filter for main setups list
            if MIN_PRICE <= price <= MAX_PRICE and gap_pct >= MIN_GAP_PCT and volume >= MIN_VOLUME:
                setups.append(setup)
                if grade in ["A+", "A"]:
                    hot_list.append(setup)

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
    if ("Notification" in window && Notification.permission === "default") {
        Notification.requestPermission();
    }
    </script>
    """
    components.html(js_code, height=0)

def send_browser_notification(title: str, body: str, tag: str = "scanner", play_sound: bool = True):
    """Send a browser notification with optional sound alert."""
    title = title.replace("'", "\\'").replace('"', '\\"')
    body = body.replace("'", "\\'").replace('"', '\\"')

    # Sound: A short alert tone (base64 encoded)
    sound_js = """
        // Play alert sound
        try {
            var audioContext = new (window.AudioContext || window.webkitAudioContext)();
            var oscillator = audioContext.createOscillator();
            var gainNode = audioContext.createGain();
            oscillator.connect(gainNode);
            gainNode.connect(audioContext.destination);
            oscillator.frequency.value = 880; // A5 note
            oscillator.type = 'sine';
            gainNode.gain.setValueAtTime(0.3, audioContext.currentTime);
            gainNode.gain.exponentialRampToValueAtTime(0.01, audioContext.currentTime + 0.5);
            oscillator.start(audioContext.currentTime);
            oscillator.stop(audioContext.currentTime + 0.5);
        } catch(e) { console.log('Sound failed:', e); }
    """ if play_sound else ""

    js_code = f"""
    <script>
    if ("Notification" in window && Notification.permission === "granted") {{
        var notification = new Notification("{title}", {{
            body: "{body}",
            icon: "https://em-content.zobj.net/source/apple/354/chart-increasing_1f4c8.png",
            tag: "{tag}",
            requireInteraction: true
        }});
        setTimeout(function() {{ notification.close(); }}, 10000);
        {sound_js}
    }}
    </script>
    """
    components.html(js_code, height=0)

def play_a_plus_alert():
    """Play a distinctive alert sound for new A+ setups."""
    js_code = """
    <script>
    try {
        var audioContext = new (window.AudioContext || window.webkitAudioContext)();
        // Play two quick beeps
        [0, 0.2].forEach(function(delay) {
            var osc = audioContext.createOscillator();
            var gain = audioContext.createGain();
            osc.connect(gain);
            gain.connect(audioContext.destination);
            osc.frequency.value = 1047; // C6 note
            osc.type = 'sine';
            gain.gain.setValueAtTime(0.3, audioContext.currentTime + delay);
            gain.gain.exponentialRampToValueAtTime(0.01, audioContext.currentTime + delay + 0.15);
            osc.start(audioContext.currentTime + delay);
            osc.stop(audioContext.currentTime + delay + 0.15);
        });
    } catch(e) { console.log('Sound failed:', e); }
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
            if symbol not in st.session_state.seen_a_plus_setups:
                new_setups.append(setup)

    for setup in new_setups:
        body = f"Gap: +{setup['gap_pct']:.1f}% | ADX: {setup['adx']} | Conf: {setup['confidence']}%"
        send_browser_notification(
            title=f"🟣 A+ SETUP: {setup['symbol']}",
            body=body,
            tag=f"setup_{setup['symbol']}",
            play_sound=True
        )

    # Play additional alert sound for any new A+ setups
    if new_setups:
        play_a_plus_alert()

    st.session_state.seen_a_plus_setups = current_a_plus

# ============================================================
# UI COMPONENTS
# ============================================================

def get_grade_emoji(grade: str) -> str:
    """Get emoji for grade."""
    return {"A+": "🟣", "A": "🟢", "B": "🟡", "C": "🔴"}.get(grade, "⚪")

def format_float(value: int) -> str:
    """Format float/shares for display."""
    if not value or value <= 0:
        return "-"
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

def get_adx_indicator(adx: float) -> str:
    """Get visual indicator for ADX strength."""
    if adx >= 25:
        return f"🟢 {adx:.0f}"
    elif adx >= 20:
        return f"🟡 {adx:.0f}"
    else:
        return f"🔴 {adx:.0f}"

def get_mtf_indicator(mtf_aligned: bool, chg_1m: float, chg_5m: float) -> str:
    """Get visual indicator for MTF alignment."""
    if mtf_aligned:
        return "🟢 ↑↑↑"
    elif chg_1m > 0 and chg_5m > 0:
        return "🟡 ↑↑"
    elif chg_1m < 0 and chg_5m < 0:
        return "🔴 ↓↓"
    return "⚪ ↔"

def get_spread_indicator(spread_bps: float) -> str:
    """Get visual indicator for spread quality."""
    if spread_bps <= 50:
        return f"🟢 {spread_bps:.0f}"
    elif spread_bps <= 100:
        return f"🟡 {spread_bps:.0f}"
    else:
        return f"🔴 {spread_bps:.0f}"

def get_vol_accel_indicator(vol_accel: float) -> str:
    """Get visual indicator for volume acceleration."""
    if vol_accel >= 1.3:
        return f"🟢 {vol_accel:.1f}x"
    elif vol_accel >= 1.0:
        return f"🟡 {vol_accel:.1f}x"
    else:
        return f"🔴 {vol_accel:.1f}x"

def display_market_indicator():
    """Display the market direction indicator with VIX - compact single line."""
    momentum = fetch_market_momentum()
    vix_data = fetch_vix_data()

    if not momentum:
        st.caption("Market data unavailable")
        return

    # Build compact single-line display
    spy_data = momentum.get("SPY", {})
    qqq_data = momentum.get("QQQ", {})

    # Determine overall sentiment
    up_count = sum(1 for d in momentum.values() if d["direction"] == "UP")
    vix_fear = vix_data and vix_data["fear_level"] in ["HIGH FEAR", "EXTREME FEAR"]

    if up_count == len(momentum) and not vix_fear:
        sentiment = "🟢 BULLISH"
        sent_color = "#00cc00"
    elif vix_fear:
        sentiment = "⚠️ CAUTION"
        sent_color = "#ffaa00"
    elif up_count == 0:
        sentiment = "🔴 BEARISH"
        sent_color = "#ff4444"
    else:
        sentiment = "🟡 MIXED"
        sent_color = "#ffcc00"

    # Format SPY with HTML
    spy_html = ""
    if spy_data:
        chg = spy_data["change_5m"]
        arrow = "▲" if chg > 0 else ("▼" if chg < 0 else "►")
        color = "#00cc00" if chg > 0 else ("#ff4444" if chg < 0 else "#888888")
        spy_html = f'<b>SPY</b> ${spy_data["price"]:.2f} <span style="color:{color}">{arrow}{chg:+.2f}%</span>'

    # Format QQQ with HTML
    qqq_html = ""
    if qqq_data:
        chg = qqq_data["change_5m"]
        arrow = "▲" if chg > 0 else ("▼" if chg < 0 else "►")
        color = "#00cc00" if chg > 0 else ("#ff4444" if chg < 0 else "#888888")
        qqq_html = f'<b>QQQ</b> ${qqq_data["price"]:.2f} <span style="color:{color}">{arrow}{chg:+.2f}%</span>'

    # Format VIX with HTML (inverse colors - up is bad)
    vix_html = ""
    if vix_data:
        chg = vix_data["day_change_pct"]
        arrow = "▲" if chg > 0 else ("▼" if chg < 0 else "►")
        color = "#ff4444" if chg > 0 else ("#00cc00" if chg < 0 else "#888888")
        vix_html = f'<b>VIX</b> ${vix_data["price"]:.2f} <span style="color:{color}">{arrow}{chg:+.1f}%</span>'

    # Display as compact single line using components.html for reliable rendering
    html_content = f'''
    <div style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
                font-size: 14px; color: #fafafa; padding: 2px 0;">
        {spy_html} &nbsp;|&nbsp; {qqq_html} &nbsp;|&nbsp; {vix_html} &nbsp;|&nbsp;
        <span style="color:{sent_color}"><b>{sentiment}</b></span>
    </div>
    '''
    components.html(html_content, height=30)

def display_setups_table(setups: list, table_key: str = "main"):
    """Display setups in a professional compact table."""
    if not setups:
        st.info("No setups currently meet all criteria. Scanning...")
        return

    rows = []
    for setup in setups:
        grade = setup["grade"]
        symbol = setup["symbol"]
        tv_link = f"{TRADINGVIEW_URL}{symbol}"

        # Compact catalyst display
        cat = setup.get("catalyst_type", "")

        # Format status
        status = setup.get("breakout_status", "")
        status_emoji = setup.get("breakout_emoji", "")
        status_display = f"{status_emoji}{status}" if status else ""

        rows.append({
            "St": status_display,
            "Grd": f"{get_grade_emoji(grade)}{grade}",
            "Sym": symbol,
            "📈": tv_link,
            "Cat": cat if cat else "-",
            "Price": f"${setup['price']:.2f}",
            "Gap": f"+{setup['gap_pct']:.0f}%",
            "RVOL": f"{setup['rvol']:.0f}x",
            "ADX": get_adx_indicator(setup['adx']),
            "MTF": get_mtf_indicator(setup['mtf_aligned'], setup['mtf_1m'], setup['mtf_5m']),
            "Sprd": get_spread_indicator(setup['spread_bps']),
            "VAcc": get_vol_accel_indicator(setup['vol_accel']),
            "Rot": f"{setup['float_rot']:.0f}%" if setup['float_rot'] else "-",
            "Conf": f"{setup['confidence']}%",
            "Float": format_float(setup["float"]),
            "Key": f"${setup['key_level']:.2f}",
            "Stop": f"${setup['stop']:.2f}",
        })

    df = pd.DataFrame(rows)

    # Dynamic height
    table_height = (len(rows) * 35) + 40

    st.dataframe(
        df,
        use_container_width=True,
        hide_index=True,
        height=table_height,
        key=f"setups_{table_key}_{len(rows)}",
        column_config={
            "St": st.column_config.TextColumn("Status", width="small"),
            "Grd": st.column_config.TextColumn("Grade", width="small"),
            "Sym": st.column_config.TextColumn("Symbol", width="small"),
            "📈": st.column_config.LinkColumn("Chart", display_text="📈", width="small"),
            "Cat": st.column_config.TextColumn("Cat", width="small"),
            "Price": st.column_config.TextColumn("Price", width="small"),
            "Gap": st.column_config.TextColumn("Gap%", width="small"),
            "RVOL": st.column_config.TextColumn("RVOL", width="small"),
            "ADX": st.column_config.TextColumn("ADX", width="small", help="Trend strength: >25=strong"),
            "MTF": st.column_config.TextColumn("MTF", width="small", help="Multi-TF momentum alignment"),
            "Sprd": st.column_config.TextColumn("Spread", width="small", help="Bid-ask spread in bps"),
            "VAcc": st.column_config.TextColumn("VAcc", width="small", help="Volume acceleration"),
            "Rot": st.column_config.TextColumn("Rot", width="small", help="Float rotation %"),
            "Conf": st.column_config.TextColumn("Conf", width="small", help="Confidence score"),
            "Float": st.column_config.TextColumn("Float", width="small"),
            "Key": st.column_config.TextColumn("Key Lvl", width="small"),
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

    if authentication_status == False:
        st.error('Username/password is incorrect')
        return
    if authentication_status == None:
        st.warning('Please enter your username and password')
        return

    # Initialize notification state
    init_notification_state()

    # SIDEBAR - Reference info
    with st.sidebar:
        st.write(f"**{name}**")
        try:
            authenticator.logout(location='sidebar')
        except TypeError:
            authenticator.logout('Logout', 'sidebar')
        st.divider()

        # Settings
        st.subheader("Settings")
        auto_refresh = st.checkbox("Auto-refresh", value=True)
        refresh_interval = st.slider("Refresh (sec)", 10, 60, 15)
        notifications_enabled = st.checkbox("A+ Alerts", value=st.session_state.get("notifications_enabled", False))
        st.session_state.notifications_enabled = notifications_enabled
        if notifications_enabled:
            request_notification_permission()

        min_grade = st.selectbox("Min Grade", ["All", "A+", "A", "B"], index=0)

        st.divider()

        # Column Legend (compact)
        st.subheader("Column Guide")
        st.markdown("""
        **Core:**
        - **St** = Status (🔥BREAKING, ⬆️HOD, 👀NEAR)
        - **Grd** = Grade (🟣A+ 🟢A 🟡B 🔴C)
        - **Gap** = Gap from previous close
        - **RVOL** = Relative Volume

        **Advanced:**
        - **ADX** = Trend strength (>25 = strong)
        - **MTF** = Multi-TF momentum (↑↑↑ = aligned)
        - **Sprd** = Bid-ask spread (bps)
        - **VAcc** = Volume acceleration
        - **Rot** = Float rotation (squeeze %)
        - **Conf** = Confidence score
        """)

        st.divider()

        # A+ Requirements
        st.subheader("A+ Requirements")
        st.caption("Gap ≥12% | RVOL ≥6x")
        st.caption("ADX ≥25 | MTF aligned")
        st.caption("Spread ≤50bps | VAcc ≥1.3x")

        st.divider()
        st.caption(f"Scanner v{VERSION} Pro")

    # =========================================================
    # MAIN CONTENT
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

    # Header
    col_title, col_session = st.columns([3, 1])
    with col_title:
        st.markdown(f"## 📈 Smallcap Scanner Pro v{VERSION}")
    with col_session:
        st.markdown(f"**{color} {session}**")
        st.caption(now.strftime('%I:%M:%S %p ET'))

    # Quick guide (collapsed)
    with st.expander("📖 Quick Guide & Glossary", expanded=False):
        st.markdown("""
### How to Use This Scanner

**1. Check Market Direction first** - Only trade A+ setups when sentiment is 🟢BULLISH or 🟡MIXED.
Avoid longs when 🔴BEARISH or ⚠️CAUTION (VIX spiking).

**2. Focus on Prime Setups table** - These are A+ grade setups that meet ALL quality criteria.
A+ setups have the highest probability of follow-through.

**3. Use the Watchlist** - Monitor A/B/C setups for potential upgrades or status changes.

---

### Column Definitions

| Column | Full Name | Description |
|--------|-----------|-------------|
| **St** | Status | 🔥BREAKING=breaking key level, ⬆️HOD=at high of day, 👀NEAR KEY=approaching breakout, ⚡EXTENDED=extended above key |
| **Grd** | Grade | 🟣A+=highest quality, 🟢A=strong, 🟡B=decent, 🔴C=weak |
| **Sym** | Symbol | Stock ticker |
| **Cat** | Catalyst | NEWS, FDA, EARN (earnings), DEAL, OFFR (offering), SQZ (squeeze) |
| **Gap** | Gap % | Percentage gap from previous close |
| **RVOL** | Relative Volume | Today's volume vs average (10x = 10 times normal) |
| **ADX** | Average Directional Index | Trend strength: 🟢>25=strong trend, 🟡20-25=emerging, 🔴<20=choppy/no trend |
| **MTF** | Multi-Timeframe Momentum | 1m/5m/15m alignment: 🟢↑↑↑=all up, 🟡↑↑=partial, 🔴↓↓=against |
| **Sprd** | Spread (bps) | Bid-ask spread in basis points: 🟢<50=tight, 🟡50-100=ok, 🔴>100=wide (hard to fill) |
| **VAcc** | Volume Acceleration | Current vs recent volume: 🟢>1.3x=surging, 🟡1.0-1.3x=steady, 🔴<1.0x=fading |
| **Rot** | Float Rotation | % of float traded today. >50%=high squeeze potential |
| **Conf** | Confidence | Overall setup quality score (0-99%) |
| **Float** | Float Size | Shares available to trade. Lower=more explosive |
| **Key** | Key Level | Nearest psychological price level ($0.50 or $1.00 increments) |
| **Stop** | Stop Loss | Suggested stop price (2x ATR below entry) |

---

### A+ Grade Requirements (ALL must be met)

- Gap ≥12% from previous close
- RVOL ≥6x average volume
- ADX ≥25 (strong directional trend)
- MTF aligned (1m/5m/15m all positive)
- Spread ≤50bps (tight execution)
- Volume Acceleration ≥1.3x

---

### Risk Management

⚠️ **Risk 1-2% of account per trade maximum.**
- Use the provided Stop level
- Small-caps are volatile - position size accordingly
- Best setups occur 9:30-11:00 AM ET ("Prime Time")
        """)

    # Market Direction
    st.markdown("##### Market Direction")
    display_market_indicator()

    # Fetch data
    setups, hot_list = fetch_enhanced_gainers()

    # Apply grade filter
    if min_grade != "All":
        grade_order = {"A+": 0, "A": 1, "B": 2, "C": 3}
        min_grade_val = grade_order.get(min_grade, 3)
        setups = [s for s in setups if grade_order.get(s["grade"], 4) <= min_grade_val]

    # Different display based on market hours
    if is_after_hours:
        st.warning("📊 **Market Closed** - Today's summary. Live scanning resumes 9:30 AM ET.")
        grade_order = {"A+": 0, "A": 1, "B": 2, "C": 3}
        setups.sort(key=lambda x: (grade_order.get(x.get("grade", "C"), 3), -x.get("gap_pct", 0)))

        a_plus = sum(1 for s in setups if s.get("grade") == "A+")
        a_count = sum(1 for s in setups if s.get("grade") == "A")
        header = f"Today's Movers ({len(setups)})"
        if a_plus: header += f" | 🟣{a_plus} A+"
        if a_count: header += f" | 🟢{a_count} A"
        st.markdown(f"##### {header}")

    elif is_premarket:
        st.info("🌅 **Pre-Market** - Previous day data. Live scanning at 9:30 AM ET.")
        grade_order = {"A+": 0, "A": 1, "B": 2, "C": 3}
        setups.sort(key=lambda x: (grade_order.get(x.get("grade", "C"), 3), -x.get("gap_pct", 0)))
        st.markdown(f"##### Watch List ({len(setups)})")

    else:
        # Market hours - full scanning with TWO tables
        check_and_notify_new_setups(hot_list)

        # Split into Prime (A+ ONLY) and Watchlist (A, B, C)
        prime_setups = [s for s in setups if s.get("grade") == "A+"]
        watch_setups = [s for s in setups if s.get("grade") in ["A", "B", "C"]]

        # Sort Prime by status, then gap
        status_priority = {"BREAKING": 0, "HOD": 1, "NEAR KEY": 2, "EXTENDED": 3, "": 4}

        prime_setups.sort(key=lambda x: (
            status_priority.get(x.get("breakout_status", ""), 4),
            -x.get("gap_pct", 0)
        ))

        # Sort Watchlist by grade first, then status, then gap
        grade_order = {"A+": 0, "A": 1, "B": 2, "C": 3}
        watch_setups.sort(key=lambda x: (
            grade_order.get(x.get("grade", "C"), 3),
            status_priority.get(x.get("breakout_status", ""), 4),
            -x.get("gap_pct", 0)
        ))

        # === PRIME SETUPS TABLE (A+ ONLY) ===
        st.markdown(f"##### 🎯 Prime Setups ({len(prime_setups)})")

        if prime_setups:
            display_setups_table(prime_setups, table_key="prime")
        else:
            st.info("No qualified setups found at this time.")

        # === WATCHLIST TABLE (A, B, C) ===
        if watch_setups:
            st.markdown(f"##### 👀 Watchlist ({len(watch_setups)})")
            display_setups_table(watch_setups, table_key="watch")

        # Skip the common display below
        setups = None

    # Display table (for after-hours and pre-market)
    if setups:
        display_setups_table(setups)

    # Disclaimer
    st.divider()
    st.caption("⚠️ **Disclaimer:** This scanner is for informational purposes only and does not constitute financial advice. "
               "Trading small-cap stocks involves significant risk of loss. Past performance does not guarantee future results. "
               "Always do your own research and consult a licensed financial advisor before making investment decisions.")

    # Auto-refresh during market hours using JavaScript (seamless - no "Running..." indicator)
    if auto_refresh and is_market_hours:
        refresh_ms = refresh_interval * 1000
        js_refresh = f"""
        <script>
        setTimeout(function() {{
            window.parent.location.reload();
        }}, {refresh_ms});
        </script>
        """
        components.html(js_refresh, height=0)

if __name__ == "__main__":
    main()
