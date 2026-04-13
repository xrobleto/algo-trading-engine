"""Type definitions for AI Investment Manager."""

from dataclasses import dataclass, field
from datetime import datetime, date
from decimal import Decimal
from enum import Enum
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field, field_validator


# ============================================================
# ENUMS
# ============================================================

class ActionType(str, Enum):
    """Types of recommended actions."""
    ADD = "ADD"
    TRIM = "TRIM"
    HOLD = "HOLD"
    HEDGE = "HEDGE"
    SET_STOP = "SET_STOP"
    TAKE_PROFIT = "TAKE_PROFIT"
    REVIEW = "REVIEW"


class Urgency(str, Enum):
    """Urgency levels for recommendations."""
    LOW = "LOW"
    MED = "MED"
    HIGH = "HIGH"


class SignalStrength(str, Enum):
    """Signal strength classification."""
    STRONG_BULLISH = "STRONG_BULLISH"
    BULLISH = "BULLISH"
    NEUTRAL = "NEUTRAL"
    BEARISH = "BEARISH"
    STRONG_BEARISH = "STRONG_BEARISH"


class TransactionType(str, Enum):
    """Transaction types from CSV."""
    BUY = "BUY"
    SELL = "SELL"
    DIVIDEND = "DIVIDEND"
    FEE = "FEE"
    UNKNOWN = "UNKNOWN"


class NewsSentiment(str, Enum):
    """News sentiment classification."""
    VERY_POSITIVE = "VERY_POSITIVE"
    POSITIVE = "POSITIVE"
    NEUTRAL = "NEUTRAL"
    NEGATIVE = "NEGATIVE"
    VERY_NEGATIVE = "VERY_NEGATIVE"


class CatalystType(str, Enum):
    """Types of news catalysts."""
    EARNINGS = "EARNINGS"
    GUIDANCE = "GUIDANCE"
    PRODUCT = "PRODUCT"
    ACQUISITION = "ACQUISITION"
    MERGER = "MERGER"
    LAWSUIT = "LAWSUIT"
    REGULATORY = "REGULATORY"
    FDA = "FDA"
    CLINICAL = "CLINICAL"
    CONTRACT = "CONTRACT"
    PARTNERSHIP = "PARTNERSHIP"
    MACRO = "MACRO"
    UNKNOWN = "UNKNOWN"


# ============================================================
# DATA CLASSES
# ============================================================

@dataclass
class Transaction:
    """A single transaction from the ledger."""
    activity_date: date
    process_date: Optional[date]
    settle_date: Optional[date]
    symbol: Optional[str]
    description: str
    trans_code: str
    trans_type: TransactionType
    quantity: Optional[Decimal]
    price: Optional[Decimal]
    amount: Optional[Decimal]
    raw_row: Dict[str, str] = field(default_factory=dict)


@dataclass
class Holding:
    """A reconstructed portfolio holding."""
    symbol: str
    shares: Decimal
    avg_cost: Decimal
    total_cost: Decimal
    current_price: Optional[float] = None
    current_value: Optional[float] = None
    unrealized_pnl: Optional[float] = None
    unrealized_pnl_pct: Optional[float] = None
    dividends_received: Decimal = Decimal("0")
    fees_paid: Decimal = Decimal("0")
    last_activity_date: Optional[date] = None
    sector: Optional[str] = None

    # Technical data (populated later)
    price_change_1d: Optional[float] = None
    price_change_5d: Optional[float] = None
    price_change_20d: Optional[float] = None
    sma_20: Optional[float] = None
    sma_50: Optional[float] = None
    sma_200: Optional[float] = None
    rsi_14: Optional[float] = None
    atr_14: Optional[float] = None
    volatility_20d: Optional[float] = None

    # News data (populated later)
    news_items: List["NewsItem"] = field(default_factory=list)
    news_sentiment: NewsSentiment = NewsSentiment.NEUTRAL
    catalyst_type: Optional[CatalystType] = None

    # Recommendation (populated by analysis)
    recommendation: str = "HOLD"  # BUY, HOLD, SELL, or TRIM
    recommendation_reasons: List[str] = field(default_factory=list)

    def weight_pct(self, total_value: float) -> float:
        """Calculate position weight as % of portfolio."""
        if total_value <= 0 or self.current_value is None:
            return 0.0
        return (self.current_value / total_value) * 100


@dataclass
class NewsItem:
    """A news article."""
    title: str
    url: str
    published_at: datetime
    source: str
    tickers: List[str] = field(default_factory=list)
    sentiment: NewsSentiment = NewsSentiment.NEUTRAL
    catalyst_type: CatalystType = CatalystType.UNKNOWN
    summary: Optional[str] = None


@dataclass
class MacroIndicator:
    """A macro economic indicator."""
    series_id: str
    name: str
    value: float
    date: date
    previous_value: Optional[float] = None
    change: Optional[float] = None
    alert_triggered: bool = False
    alert_reason: Optional[str] = None


@dataclass
class TechnicalSignal:
    """Technical analysis signal for a ticker."""
    symbol: str
    timestamp: datetime

    # Price data
    price: float
    change_1d_pct: float
    change_5d_pct: float
    change_20d_pct: float

    # Moving averages
    sma_20: Optional[float] = None
    sma_50: Optional[float] = None
    sma_200: Optional[float] = None
    ema_9: Optional[float] = None
    ema_21: Optional[float] = None

    # Trend signals
    above_sma_20: Optional[bool] = None
    above_sma_50: Optional[bool] = None
    above_sma_200: Optional[bool] = None
    sma_20_slope: Optional[float] = None
    golden_cross: bool = False
    death_cross: bool = False

    # Momentum
    rsi_14: Optional[float] = None
    macd: Optional[float] = None
    macd_signal: Optional[float] = None
    macd_histogram: Optional[float] = None

    # Volatility
    atr_14: Optional[float] = None
    volatility_20d: Optional[float] = None

    # Key levels
    support_level: Optional[float] = None
    resistance_level: Optional[float] = None
    recent_high: Optional[float] = None
    recent_low: Optional[float] = None

    # Overall signal
    signal_strength: SignalStrength = SignalStrength.NEUTRAL
    signal_score: float = 50.0  # 0-100


@dataclass
class PortfolioSnapshot:
    """Complete portfolio snapshot at a point in time."""
    timestamp: datetime
    holdings: List[Holding]
    total_value: float
    cash: Decimal = Decimal("0")

    # Allocation metrics
    top_holding_pct: float = 0.0
    top_3_holdings_pct: float = 0.0
    top_5_holdings_pct: float = 0.0
    sector_allocations: Dict[str, float] = field(default_factory=dict)

    # Risk metrics
    portfolio_volatility: Optional[float] = None
    max_drawdown_pct: Optional[float] = None
    beta_vs_spy: Optional[float] = None
    correlation_matrix: Optional[Dict[str, Dict[str, float]]] = None

    # P/L
    total_cost_basis: float = 0.0
    total_unrealized_pnl: float = 0.0
    total_unrealized_pnl_pct: float = 0.0
    total_dividends: float = 0.0

    # Flags
    concentration_warning: bool = False
    sector_concentration_warning: bool = False
    high_correlation_warning: bool = False

    def get_holding(self, symbol: str) -> Optional[Holding]:
        """Get holding by symbol."""
        for h in self.holdings:
            if h.symbol == symbol:
                return h
        return None


@dataclass
class ActionRecommendation:
    """A recommended action for a ticker."""
    ticker: str
    action: ActionType
    urgency: Urgency
    confidence: int  # 0-100
    time_horizon_days: int
    rationale_bullets: List[str]
    risks: List[str]
    key_levels: Dict[str, Optional[float]]  # support, resistance, stop, target

    # Scoring breakdown
    technical_score: float = 50.0
    news_score: float = 50.0
    macro_score: float = 50.0
    risk_score: float = 50.0
    overall_score: float = 50.0


# ============================================================
# PYDANTIC MODELS (for LLM JSON validation)
# ============================================================

class KeyLevels(BaseModel):
    """Key price levels for a recommendation."""
    support: Optional[float] = None
    resistance: Optional[float] = None
    stop: Optional[float] = None
    target: Optional[float] = None


class LLMAction(BaseModel):
    """A single action from LLM response."""
    ticker: str
    action: str = Field(..., pattern="^(ADD|TRIM|HOLD|HEDGE|SET_STOP|TAKE_PROFIT|REVIEW)$")
    urgency: str = Field(..., pattern="^(LOW|MED|HIGH)$")
    rationale_bullets: List[str] = Field(min_length=1, max_length=5)
    key_levels: KeyLevels
    risks: List[str] = Field(min_length=1, max_length=4)
    confidence: int = Field(ge=0, le=100)
    time_horizon_days: int = Field(ge=1, le=180)

    @field_validator("ticker")
    @classmethod
    def ticker_uppercase(cls, v: str) -> str:
        return v.upper().strip()


class Citation(BaseModel):
    """A citation/source from LLM response."""
    type: str = Field(..., pattern="^(news|data|macro|alert)$")
    title: str
    url: Optional[str] = None
    published_at: Optional[str] = None
    tickers: List[str] = Field(default_factory=list)


class DataFreshness(BaseModel):
    """Data freshness timestamps from LLM response."""
    massive: Optional[str] = None
    alpaca: Optional[str] = None
    fred: Optional[str] = None
    tradingview: Optional[str] = None


class LLMResponse(BaseModel):
    """Complete LLM response schema (strict JSON)."""
    executive_summary: str = Field(min_length=50, max_length=1000)
    top_actions: List[LLMAction] = Field(min_length=0, max_length=7)
    portfolio_notes: List[str] = Field(min_length=0, max_length=5)
    citations: List[Citation] = Field(default_factory=list)
    data_freshness: DataFreshness


# ============================================================
# EVIDENCE PACKET (passed to LLM)
# ============================================================

@dataclass
class EvidencePacket:
    """Compact evidence packet for LLM summarization."""
    # Portfolio summary
    total_value: float
    top_holdings: List[Dict[str, Any]]  # [{symbol, weight_pct, pnl_pct, current_price}]
    concentration_flags: List[str]

    # Scores (pre-computed, deterministic)
    risk_alert_score: float
    opportunity_score: float

    # Per-ticker signals
    ticker_signals: Dict[str, Dict[str, Any]]
    # {symbol: {technical_score, news_score, rsi, trend, support, resistance, ...}}

    # Top news items (title, url, published_at, sentiment)
    top_news: List[Dict[str, Any]]

    # Macro indicators
    macro_indicators: List[Dict[str, Any]]
    # [{name, value, change, alert}]

    # TradingView alerts (if any)
    tv_alerts: List[Dict[str, Any]]

    # Constraints
    max_single_position_pct: float
    max_sector_pct: float

    # Timestamps
    data_timestamps: Dict[str, Optional[str]]

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "total_value": self.total_value,
            "top_holdings": self.top_holdings,
            "concentration_flags": self.concentration_flags,
            "risk_alert_score": self.risk_alert_score,
            "opportunity_score": self.opportunity_score,
            "ticker_signals": self.ticker_signals,
            "top_news": self.top_news[:10],  # Limit to avoid large context
            "macro_indicators": self.macro_indicators,
            "tv_alerts": self.tv_alerts[:5],
            "constraints": {
                "max_single_position_pct": self.max_single_position_pct,
                "max_sector_pct": self.max_sector_pct,
            },
            "data_timestamps": self.data_timestamps,
        }
