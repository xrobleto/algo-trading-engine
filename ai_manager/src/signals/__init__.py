"""Signal generation modules for AI Investment Manager."""

from .technicals import TechnicalAnalyzer, compute_technical_signals
from .news import NewsAnalyzer, aggregate_news_sentiment
from .macro import MacroAnalyzer, get_macro_context
from .risk import RiskAnalyzer, compute_portfolio_risk

__all__ = [
    "TechnicalAnalyzer",
    "compute_technical_signals",
    "NewsAnalyzer",
    "aggregate_news_sentiment",
    "MacroAnalyzer",
    "get_macro_context",
    "RiskAnalyzer",
    "compute_portfolio_risk",
]
