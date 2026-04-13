"""Reporting modules for AI Investment Manager."""

from .email_renderer import EmailRenderer, render_email, send_email
from .charts import ChartGenerator, generate_portfolio_charts

__all__ = [
    "EmailRenderer",
    "render_email",
    "send_email",
    "ChartGenerator",
    "generate_portfolio_charts",
]
