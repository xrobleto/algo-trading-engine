"""
Email Rendering Module

Renders HTML emails using Jinja2 templates.
Sends emails via SMTP.
"""

import os
import smtplib
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from typing import Dict, List, Optional, Any

from jinja2 import Environment, FileSystemLoader, select_autoescape

from ..utils.logging import get_logger
from ..utils.time import format_timestamp
from ..utils.typing import (
    PortfolioSnapshot, ActionRecommendation, EvidencePacket, LLMResponse
)
from .charts import generate_portfolio_charts

logger = get_logger(__name__)


class EmailRenderer:
    """
    Renders HTML emails from templates.

    Uses Jinja2 for templating with inline CSS for email compatibility.
    """

    def __init__(self, template_dir: Optional[str] = None):
        """
        Initialize email renderer.

        Args:
            template_dir: Directory containing email templates
        """
        if template_dir is None:
            # Default to templates directory relative to package
            template_dir = str(Path(__file__).parent.parent.parent / "templates")

        self.template_dir = template_dir

        # Setup Jinja2 environment
        self.env = Environment(
            loader=FileSystemLoader(template_dir),
            autoescape=select_autoescape(['html', 'xml'])
        )

        # Add custom filters
        self.env.filters['format_money'] = self._format_money
        self.env.filters['format_pct'] = self._format_pct
        self.env.filters['format_date'] = self._format_date
        self.env.filters['urgency_color'] = self._urgency_color
        self.env.filters['action_color'] = self._action_color
        self.env.filters['sentiment_color'] = self._sentiment_color

    def render(
        self,
        snapshot: PortfolioSnapshot,
        recommendations: List[ActionRecommendation],
        evidence_packet: EvidencePacket,
        llm_response: Optional[LLMResponse] = None,
        charts: Optional[Dict[str, str]] = None,
        include_charts: bool = True,
    ) -> str:
        """
        Render email HTML.

        Args:
            snapshot: Portfolio snapshot
            recommendations: List of recommendations
            evidence_packet: Evidence packet
            llm_response: Optional LLM-generated content
            charts: Pre-generated charts (base64)
            include_charts: Whether to include charts

        Returns:
            Rendered HTML string
        """
        # Generate charts if needed
        if include_charts and charts is None:
            charts = generate_portfolio_charts(
                snapshot,
                risk_score=evidence_packet.risk_alert_score,
                opportunity_score=evidence_packet.opportunity_score
            )

        # Load template
        try:
            template = self.env.get_template("email.html.j2")
        except Exception as e:
            logger.error(f"Failed to load email template: {e}")
            # Fall back to simple template
            return self._render_simple(snapshot, recommendations, evidence_packet)

        # Prepare template context
        context = {
            "timestamp": datetime.now(),
            "snapshot": snapshot,
            "recommendations": recommendations,
            "evidence": evidence_packet,
            "llm_response": llm_response,
            "charts": charts or {},

            # Executive summary (from LLM or fallback)
            "executive_summary": (
                llm_response.executive_summary if llm_response
                else self._generate_fallback_summary(evidence_packet)
            ),

            # Portfolio metrics
            "total_value": snapshot.total_value,
            "total_pnl": snapshot.total_unrealized_pnl,
            "total_pnl_pct": snapshot.total_unrealized_pnl_pct,
            "num_holdings": len(snapshot.holdings),
            "holdings_count": len(snapshot.holdings),
            "top_holdings": snapshot.holdings[:5],
            "sector_allocations": snapshot.sector_allocations,

            # Scores
            "risk_score": evidence_packet.risk_alert_score,
            "opportunity_score": evidence_packet.opportunity_score,

            # News
            "top_news": evidence_packet.top_news[:5],

            # Macro
            "macro_indicators": evidence_packet.macro_indicators,

            # Data freshness
            "data_freshness": evidence_packet.data_timestamps,
        }

        return template.render(**context)

    def _render_simple(
        self,
        snapshot: PortfolioSnapshot,
        recommendations: List[ActionRecommendation],
        evidence_packet: EvidencePacket,
    ) -> str:
        """Render simple fallback HTML when template fails."""
        html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="utf-8">
            <title>Portfolio Alert</title>
        </head>
        <body style="font-family: Arial, sans-serif; max-width: 800px; margin: 0 auto; padding: 20px;">
            <h1 style="color: #2563eb;">AI Investment Manager Alert</h1>

            <h2>Portfolio Summary</h2>
            <p>Total Value: ${snapshot.total_value:,.2f}</p>
            <p>Risk Score: {evidence_packet.risk_alert_score:.0f}/100</p>
            <p>Opportunity Score: {evidence_packet.opportunity_score:.0f}/100</p>

            <h2>Recommendations</h2>
            <table style="width: 100%; border-collapse: collapse;">
                <tr style="background: #f3f4f6;">
                    <th style="padding: 8px; text-align: left;">Ticker</th>
                    <th style="padding: 8px; text-align: left;">Action</th>
                    <th style="padding: 8px; text-align: left;">Urgency</th>
                    <th style="padding: 8px; text-align: left;">Confidence</th>
                </tr>
        """

        for rec in recommendations[:7]:
            html += f"""
                <tr>
                    <td style="padding: 8px;">{rec.ticker}</td>
                    <td style="padding: 8px;">{rec.action.value}</td>
                    <td style="padding: 8px;">{rec.urgency.value}</td>
                    <td style="padding: 8px;">{rec.confidence}%</td>
                </tr>
            """

        html += """
            </table>

            <hr style="margin: 20px 0;">
            <p style="font-size: 12px; color: #6b7280;">
                <strong>Disclaimer:</strong> This is for educational and informational purposes only.
                It does not constitute financial advice. Always verify recommendations independently
                before making investment decisions.
            </p>
        </body>
        </html>
        """

        return html

    def _generate_fallback_summary(self, evidence: EvidencePacket) -> str:
        """Generate fallback executive summary."""
        if evidence.risk_alert_score >= 70:
            return (
                f"Risk Alert: Portfolio risk score elevated at {evidence.risk_alert_score:.0f}/100. "
                f"Review the recommended actions below for risk mitigation steps."
            )
        elif evidence.opportunity_score >= 75:
            return (
                f"Opportunity Alert: Portfolio opportunity score at {evidence.opportunity_score:.0f}/100. "
                f"Consider the recommended actions to capitalize on current market conditions."
            )
        else:
            return (
                f"Portfolio Update: Risk score {evidence.risk_alert_score:.0f}/100, "
                f"Opportunity score {evidence.opportunity_score:.0f}/100. No immediate action required."
            )

    @staticmethod
    def _format_money(value: Optional[float]) -> str:
        """Format number as currency."""
        if value is None:
            return "N/A"
        return f"${value:,.2f}"

    @staticmethod
    def _format_pct(value: Optional[float]) -> str:
        """Format number as percentage."""
        if value is None:
            return "N/A"
        return f"{value:+.2f}%"

    @staticmethod
    def _format_date(dt: Optional[datetime]) -> str:
        """Format datetime."""
        if dt is None:
            return "N/A"
        return dt.strftime("%Y-%m-%d %H:%M")

    @staticmethod
    def _urgency_color(urgency: str) -> str:
        """Get color for urgency level."""
        colors = {
            "HIGH": "#ef4444",
            "MED": "#f59e0b",
            "LOW": "#10b981",
        }
        return colors.get(urgency, "#6b7280")

    @staticmethod
    def _action_color(action: str) -> str:
        """Get color for action type."""
        colors = {
            "ADD": "#10b981",
            "TRIM": "#ef4444",
            "HOLD": "#6b7280",
            "HEDGE": "#f59e0b",
            "SET_STOP": "#f59e0b",
            "TAKE_PROFIT": "#10b981",
            "REVIEW": "#6b7280",
        }
        return colors.get(action, "#6b7280")

    @staticmethod
    def _sentiment_color(sentiment: str) -> str:
        """Get color for sentiment."""
        colors = {
            "VERY_POSITIVE": "#059669",
            "POSITIVE": "#10b981",
            "NEUTRAL": "#6b7280",
            "NEGATIVE": "#ef4444",
            "VERY_NEGATIVE": "#dc2626",
        }
        return colors.get(sentiment, "#6b7280")


def render_email(
    snapshot: PortfolioSnapshot,
    recommendations: List[ActionRecommendation],
    evidence_packet: EvidencePacket,
    llm_response: Optional[LLMResponse] = None,
    fallback_content: Optional[Dict[str, Any]] = None,
    charts: Optional[Dict[str, str]] = None,
) -> str:
    """
    Convenience function to render email.

    Args:
        snapshot: Portfolio snapshot
        recommendations: Recommendations
        evidence_packet: Evidence packet
        llm_response: Optional LLM response
        fallback_content: Fallback content dict if LLM fails
        charts: Pre-generated charts as base64 data URLs

    Returns:
        Rendered HTML string
    """
    # If LLM failed but we have fallback, convert to LLMResponse-like object
    effective_response = llm_response
    if effective_response is None and fallback_content:
        effective_response = type('FallbackResponse', (), {
            'executive_summary': fallback_content.get('executive_summary', ''),
            'top_actions': fallback_content.get('top_actions', []),
            'portfolio_notes': fallback_content.get('portfolio_notes', []),
            'citations': fallback_content.get('citations', []),
            'data_freshness': fallback_content.get('data_freshness', {}),
        })()

    renderer = EmailRenderer()
    return renderer.render(
        snapshot=snapshot,
        recommendations=recommendations,
        evidence_packet=evidence_packet,
        llm_response=effective_response,
        charts=charts,
    )


def send_email(
    html_content: str,
    subject: str,
    to_emails: List[str],
    from_email: str,
    smtp_host: str,
    smtp_port: int,
    smtp_user: str,
    smtp_password: str,
    cc_emails: Optional[List[str]] = None,
) -> bool:
    """
    Send email via SMTP.

    Args:
        html_content: Email HTML content
        subject: Email subject
        to_emails: Recipient email addresses
        from_email: Sender email address
        smtp_host: SMTP server host
        smtp_port: SMTP server port
        smtp_user: SMTP username
        smtp_password: SMTP password
        cc_emails: Optional CC recipients

    Returns:
        True if sent successfully
    """
    try:
        # Create message
        msg = MIMEMultipart('alternative')
        msg['Subject'] = subject
        msg['From'] = from_email
        msg['To'] = ", ".join(to_emails)

        if cc_emails:
            msg['Cc'] = ", ".join(cc_emails)

        # Create plain text version (fallback)
        plain_text = f"""
AI Investment Manager Alert

This email contains HTML content. Please view in an HTML-capable email client.

Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
        """

        # Attach both versions
        part1 = MIMEText(plain_text, 'plain')
        part2 = MIMEText(html_content, 'html')

        msg.attach(part1)
        msg.attach(part2)

        # Send
        all_recipients = to_emails + (cc_emails or [])

        with smtplib.SMTP(smtp_host, smtp_port) as server:
            server.starttls()
            server.login(smtp_user, smtp_password)
            server.sendmail(from_email, all_recipients, msg.as_string())

        logger.info(f"Email sent successfully to {len(all_recipients)} recipients")
        return True

    except Exception as e:
        logger.error(f"Failed to send email: {e}")
        return False
