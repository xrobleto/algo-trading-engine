"""Pytest configuration and fixtures."""

import pytest
import sys
from pathlib import Path

# Add project root to path for imports
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


@pytest.fixture(scope="session")
def project_root_path():
    """Return the project root path."""
    return project_root


@pytest.fixture(scope="session")
def sample_csv_path(project_root_path, tmp_path_factory):
    """Create a sample CSV file for testing."""
    csv_content = '''Activity Date,Process Date,Settle Date,Instrument,Description,Trans Code,Quantity,Price,Amount
01/15/2024,01/15/2024,01/17/2024,AAPL,APPLE INC,Buy,10,$150.00,"($1,500.00)"
01/20/2024,01/20/2024,01/22/2024,AAPL,APPLE INC,Buy,5,$155.00,"($775.00)"
02/01/2024,02/01/2024,02/01/2024,AAPL,APPLE INC,CDIV,,$0.24,$2.40
01/10/2024,01/10/2024,01/12/2024,MSFT,MICROSOFT CORP,Buy,20,$350.00,"($7,000.00)"
01/25/2024,01/25/2024,01/27/2024,MSFT,MICROSOFT CORP,Sell,5,$360.00,$1800.00
03/01/2024,03/01/2024,03/01/2024,JNJ,JOHNSON & JOHNSON,Buy,15,$160.00,"($2,400.00)"
'''
    tmp_dir = tmp_path_factory.mktemp("data")
    csv_path = tmp_dir / "test_activity.csv"
    csv_path.write_text(csv_content)
    return csv_path


@pytest.fixture
def mock_api_responses():
    """Provide mock API responses for testing without network calls."""
    return {
        "polygon_snapshot": {
            "ticker": {
                "ticker": "AAPL",
                "todaysChange": 2.50,
                "todaysChangePerc": 1.42,
                "day": {
                    "o": 175.00,
                    "h": 178.00,
                    "l": 174.50,
                    "c": 177.50,
                    "v": 50000000,
                },
                "prevDay": {
                    "c": 175.00,
                },
            }
        },
        "alpaca_snapshot": {
            "AAPL": {
                "latestTrade": {"p": 177.50},
                "dailyBar": {
                    "o": 175.00,
                    "h": 178.00,
                    "l": 174.50,
                    "c": 177.50,
                    "v": 50000000,
                },
            }
        },
        "fred_series": {
            "observations": [
                {"date": "2024-01-15", "value": "4.25"},
            ]
        },
    }


@pytest.fixture
def mock_config():
    """Provide mock configuration for testing."""
    return {
        "paths": {
            "portfolio_csv_path": "test_data/activity.csv",
            "output_dir": "test_output",
        },
        "email": {
            "enabled": False,
            "smtp_host": "localhost",
            "smtp_port": 587,
            "from_address": "test@example.com",
            "to_addresses": ["recipient@example.com"],
        },
        "thresholds": {
            "risk_alert_score": 65,
            "opportunity_score": 75,
            "material_score_delta": 10,
            "max_emails_per_day": 3,
            "min_hours_between_emails": 4,
        },
        "portfolio_rules": {
            "max_single_position_pct": 20,
            "max_sector_pct": 40,
            "drawdown_alert_pct": -8,
            "min_holding_value": 100,
        },
        "providers": {
            "polygon": {"enabled": False},
            "alpaca": {"enabled": False},
            "fred": {"enabled": False},
        },
        "signals": {
            "weights": {
                "concentration": 25,
                "technical_breakdown": 25,
                "news_sentiment": 20,
                "volatility": 15,
                "macro": 15,
            }
        },
        "llm": {
            "enabled": False,
            "model": "claude-sonnet-4-20250514",
            "temperature": 0.2,
            "max_tokens": 2000,
        },
        "schedule": {
            "mode": "interval",
            "interval_minutes_market_hours": 30,
            "interval_minutes_off_hours": 240,
        },
    }
