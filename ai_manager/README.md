# AI Investment Manager

A production-grade investment monitoring system that analyzes your portfolio, enriches it with market/news/macro context, and emails actionable recommendations only when meaningful thresholds are met.

## Features

- **Portfolio Reconstruction**: Parses Robinhood CSV exports to reconstruct holdings with cost basis
- **Multi-Source Data Enrichment**: Polygon (Massive), Alpaca, FRED for market data, news, and macro indicators
- **Deterministic Scoring**: Rule-based risk and opportunity scoring (LLM only for narratives)
- **Smart Alerting**: Deduplication, rate limiting, and materiality checks prevent spam
- **Beautiful Email Reports**: HTML emails with inline charts, tables, and actionable insights
- **TradingView Integration**: Optional webhook receiver for external alerts (no scraping)

## Quick Start

### 1. Clone and Setup Environment

```bash
cd ai_investment_manager
python -m venv venv

# Windows
venv\Scripts\activate

# Linux/Mac
source venv/bin/activate

pip install -r requirements.txt
```

### 2. Configure Environment Variables

Copy `.env.example` to `.env` and fill in your API keys:

```bash
cp .env.example .env
```

Required keys:
- `POLYGON_API_KEY`: Get from https://polygon.io/
- `ALPACA_API_KEY` / `ALPACA_SECRET_KEY`: Get from https://alpaca.markets/
- `ANTHROPIC_API_KEY`: Get from https://console.anthropic.com/
- `SMTP_PASSWORD`: Your email SMTP password

Optional:
- `FRED_API_KEY`: Get from https://fred.stlouisfed.org/docs/api/api_key.html

### 3. Configure Settings

Edit `config.yaml` to set:
- Path to your Robinhood CSV export
- Email settings (SMTP server, recipients)
- Risk thresholds and scoring weights
- Provider toggles

### 4. Place Your Portfolio CSV

Export your Robinhood activity ledger and place it at the path specified in `config.yaml`.

The CSV should have these columns:
```
Activity Date,Process Date,Settle Date,Instrument,Description,Trans Code,Quantity,Price,Amount
```

### 5. Run

```bash
# Single analysis run
python -m src.main --once

# Test email rendering (no send)
python -m src.main --test-email

# Explain current state without sending
python -m src.main --explain

# Daemon mode (continuous monitoring)
python -m src.main --daemon
```

## Architecture

```
src/
├── main.py                 # CLI entrypoint
├── ingestion/
│   └── robinhood_csv.py    # CSV parsing and holdings reconstruction
├── providers/
│   ├── massive_client.py   # Polygon API client
│   ├── alpaca_client.py    # Alpaca API client
│   ├── fred_client.py      # FRED economic data
│   └── tradingview_alerts.py # Webhook receiver (FastAPI)
├── signals/
│   ├── technicals.py       # Technical indicators
│   ├── news.py             # News sentiment analysis
│   ├── macro.py            # Macro indicators
│   └── risk.py             # Portfolio risk metrics
├── engine/
│   ├── scoring.py          # Risk and opportunity scoring
│   └── recommendations.py  # Action generation
├── llm/
│   └── claude_client.py    # Anthropic API with strict JSON
├── reporting/
│   ├── email_renderer.py   # Jinja2 HTML rendering
│   └── charts.py           # Matplotlib chart generation
├── scheduling/
│   └── runner.py           # APScheduler + market hours
├── storage/
│   └── state_store.py      # SQLite deduplication
└── utils/
    ├── logging.py          # Structured logging
    ├── time.py             # Timezone utilities
    ├── retry.py            # Retry decorators
    └── typing.py           # Type definitions
```

## Scoring System

### Risk Alert Score (0-100)
Triggers when portfolio risk is elevated:
- Portfolio drawdown beyond threshold
- Single position concentration
- Sector overexposure
- Volatility spikes
- Correlation breakdown
- Key level breaks
- Adverse news catalysts

### Opportunity Score (0-100)
Triggers when actionable opportunities exist:
- Strong trend + pullback to support
- Positive catalyst with technical confirmation
- Sector rotation signals
- TradingView alert boosts
- Macro tailwinds

## Email Triggers

Emails are sent ONLY when:
1. Risk Alert Score >= `risk_score_min` (default: 70), OR
2. Opportunity Score >= `action_score_min` (default: 75), AND
3. Material change since last alert (dedupe check), AND
4. Rate limits not exceeded (`max_emails_per_day`, `min_hours_between_emails`)

## TradingView Alerts (Optional)

To receive TradingView alerts via webhook:

1. Enable in `config.yaml`:
   ```yaml
   providers:
     tradingview_alerts_enabled: true
   ```

2. Start the webhook server:
   ```bash
   python -m src.providers.tradingview_alerts
   ```

3. In TradingView, create an alert with:
   - Webhook URL: `http://your-server:8000/tv-alert`
   - Message format (JSON):
     ```json
     {
       "ticker": "{{ticker}}",
       "exchange": "{{exchange}}",
       "price": {{close}},
       "alert_name": "My Alert",
       "message": "{{strategy.order.comment}}"
     }
     ```

## Running as a Service

### Windows Task Scheduler

1. Open Task Scheduler
2. Create Basic Task
3. Set trigger (e.g., every 30 minutes during market hours)
4. Action: Start a program
   - Program: `C:\path\to\venv\Scripts\python.exe`
   - Arguments: `-m src.main --once`
   - Start in: `C:\path\to\ai_investment_manager`

### Linux systemd

Create `/etc/systemd/system/ai-investment-manager.service`:

```ini
[Unit]
Description=AI Investment Manager
After=network.target

[Service]
Type=simple
User=youruser
WorkingDirectory=/path/to/ai_investment_manager
Environment="PATH=/path/to/ai_investment_manager/venv/bin"
ExecStart=/path/to/ai_investment_manager/venv/bin/python -m src.main --daemon
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

Then:
```bash
sudo systemctl daemon-reload
sudo systemctl enable ai-investment-manager
sudo systemctl start ai-investment-manager
```

## Troubleshooting

### No emails being sent
1. Check `--explain` output to see current scores
2. Verify SMTP settings with `--test-email`
3. Check `logs/` for errors
4. Ensure scores exceed thresholds in `config.yaml`

### API rate limits
- Polygon: Free tier = 5 calls/min. Upgrade for production.
- Alpaca: Generally generous limits for authenticated users.
- FRED: 120 requests/minute.

### CSV parsing errors
- Ensure CSV is exported from Robinhood (Activity > Statements & History > Download)
- Check for special characters or encoding issues
- Review `tests/test_ingestion.py` for expected format

## Disclaimer

This software is for educational and informational purposes only. It does not constitute financial advice. Always verify recommendations independently before making investment decisions. Past performance does not guarantee future results. Investing involves risk, including possible loss of principal.

## License

MIT License - See LICENSE file
