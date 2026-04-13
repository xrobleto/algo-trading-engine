# Smallcap Scanner Dashboard

Real-time web interface for the smallcap momentum scanner with user authentication.

## Quick Start

### 1. Install Dependencies

```bash
cd C:\Users\xrobl\Documents\Algo_Trading\dashboard
pip install -r requirements.txt
pip install python-dotenv
```

### 2. Set Up User Credentials

Generate password hashes for your users:

```bash
python generate_password.py yourpassword123
```

Edit `config.yaml` and add users:

```yaml
credentials:
  usernames:
    john:
      name: John Doe
      password: $2b$12$...  # paste hash here
    jane:
      name: Jane Smith
      password: $2b$12$...  # paste hash here
```

**Important:** Change the `cookie.key` in config.yaml to a random string!

### 3. Run the Dashboard

**Option A:** Use the launcher:
```
Double-click: launchers\Start Dashboard.bat
```

**Option B:** Run directly:
```bash
cd C:\Users\xrobl\Documents\Algo_Trading\dashboard
streamlit run app.py
```

The dashboard will open at: http://localhost:8501

## Features

- **User Authentication** - Login required, sessions persist for 30 days
- **Market Direction** - SPY/QQQ momentum indicator with trend status
- **Live Setups** - Top momentum stocks meeting scanner criteria
- **Auto-Refresh** - Updates every 15 seconds (configurable)
- **Session Awareness** - Shows PRE-MARKET, PRIME TIME, INTRADAY, AFTER HOURS

## Deploying for Remote Access

### Option 1: Streamlit Cloud (Easiest)

1. Push code to GitHub
2. Go to share.streamlit.io
3. Connect your repo
4. Add secrets in Streamlit Cloud settings

### Option 2: Self-Hosted VPS

1. Get a VPS ($5/month on DigitalOcean, Linode, etc.)
2. Install Python 3.10+
3. Clone your code
4. Run with: `streamlit run app.py --server.port 80 --server.address 0.0.0.0`
5. Set up nginx for HTTPS (recommended)

### Option 3: ngrok (Quick Testing)

```bash
# Install ngrok
# Run dashboard locally
streamlit run app.py

# In another terminal:
ngrok http 8501
```

This gives you a public URL to share.

## Security Notes

- Change the default cookie key in config.yaml
- Use strong passwords
- For production, use HTTPS
- Consider IP whitelisting for extra security

## Customization

Edit `app.py` to:
- Change refresh interval (default: 15s)
- Modify scanner criteria display
- Add additional data views
- Customize styling with Streamlit themes
