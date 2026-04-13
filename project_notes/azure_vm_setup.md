# Azure VM Setup Guide for Trading Bots

> **Purpose**: Step-by-step guide to deploy trading bots on Azure VM for production use.
> **Target**: simple_bot and trend_bot running 24/7 with remote access from anywhere.

---

## Overview

```
Your Setup:
┌─────────────────────────────────────────────────────────────────┐
│                        Azure (East US)                          │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │  Windows Server 2022 VM (B2s: 2 vCPU, 4GB RAM)            │  │
│  │                                                           │  │
│  │  C:\Algo_Trading\                                         │  │
│  │  ├── strategies\simple_bot.py  ──► Alpaca API            │  │
│  │  ├── strategies\trend_bot.py   ──► Polygon API           │  │
│  │  └── (full directory structure)                           │  │
│  │                                                           │  │
│  │  Task Scheduler: Auto-start bots on boot                  │  │
│  └───────────────────────────────────────────────────────────┘  │
│                           │                                      │
│                           │ RDP (Port 3389)                      │
└───────────────────────────┼──────────────────────────────────────┘
                            │
                            ▼
              ┌─────────────────────────┐
              │  You (anywhere)         │
              │  - Laptop               │
              │  - Phone (RDP app)      │
              │  - VS Code Remote       │
              └─────────────────────────┘
```

---

## Cost Estimate

| Component | Spec | Monthly Cost |
|-----------|------|--------------|
| VM (B2s) | 2 vCPU, 4GB RAM | ~$35 |
| OS Disk | 128GB Premium SSD | ~$20 |
| Outbound Data | ~10GB/month | ~$1 |
| **Total** | | **~$56/month** |

**Cost Savings:**
- Reserved Instance (1-year): Save 30% → ~$40/month
- Reserved Instance (3-year): Save 50% → ~$28/month
- Use B1ms (1 vCPU, 2GB) if bots are light: ~$30/month

---

## Phase 1: Create the VM

### Step 1: Azure Portal Setup

1. Go to [portal.azure.com](https://portal.azure.com)
2. Sign in (create account if needed)
3. Click **"Create a resource"** → **"Virtual Machine"**

### Step 2: Basic Configuration

| Setting | Value |
|---------|-------|
| **Subscription** | Your subscription |
| **Resource group** | Create new: `trading-bots-rg` |
| **Virtual machine name** | `trading-vm` |
| **Region** | `East US` or `East US 2` (closest to NYSE/NASDAQ) |
| **Availability options** | No infrastructure redundancy required |
| **Security type** | Standard |
| **Image** | Windows Server 2022 Datacenter: Azure Edition - x64 Gen2 |
| **Size** | B2s (2 vCPU, 4GB RAM) - click "See all sizes" |

### Step 3: Administrator Account

| Setting | Value |
|---------|-------|
| **Username** | `tradingadmin` (or your preference) |
| **Password** | Strong password (save in password manager!) |

### Step 4: Inbound Ports

- [x] Allow selected ports
- [x] RDP (3389)

### Step 5: Disks

| Setting | Value |
|---------|-------|
| **OS disk type** | Premium SSD (for reliability) |
| **Size** | 128 GB (default is fine) |
| **Delete with VM** | Yes |

### Step 6: Networking

- Keep defaults (creates new virtual network)
- Public IP: Create new (needed for RDP access)

### Step 7: Review + Create

- Click **"Review + create"**
- Review settings, click **"Create"**
- Wait 2-5 minutes for deployment

---

## Phase 2: Secure the VM

### Step 1: Restrict RDP Access to Your IP

**Critical for security!** Only allow RDP from your IP address.

1. Go to VM → **Networking** → **Network security group**
2. Click on the **RDP** rule
3. Change **Source** from "Any" to "IP Addresses"
4. Enter your public IP (find at [whatismyip.com](https://whatismyip.com))
5. Click **Save**

**Note:** If your home IP changes, you'll need to update this rule.

### Step 2: Enable Auto-Shutdown (Optional Cost Savings)

Skip this for trading bots - they need to run 24/7!

But useful for development/test VMs:
- VM → **Auto-shutdown** → Enable → Set time

### Step 3: Enable Azure Defender (Recommended)

- VM → **Security** → Enable Microsoft Defender for Cloud
- Free tier provides basic protection

---

## Phase 3: Connect and Configure

### Step 1: Connect via RDP

1. Go to VM → **Connect** → **RDP**
2. Click **"Download RDP file"**
3. Open the file, enter username/password
4. Accept certificate warning

### Step 2: Initial Windows Setup

Once connected to the VM desktop:

```powershell
# Open PowerShell as Administrator

# 1. Set timezone to Eastern (for market hours)
Set-TimeZone -Id "Eastern Standard Time"

# 2. Verify timezone
Get-Date

# 3. Enable Windows Defender (should be on by default)
Get-MpComputerStatus
```

### Step 3: Install Python 3.11

1. Open Edge browser on the VM
2. Go to [python.org/downloads](https://www.python.org/downloads/)
3. Download Python 3.11.x (64-bit)
4. Run installer:
   - [x] Add Python to PATH
   - [x] Install for all users
   - Click "Install Now"

5. Verify installation:
```powershell
python --version
pip --version
```

### Step 4: Install Git (for easy code updates)

1. Download from [git-scm.com](https://git-scm.com/download/win)
2. Install with defaults
3. Verify: `git --version`

---

## Phase 4: Deploy Trading Bots

### Option A: Copy Files via RDP (Simple)

1. On your local machine, zip the `Algo_Trading` folder
2. In RDP session, open Edge, upload to OneDrive/Google Drive
3. Download and extract to `C:\Algo_Trading`

Or use **Copy/Paste**:
- Local: Copy files
- RDP: Paste (can be slow for large folders)

### Option B: Git Clone (Recommended for ongoing updates)

If you have a private Git repo:

```powershell
cd C:\
git clone https://github.com/yourusername/Algo_Trading.git
```

### Step 5: Install Python Dependencies

```powershell
cd C:\Algo_Trading
pip install -r requirements.txt
```

### Step 6: Configure API Keys

**Option A: Environment Variables (Recommended)**

```powershell
# Set permanently for the system
[System.Environment]::SetEnvironmentVariable('ALPACA_API_KEY', 'your-key', 'Machine')
[System.Environment]::SetEnvironmentVariable('ALPACA_SECRET_KEY', 'your-secret', 'Machine')
[System.Environment]::SetEnvironmentVariable('POLYGON_API_KEY', 'your-key', 'Machine')

# Restart PowerShell to pick up changes
```

**Option B: Config Files**

- Update `config/` files with API keys
- Ensure config files are NOT in Git (add to .gitignore)

### Step 7: Test the Bots

```powershell
# Test simple_bot (Ctrl+C to stop after verifying it starts)
cd C:\Algo_Trading\strategies
python simple_bot.py

# Test trend_bot
python trend_bot.py --status
```

---

## Phase 5: Auto-Start on Boot

### Create Scheduled Tasks

The bots should start automatically when the VM boots (after Windows updates, etc.)

**Method 1: Task Scheduler GUI**

1. Open **Task Scheduler** (search in Start menu)
2. Click **"Create Task"** (not Basic Task)

**For simple_bot:**

| Tab | Setting | Value |
|-----|---------|-------|
| **General** | Name | `Simple Bot` |
| | Run whether user is logged on or not | Yes |
| | Run with highest privileges | Yes |
| **Triggers** | Begin the task | At startup |
| | Delay task for | 1 minute |
| **Actions** | Action | Start a program |
| | Program | `C:\Algo_Trading\launchers\Start Simple Bot.bat` |
| | Start in | `C:\Algo_Trading\launchers` |
| **Settings** | If task fails, restart every | 5 minutes |
| | Attempt to restart up to | 3 times |
| | Stop task if runs longer than | Disabled |

**Repeat for trend_bot** with appropriate paths.

**Method 2: PowerShell Script**

```powershell
# Create scheduled task for Simple Bot
$action = New-ScheduledTaskAction -Execute "C:\Algo_Trading\launchers\Start Simple Bot.bat" -WorkingDirectory "C:\Algo_Trading\launchers"
$trigger = New-ScheduledTaskTrigger -AtStartup
$settings = New-ScheduledTaskSettingsSet -RestartCount 3 -RestartInterval (New-TimeSpan -Minutes 5)
Register-ScheduledTask -TaskName "Simple Bot" -Action $action -Trigger $trigger -Settings $settings -User "SYSTEM" -RunLevel Highest

# Create scheduled task for Trend Bot
$action = New-ScheduledTaskAction -Execute "C:\Algo_Trading\launchers\Start Trend Bot.bat" -WorkingDirectory "C:\Algo_Trading\launchers"
Register-ScheduledTask -TaskName "Trend Bot" -Action $action -Trigger $trigger -Settings $settings -User "SYSTEM" -RunLevel Highest
```

### Verify Auto-Start

1. Reboot the VM: `Restart-Computer`
2. Wait 2 minutes, then RDP back in
3. Check Task Manager → Details for `python.exe` processes
4. Check logs in `C:\Algo_Trading\logs\`

---

## Phase 6: Monitoring & Alerts

### Azure Alerts (VM Health)

1. VM → **Alerts** → **Create alert rule**
2. Create alerts for:
   - CPU > 80% for 5 minutes
   - Available Memory < 500MB
   - VM stopped/deallocated

### Your Bot Alerts (Already Configured)

Your bots already send Slack/email alerts for:
- Trade executions
- Errors and exceptions
- Circuit breaker triggers
- WebSocket disconnections

### Check Bot Status Remotely

Quick status check via RDP:
```powershell
# See running Python processes
Get-Process python* | Select-Object ProcessName, Id, StartTime

# Check recent logs
Get-Content C:\Algo_Trading\logs\simple_bot.log -Tail 50

# Check if bots are running
tasklist | findstr python
```

---

## Phase 7: Maintenance

### Windows Updates

Configure Windows Update for minimal disruption:

1. Settings → Windows Update → Advanced options
2. Set **Active hours**: 6:30 AM - 9:30 PM ET (covers pre-market + RTH)
3. Updates will install outside market hours

### Backup Strategy

**Daily Backups (Automated):**
1. VM → **Backup** → Enable backup
2. Create Recovery Services vault
3. Set daily backup at 8 PM ET (after market close)
4. Retain for 7 days

**Manual Snapshot Before Major Changes:**
```powershell
# Take snapshot before updating bot code
# Azure Portal → VM → Disks → OS Disk → Create snapshot
```

### Log Rotation

Your bots already have log rotation configured. Verify:
- `MAX_LOG_SIZE_MB = 50`
- `MAX_LOG_BACKUPS = 5`

---

## Troubleshooting

### Can't Connect via RDP

1. Check VM is running (Azure Portal → VM → Status)
2. Check your IP hasn't changed (update NSG rule)
3. Try Azure Serial Console (VM → Serial console)

### Bots Not Starting

```powershell
# Check scheduled tasks
Get-ScheduledTask -TaskName "Simple Bot" | Get-ScheduledTaskInfo

# Check Task Scheduler logs
Get-WinEvent -LogName "Microsoft-Windows-TaskScheduler/Operational" -MaxEvents 20

# Run bot manually to see errors
cd C:\Algo_Trading\strategies
python simple_bot.py
```

### Bot Crashed

1. Check logs: `C:\Algo_Trading\logs\`
2. Check Windows Event Viewer for Python crashes
3. Restart via Task Scheduler: Right-click task → Run

### High CPU/Memory

```powershell
# See what's using resources
Get-Process | Sort-Object CPU -Descending | Select-Object -First 10

# If bot is stuck, kill and restart
Stop-Process -Name python -Force
# Task Scheduler will restart automatically
```

---

## Quick Reference

### RDP Connection
- **Address**: (from Azure Portal → VM → Connect)
- **Username**: `tradingadmin`
- **Password**: (your password)

### Important Paths
```
C:\Algo_Trading\
├── strategies\simple_bot.py
├── strategies\trend_bot.py
├── config\                      # Configuration files
├── data\state\                  # Bot state files
├── logs\                        # Log files
└── launchers\                   # Batch files
```

### Useful Commands
```powershell
# Check bot status
tasklist | findstr python

# View recent logs
Get-Content C:\Algo_Trading\logs\simple_bot.log -Tail 100

# Restart a bot
Stop-Process -Name python -Force
# (Task Scheduler will restart)

# Check scheduled tasks
Get-ScheduledTask | Where-Object {$_.TaskName -like "*Bot*"}
```

### Emergency: Kill All Bots
```powershell
# Create kill switch file
"HALT" | Out-File C:\Algo_Trading\data\HALT_TRADING

# Or force kill Python
Stop-Process -Name python -Force
```

---

## Checklist

### Initial Setup
- [x] VM created in East US region (2026-01-28: B2s, Windows Server 2022, 128GB disk)
- [x] RDP access restricted to your IP
- [x] Windows timezone set to Eastern
- [x] Python 3.14 installed (newer than guide - works fine)
- [x] Git installed
- [x] Algo_Trading folder deployed (git clone from GitHub)
- [x] Dependencies installed (pip install -r requirements.txt + numpy pandas alpaca-py polygon-api-client websocket-client pytz)
- [x] API keys configured (.env files copied from local machine)
- [x] SSL certificate fix applied (SSL_CERT_FILE system env var pointing to certifi cacert.pem)

### Auto-Start
- [x] Simple Bot scheduled task created (9:25 AM ET daily)
- [x] Trend Bot scheduled task created (9:25 AM ET daily)
- [x] Azure VM auto-start configured (9:15 AM ET daily via Automation Tasks)
- [x] Azure VM auto-shutdown configured (5:30 PM ET)
- [ ] Tested: reboot VM and verify bots start automatically (scheduled for next trading day)

### Monitoring
- [ ] Azure alerts configured (CPU, memory, VM status)
- [ ] Bot Slack/email alerts working
- [ ] Backup enabled

### Security
- [x] Strong admin password
- [x] RDP restricted to your IP
- [x] API keys not in Git

---

## Cost Optimization Tips

1. **Reserved Instance**: Commit to 1-year for 30% savings
2. **Right-size**: Start with B2s, downgrade to B1ms if CPU stays low
3. **Spot Instances**: NOT recommended for trading (can be evicted)
4. **Auto-shutdown**: Only for dev/test VMs, not production trading

---

## Session Log

### 2026-01-28: Initial VM Setup Complete

**What was done:**
- Created Azure VM via Cloud Shell (`az vm create` with B2s, Windows Server 2022, East US)
- Registered Azure resource providers (Microsoft.Network, Microsoft.Compute, Microsoft.Storage)
- Installed Python 3.14, Git on the VM
- Cloned Algo_Trading repo from GitHub
- Installed dependencies (had to add: numpy, pandas, alpaca-py, polygon-api-client, websocket-client, pytz)
- Fixed SSL certificate issue (Windows Server missing root certs):
  - Set `SSL_CERT_FILE` system environment variable to certifi's cacert.pem
  - Removed broken pip-system-certs and python-certifi-win32 packages
- Copied .env files from local machine for API keys
- Created Windows Task Scheduler tasks for simple_bot and trend_bot (9:25 AM ET daily)
- Configured Azure VM auto-start at 9:15 AM ET daily (via Automation Tasks)
- Configured Azure VM auto-shutdown at 5:30 PM ET
- Tested both bots manually - both connect to Alpaca successfully

**Issues encountered:**
- `MissingSubscriptionRegistration` error - fixed by registering Azure resource providers
- `ModuleNotFoundError` for numpy, websocket, pytz - installed missing packages
- SSL CERTIFICATE_VERIFY_FAILED - fixed with certifi + SSL_CERT_FILE env var
- pip-system-certs broke Python - manually deleted package folder

**Next steps:**
- Verify auto-start works on next trading day (VM starts 9:15 AM, bots start 9:25 AM)
- Configure Azure monitoring alerts (optional)
- Enable Azure backup (optional)

---

## Next Steps After Setup

1. Run bots in paper mode for 1 week on VM
2. Monitor performance and stability
3. Compare results to local machine
4. Once stable, switch to live trading
5. Set up VS Code Remote for easier development

