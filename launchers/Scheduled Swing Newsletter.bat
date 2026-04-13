@echo off
REM Scheduled Swing Newsletter - runs unattended at 8:30 AM ET on trading days
REM This version has no pause so Task Scheduler can run it automatically

set "SCRIPT_DIR=%~dp0"
cd /d "%SCRIPT_DIR%.."
set "ALGO_ROOT=%CD%"

REM Output dir for logs/state (local only, not synced by Google Drive)
set "ALGO_OUTPUT_DIR=%LOCALAPPDATA%\AlgoTrading"
if not exist "%ALGO_OUTPUT_DIR%" mkdir "%ALGO_OUTPUT_DIR%"
if not exist "%ALGO_OUTPUT_DIR%\logs" mkdir "%ALGO_OUTPUT_DIR%\logs"

REM Load environment variables
set "ENV_FILE=%ALGO_ROOT%\config\alerts.env"
for /f "usebackq eol=# tokens=1,* delims==" %%a in ("%ENV_FILE%") do (
    if not "%%a"=="" set "%%a=%%b"
)

cd /d "%ALGO_ROOT%\alerts"

REM Log start time
echo [%date% %time%] Starting Swing Newsletter >> "%ALGO_OUTPUT_DIR%\logs\newsletter_schedule.log"

REM Run the newsletter
python swing_newsletter.py

REM Log completion
echo [%date% %time%] Newsletter completed >> "%ALGO_OUTPUT_DIR%\logs\newsletter_schedule.log"
