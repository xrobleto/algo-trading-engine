@echo off
title Run All Alerts

set "SCRIPT_DIR=%~dp0"
cd /d "%SCRIPT_DIR%.."
set "ALGO_ROOT=%CD%"

REM Output dir for logs/state (local only, not synced by Google Drive)
set "ALGO_OUTPUT_DIR=%LOCALAPPDATA%\AlgoTrading"
if not exist "%ALGO_OUTPUT_DIR%" mkdir "%ALGO_OUTPUT_DIR%"

echo Loading environment variables...
set "ENV_FILE=%ALGO_ROOT%\config\alerts.env"
if not exist "%ENV_FILE%" (
    echo ERROR: Environment file not found: %ENV_FILE%
    pause
    exit /b 1
)
for /f "usebackq eol=# tokens=1,* delims==" %%a in ("%ENV_FILE%") do (
    if not "%%a"=="" set "%%a=%%b"
)

echo ============================================
echo Running All Alerts (Buy, Sell, Newsletter)
echo ============================================
echo.

cd /d "%ALGO_ROOT%\alerts"

echo [1/3] Running Buy Alerts...
python buy_alerts.py --config "..\config\alerts_config.yaml"
echo.

echo [2/3] Running Sell Alerts...
python sell_alerts.py --config "..\config\alerts_config.yaml"
echo.

echo [3/3] Running Swing Newsletter...
python swing_newsletter.py
echo.

echo ============================================
echo All alerts complete!
echo ============================================
pause
