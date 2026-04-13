@echo off
title Polymarket Sentiment Monitor

set "SCRIPT_DIR=%~dp0"
cd /d "%SCRIPT_DIR%.."
set "ALGO_ROOT=%CD%"

REM Output dir for logs/state (local only, not synced by Google Drive)
set "ALGO_OUTPUT_DIR=%LOCALAPPDATA%\AlgoTrading"
if not exist "%ALGO_OUTPUT_DIR%" mkdir "%ALGO_OUTPUT_DIR%"

REM Load environment variables (Polymarket monitor may need API keys)
set "ENV_FILE=%ALGO_ROOT%\config\alerts.env"
if exist "%ENV_FILE%" (
    for /f "usebackq eol=# tokens=1,* delims==" %%a in ("%ENV_FILE%") do (
        if not "%%a"=="" set "%%a=%%b"
    )
)

cd /d "%ALGO_ROOT%\utilities"
echo ============================================
echo Polymarket Prediction Market Monitor
echo ============================================
echo.
echo Monitoring key prediction markets:
echo   - Fed rate decisions
echo   - Recession probability
echo   - Market direction signals
echo   - Geopolitical events
echo.
echo Press Ctrl+C to stop.
echo ============================================
echo.
python polymarket_monitor.py
pause
