@echo off
title Polymarket Sentiment Check

set "SCRIPT_DIR=%~dp0"
cd /d "%SCRIPT_DIR%.."
set "ALGO_ROOT=%CD%"

REM Output dir for logs/state (local only, not synced by Google Drive)
set "ALGO_OUTPUT_DIR=%LOCALAPPDATA%\AlgoTrading"
if not exist "%ALGO_OUTPUT_DIR%" mkdir "%ALGO_OUTPUT_DIR%"

REM Load environment variables (may need API keys for some features)
set "ENV_FILE=%ALGO_ROOT%\config\alerts.env"
if exist "%ENV_FILE%" (
    for /f "usebackq eol=# tokens=1,* delims==" %%a in ("%ENV_FILE%") do (
        if not "%%a"=="" set "%%a=%%b"
    )
)

cd /d "%ALGO_ROOT%\utilities"
echo ============================================
echo Polymarket Sentiment - Quick Check
echo ============================================
echo.
python polymarket_monitor.py --once
echo.
pause
