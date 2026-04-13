@echo off
title Trend Bot - Rebalance (LIVE)
color 4F

set "SCRIPT_DIR=%~dp0"
cd /d "%SCRIPT_DIR%.."
set "ALGO_ROOT=%CD%"

REM Output dir for logs/state (local only, not synced by Google Drive)
set "ALGO_OUTPUT_DIR=%LOCALAPPDATA%\AlgoTrading"
if not exist "%ALGO_OUTPUT_DIR%" mkdir "%ALGO_OUTPUT_DIR%"

echo Loading LIVE environment variables...
set "ENV_FILE=%ALGO_ROOT%\config\trend_bot_live.env"
if not exist "%ENV_FILE%" (
    echo ERROR: Environment file not found: %ENV_FILE%
    pause
    exit /b 1
)
for /f "usebackq eol=# tokens=1,* delims==" %%a in ("%ENV_FILE%") do (
    if not "%%a"=="" set "%%a=%%b"
)

cd /d "%ALGO_ROOT%\strategies"
echo.
echo ============================================
echo   *** LIVE TRADING - REAL MONEY ***
echo   Trend Bot - Manual Rebalance
echo ============================================
echo.
python trend_bot.py --rebalance
echo.
pause
