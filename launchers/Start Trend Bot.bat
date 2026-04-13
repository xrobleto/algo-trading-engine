@echo off
title Trend Bot

REM Get the directory where this script is located (launchers folder)
set "SCRIPT_DIR=%~dp0"
REM Go up one level to Algo_Trading root
cd /d "%SCRIPT_DIR%.."
set "ALGO_ROOT=%CD%"

REM Output dir for logs/state (local only, not synced by Google Drive)
set "ALGO_OUTPUT_DIR=%LOCALAPPDATA%\AlgoTrading"
if not exist "%ALGO_OUTPUT_DIR%" mkdir "%ALGO_OUTPUT_DIR%"

echo Loading environment variables...
set "ENV_FILE=%ALGO_ROOT%\config\trend_bot.env"
if not exist "%ENV_FILE%" (
    echo ERROR: Environment file not found: %ENV_FILE%
    pause
    exit /b 1
)
for /f "usebackq eol=# tokens=1,* delims==" %%a in ("%ENV_FILE%") do (
    if not "%%a"=="" set "%%a=%%b"
)

cd /d "%ALGO_ROOT%\strategies"
echo Starting Trend Bot...
echo.
echo Commands:
echo   --status    Show current positions and state
echo   --rebalance Force immediate rebalance (market must be open)
echo.
python trend_bot.py
pause
