@echo off
title Daily Metrics Tracker

set "SCRIPT_DIR=%~dp0"
cd /d "%SCRIPT_DIR%.."
set "ALGO_ROOT=%CD%"

REM Output dir for logs/state (local only, not synced by Google Drive)
set "ALGO_OUTPUT_DIR=%LOCALAPPDATA%\AlgoTrading"
if not exist "%ALGO_OUTPUT_DIR%" mkdir "%ALGO_OUTPUT_DIR%"

echo Loading environment variables...
set "ENV_FILE=%ALGO_ROOT%\config\momentum_bot.env"
if not exist "%ENV_FILE%" (
    echo ERROR: Environment file not found: %ENV_FILE%
    pause
    exit /b 1
)
for /f "usebackq eol=# tokens=1,* delims==" %%a in ("%ENV_FILE%") do (
    if not "%%a"=="" set "%%a=%%b"
)

cd /d "%ALGO_ROOT%\utilities"

echo ============================================
echo Daily Metrics Tracker
echo ============================================
echo.
echo Options:
echo   1. Today's metrics (default)
echo   2. Weekly summary
echo   3. Specific date
echo   4. Export to CSV
echo.

set /p choice="Enter choice (1-4) or press Enter for today: "

if "%choice%"=="" (
    python daily_metrics.py
) else if "%choice%"=="1" (
    python daily_metrics.py
) else if "%choice%"=="2" (
    python daily_metrics.py --week
) else if "%choice%"=="3" (
    set /p date="Enter date (YYYY-MM-DD): "
    python daily_metrics.py --date %date%
) else if "%choice%"=="4" (
    python daily_metrics.py --week --export csv
) else (
    echo Invalid choice
)

echo.
pause
