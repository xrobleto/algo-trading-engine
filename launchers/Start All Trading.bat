@echo off
title Trading System Launcher

set "SCRIPT_DIR=%~dp0"
cd /d "%SCRIPT_DIR%.."
set "ALGO_ROOT=%CD%"
set "LAUNCHER_DIR=%ALGO_ROOT%\launchers"

REM Output dir for logs/state (local only, not synced by Google Drive)
set "ALGO_OUTPUT_DIR=%LOCALAPPDATA%\AlgoTrading"
if not exist "%ALGO_OUTPUT_DIR%" mkdir "%ALGO_OUTPUT_DIR%"

echo ============================================
echo Starting All Trading Systems
echo ============================================
echo.
echo This will open 6 windows:
echo   1. Simple Bot (intraday momentum - longs)
echo   2. Directional Bot (intraday momentum - shorts)
echo   3. Trend Bot (weekly ETF rebalancing)
echo   4. Smallcap Scanner (A+ setups)
echo   5. Buy Alerts (entry signals)
echo   6. Sell Alerts (exit signals)
echo.
echo ============================================
echo.

echo [1/6] Starting Simple Bot...
start "" "%LAUNCHER_DIR%\Start Simple Bot.bat"
timeout /t 3 /nobreak >nul

echo [2/6] Starting Directional Bot...
start "" "%LAUNCHER_DIR%\Start Directional Bot.bat"
timeout /t 3 /nobreak >nul

echo [3/6] Starting Trend Bot...
start "" "%LAUNCHER_DIR%\Start Trend Bot.bat"
timeout /t 3 /nobreak >nul

echo [4/6] Starting Smallcap Scanner...
start "" "%LAUNCHER_DIR%\Start Smallcap Scanner.bat"
timeout /t 3 /nobreak >nul

echo [5/6] Starting Buy Alerts...
start "" "%LAUNCHER_DIR%\Start Buy Alerts.bat"
timeout /t 3 /nobreak >nul

echo [6/6] Starting Sell Alerts...
start "" "%LAUNCHER_DIR%\Start Sell Alerts.bat"

echo.
echo ============================================
echo All 6 trading systems launched!
echo ============================================
echo.
echo To stop any system, press Ctrl+C in its window.
echo To stop ALL trading, create the kill switch file:
echo   echo HALT ^> %ALGO_ROOT%\data\state\HALT_TRADING
echo.
pause
