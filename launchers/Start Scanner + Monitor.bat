@echo off
title Scanner + Monitor Launcher

set "SCRIPT_DIR=%~dp0"
cd /d "%SCRIPT_DIR%.."
set "ALGO_ROOT=%CD%"
set "LAUNCHER_DIR=%ALGO_ROOT%\launchers"

REM Output dir for logs/state (local only, not synced by Google Drive)
set "ALGO_OUTPUT_DIR=%LOCALAPPDATA%\AlgoTrading"
if not exist "%ALGO_OUTPUT_DIR%" mkdir "%ALGO_OUTPUT_DIR%"

echo ============================================
echo Starting Small Cap Scanner + Position Monitor
echo ============================================
echo.
echo This will open two windows:
echo   1. Small Cap Scanner (finds setups)
echo   2. Position Monitor (manages open trades)
echo.
echo Press Ctrl+C in either window to stop it.
echo ============================================
echo.

REM Start the scanner in a new window
start "Small Cap Scanner" cmd /k "%LAUNCHER_DIR%\helper_scanner.bat"

REM Wait a moment for the first window to open
timeout /t 2 /nobreak >nul

REM Start the position monitor in a new window
start "Position Monitor" cmd /k "%LAUNCHER_DIR%\helper_monitor.bat"

echo.
echo Both windows launched!
echo.
echo Workflow:
echo   1. Scanner finds A+ setup and plays sound
echo   2. Review the setup details in scanner window
echo   3. Run "Execute Trade.bat" to enter position
echo   4. Monitor window tracks your open positions
echo.
pause
