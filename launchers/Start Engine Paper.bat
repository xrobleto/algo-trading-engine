@echo off
title Unified Engine Paper
color 1F

REM === Paths ===
set "GDRIVE_ROOT=G:\My Drive\Algo_Trading"
set "LOCAL_ROOT=%LOCALAPPDATA%\AlgoTrading"
set "SRC_DIR=%LOCAL_ROOT%\src"
set "ALGO_OUTPUT_DIR=%LOCAL_ROOT%"

REM === Sync source files from Google Drive to local disk ===
echo.
echo  =============================================
echo   Unified Engine: PAPER TRADING (Trend + Simple)
echo  =============================================
echo.
echo Syncing from Google Drive...
if not exist "%GDRIVE_ROOT%\strategies" (
    echo ERROR: Google Drive not available at %GDRIVE_ROOT%
    echo        Start Google Drive and try again.
    pause
    exit /b 1
)
robocopy "%GDRIVE_ROOT%\strategies" "%SRC_DIR%\strategies" *.py /njh /njs /ndl /nc /ns /np /s
robocopy "%GDRIVE_ROOT%\config" "%SRC_DIR%\config" *.env *.yaml /njh /njs /ndl /nc /ns /np
echo Sync complete. Running from local copy.
echo.

REM === Load paper environment variables ===
set "ENV_FILE=%SRC_DIR%\config\trend_bot.env"
if not exist "%ENV_FILE%" (
    echo ERROR: Environment file not found: %ENV_FILE%
    pause
    exit /b 1
)
for /f "usebackq eol=# tokens=1,* delims==" %%a in ("%ENV_FILE%") do (
    if not "%%a"=="" set "%%a=%%b"
)

REM === Intelligence-layer feature flags (WS3 breadth gate + WS4 chop dampener) ===
REM These modify SIMPLE/TREND allocations based on market microstructure. Default
REM off in production. Turned ON here for paper validation per the engine
REM backtest recommendations. Inspect JSONL decisions at:
REM   %LOCAL_ROOT%\logs\intelligence_decisions.jsonl
REM to verify gate_fire days and chop_dampener firing match expectations.
set "INTEL_MARKET_STRUCTURE_GATE=1"
set "INTEL_CHOP_DAMPENER=1"
echo [INTEL] WS3 breadth gate ENABLED (INTEL_MARKET_STRUCTURE_GATE=1)
echo [INTEL] WS4 chop dampener ENABLED (INTEL_CHOP_DAMPENER=1)

REM === Run engine from local copy with auto-restart ===
cd /d "%SRC_DIR%\strategies"
:loop
echo [%date% %time%] Starting Unified Engine (Paper)...
echo.
python -m engine.main
echo.
echo [%date% %time%] Engine exited. Restarting in 15 seconds...
echo   Press Ctrl+C to stop.
timeout /t 15
goto loop
