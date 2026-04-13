@echo off
title Pre-Market Executor

set "SCRIPT_DIR=%~dp0"
cd /d "%SCRIPT_DIR%.."
set "ALGO_ROOT=%CD%"

REM Output dir for logs/state (local only, not synced by Google Drive)
set "ALGO_OUTPUT_DIR=%LOCALAPPDATA%\AlgoTrading"
if not exist "%ALGO_OUTPUT_DIR%" mkdir "%ALGO_OUTPUT_DIR%"

echo Loading environment variables...
set "ENV_FILE=%ALGO_ROOT%\config\smallcap_scanner.env"
if not exist "%ENV_FILE%" (
    echo ERROR: Environment file not found: %ENV_FILE%
    pause
    exit /b 1
)
for /f "usebackq eol=# tokens=1,* delims==" %%a in ("%ENV_FILE%") do (
    if not "%%a"=="" set "%%a=%%b"
)

cd /d "%ALGO_ROOT%\scanners"

:top
cls
echo ============================================================
echo          SMALL CAP PRE-MARKET EXECUTOR
echo ============================================================
echo.
echo  Pre-Market Hours: 4:00 AM - 9:30 AM ET
echo.
echo  KEY DIFFERENCES FROM REGULAR HOURS:
echo   - Wider spreads accepted (up to 1.5%% vs 0.8%%)
echo   - Lower volume thresholds ($25K vs $100K)
echo   - Limit orders ONLY (no market orders)
echo   - More conservative sizing (35%% reduced)
echo   - Focus on A+ setups with catalysts
echo.
echo  EXIT STRATEGY (Scale-Out):
echo   - TP1: 50%% at 0.75R (quick profit)
echo   - TP2: 50%% at 1.5R
echo   - Auto-close by 9:25 AM if CLOSE_BEFORE_OPEN
echo.
echo ============================================================
echo.
echo  Options:
echo    [ticker]  - Validate and execute a trade
echo    m         - Monitor existing positions
echo    q         - Quit
echo.
echo ============================================================
echo.

set /p INPUT="Enter ticker or option: "

if /i "%INPUT%"=="q" goto :end
if "%INPUT%"=="" goto :end
if /i "%INPUT%"=="m" goto :monitor

echo.
echo Validating %INPUT% for pre-market entry...
echo.
python smallcap_premarket_executor.py %INPUT%

echo.
echo ============================================
set /p ANOTHER="Execute another trade? (y/n): "
if /i "%ANOTHER%"=="y" goto :top
goto :end

:monitor
echo.
echo Starting position monitor...
echo.
python smallcap_premarket_executor.py --monitor
goto :top

:end
echo.
echo Done!
pause
