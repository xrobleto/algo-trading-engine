@echo off
title Execute Trade

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
echo ============================================
echo Small Cap Trade Executor
echo ============================================
echo.
echo Enter a ticker from the scanner to execute
echo a trade with automated exits:
echo   - Stop: ATR-based or structure
echo   - TP1: 33%% at 1.0R
echo   - TP2: 33%% at 2.5R
echo   - Trail: 34%% with 2%% trail after TP2
echo.
echo ============================================
echo.

set /p TICKER="Enter ticker (or 'q' to quit): "

if /i "%TICKER%"=="q" goto :end
if "%TICKER%"=="" goto :end

echo.
echo Executing %TICKER%...
echo.
python smallcap_executor.py %TICKER%

echo.
echo ============================================
set /p ANOTHER="Execute another trade? (y/n): "
if /i "%ANOTHER%"=="y" goto :top

:end
echo.
echo Done!
pause
