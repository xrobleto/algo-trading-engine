@echo off
title Smallcap Scanner Dashboard

set "SCRIPT_DIR=%~dp0"
cd /d "%SCRIPT_DIR%.."
set "ALGO_ROOT=%CD%"

REM Output dir for logs/state (local only, not synced by Google Drive)
set "ALGO_OUTPUT_DIR=%LOCALAPPDATA%\AlgoTrading"
if not exist "%ALGO_OUTPUT_DIR%" mkdir "%ALGO_OUTPUT_DIR%"

echo Loading environment variables...
set "ENV_FILE=%ALGO_ROOT%\config\smallcap_scanner.env"
for /f "usebackq eol=# tokens=1,* delims==" %%a in ("%ENV_FILE%") do (
    if not "%%a"=="" set "%%a=%%b"
)

cd "%ALGO_ROOT%\dashboard"
echo.
echo ============================================
echo Smallcap Scanner Dashboard
echo ============================================
echo.
echo Starting Streamlit server...
echo Dashboard will open in your browser automatically.
echo.
echo Press Ctrl+C to stop the server.
echo ============================================
echo.

python -m streamlit run app.py --server.port 8501 --server.address localhost

pause
