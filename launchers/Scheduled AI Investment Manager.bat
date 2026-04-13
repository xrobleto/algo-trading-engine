@echo off
REM AI Investment Manager - Scheduled Task Runner
REM Runs weekly on Fridays at 6:00 AM CT

set "SCRIPT_DIR=%~dp0"
cd /d "%SCRIPT_DIR%.."
set "ALGO_ROOT=%CD%"

REM Output dir for logs/state (local only, not synced by Google Drive)
set "ALGO_OUTPUT_DIR=%LOCALAPPDATA%\AlgoTrading"
if not exist "%ALGO_OUTPUT_DIR%" mkdir "%ALGO_OUTPUT_DIR%"

cd /d "%ALGO_ROOT%\ai_manager"

REM Load environment variables from ai_manager's .env
set "ENV_FILE=.env"
if exist "%ENV_FILE%" (
    for /f "usebackq eol=# tokens=1,* delims==" %%a in ("%ENV_FILE%") do (
        if not "%%a"=="" set "%%a=%%b"
    )
)

REM Activate virtual environment and run
call venv\Scripts\activate.bat
python -m src.main --send-email

REM Deactivate when done
deactivate
