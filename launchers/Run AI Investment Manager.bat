@echo off
title AI Investment Manager

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

REM Check if venv exists, create if not
if not exist "venv\Scripts\activate.bat" (
    echo Virtual environment not found. Creating...
    python -m venv venv
    call venv\Scripts\activate.bat
    pip install -r requirements.txt
) else (
    call venv\Scripts\activate.bat
)

echo.
echo AI Investment Manager
echo =====================
echo.
echo Options:
echo   --once        Single analysis run
echo   --explain     Show current state without sending email
echo   --test-email  Test email rendering
echo   --daemon      Continuous monitoring mode
echo.
echo Running single analysis...
echo.

python -m src.main --once

echo.
echo Done!
pause
