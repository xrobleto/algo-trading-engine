@echo off
set "SCRIPT_DIR=%~dp0"
cd /d "%SCRIPT_DIR%.."
set "ALGO_ROOT=%CD%"
set "ALGO_OUTPUT_DIR=%LOCALAPPDATA%\AlgoTrading"
if not exist "%ALGO_OUTPUT_DIR%" mkdir "%ALGO_OUTPUT_DIR%"
for /f "usebackq eol=# tokens=1,* delims==" %%a in ("%ALGO_ROOT%\config\smallcap_scanner.env") do (
    if not "%%a"=="" set "%%a=%%b"
)
cd "%ALGO_ROOT%\scanners"
python smallcap_scanner.py
