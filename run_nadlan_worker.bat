@echo off
REM ========================================================================
REM  Distributed nadlan worker - one-click runner for any helper machine
REM
REM  What this does:
REM    1. Verifies Python is on PATH (and offers a download link if not).
REM    2. Auto-installs requests + playwright + Chromium on first run.
REM    3. Prompts for server URL and worker id (defaults handled).
REM    4. Connects to the central queue and starts pulling tasks.
REM
REM  Stop with Ctrl+C. Re-run to resume; server tracks state.
REM ========================================================================
setlocal EnableExtensions EnableDelayedExpansion
cd /d "%~dp0"

echo.
echo ============================================================
echo   Nadlan Distributed Worker
echo ============================================================
echo.

where python >nul 2>nul
if errorlevel 1 (
    echo [ERROR] python is not on PATH.
    echo         Install Python 3.10+ from https://www.python.org/downloads/
    echo         then re-run this script.
    pause
    exit /b 1
)

echo Checking dependencies...
python -c "import requests, playwright" >nul 2>nul
if errorlevel 1 (
    echo Installing required Python packages (one-time)...
    python -m pip install --quiet --upgrade pip
    python -m pip install --quiet requests "playwright>=1.49"
    if errorlevel 1 (
        echo [ERROR] pip install failed. Check your internet connection.
        pause
        exit /b 1
    )
)

echo Ensuring Chromium is installed (no-op if already present)...
python -m playwright install chromium >nul 2>nul
if errorlevel 1 (
    echo [ERROR] Could not install Chromium. Try running manually:
    echo         python -m playwright install chromium
    pause
    exit /b 1
)

echo.
set "DEFAULT_SERVER=https://govil-scraper.onrender.com"
set "SERVER=%NADLAN_SERVER_URL%"
if "%SERVER%"=="" (
    echo Server URL [press Enter for %DEFAULT_SERVER%]:
    set /p SERVER=
)
if "%SERVER%"=="" set "SERVER=%DEFAULT_SERVER%"

set "WORKER=%NADLAN_WORKER_ID%"
if "%WORKER%"=="" set "WORKER=%COMPUTERNAME%"
echo Worker ID: %WORKER%   (set NADLAN_WORKER_ID env var to override)

echo.
echo ============================================================
echo   Connecting to:  %SERVER%
echo   Worker ID:      %WORKER%
echo ============================================================
echo.
echo A Chromium window will pop up while scraping - leave it open.
echo This worker will keep pulling tasks until you press Ctrl+C.
echo Re-run this script any time to resume.
echo.

python nadlan_worker.py --server "%SERVER%" --worker-id "%WORKER%"

if errorlevel 1 (
    echo.
    echo [INFO] Worker exited with errors. Re-run to resume.
) else (
    echo.
    echo [INFO] Worker exited cleanly. Re-run to resume.
)

pause
