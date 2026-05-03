@echo off
REM ========================================================================
REM   Run the nadlan slice worker — pulls (settlement, room, sort) tasks
REM   from the server and scrapes via UI clicks. Uses the user's running
REM   Chrome via CDP (port 9222) for high reCAPTCHA score.
REM
REM   PREREQUISITES:
REM   1. Chrome running with --remote-debugging-port=9222 — launch via
REM      run_chrome_for_nadlan.bat
REM   2. Server URL + worker key configured below
REM   3. Server seeded: POST /api/nadlan/slice-seed (admin)
REM
REM   USAGE:
REM   - Set NADLAN_SERVER_URL below to your Render app URL
REM   - Optionally set a custom NADLAN_WORKER_ID (default: hostname)
REM   - Double-click this file
REM ========================================================================
setlocal EnableExtensions

cd /d "%~dp0"

REM ---- Configuration -----------------------------------------------------
if not defined NADLAN_SERVER_URL set "NADLAN_SERVER_URL=https://govil-scraper.onrender.com"
if not defined NADLAN_WORKER_ID set "NADLAN_WORKER_ID=%COMPUTERNAME%-slice"
REM CDP attach port — must match run_chrome_for_nadlan.bat
if not defined NADLAN_CHROME_CDP set "NADLAN_CHROME_CDP=http://127.0.0.1:9222"

echo.
echo ============================================================
echo   nadlan slice worker
echo ============================================================
echo.
echo   Server: %NADLAN_SERVER_URL%
echo   Worker: %NADLAN_WORKER_ID%
echo   CDP:    %NADLAN_CHROME_CDP%
echo.
echo   Make sure Chrome is running with debug port — if not,
echo   run run_nadlan_chrome.bat first.
echo.

REM ---- Install deps if needed --------------------------------------------
where python >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Python not found in PATH
    pause
    exit /b 1
)

python -c "import requests, playwright" >nul 2>&1
if errorlevel 1 (
    echo Installing minimal dependencies...
    python -m pip install --quiet requests playwright
)

REM ---- Run ---------------------------------------------------------------
echo Starting worker (Ctrl+C to stop)...
echo.
python nadlan_slice_worker.py ^
    --server "%NADLAN_SERVER_URL%" ^
    --worker-id "%NADLAN_WORKER_ID%" ^
    --slices-per-claim 14 ^
    --per-settlement-pause 3 ^
    --max-failed 5

echo.
echo Worker stopped.
pause
