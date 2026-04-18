@echo off
REM ========================================================================
REM  Over.org.il scraper worker - runs locally and processes tasks queued
REM  by over.org.il (the dataset version tracker).
REM
REM  Setup (one time per machine):
REM    1. Create a file named .env next to this script.
REM    2. Inside it, put at least this line:
REM         OVER_API_KEY=your-key-here
REM       (copy the key from your other machine; do NOT commit .env to git)
REM    3. Optional but recommended:  pip install -r requirements.txt
REM
REM  Usage: just double-click this file.
REM ========================================================================

setlocal EnableExtensions EnableDelayedExpansion
cd /d "%~dp0"

echo.
echo ============================================================
echo   Over.org.il Worker
echo   Repo:    govil-scraper
echo   Script:  over_worker.py
echo   Server:  https://www.over.org.il
echo ============================================================
echo.

REM ---- Load .env into this process so OVER_API_KEY is available ----
if exist ".env" (
    for /f "usebackq tokens=1,* delims==" %%A in (".env") do (
        set "_k=%%A"
        set "_v=%%B"
        if defined _k if defined _v (
            if not "!_k:~0,1!"=="#" (
                set "!_k!=!_v!"
            )
        )
    )
)

if not defined OVER_API_KEY (
    echo.
    echo [ERROR] OVER_API_KEY is missing.
    echo.
    echo   Create a file named .env in this folder:
    echo       %~dp0.env
    echo   With at least this line:
    echo       OVER_API_KEY=your-key-here
    echo.
    echo   Copy the key from your other machine.
    echo.
    pause
    exit /b 1
)

echo OVER_API_KEY loaded.
echo.
echo Tip: in another terminal run  type worker_status.txt  to see live status.
echo.

python over_worker.py --poll-interval 5

echo.
echo Worker exited. Press any key to close.
pause >nul
endlocal
