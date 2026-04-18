@echo off
REM ========================================================================
REM  Over.org.il scraper worker - runs locally and processes tasks queued
REM  by over.org.il (the dataset version tracker).
REM
REM  Setup (one time per machine):
REM    1. Create a file named ".env" next to this script.
REM    2. Inside it, put at least this line:
REM         OVER_API_KEY=<paste-your-key-here>
REM       (copy the key from your other machine - do NOT commit .env to git)
REM    3. Run once:  pip install -r requirements.txt
REM
REM  Usage: just double-click this file, or run from a terminal:
REM     run_over_worker.bat
REM ========================================================================

setlocal EnableExtensions EnableDelayedExpansion

REM Move to the script's own directory so over_worker.py and .env resolve.
cd /d "%~dp0"

echo.
echo ============================================================
echo   Over.org.il Worker
echo   Repo:    govil-scraper
echo   Script:  over_worker.py
echo   Server:  https://www.over.org.il
echo ============================================================
echo.

REM ---- Load .env into this process's environment (no python-dotenv needed) ----
if exist ".env" (
    for /f "usebackq eol=# tokens=1,* delims==" %%A in (".env") do (
        set "_k=%%A"
        set "_v=%%B"
        REM trim leading spaces in key
        for /f "tokens=* delims= " %%K in ("!_k!") do set "_k=%%K"
        if not "!_k!"=="" if not "!_v!"=="" (
            REM strip surrounding double quotes from value if present
            if "!_v:~0,1!"=="^"" set "_v=!_v:~1!"
            if "!_v:~-1!"=="^"" set "_v=!_v:~0,-1!"
            set "!_k!=!_v!"
        )
    )
)

REM ---- Pre-flight: confirm OVER_API_KEY is now set ----
if not defined OVER_API_KEY (
    echo.
    echo [ERROR] OVER_API_KEY is missing.
    echo.
    echo   The worker cannot talk to over.org.il without this key.
    echo.
    echo   Fix it like this:
    echo     1. Create a file named   .env   in this folder:
    echo            %~dp0.env
    echo     2. Put at least this line inside it:
    echo            OVER_API_KEY=your-key-here
    echo     3. Copy the key from your other machine's .env file
    echo        ^(do NOT commit .env to git^).
    echo     4. Double-click this .bat again.
    echo.
    pause
    exit /b 1
)

echo OVER_API_KEY loaded ^(ends with ...!OVER_API_KEY:~-4!^)
echo.
echo Tip: while it's running, open a second terminal and `type worker_status.txt`
echo to see what task it's currently working on.
echo.

REM Use `python` from PATH. If you want a specific venv, change this line.
python over_worker.py --poll-interval 5

echo.
echo Worker exited. Press any key to close.
pause >nul
endlocal
