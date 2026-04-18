@echo off
REM ========================================================================
REM  Over.org.il scraper worker - runs locally and processes tasks queued
REM  by over.org.il (the dataset version tracker).
REM
REM  Setup (one time per machine):
REM    1. Create a file named  .env  next to this script.
REM       IMPORTANT: in Notepad use Save As -^> Encoding: ANSI (or UTF-8
REM       WITHOUT BOM). File name should be  .env  with the leading dot.
REM    2. Inside it, put at least this line:
REM         OVER_API_KEY=your-key-here
REM       (copy the key from your other machine; do NOT commit .env to git)
REM    3. Optional but recommended:  pip install -r requirements.txt
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

REM ---- Check that .env exists ----
if not exist ".env" (
    echo [ERROR] .env file NOT FOUND at:
    echo     %~dp0.env
    echo.
    echo Common mistakes:
    echo   - File saved as  .env.txt  instead of  .env
    echo     ^(In Explorer enable: View -^> File name extensions, then rename^)
    echo   - File saved in a different folder
    echo.
    echo Copy your .env from the other machine into this folder, then try again.
    echo.
    pause
    exit /b 1
)

REM ---- Use findstr to grep just the OVER_API_KEY line (BOM-tolerant) ----
REM     findstr is more forgiving than  with weird encodings.
set "OVER_API_KEY="
for /f "tokens=1,* delims==" %%A in ('findstr /b /i /c:"OVER_API_KEY=" .env 2^>nul') do (
    set "OVER_API_KEY=%%B"
)

REM Fallback: also try without anchor in case of BOM/whitespace prefix
if not defined OVER_API_KEY (
    for /f "tokens=1,* delims==" %%A in ('findstr /i /c:"OVER_API_KEY=" .env 2^>nul') do (
        set "OVER_API_KEY=%%B"
    )
)

REM ---- Also load other common keys (so the python worker can use them) ----
for %%K in (WORKER_API_KEY GOOGLE_CLIENT_ID GOOGLE_CLIENT_SECRET ADMIN_EMAILS FLASK_SECRET_KEY) do (
    for /f "tokens=1,* delims==" %%A in ('findstr /b /i /c:"%%K=" .env 2^>nul') do (
        set "%%K=%%B"
    )
)

if not defined OVER_API_KEY (
    echo [ERROR] OVER_API_KEY is missing from .env.
    echo.
    echo The file %~dp0.env was found, but no
    echo OVER_API_KEY=... line was detected inside it.
    echo.
    echo Quick check: open cmd in this folder and run:
    echo     type .env
    echo You should see a line like:
    echo     OVER_API_KEY=fd9a44...
    echo.
    echo Common cause: file saved with UTF-16 encoding by Notepad.
    echo Fix: open .env in Notepad, File -^> Save As, Encoding = ANSI, overwrite.
    echo.
    pause
    exit /b 1
)

echo OVER_API_KEY loaded.
echo Tip: in another terminal run  type worker_status.txt  to see live status.
echo.

python over_worker.py --poll-interval 5

echo.
echo Worker exited. Press any key to close.
pause >nul
endlocal
