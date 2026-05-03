@echo off
REM ========================================================================
REM   Auto-restart loop for the slice worker. Recovers from:
REM     - Chrome crash → re-launch Chrome with CDP
REM     - Worker circuit-breaker (5 consecutive fails) → wait + retry
REM     - Network glitches → retry
REM
REM   Stops only when:
REM     - You close this window
REM     - 10 consecutive worker exits in <60s each (suggests permanent issue)
REM ========================================================================
setlocal EnableExtensions

cd /d "%~dp0"

if not defined NADLAN_SERVER_URL set "NADLAN_SERVER_URL=https://govil-scraper.onrender.com"
if not defined NADLAN_WORKER_ID set "NADLAN_WORKER_ID=%COMPUTERNAME%-slice"
if not defined NADLAN_CHROME_CDP set "NADLAN_CHROME_CDP=http://127.0.0.1:9222"
set "PROFILE_DIR=%USERPROFILE%\nadlan-chrome-profile"
set "CHROME_EXE=C:\Program Files\Google\Chrome\Application\chrome.exe"
if not exist "%CHROME_EXE%" set "CHROME_EXE=C:\Program Files (x86)\Google\Chrome\Application\chrome.exe"

set /a fast_exit_count=0
set /a iteration=0

:loop
set /a iteration+=1
echo.
echo ============================================================
echo   Iteration %iteration% (%date% %time%)
echo ============================================================

REM --- Make sure Chrome is up on debug port ------------------------------
curl -s -o nul -w "chrome_status=%%{http_code}\n" http://127.0.0.1:9222/json/version 2>nul | findstr "chrome_status=200" >nul
if errorlevel 1 (
    echo Chrome not responding on 9222 — launching...
    start "" "%CHROME_EXE%" --remote-debugging-port=9222 --user-data-dir="%PROFILE_DIR%" "https://www.nadlan.gov.il/"
    timeout /t 8 /nobreak >nul
)

REM --- Run worker --------------------------------------------------------
set "T0=%TIME%"
python -u nadlan_slice_worker.py ^
    --server "%NADLAN_SERVER_URL%" ^
    --worker-id "%NADLAN_WORKER_ID%" ^
    --slices-per-claim 14 ^
    --per-settlement-pause 3 ^
    --max-failed 5
set "T1=%TIME%"

echo.
echo Worker exited (started %T0%, ended %T1%)
echo.

REM --- Cooldown before retry ---------------------------------------------
echo Sleeping 60s before restart...
timeout /t 60 /nobreak

goto loop
