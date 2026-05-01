@echo off
REM ========================================================================
REM  Distributed nadlan worker - one-click runner for any helper machine
REM
REM  What this does:
REM    1. Verifies Python is on PATH.
REM    2. Auto-installs requests + playwright + Chromium on first run.
REM    3. Prompts for server URL and worker id (defaults handled).
REM    4. Connects to the central queue and starts pulling tasks.
REM
REM  Stop with Ctrl+C. Re-run to resume; server tracks state.
REM ========================================================================
setlocal EnableExtensions
cd /d "%~dp0"

echo.
echo ============================================================
echo   Nadlan Distributed Worker
echo ============================================================
echo.

where python >nul 2>nul
if errorlevel 1 goto NO_PYTHON

echo Checking dependencies...
python -c "import requests, playwright" >nul 2>nul
if errorlevel 1 goto INSTALL_DEPS
goto INSTALL_CHROMIUM

:INSTALL_DEPS
echo Installing required Python packages, one-time setup...
python -m pip install --quiet --upgrade pip
python -m pip install --quiet requests playwright
if errorlevel 1 goto INSTALL_FAILED
goto INSTALL_CHROMIUM

:INSTALL_CHROMIUM
echo Ensuring Chromium is installed...
python -m playwright install chromium >nul 2>nul
if errorlevel 1 goto CHROMIUM_FAILED
goto CONFIGURE

:CONFIGURE
echo.
set "DEFAULT_SERVER=https://govil-scraper.onrender.com"
set "SERVER=%NADLAN_SERVER_URL%"
if not "%SERVER%"=="" goto SERVER_SET
echo Server URL [press Enter for %DEFAULT_SERVER%]:
set /p SERVER=
if "%SERVER%"=="" set "SERVER=%DEFAULT_SERVER%"

:SERVER_SET
set "WORKER=%NADLAN_WORKER_ID%"
if "%WORKER%"=="" set "WORKER=%COMPUTERNAME%"
echo Worker ID: %WORKER%   [override with NADLAN_WORKER_ID env var]

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
if errorlevel 1 goto EXITED_WITH_ERROR
echo.
echo [INFO] Worker exited cleanly. Re-run to resume.
goto END

:EXITED_WITH_ERROR
echo.
echo [INFO] Worker exited with errors. Re-run to resume.
goto END

:NO_PYTHON
echo [ERROR] python is not on PATH.
echo         Install Python 3.10+ from https://www.python.org/downloads/
echo         then re-run this script.
goto END

:INSTALL_FAILED
echo [ERROR] pip install failed. Check your internet connection.
goto END

:CHROMIUM_FAILED
echo [ERROR] Could not install Chromium. Try running manually:
echo         python -m playwright install chromium
goto END

:END
echo.
pause
