@echo off
REM ========================================================================
REM  Nadlan settlement-level worker (full deals per settlement, paginated)
REM  Sister to run_nadlan_worker.bat which is per-parcel.
REM ========================================================================
setlocal EnableExtensions
cd /d "%~dp0"

echo.
echo ============================================================
echo   Nadlan Settlement Worker (Full Pagination)
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
echo Ensuring real Chrome is installed (required for nadlan token-verify)...
python -m playwright install chrome >nul 2>nul
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
echo Worker ID: %WORKER%

echo.
echo ============================================================
echo   Connecting to:  %SERVER%
echo   Worker ID:      %WORKER%
echo ============================================================
echo.
echo Each task = one settlement = up to thousands of deals.
echo A Chrome window pops up. Do NOT close it.
echo Stop with Ctrl+C; re-run to resume from the queue.
echo.

python nadlan_settlement_worker.py --server "%SERVER%" --worker-id "%WORKER%"
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
goto END

:INSTALL_FAILED
echo [ERROR] pip install failed. Check your internet connection.
goto END

:END
echo.
pause
