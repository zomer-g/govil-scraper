@echo off
REM ========================================================================
REM  Nadlan.gov.il deal scraper - one-click runner
REM
REM  Modes:
REM    1) single parcel  - prompts for gush+chelka, writes <gush>-<chelka>.csv
REM    2) bulk           - reads a parcels.csv, writes/appends deals.csv
REM
REM  Setup (one time): pip install -r requirements.txt
REM                    python -m playwright install chromium
REM ========================================================================
setlocal EnableExtensions EnableDelayedExpansion
cd /d "%~dp0"

echo.
echo ============================================================
echo   Nadlan.gov.il Deal Scraper
echo ============================================================
echo.

where python >nul 2>nul
if errorlevel 1 (
    echo [ERROR] python is not on PATH. Install Python and re-run.
    pause
    exit /b 1
)

echo Choose mode:
echo   [1] Single parcel    (enter gush + chelka, get one CSV)
echo   [2] Bulk             (read parcels.csv, append to deals.csv)
echo   [3] Daily incremental (settlement-level scan, append new deals only)
echo.
set /p MODE="Choice (1, 2, or 3): "

if "%MODE%"=="1" goto SINGLE
if "%MODE%"=="2" goto BULK
if "%MODE%"=="3" goto DAILY
echo [ERROR] Invalid choice.
pause
exit /b 1

:SINGLE
echo.
set /p GUSH="Gush (block number): "
set /p CHELKA="Chelka (parcel number): "
if "%GUSH%"=="" goto BAD_INPUT
if "%CHELKA%"=="" goto BAD_INPUT

set OUT=%GUSH%-%CHELKA%.csv
echo.
echo Running scraper for gush=%GUSH% chelka=%CHELKA%...
echo Output will be written to: %OUT%
echo (A Chromium window will pop up - this is required to bypass reCAPTCHA. Don't close it.)
echo.

python run_single_parcel.py "%GUSH%" "%CHELKA%" "%OUT%"

if errorlevel 1 (
    echo.
    echo [ERROR] Scraping failed. See output above.
) else (
    echo.
    echo Done. Open %OUT% in Excel.
)
goto END

:BULK
echo.
set /p PARCELS="Path to parcels.csv (output of catalog/parcels_shapefile.py): "
set /p DEALS="Path to deals.csv (will be created or appended): "
if "%PARCELS%"=="" goto BAD_INPUT
if "%DEALS%"=="" goto BAD_INPUT
set /p LIMIT="Limit (number of new parcels to process; blank = all): "

echo.
echo Running bulk scraper...
echo (A Chromium window will pop up for each parcel. Don't close it.)
echo Press Ctrl+C to pause - resume by running this again with the same files.
echo.

if "%LIMIT%"=="" (
    python bulk_nadlan.py "%PARCELS%" "%DEALS%"
) else (
    python bulk_nadlan.py "%PARCELS%" "%DEALS%" --limit %LIMIT%
)

if errorlevel 1 (
    echo.
    echo [ERROR] Bulk run failed or was interrupted. Re-run to resume.
) else (
    echo.
    echo Done.
)
goto END

:DAILY
echo.
set /p ARCHIVE_DIR="Archive directory (will hold nadlan_master.csv + checkpoint.json): "
if "%ARCHIVE_DIR%"=="" goto BAD_INPUT
set /p SETTLEMENTS="Settlements filter (comma-separated setlCodes; blank = all 1,509 settlements): "
set /p LOOKBACK="Lookback days (blank = 90): "

echo.
echo Running daily incremental against %ARCHIVE_DIR%...
echo (A Chromium window will pop up. ~3 hours for full Israel scan.)
echo If checkpoint.json is missing this will run a bootstrap instead.
echo.

set DAILY_ARGS=--archive-dir "%ARCHIVE_DIR%"
if not "%SETTLEMENTS%"=="" set DAILY_ARGS=%DAILY_ARGS% --settlements %SETTLEMENTS%
if not "%LOOKBACK%"=="" set DAILY_ARGS=%DAILY_ARGS% --lookback-days %LOOKBACK%

python incremental_nadlan_daily.py %DAILY_ARGS%

if errorlevel 1 (
    echo.
    echo [ERROR] Incremental run failed. See output above.
) else (
    echo.
    echo Done. Master CSV updated; daily delta saved as nadlan_delta_*.csv.
)
goto END

:BAD_INPUT
echo [ERROR] Empty input.
goto END

:END
echo.
pause
