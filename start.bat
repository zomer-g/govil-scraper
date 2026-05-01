@echo off
REM Unified entry point. Prefer this over the per-script run_*.bat files.
REM
REM Without arguments  : opens the Flask web UI at http://localhost:5000
REM start.bat scrape <url>                       : one-shot scrape
REM start.bat worker --source over --key ...     : poll over.org.il
REM start.bat worker --source local --server ... : poll our own server
REM start.bat worker --source nadlan-queue ...   : claim nadlan parcel batches
REM start.bat bulk-nadlan --archive-dir ...      : incremental settlement archive

setlocal

if "%~1"=="" goto serve_ui
if /I "%~1"=="serve" goto serve_ui

REM Anything else goes through the unified CLI.
python -m govscraper.cli %*
goto :eof

:serve_ui
title Gov.il Scraper
echo Starting Gov.il Scraper...
echo.
REM Open Chrome to the dashboard after a short delay.
start "" cmd /c "timeout /t 3 /nobreak >nul && start chrome http://localhost:5000"
set FLASK_DEBUG=1
python app.py
