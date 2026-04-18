@echo off
REM ========================================================================
REM  Over.org.il scraper worker — runs locally and processes tasks queued
REM  by over.org.il (the dataset version tracker).
REM
REM  Make sure OVER_API_KEY is set in your environment or in a .env file
REM  next to this script (the same key admins set on the server).
REM
REM  Usage: just double-click this file, or run from a terminal:
REM     run_over_worker.bat
REM ========================================================================

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
echo Tip: while it's running, open a second terminal and `type worker_status.txt`
echo to see what task it's currently working on.
echo.

REM Use `python` from PATH. If you want a specific venv, change this line.
python over_worker.py --poll-interval 5

echo.
echo Worker exited. Press any key to close.
pause >nul
