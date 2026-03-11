@echo off
title Gov.il Scraper
echo Starting Gov.il Scraper...
echo.

:: Open Chrome to the dashboard after a short delay
start "" cmd /c "timeout /t 3 /nobreak >nul && start chrome http://localhost:5000"

:: Enable debug mode for local development
set FLASK_DEBUG=1

:: Start the Flask server (blocks until closed)
python app.py
