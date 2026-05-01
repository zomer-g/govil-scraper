@echo off
cd /d "%~dp0"

:: Load environment variables from env file
for /f "usebackq tokens=1,* delims==" %%A in ("env") do (
    set "%%A=%%B"
)

:: Set server URL if not already set
if "%RENDER_SERVER_URL%"=="" set "RENDER_SERVER_URL=https://govil-scraper.onrender.com"

echo Starting Gov.il Scraper Worker...
echo Server: %RENDER_SERVER_URL%
echo Worker ID: %COMPUTERNAME%

python worker.py --server "%RENDER_SERVER_URL%" --key "%WORKER_API_KEY%" --worker-id "%COMPUTERNAME%"
