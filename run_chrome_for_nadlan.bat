@echo off
REM ========================================================================
REM  Launch Chrome with the user's profile + remote-debugging port so the
REM  nadlan scrapers can attach via CDP.
REM
REM  WHY: nadlan.gov.il's reCAPTCHA Enterprise scores fresh Playwright Chrome
REM  profiles as bot. Reusing the user's real Chrome (with site engagement
REM  history on nadlan) gives us a human score and pages stop returning 0.
REM
REM  IMPORTANT: Close all running Chrome windows before running this script.
REM  Chrome can only attach the debug port when it owns the profile.
REM ========================================================================
setlocal EnableExtensions

REM A separate user-data-dir so we don't pollute the user's daily-driver
REM profile (and so this works even if main Chrome is open).
set "PROFILE_DIR=%USERPROFILE%\nadlan-chrome-profile"
if not exist "%PROFILE_DIR%" mkdir "%PROFILE_DIR%"

set "CHROME_EXE=C:\Program Files\Google\Chrome\Application\chrome.exe"
if not exist "%CHROME_EXE%" set "CHROME_EXE=C:\Program Files (x86)\Google\Chrome\Application\chrome.exe"

if not exist "%CHROME_EXE%" (
    echo [ERROR] Chrome not found at the standard install paths.
    pause
    exit /b 1
)

echo.
echo ============================================================
echo   Launching Chrome with debug port 9222
echo ============================================================
echo.
echo   Profile dir: %PROFILE_DIR%
echo   This is a SEPARATE profile from your daily Chrome.
echo.
echo   Visit nadlan.gov.il manually a couple of times and click around
echo   so the profile builds up site-engagement signal — that's what
echo   reCAPTCHA Enterprise scores high.
echo.
echo   When ready, leave this Chrome window open and run:
echo     run_nadlan_settlement_worker.bat
echo.

start "" "%CHROME_EXE%" ^
    --remote-debugging-port=9222 ^
    --user-data-dir="%PROFILE_DIR%" ^
    https://www.nadlan.gov.il/

echo Chrome launched. Closing this window won't close Chrome.
timeout /t 5 >nul
