@echo off
setlocal
cd /d "%~dp0"

echo ============================================================
echo  StockPicker Sentiment Agent
echo  Scans AI/Tech news every 60 minutes
echo  Sends SMS only on material disruptions
echo ============================================================
echo.

where python >nul 2>nul
if errorlevel 1 (
  echo Python is not available on PATH. Install Python or repair your PATH.
  pause
  exit /b 1
)

:: Optional: run a test SMS first to verify Twilio is configured
:: python sentiment_agent.py --test-sms

echo Starting sentiment agent loop (Ctrl+C to stop)...
echo Logs are written to sentiment_agent.log
echo.
python sentiment_agent.py --loop --interval 60
set "EXIT_CODE=%ERRORLEVEL%"
echo.
echo Sentiment agent stopped (exit code %EXIT_CODE%).
pause
endlocal
exit /b %EXIT_CODE%
