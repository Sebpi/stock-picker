@echo off
setlocal
cd /d "%~dp0"

set "APP_HOST=127.0.0.1"
set "APP_PORT=8000"
if defined STOCKPICKER_APP_HOST set "APP_HOST=%STOCKPICKER_APP_HOST%"
if defined STOCKPICKER_APP_PORT set "APP_PORT=%STOCKPICKER_APP_PORT%"
set "APP_URL=http://%APP_HOST%:%APP_PORT%"
set "HEALTH_URL=%APP_URL%/api/health"
set "HTTP_PROXY="
set "HTTPS_PROXY="
set "ALL_PROXY="
set "http_proxy="
set "https_proxy="
set "all_proxy="
set "GIT_HTTP_PROXY="
set "GIT_HTTPS_PROXY="

where python >nul 2>nul
if errorlevel 1 (
  echo Python is not available on PATH. Install Python or repair your PATH, then try again.
  pause
  exit /b 1
)

powershell -NoProfile -ExecutionPolicy Bypass -Command "$ProgressPreference = 'SilentlyContinue'; try { $response = Invoke-WebRequest -UseBasicParsing '%HEALTH_URL%' -TimeoutSec 2; if ($response.StatusCode -eq 200) { exit 0 } } catch { } exit 1"
if not errorlevel 1 (
  echo StockPicker backend is already running at %APP_URL%.
  exit /b 0
)

echo Starting StockPicker Backend...
python -m uvicorn main:app --host %APP_HOST% --port %APP_PORT%
set "EXIT_CODE=%ERRORLEVEL%"
if not "%EXIT_CODE%"=="0" (
  echo.
  echo Backend exited with code %EXIT_CODE%.
  echo If port %APP_PORT% is already in use, close the other StockPicker window and try again.
)
pause
endlocal
exit /b %EXIT_CODE%
