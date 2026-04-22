@echo off
setlocal

set "ROOT=%~dp0"
set "BACKEND_DIR=%ROOT%backend"
set "APP_HOST=127.0.0.1"
set "APP_PORT=8000"
if defined STOCKPICKER_APP_HOST set "APP_HOST=%STOCKPICKER_APP_HOST%"
if defined STOCKPICKER_APP_PORT set "APP_PORT=%STOCKPICKER_APP_PORT%"
set "APP_URL=http://%APP_HOST%:%APP_PORT%"
set "HEALTH_URL=%APP_URL%/api/health"
set "WAIT_SECONDS=45"
if defined STOCKPICKER_WAIT_SECONDS set "WAIT_SECONDS=%STOCKPICKER_WAIT_SECONDS%"
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

call :is_healthy
if not errorlevel 1 (
  echo StockPicker is already running at %APP_URL%.
  goto open_browser
)

echo Starting StockPicker backend...
start "StockPicker Backend" cmd /k "cd /d ""%BACKEND_DIR%"" && python -m uvicorn main:app --host %APP_HOST% --port %APP_PORT%"

echo Waiting for backend to become ready...
powershell -NoProfile -ExecutionPolicy Bypass -Command "$ProgressPreference = 'SilentlyContinue'; $deadline = (Get-Date).AddSeconds(%WAIT_SECONDS%); while ((Get-Date) -lt $deadline) { try { $response = Invoke-WebRequest -UseBasicParsing '%HEALTH_URL%' -TimeoutSec 2; if ($response.StatusCode -eq 200) { exit 0 } } catch { } Start-Sleep -Milliseconds 500 }; exit 1"
if errorlevel 1 (
  echo Backend did not become ready within %WAIT_SECONDS% seconds.
  echo Check the "StockPicker Backend" window for port or import errors.
  pause
  exit /b 1
)

:open_browser
if /i "%STOCKPICKER_SKIP_BROWSER%"=="1" (
  echo Browser launch skipped because STOCKPICKER_SKIP_BROWSER=1.
) else (
  start "" "%APP_URL%"
)

endlocal
exit /b 0

:is_healthy
powershell -NoProfile -ExecutionPolicy Bypass -Command "$ProgressPreference = 'SilentlyContinue'; try { $response = Invoke-WebRequest -UseBasicParsing '%HEALTH_URL%' -TimeoutSec 2; if ($response.StatusCode -eq 200) { exit 0 } } catch { } exit 1"
exit /b %ERRORLEVEL%
