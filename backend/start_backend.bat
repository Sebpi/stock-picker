@echo off
cd /d "%~dp0"
echo Starting StockPicker Backend...
python -m uvicorn main:app --reload
pause
