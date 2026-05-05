@echo off
cd /d "C:\Users\User\OneDrive\Desktop\StockPicker\backend"
"C:\Users\User\AppData\Local\Python\pythoncore-3.14-64\python.exe" -m uvicorn main:app --host 0.0.0.0 --port 8000
