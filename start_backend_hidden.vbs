Set WshShell = CreateObject("WScript.Shell")
WshShell.Run """C:\Users\User\AppData\Local\Python\pythoncore-3.14-64\python.exe"" -m uvicorn main:app --host 0.0.0.0 --port 8000", 0, False
