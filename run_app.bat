@echo off
cd /d "%~dp0"
echo Starting Stock Data Viewer...
if exist .venv\Scripts\activate (
    call .venv\Scripts\activate
) else (
    echo Virtual environment not found, attempting to use system python...
)

echo Installing dependencies...
pip install -r requirements.txt

echo Running application...
python app.py
pause
