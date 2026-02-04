@echo off
echo Starting Test Run > status.log
python --version > version.log 2>&1
echo Installing Requirements >> status.log
pip install -r requirements.txt > pip.log 2>&1
echo Running Script >> status.log
python daily_update.py --date 2026-02-02 > run.log 2>&1
echo Listing StockData >> status.log
dir StockData > dir.log 2>&1
echo Done >> status.log
