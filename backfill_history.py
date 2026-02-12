import os
import datetime
import daily_update

def run_backfill(start_date, end_date):
    # Ensure directory setup
    daily_update.setup_directories()
    
    current = start_date
    while current <= end_date:
        # Skip weekends (Saturday=5, Sunday=6)
        if current.weekday() < 5:
            print(f"--- Running update for {current.strftime('%Y-%m-%d')} ---")
            try:
                daily_update.process_date(current)
            except Exception as e:
                print(f"Error processing {current}: {e}")
        else:
            print(f"Skipping weekend: {current.strftime('%Y-%m-%d')}")
        
        current += datetime.timedelta(days=1)

if __name__ == "__main__":
    start = datetime.datetime(2025, 11, 3) # Nov 3rd, 2025
    end = datetime.datetime(2026, 2, 3)    # Feb 3rd, 2026
    
    # We are appending to existing data (which has Feb 4+), so DO NOT delete existing files.
    # The updated daily_update.py sorting logic will handle the ordering.
    
    print(f"Starting backfill from {start.strftime('%Y-%m-%d')} to {end.strftime('%Y-%m-%d')}")
    run_backfill(start, end)
