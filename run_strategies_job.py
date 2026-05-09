import os
import sys
import json
from datetime import datetime
import pytz

# Ensure we can import from the current directory
sys.path.append(os.getcwd())

from database import get_orders_db_connection
from strategies.bullish_reversal import get_bullish_reversal_stocks
from strategies.double_bottom_v1 import get_double_bottom_stocks as get_double_bottom_v1_stocks, default_params as db_v1_default_params
from strategies.multi_frame import get_multi_frame

IST = pytz.timezone('Asia/Kolkata')

def run_and_store_strategies():
    run_date = datetime.now(IST).strftime('%Y-%m-%d')
    print(f"Starting strategy batch job for {run_date}")

    strategies_to_run = [
        ('bullish_reversal', get_bullish_reversal_stocks, {}),
        ('double_bottom_v1', get_double_bottom_v1_stocks, {'params': db_v1_default_params}),
        ('multi_frame', get_multi_frame, {})
    ]

    conn = get_orders_db_connection()
    if not conn:
        print("Failed to connect to database.")
        return

    is_postgres = 'psycopg2' in str(type(conn))

    try:
        cur = conn.cursor()
        
        # Ensure the table exists
        print("Ensuring strategy_results table exists...")
        if is_postgres:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS strategy_results (
                    id SERIAL PRIMARY KEY,
                    sc_code VARCHAR(20),
                    sc_name VARCHAR(255),
                    strategy_name VARCHAR(50),
                    run_date VARCHAR(20),
                    data_json TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
        else:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS strategy_results (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    sc_code TEXT,
                    sc_name TEXT,
                    strategy_name TEXT,
                    run_date TEXT,
                    data_json TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
        
        for strategy_name, func, kwargs in strategies_to_run:
            print(f"Running {strategy_name}...")
            try:
                # Some functions might return a DataFrame, so we handle that if needed
                results = func(**kwargs)
                
                # Convert DataFrame to dict if necessary (double_bottom_v1 returns DataFrame usually based on app.py)
                import pandas as pd
                if isinstance(results, pd.DataFrame):
                    if not results.empty:
                        results = results.to_dict('records')
                    else:
                        results = []
                
                print(f"Found {len(results)} records for {strategy_name}")

                # Delete existing records for this date and strategy to avoid duplicates if re-run
                if is_postgres:
                    cur.execute("DELETE FROM strategy_results WHERE run_date = %s AND strategy_name = %s", (run_date, strategy_name))
                else:
                    cur.execute("DELETE FROM strategy_results WHERE run_date = ? AND strategy_name = ?", (run_date, strategy_name))

                # Insert new records
                for row in results:
                    sc_code = str(row.get('SC_CODE', ''))
                    sc_name = str(row.get('SC_NAME', ''))
                    data_json = json.dumps(row, default=str) # handle dates/numpy types

                    if is_postgres:
                        cur.execute(
                            "INSERT INTO strategy_results (sc_code, sc_name, strategy_name, run_date, data_json) VALUES (%s, %s, %s, %s, %s)",
                            (sc_code, sc_name, strategy_name, run_date, data_json)
                        )
                    else:
                        cur.execute(
                            "INSERT INTO strategy_results (sc_code, sc_name, strategy_name, run_date, data_json) VALUES (?, ?, ?, ?, ?)",
                            (sc_code, sc_name, strategy_name, run_date, data_json)
                        )

            except Exception as e:
                print(f"Error running {strategy_name}: {e}")
        
        # Cleanup logic: delete data older than 30 days
        print("Running cleanup for data older than 30 days...")
        try:
            if is_postgres:
                cur.execute("DELETE FROM strategy_results WHERE run_date < TO_CHAR(CURRENT_DATE - INTERVAL '30 days', 'YYYY-MM-DD')")
            else:
                cur.execute("DELETE FROM strategy_results WHERE run_date < date('now', '-30 days')")
            print(f"Cleanup complete. Deleted {cur.rowcount} old records.")
        except Exception as e:
            print(f"Error during cleanup: {e}")

        conn.commit()
        print(f"Batch job complete for {run_date}")
        cur.close()

    except Exception as e:
        print(f"Database error during batch job: {e}")
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    run_and_store_strategies()
