
import sqlite3
import pandas as pd

DB_PATH = r'c:\Projects\StockAnalyser\StockDataMerged\StockData\stock_data.db'

def inspect_db():
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        # 1. Get Schema
        print("--- Table Info ---")
        cursor.execute("PRAGMA table_info(stocks)")
        columns = [info[1] for info in cursor.fetchall()]
        print(f"Columns: {columns}")
        
        # 2. Sample Data
        print("\n--- Sample Data (First 5 rows) ---")
        cursor.execute(f"SELECT * FROM stocks LIMIT 5")
        rows = cursor.fetchall()
        for row in rows:
            print(row)
            
        # 3. specific column check
        print("\n--- SC NAME Sample ---")
        if "SC NAME" in columns:
            cursor.execute('SELECT "SC NAME", "SCRIP CODE" FROM stocks LIMIT 5')
            print(cursor.fetchall())
            
            # 4. Test Query
            test_q = "Rel"
            print(f"\n--- Testing Query for '{test_q}' ---")
            sql = 'SELECT DISTINCT "SC NAME", "SCRIP CODE" FROM stocks WHERE "SC NAME" LIKE ? LIMIT 10'
            cursor.execute(sql, ('%' + test_q + '%',))
            results = cursor.fetchall()
            print(f"Results: {results}")
        elif "SC_NAME" in columns:
             print("Found SC_NAME instead of SC NAME")
             cursor.execute('SELECT "SC_NAME", "SCRIP CODE" FROM stocks LIMIT 5')
             print(cursor.fetchall())
        else:
            print("Could not find SC NAME or SC_NAME column.")

        conn.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    inspect_db()
