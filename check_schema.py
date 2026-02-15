
import sqlite3
import os

db_path = r'c:\Projects\StockAnalyser\StockDataMerged\StockData\stock_data.db'

if not os.path.exists(db_path):
    print(f"DB not found at {db_path}")
else:
    try:
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        
        # List tables
        cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cur.fetchall()
        print(f"Tables found: {[t[0] for t in tables]}")
        
        if tables:
            first_table = tables[0][0]
            print(f"Columns in {first_table}:")
            cur.execute(f"PRAGMA table_info('{first_table}')")
            cols = cur.fetchall()
            for col in cols:
                print(col)
                
            # Check for 'stocks' table specifically if user requested it
            if 'stocks' in [t[0] for t in tables]:
                print("\nColumns in 'stocks' table:")
                cur.execute("PRAGMA table_info('stocks')")
                cols = cur.fetchall()
                for col in cols:
                    print(col)
        
        conn.close()
    except Exception as e:
        print(f"Error: {e}")
