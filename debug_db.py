import sqlite3
import os
import requests

# DATA_URL = "https://github.com/rahulpraj10/stock_tracker_v2/raw/main/StockData/merged_stock_data.pkl"
DB_URL = "https://github.com/rahulpraj10/stock_tracker_v2/raw/main/StockData/stock_data.db"
DB_PATH = os.path.join("StockData", "stock_data.db")

def check_db():
    if not os.path.exists(DB_PATH):
        print(f"DB not found at {DB_PATH}, downloading...")
        try:
            response = requests.get(DB_URL)
            with open(DB_PATH, 'wb') as f:
                f.write(response.content)
            print("Downloaded.")
        except Exception as e:
            print(f"Download failed: {e}")
            return

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    try:
        # Check Min/Max Date
        cursor.execute("SELECT MIN(Date), MAX(Date) FROM stocks")
        min_date, max_date = cursor.fetchone()
        print(f"Stocks Data Range: {min_date} to {max_date}")
        
        # Check SC_CODE type
        cursor.execute("SELECT SC_CODE, typeof(SC_CODE) FROM stocks LIMIT 5")
        rows = cursor.fetchall()
        print("\nSample SC_CODEs and Types:")
        for row in rows:
            print(f"Value: {row[0]}, Type: {row[1]}")
            
        # Check if query works with CAST
        demo_code = rows[0][0]
        print(f"\nTesting Query for SC_CODE={demo_code}...")
        cursor.execute("SELECT COUNT(*) FROM stocks WHERE CAST(SC_CODE AS TEXT) = ?", (str(demo_code),))
        count = cursor.fetchone()[0]
        print(f"Match count with CAST: {count}")
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    check_db()
