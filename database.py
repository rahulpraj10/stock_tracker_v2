import sqlite3
import os
import requests

DB_URL = "https://github.com/rahulpraj10/stock_tracker_v2/raw/main/StockData/stock_data.db"
DB_PATH = os.path.join("StockData", "stock_data.db")

def get_db_connection():
    if not os.path.exists("StockData"):
        os.makedirs("StockData")

    # Check if DB needs to be downloaded
    if not os.path.exists(DB_PATH):
        print(f"Downloading database from {DB_URL}...")
        try:
            response = requests.get(DB_URL)
            response.raise_for_status()
            with open(DB_PATH, 'wb') as f:
                f.write(response.content)
            print("Database downloaded.")
        except Exception as e:
            print(f"Error downloading database: {e}")
            return None
            
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn
