import sqlite3
import os
import requests
import psycopg2
from psycopg2.extras import RealDictCursor

DB_URL = "https://github.com/rahulpraj10/stock_tracker_v2/raw/main/StockData/stock_data.db"
DB_PATH = os.path.join("StockData", "stock_data.db")
ORDERS_DB_PATH = "orders.db"

def get_stock_db_connection():
    """Connects to the read-only Stock Data SQLite database."""
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

def get_orders_db_connection():
    """Connects to the persistent Orders database (Postgres or SQLite)."""
    database_url = os.environ.get('DATABASE_URL')
    
    if database_url:
        # Use PostgreSQL (Supabase/Render)
        try:
            conn = psycopg2.connect(database_url, cursor_factory=RealDictCursor)
            return conn
        except Exception as e:
            print(f"Error connecting to PostgreSQL: {e}")
            return None
    else:
        # Fallback to local SQLite for development
        conn = sqlite3.connect(ORDERS_DB_PATH)
        conn.row_factory = sqlite3.Row
        return conn

# For backward compatibility during refactor, could alias
get_db_connection = get_stock_db_connection
