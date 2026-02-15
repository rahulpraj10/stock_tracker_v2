
import sqlite3
import os

def check_table_access():
    db_path = 'stocks.db'
    if not os.path.exists(db_path):
        print(f"Error: {db_path} not found.")
        return

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # 1. List some tables to confirm names
    print("--- Listing first 5 tables ---")
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' LIMIT 5")
    tables = cursor.fetchall()
    print(tables)
    
    if not tables:
        print("No tables found!")
        return

    test_table = tables[0][0]
    print(f"\n--- Testing access to table: {test_table} ---")

    # 2. Test Single Quotes (What is currently in app.py)
    query_single = f"SELECT Close, Date FROM '{test_table}' LIMIT 1"
    print(f"Query 1 (Single Quotes): {query_single}")
    try:
        cursor.execute(query_single)
        print("Success!")
        print(cursor.fetchone())
    except Exception as e:
        print(f"Failed: {e}")

    # 3. Test Double Quotes
    query_double = f'SELECT Close, Date FROM "{test_table}" LIMIT 1'
    print(f"\nQuery 2 (Double Quotes): {query_double}")
    try:
        cursor.execute(query_double)
        print("Success!")
        print(cursor.fetchone())
    except Exception as e:
        print(f"Failed: {e}")

    # 4. Test Brackets (SQLite specific)
    query_brackets = f"SELECT Close, Date FROM [{test_table}] LIMIT 1"
    print(f"\nQuery 3 (Brackets): {query_brackets}")
    try:
        cursor.execute(query_brackets)
        print("Success!")
        print(cursor.fetchone())
    except Exception as e:
        print(f"Failed: {e}")

    conn.close()

if __name__ == "__main__":
    check_table_access()
