import os
import sys

# Ensure we can import from the current directory
sys.path.append(os.getcwd())

from database import get_orders_db_connection

def migrate():
    conn = get_orders_db_connection()
    if not conn:
        print("Could not connect to database.")
        return

    is_postgres = 'psycopg2' in str(type(conn))
    print(f"Connected to database. Is Postgres: {is_postgres}")

    # Adding new columns to orders table
    # status VARCHAR(20) DEFAULT 'OPEN'
    # sell_date TEXT
    # sell_price REAL

    # Since SQLite ALTER TABLE ADD COLUMN has limitations with DEFAULT values and adding multiple cols, 
    # we add them one by one. SQLite allows ADD COLUMN with DEFAULT.
    
    columns_to_add = [
        ("status", "VARCHAR(20) DEFAULT 'OPEN'"),
        ("sell_date", "TEXT"),
        ("sell_price", "REAL")
    ]

    try:
        cur = conn.cursor()
        for col_name, col_def in columns_to_add:
            try:
                sql = f"ALTER TABLE orders ADD COLUMN {col_name} {col_def};"
                cur.execute(sql)
                conn.commit()
                print(f"Successfully added column '{col_name}' to orders table.")
            except Exception as e:
                conn.rollback()
                msg = str(e).lower()
                if 'duplicate column' in msg or 'already exists' in msg:
                    print(f"Column '{col_name}' already exists in orders table. Skipping.")
                else:
                    print(f"Error adding '{col_name}': {e}")
        
        cur.close()
    except Exception as e:
        print(f"Critical error during migration: {e}")
    finally:
        conn.close()
        print("Migration complete.")

if __name__ == "__main__":
    migrate()
