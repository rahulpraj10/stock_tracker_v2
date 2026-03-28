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

    # Adding new columns to orders and watchstocks
    
    tables_to_update = ["orders", "watchstocks"]
    col_name = "source_strategy"
    col_def = "VARCHAR(50)"

    try:
        cur = conn.cursor()
        for table_name in tables_to_update:
            try:
                # Basic ALTER TABLE ADD COLUMN syntax is compatible with both SQLite and Postgres
                sql = f"ALTER TABLE {table_name} ADD COLUMN {col_name} {col_def};"
                cur.execute(sql)
                conn.commit()
                print(f"Successfully added column '{col_name}' to {table_name} table.")
            except Exception as e:
                conn.rollback()
                msg = str(e).lower()
                if 'duplicate column' in msg or 'already exists' in msg:
                    print(f"Column '{col_name}' already exists in {table_name} table. Skipping.")
                else:
                    print(f"Error adding '{col_name}' to {table_name}: {e}")
        
        cur.close()
    except Exception as e:
        print(f"Critical error during migration: {e}")
    finally:
        conn.close()
        print("Migration complete.")

if __name__ == "__main__":
    migrate()
