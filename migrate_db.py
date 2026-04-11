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

    tables = ['orders', 'watchstocks']
    columns = ['attrib_01', 'attrib_02', 'attrib_03', 'attrib_04', 'attrib_05']
    col_type = 'VARCHAR(50)'

    try:
        cur = conn.cursor()
        for table in tables:
            for col in columns:
                try:
                    if is_postgres:
                        # Postgres syntax supports IF NOT EXISTS in newer versions, but catching error is safer across versions
                        sql = f'ALTER TABLE {table} ADD COLUMN {col} {col_type};'
                        cur.execute(sql)
                    else:
                        # SQLite syntax
                        sql = f'ALTER TABLE {table} ADD COLUMN {col} {col_type};'
                        cur.execute(sql)
                    
                    conn.commit()
                    print(f"Successfully added {col} to {table}.")
                except Exception as e:
                    # Rollback the failed transaction block
                    conn.rollback()
                    msg = str(e).lower()
                    if 'duplicate column' in msg or 'already exists' in msg:
                        print(f"Column {col} already exists in {table}. Skipping.")
                    else:
                        print(f"Error adding {col} to {table}: {e}")
        
        cur.close()
    except Exception as e:
        print(f"Critical error during migration: {e}")
    finally:
        conn.close()
        print("Migration complete.")

if __name__ == "__main__":
    migrate()
