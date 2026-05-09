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
    
    try:
        cur = conn.cursor()
        
        if is_postgres:
            sql = """
            CREATE TABLE IF NOT EXISTS strategy_results (
                id SERIAL PRIMARY KEY,
                sc_code VARCHAR(50) NOT NULL,
                sc_name VARCHAR(255) NOT NULL,
                strategy_name VARCHAR(100) NOT NULL,
                run_date VARCHAR(20) NOT NULL,
                data_json TEXT
            );
            """
        else:
            sql = """
            CREATE TABLE IF NOT EXISTS strategy_results (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sc_code VARCHAR(50) NOT NULL,
                sc_name VARCHAR(255) NOT NULL,
                strategy_name VARCHAR(100) NOT NULL,
                run_date VARCHAR(20) NOT NULL,
                data_json TEXT
            );
            """
            
        cur.execute(sql)
        
        # Add indexes for faster querying
        index_sql_1 = "CREATE INDEX IF NOT EXISTS idx_strategy_run_date ON strategy_results(run_date);"
        index_sql_2 = "CREATE INDEX IF NOT EXISTS idx_strategy_name ON strategy_results(strategy_name);"
        
        cur.execute(index_sql_1)
        cur.execute(index_sql_2)
        
        conn.commit()
        print("Successfully created strategy_results table.")
        
        cur.close()
    except Exception as e:
        print(f"Critical error during migration: {e}")
        conn.rollback()
    finally:
        conn.close()
        print("Migration complete.")

if __name__ == "__main__":
    migrate()
