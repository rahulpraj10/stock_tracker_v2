import pandas as pd
import sqlite3
import os

STOCK_DATA_DIR = "StockData"
CSV_FILENAME = "merged_stock_data.csv"
DB_FILENAME = "stock_data.db"

def migrate_to_db():
    csv_path = os.path.join(STOCK_DATA_DIR, CSV_FILENAME)
    db_path = os.path.join(STOCK_DATA_DIR, DB_FILENAME)
    
    if not os.path.exists(csv_path):
        print(f"Error: CSV file not found at {csv_path}")
        return

    print(f"Reading CSV data from {csv_path}...")
    try:
        df = pd.read_csv(csv_path)
        
        # Handle 'DATE' vs 'Date' conflict
        # User specified: first column is DATE (source), second is Date (tracking)
        if 'DATE' in df.columns:
            print("Renaming source 'DATE' column to 'DATE_GEN'.")
            df.rename(columns={'DATE': 'DATE_GEN'}, inplace=True)
            
        # Fallback for duplicate 'Date' columns if they exist (just in case)
        cols = list(df.columns)
        if cols.count('Date') > 1:
            print("Detected duplicate 'Date' columns. Renaming first one to 'DATE_GEN_legacy'.")
            first_idx = cols.index('Date')
            cols[first_idx] = 'DATE_GEN_legacy'
            df.columns = cols
            
        # Check for 'Date.1' style mangling
        if 'Date.1' in df.columns:
             print("Detected 'Date.1' column. Renaming 'Date' -> 'DATE_GEN' and 'Date.1' -> 'Date'")
             df.rename(columns={'Date': 'DATE_GEN', 'Date.1': 'Date'}, inplace=True)

        # Ensure correct data types
        if 'Date' in df.columns:
            df['Date'] = pd.to_datetime(df['Date']).dt.date # Store as YYYY-MM-DD string in sqlite
            
        print(f"Loaded {len(df)} records.")
        
        print(f"Connecting to database: {db_path}...")
        conn = sqlite3.connect(db_path)
        
        print("Writing data to 'stocks' table...")
        # if_exists='replace' ensures we start fresh with the full CSV data
        df.to_sql('stocks', conn, if_exists='replace', index=False)
        
        # Verify
        cursor = conn.cursor()
        cursor.execute("SELECT count(*) FROM stocks")
        count = cursor.fetchone()[0]
        print(f"Successfully migrated {count} records to database.")
        
        # Create indexes for performance
        print("Creating indexes...")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_date ON stocks (Date)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_sc_code ON stocks ('SC_CODE')")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_sc_name ON stocks ('SC_NAME')")
        
        conn.commit()
        conn.close()
        print("Migration complete.")
        
    except Exception as e:
        print(f"Migration failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    if not os.path.exists(STOCK_DATA_DIR):
        print(f"Directory {STOCK_DATA_DIR} does not exist.")
    else:
        migrate_to_db()
