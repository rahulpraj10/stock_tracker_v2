import pandas as pd
from database import get_db_connection

def get_min_increase_stocks(days):
    conn = get_db_connection()
    if not conn:
        return []

    try:
        # Optimization: Instead of loading all data, fetch only recent data.
        # 1. Get the last N+1 distinct dates from the database.
        cursor = conn.cursor()
        date_query = "SELECT DISTINCT Date FROM stocks ORDER BY Date DESC LIMIT ?"
        cursor.execute(date_query, (days + 1,))
        dates = [row[0] for row in cursor.fetchall()]
        
        if len(dates) < days + 1:
            conn.close()
            return []
            
        # 2. Fetch data only for these dates
        placeholders = ','.join(['?'] * len(dates))
        query = f"SELECT SC_CODE, SC_NAME, Date, \"DAY'S VOLUME\" FROM stocks WHERE Date IN ({placeholders}) ORDER BY SC_CODE, Date"
        
        df = pd.read_sql_query(query, conn, params=dates)
        
        # Ensure Date is datetime for sorting if pandas didn't convert
        if 'Date' in df.columns:
             df['Date'] = pd.to_datetime(df['Date'])
             
    except Exception as e:
        print(f"Error in strategy: {e}")
        conn.close()
        return []
        
    conn.close()
    
    # Now continue with the Pandas logic on this smaller subset
    # Sort by SC_CODE and Date (already sorted by SQL but good to verify)
    df = df.sort_values(by=['SC_CODE', 'Date'])
    
    results = []
    
    # Group by SC_CODE
    for sc_code, group in df.groupby('SC_CODE'):
        if len(group) < days + 1:
            continue
        
        # Get last n+1 records to compare n periods of increase
        recent_data = group.tail(days + 1)
        volumes = recent_data["DAY'S VOLUME"].tolist()
        
        # Check if strictly increasing
        is_increasing = True
        for i in range(len(volumes) - 1):
            if volumes[i+1] <= volumes[i]:
                is_increasing = False
                break
        
        if is_increasing:
            # Get latest SC_NAME
            sc_name = group.iloc[-1]['SC_NAME']
            results.append({'SC_CODE': sc_code, 'SC_NAME': sc_name, 'Volumes': volumes})
            
    return results
