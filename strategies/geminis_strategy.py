import pandas as pd
from database import get_db_connection
import numpy as np

def get_geminis_strategy_stocks():
    """
    Identifies potentially bullish stocks from groups A, B, X, T based on:
    - Close > Open (Green Candle) or Close > Prev Close
    - High Volume Spike (> 1.5x of 5-day Avg)
    - High Delivery % (> 60%)
    - Sorts by Delivery Value (Close * Volume * Del%)
    """
    conn = get_db_connection()
    if not conn:
        return []

    try:
        # Get last 7 days of data to calculate moving averages
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT Date FROM stocks ORDER BY Date DESC LIMIT 7")
        dates_result = cursor.fetchall()
        
        if len(dates_result) < 5:
            conn.close()
            return []
            
        dates = [row[0] for row in dates_result]
        placeholders = ','.join(['?'] * len(dates))
        
        # Use simple string formatting since sqlite3 placeholders are quirky with lists
        # We'll rely on pandas read_sql to handle parameter binding safely
        query = f"""
            SELECT SC_CODE, SC_NAME, SC_GROUP, Date, "OPEN", "CLOSE", "DAY'S VOLUME", "DELV. PER."
            FROM stocks
            WHERE Date IN ({placeholders})
            AND SC_GROUP IN ('A', 'B', 'X', 'T')
            ORDER BY SC_CODE, Date ASC
        """
        
        df = pd.read_sql_query(query, conn, params=dates)
        
    except Exception as e:
        print(f"Error in Geminis Strategy: {e}")
        conn.close()
        return []
    
    conn.close()

    if df.empty:
        return []

    # --- Data Cleaning ---
    if 'Date' in df.columns:
        df['Date'] = pd.to_datetime(df['Date'])
    
    # Numeric conversions
    numeric_cols = ['OPEN', 'CLOSE', "DAY'S VOLUME"]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Clean Delivery Percentage (remove '%' if string)
    if "DELV. PER." in df.columns and df["DELV. PER."].dtype == object:
        df["DELV. PER."] = df["DELV. PER."].astype(str).str.replace('%', '', regex=False)
    
    df["DELV. PER."] = pd.to_numeric(df["DELV. PER."], errors='coerce')

    results = []

    # Process each stock
    for sc_code, group in df.groupby('SC_CODE'):
        if len(group) < 5:
            continue
        
        # Ensure sorted chronologically
        group = group.sort_values(by='Date')
        
        # Calculate 5-day Avg Volume (including today for simplicity of rolling, or window logic)
        group['Vol_MA_5'] = group["DAY'S VOLUME"].rolling(window=5).mean()
        
        # Get Latest Record (Today)
        today = group.iloc[-1]
        
        # --- Strategy Logic ---
        
        # 0. Basic Sanity Check
        if pd.isna(today['CLOSE']) or pd.isna(today['OPEN']):
            continue

        # 1. Bullish Candle or Up Move
        is_green_candle = today['CLOSE'] > today['OPEN']
        
        # Get prev close if available (second to last row)
        prev_close = group.iloc[-2]['CLOSE'] if len(group) >= 2 else today['OPEN'] 
        is_up_move = today['CLOSE'] > prev_close
        
        if not (is_green_candle or is_up_move):
            continue

        # 2. Volume Spike
        # Volume > 1.5x of 5-day Average
        if pd.isna(today['Vol_MA_5']) or today["DAY'S VOLUME"] < (1.5 * today['Vol_MA_5']):
            continue

        # 3. High Delivery %
        # Default > 60%
        if pd.isna(today["DELV. PER."]) or today["DELV. PER."] < 60:
            continue

        # 4. Delivery Value Calculation
        # Del Value = Close * Volume * (Del % / 100)
        del_value = today['CLOSE'] * today["DAY'S VOLUME"] * (today["DELV. PER."] / 100)
        
        results.append({
            'SC_CODE': sc_code,
            'SC_NAME': today['SC_NAME'],
            'SC_GROUP': today['SC_GROUP'],
            'Date': today['Date'].strftime('%Y-%m-%d'),
            'Close': round(today['CLOSE'], 2),
            'Volume': int(today["DAY'S VOLUME"]),
            'Delv_Per': round(today["DELV. PER."], 2),
            'Delv_Value': round(del_value, 2)
        })

    # Convert to DataFrame to sort
    if results:
        results_df = pd.DataFrame(results)
        # Sort by Delivery Value Descending (highest value first)
        results_df = results_df.sort_values(by='Delv_Value', ascending=False)
        
        return results_df.to_dict('records')
        
    return []
