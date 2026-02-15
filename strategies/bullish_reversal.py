import pandas as pd
from database import get_db_connection

def get_bullish_reversal_stocks():
    conn = get_db_connection()
    if not conn:
        return []

    try:
        # 1. Get last 7 distinct dates
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT Date FROM stocks ORDER BY Date DESC LIMIT 7")
        dates = [row[0] for row in cursor.fetchall()]
        
        if len(dates) < 5: 
            conn.close()
            return []
            
        placeholders = ','.join(['?'] * len(dates))
        query = f"""
            SELECT SC_CODE, SC_NAME, Date, "CLOSE", "DAY'S VOLUME", "DELV. PER." 
            FROM stocks 
            WHERE Date IN ({placeholders}) 
            ORDER BY SC_CODE, Date ASC
        """
        
        df = pd.read_sql_query(query, conn, params=dates)
        
    except Exception as e:
        print(f"Error in bullish reversal strategy: {e}")
        conn.close()
        return []
        
    conn.close()

    # Data Processing
    if 'Date' in df.columns:
        df['Date'] = pd.to_datetime(df['Date'])
    
    # Clean numeric columns
    df['CLOSE'] = pd.to_numeric(df['CLOSE'], errors='coerce')
    df["DAY'S VOLUME"] = pd.to_numeric(df["DAY'S VOLUME"], errors='coerce')
    # Remove % if present and convert
    if df["DELV. PER."].dtype == object:
        df["DELV. PER."] = df["DELV. PER."].astype(str).str.replace('%', '', regex=False)
    df["DELV. PER."] = pd.to_numeric(df["DELV. PER."], errors='coerce')

    results = []

    for sc_code, group in df.groupby('SC_CODE'):
        if len(group) < 5:
            continue
            
        group = group.sort_values('Date')
        
        # Calculate Indicators
        group['Price_Change'] = group['CLOSE'].diff()
        group['Vol_MA_5'] = group["DAY'S VOLUME"].rolling(window=5).mean()
        
        # Get Latest Record (Today)
        today = group.iloc[-1]
        
        # Check Conditions
        # 1. Price Increase Today
        if pd.isna(today['Price_Change']) or today['Price_Change'] <= 0:
            continue
            
        # 2. Period of Decline (Sum of price changes for prev 3 days)
        # We need at least 4 records (3 prev + 1 today)
        if len(group) < 4:
            continue
            
        prev_3_days_change = group['Price_Change'].iloc[-4:-1].sum()
        
        if prev_3_days_change >= 0: # Must be negative (decline)
            continue
            
        # 3. Volume Spike
        if pd.isna(today['Vol_MA_5']) or today["DAY'S VOLUME"] <= today['Vol_MA_5']:
            continue
            
        # 4. High Delivery
        if pd.isna(today["DELV. PER."]) or today["DELV. PER."] <= 50:
            continue
            
        results.append({
            'SC_CODE': sc_code,
            'SC_NAME': today['SC_NAME'],
            'Date': today['Date'].strftime('%Y-%m-%d'),
            'Close': today['CLOSE'],
            'Volume': int(today["DAY'S VOLUME"]),
            'Delv_Per': today["DELV. PER."]
        })
        
    return results
