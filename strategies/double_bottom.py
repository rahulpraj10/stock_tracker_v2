import pandas as pd
from database import get_db_connection
import numpy as np

def get_double_bottom_stocks(min_days=10, max_days=60, tolerance_pct=3.0, lookback_days=90, peak_prominence_pct=5.0):
    conn = get_db_connection()
    if not conn:
        return []

    try:
        # 1. Get distinct dates for lookback period
        cursor = conn.cursor()
        date_query = "SELECT DISTINCT Date FROM stocks ORDER BY Date DESC LIMIT ?"
        cursor.execute(date_query, (lookback_days,))
        dates = [row[0] for row in cursor.fetchall()]
        
        if len(dates) < max_days: # Need enough history
            conn.close()
            return []
            
        placeholders = ','.join(['?'] * len(dates))
        query = f"""
            SELECT SC_CODE, SC_NAME, Date, "CLOSE"
            FROM stocks 
            WHERE Date IN ({placeholders}) 
            ORDER BY SC_CODE, Date ASC
        """
        
        df = pd.read_sql_query(query, conn, params=dates)
        
    except Exception as e:
        print(f"Error in double bottom strategy: {e}")
        conn.close()
        return []
        
    conn.close()

    # Data Processing
    if 'Date' in df.columns:
        df['Date'] = pd.to_datetime(df['Date'])
    
    df['CLOSE'] = pd.to_numeric(df['CLOSE'], errors='coerce')

    results = []

    for sc_code, group in df.groupby('SC_CODE'):
        if len(group) < max_days:
            continue
            
        group = group.sort_values('Date').reset_index(drop=True)
        closes = group['CLOSE'].values
        dates_arr = group['Date'].values
        
        # Find local minima manually to avoid scipy dependency
        # Check 3 neighbors on each side
        minima_indices = []
        order = 3
        
        for i in range(order, len(closes) - order):
            window = closes[i-order:i+order+1]
            if np.argmin(window) == order:
               # Also ensuring it's not just a flat bottom sequence where the middle isn't strictly smaller
               # But for simplicity, we accept if it is the minimum in the window
               minima_indices.append(i)
        
        if len(minima_indices) < 2:
            continue
            
        # Check pairs of minima
        # Iterate backwards to find most recent pattern
        found_pattern = False
        for i in range(len(minima_indices) - 1, 0, -1):
            if found_pattern: break
            
            idx2 = minima_indices[i]
            
            for j in range(i - 1, -1, -1):
                idx1 = minima_indices[j]
                
                date1 = pd.Timestamp(dates_arr[idx1])
                date2 = pd.Timestamp(dates_arr[idx2])
                price1 = closes[idx1]
                price2 = closes[idx2]
                
                # Check Time Distance
                days_diff = (date2 - date1).days
                if days_diff < min_days:
                    continue # Too close
                if days_diff > max_days:
                    continue # Too far
                    
                # Check Price Tolerance
                price_diff_pct = abs(price1 - price2) / ((price1 + price2) / 2) * 100
                if price_diff_pct > tolerance_pct:
                    continue
                    
                # Check Peak in between
                # Slice data between the two bottoms
                intermediate_slice = closes[idx1+1:idx2]
                if len(intermediate_slice) == 0: continue
                
                peak_price = np.max(intermediate_slice)
                min_bottom = min(price1, price2)
                
                # Check Peak Prominence
                prominence = (peak_price - min_bottom) / min_bottom * 100
                if prominence < peak_prominence_pct:
                    continue # Not deep enough "W"
                    
                # Optional: Check if current price is supportive
                # (e.g., not crashed way below bottom 2)
                current_price = closes[-1]
                if current_price < min_bottom * 0.95:
                    continue # Pattern invalidated by further drop
                
                results.append({
                    'SC_CODE': sc_code,
                    'SC_NAME': group.iloc[-1]['SC_NAME'],
                    'Bottom1_Date': pd.to_datetime(date1).strftime('%Y-%m-%d'),
                    'Bottom1_Price': round(price1, 2),
                    'Bottom2_Date': pd.to_datetime(date2).strftime('%Y-%m-%d'),
                    'Bottom2_Price': round(price2, 2),
                    'Neckline_Price': round(peak_price, 2),
                    'Prominence_Pct': round(prominence, 2)
                })
                found_pattern = True
                break
        
    return sorted(results, key=lambda x: x['Bottom2_Date'], reverse=True)
