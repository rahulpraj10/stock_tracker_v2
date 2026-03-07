import pandas as pd
from database import get_db_connection
import numpy as np

def get_multi_frame():
    conn = get_db_connection()
    if not conn:
        return []

    try:
        # 1. Get distinct dates for lookback period
        cursor = conn.cursor()
        lookback_days = 90
        date_query = "SELECT DISTINCT Date FROM stocks ORDER BY Date DESC LIMIT ?"
        cursor.execute(date_query, (lookback_days,))
        dates = [row[0] for row in cursor.fetchall()]

        if len(dates) < 60:  # Need enough history
            conn.close()
            return []

        placeholders = ','.join(['?'] * len(dates))
        query = f"""
                SELECT SC_GROUP, SC_CODE, SC_NAME, Date, Date as ParsedDate, CLOSE, HIGH, LOW
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

    print('Unique Dates: ',df.Date.nunique())
    growth_analysis_df = df[
        df['SC_CODE'].map(df['SC_CODE'].value_counts()) > 50]

    #print(growth_analysis_df[['SC_CODE','Date']].head(4))

    # 2. Parse the DATE column into actual datetime objects
    # Format is DDMMYYYY, so we pad to 8 characters
    growth_analysis_df['ParsedDate'] = pd.to_datetime(
        growth_analysis_df['ParsedDate']
    )

    #print(growth_analysis_df[['SC_CODE', 'Date','ParsedDate']].head(4))

    # 3. Sort by SC_CODE and ParsedDate for chronological order per stock
    # growth_analysis_df = growth_analysis_df.sort_values(by=['SC_CODE', 'ParsedDate'])
    growth_analysis_df = growth_analysis_df[(growth_analysis_df.SC_GROUP.isin(['A', 'B', 'X', 'T']))].sort_values(
        by=['SC_CODE', 'ParsedDate'])

    # 4. Convert CLOSE, HIGH, and LOW to numeric types
    numeric_cols = ['CLOSE', 'HIGH', 'LOW']
    for col in numeric_cols:
        growth_analysis_df[col] = pd.to_numeric(growth_analysis_df[col], errors='coerce')

    latest_date = growth_analysis_df['ParsedDate'].max()
    windows = [7, 30, 60, 90]
    growth_results = []
    segments = 5

    for (code, name), group in growth_analysis_df.groupby(['SC_CODE', 'SC_NAME']):
        stock_record = {'SC_CODE': code, 'SC_NAME': name}

        for window in windows:
            cutoff = latest_date - pd.Timedelta(days=window)
            window_data = group[group['ParsedDate'] >= cutoff]

            # Use our new 3-segment logic (default segments=3)
            if segments > 5:
                window_segments = {7: 2, 30: 4, 60: 7, 90: 10}
                trend_segment = window_segments[window]
            else:
                trend_segment = 3
            stock_record[f'growth_{window}d'] = check_hh_hl(window_data['CLOSE'], trend_segment)

        # get the latest price for that stock
        latest_price = group['CLOSE'].iloc[-1]
        stock_record['latest_price'] = latest_price
        stock_record['latest_date'] = latest_date.date()
        stock_record['Close'] = latest_price
        growth_results.append(stock_record)

    growth_signals_df = pd.DataFrame(growth_results)
    #print(growth_signals_df.head(10))
    consistent_growers = growth_signals_df[
        (growth_signals_df['growth_7d'] == True) &
        (growth_signals_df['growth_30d'] == True) &
        (growth_signals_df['growth_60d'] == True) &
        (growth_signals_df['growth_90d'] == True)
        ]

    print(consistent_growers.head(10))
    consistent_growers1 = consistent_growers.to_dict(orient='records')
    return consistent_growers1

def check_hh_hl(prices, trend_segment=3):
    """
    Detects 'Higher High Higher Low' (HHHL) patterns by splitting a Series
    into multiple segments and ensuring each subsequent segment
    shows growth over the previous one.
    """
    # 1. Handle edge cases
    if prices is None or len(prices) < trend_segment:
        return False

    # 2. Split the input series into N chronological segments
    # array_split handles cases where length isn't perfectly divisible
    chunks = np.array_split(prices, trend_segment)

    # 3. Calculate extremes for each segment
    extremes = []
    for chunk in chunks:
        # Fix: Check length instead of using .empty for numpy arrays
        if len(chunk) == 0: return False
        extremes.append((chunk.max(), chunk.min()))

    # 4. Logic: Every segment must have a Higher High and Higher Low than the previous
    for i in range(1, len(extremes)):
        curr_h, curr_l = extremes[i]
        prev_h, prev_l = extremes[i - 1]

        if not (curr_h > prev_h and curr_l > prev_l):
            return False

    return True