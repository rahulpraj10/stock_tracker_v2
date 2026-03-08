import pandas as pd
import numpy as np
from database import get_db_connection


def get_multi_frame():

    conn = get_db_connection()
    if not conn:
        return []

    lookback_days = 90

    # Fetch data
    query = f"""
        SELECT SC_GROUP, SC_CODE, SC_NAME, Date as ParsedDate, CLOSE, HIGH, LOW
        FROM stocks
        ORDER BY SC_CODE, Date ASC
        LIMIT 500000
    """

    df = pd.read_sql_query(query, conn)
    conn.close()

    df = df[df.SC_GROUP.isin(['A','B','X','T'])].copy()

    df['ParsedDate'] = pd.to_datetime(df['ParsedDate'])
    df[['CLOSE','HIGH','LOW']] = df[['CLOSE','HIGH','LOW']].apply(
        pd.to_numeric, errors='coerce'
    )

    df = df.sort_values(['SC_CODE','ParsedDate'])

    latest_date = df['ParsedDate'].max()

    windows = [7,30,60,90]

    results = []

    grouped = df.groupby(['SC_CODE','SC_NAME'])

    for (code,name), group in grouped:

        if len(group) < 50:
            continue

        stock_record = {
            'SC_CODE': code,
            'SC_NAME': name
        }

        prices = group['CLOSE'].values
        dates = group['ParsedDate'].values
        segments = 5

        for window in windows:

            cutoff = latest_date - pd.Timedelta(days=window)

            mask = dates >= np.datetime64(cutoff)

            window_prices = prices[mask]

            # Use our new 3-segment logic (default segments=3)
            if segments > 5:
                window_segments = {7: 2, 30: 4, 60: 7, 90: 10}
                trend_segment = window_segments[window]
            else:
                trend_segment = 3

            stock_record[f'growth_{window}d'] = check_hh_hl_fast(window_prices, trend_segment)

        stock_record['latest_price'] = prices[-1]
        stock_record['latest_date'] = latest_date.date()
        stock_record['Close'] = prices[-1]

        results.append(stock_record)

    growth_df = pd.DataFrame(results)

    consistent = growth_df[
        (growth_df.growth_7d) &
        (growth_df.growth_30d) &
        (growth_df.growth_60d) &
        (growth_df.growth_90d)
    ]

    return consistent.to_dict(orient='records')


def check_hh_hl_fast(prices, segments=3):

    if len(prices) < segments:
        return False

    chunks = np.array_split(prices, segments)

    highs = [c.max() for c in chunks]
    lows = [c.min() for c in chunks]

    highs = np.array(highs)
    lows = np.array(lows)

    return np.all(np.diff(highs) > 0) and np.all(np.diff(lows) > 0)