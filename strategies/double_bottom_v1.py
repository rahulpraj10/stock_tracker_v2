import pandas as pd
from database import get_db_connection
import numpy as np
from scipy.signal import argrelextrema

def preprocess_df(df):
    if 'ParsedDate' not in df.columns:
        df['ParsedDate'] = pd.to_datetime(df['DATE'], errors='coerce')
    return df.sort_values(['SC_CODE', 'ParsedDate'])


def detect_double_bottom_fast(stock_data):
    prices = stock_data['CLOSE'].to_numpy()
    dates = stock_data['ParsedDate'].to_numpy()

    if len(prices) < 40:
        return None

    local_min = argrelextrema(prices, np.less, order=5)[0]
    local_max = argrelextrema(prices, np.greater, order=5)[0]

    if local_min.size < 2 or local_max.size == 0:
        return None

    for i in range(len(local_min) - 1):
        b1, b2 = local_min[i], local_min[i + 1]

        if b2 - b1 < 10:
            continue

        # Pre-drop
        pre_start = max(0, b1 - 20)
        if b1 - pre_start < 5:
            continue

        pre_max = prices[pre_start:b1].max()
        b1_price = prices[b1]

        if (pre_max - b1_price) / pre_max < 0.10:
            continue

        # Clean reversal B1
        if np.sum(prices[b1:b1+10] < b1_price * 1.03) > 6:
            continue

        # Peaks between
        peaks = local_max[(local_max > b1) & (local_max < b2)]
        if peaks.size == 0:
            continue

        p = peaks[np.argmax(prices[peaks])]
        p_price = prices[p]

        if ((p - b1) < 4) or ((b2 - p) < 4):
            continue

        b2_price = prices[b2]

        if abs(b1_price - b2_price) / max(b1_price, b2_price) > 0.02:
            continue

        if (p_price - max(b1_price, b2_price)) / max(b1_price, b2_price) < 0.04:
            continue

        # Clean reversal B2
        if np.sum(prices[b2:b2+10] < b2_price * 1.03) > 6:
            continue

        prices_after = prices[b2:-1]
        if np.any(prices_after >= p_price * 0.995):
            continue

        curr = prices[-1]
        if curr > prices[-2] and curr >= p_price * 0.985:
            return {
                'SC_CODE': stock_data['SC_CODE'].iloc[0],
                'SC_NAME': stock_data['SC_NAME'].iloc[0],
                'Bottom1_Date': dates[b1],
                'Bottom1_Price': b1_price,
                'Bottom2_Date': dates[b2],
                'Bottom2_Price': b2_price,
                'Neckline_Price': p_price,
                'Prominence_Pct': round(10.2,2)
            }

    return None


def get_double_bottom_stocks():
    conn = get_db_connection()

    df = pd.read_sql_query("""
        SELECT Date as DATE, SC_CODE, SC_NAME, CLOSE
        FROM stocks
        WHERE SC_GROUP IN ('A','B','X','T')
        ORDER BY SC_CODE, Date
    """, conn)

    conn.close()

    df = preprocess_df(df)

    results = []
    for _, stock_df in df.groupby('SC_CODE'):
        res = detect_double_bottom_fast(stock_df)
        if res:
            results.append(res)
    results = pd.DataFrame(results)
    # print(results.shape)
    # print(results.head())
    return results
