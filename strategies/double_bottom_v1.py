import pandas as pd
from database import get_db_connection
import numpy as np
from scipy.signal import argrelextrema

default_params = {
    "min_data": 40,
    "extrema_order": 5,
    "min_bottom_gap": 12,
    "pre_drop_lookback": 20,
    "min_pre_days": 5,
    "min_prior_drop": 0.12,              # 12%
    "reversal_window": 10,
    "reversal_tolerance": 0.03,          # 3%
    "max_linger_days": 6,
    "min_peak_spacing": 4,
    "peak_separation": 3,
    "min_peak_drop_from_prior": 0.05,    # 5%
    "bottom_tolerance": 0.025,           # 2.5%
    "min_peak_height": 0.06,             # 6%
    "no_breakout_tolerance": 0.005,      # 0.5%
    "resistance_buffer": 0.015,          # 1.5%
    "volume_multiplier": 1.5
}

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
        if np.sum(prices[b1:b1 + 10] < b1_price * 1.03) > 6:
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
        if np.sum(prices[b2:b2 + 10] < b2_price * 1.03) > 6:
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
                'Prominence_Pct': round(10.2, 2)
            }

    return None
def detect_double_bottom_strict(stock_data, params):
    prices = stock_data['CLOSE'].to_numpy()
    volumes = stock_data['VOLUME'].to_numpy()
    dates = stock_data['ParsedDate'].to_numpy()

    if len(prices) < params['min_data']:
        return None

    # Ensure sorted
    sort_idx = np.argsort(dates)
    prices = prices[sort_idx]
    volumes = volumes[sort_idx]
    dates = dates[sort_idx]

    local_min = argrelextrema(prices, np.less, order=params['extrema_order'])[0]
    local_max = argrelextrema(prices, np.greater, order=params['extrema_order'])[0]

    if local_min.size < 2 or local_max.size == 0:
        return None

    results = []
    prominence_pct = 0

    for i in range(len(local_min) - 1):
        b1, b2 = local_min[i], local_min[i + 1]

        # --- spacing between bottoms ---
        if b2 - b1 < params['min_bottom_gap']:
            continue

        # --- prior drop ---
        pre_start = max(0, b1 - params['pre_drop_lookback'])
        if b1 - pre_start < params['min_pre_days']:
            continue

        pre_max = prices[pre_start:b1].max()
        b1_price = prices[b1]

        if (pre_max - b1_price) / pre_max < params['min_prior_drop']:
            continue

        # --- clean reversal B1 ---
        if np.sum(prices[b1:b1+params['reversal_window']] <
                  b1_price * (1 + params['reversal_tolerance'])) > params['max_linger_days']:
            continue

        # --- peaks between ---
        peaks = local_max[(local_max > b1) & (local_max < b2)]
        if peaks.size == 0:
            continue

        p = peaks[np.argmax(prices[peaks])]
        p_price = prices[p]

        # --- single clean peak rule ---
        other_peaks = peaks[np.abs(peaks - p) > params['peak_separation']]
        if other_peaks.size > 0:
            continue

        # --- spacing B1-peak-B2 ---
        if (p - b1) < params['min_peak_spacing'] or (b2 - p) < params['min_peak_spacing']:
            continue

        # --- peak vs prior high ---
        if (pre_max - p_price) / pre_max < params['min_peak_drop_from_prior']:
            continue

        b2_price = prices[b2]

        # --- bottom similarity ---
        if abs(b1_price - b2_price) / max(b1_price, b2_price) > params['bottom_tolerance']:
            continue

        # --- peak height ---
        if (p_price - max(b1_price, b2_price)) / max(b1_price, b2_price) < params['min_peak_height']:
            prominence_pct = (
                                     (p_price - max(b1_price, b2_price))
                                     / max(b1_price, b2_price)
                             ) * 1
            continue

        # --- clean reversal B2 ---
        if np.sum(prices[b2:b2+params['reversal_window']] <
                  b2_price * (1 + params['reversal_tolerance'])) > params['max_linger_days']:
            continue

        # --- breakout attempt ---
        if np.any(prices[b2:-1] >= p_price * (1 - params['no_breakout_tolerance'])):
            continue

        curr = prices[-1]

        if curr > prices[-2] and curr >= p_price * (1 - params['resistance_buffer']):

            # --- Volume confirmation ---
            avg_vol = volumes[p-5:p].mean()
            breakout_vol = volumes[-1]

            if breakout_vol < avg_vol * params['volume_multiplier']:
                continue

            results.append({
                'SC_CODE': stock_data['SC_CODE'].iloc[0],
                'SC_NAME': stock_data['SC_NAME'].iloc[0],
                'Bottom1_Date': dates[b1],
                'Bottom1_Price': b1_price,
                'Bottom2_Date': dates[b2],
                'Bottom2_Price': b2_price,
                'Neckline_Price': p_price,
                'Prominence_Pct': prominence_pct,
                "Breakout_Price": curr
            })

    return results[-1] if results else None

def get_double_bottom_stocks(params=None):
    if params is None:
        params = default_params

    conn = get_db_connection()

    df = pd.read_sql_query("""
                           SELECT Date as DATE, SC_CODE, SC_NAME, CLOSE, "DAY'S VOLUME" as VOLUME
                           FROM stocks
                           WHERE SC_GROUP IN ('A', 'B', 'X', 'T')
                           ORDER BY SC_CODE, Date
                           """, conn)

    conn.close()

    df = preprocess_df(df)

    results = []
    for _, stock_df in df.groupby('SC_CODE'):
        res = detect_double_bottom_strict(stock_df, params)
        if res:
            results.append(res)
    results = pd.DataFrame(results)
    return results
