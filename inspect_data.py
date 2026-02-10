import pandas as pd
import io
import requests

DATA_URL = "https://github.com/rahulpraj10/stock_tracker_v2/raw/main/StockData/merged_stock_data.pkl"

try:
    response = requests.get(DATA_URL)
    response.raise_for_status()
    df = pd.read_pickle(io.BytesIO(response.content))
    print("Columns:", df.columns.tolist())
    print("First few rows:\n", df.head())
    print("Data types:\n", df.dtypes)
except Exception as e:
    print(f"Error: {e}")
