import pandas as pd
import os
import sys

pkl_path = "StockData/merged_stock_data.pkl"

print(f"Python Info: {sys.version}")
print(f"Pandas Version: {pd.__version__}")

if os.path.exists(pkl_path):
    size = os.path.getsize(pkl_path)
    print(f"File found: {pkl_path}")
    print(f"File size: {size} bytes")
    
    if size == 0:
        print("ERROR: File is empty.")
    else:
        try:
            print("Attempting to read PKL file...")
            df = pd.read_pickle(pkl_path)
            print("Successfully read PKL file.")
            print(df.head())
        except Exception as e:
            print("\n!!! Error Reading PKL File !!!")
            print(f"Error Type: {type(e).__name__}")
            print(f"Error Message: {e}")
            import traceback
            traceback.print_exc()
else:
    print(f"ERROR: File not found at {pkl_path}")
