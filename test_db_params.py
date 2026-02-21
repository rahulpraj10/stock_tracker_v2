from strategies.double_bottom_v1 import get_double_bottom_stocks, default_params
import pandas as pd

if __name__ == '__main__':
    print("Testing with default parameters...")
    df_default = get_double_bottom_stocks()
    print(f"Results with defaults: {len(df_default) if not df_default.empty else 0}")
    
    print("\nTesting with custom strict parameters (min_data=100)...")
    custom_params = default_params.copy()
    custom_params['min_data'] = 100
    df_custom = get_double_bottom_stocks(params=custom_params)
    print(f"Results with custom params: {len(df_custom) if not df_custom.empty else 0}")
    
    print("\nVerification successful if script completes.")
