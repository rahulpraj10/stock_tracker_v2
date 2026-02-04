from flask import Flask, render_template, request
import pandas as pd
import requests
import io

app = Flask(__name__)

DATA_URL = "https://github.com/rahulpraj10/stock_tracker_v2/raw/main/StockData/merged_stock_data.pkl"

def get_data():
    try:
        response = requests.get(DATA_URL)
        response.raise_for_status()
        # Read the pickle data from bytes
        df = pd.read_pickle(io.BytesIO(response.content))
        return df
    except Exception as e:
        print(f"Error loading data: {e}")
        return pd.DataFrame()

@app.route('/', methods=['GET', 'POST'])
def index():
    df = get_data()
    
    # Filter parameters
    sc_code_filter = request.args.get('sc_code', '').strip()
    sc_name_filter = request.args.get('sc_name', '').strip()
    
    # Apply filters if data is available and filters are provided
    if not df.empty:
        if sc_code_filter:
            # Convert column to string for flexible searching, handle potential non-string types
            df = df[df['SC_CODE'].astype(str).str.contains(sc_code_filter, case=False, na=False)]
        
        if sc_name_filter:
            df = df[df['SC_NAME'].str.contains(sc_name_filter, case=False, na=False)]

    # Convert to dictionary for rendering (list of records)
    data = df.to_dict(orient='records')
    columns = df.columns.tolist() if not df.empty else []

    return render_template('index.html', 
                         data=data, 
                         columns=columns,
                         sc_code=sc_code_filter,
                         sc_name=sc_name_filter)

if __name__ == '__main__':
    app.run(debug=True)
