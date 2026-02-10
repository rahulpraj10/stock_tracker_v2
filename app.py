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

    # Pagination
    page = request.args.get('page', 1, type=int)
    per_page = 18
    total_records = len(df)
    total_pages = (total_records + per_page - 1) // per_page
    
    # Ensure page is within valid range
    page = max(1, min(page, total_pages)) if total_pages > 0 else 1
    
    start_idx = (page - 1) * per_page
    end_idx = start_idx + per_page
    
    # Slice the dataframe for current page
    df_page = df.iloc[start_idx:end_idx]

    # Convert to dictionary for rendering (list of records)
    data = df_page.to_dict(orient='records')
    columns = df.columns.tolist() if not df.empty else []

    return render_template('index.html', 
                         data=data, 
                         columns=columns,
                         sc_code=sc_code_filter,
                         sc_name=sc_name_filter,
                         page=page,
                         total_pages=total_pages,
                         total_records=total_records)

def get_min_increase_stocks(days):
    df = get_data()
    if df.empty:
        return []
    
    # Ensure Date is datetime
    if 'Date' in df.columns:
        df['Date'] = pd.to_datetime(df['Date'])
    else:
        # If 'Date' not found, try to use 'DATE' column if available and convert format
        # Based on CSV header: DATE (int e.g. 4022026 -> 04-02-2026), Date (YYYY-MM-DD)
        # We prefer 'Date' column which seems to be added during accumulation
        pass

    # Sort by SC_CODE and Date
    df = df.sort_values(by=['SC_CODE', 'Date'])
    
    results = []
    
    # Group by SC_CODE
    for sc_code, group in df.groupby('SC_CODE'):
        if len(group) < days + 1:
            continue
        
        # Get last n+1 records to compare n periods of increase
        recent_data = group.tail(days + 1)
        volumes = recent_data["DAY'S VOLUME"].tolist()
        
        # Check if strictly increasing
        is_increasing = True
        for i in range(len(volumes) - 1):
            if volumes[i+1] <= volumes[i]:
                is_increasing = False
                break
        
        if is_increasing:
            # Get latest SC_NAME
            sc_name = group.iloc[-1]['SC_NAME']
            results.append({'SC_CODE': sc_code, 'SC_NAME': sc_name, 'Volumes': volumes})
            
    return results

@app.route('/strategies', methods=['GET', 'POST'])
def strategies():
    selected_strategy = request.args.get('strategy')
    strategy_results = []
    days = 5 # Default
    
    if selected_strategy == 'min_increase':
        try:
            days = int(request.args.get('days', 5))
            strategy_results = get_min_increase_stocks(days)
        except ValueError:
            pass # Handle invalid input gracefully

    return render_template('strategies.html', 
                         strategy=selected_strategy, 
                         results=strategy_results,
                         days=days)

if __name__ == '__main__':
    app.run(debug=True)
