from flask import Flask, render_template, request, redirect, url_for, flash, session
from flask_login import LoginManager, UserMixin, login_user, login_required, logout_user, current_user
import pandas as pd
import requests
import io
from datetime import timedelta
import secrets

app = Flask(__name__)
app.secret_key = 'your_secret_key_here_change_in_production' # For development
app.permanent_session_lifetime = timedelta(minutes=2)

# Flask-Login Configuration
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login'

# Mock Database
USERS = {
    'rahul': {'password': 'rahul123'},
    'snehashish': {'password': 'sneh123'}
}

class User(UserMixin):
    def __init__(self, id):
        self.id = id

@login_manager.user_loader
def load_user(user_id):
    if user_id in USERS:
        return User(user_id)
    return None

@app.before_request
def make_session_permanent():
    session.permanent = True
    app.permanent_session_lifetime = timedelta(minutes=2)

# DATA_URL = "https://github.com/rahulpraj10/stock_tracker_v2/raw/main/StockData/merged_stock_data.pkl"
DB_URL = "https://github.com/rahulpraj10/stock_tracker_v2/raw/main/StockData/stock_data.db"
import sqlite3
import os

DB_PATH = os.path.join("StockData", "stock_data.db")
if not os.path.exists("StockData"):
    os.makedirs("StockData")

def get_data():
    try:
        # Check if DB needs to be downloaded (e.g. if it doesn't exist)
        # For a read-only viewer on Render, we might want to download it on startup
        # But here we do it lazily if missing. 
        # Ideally, we should check for updates, but for now let's ensure presence.
        if not os.path.exists(DB_PATH):
            print(f"Downloading database from {DB_URL}...")
            response = requests.get(DB_URL)
            response.raise_for_status()
            with open(DB_PATH, 'wb') as f:
                f.write(response.content)
            print("Database downloaded.")
            
        conn = sqlite3.connect(DB_PATH)
        # Assuming table name is 'stocks'. If unknown, we could query sqlite_master
        # But hardcoding is faster if known.
        query = "SELECT * FROM stocks"
        
        # Check if table exists first to avoid error if DB is empty/different
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='stocks';")
        if not cursor.fetchone():
             # Fallback or error
             print("Table 'stocks' not found in database.")
             conn.close()
             return pd.DataFrame()
             
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df
    except Exception as e:
        print(f"Error loading data: {e}")
        return pd.DataFrame()

@app.route('/login', methods=['GET', 'POST'])
def login():
    if current_user.is_authenticated:
        return redirect(url_for('index'))
        
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        
        if username in USERS and USERS[username]['password'] == password:
            user = User(username)
            login_user(user)
            return redirect(url_for('index'))
        else:
            flash('Invalid username or password', 'error')
            
    return render_template('login.html')

@app.route('/logout')
@login_required
def logout():
    logout_user()
    return redirect(url_for('login'))

@app.route('/', methods=['GET', 'POST'])
@login_required
def index():
    df = get_data()
    
    # Filter parameters
    sc_code_filter = request.args.get('sc_code', '').strip()
    sc_name_filter = request.args.get('sc_name', '').strip()
    sc_group_filter = request.args.get('sc_group', '').strip()
    date_filter = request.args.get('date', '').strip()
    
    # Apply filters if data is available and filters are provided
    if not df.empty:
        # Ensure Date column is in datetime format for filtering if needed
        # In SQLite we store as string YYYY-MM-DD, pandas might read as object
        if 'Date' in df.columns:
             df['Date'] = pd.to_datetime(df['Date'])
             
        if sc_code_filter:
            # Convert column to string for flexible searching, handle potential non-string types
            df = df[df['SC_CODE'].astype(str).str.contains(sc_code_filter, case=False, na=False)]
        
        if sc_name_filter:
            df = df[df['SC_NAME'].str.contains(sc_name_filter, case=False, na=False)]

        if sc_group_filter:
            # Split comma-separated values and strip whitespace
            groups = [g.strip() for g in sc_group_filter.split(',') if g.strip()]
            if groups:
                # Case-insensitive match for groups
                df = df[df['SC_GROUP'].astype(str).str.upper().isin([g.upper() for g in groups])]

        if date_filter:
            try:
                filter_date = pd.to_datetime(date_filter)
                df = df[df['Date'].dt.date == filter_date.date()]
            except Exception:
                pass # Ignore invalid date format

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
                         sc_group=sc_group_filter,
                         date=date_filter,
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
@login_required
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
