from flask import Flask, render_template, request, redirect, url_for, flash, session, jsonify
from flask_login import LoginManager, UserMixin, login_user, login_required, logout_user, current_user
import pandas as pd
from datetime import timedelta, datetime, date
import secrets
import os
from functools import lru_cache
import yfinance as yf
from database import get_stock_db_connection, get_orders_db_connection
from strategies.min_increase import get_min_increase_stocks
from strategies.bullish_reversal import get_bullish_reversal_stocks
from strategies.double_bottom import get_double_bottom_stocks
from strategies.double_bottom_v1 import get_double_bottom_stocks as get_double_bottom_v1_stocks, \
    default_params as db_v1_default_params
from strategies.geminis_strategy import get_geminis_strategy_stocks
from strategies.multi_frame import get_multi_frame
from fundamentals.generate_scores import create_score
from tinydb import TinyDB, Query
import numpy as np
import pytz
import time
import json
import pickle
from flask_session import Session

IST = pytz.timezone('Asia/Kolkata')

# Global Strategy Cache (In-Memory)
# Structure: { user_id: { strategy_name: { 'params': {...}, 'data': [...] } } }
STRATEGY_CACHE = {}
BASKET_CACHE = {}
USER_BASKET_CACHE = {}


def format_data_for_render(data):
    for stock_code, details in data.items():
        # Access the list of features
        for df in details.keys():
            for record in details[df]:
                # Check if 'Date' exists and is a string
                if 'Date' in record and isinstance(record['Date'], str):
                    # Slice the first 10 characters "YYYY-MM-DD"
                    record['Date'] = record['Date'][:10]
    return data


def even_day_cleanup(session_folder, max_age_hours=24):
    now_ist = datetime.now(IST)
    current_day = now_ist.day

    # Logic: Only run if the day is an even number (2, 4, 6...)
    if current_day % 2 == 0:
        print(f"--- Periodic Cleanup Triggered (Day {current_day} is Even) ---")

        if not os.path.exists(session_folder):
            return

        now_timestamp = time.time()
        cutoff = now_timestamp - (max_age_hours * 3600)

        for filename in os.listdir(session_folder):
            file_path = os.path.join(session_folder, filename)
            if os.path.isfile(file_path):
                # Delete files older than your threshold (e.g., 24 hours)
                if os.path.getmtime(file_path) < cutoff:
                    try:
                        os.remove(file_path)
                    except OSError:
                        pass


def json_safe(obj):
    """
    Recursively transforms a nested dictionary/list to be JSON-compatible.
    Handles: Timestamps -> Strings, NaNs -> None (null)
    """
    if isinstance(obj, dict):
        return {k: json_safe(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [json_safe(i) for i in obj]
    elif isinstance(obj, (pd.Timestamp, datetime)):
        return obj.isoformat()
    elif isinstance(obj, float) and (np.isnan(obj) or np.isinf(obj)):
        return None  # JSON doesn't support NaN; 'None' becomes 'null'
    return obj


def init_db():
    conn = get_orders_db_connection()
    if conn:
        try:
            cur = conn.cursor()
            # Check if Postgres or SQLite
            is_postgres = 'psycopg2' in str(type(conn))

            if is_postgres:
                # Postgres Syntax
                cur.execute('''
                    CREATE TABLE IF NOT EXISTS orders (
                        id SERIAL PRIMARY KEY,
                        username TEXT,
                        sc_code TEXT,
                        sc_name TEXT,
                        quantity INTEGER,
                        order_date TEXT,
                        attrib_01 VARCHAR(50),
                        attrib_02 VARCHAR(50),
                        attrib_03 VARCHAR(50),
                        attrib_04 VARCHAR(50),
                        attrib_05 VARCHAR(50),
                        source_strategy VARCHAR(50),
                        status VARCHAR(20) DEFAULT 'OPEN',
                        sell_date TEXT,
                        sell_price REAL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                cur.execute('''
                    CREATE TABLE IF NOT EXISTS watchstocks (
                        id SERIAL PRIMARY KEY,
                        username TEXT,
                        sc_code TEXT,
                        sc_name TEXT,
                        quantity INTEGER,
                        order_date TEXT,
                        attrib_01 VARCHAR(50),
                        attrib_02 VARCHAR(50),
                        attrib_03 VARCHAR(50),
                        attrib_04 VARCHAR(50),
                        attrib_05 VARCHAR(50),
                        source_strategy VARCHAR(50),
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                cur.execute('''
                    CREATE TABLE IF NOT EXISTS strategy_results (
                        id SERIAL PRIMARY KEY,
                        sc_code VARCHAR(50) NOT NULL,
                        sc_name VARCHAR(255) NOT NULL,
                        strategy_name VARCHAR(100) NOT NULL,
                        run_date VARCHAR(20) NOT NULL,
                        data_json TEXT
                    )
                ''')
                cur.execute("CREATE INDEX IF NOT EXISTS idx_strategy_run_date ON strategy_results(run_date);")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_strategy_name ON strategy_results(strategy_name);")
            else:
                # SQLite Syntax
                cur.execute('''
                    CREATE TABLE IF NOT EXISTS orders (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        username TEXT,
                        sc_code TEXT,
                        sc_name TEXT,
                        quantity INTEGER,
                        order_date TEXT,
                        attrib_01 VARCHAR(50),
                        attrib_02 VARCHAR(50),
                        attrib_03 VARCHAR(50),
                        attrib_04 VARCHAR(50),
                        attrib_05 VARCHAR(50),
                        source_strategy VARCHAR(50),
                        status VARCHAR(20) DEFAULT 'OPEN',
                        sell_date TEXT,
                        sell_price REAL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                cur.execute('''
                    CREATE TABLE IF NOT EXISTS watchstocks (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        username TEXT,
                        sc_code TEXT,
                        sc_name TEXT,
                        quantity INTEGER,
                        order_date TEXT,
                        attrib_01 VARCHAR(50),
                        attrib_02 VARCHAR(50),
                        attrib_03 VARCHAR(50),
                        attrib_04 VARCHAR(50),
                        attrib_05 VARCHAR(50),
                        source_strategy VARCHAR(50),
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                cur.execute('''
                    CREATE TABLE IF NOT EXISTS strategy_results (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        sc_code VARCHAR(50) NOT NULL,
                        sc_name VARCHAR(255) NOT NULL,
                        strategy_name VARCHAR(100) NOT NULL,
                        run_date VARCHAR(20) NOT NULL,
                        data_json TEXT
                    )
                ''')
                cur.execute("CREATE INDEX IF NOT EXISTS idx_strategy_run_date ON strategy_results(run_date);")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_strategy_name ON strategy_results(strategy_name);")
            conn.commit()
            cur.close()
        except Exception as e:
            print(f"Error initializing DB: {e}")
        finally:
            conn.close()


# Initialize DB on startup (create table if needed)
init_db()

app = Flask(__name__)
app.secret_key = 'your_secret_key_here_change_in_production'  # For development

# --- The "Clean" Filesystem Config ---
app.config["SESSION_TYPE"] = "filesystem"
app.config["SESSION_FILE_DIR"] = "./flask_session"  # Folder for the data
app.config["SESSION_PERMANENT"] = False  # Delete when browser closes
app.config["SESSION_USE_SIGNER"] = True  # Extra security
Session(app)

app.permanent_session_lifetime = timedelta(minutes=5)

# Flask-Login Configuration
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login'

# Mock Database
USERS = {
    'rahul': {'password': 'Sachin@1010##^^'},
    'watch': {'password': 'watch123'},
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
    app.permanent_session_lifetime = timedelta(minutes=10)


@app.after_request
def add_cache_control_headers(response):
    """Disable caching for specific dynamic pages to ensure fresh data."""
    if request.endpoint in ['paper_trading', 'watchlist', 'sold_orders']:
        response.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
        response.headers['Pragma'] = 'no-cache'
        response.headers['Expires'] = '0'
    return response


@app.before_request
def handle_maintenance():
    # This runs before every request, but the "if % 2 == 0"
    # ensures it only actually does work on even days.
    # even_day_cleanup("./flask_session", max_age_hours=24)
    print('even_day_cleanup')


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


# Global Indices Map for the Charts
INDICES_MAP = {
    "Nifty 50": "^NSEI",
    "Nifty Auto": "^CNXAUTO",
    "Nifty Metal": "^CNXMETAL",
    "Nifty Bank": "^NSEBANK",
    "Nifty IT": "^CNXIT",
    "Nifty Pharma": "^CNXPHARMA",
    "Nifty FMCG": "^CNXFMCG",
    "Nifty Realty": "^CNXREALTY",
    "Nifty Media": "^CNXMEDIA",
    "Nifty Energy": "^CNXENERGY",
    "Nifty PSU Bank": "^CNXPSUBANK",
    "Nifty Infra": "^CNXINFRA",
    "India VIX": "^INDIAVIX"
}

INDICES_Performance = dict.fromkeys(INDICES_MAP, 0)


@app.route('/', methods=['GET', 'POST'])
@login_required
def index():
    # Filter parameters
    sc_code_filter = request.args.get('sc_code', '').strip()
    sc_name_filter = request.args.get('sc_name', '').strip()
    sc_group_filter = request.args.get('sc_group', '').strip()
    date_filter = request.args.get('date', '').strip()

    # Pagination
    page = request.args.get('page', 1, type=int)
    per_page = 18

    conn = get_stock_db_connection()
    if not conn:
        return "Database Error", 500

    try:
        # Build SQL Query
        where_clauses = ["1=1"]
        params = []

        if sc_code_filter:
            where_clauses.append("CAST(SC_CODE AS TEXT) LIKE ?")
            params.append(f"%{sc_code_filter}%")

        if sc_name_filter:
            where_clauses.append("SC_NAME LIKE ?")
            params.append(f"%{sc_name_filter}%")

        if sc_group_filter:
            groups = [g.strip() for g in sc_group_filter.split(',') if g.strip()]
            if groups:
                placeholders = ','.join(['?'] * len(groups))
                where_clauses.append(f"UPPER(SC_GROUP) IN ({placeholders})")
                params.extend([g.upper() for g in groups])

        if date_filter:
            # Assuming Date is stored as 'YYYY-MM-DD ...' string or similar.
            # We use DATE() function to normalize.
            where_clauses.append("DATE(Date) = ?")
            params.append(date_filter)

        where_sql = " AND ".join(where_clauses)

        # Get Total Count
        count_sql = f"SELECT COUNT(*) FROM stocks WHERE {where_sql}"
        cursor = conn.cursor()
        cursor.execute(count_sql, params)
        total_records = cursor.fetchone()[0]

        total_pages = (total_records + per_page - 1) // per_page
        page = max(1, min(page, total_pages)) if total_pages > 0 else 1

        start_idx = (page - 1) * per_page

        # Get Data
        data_sql = f'''SELECT SC_CODE, SC_NAME, SC_GROUP, SC_TYPE, OPEN, HIGH, LOW, CLOSE, LAST, PREVCLOSE, 
        NO_TRADES, NO_OF_SHRS, NET_TURNOV, TDCLOINDI,DATE_GEN, "SCRIP CODE", "DELIVERY QTY", "DELIVERY VAL", 
        "DAYS VOLUME", "DAYS TURNOVER", "DELV. PER.", Date
        FROM stocks WHERE {where_sql} ORDER BY Date DESC LIMIT ? OFFSET ?'''
        # We need to create a new params list for the data query because it has extra args
        data_params = params + [per_page, start_idx]

        cursor.execute(data_sql, data_params)
        rows = cursor.fetchall()

        # Convert to list of dicts
        data = [dict(row) for row in rows]

        # We also need columns for the header. If no data, we might need to fetch schema
        if rows:
            columns = rows[0].keys()
        else:
            # Fallback to get columns if empty result
            cursor.execute('''SELECT SC_CODE, SC_NAME, SC_GROUP, SC_TYPE, OPEN, HIGH, LOW, CLOSE, LAST, PREVCLOSE, 
                NO_TRADES, NO_OF_SHRS, NET_TURNOV, TDCLOINDI,DATE_GEN, "SCRIP CODE", "DELIVERY QTY", "DELIVERY VAL", 
                "DAYS VOLUME", "DAYS TURNOVER", "DELV. PER.", Date FROM stocks LIMIT 0''')
            columns = [description[0] for description in cursor.description]

    except Exception as e:
        print(f"Error querying database: {e}")
        data = []
        columns = []
        total_records = 0
        total_pages = 0
    finally:
        conn.close()

    tickers = list(INDICES_MAP.values())

    # today_str = date.today().isoformat()

    def get_smart_data():
        now_ist = datetime.now(IST)

        if now_ist.hour < 16:
            # Before 4 PM: Use yesterday's date as the cache key
            effective_date = (now_ist - timedelta(days=1)).date()
        else:
            # After 4 PM: Use today's date as the cache key
            effective_date = now_ist.date()
        return effective_date

    today_str = get_smart_data()
    print('Today str', today_str)
    data1 = _get_all_index_data(today_str)

    for name, ticker in INDICES_MAP.items():
        try:
            # Slicing a MultiIndex DataFrame: data[ticker]
            # Then we get the 'Close' column
            if ticker in data1.columns.levels[0]:
                series = data1[ticker]['Close'].dropna()

                if len(series) >= 2:
                    prev_close = series.iloc[-2]
                    curr_close = series.iloc[-1]
                    change = ((curr_close - prev_close) / prev_close) * 100
                    INDICES_Performance[name] = change
                else:
                    INDICES_Performance[name] = 0.0
            else:
                INDICES_Performance[name] = 0.0
        except Exception as e:
            print(f"Error for {ticker}: {e}")
            INDICES_Performance[name] = 0.0

    return render_template('index.html',
                           data=data,
                           columns=columns,
                           sc_code=sc_code_filter,
                           sc_name=sc_name_filter,
                           sc_group=sc_group_filter,
                           date=date_filter,
                           page=page,
                           total_pages=total_pages,
                           total_records=total_records,
                           indices=INDICES_MAP,
                           index_performance=INDICES_Performance)


@app.route('/strategies', methods=['GET', 'POST'])
@login_required
def strategies():
    selected_strategy = request.args.get('strategy')
    run_date_filter = request.args.get('run_date')

    # Default parameters for strategies (Base + DB v1)
    params = {
        'days': 5,  # for min_increase
        'min_days': 10,
        'max_days': 60,
        'tolerance': 3.0,
        'lookback': 90,
        'prominence': 5.0
    }
    # Add double_bottom_v1 parameters to the global dictionary
    params.update(db_v1_default_params)

    # Update params from request
    for key in params:
        if request.args.get(key):
            try:
                params[key] = float(request.args.get(key)) if '.' in request.args.get(key) else int(
                    request.args.get(key))
            except ValueError:
                pass

    available_dates = []
    SCHEDULED_STRATEGIES = ['bullish_reversal', 'multi_frame', 'double_bottom_v1']
    
    # We build a fresh cache dict for this request to pass to the template
    request_cached_data = {}

    conn = get_orders_db_connection()
    if conn:
        try:
            cur = conn.cursor()
            is_postgres = 'psycopg2' in str(type(conn))
            
            # Fetch data for all scheduled strategies to populate the tabs instantly
            for strat in SCHEDULED_STRATEGIES:
                if is_postgres:
                    cur.execute("SELECT DISTINCT run_date FROM strategy_results WHERE strategy_name = %s ORDER BY run_date DESC", (strat,))
                else:
                    cur.execute("SELECT DISTINCT run_date FROM strategy_results WHERE strategy_name = ? ORDER BY run_date DESC", (strat,))
                
                strat_dates = [r['run_date'] if isinstance(r, dict) else r[0] for r in cur.fetchall()]
                
                if strat == selected_strategy:
                    available_dates = strat_dates
                
                # Determine target date for this strategy
                target_date = None
                if strat == selected_strategy and run_date_filter:
                    target_date = run_date_filter
                elif strat_dates:
                    target_date = strat_dates[0]
                
                strat_results = []
                if target_date:
                    if is_postgres:
                        cur.execute("SELECT data_json FROM strategy_results WHERE strategy_name = %s AND run_date = %s", (strat, target_date))
                    else:
                        cur.execute("SELECT data_json FROM strategy_results WHERE strategy_name = ? AND run_date = ?", (strat, target_date))
                        
                    for row in cur.fetchall():
                        try:
                            data_val = row['data_json'] if isinstance(row, dict) else row[0]
                            if isinstance(data_val, str):
                                strat_results.append(json.loads(data_val))
                            else:
                                strat_results.append(data_val)
                        except:
                            pass
                
                request_cached_data[strat] = {'data': strat_results, 'run_date': target_date}
                
        except Exception as e:
            print(f"Error fetching strategy results: {e}")
        finally:
            conn.close()

    # Dynamic Strategies (Run strictly on demand when selected)
    if selected_strategy and selected_strategy not in SCHEDULED_STRATEGIES:
        new_results = []
        if selected_strategy == 'min_increase':
            new_results = get_min_increase_stocks(params['days'])
        elif selected_strategy == 'geminis_strategy':
            new_results = get_geminis_strategy_stocks()
        elif selected_strategy == 'double_bottom':
            new_results = get_double_bottom_stocks(
                min_days=params['min_days'],
                max_days=params['max_days'],
                tolerance_pct=params['tolerance'],
                lookback_days=params['lookback'],
                peak_prominence_pct=params['prominence']
            )
        request_cached_data[selected_strategy] = {'data': new_results}

    return render_template('strategies.html',
                           strategy=selected_strategy,
                           cached_data=request_cached_data,
                           params=params,
                           available_dates=available_dates,
                           current_run_date=run_date_filter)


@app.route('/baskets')
@login_required
def baskets():
    # 1. Get current time in IST
    now_ist = datetime.now(IST)

    # 2. Logic: If it's before 10 PM, we are still on "yesterday's" data cycle.
    # By subtracting 22 hours, the date only rolls over at exactly 11:00 PM.
    effective_date = (now_ist - timedelta(hours=23)).strftime('%Y-%m-%d')

    # 3. Check Cache.
    if "data" in BASKET_CACHE and BASKET_CACHE.get("date") == effective_date:
        return render_template('baskets.html', baskets=BASKET_CACHE["data"])
    # else read the pkl file and update the cache
    try:
        with open('StockData/basket_data.pkl', 'rb') as file:
            # Deserialize the object from the file
            all_baskets_results = pickle.load(file)
        BASKET_CACHE["date"] = effective_date
        BASKET_CACHE["data"] = all_baskets_results
    except FileNotFoundError as e:
        print(e)
    return render_template('baskets.html', baskets=all_baskets_results)
    # with open('BSE_baskets.json', 'r') as fp:
    #     BASKETS = json.load(fp)
    #
    # conn = get_stock_db_connection()
    # all_baskets_results = {}
    #
    # try:
    #     all_codes = list(set(int(code) for codes in BASKETS.values() for code in codes))
    #     # Query all_codes once, THEN loop through the dataframe to split by basket.
    #     placeholders = ','.join(['?'] * len(all_codes))
    #     query = f"""
    #                     SELECT SC_CODE, SC_NAME, CLOSE, Date
    #                     FROM stocks
    #                     WHERE SC_CODE IN ({placeholders})
    #                     AND Date >= date('now', '-100 days')
    #                     ORDER BY Date ASC
    #                 """
    #     df_all = pd.read_sql_query(query, conn, params=all_codes)
    #
    #     for basket_name, sc_codes in BASKETS.items():
    #         basket_stocks = []
    #         clean_sc_codes = [int(code) for code in sc_codes]
    #         basket_df = df_all[df_all['SC_CODE'].isin(clean_sc_codes)]
    #
    #         for code in sc_codes:
    #             stock_df = basket_df[basket_df['SC_CODE'] == code].copy()
    #             if stock_df.empty: continue
    #
    #             # Get latest values
    #             latest = stock_df.iloc[-1]
    #             current_price = latest['CLOSE']
    #
    #             # 1. Calculate RSI (14 period)
    #             delta = stock_df['CLOSE'].diff()
    #             gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    #             loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    #             rs = gain / loss
    #             rsi = 100 - (100 / (1 + rs)).iloc[-1]
    #
    #             # 2. Trend Indicators (Current vs X days ago)
    #             def get_trend(days):
    #                 if len(stock_df) >= days:
    #                     prev_price = stock_df.iloc[-days]['CLOSE']
    #                     return "up" if current_price > prev_price else "down"
    #                 return "neutral"
    #
    #             basket_stocks.append({
    #                 "sc_code": code,
    #                 "name": latest['SC_NAME'],
    #                 "price": round(current_price, 2),
    #                 "rsi": round(rsi, 2) if not pd.isna(rsi) else "-",
    #                 "trends": {
    #                     "5d": get_trend(5),
    #                     "15d": get_trend(15),
    #                     "30d": get_trend(30),
    #                     "90d": get_trend(90)
    #                 }
    #             })
    #
    #         all_baskets_results[basket_name] = basket_stocks
    #
    # finally:
    #     conn.close()
    #
    # BASKET_CACHE["date"] = effective_date
    # BASKET_CACHE["data"] = all_baskets_results
    return render_template('baskets.html', baskets=all_baskets_results)


# Initialize TinyDB (adjust path as needed)
tdb = TinyDB('fundamentals_scores.json')
ScoreQuery = Query()


@app.route('/paper_trading', methods=['GET', 'POST'])
@login_required
def paper_trading():
    conn_orders = get_orders_db_connection()
    if not conn_orders:
        flash("Orders Database Error", "error")
        return redirect(url_for('index'))

    # Check if Postgres or SQLite for parameter placeholder style
    # generic psycopg2 uses %s, sqlite3 uses ?
    # But wait, pandas read_sql uses params tuple.
    # Standard cursor.execute matches driver.
    # Psycopg2: %s
    # PySQLite: ?
    # This is a bit annoying.
    # BUT, if I use an ORM later it's better. For now, I need to detect dialet.

    is_postgres = 'psycopg2' in str(type(conn_orders))
    param_style = '%s' if is_postgres else '?'

    if request.method == 'POST':
        sc_code = request.form['sc_code'].strip()
        # sc_name = request.form['sc_name'].strip() # Optional, maybe fetch from DB based on code
        order_date = request.form['order_date']
        quantity = int(request.form['quantity'])

        # Basic validation
        if not sc_code or not order_date or quantity <= 0:
            flash("Invalid input parameters.", "error")
        else:
            # Validate date restriction
            min_date = datetime(2025, 11, 3)
            max_date = datetime.now()
            input_date = datetime.strptime(order_date, '%Y-%m-%d')

            if input_date < min_date or input_date > max_date:
                flash("Order date must be between Nov 03, 2025 and Today.", "error")
            else:
                conn_stock = get_stock_db_connection()
                real_sc_name = None

                if conn_stock:
                    try:
                        stock_cur = conn_stock.cursor()
                        stock_cur.execute("SELECT SC_NAME FROM stocks WHERE CAST(SC_CODE AS TEXT) = ? LIMIT 1",
                                          (sc_code,))
                        row = stock_cur.fetchone()
                        if row:
                            real_sc_name = row['SC_NAME']
                    except Exception as e:
                        print(f"Error checking stock: {e}")
                    finally:
                        conn_stock.close()

                if real_sc_name:
                    try:
                        cur = conn_orders.cursor()

                        if is_postgres:
                            cur.execute(
                                'INSERT INTO orders (username, sc_code, sc_name, quantity, order_date) VALUES (%s, %s, %s, %s, %s)',
                                (current_user.id, sc_code, real_sc_name, quantity, order_date)
                            )
                        else:
                            cur.execute(
                                'INSERT INTO orders (username, sc_code, sc_name, quantity, order_date) VALUES (?, ?, ?, ?, ?)',
                                (current_user.id, sc_code, real_sc_name, quantity, order_date)
                            )

                        conn_orders.commit()
                        cur.close()
                        flash("Order placed successfully!", "success")
                    except Exception as e:
                        print(f"Error placing order: {e}")
                        flash("Failed to place order.", "error")
                else:
                    flash("Invalid Stock Code. Please verify.", "error")

    # Fetch user's orders
    orders = []
    try:
        cur = conn_orders.cursor()

        if is_postgres:
            cur.execute("SELECT * FROM orders WHERE username = %s AND status = 'OPEN' ORDER BY created_at DESC",
                        (current_user.id,))
        else:
            cur.execute("SELECT * FROM orders WHERE username = ? AND status = 'OPEN' ORDER BY created_at DESC",
                        (current_user.id,))

        orders = cur.fetchall()
        # print(len(orders))
        # Get scores from tinyb,
        # Run a scan of all unique SC_CODE in orders. Cheeck if all those SC_CODES are present in tiny DB
        # For code which are not present, call function to generate code and push to tiny db
        # Then download all score and proceed
        cur.close()
    except Exception as e:
        print(f"Error fetching orders: {e}")
    finally:
        conn_orders.close()

    # --- TINYDB SCORE LOGIC START ---
    orders_list = [dict(row) for row in orders]
    # for row in orders:
    #     # This converts the sqlite3.Row or Postgres Row to a mutable dict
    #     orders_list.append(dict(row))

    if orders_list:
        # 1. Get unique SC_CODEs from the user's orders
        unique_sc_codes = {order['sc_code'] for order in orders_list}

        # 2. Update orders_list with actual scores from TinyDB
        for order in orders_list:
            record = tdb.get(ScoreQuery.sc_code == order['sc_code'])
            if record:
                order['score'] = round(record['score'], 2)
            else:
                order['score'] = 'Pending'
    # --- TINYDB SCORE LOGIC END ---

    # Calculate Portfolio Summary
    total_invested = 0.0
    total_current_value = 0.0

    stock_conn = get_stock_db_connection()
    if orders and stock_conn:
        try:
            # OPTIMIZED: Fetch all needed stocks in one single query
            unique_sc_codes = list(set(order['sc_code'] for order in orders))
            if not unique_sc_codes:
                raise Exception("No unique sc_codes found")

            placeholders = ','.join(['?'] * len(unique_sc_codes))
            min_date = min(order['order_date'] for order in orders)

            query = f'SELECT "SCRIP CODE" as sc_code, Close, Date FROM stocks WHERE "SCRIP CODE" IN ({placeholders}) AND Date >= ? ORDER BY Date ASC'

            params = unique_sc_codes + [min_date]
            all_stock_data = stock_conn.execute(query, params).fetchall()

            # Group by SC_CODE
            grouped_data = {}
            for row in all_stock_data:
                sc = str(row['sc_code'])
                if sc not in grouped_data:
                    grouped_data[sc] = []
                grouped_data[sc].append(row)

            for order in orders:
                try:
                    sc_code_str = str(order['sc_code'])
                    stock_data = grouped_data.get(sc_code_str, [])

                    if stock_data:
                        # Find Purchase Price
                        purchase_price = 0.0
                        order_date_str = order['order_date']

                        # Find first date >= order_date
                        for row in stock_data:
                            if row['Date'] >= order_date_str:
                                purchase_price = float(row['Close'])
                                break

                        if purchase_price == 0.0 and stock_data:
                            purchase_price = float(stock_data[-1]['Close'])

                        current_price = float(stock_data[-1]['Close'])

                        total_invested += purchase_price * order['quantity']
                        total_current_value += current_price * order['quantity']

                except Exception as e:
                    print(f"Error calculating stats for order {order['id']}: {e}")
        except Exception as e:
            print(f"Error in portfolio calc: {e}")
        finally:
            stock_conn.close()
    elif stock_conn:
        stock_conn.close()

    total_pl = total_current_value - total_invested
    total_pl_pct = (total_pl / total_invested * 100) if total_invested > 0 else 0.0

    return render_template('paper_trading.html',
                           orders=orders_list,
                           summary={
                               'total_invested': total_invested,
                               'total_current_value': total_current_value,
                               'total_pl': total_pl,
                               'total_pl_pct': total_pl_pct
                           })


def get_technical_indicators(stock_data_rows):
    if len(stock_data_rows) < 20:
        return None

    # Convert rows to DataFrame
    df = pd.DataFrame([dict(row) for row in stock_data_rows])

    # Ensure numeric columns [cite: 26]
    df['CLOSE'] = pd.to_numeric(df['CLOSE'], errors='coerce')
    df['HIGH'] = pd.to_numeric(df['HIGH'], errors='coerce')
    df['LOW'] = pd.to_numeric(df['LOW'], errors='coerce')
    # Use the alias 'Volume' defined in your SQL query
    df['Volume'] = pd.to_numeric(df['Volume'], errors='coerce')

    # --- RSI Calculation (14-period) ---
    delta = df['CLOSE'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    rs = gain / loss
    df['RSI'] = 100 - (100 / (1 + rs))

    # --- Bollinger Bands (20-period, 2 Std Dev) ---
    df['MA20'] = df['CLOSE'].rolling(window=20).mean()
    df['STD20'] = df['CLOSE'].rolling(window=20).std()
    df['BB_Upper'] = df['MA20'] + (df['STD20'] * 2)
    df['BB_Lower'] = df['MA20'] - (df['STD20'] * 2)

    # --- VWAP Calculation ---
    # Standard VWAP = sum(Price * Volume) / sum(Volume)
    df['VWAP'] = (df['CLOSE'] * df['Volume']).cumsum() / df['Volume'].cumsum()

    latest = df.iloc[-1]

    # RSI Signal logic
    rsi_val = latest['RSI']
    rsi_signal = "Overbought" if rsi_val > 70 else ("Oversold" if rsi_val < 30 else "Neutral")

    # BB Signal logic
    bb_signal = "Inside Bands"
    if latest['CLOSE'] > latest['BB_Upper']:
        bb_signal = "Bullish Breakout"
    elif latest['CLOSE'] < latest['BB_Lower']:
        bb_signal = "Bearish Breakdown"

    return {
        'rsi': round(float(rsi_val), 2) if not pd.isna(rsi_val) else "N/A",
        'rsi_signal': rsi_signal,
        'vwap': round(float(latest['VWAP']), 2) if not pd.isna(latest['VWAP']) else "N/A",
        'bb_signal': bb_signal
    }


@app.route('/get_fundamental_report/<sc_code>')
@login_required
def get_fundamental_report(sc_code):
    Company = Query()
    # Fetch the record from TinyDB
    record = tdb.get(Company.sc_code == sc_code)

    if record and 'data' in record:
        print('Record is found')
        data1 = record['data']
        print(data1)
        formatted_json = format_data_for_render(data1)
        jsonified_data = jsonify(formatted_json[sc_code])

        return jsonified_data
    return jsonify({"error": "No report data found"}), 404


@app.route('/delete_order/<int:order_id>', methods=['POST'])
@login_required
def delete_order(order_id):
    conn = get_orders_db_connection()
    if not conn:
        flash("Orders Database Error", "error")
        return redirect(url_for('paper_trading'))

    is_postgres = 'psycopg2' in str(type(conn))

    try:
        # Verify order belongs to user
        cur = conn.cursor()

        if is_postgres:
            cur.execute("SELECT id FROM orders WHERE id = %s AND username = %s", (order_id, current_user.id))
        else:
            cur.execute("SELECT id FROM orders WHERE id = ? AND username = ?", (order_id, current_user.id))

        if cur.fetchone():
            if is_postgres:
                cur.execute("DELETE FROM orders WHERE id = %s", (order_id,))
            else:
                cur.execute("DELETE FROM orders WHERE id = ?", (order_id,))

            conn.commit()
            flash("Order deleted successfully.", "success")
        else:
            flash("Order not found or unauthorized.", "error")
        cur.close()
    except Exception as e:
        print(f"Error deleting order: {e}")
        flash("Failed to delete order.", "error")
    finally:
        conn.close()

    return redirect(url_for('paper_trading'))


@app.route('/order_chart_data/<int:order_id>')
@login_required
def order_chart_data(order_id):
    conn_orders = get_orders_db_connection()
    if not conn_orders:
        return {"error": "Orders Database error"}, 500

    data = {"dates": [], "values": []}

    is_postgres = 'psycopg2' in str(type(conn_orders))

    try:
        cur_orders = conn_orders.cursor()
        if is_postgres:
            cur_orders.execute('SELECT * FROM orders WHERE id = %s AND username = %s', (order_id, current_user.id))
        else:
            cur_orders.execute('SELECT * FROM orders WHERE id = ? AND username = ?', (order_id, current_user.id))

        order = cur_orders.fetchone()
        cur_orders.close()

        if order:
            sc_code = order['sc_code']
            order_date = order['order_date']
            quantity = order['quantity']

            # Fetch stock data from stock DB
            conn_stock = get_stock_db_connection()
            if conn_stock:
                try:
                    # Log debug info
                    with open("app_debug.log", "a") as f:
                        f.write(f"Chart Request: OrderID={order_id}, SC_CODE={sc_code}, Date={order_date}\n")

                    # Fetch stock data from order_date to present
                    # Use "SCRIP CODE" as per DB schema

                    query_all = """
                        SELECT Date, "CLOSE", "HIGH", "LOW", "DAY'S VOLUME" as Volume 
                        FROM stocks 
                        WHERE "SCRIP CODE" = ? 
                        ORDER BY Date ASC
                    """
                    cursor_stock = conn_stock.cursor()
                    all_rows = cursor_stock.execute(query_all, (sc_code,)).fetchall()

                    # 1. CALL THE INDICATOR FUNCTION HERE
                    # We pass 'all_rows' because it contains the history needed for RSI/BB
                    tech_indicators = get_technical_indicators(all_rows)

                    # 2. Filter rows for the Chart (only from order_date onwards)
                    chart_rows = [r for r in all_rows if r['Date'] >= order_date]
                    print('Length of chart rows: ', len(chart_rows))

                    with open("app_debug.log", "a") as f:
                        f.write(f"Rows found: {len(chart_rows)}\n")

                    for row in chart_rows:
                        data["dates"].append(row['Date'])
                        # Ensure Close is float
                        try:
                            close_val = float(row['CLOSE'])
                        except:
                            close_val = 0.0
                        data["values"].append(close_val * quantity)

                    # Calculate Stats
                    if data["values"]:
                        try:
                            # Calculate per-unit prices first for % change
                            unit_purchase_price = float(chart_rows[0]['CLOSE'])
                            unit_current_price = float(chart_rows[-1]['CLOSE'])

                            pct_change = ((
                                                      unit_current_price - unit_purchase_price) / unit_purchase_price) * 100 if unit_purchase_price != 0 else 0

                            data["stats"] = {
                                "purchase_price": unit_purchase_price * quantity,
                                "current_price": unit_current_price * quantity,
                                "pct_change": round(pct_change, 2),
                                "profit_loss": (unit_current_price - unit_purchase_price) * quantity,
                                "rsi": tech_indicators['rsi'] if tech_indicators else "N/A",
                                "rsi_signal": tech_indicators['rsi_signal'] if tech_indicators else "N/A",
                                "vwap": tech_indicators['vwap'] if tech_indicators else "N/A",
                                "bb_signal": tech_indicators['bb_signal'] if tech_indicators else "Neutral"
                            }
                        except Exception as e:
                            print(f"Error calculating stats: {e}")
                            data["stats"] = None
                except Exception as e:
                    print(f"Error fetching stock data: {e}")
                finally:
                    conn_stock.close()

    except Exception as e:
        error_msg = f"Error fetching chart data: {e}"
        print(error_msg)
        with open("app_debug.log", "a") as f:
            f.write(error_msg + "\n")
        return {"error": str(e)}, 500
    finally:
        conn_orders.close()

    return data


@app.route('/api/sell_order/<int:order_id>', methods=['POST'])
@login_required
def sell_order(order_id):
    conn = get_orders_db_connection()
    if not conn:
        return jsonify({'error': 'Database Error'}), 500

    is_postgres = 'psycopg2' in str(type(conn))

    try:
        cur = conn.cursor()

        # Verify order belongs to user and is OPEN
        if is_postgres:
            cur.execute("SELECT * FROM orders WHERE id = %s AND username = %s AND status = 'OPEN'",
                        (order_id, current_user.id))
        else:
            cur.execute("SELECT * FROM orders WHERE id = ? AND username = ? AND status = 'OPEN'",
                        (order_id, current_user.id))

        order = cur.fetchone()

        if not order:
            cur.close()
            return jsonify({'error': 'Order not found or already sold.'}), 404

        sc_code = order['sc_code']

        # Get latest price
        conn_stock = get_stock_db_connection()
        sell_price = 0.0
        if conn_stock:
            try:
                stock_cur = conn_stock.cursor()
                stock_cur.execute("SELECT CLOSE FROM stocks WHERE CAST(SC_CODE AS TEXT) = ? ORDER BY Date DESC LIMIT 1",
                                  (sc_code,))
                stock_row = stock_cur.fetchone()
                if stock_row:
                    sell_price = float(stock_row['CLOSE'])
            except Exception as e:
                print(f"Error getting stock price for sell: {e}")
            finally:
                conn_stock.close()

        if sell_price == 0.0:
            cur.close()
            return jsonify({'error': 'Could not fetch latest stock price.'}), 500

        sell_date = datetime.now().strftime('%Y-%m-%d')

        # Update order
        if is_postgres:
            cur.execute("UPDATE orders SET status = 'SOLD', sell_date = %s, sell_price = %s WHERE id = %s",
                        (sell_date, sell_price, order_id))
        else:
            cur.execute("UPDATE orders SET status = 'SOLD', sell_date = ?, sell_price = ? WHERE id = ?",
                        (sell_date, sell_price, order_id))

        conn.commit()
        cur.close()

        return jsonify({'success': True, 'message': 'Order marked as sold successfully.'})
    except Exception as e:
        print(f"Error selling order: {e}")
        return jsonify({'error': str(e)}), 500
    finally:
        conn.close()


@app.route('/sold_orders')
@login_required
def sold_orders():
    conn_orders = get_orders_db_connection()
    if not conn_orders:
        flash("Orders Database Error", "error")
        return redirect(url_for('index'))

    is_postgres = 'psycopg2' in str(type(conn_orders))

    orders = []
    try:
        cur = conn_orders.cursor()
        if is_postgres:
            cur.execute(
                "SELECT * FROM orders WHERE username = %s AND status = 'SOLD' ORDER BY sell_date DESC, created_at DESC",
                (current_user.id,))
        else:
            cur.execute(
                "SELECT * FROM orders WHERE username = ? AND status = 'SOLD' ORDER BY sell_date DESC, created_at DESC",
                (current_user.id,))

        orders = cur.fetchall()
        cur.close()
    except Exception as e:
        print(f"Error fetching sold orders: {e}")
    finally:
        conn_orders.close()

    orders_list = [dict(row) for row in orders]

    total_invested = 0.0
    total_returned = 0.0

    stock_conn = get_stock_db_connection()
    if orders_list and stock_conn:
        try:
            for order in orders_list:
                try:
                    sc_code = order['sc_code']
                    stock_cur = stock_conn.cursor()
                    stock_cur.execute(
                        "SELECT Date, CLOSE FROM stocks WHERE CAST(SC_CODE AS TEXT) = ? ORDER BY Date ASC", (sc_code,))
                    stock_data = [{'Date': r['Date'], 'Close': float(r['CLOSE'])} for r in stock_cur.fetchall()]

                    purchase_price = 0.0
                    order_date_str = order['order_date']

                    for row in stock_data:
                        if row['Date'] >= order_date_str:
                            purchase_price = float(row['Close'])
                            break

                    if purchase_price == 0.0 and stock_data:
                        purchase_price = float(stock_data[-1]['Close'])

                    sell_price = float(order['sell_price']) if order['sell_price'] else 0.0

                    order['purchase_price'] = purchase_price
                    order['pnl'] = (sell_price - purchase_price) * order['quantity']
                    order['pnl_pct'] = (
                                (sell_price - purchase_price) / purchase_price * 100) if purchase_price > 0 else 0.0

                    total_invested += purchase_price * order['quantity']
                    total_returned += sell_price * order['quantity']
                except Exception as e:
                    print(f"Error calculating stats for sold order {order['id']}: {e}")
        except Exception as e:
            print(f"Error processing sold orders: {e}")
        finally:
            stock_conn.close()
    elif stock_conn:
        stock_conn.close()

    total_pl = total_returned - total_invested
    total_pl_pct = (total_pl / total_invested * 100) if total_invested > 0 else 0.0

    return render_template('sold_orders.html',
                           orders=orders_list,
                           summary={
                               'total_invested': total_invested,
                               'total_returned': total_returned,
                               'total_pl': total_pl,
                               'total_pl_pct': total_pl_pct
                           })


@app.route('/api/add_to_watchlist', methods=['POST'])
@login_required
def api_add_to_watchlist():
    data = request.get_json()
    sc_code = data.get('sc_code')
    sc_name = data.get('sc_name')
    source_strategy = data.get('source_strategy', '')

    if not sc_code or not sc_name:
        return jsonify({'error': 'Missing sc_code or sc_name'}), 400

    order_date = datetime.now().strftime('%Y-%m-%d')
    quantity = 1

    conn_orders = get_orders_db_connection()
    if not conn_orders:
        return jsonify({'error': 'Database error'}), 500

    is_postgres = 'psycopg2' in str(type(conn_orders))

    try:
        cur = conn_orders.cursor()
        if is_postgres:
            cur.execute(
                'INSERT INTO watchstocks (username, sc_code, sc_name, quantity, order_date, source_strategy) VALUES (%s, %s, %s, %s, %s, %s)',
                (current_user.id, sc_code, sc_name, quantity, order_date, source_strategy)
            )
        else:
            cur.execute(
                'INSERT INTO watchstocks (username, sc_code, sc_name, quantity, order_date, source_strategy) VALUES (?, ?, ?, ?, ?, ?)',
                (current_user.id, sc_code, sc_name, quantity, order_date, source_strategy)
            )
        conn_orders.commit()
        cur.close()
        return jsonify({'success': True, 'message': 'Added to watchlist successfully'})
    except Exception as e:
        print(f"Error in api_add_to_watchlist: {e}")
        return jsonify({'error': str(e)}), 500
    finally:
        conn_orders.close()


@app.route('/api/add_to_paper_trading', methods=['POST'])
@login_required
def api_add_to_paper_trading():
    data = request.get_json()
    sc_code = data.get('sc_code')
    sc_name = data.get('sc_name')
    source_strategy = data.get('source_strategy', '')

    if not sc_code or not sc_name:
        return jsonify({'error': 'Missing sc_code or sc_name'}), 400

    order_date = datetime.now().strftime('%Y-%m-%d')
    quantity = 1

    conn_orders = get_orders_db_connection()
    if not conn_orders:
        return jsonify({'error': 'Database error'}), 500

    is_postgres = 'psycopg2' in str(type(conn_orders))

    try:
        cur = conn_orders.cursor()
        if is_postgres:
            cur.execute(
                '''INSERT INTO orders (username, sc_code, sc_name, quantity, order_date, source_strategy, status) 
                   VALUES (%s, %s, %s, %s, %s, %s, 'OPEN')''',
                (current_user.id, sc_code, sc_name, quantity, order_date, source_strategy)
            )
        else:
            cur.execute(
                '''INSERT INTO orders (username, sc_code, sc_name, quantity, order_date, source_strategy, status) 
                   VALUES (?, ?, ?, ?, ?, ?, 'OPEN')''',
                (current_user.id, sc_code, sc_name, quantity, order_date, source_strategy)
            )
        conn_orders.commit()
        cur.close()
        return jsonify({'success': True, 'message': 'Added to Paper Trading successfully'})
    except Exception as e:
        print(f"Error in api_add_to_paper_trading: {e}")
        return jsonify({'error': str(e)}), 500
    finally:
        conn_orders.close()


# --- Watchlist Functionality ---

@app.route('/watchlist', methods=['GET', 'POST'])
@login_required
def watchlist():
    conn_orders = get_orders_db_connection()
    if not conn_orders:
        flash("Database Error", "error")
        return redirect(url_for('index'))

    is_postgres = 'psycopg2' in str(type(conn_orders))

    if request.method == 'POST':
        sc_code = request.form['sc_code'].strip()
        order_date = request.form['order_date']
        quantity = int(request.form['quantity'])

        if not sc_code or not order_date or quantity <= 0:
            flash("Invalid input parameters.", "error")
        else:
            min_date = datetime(2025, 11, 3)
            max_date = datetime.now()
            input_date = datetime.strptime(order_date, '%Y-%m-%d')

            if input_date < min_date or input_date > max_date:
                flash("Date must be between Nov 03, 2025 and Today.", "error")
            else:
                conn_stock = get_stock_db_connection()
                real_sc_name = None

                if conn_stock:
                    try:
                        stock_cur = conn_stock.cursor()
                        stock_cur.execute("SELECT SC_NAME FROM stocks WHERE CAST(SC_CODE AS TEXT) = ? LIMIT 1",
                                          (sc_code,))
                        row = stock_cur.fetchone()
                        if row:
                            real_sc_name = row['SC_NAME']
                    except Exception as e:
                        print(f"Error checking stock: {e}")
                    finally:
                        conn_stock.close()

                if real_sc_name:
                    try:
                        cur = conn_orders.cursor()
                        if is_postgres:
                            cur.execute(
                                'INSERT INTO watchstocks (username, sc_code, sc_name, quantity, order_date) VALUES (%s, %s, %s, %s, %s)',
                                (current_user.id, sc_code, real_sc_name, quantity, order_date)
                            )
                        else:
                            cur.execute(
                                'INSERT INTO watchstocks (username, sc_code, sc_name, quantity, order_date) VALUES (?, ?, ?, ?, ?)',
                                (current_user.id, sc_code, real_sc_name, quantity, order_date)
                            )
                        conn_orders.commit()
                        cur.close()
                        flash("Added to Watchlist!", "success")
                    except Exception as e:
                        print(f"Error adding to watchlist: {e}")
                        flash("Failed to add to watchlist.", "error")
                else:
                    flash("Invalid Stock Code. Please verify.", "error")

    # Fetch user's watchlist
    orders = []
    try:
        cur = conn_orders.cursor()
        if is_postgres:
            cur.execute('SELECT * FROM watchstocks WHERE username = %s ORDER BY created_at DESC', (current_user.id,))
        else:
            cur.execute('SELECT * FROM watchstocks WHERE username = ? ORDER BY created_at DESC', (current_user.id,))
        orders = cur.fetchall()
        cur.close()
    except Exception as e:
        print(f"Error fetching watchlist: {e}")
    finally:
        conn_orders.close()

        # --- TINYDB SCORE LOGIC START ---
        watch_list = [dict(row) for row in orders]
        # for row in orders:
        #     # This converts the sqlite3.Row or Postgres Row to a mutable dict
        #     orders_list.append(dict(row))

        if watch_list:
            # 1. Get unique SC_CODEs from the user's orders
            unique_sc_codes = {order['sc_code'] for order in watch_list}

            # 2. Update watch_list with actual scores from TinyDB
            for order in watch_list:
                record = tdb.get(ScoreQuery.sc_code == order['sc_code'])
                if record:
                    order['score'] = round(record['score'], 2)
                else:
                    order['score'] = 'Pending'
        # --- TINYDB SCORE LOGIC END ---

    # Calculate Portfolio Summary
    total_invested = 0.0
    total_current_value = 0.0

    stock_conn = get_stock_db_connection()
    if orders and stock_conn:
        try:
            # OPTIMIZED: Fetch all needed stocks in one single query
            unique_sc_codes = list(set(order['sc_code'] for order in orders))
            if not unique_sc_codes:
                raise Exception("No unique sc_codes found")

            placeholders = ','.join(['?'] * len(unique_sc_codes))
            min_date = min(order['order_date'] for order in orders)

            query = f'SELECT "SCRIP CODE" as sc_code, Close, Date FROM stocks WHERE "SCRIP CODE" IN ({placeholders}) AND Date >= ? ORDER BY Date ASC'

            params = unique_sc_codes + [min_date]
            all_stock_data = stock_conn.execute(query, params).fetchall()

            # Group by SC_CODE
            grouped_data = {}
            for row in all_stock_data:
                sc = str(row['sc_code'])
                if sc not in grouped_data:
                    grouped_data[sc] = []
                grouped_data[sc].append(row)

            for order in orders:
                try:
                    sc_code_str = str(order['sc_code'])
                    stock_data = grouped_data.get(sc_code_str, [])

                    if stock_data:
                        purchase_price = 0.0
                        order_date_str = order['order_date']

                        for row in stock_data:
                            if row['Date'] >= order_date_str:
                                purchase_price = float(row['Close'])
                                break

                        if purchase_price == 0.0 and stock_data:
                            purchase_price = float(stock_data[-1]['Close'])

                        current_price = float(stock_data[-1]['Close'])

                        total_invested += purchase_price * order['quantity']
                        total_current_value += current_price * order['quantity']
                except Exception as e:
                    print(f"Error calculating stats for watchlist {order['id']}: {e}")
        except Exception as e:
            print(f"Error in watchlist calc: {e}")
        finally:
            stock_conn.close()
    elif stock_conn:
        stock_conn.close()

    total_pl = total_current_value - total_invested
    total_pl_pct = (total_pl / total_invested * 100) if total_invested > 0 else 0.0

    return render_template('watchlist.html',
                           orders=watch_list,
                           summary={
                               'total_invested': total_invested,
                               'total_current_value': total_current_value,
                               'total_pl': total_pl,
                               'total_pl_pct': total_pl_pct
                           })


@app.route('/delete_watchlist_order/<int:order_id>', methods=['POST'])
@login_required
def delete_watchlist_order(order_id):
    conn = get_orders_db_connection()
    if not conn:
        flash("Database Error", "error")
        return redirect(url_for('watchlist'))

    is_postgres = 'psycopg2' in str(type(conn))

    try:
        cur = conn.cursor()
        if is_postgres:
            cur.execute("SELECT id FROM watchstocks WHERE id = %s AND username = %s", (order_id, current_user.id))
        else:
            cur.execute("SELECT id FROM watchstocks WHERE id = ? AND username = ?", (order_id, current_user.id))

        if cur.fetchone():
            if is_postgres:
                cur.execute("DELETE FROM watchstocks WHERE id = %s", (order_id,))
            else:
                cur.execute("DELETE FROM watchstocks WHERE id = ?", (order_id,))

            conn.commit()
            flash("Item removed from Watchlist.", "success")
        else:
            flash("Item not found or unauthorized.", "error")
        cur.close()
    except Exception as e:
        print(f"Error deleting from watchlist: {e}")
        flash("Failed to delete item.", "error")
    finally:
        conn.close()

    return redirect(url_for('watchlist'))


@app.route('/watchlist_chart_data/<int:order_id>')
@login_required
def watchlist_chart_data(order_id):
    conn_orders = get_orders_db_connection()
    if not conn_orders:
        return {"error": "Database error"}, 500

    data = {"dates": [], "values": []}

    is_postgres = 'psycopg2' in str(type(conn_orders))

    try:
        cur_orders = conn_orders.cursor()
        if is_postgres:
            cur_orders.execute('SELECT * FROM watchstocks WHERE id = %s AND username = %s', (order_id, current_user.id))
        else:
            cur_orders.execute('SELECT * FROM watchstocks WHERE id = ? AND username = ?', (order_id, current_user.id))

        order = cur_orders.fetchone()
        cur_orders.close()

        if order:
            sc_code = order['sc_code']
            order_date = order['order_date']
            quantity = order['quantity']

            conn_stock = get_stock_db_connection()
            if conn_stock:
                try:
                    query = """
                        SELECT Date, "CLOSE" 
                        FROM stocks 
                        WHERE "SCRIP CODE" = ? AND Date >= ? 
                        ORDER BY Date ASC
                    """
                    cursor_stock = conn_stock.cursor()
                    rows = cursor_stock.execute(query, (sc_code, order_date)).fetchall()

                    for row in rows:
                        data["dates"].append(row['Date'])
                        try:
                            close_val = float(row['CLOSE'])
                        except:
                            close_val = 0.0
                        data["values"].append(close_val * quantity)

                    if data["values"]:
                        try:
                            unit_purchase_price = float(rows[0]['CLOSE'])
                            unit_current_price = float(rows[-1]['CLOSE'])

                            pct_change = ((
                                                  unit_current_price - unit_purchase_price) / unit_purchase_price) * 100 if unit_purchase_price != 0 else 0

                            data["stats"] = {
                                "purchase_price": unit_purchase_price * quantity,
                                "current_price": unit_current_price * quantity,
                                "pct_change": round(pct_change, 2),
                                "profit_loss": (unit_current_price - unit_purchase_price) * quantity
                            }
                        except Exception as e:
                            print(f"Error calculating stats: {e}")
                            data["stats"] = None
                except Exception as e:
                    print(f"Error fetching stock data: {e}")
                finally:
                    conn_stock.close()
    except Exception as e:
        return {"error": str(e)}, 500
    finally:
        conn_orders.close()

    return data


@app.route('/health')
@login_required
def health():
    # 1. Get user's active holdings from TinyDB or your paper trading DB
    # Assuming you fetch codes from your paper trading table
    conn_orders = get_orders_db_connection()
    if not conn_orders:
        flash("Orders Database Error", "error")
        return redirect(url_for('index'))
    is_postgres = 'psycopg2' in str(type(conn_orders))
    param_style = '%s' if is_postgres else '?'

    # Fetch user's orders
    orders = []
    try:
        cur = conn_orders.cursor()

        if is_postgres:
            cur.execute("SELECT * FROM orders WHERE username = %s AND status = 'OPEN' ORDER BY created_at DESC",
                        (current_user.id,))
        else:
            cur.execute("SELECT * FROM orders WHERE username = ? AND status = 'OPEN' ORDER BY created_at DESC",
                        (current_user.id,))

        orders = cur.fetchall()
        # print(len(orders))
        # Get scores from tinyb,
        # Run a scan of all unique SC_CODE in orders. Cheeck if all those SC_CODES are present in tiny DB
        # For code which are not present, call function to generate code and push to tiny db
        # Then download all score and proceed
        cur.close()
    except Exception as e:
        print(f"Error fetching orders: {e}")
    finally:
        conn_orders.close()

    orders_list = [dict(row) for row in orders]
    holding_codes = None
    if orders_list:
        holding_codes = {order['sc_code'] for order in orders_list}

    if not holding_codes:
        return render_template('health.html', holdings=[])

    # 2. Fetch technical data (Reuse your logic from Baskets)
    conn = get_stock_db_connection()
    holdings_data = []

    try:
        # Optimization: Fetch only unique codes
        unique_codes = list(set(holding_codes))
        placeholders = ','.join(['?'] * len(unique_codes))

        query = f"""
            SELECT SC_CODE, SC_NAME, CLOSE, Date 
            FROM stocks 
            WHERE SC_CODE IN ({placeholders}) 
            AND Date >= date('now', '-100 days')
            ORDER BY Date ASC
        """
        df = pd.read_sql_query(query, conn, params=unique_codes)

        for code in unique_codes:
            stock_df = df[df['SC_CODE'].astype(str) == str(code)].copy()
            if stock_df.empty: continue

            latest = stock_df.iloc[-1]
            last_price = latest['CLOSE']

            # 1. Calculate RSI (14 period)
            delta = stock_df['CLOSE'].diff()
            gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
            rs = gain / loss
            rsi = 100 - (100 / (1 + rs)).iloc[-1]
            print(f'Length of stock {len(stock_df)}')
            if len(stock_df) >= 90:
                old_st_price = stock_df.iloc[-90]['CLOSE']
            elif len(stock_df) >= 60:
                old_st_price = stock_df.iloc[-60]['CLOSE']
            elif len(stock_df) >= 30:
                old_st_price = stock_df.iloc[-30]['CLOSE']
            else:
                old_st_price = 1000

            holdings_data.append({
                "sc_code": code,
                "name": stock_df.iloc[-1]['SC_NAME'],
                "price": round(stock_df.iloc[-1]['CLOSE'], 2),
                "rsi": round(rsi, 2),
                "trend_90": "up" if last_price > old_st_price else "down",
                # "trend_90": "up"

            })

    finally:
        conn.close()

    return render_template('health.html', holdings=holdings_data)


@app.route('/api/get_score/<sc_code>')
@login_required
def api_get_score(sc_code):
    record = tdb.get(ScoreQuery.sc_code == sc_code)
    if record:
        return jsonify({'sc_code': sc_code, 'score': round(record['score'], 2), 'status': 'cached'})

    # Missing, calculate it
    try:
        new_score, fundamentals_dict = create_score(sc_code)
        score_val = round(float(new_score), 2)
        tdb.insert({
            'sc_code': sc_code,
            'score': score_val,
            'data': json_safe(fundamentals_dict),
            'last_updated': datetime.now().isoformat()
        })
        return jsonify({'sc_code': sc_code, 'score': score_val, 'status': 'calculated'})
    except Exception as e:
        print(f"Error generating score for {sc_code}: {e}")
        return jsonify({'sc_code': sc_code, 'error': str(e)}), 500


@app.route('/api/search_stocks')
@login_required
def search_stocks():
    query_str = request.args.get('q', '').strip()
    if not query_str or len(query_str) < 2:
        return jsonify([])

    conn = get_stock_db_connection()
    if not conn:
        return jsonify([])

    try:
        cursor = conn.cursor()

        # 1. Get the latest date available in the DB
        cursor.execute("SELECT MAX(Date) FROM stocks")
        latest_date_row = cursor.fetchone()
        latest_date = latest_date_row[0] if latest_date_row else None

        if not latest_date:
            return jsonify([])

        # 2. Search for stock names containing the query string on the latest date
        # We try both "SC_NAME" and "SC NAME" just in case, but rely on previous findings
        # Actually, let's just stick to what likely works: SC_NAME (from line 141) or "SC NAME" (from line 483 context)
        # To be safe, let's use the one consistent with the rest of app.py index route: SC_NAME

        sql = """
            SELECT SC_NAME, "SCRIP CODE", "CLOSE" 
            FROM stocks 
            WHERE Date = ? AND SC_NAME LIKE ? 
            LIMIT 10
        """
        cursor.execute(sql, (latest_date, '%' + query_str + '%'))

        results = [
            {
                'sc_name': row['SC_NAME'],
                'sc_code': row['SCRIP CODE'],
                'close': row['CLOSE']
            }
            for row in cursor.fetchall()
        ]
        return jsonify(results)
    except Exception as e:
        print(f"Error searching stocks: {e}")
        # Fallback query if SC_NAME column name issue
        try:
            # Try with "SC NAME" if SC_NAME fails
            sql_fallback = """
                SELECT "SC NAME", "SCRIP CODE", "CLOSE" 
                FROM stocks 
                WHERE Date = ? AND "SC NAME" LIKE ? 
                LIMIT 10
            """
            cursor.execute(sql_fallback, (latest_date, '%' + query_str + '%'))
            results = [
                {
                    'sc_name': row['SC NAME'],
                    'sc_code': row['SCRIP CODE'],
                    'close': row['CLOSE']
                }
                for row in cursor.fetchall()
            ]
            return jsonify(results)
        except Exception as e2:
            print(f"Error searching stocks fallback: {e2}")
            return jsonify([])
    finally:
        conn.close()


# --- API Endpoints and Secondary Views ---

@lru_cache(maxsize=32)
def _get_all_sectors_data(start_date):
    tickers = list(INDICES_MAP.values())
    data = yf.download(tickers, start=start_date, group_by='ticker', auto_adjust=True)
    return data


@lru_cache(maxsize=5)
def _get_all_index_data(start_date):
    tickers = list(INDICES_MAP.values())
    print('Called YF')
    data = yf.download(tickers, period="5d", group_by='ticker', auto_adjust=True)
    return data


@app.route('/api/sectors/all/history')
@login_required
def all_sectors_history():
    start_date = request.args.get('start_date')

    if not start_date:
        # Default to 1 year ago if no date is provided
        start_date = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')

    try:
        # Fetch data for all tickers concurrently (cached)
        data = _get_all_sectors_data(start_date)

        if data.empty:
            return jsonify({"error": "No data found for the given date range"}), 404

        # Extract dates ensuring it's not a multi-index and format as string
        dates = data.index.strftime('%Y-%m-%d').tolist()
        datasets = []

        # Calculate relative percentage change for each index
        for name, ticker in INDICES_MAP.items():
            if ticker in data.columns.levels[0]:
                series = data[(ticker, 'Close')].dropna()

                # We need to map the series back to the original dates index to keep arrays aligned.
                # Use interpolation or forward fill to handle NaN values (missing trading days)
                aligned_series = series.reindex(data.index).ffill()

                # Get the absolute first valid price in the aligned series to calculate % change
                first_valid_index = aligned_series.first_valid_index()
                if first_valid_index is None:
                    continue  # No valid data for this ticker

                start_price = aligned_series.loc[first_valid_index]

                # Calculate % change: ((Current - Start) / Start) * 100
                pct_change = ((aligned_series - start_price) / start_price) * 100

                # Smart Code for change to color the tabs
                if len(aligned_series) < 2:
                    continue  # Need at least two points to calculate a change
                start_price = aligned_series.iloc[-2]
                current_pct_change = ((aligned_series.iloc[-1] - start_price) / start_price) * 100
                INDICES_Performance[name] = current_pct_change

                datasets.append({
                    "label": name,
                    "data": [round(val, 2) if not pd.isna(val) else None for val in pct_change.tolist()]
                })

        return jsonify({
            "dates": dates,
            "datasets": datasets
        })

    except Exception as e:
        print(f"Error fetching all sectors history: {e}")
        return jsonify({"error": "Failed to fetch all sectors data"}), 500


@lru_cache(maxsize=128)
def _get_sector_history_data(ticker):
    stock = yf.Ticker(ticker)
    hist = stock.history(period="1y")
    return hist


@app.route('/api/sector/<ticker>/history')
@login_required
def sector_history(ticker):
    try:
        # Fetch 1 year of historical data from Yahoo Finance (cached)
        hist = _get_sector_history_data(ticker)

        if hist.empty:
            return jsonify({"error": "No data found for ticker"}), 404

        # Format dates as YYYY-MM-DD strings
        dates = hist.index.strftime('%Y-%m-%d').tolist()
        # Round prices to 2 decimal places
        prices = [round(p, 2) for p in hist['Close'].tolist()]

        return jsonify({
            "ticker": ticker,
            "dates": dates,
            "prices": prices
        })
    except Exception as e:
        print(f"Error fetching Yahoo Finance data for {ticker}: {e}")
        return jsonify({"error": "Failed to fetch sector data"}), 500


@app.route('/api/stock/<sc_code>/history')
@login_required
def stock_history(sc_code):
    conn = get_stock_db_connection()
    if not conn:
        return jsonify({"error": "Database error"}), 500

    try:
        query = """
             SELECT Date, CLOSE 
             FROM stocks 
             WHERE SC_CODE = ? OR SC_CODE = CAST(? AS INTEGER)
             ORDER BY Date ASC
         """
        df = pd.read_sql_query(query, conn, params=(sc_code, sc_code))

        if df.empty:
            return jsonify({"error": "Stock not found"}), 404

        return jsonify({
            "sc_code": sc_code,
            "dates": df["Date"].tolist(),
            "prices": df["CLOSE"].tolist()
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


if __name__ == '__main__':
    app.run(debug=True)