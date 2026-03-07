from flask import Flask, render_template, request, redirect, url_for, flash, session, jsonify
from flask_login import LoginManager, UserMixin, login_user, login_required, logout_user, current_user
import pandas as pd
from datetime import timedelta, datetime
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

# Global Strategy Cache (In-Memory)
# Structure: { user_id: { strategy_name: { 'params': {...}, 'data': [...] } } }
STRATEGY_CACHE = {}


# ... (Previous imports remain)

# ... (Previous code remains)

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
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
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
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
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
    app.permanent_session_lifetime = timedelta(minutes=5)


@app.after_request
def add_cache_control_headers(response):
    """Disable caching for specific dynamic pages to ensure fresh data."""
    if request.endpoint in ['paper_trading', 'watchlist']:
        response.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
        response.headers['Pragma'] = 'no-cache'
        response.headers['Expires'] = '0'
    return response


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
<<<<<<< HEAD
data1 = None
=======


>>>>>>> origin/main
@app.route('/', methods=['GET', 'POST'])
@login_required
def index():
    global data1
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
<<<<<<< HEAD
    if data1 is None:
        data1 = yf.download(tickers, period="5d", group_by='ticker', auto_adjust=True)
        print('Running Again')
=======
    data1 = yf.download(tickers, period="5d", group_by='ticker', auto_adjust=True)
>>>>>>> origin/main
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

<<<<<<< HEAD
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
                         index_performance = INDICES_Performance)
=======
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

>>>>>>> origin/main

@app.route('/strategies', methods=['GET', 'POST'])
@login_required
def strategies():
    selected_strategy = request.args.get('strategy')

    # Initialize cache for user if not exists
    user_id = current_user.id
    if user_id not in STRATEGY_CACHE:
        STRATEGY_CACHE[user_id] = {}

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

    # If a strategy is selected, run it (or check cache)
    if selected_strategy:
        # Check if we need to re-run
        # We re-run if:
        # 1. Strategy not in cache
        # 2. Params changed
        # 3. Explicit 'reload' forced (not implemented yet, but good practice)

        cached = STRATEGY_CACHE[user_id].get(selected_strategy)
        run_new = True

        if cached:
            # Compare params (exclude 'days' if not relevant to the strategy, but simple comparison is okay)
            # For strictness, we should compare only relevant params, but comparing all is safer/easier
            if cached['params'] == params:
                run_new = False

        if run_new:
            new_results = []
            if selected_strategy == 'min_increase':
                new_results = get_min_increase_stocks(params['days'])
            elif selected_strategy == 'bullish_reversal':
                new_results = get_bullish_reversal_stocks()
            elif selected_strategy == 'geminis_strategy':
                new_results = get_geminis_strategy_stocks()
            elif selected_strategy == 'multi_frame':
                new_results = get_multi_frame()
            elif selected_strategy == 'double_bottom':
                new_results = get_double_bottom_stocks(
                    min_days=params['min_days'],
                    max_days=params['max_days'],
                    tolerance_pct=params['tolerance'],
                    lookback_days=params['lookback'],
                    peak_prominence_pct=params['prominence']
                )
            elif selected_strategy == 'double_bottom_v1':
                df_results = get_double_bottom_v1_stocks(params=params)
                if not df_results.empty:
                    new_results = df_results.to_dict('records')
                else:
                    new_results = []

            # Update Cache
            STRATEGY_CACHE[user_id][selected_strategy] = {
                'params': params.copy(),  # Store copy of current params
                'data': new_results
            }

    # Prepare data for template
    # We pass the entire cache for this user so the template can render any tab that has data
    cached_data = STRATEGY_CACHE[user_id]

    # For backward compatibility with template (which expects 'results' for the selected strategy)
    current_results = cached_data.get(selected_strategy, {}).get('data', []) if selected_strategy else []

    return render_template('strategies.html',
                           strategy=selected_strategy,
                           results=current_results,
                           cached_data=cached_data,
                           params=params)


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
            cur.execute('SELECT * FROM orders WHERE username = %s ORDER BY created_at DESC', (current_user.id,))
        else:
            cur.execute('SELECT * FROM orders WHERE username = ? ORDER BY created_at DESC', (current_user.id,))

        orders = cur.fetchall()
        cur.close()
    except Exception as e:
        print(f"Error fetching orders: {e}")
    finally:
        conn_orders.close()

    # Calculate Portfolio Summary
    total_invested = 0.0
    total_current_value = 0.0

    stock_conn = get_stock_db_connection()
    if orders and stock_conn:
        try:
            for order in orders:
                try:
                    # Optimized: Could fetch all needed stocks in one go, but this is fine for now
                    # We need the price on order_date and current price
                    # Query the main 'stocks' table for the specific SCRIP CODE
                    # usage of "SCRIP CODE" (quoted) to handle space in column name
                    query = 'SELECT Close, Date FROM stocks WHERE "SCRIP CODE" = ? ORDER BY Date ASC'
                    stock_data = stock_conn.execute(query, (order['sc_code'],)).fetchall()

                    if stock_data:
                        # Find Purchase Price
                        purchase_price = 0.0
                        order_date_str = order['order_date']

                        # Find first date >= order_date
                        # Since list is sorted by date ASC, we can iterate
                        for row in stock_data:
                            if row['Date'] >= order_date_str:
                                purchase_price = float(row['Close'])
                                break

                        # If date is in future relative to data, use last available?
                        # Or if we didn't find any date >= order_date (unlikely if order validated)
                        if purchase_price == 0.0 and stock_data:
                            # Fallback to last close if order date is very recent/future
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
                           orders=orders,
                           summary={
                               'total_invested': total_invested,
                               'total_current_value': total_current_value,
                               'total_pl': total_pl,
                               'total_pl_pct': total_pl_pct
                           })


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
                    query = """
                        SELECT Date, "CLOSE" 
                        FROM stocks 
                        WHERE "SCRIP CODE" = ? AND Date >= ? 
                        ORDER BY Date ASC
                    """
                    # Stock DB is always SQLite, use ?
                    cursor_stock = conn_stock.cursor()
                    rows = cursor_stock.execute(query, (sc_code, order_date)).fetchall()

                    with open("app_debug.log", "a") as f:
                        f.write(f"Rows found: {len(rows)}\n")

                    for row in rows:
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
        error_msg = f"Error fetching chart data: {e}"
        print(error_msg)
        with open("app_debug.log", "a") as f:
            f.write(error_msg + "\n")
        return {"error": str(e)}, 500
    finally:
        conn_orders.close()

    return data


@app.route('/api/add_to_watchlist', methods=['POST'])
@login_required
def api_add_to_watchlist():
    data = request.get_json()
    sc_code = data.get('sc_code')
    sc_name = data.get('sc_name')

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
                'INSERT INTO watchstocks (username, sc_code, sc_name, quantity, order_date) VALUES (%s, %s, %s, %s, %s)',
                (current_user.id, sc_code, sc_name, quantity, order_date)
            )
        else:
            cur.execute(
                'INSERT INTO watchstocks (username, sc_code, sc_name, quantity, order_date) VALUES (?, ?, ?, ?, ?)',
                (current_user.id, sc_code, sc_name, quantity, order_date)
            )
        conn_orders.commit()
        cur.close()
        return jsonify({'success': True, 'message': 'Added to watchlist successfully'})
    except Exception as e:
        print(f"Error in api_add_to_watchlist: {e}")
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

    # Calculate Portfolio Summary
    total_invested = 0.0
    total_current_value = 0.0

    stock_conn = get_stock_db_connection()
    if orders and stock_conn:
        try:
            for order in orders:
                try:
                    query = 'SELECT Close, Date FROM stocks WHERE "SCRIP CODE" = ? ORDER BY Date ASC'
                    stock_data = stock_conn.execute(query, (order['sc_code'],)).fetchall()

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
                           orders=orders,
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

<<<<<<< HEAD
                
=======
>>>>>>> origin/main
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
             ORDER BY Date DESC
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


