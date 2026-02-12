from flask import Flask, render_template, request, redirect, url_for, flash, session
from flask_login import LoginManager, UserMixin, login_user, login_required, logout_user, current_user
import pandas as pd
import requests
import io
from datetime import timedelta
import secrets

# DATA_URL = "https://github.com/rahulpraj10/stock_tracker_v2/raw/main/StockData/merged_stock_data.pkl"
DB_URL = "https://github.com/rahulpraj10/stock_tracker_v2/raw/main/StockData/stock_data.db"
import sqlite3
import os
from datetime import datetime

DB_PATH = os.path.join("StockData", "stock_data.db")
if not os.path.exists("StockData"):
    os.makedirs("StockData")

def get_db_connection():
    # Check if DB needs to be downloaded
    if not os.path.exists(DB_PATH):
        print(f"Downloading database from {DB_URL}...")
        try:
            response = requests.get(DB_URL)
            response.raise_for_status()
            with open(DB_PATH, 'wb') as f:
                f.write(response.content)
            print("Database downloaded.")
        except Exception as e:
            print(f"Error downloading database: {e}")
            return None
            
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_db_connection()
    if conn:
        try:
            conn.execute('''
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
            conn.commit()
        except Exception as e:
            print(f"Error initializing DB: {e}")
        finally:
            conn.close()

# Initialize DB on startup (create table if needed)
init_db()

app = Flask(__name__)
app.secret_key = 'your_secret_key_here_change_in_production' # For development
app.permanent_session_lifetime = timedelta(minutes=5)

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
    app.permanent_session_lifetime = timedelta(minutes=5)

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
    # Filter parameters
    sc_code_filter = request.args.get('sc_code', '').strip()
    sc_name_filter = request.args.get('sc_name', '').strip()
    sc_group_filter = request.args.get('sc_group', '').strip()
    date_filter = request.args.get('date', '').strip()
    
    # Pagination
    page = request.args.get('page', 1, type=int)
    per_page = 18
    
    conn = get_db_connection()
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
        data_sql = f"SELECT * FROM stocks WHERE {where_sql} LIMIT ? OFFSET ?"
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
            cursor.execute("SELECT * FROM stocks LIMIT 0")
            columns = [description[0] for description in cursor.description]

    except Exception as e:
        print(f"Error querying database: {e}")
        data = []
        columns = []
        total_records = 0
        total_pages = 0
    finally:
        conn.close()

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
    conn = get_db_connection()
    if not conn:
        return []

    try:
        # Optimization: Instead of loading all data, fetch only recent data.
        # 1. Get the last N+1 distinct dates from the database.
        cursor = conn.cursor()
        date_query = "SELECT DISTINCT Date FROM stocks ORDER BY Date DESC LIMIT ?"
        cursor.execute(date_query, (days + 1,))
        dates = [row[0] for row in cursor.fetchall()]
        
        if len(dates) < days + 1:
            conn.close()
            return []
            
        # 2. Fetch data only for these dates
        placeholders = ','.join(['?'] * len(dates))
        query = f"SELECT SC_CODE, SC_NAME, Date, \"DAY'S VOLUME\" FROM stocks WHERE Date IN ({placeholders}) ORDER BY SC_CODE, Date"
        
        df = pd.read_sql_query(query, conn, params=dates)
        
        # Ensure Date is datetime for sorting if pandas didn't convert
        if 'Date' in df.columns:
             df['Date'] = pd.to_datetime(df['Date'])
             
    except Exception as e:
        print(f"Error in strategy: {e}")
        conn.close()
        return []
        
    conn.close()
    
    # Now continue with the Pandas logic on this smaller subset
    # Sort by SC_CODE and Date (already sorted by SQL but good to verify)
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

@app.route('/paper_trading', methods=['GET', 'POST'])
@login_required
def paper_trading():
    conn = get_db_connection()
    if not conn:
        flash("Database Error", "error")
        return redirect(url_for('index'))
    
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
                  try:
                      cursor = conn.cursor()
                      # Verify SC_CODE exists
                      cursor.execute("SELECT SC_NAME FROM stocks WHERE CAST(SC_CODE AS TEXT) = ? LIMIT 1", (sc_code,))
                      row = cursor.fetchone()
                      if row:
                          real_sc_name = row['SC_NAME']
                          cursor.execute(
                              'INSERT INTO orders (username, sc_code, sc_name, quantity, order_date) VALUES (?, ?, ?, ?, ?)',
                              (current_user.id, sc_code, real_sc_name, quantity, order_date)
                          )
                          conn.commit()
                          flash("Order placed successfully!", "success")
                      else:
                          flash("Invalid Stock Code. Please verify.", "error")
                  except Exception as e:
                      print(f"Error placing order: {e}")
                      flash("Failed to place order.", "error")

    # Fetch user's orders
    orders = []
    try:
        orders = conn.execute('SELECT * FROM orders WHERE username = ? ORDER BY created_at DESC', (current_user.id,)).fetchall()
    except Exception as e:
        print(f"Error fetching orders: {e}")
    finally:
        conn.close()

    return render_template('paper_trading.html', orders=orders)

@app.route('/delete_order/<int:order_id>', methods=['POST'])
@login_required
def delete_order(order_id):
    conn = get_db_connection()
    if not conn:
        flash("Database Error", "error")
        return redirect(url_for('paper_trading'))
    
    try:
        # Verify order belongs to user
        cursor = conn.cursor()
        cursor.execute("SELECT id FROM orders WHERE id = ? AND username = ?", (order_id, current_user.id))
        if cursor.fetchone():
            cursor.execute("DELETE FROM orders WHERE id = ?", (order_id,))
            conn.commit()
            flash("Order deleted successfully.", "success")
        else:
            flash("Order not found or unauthorized.", "error")
    except Exception as e:
        print(f"Error deleting order: {e}")
        flash("Failed to delete order.", "error")
    finally:
        conn.close()
        
    return redirect(url_for('paper_trading'))

@app.route('/order_chart_data/<int:order_id>')
@login_required
def order_chart_data(order_id):
    conn = get_db_connection()
    if not conn:
        return {"error": "Database error"}, 500
        
    data = {"dates": [], "values": []}
    try:
        order = conn.execute('SELECT * FROM orders WHERE id = ? AND username = ?', (order_id, current_user.id)).fetchone()
        if order:
            sc_code = order['sc_code']
            order_date = order['order_date']
            quantity = order['quantity']
            
            # Log debug info
            with open("app_debug.log", "a") as f:
                f.write(f"Chart Request: OrderID={order_id}, SC_CODE={sc_code}, Date={order_date}\n")

            # Fetch stock data from order_date to present
            # Removed CAST(SC_CODE AS TEXT) to try direct comparison
            query = """
                SELECT Date, "CLOSE" 
                FROM stocks 
                WHERE SC_CODE = ? AND Date >= ? 
                ORDER BY Date ASC
            """
            rows = conn.execute(query, (sc_code, order_date)).fetchall()
            
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
                
    except Exception as e:
        error_msg = f"Error fetching chart data: {e}"
        print(error_msg)
        with open("app_debug.log", "a") as f:
            f.write(error_msg + "\n")
        return {"error": str(e)}, 500
    finally:
        conn.close()
        
    return data

if __name__ == '__main__':
    app.run(debug=True)
