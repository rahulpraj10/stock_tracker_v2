from flask import Flask, render_template, request, redirect, url_for, flash, session
from flask_login import LoginManager, UserMixin, login_user, login_required, logout_user, current_user
import pandas as pd
from datetime import timedelta
import secrets
from database import get_db_connection
from strategies.min_increase import get_min_increase_stocks
from strategies.bullish_reversal import get_bullish_reversal_stocks
from strategies.double_bottom import get_double_bottom_stocks

# ... (Previous imports remain)

# ... (Previous code remains)

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

@app.route('/strategies', methods=['GET', 'POST'])
@login_required
def strategies():
    selected_strategy = request.args.get('strategy')
    strategy_results = []
    
    # Default parameters for strategies
    params = {
        'days': 5, # for min_increase
        'min_days': 10,
        'max_days': 60,
        'tolerance': 3.0,
        'lookback': 90,
        'prominence': 5.0
    }
    
    # Update params from request
    for key in params:
        if request.args.get(key):
            try:
                params[key] = float(request.args.get(key)) if '.' in request.args.get(key) else int(request.args.get(key))
            except ValueError:
                pass

    if selected_strategy == 'min_increase':
        strategy_results = get_min_increase_stocks(params['days'])
    elif selected_strategy == 'bullish_reversal':
         strategy_results = get_bullish_reversal_stocks()
    elif selected_strategy == 'double_bottom':
        strategy_results = get_double_bottom_stocks(
            min_days=params['min_days'], 
            max_days=params['max_days'], 
            tolerance_pct=params['tolerance'], 
            lookback_days=params['lookback'], 
            peak_prominence_pct=params['prominence']
        )

    return render_template('strategies.html', 
                         strategy=selected_strategy, 
                         results=strategy_results,
                         params=params)

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


