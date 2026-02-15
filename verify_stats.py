
import os
import sys
import json
sys.path.append(os.getcwd())

from app import app, init_db, get_orders_db_connection

def test_stats():
    print("Testing Order Stats API...", flush=True)
    
    app.config['TESTING'] = True
    app.config['WTF_CSRF_ENABLED'] = False
    client = app.test_client()
    
    with app.app_context():
        # Login
        client.post('/login', data=dict(
            username='rahul',
            password='rahul123'
        ), follow_redirects=True)
        
        # Get an order ID
        conn = get_orders_db_connection()
        cur = conn.cursor()
        # Ensure we have at least one order. If not, create one.
        # Check for numeric ID (sqlite) or integer (postgres), just grab first.
        try:
            # Try getting one
            cur.execute("SELECT id, sc_code FROM orders LIMIT 1")
            order = cur.fetchone()
            
            if not order:
                 print("No orders found. Creating dummy order for test...", flush=True)
                 # Create order
                 client.post('/paper_trading', data=dict(
                    sc_code='500325', 
                    order_date='2025-11-04',
                    quantity='10'
                ), follow_redirects=True)
                 cur.execute("SELECT id, sc_code FROM orders LIMIT 1")
                 order = cur.fetchone()
        except Exception as e:
            print(f"DB Error: {e}", flush=True)
            return

        conn.close()
        
        if not order:
            print("Failed to setup test order.", flush=True)
            return

        order_id = order['id']
        print(f"Testing with Order ID: {order_id}", flush=True)
        
        # Hit API
        response = client.get(f'/order_chart_data/{order_id}')
        
        if response.status_code == 200:
            data = json.loads(response.data)
            print("Response received.", flush=True)
            
            if "stats" in data and data["stats"] is not None:
                stats = data["stats"]
                print("Stats Object Found:", flush=True)
                print(f"  Purchase Price: {stats.get('purchase_price')}", flush=True)
                print(f"  Current Price: {stats.get('current_price')}", flush=True)
                print(f"  Change %: {stats.get('pct_change')}", flush=True)
                print(f"  P/L: {stats.get('profit_loss')}", flush=True)
                
                if stats.get('purchase_price') is not None:
                     print("SUCCESS: Stats are present and calculated.", flush=True)
                else:
                     print("FAILURE: Stats object empty properties.", flush=True)
            else:
                print("FAILURE: 'stats' key missing or null in response.", flush=True)
                print(f"Keys found: {data.keys()}", flush=True)
        else:
            print(f"API Call Failed: {response.status_code}", flush=True)

if __name__ == '__main__':
    try:
        test_stats()
    except Exception as e:
        print(f"Test crashed: {e}", flush=True)
