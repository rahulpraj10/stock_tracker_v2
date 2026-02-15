
import os
import sys
# Make sure we can import from current directory
sys.path.append(os.getcwd())

from app import app, init_db, get_orders_db_connection

def test_persistence():
    print("Starting Persistence Test...", flush=True)
    
    app.config['TESTING'] = True
    app.config['WTF_CSRF_ENABLED'] = False
    client = app.test_client()
    
    with app.app_context():
        # Login
        print("Logging in...", flush=True)
        resp = client.post('/login', data=dict(
            username='rahul',
            password='rahul123'
        ), follow_redirects=True)
        if b'Login' in resp.data and b'Logout' not in resp.data:
             print("Login failed?", flush=True)
        
        # 1. Place Order
        print("Placing Order...", flush=True)
        response = client.post('/paper_trading', data=dict(
            sc_code='500325', 
            # sc_name='RELIANCE INDUSTRIES LTD.', # Not used
            order_date='2025-11-04',
            quantity='10'
        ), follow_redirects=True)
        
        if b'Order placed successfully!' in response.data:
            print("Order Placement: SUCCESS", flush=True)
        else:
            print(f"Order Placement: FAILED. Response: {response.data}", flush=True)
            return
        
        # 2. Verify in DB directly
        print("Verifying in orders.db...", flush=True)
        conn = get_orders_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT * FROM orders WHERE sc_code = '500325' AND quantity = 10")
        order = cur.fetchone()
        conn.close()
        
        if order:
            print(f"DB Verification: SUCCESS. Order ID={order['id']}", flush=True)
        else:
             print("DB Verification: FAILED via direct DB check.", flush=True)
             return
        
        # 3. Delete Order
        print("Deleting Order...", flush=True)
        order_id = order['id']
        response = client.post(f'/delete_order/{order_id}', follow_redirects=True)
        
        if b'Order deleted successfully.' in response.data:
            print("Order Deletion: SUCCESS", flush=True)
        else:
            print(f"Order Deletion: FAILED. Response: {response.data}", flush=True)
        
        # 4. Verify gone from DB
        conn = get_orders_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT * FROM orders WHERE id = ?", (order_id,))
        deleted_order = cur.fetchone()
        conn.close()
        
        if not deleted_order:
             print("Deletion Verification: SUCCESS", flush=True)
        else:
             print("Deletion Verification: FAILED. Order still exists.", flush=True)

if __name__ == '__main__':
    try:
        test_persistence()
    except Exception as e:
        print(f"Test crashed with error: {e}", flush=True)
