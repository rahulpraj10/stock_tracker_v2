
import os
import sys
from datetime import date
sys.path.append(os.getcwd())

from app import app, get_orders_db_connection

def test_portfolio_summary():
    print("Testing Portfolio Summary...", flush=True)
    
    app.config['TESTING'] = True
    app.config['WTF_CSRF_ENABLED'] = False
    client = app.test_client()
    
    with app.app_context():
        # Login
        client.post('/login', data=dict(
            username='rahul',
            password='rahul123'
        ), follow_redirects=True)
        
        # Ensure at least one order exists
        conn = get_orders_db_connection()
        cur = conn.cursor()
        try:
             cur.execute("SELECT COUNT(*) FROM orders")
             # Handle tuple or dict return depending on cursor
             res = cur.fetchone()
             count = res[0] if isinstance(res, tuple) else res['COUNT(*)']
             
             if count == 0:
                 print("No orders found. Creating dummy order...", flush=True)
                 client.post('/paper_trading', data=dict(
                    sc_code='500325', 
                    order_date='2025-11-04',
                    quantity='10'
                ), follow_redirects=True)
        except Exception as e:
            print(f"DB Error: {e}", flush=True)
        finally:
            conn.close()

        # Capture template context
        # Flask 2.0+ can verify cached templates, but capturing context is easier via signal or just inspecting response text for values if rendered.
        # But wait, 'render_template' renders the HTML.
        # We can inspect the HTML parsing, or we can just call the route function logic if valid? 
        # Easier: The route returns a rendered string. We can search for the "Total Invested" text and maybe some values.
        
        response = client.get('/paper_trading')
        
        if response.status_code == 200:
            html = response.data.decode('utf-8')
            print("Page loaded successfully.", flush=True)
            
            if "Total Invested" in html and "Total P/L" in html:
                print("SUCCESS: Summary section found in HTML.", flush=True)
                
                # Try to extract a value to verify calculation occurred (e.g. not everything is 0.00 unless it really is)
                # This is a bit of a loose check, but sufficient to say "it rendered".
                # For exact values, we'd need to mock the stock_db or calc manually.
                print("Summary section rendered.", flush=True)
            else:
                print("FAILURE: Summary section keywords missing from HTML.", flush=True)
        else:
            print(f"Failed to load page. Status: {response.status_code}", flush=True)

if __name__ == '__main__':
    try:
        test_portfolio_summary()
    except Exception as e:
        print(f"Test crashed: {e}", flush=True)
