
import os
import sys
import json
sys.path.append(os.getcwd())

from app import app
from database import get_orders_db_connection

def test_add_to_watchlist():
    print("Testing Add to Watchlist API...", flush=True)
    
    app.config['TESTING'] = True
    app.config['WTF_CSRF_ENABLED'] = False
    client = app.test_client()
    
    with app.app_context():
        # Login
        client.post('/login', data=dict(
            username='rahul',
            password='rahul123'
        ), follow_redirects=True)
        
        # Test Add to Watchlist
        payload = {
            'sc_code': '500325',
            'sc_name': 'RELIANCE INDUSTRIES LTD.'
        }
        
        response = client.post('/api/add_to_watchlist', json=payload)
        
        if response.status_code == 200:
            data = json.loads(response.data)
            print(f"Response: {data}", flush=True)
            if data.get('success'):
                print("SUCCESS: API returned success.", flush=True)
                
                # Check DB
                conn = get_orders_db_connection()
                cur = conn.cursor()
                is_postgres = 'psycopg2' in str(type(conn))
                
                if is_postgres:
                    cur.execute("SELECT * FROM watchstocks WHERE username = %s AND sc_code = %s", ('rahul', '500325'))
                else:
                    cur.execute("SELECT * FROM watchstocks WHERE username = ? AND sc_code = ?", ('rahul', '500325'))
                
                row = cur.fetchone()
                if row:
                    print(f"SUCCESS: Database entry found: {dict(row) if hasattr(row, 'keys') else row}", flush=True)
                else:
                    print("FAILURE: Entry not found in database.", flush=True)
                
                conn.close()
            else:
                print("FAILURE: API did not return success=True.", flush=True)
        else:
            print(f"API Call Failed: {response.status_code} - {response.data}", flush=True)

if __name__ == '__main__':
    try:
        test_add_to_watchlist()
    except Exception as e:
        print(f"Test crashed: {e}", flush=True)
