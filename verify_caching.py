
import os
import sys
sys.path.append(os.getcwd())

from app import app

def test_caching_headers():
    print("Testing Caching Headers API...", flush=True)
    
    app.config['TESTING'] = True
    app.config['WTF_CSRF_ENABLED'] = False
    client = app.test_client()
    
    with app.app_context():
        # Login
        client.post('/login', data=dict(
            username='rahul',
            password='rahul123'
        ), follow_redirects=True)
        
        # Test Paper Trading
        response1 = client.get('/paper_trading')
        cache_control1 = response1.headers.get('Cache-Control', '')
        print(f"Paper Trading Cache-Control: {cache_control1}", flush=True)
        
        if 'no-cache' in cache_control1 and 'no-store' in cache_control1:
            print("SUCCESS: Paper Trading has correct no-cache headers.", flush=True)
        else:
            print("FAILURE: Paper Trading missing no-cache headers.", flush=True)
            
        # Test Watchlist
        response2 = client.get('/watchlist')
        cache_control2 = response2.headers.get('Cache-Control', '')
        print(f"Watchlist Cache-Control: {cache_control2}", flush=True)
        
        if 'no-cache' in cache_control2 and 'no-store' in cache_control2:
            print("SUCCESS: Watchlist has correct no-cache headers.", flush=True)
        else:
            print("FAILURE: Watchlist missing no-cache headers.", flush=True)
            
        # Test Strategies (Should NOT have the strict no-cache headers, or inherit default)
        response3 = client.get('/strategies')
        cache_control3 = response3.headers.get('Cache-Control', '')
        print(f"Strategies Cache-Control: {cache_control3}", flush=True)

if __name__ == '__main__':
    try:
        test_caching_headers()
    except Exception as e:
        print(f"Test crashed: {e}", flush=True)
