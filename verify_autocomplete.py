
import os
import sys
import json
sys.path.append(os.getcwd())

from app import app

def test_autocomplete():
    print("Testing Autocomplete API...", flush=True)
    
    app.config['TESTING'] = True
    app.config['WTF_CSRF_ENABLED'] = False
    client = app.test_client()
    
    with app.app_context():
        # Login
        client.post('/login', data=dict(
            username='rahul',
            password='rahul123'
        ), follow_redirects=True)
        
        # Test Search
        query = "Rel" # e.g. Reliance
        response = client.get(f'/api/search_stocks?q={query}')
        
        if response.status_code == 200:
            data = json.loads(response.data)
            print(f"Search query: '{query}'", flush=True)
            print(f"Results found: {len(data)}", flush=True)
            if len(data) > 0:
                print("First result:", data[0], flush=True)
                if 'sc_name' in data[0] and 'sc_code' in data[0]:
                    print("SUCCESS: Result has correct keys.", flush=True)
                else:
                    print("FAILURE: Result missing keys.", flush=True)
            else:
                print("WARNING: No results found (might be valid if DB is empty or no match).", flush=True)
        else:
            print(f"API Call Failed: {response.status_code}", flush=True)

if __name__ == '__main__':
    try:
        test_autocomplete()
    except Exception as e:
        print(f"Test crashed: {e}", flush=True)
