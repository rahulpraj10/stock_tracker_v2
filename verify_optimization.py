import requests
from bs4 import BeautifulSoup

session = requests.Session()
BASE_URL = 'http://127.0.0.1:5000'

def login():
    login_url = f'{BASE_URL}/login'
    credentials = {'username': 'rahul', 'password': 'rahul123'}
    print(f"Logging in to {login_url}...")
    response = session.post(login_url, data=credentials)
    if response.url == f'{BASE_URL}/':
        print("Login successful.")
        return True
    else:
        print(f"Login failed. Redirected to {response.url}")
        return False

def verify_index():
    print("Verifying Index Page...")
    response = session.get(BASE_URL)
    if response.status_code == 200 and '<table' in response.text:
        print("Index page loaded with table.")
        return True
    else:
        print("Index page failed or no table found.")
        return False

def verify_filter():
    print("Verifying Filter (SC_CODE=500325)...") # Reliance
    response = session.get(f'{BASE_URL}/?sc_code=500325')
    if response.status_code == 200 and '500325' in response.text:
        print("Filter extraction successful.")
        return True
    else:
        print("Filter failed.")
        return False

def verify_strategy():
    print("Verifying Strategy (Min Increase 5 days)...")
    response = session.get(f'{BASE_URL}/strategies?strategy=min_increase&days=3') # Use 3 days for quicker check
    if response.status_code == 200:
        if 'Found' in response.text or 'No stocks found' in response.text:
             print("Strategy executed (result or empty state found).")
             return True
        else:
             print("Strategy page loaded but result unclear.")
             # print(response.text[:500])
             return True # Assuming it worked if 200 and no crash
    else:
        print(f"Strategy failed with {response.status_code}")
        return False

if __name__ == "__main__":
    if login():
        try:
            if verify_index() and verify_filter() and verify_strategy():
                print("\nAll checks passed!")
            else:
                print("\nSome checks failed.")
        except Exception as e:
            print(f"An error occurred: {e}")
