import requests

session = requests.Session()

# Login
login_url = 'http://127.0.0.1:5000/login'
credentials = {'username': 'rahul', 'password': 'rahul123'}

print("Logging in...")
response = session.post(login_url, data=credentials)

print(f"Login status: {response.status_code}")
if response.url == 'http://127.0.0.1:5000/':
    print("Login successful, redirected to index.")
else:
    print(f"Login failed/redirected to: {response.url}")

# Access Index to trigger get_data
print("Accessing index...")
response = session.get('http://127.0.0.1:5000/')
print(f"Index status: {response.status_code}")
# Check if data table is present in HTML
if '<table' in response.text:
    print("Table found in response.")
else:
    print("Table NOT found in response.")
