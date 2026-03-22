"""Quick test of EODHD options API."""
import requests
import json
from datetime import datetime, timedelta

API_KEY = "688202596a2968.33250849"

today = datetime.now()
exp_from = (today + timedelta(days=20)).strftime("%Y-%m-%d")
exp_to = (today + timedelta(days=100)).strftime("%Y-%m-%d")

# Test 1: Stock price
print("=== STOCK PRICE ===")
url = f"https://eodhd.com/api/real-time/AAPL.US?api_token={API_KEY}&fmt=json"
resp = requests.get(url, timeout=15)
print(f"Status: {resp.status_code}")
if resp.status_code == 200:
    data = resp.json()
    print(f"Data: {json.dumps(data, indent=2)[:500]}")

# Test 2: Options chain (marketplace endpoint)
print("\n=== OPTIONS CHAIN (marketplace) ===")
url = (f"https://eodhd.com/api/mp/unicornbay/options/eod?"
       f"filter[underlying_symbol]=AAPL"
       f"&filter[exp_date_from]={exp_from}"
       f"&filter[exp_date_to]={exp_to}"
       f"&sort=strike"
       f"&page[limit]=10"
       f"&compact=1"
       f"&api_token={API_KEY}")
print(f"URL: {url[:120]}...")
resp = requests.get(url, timeout=30)
print(f"Status: {resp.status_code}")
if resp.status_code == 200:
    data = resp.json()
    if isinstance(data, list):
        print(f"Rows: {len(data)}")
        if len(data) > 0:
            print(f"Columns: {list(data[0].keys())}")
            print(f"Sample: {json.dumps(data[0], indent=2)}")
    elif isinstance(data, dict):
        print(f"Keys: {list(data.keys())[:20]}")
        print(f"Data: {json.dumps(data, indent=2)[:800]}")
    else:
        print(f"Type: {type(data)}, Content: {str(data)[:500]}")
else:
    print(f"Error: {resp.text[:500]}")

# Test 3: Options v2 endpoint
print("\n=== OPTIONS CHAIN (v2) ===")
today_str = today.strftime("%Y-%m-%d")
url = (f"https://eodhd.com/api/v2/options/AAPL.US?"
       f"from={today_str}&to={today_str}"
       f"&api_token={API_KEY}")
print(f"URL: {url[:120]}...")
resp = requests.get(url, timeout=30)
print(f"Status: {resp.status_code}")
if resp.status_code == 200:
    data = resp.json()
    if isinstance(data, list):
        print(f"Rows: {len(data)}")
        if len(data) > 0:
            first = data[0]
            print(f"Keys: {list(first.keys())[:20]}")
            print(f"Sample: {json.dumps(first, indent=2)[:800]}")
    elif isinstance(data, dict):
        print(f"Keys: {list(data.keys())[:20]}")
        # Check nested structure
        for k, v in data.items():
            if isinstance(v, list):
                print(f"  {k}: list of {len(v)} items")
                if len(v) > 0 and isinstance(v[0], dict):
                    print(f"    Keys: {list(v[0].keys())[:15]}")
                    print(f"    Sample: {json.dumps(v[0], indent=2)[:400]}")
            else:
                print(f"  {k}: {str(v)[:100]}")
else:
    print(f"Error: {resp.text[:500]}")

# Test 4: Earnings calendar
print("\n=== EARNINGS CALENDAR ===")
url = (f"https://eodhd.com/api/calendar/earnings?"
       f"symbols=AAPL.US&api_token={API_KEY}&fmt=json")
resp = requests.get(url, timeout=15)
print(f"Status: {resp.status_code}")
if resp.status_code == 200:
    data = resp.json()
    print(f"Data: {json.dumps(data, indent=2)[:500]}")
