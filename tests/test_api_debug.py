"""Explore /api/options/ structure in detail."""
import requests
import pandas as pd
from datetime import datetime

API_KEY = "688202596a2968.33250849"
today = datetime.now()

url = f"https://eodhd.com/api/options/AAPL.US?api_token={API_KEY}&fmt=json"
resp = requests.get(url, timeout=30)
data = resp.json()

stock_px = data["lastTradePrice"]
print(f"Stock: ${stock_px}, Last trade: {data['lastTradeDate']}")
print(f"Expirations: {len(data['data'])}")

# Show all expirations
print(f"\n=== ALL EXPIRATIONS ===")
for exp_data in data["data"]:
    exp_date = exp_data["expirationDate"]
    dte = (pd.Timestamp(exp_date) - pd.Timestamp(today)).days
    calls = exp_data["options"].get("CALL", [])
    puts = exp_data["options"].get("PUT", [])
    print(f"  {exp_date}  DTE={dte:4d}  calls={len(calls):4d}  puts={len(puts):4d}")

# Show ATM calls for first 5 expirations with DTE > 15
print(f"\n=== ATM CALLS ===")
for exp_data in data["data"]:
    exp_date = exp_data["expirationDate"]
    dte = (pd.Timestamp(exp_date) - pd.Timestamp(today)).days
    if dte < 15 or dte > 120:
        continue
    calls = exp_data["options"].get("CALL", [])
    print(f"\n  {exp_date} (DTE={dte}):")
    for c in calls:
        strike = float(c.get("strike", 0))
        if abs(strike - stock_px) <= 5:
            iv = float(c.get("impliedVolatility", 0))
            bid = float(c.get("bid", 0))
            ask = float(c.get("ask", 0))
            vol = int(c.get("volume", 0))
            mid = (bid + ask) / 2
            print(f"    K={strike:>7.1f}  bid={bid:>6.2f}  ask={ask:>6.2f}  "
                  f"mid={mid:>6.2f}  IV={iv:.4f}  vol={vol}")
