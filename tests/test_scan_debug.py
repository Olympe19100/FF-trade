"""Debug scan for one ticker — show all FF values."""
import requests
import numpy as np
import pandas as pd
from datetime import datetime, timedelta

API_KEY = "688202596a2968.33250849"
BASE_URL = "https://eodhd.com/api"
DTE_COMBOS = [(30, 60), (30, 90), (60, 90)]
DTE_TOL = 5
STRIKE_PCT = 0.03

TICKER = "AAPL"
today = datetime.now()

# Get stock price
resp = requests.get(f"{BASE_URL}/real-time/{TICKER}.US?api_token={API_KEY}&fmt=json", timeout=15)
stock_px = resp.json()["close"]
print(f"Stock price: ${stock_px}")

# Fetch chain
exp_from = (today + timedelta(days=20)).strftime("%Y-%m-%d")
exp_to = (today + timedelta(days=100)).strftime("%Y-%m-%d")
print(f"Expiry range: {exp_from} to {exp_to}")

strike_lo = int(stock_px * 0.95)
strike_hi = int(stock_px * 1.05)
print(f"Strike range: {strike_lo} to {strike_hi}")

url = (f"{BASE_URL}/mp/unicornbay/options/eod?"
       f"filter[underlying_symbol]={TICKER}"
       f"&filter[type]=call"
       f"&filter[exp_date_from]={exp_from}"
       f"&filter[exp_date_to]={exp_to}"
       f"&filter[strike_from]={strike_lo}"
       f"&filter[strike_to]={strike_hi}"
       f"&sort=strike"
       f"&page[limit]=500"
       f"&api_token={API_KEY}")

resp = requests.get(url, timeout=30)
result = resp.json()
rows = result.get("data", [])
print(f"Options fetched: {len(rows)}")

if not rows:
    print("No data!")
    exit()

# JSONAPI format: each row has {id, type, attributes}
# Flatten attributes
print(f"Row format: {list(rows[0].keys())}")
if "attributes" in rows[0]:
    print(f"Attributes: {list(rows[0]['attributes'].keys())[:15]}")
    flat_rows = [r["attributes"] for r in rows]
else:
    flat_rows = rows
chain = pd.DataFrame(flat_rows)
print(f"Columns: {list(chain.columns)}")

# Parse
chain["bid"] = pd.to_numeric(chain["bid"], errors="coerce")
chain["ask"] = pd.to_numeric(chain["ask"], errors="coerce")
chain["strike"] = pd.to_numeric(chain["strike"], errors="coerce")
chain["volatility"] = pd.to_numeric(chain["volatility"], errors="coerce")

# Compute DTE
chain["exp_dt"] = pd.to_datetime(chain["exp_date"], errors="coerce")
chain["dte"] = (chain["exp_dt"] - pd.Timestamp(today)).dt.days

chain = chain[(chain["bid"] > 0) & (chain["ask"] > 0) &
              chain["volatility"].notna() & (chain["volatility"] > 0)]
chain["mid"] = (chain["bid"] + chain["ask"]) / 2
chain["iv"] = chain["volatility"]

print(f"\nValid options: {len(chain)}")
print(f"DTE range: {chain['dte'].min()} to {chain['dte'].max()}")
print(f"Strike range: {chain['strike'].min()} to {chain['strike'].max()}")
print(f"Unique expirations: {sorted(chain['exp_date'].unique())}")

# Show DTE distribution
for exp, grp in chain.groupby("exp_date"):
    dte = grp["dte"].iloc[0]
    n_strikes = len(grp)
    atm = grp.loc[(grp["strike"] - stock_px).abs().idxmin()]
    print(f"  {exp} (DTE={dte:3d}): {n_strikes} strikes, "
          f"ATM strike={atm['strike']:.0f}, IV={atm['iv']:.4f}, "
          f"mid=${atm['mid']:.2f}")

# Try each DTE combo
print(f"\n{'='*60}")
for short_dte, long_dte in DTE_COMBOS:
    print(f"\n  Combo {short_dte}-{long_dte}:")

    front = chain[(chain["dte"] >= short_dte - DTE_TOL) &
                   (chain["dte"] <= short_dte + DTE_TOL)]
    back = chain[(chain["dte"] >= long_dte - DTE_TOL) &
                  (chain["dte"] <= long_dte + DTE_TOL)]

    print(f"    Front candidates: {len(front)} (DTE {short_dte}+/-{DTE_TOL})")
    print(f"    Back candidates: {len(back)} (DTE {long_dte}+/-{DTE_TOL})")

    if front.empty or back.empty:
        print(f"    SKIP: no front or back options")
        continue

    # ATM
    front["strike_pct"] = (front["strike"] - stock_px).abs() / stock_px
    front_atm = front.loc[front["strike_pct"].idxmin()]
    strike = front_atm["strike"]
    front_iv = front_atm["iv"]
    front_mid = front_atm["mid"]
    front_dte_val = front_atm["dte"]

    print(f"    Front ATM: K={strike:.0f}, IV={front_iv:.4f}, mid=${front_mid:.2f}, "
          f"DTE={front_dte_val}")

    # Back at same strike
    back_same = back[(back["strike"] - strike).abs() < 1]
    if back_same.empty:
        back["sdiff"] = (back["strike"] - strike).abs()
        back_best = back.loc[back["sdiff"].idxmin()]
    else:
        back_best = back_same.iloc[0]

    back_iv = back_best["iv"]
    back_mid = back_best["mid"]
    back_dte_val = back_best["dte"]

    print(f"    Back:     K={back_best['strike']:.0f}, IV={back_iv:.4f}, "
          f"mid=${back_mid:.2f}, DTE={back_dte_val}")

    spread_cost = back_mid - front_mid
    print(f"    Spread cost: ${spread_cost:.2f}")

    # FF (OLD formula: fwd_var / front_var - 1, aligned with PDF)
    FF_THRESHOLD_OLD = {"30-60": 0.230, "30-90": 0.230, "60-90": 0.200}
    combo_key = f"{short_dte}-{long_dte}"
    ff_thresh = FF_THRESHOLD_OLD.get(combo_key, 0.230)

    T_f = front_dte_val / 365.0
    T_b = back_dte_val / 365.0
    dT = T_b - T_f
    if dT > 0 and front_iv > 0 and back_iv > 0:
        front_var = front_iv**2
        fwd_var = (back_iv**2 * T_b - front_iv**2 * T_f) / dT
        if fwd_var > 0:
            sigma_fwd = np.sqrt(fwd_var)
            ff_old = fwd_var / front_var - 1  # OLD formula
            ff_gui = front_iv / sigma_fwd - 1  # GUI formula (for display)
            print(f"    Forward vol: {sigma_fwd:.4f}")
            print(f"    FF_old = {ff_old:.3f}  (threshold={ff_thresh:.3f})  "
                  f"{'<-- SIGNAL!' if ff_old >= ff_thresh else '(below threshold)'}")
            print(f"    FF_gui = {ff_gui*100:.1f}%")
        else:
            print(f"    Forward variance negative! ({fwd_var:.6f})")
    else:
        print(f"    Cannot compute FF (dT={dT:.4f})")
