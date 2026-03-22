import sqlite3
import os

db_path = r"C:\Users\ANTEC MSI\Desktop\pro\Option trading\sp500_options.db"
conn = sqlite3.connect(db_path)

# 1. Earnings table range and count per year
print("=== EARNINGS TABLE ===")
row = conn.execute("SELECT MIN(report_date), MAX(report_date), COUNT(*) FROM earnings").fetchone()
print(f"  Range: {row[0]} - {row[1]}, Total: {row[2]:,}")

rows = conn.execute("""
    SELECT CAST(report_date/10000 AS INT) as year, COUNT(*) as cnt, COUNT(DISTINCT root) as tickers
    FROM earnings
    GROUP BY year ORDER BY year
""").fetchall()
print("\n  Year  | Events | Tickers")
print("  ------|--------|--------")
for r in rows:
    print(f"  {r[0]}  | {r[1]:>6,} | {r[2]:>5}")

# 2. EOD History: dates per year, records per year
print("\n=== EOD HISTORY ===")
row = conn.execute("SELECT MIN(date), MAX(date), COUNT(*) FROM eod_history").fetchone()
print(f"  Range: {row[0]} - {row[1]}, Total: {row[2]:,}")

rows = conn.execute("""
    SELECT CAST(date/10000 AS INT) as year, COUNT(DISTINCT date) as dates, COUNT(*) as records,
           SUM(CASE WHEN volume > 0 THEN 1 ELSE 0 END) as with_vol
    FROM eod_history
    GROUP BY year ORDER BY year
""").fetchall()
print("\n  Year  | Trading Days | Records      | With Volume  | Vol%")
print("  ------|-------------|--------------|--------------|-----")
for r in rows:
    vol_pct = r[3]/r[2]*100 if r[2] > 0 else 0
    print(f"  {r[0]}  | {r[1]:>11} | {r[2]:>12,} | {r[3]:>12,} | {vol_pct:.1f}%")

# 3. Contracts table
print("\n=== CONTRACTS ===")
row = conn.execute("SELECT COUNT(*), COUNT(DISTINCT root), MIN(expiration), MAX(expiration) FROM contracts").fetchone()
print(f"  Total contracts: {row[0]:,}, Unique tickers: {row[1]}, Expiration range: {row[2]} - {row[3]}")

# 4. Check for gaps in Q1 2016 specifically (the problematic period)
print("\n=== Q1 2016 DETAIL ===")
rows = conn.execute("""
    SELECT date, COUNT(*) as records, SUM(CASE WHEN volume > 0 THEN 1 ELSE 0 END) as with_vol
    FROM eod_history
    WHERE date >= 20160101 AND date <= 20160331
    GROUP BY date ORDER BY date
""").fetchall()
print(f"  Trading days in Q1 2016: {len(rows)}")
if rows:
    print(f"  First date: {rows[0][0]}, Last date: {rows[-1][0]}")
    total_rec = sum(r[1] for r in rows)
    total_vol = sum(r[2] for r in rows)
    print(f"  Total records: {total_rec:,}, With volume: {total_vol:,} ({total_vol/total_rec*100:.1f}%)")

# 5. Check earnings in Q1 2016
print("\n=== Q1 2016 EARNINGS ===")
rows = conn.execute("""
    SELECT report_date, root, before_after
    FROM earnings
    WHERE report_date >= 20160101 AND report_date <= 20160331
    ORDER BY report_date
    LIMIT 30
""").fetchall()
print(f"  Total Q1 2016 earnings events: ", end="")
cnt = conn.execute("SELECT COUNT(*) FROM earnings WHERE report_date >= 20160101 AND report_date <= 20160331").fetchone()[0]
print(f"{cnt}")
print(f"  First 30:")
for r in rows:
    print(f"    {r[0]} {r[1]:>5} ({r[2]})")

# 6. Check if H1 2016 has enough option data
print("\n=== H1 2016 OPTION DATA DENSITY ===")
rows = conn.execute("""
    SELECT SUBSTR(CAST(date AS TEXT), 1, 6) as month, 
           COUNT(DISTINCT date) as days,
           COUNT(*) as records,
           SUM(CASE WHEN volume > 0 THEN 1 ELSE 0 END) as with_vol,
           COUNT(DISTINCT c.root) as tickers
    FROM eod_history e
    JOIN contracts c ON e.contract_id = c.contract_id
    WHERE date >= 20160101 AND date <= 20160630
    GROUP BY month ORDER BY month
""").fetchall()
print("  Month  | Days | Records      | With Vol     | Tickers")
print("  -------|------|--------------|--------------|--------")
for r in rows:
    print(f"  {r[0]}  | {r[1]:>4} | {r[2]:>12,} | {r[3]:>12,} | {r[4]:>5}")

conn.close()
print("\nDone.")
