import pandas as pd
import psycopg2
import os
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

# Connect to PostgreSQL
conn = psycopg2.connect(
    host=os.getenv('POSTGRES_HOST'),
    port=os.getenv('POSTGRES_PORT'),
    dbname=os.getenv('POSTGRES_DB'),
    user=os.getenv('POSTGRES_USER'),
    password=os.getenv('POSTGRES_PASSWORD')
)

# Load data into pandas
df = pd.read_sql("SELECT * FROM raw_stock_prices", conn)
conn.close()

print(f"Validating {len(df)} records...\n")

# ── Run Expectations ──────────────────────────────────────────
failures = []
passed   = 0

def check(description, condition):
    global passed
    if condition:
        print(f"  ✅ {description}")
        passed += 1
    else:
        print(f"  ❌ FAILED: {description}")
        failures.append(description)

print("=== Data Quality Checks ===\n")

# 1. No null symbols
check(
    "symbol column has no nulls",
    df['symbol'].isnull().sum() == 0
)

# 2. No null dates
check(
    "date column has no nulls",
    df['date'].isnull().sum() == 0
)

# 3. Close price always positive
check(
    "close price is always > 0",
    (df['close'] > 0).all()
)

# 4. High >= Low always
check(
    "high price is always >= low price",
    (df['high'] >= df['low']).all()
)

# 5. Volume check — allow zero for non-US listed stocks (TCS, INFY)
zero_volume_us_stocks = df[
    (df['volume'] == 0) & 
    (~df['symbol'].isin(['TCS', 'INFY']))
].shape[0]
check(
    "US-listed stocks (MSFT, GOOGL, AMZN) have no zero volume days",
    zero_volume_us_stocks == 0
)

# 6. Open price always positive
check(
    "open price is always > 0",
    (df['open'] > 0).all()
)

# 7. All 5 symbols present
expected_symbols = {'MSFT', 'GOOGL', 'AMZN', 'INFY', 'TCS'}
actual_symbols   = set(df['symbol'].unique())
check(
    f"all 5 symbols present — {expected_symbols}",
    expected_symbols == actual_symbols
)

# 8. No future dates
check(
    "no future dates in dataset",
    pd.to_datetime(df['date']).max() <= pd.Timestamp(datetime.utcnow().date())
)

# 9. Close price within reasonable range
check(
    "close price between $0.01 and $10,000",
    ((df['close'] >= 0.01) & (df['close'] <= 10000)).all()
)

# 10. High >= Close always
check(
    "high price is always >= close price",
    (df['high'] >= df['close']).all()
)

# 11. Low <= Close always
check(
    "low price is always <= close price",
    (df['low'] <= df['close']).all()
)

# 12. No duplicate symbol + date combinations
duplicates = df.duplicated(subset=['symbol', 'date']).sum()
check(
    "no duplicate symbol + date combinations",
    duplicates == 0
)

# 13. Minimum record count per symbol
min_records = df.groupby('symbol').size().min()
check(
    f"each symbol has at least 50 records (min found: {min_records})",
    min_records >= 50
)

# 14. Volume within reasonable range
check(
    "volume is always < 10 billion",
    (df['volume'] < 10_000_000_000).all()
)

# ── Summary ───────────────────────────────────────────────────
print(f"\n=== Validation Summary ===")
print(f"  Total checks : {passed + len(failures)}")
print(f"  Passed       : {passed}")
print(f"  Failed       : {len(failures)}")

if failures:
    print(f"\n❌ PIPELINE BLOCKED — Fix these issues:")
    for f in failures:
        print(f"   - {f}")
    exit(1)
else:
    print(f"\n✅ ALL CHECKS PASSED — Data quality confirmed!")
    print(f"   Safe to proceed with dbt transformations.")