import boto3
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import json
import os
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

# AWS
s3 = boto3.client(
    's3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    region_name=os.getenv('AWS_DEFAULT_REGION')
)

BUCKET = os.getenv('S3_BUCKET_NAME')

# PostgreSQL
conn = psycopg2.connect(
    host=os.getenv('POSTGRES_HOST'),
    port=os.getenv('POSTGRES_PORT'),
    dbname=os.getenv('POSTGRES_DB'),
    user=os.getenv('POSTGRES_USER'),
    password=os.getenv('POSTGRES_PASSWORD')
)
cursor = conn.cursor()

# Create raw table
cursor.execute("""
    CREATE TABLE IF NOT EXISTS raw_stock_prices (
        symbol       VARCHAR(10),
        date         DATE,
        open         NUMERIC(10,4),
        high         NUMERIC(10,4),
        low          NUMERIC(10,4),
        close        NUMERIC(10,4),
        volume       BIGINT,
        fetched_at   TIMESTAMP,
        loaded_at    TIMESTAMP DEFAULT NOW()
    )
""")

# Clear existing data for fresh load
cursor.execute("TRUNCATE TABLE raw_stock_prices")
conn.commit()
print("✅ PostgreSQL table ready")

# Read combined JSON from S3
today    = datetime.utcnow().strftime('%Y-%m-%d')
s3_key   = f"raw/combined/{today}_all_stocks.json"

print(f"Reading from S3: {s3_key}")

response = s3.get_object(Bucket=BUCKET, Key=s3_key)
data     = json.loads(response['Body'].read().decode('utf-8'))
df       = pd.DataFrame(data)

print(f"Records fetched: {len(df)}")

# Insert into PostgreSQL
rows = [
    (
        row['symbol'],
        row['date'],
        row['open'],
        row['high'],
        row['low'],
        row['close'],
        row['volume'],
        row['fetched_at']
    )
    for _, row in df.iterrows()
]

execute_values(cursor, """
    INSERT INTO raw_stock_prices 
    (symbol, date, open, high, low, close, volume, fetched_at)
    VALUES %s
""", rows)

conn.commit()

# Verify
cursor.execute("SELECT COUNT(*) FROM raw_stock_prices")
count = cursor.fetchone()[0]

cursor.execute("SELECT symbol, COUNT(*) FROM raw_stock_prices GROUP BY symbol ORDER BY symbol")
by_symbol = cursor.fetchall()

print(f"\n✅ Data loaded into PostgreSQL!")
print(f"   Total records: {count}")
print(f"\n   By symbol:")
for symbol, cnt in by_symbol:
    print(f"   {symbol}: {cnt} days")

cursor.close()
conn.close()