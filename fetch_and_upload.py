import requests
import boto3
import json
import os
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# Config
API_KEY    = os.getenv('ALPHA_VANTAGE_API_KEY')
BUCKET     = os.getenv('S3_BUCKET_NAME')
REGION     = os.getenv('AWS_DEFAULT_REGION')
SYMBOLS    = ['MSFT', 'GOOGL', 'AMZN', 'INFY', 'TCS']

s3 = boto3.client(
    's3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    region_name=REGION
)

def fetch_stock_data(symbol):
    """Fetch daily stock data from Alpha Vantage"""
    url = (
        f"https://www.alphavantage.co/query"
        f"?function=TIME_SERIES_DAILY"
        f"&symbol={symbol}"
        f"&outputsize=compact"
        f"&apikey={API_KEY}"
    )
    response = requests.get(url)
    data     = response.json()

    time_series = data.get('Time Series (Daily)', {})
    if not time_series:
        print(f"⚠️  No data for {symbol} — API limit may be reached")
        return None

    rows = []
    for date, values in time_series.items():
        rows.append({
            'symbol':     symbol,
            'date':       date,
            'open':       float(values['1. open']),
            'high':       float(values['2. high']),
            'low':        float(values['3. low']),
            'close':      float(values['4. close']),
            'volume':     int(values['5. volume']),
            'fetched_at': datetime.utcnow().isoformat()
        })

    return pd.DataFrame(rows)

def upload_to_s3(df, symbol):
    """Upload stock data as JSON to S3"""
    today     = datetime.utcnow().strftime('%Y-%m-%d')
    s3_key    = f"raw/stocks/{symbol}/{today}.json"
    json_data = df.to_json(orient='records', indent=2)

    s3.put_object(
        Bucket=BUCKET,
        Key=s3_key,
        Body=json_data,
        ContentType='application/json'
    )
    print(f"✅ {symbol}: {len(df)} days uploaded → s3://{BUCKET}/{s3_key}")

def main():
    print(f"Starting stock data ingestion — {datetime.utcnow().strftime('%Y-%m-%d %H:%M')} UTC")
    print(f"Fetching data for: {', '.join(SYMBOLS)}\n")

    all_data = []
    for symbol in SYMBOLS:
        print(f"Fetching {symbol}...")
        df = fetch_stock_data(symbol)
        if df is not None:
            upload_to_s3(df, symbol)
            all_data.append(df)
        # Alpha Vantage free tier: 25 calls/day, no rate limit between calls

    # Also save combined file
    if all_data:
        combined = pd.concat(all_data, ignore_index=True)
        today    = datetime.utcnow().strftime('%Y-%m-%d')
        s3_key   = f"raw/combined/{today}_all_stocks.json"
        s3.put_object(
            Bucket=BUCKET,
            Key=s3_key,
            Body=combined.to_json(orient='records', indent=2),
            ContentType='application/json'
        )
        print(f"\n✅ Combined file uploaded → s3://{BUCKET}/{s3_key}")
        print(f"   Total records: {len(combined)}")
        print(f"   Symbols: {combined['symbol'].unique().tolist()}")

if __name__ == "__main__":
    main()