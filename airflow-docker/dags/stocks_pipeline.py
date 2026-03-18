from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess
import os

default_args = {
    'owner': 'rithesh',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='stocks_elt_pipeline',
    default_args=default_args,
    description='ELT pipeline: Alpha Vantage API → S3 → PostgreSQL → dbt → Great Expectations',
    schedule_interval='0 6 * * 1-5',  # 6am Mon-Fri (weekdays only)
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['stocks', 'aws', 'data-engineering']
)

DATA_PATH = '/opt/airflow'
DBT_PATH  = '/opt/airflow/stock_dw'

SNOWFLAKE_CONFIG = {
    'host'    : os.environ.get('POSTGRES_HOST'),
    'port'    : os.environ.get('POSTGRES_PORT'),
    'dbname'  : os.environ.get('POSTGRES_DB'),
    'user'    : os.environ.get('POSTGRES_USER'),
    'password': os.environ.get('POSTGRES_PASSWORD')
}

def task_fetch_and_upload():
    """Fetch stock data from Alpha Vantage and upload to S3"""
    import requests
    import boto3
    import json
    import pandas as pd
    from datetime import datetime as dt

    API_KEY = os.environ.get('ALPHA_VANTAGE_API_KEY')
    BUCKET  = os.environ.get('S3_BUCKET_NAME')
    SYMBOLS = ['MSFT', 'GOOGL', 'AMZN', 'INFY', 'TCS']

    s3 = boto3.client(
        's3',
        aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'),
        region_name=os.environ.get('AWS_DEFAULT_REGION')
    )

    all_data = []
    for symbol in SYMBOLS:
        url = (
            f"https://www.alphavantage.co/query"
            f"?function=TIME_SERIES_DAILY"
            f"&symbol={symbol}"
            f"&outputsize=compact"
            f"&apikey={API_KEY}"
        )
        response    = requests.get(url)
        data        = response.json()
        time_series = data.get('Time Series (Daily)', {})

        if not time_series:
            print(f"⚠️ No data for {symbol}")
            continue

        rows = []
        for date, values in time_series.items():
            rows.append({
                'symbol'    : symbol,
                'date'      : date,
                'open'      : float(values['1. open']),
                'high'      : float(values['2. high']),
                'low'       : float(values['3. low']),
                'close'     : float(values['4. close']),
                'volume'    : int(values['5. volume']),
                'fetched_at': dt.utcnow().isoformat()
            })

        df      = pd.DataFrame(rows)
        today   = dt.utcnow().strftime('%Y-%m-%d')
        s3_key  = f"raw/stocks/{symbol}/{today}.json"
        s3.put_object(
            Bucket=BUCKET,
            Key=s3_key,
            Body=df.to_json(orient='records'),
            ContentType='application/json'
        )
        print(f"✅ {symbol}: {len(df)} days → S3")
        all_data.append(df)

    if all_data:
        combined = pd.concat(all_data, ignore_index=True)
        s3_key   = f"raw/combined/{today}_all_stocks.json"
        s3.put_object(
            Bucket=BUCKET,
            Key=s3_key,
            Body=combined.to_json(orient='records'),
            ContentType='application/json'
        )
        print(f"✅ Combined: {len(combined)} records → S3")


def task_load_to_postgres():
    """Load data from S3 into PostgreSQL"""
    import boto3
    import psycopg2
    from psycopg2.extras import execute_values
    import pandas as pd
    import json
    from datetime import datetime as dt

    s3 = boto3.client(
        's3',
        aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'),
        region_name=os.environ.get('AWS_DEFAULT_REGION')
    )

    BUCKET  = os.environ.get('S3_BUCKET_NAME')
    today   = dt.utcnow().strftime('%Y-%m-%d')
    s3_key  = f"raw/combined/{today}_all_stocks.json"

    response = s3.get_object(Bucket=BUCKET, Key=s3_key)
    data     = json.loads(response['Body'].read().decode('utf-8'))
    df       = pd.DataFrame(data)

    conn = psycopg2.connect(**SNOWFLAKE_CONFIG)
    cur  = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS raw_stock_prices (
            symbol VARCHAR(10), date DATE, open NUMERIC(10,4),
            high NUMERIC(10,4), low NUMERIC(10,4), close NUMERIC(10,4),
            volume BIGINT, fetched_at TIMESTAMP, loaded_at TIMESTAMP DEFAULT NOW()
        )
    """)
    cur.execute("TRUNCATE TABLE raw_stock_prices")

    rows = [
        (r['symbol'], r['date'], r['open'], r['high'],
         r['low'], r['close'], r['volume'], r['fetched_at'])
        for _, r in df.iterrows()
    ]
    execute_values(cur, """
        INSERT INTO raw_stock_prices
        (symbol, date, open, high, low, close, volume, fetched_at)
        VALUES %s
    """, rows)

    conn.commit()
    cur.execute("SELECT COUNT(*) FROM raw_stock_prices")
    count = cur.fetchone()[0]
    print(f"✅ Loaded {count} records into PostgreSQL")
    cur.close()
    conn.close()


def task_validate_data():
    """Run data quality checks"""
    import psycopg2
    import pandas as pd

    conn = psycopg2.connect(**SNOWFLAKE_CONFIG)
    df   = pd.read_sql("SELECT * FROM raw_stock_prices", conn)
    conn.close()

    failures = []

    checks = [
        ("No null symbols",            df['symbol'].isnull().sum() == 0),
        ("No null dates",              df['date'].isnull().sum() == 0),
        ("Close price > 0",            (df['close'] > 0).all()),
        ("High >= Low",                (df['high'] >= df['low']).all()),
        ("Open price > 0",             (df['open'] > 0).all()),
        ("At least 3 symbols present", len(set(df['symbol'].unique())) >= 3),
        ("No duplicate symbol+date",   df.duplicated(subset=['symbol','date']).sum() == 0),
        ("Close within range",         ((df['close'] >= 0.01) & (df['close'] <= 10000)).all()),
    ]

    for description, condition in checks:
        if condition:
            print(f"✅ {description}")
        else:
            print(f"❌ FAILED: {description}")
            failures.append(description)

    if failures:
        raise Exception(f"Data quality failed: {failures}")

    print("✅ All data quality checks passed")


def task_run_dbt():
    """Run dbt models"""
    result = subprocess.run(
        ["dbt", "run", "--profiles-dir", "."],
        cwd=DBT_PATH,
        capture_output=True,
        text=True
    )
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        raise Exception("❌ dbt run failed")
    print("✅ dbt run complete")


def task_run_dbt_tests():
    """Run dbt tests"""
    result = subprocess.run(
        ["dbt", "test", "--profiles-dir", "."],
        cwd=DBT_PATH,
        capture_output=True,
        text=True
    )
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        raise Exception("❌ dbt tests failed")
    print("✅ dbt tests passed")


def task_pipeline_complete():
    """Log pipeline summary"""
    import psycopg2
    conn = psycopg2.connect(**SNOWFLAKE_CONFIG)
    cur  = conn.cursor()
    cur.execute("SELECT symbol, COUNT(*), MAX(close) FROM raw_stock_prices GROUP BY symbol")
    results = cur.fetchall()
    cur.close()
    conn.close()

    print("=" * 50)
    print("✅ STOCKS ELT PIPELINE COMPLETED")
    print(f"   Run time: {datetime.now()}")
    print("\n   Stock Summary:")
    for symbol, count, latest_close in results:
        print(f"   {symbol}: {count} days | Latest close: ${latest_close}")
    print("=" * 50)


# Define tasks
t1 = PythonOperator(task_id='fetch_and_upload_to_s3',  python_callable=task_fetch_and_upload,  dag=dag)
t2 = PythonOperator(task_id='load_to_postgres',         python_callable=task_load_to_postgres,  dag=dag)
t3 = PythonOperator(task_id='validate_data',            python_callable=task_validate_data,     dag=dag)
t4 = PythonOperator(task_id='run_dbt_models',           python_callable=task_run_dbt,           dag=dag)
t5 = PythonOperator(task_id='run_dbt_tests',            python_callable=task_run_dbt_tests,     dag=dag)
t6 = PythonOperator(task_id='pipeline_complete',        python_callable=task_pipeline_complete, dag=dag)

# Task order
t1 >> t2 >> t3 >> t4 >> t5 >> t6