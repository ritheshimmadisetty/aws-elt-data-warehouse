# Automated ELT Data Warehouse (AWS)

An automated ELT pipeline that fetches real stock market data from Alpha Vantage API, 
stores it in AWS S3, catalogs it with AWS Glue, loads into PostgreSQL, and transforms 
it with dbt — with Great Expectations for data quality and Airflow for orchestration.

## What this project does

Fetches daily stock prices for 5 companies (MSFT, GOOGL, AMZN, INFY, TCS) from a 
public API, runs them through a full ELT pipeline, and delivers clean analytical 
tables with moving averages and performance metrics.

## Tech Stack

- **Storage:** AWS S3
- **Catalog:** AWS Glue (schema detection + data catalog)
- **Warehouse:** PostgreSQL (Docker)
- **Transformation:** dbt
- **Data Quality:** Great Expectations (14 validation checks)
- **Orchestration:** Apache Airflow, Docker
- **CI/CD:** GitHub Actions
- **Programming:** Python, SQL

## Pipeline Flow
```
Alpha Vantage API (real stock data)
         ↓
AWS S3 (raw JSON by symbol and date)
         ↓
AWS Glue (auto-detects schema → stocks_catalog)
         ↓
PostgreSQL — raw_stock_prices table
         ↓
Great Expectations (14 data quality checks)
         ↓
dbt — stg_stock_prices → mart_stock_summary → mart_stock_performance
```

## dbt Models

- `stg_stock_prices` — cleaned prices with price direction and % change
- `mart_stock_summary` — daily summary with 7-day and 30-day moving averages
- `mart_stock_performance` — overall metrics: win rate, all-time high/low, avg volume

## Airflow DAG

Runs every weekday at 6am with 6 tasks:
```
fetch_and_upload_to_s3
         ↓
load_to_postgres
         ↓
validate_data          ← 14 Great Expectations checks
         ↓
run_dbt_models
         ↓
run_dbt_tests
         ↓
pipeline_complete
```

## Sample Output
```
symbol | total_trading_days | all_time_high | win_rate_pct
-------+--------------------+---------------+-------------
GOOGL  |                100 |      349.0000 |        51.00
AMZN   |                100 |      258.6000 |        48.00
MSFT   |                100 |      553.7200 |        46.00
```

## How to run
```bash
# Start PostgreSQL
docker run --name postgres-dw -e POSTGRES_USER=admin \
  -e POSTGRES_PASSWORD=admin123 -e POSTGRES_DB=stocks_dw \
  -p 5432:5432 -d postgres:13

# Fetch data and load to S3 + PostgreSQL
python fetch_and_upload.py
python load_to_postgres.py

# Run data quality checks
python validate_data.py

# Run dbt
cd stock_dw
dbt run --profiles-dir .
dbt test --profiles-dir .

# Start Airflow
cd airflow-docker
docker compose up -d
# Open http://localhost:8081
```

## Author

Rithesh Immadisetty — Data Engineer, Bengaluru
[LinkedIn](https://linkedin.com/in/ritheshimmadisetty)