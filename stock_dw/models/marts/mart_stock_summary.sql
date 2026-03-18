-- Daily summary per stock
SELECT
    symbol,
    price_date,
    price_year,
    price_month,
    open,
    high,
    low,
    close,
    volume,
    price_change,
    pct_change,
    price_direction,
    -- Moving averages
    ROUND(AVG(close) OVER (
        PARTITION BY symbol
        ORDER BY price_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    )::NUMERIC, 4)               AS moving_avg_7d,
    ROUND(AVG(close) OVER (
        PARTITION BY symbol
        ORDER BY price_date
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    )::NUMERIC, 4)               AS moving_avg_30d,
    -- 52 week high/low
    MAX(high) OVER (
        PARTITION BY symbol
        ORDER BY price_date
        ROWS BETWEEN 251 PRECEDING AND CURRENT ROW
    )                            AS week_52_high,
    MIN(low) OVER (
        PARTITION BY symbol
        ORDER BY price_date
        ROWS BETWEEN 251 PRECEDING AND CURRENT ROW
    )                            AS week_52_low
FROM {{ ref('stg_stock_prices') }}