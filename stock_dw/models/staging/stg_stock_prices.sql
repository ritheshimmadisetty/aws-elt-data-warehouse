SELECT
    UPPER(TRIM(symbol))          AS symbol,
    date::DATE                   AS price_date,
    EXTRACT(YEAR FROM date)      AS price_year,
    EXTRACT(MONTH FROM date)     AS price_month,
    EXTRACT(DOW FROM date)       AS day_of_week,
    open,
    high,
    low,
    close,
    volume,
    ROUND((close - open)::NUMERIC, 4)              AS price_change,
    ROUND(((close - open) / open * 100)::NUMERIC, 2) AS pct_change,
    CASE
        WHEN close > open THEN 'UP'
        WHEN close < open THEN 'DOWN'
        ELSE 'FLAT'
    END                          AS price_direction,
    fetched_at::TIMESTAMP        AS fetched_at
FROM {{ source('raw', 'raw_stock_prices') }}
WHERE symbol   IS NOT NULL
  AND date     IS NOT NULL
  AND close    > 0
  AND volume   > 0