-- Overall performance metrics per stock
SELECT
    symbol,
    COUNT(*)                          AS total_trading_days,
    MIN(price_date)                   AS first_date,
    MAX(price_date)                   AS last_date,
    ROUND(MIN(low)::NUMERIC, 4)       AS all_time_low,
    ROUND(MAX(high)::NUMERIC, 4)      AS all_time_high,
    ROUND(AVG(close)::NUMERIC, 4)     AS avg_close_price,
    ROUND(AVG(volume)::NUMERIC, 0)    AS avg_daily_volume,
    SUM(CASE WHEN price_direction = 'UP'   THEN 1 ELSE 0 END) AS up_days,
    SUM(CASE WHEN price_direction = 'DOWN' THEN 1 ELSE 0 END) AS down_days,
    SUM(CASE WHEN price_direction = 'FLAT' THEN 1 ELSE 0 END) AS flat_days,
    ROUND(
        SUM(CASE WHEN price_direction = 'UP' THEN 1 ELSE 0 END)::NUMERIC
        / COUNT(*) * 100, 2
    )                                 AS win_rate_pct
FROM {{ ref('stg_stock_prices') }}
GROUP BY symbol
ORDER BY avg_close_price DESC