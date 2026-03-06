-- stg_sales.sql
-- Silver layer: clean, cast, and validate raw sales data

{{
    config(
        materialized = 'incremental',
        unique_key   = 'sale_id',
        incremental_strategy = 'merge',
        cluster_by   = ['order_date', 'territory_id'],
        tags         = ['staging', 'sales', 'incremental']
    )
}}

WITH source AS (
    SELECT * FROM {{ source('raw', 'sales') }}
    {% if is_incremental() %}
    WHERE _ingested_at > (SELECT MAX(_ingested_at) FROM {{ this }})
    {% endif %}
),

validated AS (
    SELECT
        sale_id,
        customer_id,
        product_id,
        territory_id,
        salesperson_id,
        order_date::DATE                                    AS order_date,
        ship_date::DATE                                     AS ship_date,
        ABS(quantity)                                       AS quantity,
        ROUND(ABS(unit_price), 4)                          AS unit_price,
        COALESCE(discount_pct, 0)                          AS discount_pct,
        ROUND(ABS(total_amount), 4)                        AS total_amount,
        UPPER(COALESCE(currency, 'USD'))                   AS currency,
        LOWER(TRIM(channel))                               AS channel,
        LOWER(TRIM(status))                                AS status,
        _ingested_at,
        _source,
        _file_name
    FROM source
    WHERE sale_id       IS NOT NULL
      AND customer_id   IS NOT NULL
      AND product_id    IS NOT NULL
      AND order_date    IS NOT NULL
      AND total_amount  > 0
      AND quantity      > 0
),

enriched AS (
    SELECT
        *,
        ROUND(total_amount * (1 - discount_pct), 4)        AS net_revenue,
        ROUND(total_amount * discount_pct, 4)              AS discount_amount,
        DATEDIFF('day', order_date, COALESCE(ship_date, order_date + 3)) AS days_to_ship,
        CASE
            WHEN total_amount >= 10000 THEN 'enterprise'
            WHEN total_amount >= 1000  THEN 'mid_market'
            WHEN total_amount >= 100   THEN 'smb'
            ELSE 'micro'
        END                                                AS deal_size_tier,
        DATE_TRUNC('month', order_date)                    AS order_month,
        DATE_TRUNC('quarter', order_date)                  AS order_quarter,
        YEAR(order_date)                                   AS order_year,
        CURRENT_TIMESTAMP()                                AS _transformed_at
    FROM validated
)

SELECT * FROM enriched
