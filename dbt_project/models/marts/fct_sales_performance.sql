-- fct_sales_performance.sql
-- Gold layer: pre-aggregated KPIs for Power BI dashboard performance

{{
    config(
        materialized = 'table',
        tags         = ['marts', 'kpis', 'powerbi'],
        post_hook    = "ALTER TABLE {{ this }} CLUSTER BY (period_date, territory_id)"
    )
}}

WITH base AS (
    SELECT * FROM {{ ref('fct_sales') }}
    WHERE status != 'cancelled'
),

monthly AS (
    SELECT
        order_month                                        AS period_date,
        'month'                                            AS period_type,
        territory_id,
        territory_name,
        territory_region,
        product_category,
        channel,
        customer_tier,

        COUNT(DISTINCT sale_id)                            AS order_count,
        COUNT(DISTINCT customer_id)                        AS unique_customers,
        COUNT(DISTINCT product_id)                         AS unique_products,
        SUM(quantity)                                      AS total_units,
        SUM(net_revenue)                                   AS total_revenue,
        SUM(gross_profit)                                  AS total_gross_profit,
        SUM(discount_amount)                               AS total_discounts,
        AVG(net_revenue)                                   AS avg_order_value,
        AVG(gross_margin_pct)                              AS avg_margin_pct,
        AVG(days_to_ship)                                  AS avg_days_to_ship,

        -- MoM Growth (will be calculated via window functions in Power BI)
        SUM(net_revenue) / NULLIF(
            LAG(SUM(net_revenue)) OVER (
                PARTITION BY territory_id, product_category
                ORDER BY order_month
            ), 0
        ) - 1                                              AS revenue_mom_growth,

        -- YoY Growth
        SUM(net_revenue) / NULLIF(
            LAG(SUM(net_revenue), 12) OVER (
                PARTITION BY territory_id, product_category
                ORDER BY order_month
            ), 0
        ) - 1                                              AS revenue_yoy_growth,

        CURRENT_TIMESTAMP()                                AS _updated_at

    FROM base
    GROUP BY 1,2,3,4,5,6,7,8
)

SELECT * FROM monthly
