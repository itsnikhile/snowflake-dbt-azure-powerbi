-- dim_customers.sql
-- Gold layer: enriched customer dimension with RFM segmentation

{{
    config(
        materialized = 'table',
        tags         = ['marts', 'customers', 'powerbi']
    )
}}

WITH customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
),

sales_stats AS (
    SELECT
        customer_id,
        COUNT(*)                                            AS total_orders,
        SUM(net_revenue)                                   AS lifetime_value,
        AVG(net_revenue)                                   AS avg_order_value,
        MIN(order_date)                                    AS first_order_date,
        MAX(order_date)                                    AS last_order_date,
        DATEDIFF('day', MAX(order_date), CURRENT_DATE())   AS days_since_last_order,
        COUNT(DISTINCT product_id)                         AS unique_products_bought,
        COUNT(DISTINCT territory_id)                       AS unique_territories,
        SUM(gross_profit)                                  AS total_gross_profit
    FROM {{ ref('fct_sales') }}
    WHERE status != 'cancelled'
    GROUP BY 1
),

rfm AS (
    SELECT
        customer_id,
        -- Recency score (1=most recent)
        NTILE(5) OVER (ORDER BY days_since_last_order DESC) AS recency_score,
        -- Frequency score
        NTILE(5) OVER (ORDER BY total_orders)               AS frequency_score,
        -- Monetary score
        NTILE(5) OVER (ORDER BY lifetime_value)             AS monetary_score
    FROM sales_stats
),

segmented AS (
    SELECT
        c.*,
        COALESCE(s.total_orders, 0)                        AS total_orders,
        COALESCE(s.lifetime_value, 0)                      AS lifetime_value,
        ROUND(COALESCE(s.avg_order_value, 0), 2)           AS avg_order_value,
        s.first_order_date,
        s.last_order_date,
        COALESCE(s.days_since_last_order, 9999)            AS days_since_last_order,
        COALESCE(s.unique_products_bought, 0)              AS unique_products_bought,
        COALESCE(s.total_gross_profit, 0)                  AS total_gross_profit,

        -- RFM scores
        COALESCE(r.recency_score, 1)                       AS recency_score,
        COALESCE(r.frequency_score, 1)                     AS frequency_score,
        COALESCE(r.monetary_score, 1)                      AS monetary_score,
        COALESCE(r.recency_score + r.frequency_score + r.monetary_score, 3) AS rfm_total,

        -- Customer segment
        CASE
            WHEN COALESCE(r.recency_score, 1) >= 4
             AND COALESCE(r.frequency_score, 1) >= 4
             AND COALESCE(r.monetary_score, 1) >= 4  THEN 'Champions'
            WHEN COALESCE(r.recency_score, 1) >= 3
             AND COALESCE(r.monetary_score, 1) >= 3  THEN 'Loyal Customers'
            WHEN COALESCE(r.recency_score, 1) >= 4   THEN 'Recent Customers'
            WHEN COALESCE(r.frequency_score, 1) >= 4 THEN 'Potential Loyalists'
            WHEN COALESCE(r.recency_score, 1) <= 2
             AND COALESCE(r.monetary_score, 1) >= 4  THEN 'At Risk'
            WHEN COALESCE(r.recency_score, 1) = 1
             AND COALESCE(r.frequency_score, 1) = 1  THEN 'Lost'
            ELSE 'Needs Attention'
        END                                                AS customer_segment,

        -- Lifetime tier
        CASE
            WHEN COALESCE(s.lifetime_value, 0) >= 100000 THEN 'Platinum'
            WHEN COALESCE(s.lifetime_value, 0) >= 25000  THEN 'Gold'
            WHEN COALESCE(s.lifetime_value, 0) >= 5000   THEN 'Silver'
            ELSE 'Bronze'
        END                                                AS lifetime_tier,

        COALESCE(s.total_orders, 0) > 0                    AS is_active,
        CURRENT_TIMESTAMP()                                AS _updated_at
    FROM customers c
    LEFT JOIN sales_stats s USING (customer_id)
    LEFT JOIN rfm         r USING (customer_id)
)

SELECT * FROM segmented
