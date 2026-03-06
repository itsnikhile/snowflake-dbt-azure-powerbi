-- fct_sales.sql
-- Gold layer: central sales fact table for Power BI

{{
    config(
        materialized = 'incremental',
        unique_key   = 'sale_id',
        incremental_strategy = 'merge',
        cluster_by   = ['order_date', 'territory_id'],
        tags         = ['marts', 'sales', 'powerbi']
    )
}}

WITH sales AS (
    SELECT * FROM {{ ref('stg_sales') }}
    {% if is_incremental() %}
    WHERE _transformed_at > (SELECT MAX(_transformed_at) FROM {{ this }})
    {% endif %}
),

customers AS (
    SELECT customer_id, tier, country, region, industry
    FROM {{ ref('stg_customers') }}
),

products AS (
    SELECT product_id, category, subcategory, brand, cost_price, list_price
    FROM {{ ref('stg_products') }}
),

territories AS (
    SELECT territory_id, territory_name, region AS territory_region, country AS territory_country
    FROM {{ ref('stg_territories') }}
),

enriched AS (
    SELECT
        s.sale_id,
        s.customer_id,
        s.product_id,
        s.territory_id,
        s.salesperson_id,
        s.order_date,
        s.ship_date,
        s.order_month,
        s.order_quarter,
        s.order_year,
        s.quantity,
        s.unit_price,
        s.discount_pct,
        s.total_amount,
        s.net_revenue,
        s.discount_amount,
        s.days_to_ship,
        s.deal_size_tier,
        s.channel,
        s.status,
        s.currency,

        -- Customer context
        c.tier                                              AS customer_tier,
        c.country                                          AS customer_country,
        c.region                                           AS customer_region,
        c.industry                                         AS customer_industry,

        -- Product context
        p.category                                         AS product_category,
        p.subcategory                                      AS product_subcategory,
        p.brand                                            AS product_brand,
        ROUND(s.quantity * p.cost_price, 4)                AS total_cost,
        ROUND(s.net_revenue - (s.quantity * p.cost_price), 4) AS gross_profit,
        CASE
            WHEN p.cost_price > 0 THEN
                ROUND((s.net_revenue - s.quantity * p.cost_price)
                    / NULLIF(s.net_revenue, 0) * 100, 2)
            ELSE NULL
        END                                                AS gross_margin_pct,

        -- Territory context
        t.territory_name,
        t.territory_region,
        t.territory_country,

        CURRENT_TIMESTAMP()                                AS _updated_at

    FROM sales s
    LEFT JOIN customers  c USING (customer_id)
    LEFT JOIN products   p USING (product_id)
    LEFT JOIN territories t USING (territory_id)
)

SELECT * FROM enriched
