-- stg_customers.sql
-- Silver layer: cleaned customer dimension

{{
    config(
        materialized = 'incremental',
        unique_key   = 'customer_id',
        incremental_strategy = 'merge',
        tags         = ['staging', 'customers']
    )
}}

WITH source AS (
    SELECT * FROM {{ source('raw', 'customers') }}
    {% if is_incremental() %}
    WHERE _ingested_at > (SELECT MAX(_ingested_at) FROM {{ this }})
    {% endif %}
),

cleaned AS (
    SELECT
        customer_id,
        INITCAP(TRIM(first_name))                          AS first_name,
        INITCAP(TRIM(last_name))                           AS last_name,
        INITCAP(TRIM(first_name)) || ' ' ||
            INITCAP(TRIM(last_name))                       AS full_name,
        LOWER(TRIM(email))                                 AS email,
        REGEXP_REPLACE(phone, '[^0-9+]', '')               AS phone,
        TRIM(company)                                      AS company,
        LOWER(TRIM(industry))                              AS industry,
        UPPER(TRIM(country))                               AS country,
        TRIM(region)                                       AS region,
        INITCAP(TRIM(city))                                AS city,
        LOWER(COALESCE(tier, 'standard'))                  AS tier,
        signup_date::DATE                                  AS signup_date,
        DATEDIFF('day', signup_date, CURRENT_DATE())       AS days_since_signup,
        _ingested_at,
        CURRENT_TIMESTAMP()                                AS _transformed_at
    FROM source
    WHERE customer_id IS NOT NULL
      AND email IS NOT NULL
)

SELECT * FROM cleaned
