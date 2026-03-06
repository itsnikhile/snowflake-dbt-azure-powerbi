{% macro current_timestamp_utc() %}
    CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ
{% endmacro %}

{% macro date_spine(start_date, end_date) %}
    SELECT DATEADD(day, SEQ4(), '{{ start_date }}'::DATE) AS date_day
    FROM TABLE(GENERATOR(ROWCOUNT => DATEDIFF('day', '{{ start_date }}'::DATE, '{{ end_date }}'::DATE) + 1))
{% endmacro %}
