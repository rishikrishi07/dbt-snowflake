-- daily_sales_incremental.sql
-- Incremental model for daily sales data, filtered by execution date

{{
    config(
        materialized='incremental',
        unique_key='id',
        incremental_strategy='delete+insert',
        partition_by=['create_date'],
        on_schema_change='fail'
    )
}}

WITH source_data AS (
    SELECT
        *,
        -- Create a unique ID using concatenated fields
        MD5(CONCAT(
            sale_date::STRING, '-',
            product, '-',
            region, '-',
            units_sold::STRING, '-',
            record_timestamp::STRING
        )) as id
    FROM {{ source('public', 'daily_sales') }}
    
    {% if is_incremental() %}
        -- Filter for the execution date passed from Airflow
        WHERE create_date = '{{ var("execution_date") }}'
    {% endif %}
),

transformed AS (
    SELECT
        id,
        sale_date,
        product,
        region,
        units_sold,
        revenue,
        create_date,
        record_timestamp,
        -- Add calculated fields
        revenue / NULLIF(units_sold, 0) AS unit_price,
        -- Add a load timestamp
        CURRENT_TIMESTAMP() AS loaded_at
    FROM source_data
)

SELECT * FROM transformed 