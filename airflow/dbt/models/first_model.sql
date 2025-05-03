-- Simple model to test dbt and Snowflake integration
-- This model queries Snowflake's information schema to get a list of tables

SELECT 
    table_schema,
    table_name,
    table_type,
    row_count
FROM 
    {{ source('information_schema', 'tables') }}
WHERE 
    table_schema NOT IN ('INFORMATION_SCHEMA', 'PUBLIC') 