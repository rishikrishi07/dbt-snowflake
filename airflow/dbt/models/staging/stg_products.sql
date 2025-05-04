-- stg_products.sql
-- Staging model for pharmaceutical products

{{
    config(
        materialized = 'view'
    )
}}

with source as (
    select * from {{ source('public', 'products') }}
),

renamed as (
    select
        product_id,
        product_name,
        therapeutic_area,
        launch_date,
        price_per_unit,
        annual_revenue_target,
    from source
)

select * from renamed 