-- stg_products.sql
-- Staging model for Products

{{
    config(
        materialized = 'view'
    )
}}

with source as (
    select * from {{ ref('products') }}
),

renamed as (
    select
        product_id,
        product_name as product,
        therapeutic_area,
        launch_date,
        price_per_unit,
        annual_revenue_target
    from source
)

select * from renamed 