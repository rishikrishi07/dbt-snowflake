-- stg_territories.sql
-- Staging model for sales territories

{{
    config(
        materialized = 'view'
    )
}}

with source as (
    select * from {{ source('public', 'territories') }}
),

renamed as (
    select
        territory_id,
        territory_name,
        region,
        states,
        potential_value,
    from source
)

select * from renamed 