-- stg_territories.sql
-- Staging model for Territories

{{
    config(
        materialized = 'view'
    )
}}

with source as (
    select * from {{ ref('territories') }}
),

renamed as (
    select
        territory_id,
        territory_name,
        region,
        states,
        potential_value
    from source
)

select * from renamed 