-- int_hcp_enriched.sql
-- Intermediate model for HCP data enriched with territory information

{{
    config(
        materialized = 'view'
    )
}}

with hcp_source as (
    select * from {{ ref('stg_hcp') }}
),

territory_source as (
    select * from {{ ref('stg_territories') }}
),

hcp_with_territory as (
    select
        h.*,
        t.territory_name,
        t.region,
        t.states,
        t.potential_value as territory_potential_value
    from hcp_source h
    left join territory_source t
        on h.territory_id = t.territory_id
)

select * from hcp_with_territory 