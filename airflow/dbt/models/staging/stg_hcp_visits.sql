-- stg_hcp_visits.sql
-- Staging model for HCP visit records

{{
    config(
        materialized = 'view'
    )
}}

with source as (
    select * from {{ source('public', 'hcp_visits') }}
),

renamed as (
    select
        visit_id,
        hcp_id,
        mr_id,
        visit_date,
        visit_type,
        duration_minutes,
        outcome,
        engagement_score,
        notes,
        products_discussed
    from source
)

select * from renamed 