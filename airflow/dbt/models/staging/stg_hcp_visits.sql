-- stg_hcp_visits.sql
-- Staging model for Healthcare Professional Visits

{{
    config(
        materialized = 'view'
    )
}}

with source as (
    select * from {{ ref('hcp_visits') }}
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
        products_discussed,
        notes
    from source
)

select * from renamed 