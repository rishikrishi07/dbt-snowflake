-- stg_hcp.sql
-- Staging model for Healthcare Professionals

{{
    config(
        materialized = 'view'
    )
}}

with source as (
    select * from {{ source('public', 'hcp_master') }}
),

renamed as (
    select
        hcp_id,
        first_name,
        last_name,
        specialty,
        facility,
        years_of_practice,
        state,
        city,
        zip_code,
        email,
        phone,
        territory_id,
        potential_score,
        influence_score,
        created_at
    from source
)

select * from renamed 