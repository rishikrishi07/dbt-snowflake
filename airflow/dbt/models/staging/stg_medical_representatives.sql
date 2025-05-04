-- stg_medical_representatives.sql
-- Staging model for Medical Representatives

{{
    config(
        materialized = 'view'
    )
}}

with source as (
    select * from {{ ref('medical_representatives') }}
),

renamed as (
    select
        mr_id,
        first_name,
        last_name,
        email,
        hire_date,
        territory_ids
    from source
)

select * from renamed 