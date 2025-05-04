-- stg_prescriptions.sql
-- Staging model for Prescription data

{{
    config(
        materialized = 'view'
    )
}}

with source as (
    select * from {{ ref('prescriptions') }}
),

renamed as (
    select
        hcp_id,
        product,
        year_month,
        num_prescriptions,
        total_units
    from source
)

select * from renamed 