-- stg_prescriptions.sql
-- Staging model for prescriptions written by HCPs

{{
    config(
        materialized = 'view'
    )
}}

with source as (
    select * from {{ source('public', 'prescriptions') }}
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