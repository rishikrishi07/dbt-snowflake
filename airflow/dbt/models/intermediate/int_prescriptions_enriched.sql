-- int_prescriptions_enriched.sql
-- Intermediate model for prescriptions with HCP and product information

{{
    config(
        materialized = 'view'
    )
}}

with prescriptions_source as (
    select * from {{ ref('stg_prescriptions') }}
),

hcp_source as (
    select * from {{ ref('stg_hcp') }}
),

product_source as (
    select * from {{ ref('stg_products') }}
),

-- Parse the year_month field into separate year and month
parsed_prescriptions as (
    select
        *,
        substr(year_month, 1, 4) as year,
        substr(year_month, 6, 2) as month
    from prescriptions_source
),

-- Join with HCP and product information
enriched_prescriptions as (
    select
        p.hcp_id,
        p.product,
        p.year_month,
        p.year,
        p.month,
        p.num_prescriptions,
        p.total_units,
        
        -- HCP information
        h.first_name as hcp_first_name,
        h.last_name as hcp_last_name,
        h.specialty as hcp_specialty,
        h.territory_id as hcp_territory_id,
        
        -- Product information
        pr.product_id,
        pr.therapeutic_area,
        pr.price_per_unit,
        pr.annual_revenue_target,
        
        -- Calculate revenue from this prescription
        p.total_units * coalesce(pr.price_per_unit, 0) as estimated_revenue
    from parsed_prescriptions p
    left join hcp_source h
        on p.hcp_id = h.hcp_id
    left join product_source pr
        on p.product = pr.product_name
)

select * from enriched_prescriptions 