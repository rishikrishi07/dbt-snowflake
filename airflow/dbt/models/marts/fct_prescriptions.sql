-- fct_prescriptions.sql
-- Fact table for Prescriptions

{{
    config(
        materialized = 'table'
    )
}}

with prescriptions_source as (
    select * from {{ ref('int_prescriptions_enriched') }}
),

territory_source as (
    select * from {{ ref('stg_territories') }}
),

final as (
    select
        -- Time dimensions
        year_month,
        year::integer as prescription_year,
        month::integer as prescription_month,
        
        -- HCP dimension
        hcp_id,
        hcp_first_name || ' ' || hcp_last_name as hcp_full_name,
        hcp_specialty,
        hcp_territory_id,
        
        -- Product dimension
        product,
        product_id,
        therapeutic_area,
        
        -- Territory dimension
        t.territory_name,
        t.region,
        
        -- Metrics
        num_prescriptions,
        total_units,
        price_per_unit,
        estimated_revenue,
        
        -- KPIs and targets
        annual_revenue_target,
        round(estimated_revenue / nullif(annual_revenue_target, 0) * 100, 2) as revenue_target_percentage
    from prescriptions_source p
    left join territory_source t
        on p.hcp_territory_id = t.territory_id
)

select * from final 