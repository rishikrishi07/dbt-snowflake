-- fct_visits.sql
-- Fact table for HCP Visits

{{
    config(
        materialized = 'table'
    )
}}

with visits_source as (
    select * from {{ ref('int_hcp_visits_enriched') }}
),

territory_source as (
    select * from {{ ref('stg_territories') }}
),

final as (
    select
        visit_id,
        visit_date,
        extract(year from visit_date) as visit_year,
        extract(month from visit_date) as visit_month,
        extract(quarter from visit_date) as visit_quarter,
        
        -- Visit details
        visit_type,
        duration_minutes,
        outcome,
        engagement_score,
        products_discussed,
        
        -- HCP dimension keys and attributes
        hcp_id,
        hcp_first_name || ' ' || hcp_last_name as hcp_full_name,
        hcp_specialty,
        hcp_facility,
        hcp_territory_id,
        
        -- MR dimension keys and attributes
        mr_id,
        mr_first_name || ' ' || mr_last_name as mr_full_name,
        mr_territory_ids,
        
        -- Flags for analysis
        case 
            when outcome in ('Very Positive', 'Positive') then 1
            else 0
        end as is_positive_outcome,
        
        case 
            when outcome in ('Very Negative', 'Negative') then 1
            else 0
        end as is_negative_outcome,
        
        case
            when engagement_score >= 7 then 1
            else 0
        end as is_high_engagement
    from visits_source
)

select * from final 