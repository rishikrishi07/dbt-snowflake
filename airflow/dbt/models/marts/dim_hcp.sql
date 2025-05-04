-- dim_hcp.sql
-- Dimension table for Healthcare Professionals

{{
    config(
        materialized = 'table'
    )
}}

with hcp_source as (
    select * from {{ ref('int_hcp_enriched') }}
),

final as (
    select
        -- HCP primary key
        hcp_id,
        
        -- HCP attributes
        first_name,
        last_name,
        first_name || ' ' || last_name as full_name,
        specialty,
        facility,
        years_of_practice,
        email,
        phone,
        
        -- Location
        state,
        city,
        zip_code,
        
        -- Territory information
        territory_id,
        territory_name,
        region,
        states as territory_states,
        territory_potential_value,
        
        -- Scoring and segmentation
        potential_score,
        influence_score,
        created_at,
        
        -- Score categorization
        case
            when potential_score >= 80 then 'High'
            when potential_score >= 50 then 'Medium'
            else 'Low'
        end as potential_category,
        
        case
            when influence_score >= 8 then 'High'
            when influence_score >= 5 then 'Medium'
            else 'Low'
        end as influence_category,
        
        -- Combined segmentation
        case
            when potential_score >= 80 and influence_score >= 8 then 'Key Target'
            when potential_score >= 50 and influence_score >= 5 then 'Priority'
            else 'Standard'
        end as hcp_segment
    from hcp_source
)

select * from final 