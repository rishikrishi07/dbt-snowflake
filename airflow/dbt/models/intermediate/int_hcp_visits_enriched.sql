-- int_hcp_visits_enriched.sql
-- Intermediate model for HCP visits with medical representative and HCP information

{{
    config(
        materialized = 'view'
    )
}}

with visits_source as (
    select * from {{ ref('stg_hcp_visits') }}
),

hcp_source as (
    select * from {{ ref('stg_hcp') }}
),

mr_source as (
    select * from {{ ref('stg_medical_representatives') }}
),

enriched_visits as (
    select
        v.visit_id,
        v.visit_date,
        v.visit_type,
        v.duration_minutes,
        v.outcome,
        v.engagement_score,
        v.products_discussed,
        v.notes,
        
        -- HCP details
        v.hcp_id,
        h.first_name as hcp_first_name,
        h.last_name as hcp_last_name,
        h.specialty as hcp_specialty,
        h.facility as hcp_facility,
        h.territory_id as hcp_territory_id,
        
        -- MR details
        v.mr_id,
        m.first_name as mr_first_name,
        m.last_name as mr_last_name,
        m.territory_ids as mr_territory_ids
    from visits_source v
    left join hcp_source h
        on v.hcp_id = h.hcp_id
    left join mr_source m
        on v.mr_id = m.mr_id
)

select * from enriched_visits 