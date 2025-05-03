with source_data as (
    select 1 as id, 'Sample data' as content
    union all
    select 2 as id, 'More sample data' as content
)

select *
from source_data