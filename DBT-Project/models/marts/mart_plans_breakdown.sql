-- models/marts/mart_plans_breakdown.sql
select
    procedure_code,
    plan_id,
    payer_name,
    parent_name,
    count(*) as procedure_volume
from {{ ref('stg_procedure_events') }}
group by 1, 2, 3, 4
