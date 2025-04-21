-- models/marts/mart_procedure_trends.sql
select
    procedure_month,
    procedure_code,
    provider_state,
    payer_name,
    patient_gender,
    count(*) as procedure_volume
from {{ ref('stg_procedure_events') }}
group by 1, 2, 3, 4, 5
