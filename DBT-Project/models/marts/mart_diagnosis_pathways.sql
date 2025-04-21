-- models/marts/mart_diagnosis_pathways.sql
select
    procedure_code,
    diagnosis_code,
    count(*) as procedure_volume
from {{ ref('stg_procedure_events') }}
where diagnosis_code is not null
group by 1, 2
