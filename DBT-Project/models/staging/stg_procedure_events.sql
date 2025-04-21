-- models/staging/stg_procedure_events.sql
with parsed as (
    select *,
        try_to_date(service_date) as service_date_parsed,
        date_trunc('month', try_to_date(service_date)) as procedure_month,
        extract(year from current_date()) - extract(year from try_to_date(patient_yob)) as patient_age,
        case when procedure_code = 'S2900' then true else false end as is_robotic
    from {{ ref('base_procedure_events') }}
)

select * from parsed
