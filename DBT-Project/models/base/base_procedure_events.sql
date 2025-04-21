-- models/base/base_procedure_events.sql
select
    patient_id,
    medical_event_id,
    procedure_code,
    service_date,
    place_of_service,
    rendering_npi,
    first_name,
    primary_specialty,
    provider_state,
    provider_zip,
    payer_name,
    parent_name,
    plan_id,
    patient_yob,
    patient_gender,
    diagnosis_code
from {{ source('raw', 'komodo_procedure_adoption') }}
