create or replace view mart_patient_360 as
with claim_summary as (
    select
        enterprise_patient_id,
        count(*) as total_claims,
        round(sum(paid_amount), 2) as total_claims_paid
    from fct_claims
    group by enterprise_patient_id
),
encounter_summary as (
    select
        enterprise_patient_id,
        count(*) as total_encounters,
        max(discharge_at) as latest_discharge_at
    from fct_encounters
    group by enterprise_patient_id
),
latest_labs as (
    select
        enterprise_patient_id,
        lab_name as latest_lab_name,
        result_value as latest_lab_value,
        row_number() over (
            partition by enterprise_patient_id
            order by collected_at desc nulls last
        ) as lab_rank
    from fct_labs
)
select
    p.enterprise_patient_id,
    concat(p.first_name, ' ', p.last_name) as patient_name,
    p.first_name,
    p.last_name,
    p.city,
    p.state,
    p.gender_clean,
    p.dob_clean,
    p.source_record_count,
    p.dedupe_reasons,
    coalesce(e.total_encounters, 0) as total_encounters,
    coalesce(c.total_claims, 0) as total_claims,
    coalesce(c.total_claims_paid, 0) as total_claims_paid,
    l.latest_lab_name,
    l.latest_lab_value
from dim_patients p
left join encounter_summary e using (enterprise_patient_id)
left join claim_summary c using (enterprise_patient_id)
left join latest_labs l
    on p.enterprise_patient_id = l.enterprise_patient_id
   and l.lab_rank = 1
order by total_claims_paid desc, total_encounters desc;

create or replace view mart_data_quality as
select
    metric_name,
    metric_value,
    threshold,
    status
from monitoring_metrics;

create or replace view mart_pipeline_monitor as
select
    event_timestamp,
    run_id,
    stage,
    status,
    message,
    payload
from pipeline_run_audit
order by event_timestamp desc;
