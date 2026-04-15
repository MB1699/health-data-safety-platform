"""Optional PySpark version of the normalization layer for interview discussion."""

from __future__ import annotations


def build_patient_transform_sql() -> str:
    return """
    select
        upper(trim(source_system)) as source_system,
        source_record_id,
        initcap(trim(first_name)) as first_name,
        initcap(trim(last_name)) as last_name,
        lower(trim(email)) as email,
        regexp_replace(phone, '[^0-9]', '') as phone_norm,
        upper(trim(state)) as state,
        regexp_replace(zip_code, '[^0-9]', '') as zip_norm,
        upper(trim(mrn)) as mrn,
        to_timestamp(updated_at) as updated_at
    from raw_patients
    """
