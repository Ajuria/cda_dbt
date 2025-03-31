SELECT
    event_type_id,
    event_type_label,
    created_at
FROM {{ source('dbt_jdeajuriaguerra_cda_pipeline', 'dim_event_type') }}
