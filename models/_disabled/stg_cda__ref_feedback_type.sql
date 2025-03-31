SELECT
    feedback_type_id,
    feedback_type_label,
    created_at
FROM {{ source('dbt_jdeajuriaguerra_cda_pipeline', 'dim_feedback_type') }}
