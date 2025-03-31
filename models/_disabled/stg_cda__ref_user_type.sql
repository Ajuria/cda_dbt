SELECT
    user_type_id,
    user_type_label,
    created_at
FROM {{ source('dbt_jdeajuriaguerra_cda_pipeline', 'dim_user_type') }}
