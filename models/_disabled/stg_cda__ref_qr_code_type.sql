SELECT
    qr_code_type_id,
    qr_code_type_label,
    created_at
FROM {{ source('dbt_jdeajuriaguerra_cda_pipeline', 'dim_qr_code_type') }}
