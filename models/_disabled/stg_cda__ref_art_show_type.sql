SELECT
    art_show_type_id,
    art_show_type_label,
    created_at
FROM {{ source('dbt_jdeajuriaguerra_cda_pipeline', 'dim_art_show_type') }}
