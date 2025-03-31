SELECT
    art_show_id,
    brand_id
FROM {{ source('dbt_jdeajuriaguerra_cda_pipeline', 'seed_art_show__brand_mapping') }}