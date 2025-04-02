-- models/dbt_pipeline/intermediate/dimensions/int_dim__art_show_type.sql

{{ config(materialized='view') }}

SELECT
    DISTINCT
    a.post_type AS art_show_type_id,
    SAFE_CAST(NULL AS STRING) AS art_show_type_label,
    SAFE_CAST(NULL AS STRING) AS art_show_type_description
FROM {{ ref('stg__cda_owned__art_show') }} AS a
WHERE a.post_type IS NOT NULL
