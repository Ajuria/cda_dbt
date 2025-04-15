{{ config(materialized='view') }}

SELECT
    art_show_type_id,
    art_show_type_label,
    art_show_type_description
FROM {{ ref('dim_art_show_type') }}
