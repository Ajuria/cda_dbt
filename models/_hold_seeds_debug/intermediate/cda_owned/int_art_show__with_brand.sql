-- models/dbt_pipeline/intermediate/cda_owned/int_art_show__with_brand.sql

{{ config(materialized='view') }}

SELECT
    a.art_show_id AS art_show_id,
    a.art_show_title AS art_show_title,
    b.brand_id AS brand_id,
    a.post_date AS post_date,
    b.user_id AS user_id
FROM {{ ref('stg__cda_owned__art_show') }} AS a
LEFT JOIN {{ ref('stg__cda_owned__brand') }} AS b
    ON a.art_show_slug = b.brand_slug
