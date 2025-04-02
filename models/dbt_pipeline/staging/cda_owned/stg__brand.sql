{{ config(materialized='view') }}

SELECT
    id             AS brand_id,
    title          AS brand_name,
    slug           AS brand_slug,
    type           AS post_type,
    date           AS post_date,
    NULL           AS post_modified,
    NULL           AS post_parent,
    NULL           AS user_id
FROM {{ source('wordpress', 'wp_posts') }}
WHERE type = 'brand'
