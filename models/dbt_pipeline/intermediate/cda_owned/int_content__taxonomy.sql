{{ config(materialized='view') }}

WITH combined_content AS (
  SELECT
    id,
    slug,
    title,
    type,
    CAST(date AS TIMESTAMP) AS published_at,
    'page' AS source
  FROM {{ ref('stg__wp_pages') }}

  UNION ALL

  SELECT
    post_id         AS id,
    post_slug       AS slug,
    post_title      AS title,
    post_type       AS type,
    CAST(post_date  AS TIMESTAMP) AS published_at,
    'post'          AS source
  FROM {{ ref('stg__wp_posts') }}
)

SELECT * FROM combined_content
