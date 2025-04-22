{{ config(materialized='view') }}

WITH cleaned AS (
  SELECT
    id,
    JSON_VALUE(title, '$.rendered') AS title,
    slug,
    date,
    'project' AS type
  FROM {{ source('cda_owned', 'wp_posts') }}
  WHERE type = 'project'
    AND status = 'publish'
)

SELECT *
FROM cleaned

