{{ config(materialized='view') }}

WITH cleaned AS (
  SELECT
    id,
    JSON_VALUE(title, '$.rendered') AS title,
    slug,
    date,
    type,
    content
  FROM {{ source('cda_owned', 'wp_pages') }}
)

SELECT *
FROM cleaned


