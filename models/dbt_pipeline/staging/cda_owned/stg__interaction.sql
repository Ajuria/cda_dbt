{{ config(materialized='view') }}

WITH cleaned AS (
  SELECT
    interaction_id,
    source_platform,

    NULLIF(
      REGEXP_REPLACE(
        REGEXP_REPLACE(source_url, r'^https://costieresdelart\.fr/', ''),
        r'^category/', ''
      ),
      ''
    ) AS cda_website_source,

    session_id,
    target_id,
    user_id,
    social_media_action,
    timestamp,

    -- Extract title or page_title from JSON details
    COALESCE(
      JSON_VALUE(details, '$.page_title'),
      JSON_VALUE(details, '$.title')
    ) AS interaction_label

  FROM {{ source('cda_owned', 'interaction') }}
)

SELECT
  FORMAT('%s', interaction_id) AS interaction_id,
  source_platform,
  cda_website_source,
  CAST(session_id AS STRING)   AS session_id,
  target_id,
  user_id,
  social_media_action,
  timestamp,
  interaction_label
FROM cleaned
