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

    -- 🛠 Add slug_cleaned inside cleaned
    REGEXP_REPLACE(
      REGEXP_REPLACE(
        REGEXP_REPLACE(LOWER(TRIM(NULLIF(
          REGEXP_REPLACE(
            REGEXP_REPLACE(source_url, r'^https://costieresdelart\.fr/', ''),
            r'^category/', ''
          ),
          ''
        ))), r'^(artistes/|evenements/|programme/|a-propos/|actualites/)', ''),
        r'/$', ''  -- remove trailing slash
      ),
      r'\?.*$', ''  -- remove query params
    ) AS slug_cleaned,

    session_id,
    target_id,
    user_id,
    social_media_action,
    timestamp,
    details,

    -- Extract title or page_title from JSON details
    COALESCE(
      JSON_VALUE(details, '$.page_title'),
      JSON_VALUE(details, '$.title')
    ) AS interaction_label

  FROM {{ source('cda_owned', 'interaction') }}
)

SELECT
  -- Hash interaction_id to 16 chars
  SUBSTR(TO_HEX(SHA256(CAST(interaction_id AS STRING))), 1, 16) AS interaction_id,

  source_platform,
  cda_website_source,
  slug_cleaned,

  -- Hash session_id to 16 chars
  SUBSTR(TO_HEX(SHA256(CAST(session_id AS STRING))), 1, 16) AS session_id,

  -- Hash user_id or fallback on session_id, 16 chars
  SUBSTR(TO_HEX(SHA256(CAST(COALESCE(user_id, session_id) AS STRING))), 1, 16) AS user_id,

  social_media_action,
  timestamp,
  interaction_label
FROM cleaned
