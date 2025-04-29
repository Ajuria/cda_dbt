{{ config(materialized='view') }}

SELECT
  interaction_id,
  session_id,
  user_id,
  source_platform,
  cda_website_source,
  slug_cleaned,
  
  -- New final ID
  SUBSTR(TO_HEX(SHA256(CAST(COALESCE(slug_cleaned, cda_website_source) AS STRING))), 1, 16) AS page_id_final,

  social_media_action,
  timestamp,
  interaction_label
FROM {{ ref('stg__interaction') }}

