-- models/dbt_pipeline/intermediate/cda_owned/int_fact__interaction.sql

{{ config(materialized='view') }}

SELECT
  SUBSTR(TO_HEX(SHA256(i.interaction_id)), 1, 16)               AS interaction_id,
  SUBSTR(TO_HEX(SHA256(i.session_id)), 1, 16)                   AS session_id,

  -- Use user_id when available, else fallback to session_id -> ADD INTERACTION
  CASE
    WHEN i.user_id IS NOT NULL THEN SUBSTR(TO_HEX(SHA256(CAST(i.user_id AS STRING))), 1, 16)
    WHEN i.session_id IS NOT NULL THEN SUBSTR(TO_HEX(SHA256(CAST(i.session_id AS STRING))), 1, 16)
    ELSE NULL
  END AS user_id,

  FORMAT_TIMESTAMP('%F %H:%M', CAST(i.timestamp AS TIMESTAMP)) AS occurred_at,
  i.cda_website_source                                         AS url,
  REGEXP_EXTRACT(i.cda_website_source, r'code=([^&]+)')        AS code,
  s.device,
  s.geo_ip,
  s.geo_name,
  FORMAT_TIMESTAMP('%F %H:%M', CURRENT_TIMESTAMP())            AS ingested_at

FROM {{ ref('stg__interaction') }} i
LEFT JOIN {{ ref('stg__session') }} s
  ON i.session_id = s.session_id
