-- models/dbt_pipeline/intermediate/cda_owned/int_fact__session.sql

SELECT
  SUBSTR(TO_HEX(SHA256(s.session_id)), 1, 16)                   AS session_id,

  COALESCE(
    SUBSTR(TO_HEX(SHA256(CAST(s.user_id AS STRING))), 1, 16),
    SUBSTR(TO_HEX(SHA256(CAST(s.session_id AS STRING))), 1, 16)
  ) AS user_id,

  s.source_platform,
  s.session_source_url,
  REGEXP_EXTRACT(s.session_source_url, r'code=([^&]+)')         AS code,
  CAST(s.started_at AS TIMESTAMP)                               AS started_at,
  CAST(s.ended_at AS TIMESTAMP)                                 AS ended_at,
  s.device,
  s.geo_ip,
  s.geo_name,
  u.email,
  u.country

FROM {{ ref('stg__session') }} s
LEFT JOIN {{ ref('int_fact__user') }} u
  ON u.user_id = COALESCE(
    SUBSTR(TO_HEX(SHA256(CAST(s.user_id AS STRING))), 1, 16),
    SUBSTR(TO_HEX(SHA256(CAST(s.session_id AS STRING))), 1, 16)
  )
