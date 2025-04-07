-- models/dbt_pipeline/intermediate/cda_owned/int_session__with_user_details.sql

{{ config(materialized='view') }}

SELECT
  s.session_id,
  s.session_source_url,
  FORMAT_TIMESTAMP('%F %T', s.started_at)   AS started_at,
  FORMAT_TIMESTAMP('%F %T', s.ended_at)     AS ended_at,

  u.payer_first_name,
  u.payer_last_name,
  u.payer_email,
  u.payer_country,
  u.device,
  u.geo_ip,
  u.geo_name,
  u.ingested_at AS user_ingested_at

FROM {{ ref('stg__session') }} AS s
LEFT JOIN {{ ref('int_user__consolidated') }} AS u
  ON s.user_id = u.user_id
