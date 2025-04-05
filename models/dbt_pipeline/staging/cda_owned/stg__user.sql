{{ config(materialized='view') }}

SELECT
  CONCAT('user_', FORMAT('%x', FARM_FINGERPRINT(CAST(session_id AS STRING)))) AS user_id,
  session_id,
  device,
  geo_ip,
  geo_name,
  CURRENT_TIMESTAMP() AS ingested_at
FROM {{ ref('stg__session') }}


