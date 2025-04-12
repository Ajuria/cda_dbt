{{ config(materialized='view') }}

WITH latest_session_per_user AS (
  SELECT
    user_id,
    device,
    geo_ip,
    geo_name,
    ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY started_at DESC) AS rn
  FROM {{ ref('stg__session') }}
)

, enriched_user AS (
  SELECT
    u.user_id                                   AS user_id_raw,
    SUBSTR(TO_HEX(SHA256(u.user_id)), 1, 16)    AS user_id,
    u.first_name                                AS payer_first_name,
    u.last_name                                 AS payer_last_name,
    u.email                                     AS payer_email,
    u.country                                   AS payer_country,
    s.device,
    s.geo_ip,
    s.geo_name,
    CAST(u.ingested_at AS TIMESTAMP)            AS ingested_at
  FROM {{ ref('stg__user') }} u
  LEFT JOIN latest_session_per_user s
    ON u.user_id = s.user_id
   AND s.rn = 1
)

SELECT * FROM enriched_user
