{{ config(materialized='view') }}

WITH cleaned AS (
  SELECT
    CAST(session_id AS STRING)      AS session_id,      
    CAST(source_platform AS STRING) AS source_platform,
    NULLIF(
      REGEXP_REPLACE(
        REGEXP_REPLACE(session_source_url, r'^https?://costieresdelart\.fr/', ''),
        r'^category/', ''
      ),
      ''
    ) AS session_source_url,

    CAST(user_id AS STRING)                 AS user_id,
    CAST(brand_id AS STRING)                AS brand_id,
    CAST(device AS STRING)                  AS device,
    CAST(geo_ip AS STRING)                  AS geo_ip,
    CAST(geo_name AS STRING)                AS geo_name,
    FORMAT_TIMESTAMP('%F %T', started_at)   AS started_at,
    FORMAT_TIMESTAMP('%F %T', ended_at)     AS ended_at
  FROM {{ source('cda_owned', 'session') }}
)

SELECT *
FROM cleaned
