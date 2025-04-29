{{ config(materialized='view') }}

WITH cleaned AS (
  SELECT
    CAST(session_id AS STRING) AS session_id,

    -- Ensure all branches in CASE return STRING
    CAST(
      CASE 
        WHEN source_platform IS NOT NULL THEN CAST(source_platform AS STRING)
        WHEN LOWER(session_source_url) LIKE '%bonjour%' THEN 'qr_code'
        WHEN LOWER(session_source_url) = 'http://costieresdelart.fr' THEN 'qr_code'
        ELSE NULL
      END
    AS STRING) AS source_platform,

    NULLIF(
      REGEXP_REPLACE(
        REGEXP_REPLACE(session_source_url, r'^https?://costieresdelart\.fr/', ''),
        r'^category/', ''
      ),
      ''
    ) AS session_source_url,

    -- 🧠 Generate hashed user_id (fallback to session_id if user is null)
    SUBSTR(TO_HEX(SHA256(CAST(
      COALESCE(user_id, session_id) AS STRING
    ))), 1, 16) AS user_id,

    CAST(brand_id AS STRING)              AS brand_id,
    CAST(device AS STRING)                AS device,
    CAST(geo_ip AS STRING)                AS geo_ip,
    CAST(geo_name AS STRING)              AS geo_name,
    FORMAT_TIMESTAMP('%F %T', started_at) AS started_at,
    FORMAT_TIMESTAMP('%F %T', ended_at)   AS ended_at
  FROM {{ source('cda_owned', 'session') }}
)

SELECT *
FROM cleaned
