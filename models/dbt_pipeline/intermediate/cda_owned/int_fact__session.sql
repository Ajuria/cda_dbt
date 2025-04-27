{{ config(materialized='view') }}

WITH base_session AS (

    SELECT
        -- Hash session_id
        SUBSTR(TO_HEX(SHA256(CAST(session_id AS STRING))), 1, 16) AS session_id,

        -- Map numeric source_platform to readable string
        CASE CAST(source_platform AS INT64)
            WHEN 1 THEN 'cda_website'
            WHEN 2 THEN 'third_party_website'
            WHEN 3 THEN 'instagram'
            WHEN 4 THEN 'facebook'
            WHEN 5 THEN 'helloasso'
            WHEN 6 THEN 'qr_code'
            ELSE 'unknown'
        END AS source_platform,

        -- Website source cleaned
        session_source_url,

        -- user_id generated (already hashed at stg__session level)
        SUBSTR(TO_HEX(SHA256(CAST(COALESCE(user_id, session_id) AS STRING))), 1, 16) AS user_id,

        -- Other session metadata
        CAST(brand_id AS STRING) AS brand_id,
        CAST(device AS STRING)   AS device,
        CAST(geo_ip AS STRING)   AS geo_ip,
        CAST(geo_name AS STRING) AS geo_name,

        -- Format timestamps without seconds
        FORMAT_TIMESTAMP('%F %H:%M:00', CAST(started_at AS TIMESTAMP)) AS started_at,
        FORMAT_TIMESTAMP('%F %H:%M:00', CAST(ended_at AS TIMESTAMP)) AS ended_at

    FROM {{ ref('stg__session') }}

)

SELECT
    *
FROM base_session