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

),

session_with_brand_fix AS (

    SELECT
        base.session_id,
        base.source_platform,
        base.session_source_url,
        base.user_id,
        
        -- Prefer original brand_id if present, fallback to QR code brand_id
        COALESCE(base.brand_id, qr.brand_id_final) AS brand_id,
        
        -- Extra QR info
        qr.artist_id,
        qr.art_show_id_final,
        qr.event_id_final,
        
        base.device,
        base.geo_ip,
        base.geo_name,
        base.started_at,
        base.ended_at,

        -- Flag unmapped sessions
        CASE WHEN COALESCE(base.brand_id, qr.brand_id_final) IS NULL THEN TRUE ELSE FALSE END AS is_unmapped_session

    FROM base_session AS base
    LEFT JOIN {{ ref('int_qr_code__unified') }} AS qr
        ON base.session_source_url = qr.content_slug

)

SELECT
    *
FROM session_with_brand_fix
