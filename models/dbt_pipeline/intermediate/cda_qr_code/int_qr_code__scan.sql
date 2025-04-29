{{ config(materialized='view') }}

WITH session_scans AS (

    SELECT
        -- Hashed session_id
        SUBSTR(TO_HEX(SHA256(CAST(session_id AS STRING))), 1, 16) AS session_id,

        -- Unified user_id based on fallback logic
        SUBSTR(TO_HEX(SHA256(CAST(COALESCE(user_id, session_id) AS STRING))), 1, 16) AS user_id,

        -- Extract slug from the cleaned session_source_url
        REGEXP_EXTRACT(session_source_url, r'([^\/]+)\/?$') AS slug,

        -- Standardized timestamp format
        FORMAT_TIMESTAMP('%F %H:%M', CAST(started_at AS TIMESTAMP)) AS event_timestamp

    FROM {{ ref('stg__session') }}
    WHERE source_platform = 'qr_code'

),

scan_with_metadata AS (

    SELECT
        -- Deterministic scan_id based on raw session_id
        SUBSTR(TO_HEX(SHA256(CAST(session_id AS STRING))), 1, 16) AS scan_id,

        s.session_id,
        s.user_id,
        s.slug,
        s.event_timestamp,

        q.brand_id_final  AS brand_id,
        q.art_show_id_final AS art_show_id,
        q.event_id_final  AS event_id,
        q.artist_id       AS artist_id,

        -- ✅ Detect scans with no matched QR metadata
        CASE 
          WHEN q.brand_id_final IS NULL THEN TRUE 
          ELSE FALSE 
        END AS is_unmapped_scan

    FROM session_scans s
    LEFT JOIN {{ ref('int_qr_code__unified') }} q
      ON s.slug = q.content_slug

)

SELECT
    scan_id,
    session_id,
    user_id,
    slug,
    event_timestamp,
    brand_id,
    art_show_id,
    event_id,
    artist_id,
    is_unmapped_scan

FROM scan_with_metadata