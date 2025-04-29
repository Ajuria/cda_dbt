{{ config(materialized='view') }}

WITH session_scans AS (

    SELECT
        SUBSTR(TO_HEX(SHA256(CAST(session_id AS STRING))), 1, 16) AS session_id,
        SUBSTR(TO_HEX(SHA256(CAST(COALESCE(user_id, session_id) AS STRING))), 1, 16) AS user_id,
        REGEXP_EXTRACT(session_source_url, r'([^\/]+)\/?$') AS slug, 
        FORMAT_TIMESTAMP('%F %H:%M', CAST(started_at AS TIMESTAMP)) AS event_timestamp
    FROM `cda-database`.`dbt_jdeajuriaguerra_dbt_jdeajuriaguerra_cda_pipeline`.`stg__session`
    WHERE source_platform = 'qr_code'  

),

scan_with_metadata AS (

    SELECT
        s.session_id,
        s.user_id,
        s.slug,
        s.event_timestamp,

        q.brand_id_final AS brand_id,
        q.art_show_id_final AS art_show_id,
        q.event_id_final AS event_id,
        q.artist_id AS artist_id,

        CASE WHEN q.brand_id_final IS NULL THEN TRUE ELSE FALSE END AS is_unmapped_scan

    FROM session_scans s
    LEFT JOIN `cda-database`.`dbt_jdeajuriaguerra_dbt_jdeajuriaguerra_cda_pipeline`.`int_qr_code__unified` q
      ON s.slug = q.content_slug

)

SELECT
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

