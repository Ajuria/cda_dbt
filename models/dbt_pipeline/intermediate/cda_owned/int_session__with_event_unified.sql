{{ config(materialized='view') }}

SELECT
    s.session_id,
    s.user_id,
    s.started_at AS start_timestamp,
    s.ended_at AS end_timestamp,
    s.source_platform,
    s.session_source_url,
    s.device,
    s.geo_ip,
    s.geo_name,
    s.brand_id,
    NULL AS event_id,
    NULL AS event_type_id,
    NULL AS art_show_id,
    NULL AS art_show_type_id

FROM {{ ref('stg__session') }} AS s

