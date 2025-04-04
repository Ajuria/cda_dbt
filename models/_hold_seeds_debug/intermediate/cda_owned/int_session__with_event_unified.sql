-- models/dbt_pipeline/intermediate/cda_owned/int_session__with_event_unified.sql

{{ config(materialized='view') }}

WITH base AS (
    SELECT
        s.session_id    AS session_id,
        s.user_id       AS user_id,
        s.brand_id      AS brand_id,
        s.device        AS device,
        s.geo_ip        AS geo_ip,
        s.geo_name      AS geo_name,
        s.started_at    AS started_at,
        s.ended_at      AS ended_at,
        -- Mapping session source to events or art shows
        COALESCE(e.event_id, a.art_show_id)     AS event_id,
        COALESCE(e.event_type, 'art_show')      AS event_type
    FROM {{ ref('stg__cda_owned__session') }}   AS s
    LEFT JOIN {{ ref('stg__cda_owned__event') }} AS e
        ON s.session_source_url = e.event_id  -- assuming this maps directly
    LEFT JOIN {{ ref('stg__cda_owned__art_show') }} AS a
        ON s.session_source_url = a.art_show_id
)

SELECT * FROM base
