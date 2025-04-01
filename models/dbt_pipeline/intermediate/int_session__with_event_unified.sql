WITH base_session AS (
    SELECT
        session_id,
        source_platform,
        session_source_url,
        user_id,
        brand_id,
        device,
        geo_ip,
        geo_name,
        started_at,
        ended_at
    FROM {{ ref('stg_cda_owned__session') }}
),

event_attendance AS (
    SELECT
        session_id,
        event_id,
        attended_at
    FROM {{ ref('stg_cda_owned__event_attendance') }}
),

interactions AS (
    SELECT
        session_id,
        interaction_type,
        target_id,
        timestamp AS interaction_ts
    FROM {{ ref('stg_cda_owned__interaction') }}
)

SELECT
    s.*,
    ea.event_id                   AS attended_event_id,
    ea.attended_at,
    i.interaction_type            AS interaction_type,
    i.target_id                   AS interaction_target_id,
    i.interaction_ts
FROM base_session s
LEFT JOIN event_attendance ea
    ON s.session_id = ea.session_id
LEFT JOIN interactions i
    ON s.session_id = i.session_id
