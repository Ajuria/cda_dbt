SELECT
    e.event_id,
    e.event_title,
    e.event_slug,
    e.brand_id,
    e.event_type_id,
    e.event_datetime,
    e.post_date,
    COUNT(DISTINCT ia.interaction_id)           AS total_interactions,
    COUNT(DISTINCT ea.event_attendance_id)      AS total_attendances,
    COUNT(DISTINCT s.session_id)                AS total_sessions,
    COUNT(DISTINCT s.user_id)                   AS unique_users
FROM {{ ref('int_session__with_event_unified') }} s
LEFT JOIN {{ ref('stg_wp__event') }} e
    ON s.event_id = e.event_id
LEFT JOIN {{ ref('stg_cda__event_attendance') }} ea
    ON e.event_id = ea.event_id
LEFT JOIN {{ ref('stg_wp__interactions') }} ia
    ON s.session_id = ia.session_id
GROUP BY
    e.event_id,
    e.event_title,
    e.event_slug,
    e.brand_id,
    e.event_type_id,
    e.event_datetime,
    e.post_date
