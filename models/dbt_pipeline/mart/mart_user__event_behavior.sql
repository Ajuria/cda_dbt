SELECT
    s.user_id,
    COUNT(DISTINCT s.session_id)                AS session_count,
    COUNT(DISTINCT s.event_id)                  AS events_visited,
    COUNT(DISTINCT ea.event_attendance_id)      AS events_attended,
    COUNT(DISTINCT i.interaction_id)            AS interactions_made,
    COUNT(DISTINCT i.interaction_type_id)       AS distinct_interaction_types
FROM {{ ref('int_session__with_event_unified') }} s
LEFT JOIN {{ ref('stg_wp__interactions') }} i
    ON s.session_id = i.session_id
LEFT JOIN {{ ref('stg_cda__event_attendance') }} ea
    ON s.event_id = ea.event_id AND s.user_id = ea.user_id
GROUP BY
    s.user_id
