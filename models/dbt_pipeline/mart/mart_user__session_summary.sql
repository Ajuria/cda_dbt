SELECT
    s.user_id                              AS user_id,
    COUNT(DISTINCT s.session_id)          AS total_sessions,
    MIN(s.started_at)                     AS first_session_at,
    MAX(s.started_at)                     AS last_session_at,
    COUNTIF(EXTRACT(HOUR FROM s.started_at) BETWEEN 0 AND 6) AS night_sessions,
    COUNTIF(device = 'mobile')            AS mobile_sessions,
    COUNTIF(device = 'desktop')           AS desktop_sessions
FROM ref('stg_cda_owned__session')
WHERE s.user_id IS NOT NULL
GROUP BY s.user_id
