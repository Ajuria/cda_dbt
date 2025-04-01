SELECT
    u.user_id,
    u.user_email,
    u.user_name,
    u.user_created_at,
    COUNT(s.session_id)             AS total_sessions,
    MIN(s.started_at)              AS first_session_at,
    MAX(s.ended_at)                AS last_session_at

FROM {{ ref('stg_wp__users') }} u
LEFT JOIN {{ ref('stg_cda_owned__session') }} s
    ON u.user_id = s.user_id

GROUP BY
    u.user_id,
    u.user_email,
    u.user_name,
    u.user_created_at

