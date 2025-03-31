SELECT
    COALESCE(s.user_id, i.user_id)           AS user_id,
    s.total_sessions,
    s.first_session_at,
    s.last_session_at,
    i.total_interactions,
    i.first_interaction_at,
    i.last_interaction_at

FROM {{ ref('int_user__with_session_summary') }} s
FULL OUTER JOIN {{ ref('int_user__with_interaction_summary') }} i
    ON s.user_id = i.user_id
