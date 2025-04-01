SELECT
  s.user_id,
  COUNT(DISTINCT s.session_id) AS total_sessions,
  COUNT(DISTINCT s.event_id) AS total_events,
  COUNT(DISTINCT i.interaction_id) AS total_interactions
FROM {{ ref('stg_cda_owned__user') }} u
LEFT JOIN {{ ref('stg_cda_owned__session') }} s ON u.user_id = s.user_id
LEFT JOIN {{ ref('stg_cda_owned__interaction') }} i ON s.session_id = i.session_id
GROUP BY u.user_id


