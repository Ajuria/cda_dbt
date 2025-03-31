SELECT
    i.user_id                                      AS user_id,
    COUNT(DISTINCT i.interaction_id)              AS total_interactions,
    COUNT(DISTINCT i.session_id)                  AS sessions_with_interaction,
    COUNTIF(i.interaction_type = 'click')         AS total_clicks,
    COUNTIF(i.interaction_type = 'view')          AS total_views,
    MIN(i.timestamp)                              AS first_interaction,
    MAX(i.timestamp)                              AS last_interaction
FROM {{ ref('stg_wp__interactions') }} i
WHERE i.user_id IS NOT NULL
GROUP BY i.user_id
