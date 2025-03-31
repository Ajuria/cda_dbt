SELECT
    user_id,
    COUNT(interaction_id)     AS total_interactions,
    MIN(timestamp)            AS first_interaction_at,
    MAX(timestamp)            AS last_interaction_at

FROM {{ ref('stg_wp__interactions') }}

GROUP BY
    user_id
