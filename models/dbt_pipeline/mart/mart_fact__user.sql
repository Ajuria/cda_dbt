{{ config(materialized='table') }}

WITH session_metrics AS (
    SELECT
        user_id,
        COUNT(*) AS session_count,
        AVG(TIMESTAMP_DIFF(CAST(ended_at AS TIMESTAMP), CAST(started_at AS TIMESTAMP), MINUTE)) AS avg_session_duration,
        MIN(TIMESTAMP_DIFF(CAST(ended_at AS TIMESTAMP), CAST(started_at AS TIMESTAMP), MINUTE)) AS min_session_duration,
        MAX(TIMESTAMP_DIFF(CAST(ended_at AS TIMESTAMP), CAST(started_at AS TIMESTAMP), MINUTE)) AS max_session_duration
    FROM {{ ref('int_fact__session') }}
    WHERE user_id IS NOT NULL
    GROUP BY user_id
),

interaction_per_session AS (
    SELECT
        session_id,
        COUNT(*) AS interaction_count
    FROM {{ ref('int_fact__interaction') }}
    WHERE session_id IS NOT NULL
    GROUP BY session_id
),

interaction_metrics AS (
    SELECT
        s.user_id,
        AVG(i.interaction_count) AS avg_interactions_per_session,
        MIN(i.interaction_count) AS min_interactions_per_session,
        MAX(i.interaction_count) AS max_interactions_per_session
    FROM {{ ref('int_fact__session') }} s
    LEFT JOIN interaction_per_session i
      ON s.session_id = i.session_id
    WHERE s.user_id IS NOT NULL
    GROUP BY s.user_id
),

combined_user_metrics AS (
    SELECT
        s.user_id,
        s.session_count,
        s.avg_session_duration,
        s.min_session_duration,
        s.max_session_duration,
        i.avg_interactions_per_session,
        i.min_interactions_per_session,
        i.max_interactions_per_session
    FROM session_metrics s
    LEFT JOIN interaction_metrics i USING (user_id)
)

SELECT
    COUNT(*)                      AS total_users,

    MAX(session_count)           AS max_sessions_per_user,
    MIN(session_count)           AS min_sessions_per_user,
    AVG(session_count)           AS avg_sessions_per_user,

    MAX(avg_session_duration)    AS max_avg_session_duration,
    MIN(avg_session_duration)    AS min_avg_session_duration,
    AVG(avg_session_duration)    AS avg_avg_session_duration,

    MAX(avg_interactions_per_session) AS max_avg_interactions_per_session,
    MIN(avg_interactions_per_session) AS min_avg_interactions_per_session,
    ROUND(AVG(avg_interactions_per_session),1) AS avg_interactions_per_session

FROM combined_user_metrics
