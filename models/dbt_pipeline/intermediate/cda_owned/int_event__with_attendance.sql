-- models/dbt_pipeline/intermediate/cda_owned/int_event__with_attendance.sql

{{ config(materialized='view') }}

WITH attendance_agg AS (
    SELECT
        ea.event_id AS event_id,
        COUNT(DISTINCT ea.user_id) AS total_attendance,
        ARRAY_AGG(DISTINCT ea.user_id) AS attendee_user_ids
    FROM {{ ref('stg__cda_owned__event_attendance') }} AS ea
    WHERE ea.event_id IS NOT NULL
    GROUP BY ea.event_id
),

event_base AS (
    SELECT
        e.event_id AS event_id,
        e.event_name AS event_name,
        e.event_type AS event_type,
        e.event_start AS event_start,
        e.event_end AS event_end,
        e.updated_at AS updated_at
    FROM {{ ref('stg__cda_owned__event') }} AS e
)

SELECT
    eb.event_id,
    eb.event_name,
    eb.event_type,
    eb.event_start,
    eb.event_end,
    eb.updated_at,
    COALESCE(aa.total_attendance, 0) AS total_attendance,
    aa.attendee_user_ids AS attendee_user_ids
FROM event_base AS eb
LEFT JOIN attendance_agg AS aa
    ON eb.event_id = aa.event_id
