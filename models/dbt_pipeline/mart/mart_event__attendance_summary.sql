SELECT
    ea.event_id                         AS event_id,
    COUNT(DISTINCT ea.user_id)         AS unique_attendees,
    COUNT(ea.event_attendance_id)      AS total_attendances,
    MIN(ea.timestamp)                  AS first_attendance,
    MAX(ea.timestamp)                  AS last_attendance
FROM {{ ref('stg_cda__event_attendance') }} ea
GROUP BY ea.event_id
