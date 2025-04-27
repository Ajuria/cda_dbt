{{ config(materialized='view') }}

SELECT
    event_attendance_id,
    event_id,
    user_id,
    FORMAT_TIMESTAMP('%F %T', attended_at) AS attended_at
FROM {{ source('cda_owned', 'event_attendance') }}
