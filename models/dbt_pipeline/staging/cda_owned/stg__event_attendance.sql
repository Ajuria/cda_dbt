{{ config(materialized='view') }}

SELECT
    event_attendance_id,
    event_id,
    user_id,
    attended_at
FROM `cda-database.cda_owned.event_attendance`
