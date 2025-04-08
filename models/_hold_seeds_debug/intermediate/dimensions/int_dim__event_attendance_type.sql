{{ config(materialized='view') }}

SELECT
    event_attendance_type_id,
    event_attendance_type_label,
    event_attendance_type_description
FROM {{ ref('dim_event_attendance_type') }}
