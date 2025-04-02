-- models/dbt_pipeline/intermediate/dimensions/int_dim__event_attendance_type.sql

{{ config(materialized='view') }}

SELECT
    DISTINCT
    SAFE_CAST(NULL AS STRING) AS event_attendance_type_id,
    SAFE_CAST(NULL AS STRING) AS event_attendance_type_label,
    SAFE_CAST(NULL AS STRING) AS event_attendance_type_description
WHERE FALSE
