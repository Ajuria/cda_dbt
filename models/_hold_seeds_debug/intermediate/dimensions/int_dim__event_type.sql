{{ config(materialized='view') }}

SELECT
    event_type_id,
    event_type_label,
    event_type_description
FROM {{ ref('dim_event_type') }}

