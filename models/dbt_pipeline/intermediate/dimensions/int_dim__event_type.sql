-- models/dbt_pipeline/intermediate/dimensions/int_dim__event_type.sql

{{ config(materialized='view') }}

SELECT
    DISTINCT
    e.event_type AS event_type_id,
    SAFE_CAST(NULL AS STRING) AS event_type_label,
    SAFE_CAST(NULL AS STRING) AS event_type_description
FROM {{ ref('stg__cda_owned__event') }} AS e
WHERE e.event_type IS NOT NULL
