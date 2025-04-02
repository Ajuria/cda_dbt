-- models/dbt_pipeline/intermediate/dimensions/int_dim__interaction_type.sql

{{ config(materialized='view') }}

SELECT
    DISTINCT
    i.interaction_type AS interaction_type_id,
    SAFE_CAST(NULL AS STRING) AS interaction_type_label,
    SAFE_CAST(NULL AS STRING) AS interaction_type_description
FROM {{ ref('stg__cda_owned__interaction') }} AS i
WHERE i.interaction_type IS NOT NULL
