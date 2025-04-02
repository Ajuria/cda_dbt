-- models/dbt_pipeline/intermediate/dimensions/int_dim__user_type.sql

{{ config(materialized='view') }}

SELECT
    DISTINCT
    u.user_type_id AS user_type_id,
    SAFE_CAST(NULL AS STRING) AS user_type_label,  -- can be replaced by join or mapping later
    SAFE_CAST(NULL AS STRING) AS user_type_description
FROM {{ ref('stg__cda_owned__user') }} AS u
WHERE u.user_type_id IS NOT NULL
