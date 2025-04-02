-- models/dbt_pipeline/intermediate/dimensions/int_dim__brand_type.sql

{{ config(materialized='view') }}

SELECT
    DISTINCT
    b.brand_id AS brand_id,
    SAFE_CAST(NULL AS STRING) AS brand_type_id,
    SAFE_CAST(NULL AS STRING) AS brand_type_label,
    SAFE_CAST(NULL AS STRING) AS brand_type_description
FROM {{ ref('stg__cda_owned__brand') }} AS b
WHERE b.brand_id IS NOT NULL
