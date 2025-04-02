-- models/dbt_pipeline/intermediate/dimensions/int_dim__qr_code_type.sql

{{ config(materialized='view') }}

SELECT
    DISTINCT
    q.qr_code_type AS qr_code_type_id,
    SAFE_CAST(NULL AS STRING) AS qr_code_type_label,
    SAFE_CAST(NULL AS STRING) AS qr_code_type_description
FROM {{ ref('stg__cda_owned__qr_code') }} AS q
WHERE q.qr_code_type IS NOT NULL
