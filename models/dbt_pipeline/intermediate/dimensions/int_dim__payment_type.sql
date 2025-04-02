-- models/dbt_pipeline/intermediate/dimensions/int_dim__payment_type.sql

{{ config(materialized='view') }}

SELECT
    DISTINCT
    p.payment_type AS payment_type_id,
    SAFE_CAST(NULL AS STRING) AS payment_type_label,
    SAFE_CAST(NULL AS STRING) AS payment_type_description
FROM {{ ref('int_payment__all') }} AS p
WHERE p.payment_type IS NOT NULL
