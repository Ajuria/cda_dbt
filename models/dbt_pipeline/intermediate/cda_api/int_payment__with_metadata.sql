-- models/dbt_pipeline/intermediate/cda_api/int_payment__with_metadata.sql

{{ config(materialized='view') }}

SELECT
    p.payment_id                            AS payment_id,
    SAFE_CAST(p.amount AS FLOAT64)          AS amount,
    p.payment_status                        AS payment_status,
    SAFE_CAST(p.payment_date AS TIMESTAMP)  AS payment_date,
    TO_HEX(SHA256(p.payer_email))           AS user_id,
    'helloasso'                             AS payment_type,
    SAFE_CAST(NULL AS STRING)               AS brand_id  -- to be enriched later
FROM {{ ref('stg__payments') }} AS p
