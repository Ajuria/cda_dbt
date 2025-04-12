-- models/dbt_pipeline/intermediate/cda_api/int_payment__all.sql

{{ config(materialized='view') }}

SELECT
    p.payment_id                AS payment_id,
    p.payment_type              AS payment_type,
    SAFE_CAST(p.amount          AS FLOAT64) AS amount,
    p.user_id                   AS user_id,
    SAFE_CAST(NULL AS STRING)   AS interaction_id,  -- not available in HelloAsso
    p.payment_date              AS created_at,
    p.brand_id                  AS brand_id
FROM {{ ref('int_payment__with_metadata') }} AS p
