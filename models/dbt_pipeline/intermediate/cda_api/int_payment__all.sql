-- models/dbt_pipeline/intermediate/cda_api/int_payment__all.sql

{{ config(materialized='view') }}

SELECT
    p.payment_id AS payment_id,
    p.payment_type AS payment_type,
    SAFE_CAST(p.amount AS FLOAT64) AS amount,
    p.user_id AS user_id,
    SAFE_CAST(NULL AS STRING) AS interaction_id,  -- not available in HelloAsso
    p.payment_date AS created_at,
    p.brand_id AS brand_id
FROM {{ ref('int_payment__with_metadata') }} AS p

UNION ALL

SELECT
    t.payment_id AS payment_id,
    'other' AS payment_type,
    SAFE_CAST(t.amount AS FLOAT64) AS amount,
    t.user_id AS user_id,
    t.interaction_id AS interaction_id,
    SAFE_CAST(t.created_at AS TIMESTAMP) AS created_at,
    SAFE_CAST(NULL AS STRING) AS brand_id  -- placeholder, to be enriched
FROM {{ ref('stg__cda_api__transaction') }} AS t
