-- models/dbt_pipeline/intermediate/int_payment__with_brand.sql

WITH base_payment AS (
    SELECT
        payment_id,
        amount           AS payment_amount,
        payment_status,
        payer_json,
        payment_date,
        created_at
    FROM {{ ref('stg_cda_api__helloasso_payment') }}
)

SELECT
    bp.payment_id,
    bp.payment_amount,
    bp.payment_status,
    bp.payer_json,
    bp.payment_date,
    bp.created_at
FROM base_payment bp
