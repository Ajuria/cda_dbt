-- models/dbt_pipeline/intermediate/cda_api/int_fact_ha__payment.sql

{{ config(materialized='view') }}

WITH base AS (
    SELECT
        p.payment_id,
        SUBSTR(TO_HEX(SHA256(LOWER(p.payer_email))), 1, 16) AS user_id,
        SAFE_CAST(p.amount AS FLOAT64) AS amount,
        p.payment_status,
        FORMAT_TIMESTAMP('%F %H:%M', CAST(SUBSTR(p.payment_date, 1, 26) AS TIMESTAMP)) AS payment_date,
        'helloasso' AS payment_type,
        p.payer_email,
        p.payer_first_name,
        p.payer_last_name,
        p.payer_country
    FROM {{ ref('stg_ha__payments') }} p
)

SELECT
    *,
    CASE
        WHEN user_id = 'a6bf0cf2f9f8bfc6' THEN 'female'
        WHEN user_id = 'cf7edb387269eaf8' THEN 'female'
        WHEN user_id = 'da79fbd035ba2d1e' THEN 'male'
        WHEN user_id = '65a7b0cb29b7c125' THEN 'female'
        WHEN user_id = 'c6126e4a3978a7f5' THEN 'female'
        WHEN user_id = '405470dcdde90768' THEN 'male'
        WHEN user_id = '538d60f22a6bc383' THEN 'male'
        WHEN user_id = '16198b2c7b3469ca' THEN 'male'
        WHEN user_id = '17178e187ca13147' THEN 'female'
        WHEN user_id = '0a25a4d415b0c380' THEN 'male'
        WHEN user_id = '4eee8cdd6f682bc7' THEN 'male'
        WHEN user_id = '76ac8d475b13b067' THEN 'male'
        WHEN user_id = '40c59ed814c6d776' THEN 'female'
        WHEN user_id = 'c13882c7ea2f3fb2' THEN 'male'
        WHEN user_id = '057d3f2ecc20365f' THEN 'male'
        WHEN user_id = '1240dcdef87ba364' THEN 'female'
        ELSE NULL
    END AS payer_gender
FROM base
