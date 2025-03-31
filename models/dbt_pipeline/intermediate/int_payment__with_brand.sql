-- models/dbt_pipeline/intermediate/int_payment__with_brand.sql

WITH base_payment AS (
    SELECT
        id,
        amount,
        fee,
        payout_amount,
        created_at,
        payer_email,
        user_id,
        art_show_id,
        event_id
    FROM {{ ref('stg_cda_api__helloasso_payment') }}
),

brand_mapping AS (
    SELECT
        art_show_id,
        brand_id
    FROM {{ ref('stg_seed__art_show_brand_mapping') }}
)

SELECT
    bp.*,
    bm.brand_id
FROM base_payment bp
LEFT JOIN brand_mapping bm
    ON bp.art_show_id = bm.art_show_id
