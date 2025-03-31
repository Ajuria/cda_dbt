SELECT
    transaction_id,
    transaction_amount_eur,
    transaction_date,
    user_id,
    brand_id,
    event_id,
    source_platform

FROM {{ ref('stg_cda_api__helloasso_payment') }}

