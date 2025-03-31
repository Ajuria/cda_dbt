SELECT
    payment_id,
    interaction_id,
    user_id,
    amount,
    status,
    created_at,
    'helloasso' AS payment_type
FROM {{ ref('stg_cda_api__helloasso_payment') }}
