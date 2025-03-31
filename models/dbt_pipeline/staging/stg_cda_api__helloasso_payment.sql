SELECT
    id,
    payer_email,
    payer_first_name,
    payer_last_name,
    amount,
    date,
    status,
    payment_type,
    user_id,
    form_id,
    organization_id,
    brand_id
FROM `cda-database.cda_api.payments`
