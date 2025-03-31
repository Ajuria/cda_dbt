SELECT
    brand_id,
    COUNT(DISTINCT user_id)         AS total_users,
    COUNT(DISTINCT session_id)      AS total_sessions,
    COUNT(DISTINCT event_id)        AS total_events,
    COUNT(DISTINCT payment_id)      AS total_payments,
    SUM(amount)                     AS total_revenue
FROM {{ ref('int_session__with_event_unified') }} s
LEFT JOIN {{ ref('int_payment__with_brand') }} p
    ON s.session_id = p.session_id
GROUP BY
    brand_id
