SELECT
    s.event_id,
    s.brand_id,
    COUNT(DISTINCT p.payment_id)     AS payment_count,
    SUM(p.amount)                    AS total_revenue,
    COUNT(DISTINCT s.session_id)     AS contributing_sessions,
    COUNT(DISTINCT s.user_id)        AS unique_users
FROM {{ ref('int_session__with_event_unified') }} s
LEFT JOIN {{ ref('int_payment__with_brand') }} p
    ON s.session_id = p.session_id
GROUP BY
    s.event_id,
    s.brand_id
