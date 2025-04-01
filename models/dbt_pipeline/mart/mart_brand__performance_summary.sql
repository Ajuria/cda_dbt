SELECT
  brand_id,
  COUNT(DISTINCT session_id) AS total_sessions,
  COUNT(DISTINCT user_id) AS total_users,
  COUNT(DISTINCT event_id) AS total_events,
  SUM(payment_amount) AS total_revenue
FROM {{ ref('stg_cda_owned__session') }}
LEFT JOIN {{ ref('int_payment__with_brand') }} ON session_id = session_id
GROUP BY brand_id
