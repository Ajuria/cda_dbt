{{ config(materialized='view') }}

SELECT
  -- Hash email for uniqueness and compatibility with stg__user format
  TO_HEX(SHA256(payer_email)) AS user_id,

  payer_first_name,
  payer_last_name,
  payer_email,
  payer_country,
  MIN(payment_date) AS first_seen_date,
  CURRENT_TIMESTAMP() AS ingested_at

FROM {{ ref('stg__payments') }}
WHERE payer_email IS NOT NULL
GROUP BY
  payer_first_name,
  payer_last_name,
  payer_email,
  payer_country
