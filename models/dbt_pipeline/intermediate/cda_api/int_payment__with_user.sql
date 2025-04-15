-- models/dbt_pipeline/intermediate/cda_api/int_payment__with_user.sql

{{ config(materialized='view') }}

SELECT
  p.payment_id,
  p.amount,
  p.payment_status,
  p.payer_email,
  p.payer_first_name,
  p.payer_last_name,
  p.payer_country,
  p.payment_date,
  p.ingested_at             AS payment_ingested_at,

  u.user_id,
  u.geo_ip,
  u.geo_name,
  u.device,
  u.ingested_at             AS user_ingested_at

FROM {{ ref('stg__payments') }} AS p
LEFT JOIN {{ ref('int_user__consolidated') }} AS u
  ON p.payer_email = u.payer_email
