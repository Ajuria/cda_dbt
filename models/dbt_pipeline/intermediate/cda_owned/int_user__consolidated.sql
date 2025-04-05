-- models/dbt_pipeline/intermediate/cda_owned/int_user__consolidated.sql

{{ config(materialized='view') }}

SELECT
  user_id,
  NULL AS payer_first_name,
  NULL AS payer_last_name,
  NULL AS payer_email,
  NULL AS payer_country,
  device,
  geo_ip,
  geo_name,
  ingested_at
FROM {{ ref('stg__user') }}

UNION ALL

SELECT
  user_id,
  payer_first_name,
  payer_last_name,
  payer_email,
  payer_country,
  NULL AS device,
  NULL AS geo_ip,
  NULL AS geo_name,
  ingested_at
FROM {{ ref('int_user__from_payments') }}

