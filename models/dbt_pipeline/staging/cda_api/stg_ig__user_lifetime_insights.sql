-- File: models/staging/cda_api/stg_ig__user_lifetime_insights.sql
{{ config(materialized='view') }}

SELECT
  JSON_VALUE(value, '$.value') AS value,
  metric,
  page_id,
  breakdown,
  business_account_id
FROM {{ source('cda_api', 'user_lifetime_insights') }}