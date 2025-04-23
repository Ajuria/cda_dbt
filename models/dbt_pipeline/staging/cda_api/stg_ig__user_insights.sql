-- File: models/staging/cda_api/stg_ig__user_insights.sql
{{ config(materialized='view') }}

SELECT
  date,
  reach,
  page_id,
  reach_week,
  reach_days_28,
  follower_count,
  JSON_VALUE(online_followers, '$.value') AS online_followers_value,
  business_account_id
FROM {{ source('cda_api', 'user_insights') }}