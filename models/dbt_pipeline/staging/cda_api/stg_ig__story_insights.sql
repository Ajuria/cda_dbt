-- File: models/staging/cda_api/stg_ig__story_insights.sql
{{ config(materialized='view') }}

SELECT
  id,
  reach,
  shares,
  follows,
  page_id,
  replies,
  profile_visits,
  total_interactions,
  business_account_id
FROM {{ source('cda_api', 'story_insights') }}