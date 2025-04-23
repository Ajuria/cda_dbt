-- File: models/staging/cda_api/stg_ig__media_insights.sql
{{ config(materialized='view') }}

SELECT
  id,
  likes,
  reach,
  saved,
  shares,
  follows,
  page_id,
  comments,
  profile_visits,
  total_interactions,
  ig_reels_avg_watch_time,
  ig_reels_video_view_total_time,
  business_account_id
FROM {{ source('cda_api', 'media_insights') }}