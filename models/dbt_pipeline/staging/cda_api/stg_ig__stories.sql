-- File: models/staging/cda_api/stg_ig__stories.sql
{{ config(materialized='view') }}

SELECT
  id,
  ig_id,
  JSON_VALUE(owner, '$.id') AS owner_id,
  caption,
  page_id,
  username,
  media_url,
  permalink,
  shortcode,
  timestamp,
  like_count,
  media_type,
  thumbnail_url,
  media_product_type,
  business_account_id
FROM {{ source('cda_api', 'stories') }}