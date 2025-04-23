{{ config(materialized='view') }}

SELECT
  id,
  ig_id,
  caption,
  page_id,
  owner, 
  children,
  JSON_VALUE(owner, '$.id') AS owner_id,
  JSON_VALUE(children, '$.data[0].id') AS first_child_id,
  username,
  media_url,
  permalink,
  shortcode,
  timestamp,
  like_count,
  media_type,
  thumbnail_url,
  comments_count,
  is_comment_enabled,
  media_product_type,
  business_account_id
FROM {{ source('cda_api', 'media') }}
