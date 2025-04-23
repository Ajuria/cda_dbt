-- File: models/staging/cda_api/stg_ig__users.sql
{{ config(materialized='view') }}

SELECT
  id,
  name,
  ig_id,
  page_id,
  website,
  username,
  biography,
  media_count,
  follows_count,
  followers_count,
  profile_picture_url
FROM {{ source('cda_api', 'users') }}