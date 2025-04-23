-- File: models/intermediate/cda_api/int_fact_ig_stg__media.sql
{{ config(materialized='view') }}

WITH raw_media AS (
  SELECT
    id                             AS media_id,
    ig_id                          AS media_ig_id,
    caption                        AS media_caption,
    page_id                        AS media_page_id,
    owner                          AS owner_json,
    children                       AS children_json,
    username                       AS media_username,
    media_url                      AS media_url,
    permalink                      AS media_permalink,
    shortcode                      AS media_shortcode,
    timestamp                      AS media_timestamp,
    like_count                     AS raw_like_count,
    media_type                     AS media_type,
    thumbnail_url                  AS media_thumbnail_url,
    comments_count                 AS media_comments_count,
    is_comment_enabled             AS media_comment_enabled,
    media_product_type             AS media_product_type,
    business_account_id            AS business_account_id
  FROM {{ ref('stg_ig__media') }}
),

media AS (
  SELECT
    *,
    JSON_VALUE(owner_json, '$.id')                AS owner_id,
    JSON_VALUE(children_json, '$.data[0].id')     AS first_child_id
  FROM raw_media
),

metrics AS (
  SELECT
    id                             AS media_id,
    likes                          AS metrics_likes,
    reach                          AS metrics_reach,
    saved                          AS metrics_saved,
    shares                         AS metrics_shares,
    follows                        AS metrics_follows,
    page_id                        AS metrics_page_id,
    comments                       AS metrics_comments,
    profile_visits                 AS metrics_profile_visits,
    total_interactions             AS metrics_total_interactions,
    ig_reels_avg_watch_time        AS metrics_avg_watch_time,
    ig_reels_video_view_total_time AS metrics_view_time,
    business_account_id            AS business_account_id
  FROM {{ ref('stg_ig__media_insights') }}
)

SELECT
  m.media_id                           AS media_id,
  m.media_ig_id                        AS media_ig_id,
  m.media_caption                      AS media_caption,
  m.media_page_id                      AS media_page_id,
  m.owner_id                           AS owner_id,
  m.first_child_id                     AS first_child_id,
  m.media_username                     AS media_username,
  m.media_url                          AS media_url,
  m.media_permalink                    AS media_permalink,
  m.media_shortcode                    AS media_shortcode,
  m.media_timestamp                    AS media_timestamp,
  m.raw_like_count                     AS raw_like_count,
  m.media_type                         AS media_type,
  m.media_thumbnail_url                AS media_thumbnail_url,
  m.media_comments_count               AS media_comments_count,
  m.media_comment_enabled              AS media_comment_enabled,
  m.media_product_type                 AS media_product_type,
  m.business_account_id                AS business_account_id,
  met.metrics_likes                    AS metrics_likes,
  met.metrics_reach                    AS metrics_reach,
  met.metrics_saved                    AS metrics_saved,
  met.metrics_shares                   AS metrics_shares,
  met.metrics_follows                  AS metrics_follows,
  met.metrics_comments                 AS metrics_comments,
  met.metrics_profile_visits          AS metrics_profile_visits,
  met.metrics_total_interactions      AS metrics_total_interactions,
  met.metrics_avg_watch_time          AS metrics_avg_watch_time,
  met.metrics_view_time               AS metrics_view_time
FROM media AS m
LEFT JOIN metrics AS met
  ON m.media_id = met.media_id