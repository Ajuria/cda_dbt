-- File: models/intermediate/cda_api/int_fact_stg__stories.sql
{{ config(materialized='view') }}

WITH raw_stories AS (
  SELECT
    id                             AS story_id,
    ig_id                          AS story_ig_id,
    caption                        AS story_caption,
    page_id                        AS story_page_id,
    username                       AS story_username,
    media_url                      AS story_url,
    permalink                      AS story_permalink,
    shortcode                      AS story_shortcode,
    timestamp                      AS story_timestamp,
    like_count                     AS raw_like_count,
    media_type                     AS story_type,
    thumbnail_url                  AS story_thumbnail_url,
    media_product_type             AS story_product_type,
    business_account_id            AS business_account_id
  FROM {{ ref('stg_ig__stories') }}
),

metrics AS (
  SELECT
    id                             AS story_id,
    reach                          AS metrics_reach,
    shares                         AS metrics_shares,
    follows                        AS metrics_follows,
    page_id                        AS metrics_page_id,
    replies                        AS metrics_replies,
    profile_visits                 AS metrics_profile_visits,
    total_interactions             AS metrics_total_interactions,
    business_account_id            AS business_account_id
  FROM {{ ref('stg_ig__story_insights') }}
)

SELECT
  s.story_id                          AS story_id,
  s.story_ig_id                       AS story_ig_id,
  s.story_caption                     AS story_caption,
  s.story_page_id                     AS story_page_id,
  s.story_username                    AS story_username,
  s.story_url                         AS story_url,
  s.story_permalink                   AS story_permalink,
  s.story_shortcode                   AS story_shortcode,
  s.story_timestamp                   AS story_timestamp,
  s.raw_like_count                    AS raw_like_count,
  s.story_type                        AS story_type,
  s.story_thumbnail_url               AS story_thumbnail_url,
  s.story_product_type                AS story_product_type,
  s.business_account_id               AS business_account_id,
  met.metrics_reach                   AS metrics_reach,
  met.metrics_shares                  AS metrics_shares,
  met.metrics_follows                 AS metrics_follows,
  met.metrics_replies                 AS metrics_replies,
  met.metrics_profile_visits          AS metrics_profile_visits,
  met.metrics_total_interactions      AS metrics_total_interactions
FROM raw_stories AS s
LEFT JOIN metrics AS met
  ON s.story_id = met.story_id