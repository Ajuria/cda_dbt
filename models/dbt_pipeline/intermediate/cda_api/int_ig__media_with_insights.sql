{{ config(materialized='view') }}

WITH media AS (

    SELECT
        id,
        ig_id,
        caption,
        page_id,
        owner_id,
        first_child_id,
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
    FROM {{ ref('stg_ig__media') }}

),

media_insights AS (

    SELECT
        id,
        likes,
        reach,
        saved,
        shares,
        follows,
        page_id AS insights_page_id,
        comments,
        profile_visits,
        total_interactions,
        ig_reels_avg_watch_time,
        ig_reels_video_view_total_time,
        business_account_id AS insights_business_account_id
    FROM {{ ref('stg_ig__media_insights') }}

)

SELECT
    m.*,
    i.likes,
    i.reach,
    i.saved,
    i.shares,
    i.follows,
    i.comments,
    i.profile_visits,
    i.total_interactions,
    i.ig_reels_avg_watch_time,
    i.ig_reels_video_view_total_time
FROM media m
LEFT JOIN media_insights i
ON m.id = i.id
