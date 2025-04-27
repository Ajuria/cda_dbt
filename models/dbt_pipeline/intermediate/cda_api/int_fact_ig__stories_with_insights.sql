{{ config(materialized='view') }}

WITH stories AS (

    SELECT
        id,
        ig_id,
        owner_id,
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
    FROM {{ ref('stg_ig__stories') }}

),

story_insights AS (

    SELECT
        id,
        reach,
        shares,
        follows,
        page_id AS insights_page_id,
        replies,
        profile_visits,
        total_interactions,
        business_account_id AS insights_business_account_id
    FROM {{ ref('stg_ig__story_insights') }}

)

SELECT
    s.*,
    i.reach,
    i.shares,
    i.follows,
    i.replies,
    i.profile_visits,
    i.total_interactions
FROM stories s
LEFT JOIN story_insights i
ON s.id = i.id
