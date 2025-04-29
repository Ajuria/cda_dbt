{{ config(materialized='view') }}

WITH base_ig_media AS (

    SELECT
        SUBSTR(TO_HEX(SHA256(CAST(id AS STRING))), 1, 16) AS ig_interaction_id,
        id AS ig_media_id,
        ig_id AS ig_user_id,
        page_id,
        username,
        media_url,
        permalink,
        timestamp,
        'media' AS ig_content_type
    FROM {{ ref('int_ig__media_with_insights') }}

    UNION ALL

    SELECT
        SUBSTR(TO_HEX(SHA256(CAST(id AS STRING))), 1, 16) AS ig_interaction_id,
        id AS ig_story_id,
        ig_id AS ig_user_id,
        page_id,
        username,
        media_url,
        permalink,
        timestamp,
        'story' AS ig_content_type
    FROM {{ ref('int_fact_ig__stories_with_insights') }}

)

SELECT
    ig_interaction_id,
    ig_user_id,
    page_id,
    username,
    permalink,
    media_url,
    timestamp AS event_timestamp,
    ig_content_type

FROM base_ig_media

WHERE timestamp IS NOT NULL
