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
        REGEXP_EXTRACT(permalink, r'([^\/]+)\/?$') AS slug_cleaned,
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
        REGEXP_EXTRACT(permalink, r'([^\/]+)\/?$') AS slug_cleaned,
        timestamp,
        'story' AS ig_content_type
    FROM {{ ref('int_fact_ig__stories_with_insights') }}

),

base_website_pages AS (

    SELECT
        content_slug,
        source_id,
        brand_id_final,
        art_show_id_final,
        event_id_final,
        artist_id
    FROM {{ ref('int_fact__interaction_website') }}

),

enriched_ig_media AS (

    SELECT
        base.ig_interaction_id,
        base.ig_user_id,
        base.page_id,
        base.username,
        base.permalink,
        base.media_url,
        base.slug_cleaned,
        base.timestamp AS event_timestamp,
        base.ig_content_type,
        matched.source_id,
        matched.brand_id_final,
        matched.art_show_id_final,
        matched.event_id_final,
        matched.artist_id,
        CASE WHEN matched.source_id IS NULL THEN TRUE ELSE FALSE END AS is_unmapped_interaction
    FROM base_ig_media AS base
    LEFT JOIN base_website_pages AS matched
        ON base.slug_cleaned = matched.content_slug

)

SELECT
    ig_interaction_id,
    ig_user_id,
    page_id,
    username,
    permalink,
    media_url,
    slug_cleaned,
    event_timestamp,
    ig_content_type,
    source_id AS page_id_final,
    brand_id_final,
    art_show_id_final,
    event_id_final,
    artist_id,
    is_unmapped_interaction

FROM enriched_ig_media
WHERE event_timestamp IS NOT NULL