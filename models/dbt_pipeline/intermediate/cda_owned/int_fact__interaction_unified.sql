{{ config(materialized='view') }}

WITH website_interactions AS (

    SELECT
        source_id AS interaction_id,
        CAST(NULL AS STRING) AS ig_user_id,
        CAST(NULL AS STRING) AS page_id,
        CAST(NULL AS STRING) AS username,
        CAST(NULL AS STRING) AS permalink,
        CAST(NULL AS STRING) AS media_url,
        CAST(NULL AS STRING) AS ig_content_type,
        event_timestamp,
        'website' AS source_platform
    FROM {{ ref('int_fact__interaction_website') }}

),

instagram_interactions AS (

    SELECT
        ig_interaction_id AS interaction_id,
        ig_user_id,
        page_id,
        username,
        permalink,
        media_url,
        ig_content_type,
        FORMAT_TIMESTAMP('%F %H:%M', CAST(event_timestamp AS TIMESTAMP)) AS event_timestamp,
        'instagram' AS source_platform
    FROM {{ ref('int_fact_ig__interaction') }}

),

all_interactions AS (

    SELECT * FROM website_interactions
    UNION ALL
    SELECT * FROM instagram_interactions

)

SELECT
    interaction_id,
    ig_user_id,
    page_id,
    username,
    permalink,
    media_url,
    ig_content_type,
    event_timestamp,
    source_platform
FROM all_interactions
WHERE interaction_id IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY interaction_id ORDER BY event_timestamp DESC) = 1

