{{ config(materialized='view') }}

WITH base_website_interaction AS (

    SELECT
        CAST(content.source_id AS STRING) AS interaction_id,
        inter.session_id,   -- 🛠 session_id from website
        inter.user_id,      -- 🛠 user_id from website
        CAST(NULL AS STRING) AS ig_user_id,
        CAST(NULL AS STRING) AS page_id,
        CAST(NULL AS STRING) AS username,
        CAST(NULL AS STRING) AS permalink,
        CAST(NULL AS STRING) AS media_url,
        CAST(NULL AS STRING) AS ig_content_type,
        CAST(CONCAT(content.event_timestamp, ':00') AS TIMESTAMP) AS event_timestamp,
        'website' AS source_platform,
        scan.scan_id,            -- 🛠 NEW: linked scan_id if matched
        scan.is_unmapped_scan,   -- 🛠 NEW: unmapped QR detection
        TRUE AS via_qr_scan      -- 🛠 NEW: mark as QR scan
    FROM {{ ref('int_fact__interaction_website') }} AS content
    LEFT JOIN {{ ref('stg__interaction_with_page_id') }} AS inter
      ON content.source_id = inter.page_id_final
    LEFT JOIN {{ ref('int_qr_code__scan') }} AS scan
      ON inter.session_id = scan.session_id
    WHERE content.source_id IS NOT NULL

),

base_ig_interaction AS (

    SELECT
        CAST(ig_interaction_id AS STRING) AS interaction_id,
        SUBSTR(TO_HEX(SHA256(CAST(ig_user_id AS STRING))), 1, 16) AS user_id,
        CAST(NULL AS STRING) AS session_id,
        CAST(ig_user_id AS STRING) AS ig_user_id,
        CAST(page_id AS STRING) AS page_id,
        CAST(username AS STRING) AS username,
        CAST(permalink AS STRING) AS permalink,
        CAST(media_url AS STRING) AS media_url,
        CAST(ig_content_type AS STRING) AS ig_content_type,
        CAST(event_timestamp AS TIMESTAMP) AS event_timestamp,
        'instagram' AS source_platform,
        CAST(NULL AS STRING) AS scan_id,         
        CAST(NULL AS BOOLEAN) AS is_unmapped_scan,
        FALSE AS via_qr_scan
    FROM {{ ref('int_fact_ig__interaction') }}
    WHERE ig_interaction_id IS NOT NULL

),

all_interactions_raw AS (

    SELECT * FROM base_website_interaction
    UNION ALL
    SELECT * FROM base_ig_interaction

),

-- 🛠 SAFE FILTER: keep only valid user_id (present in user table) or NULL
all_interactions AS (

    SELECT *
    FROM all_interactions_raw
    WHERE user_id IS NULL 
       OR user_id IN (SELECT user_id FROM {{ ref('int_fact__user') }})

)

SELECT
    interaction_id,
    user_id,
    session_id,
    ig_user_id,
    page_id,
    username,
    permalink,
    media_url,
    ig_content_type,
    event_timestamp,
    source_platform,
    scan_id,      
    is_unmapped_scan, 
    via_qr_scan     
FROM all_interactions
QUALIFY ROW_NUMBER() OVER (PARTITION BY interaction_id ORDER BY event_timestamp DESC) = 1
