{{ config(materialized='view') }}

WITH base_qr_code_content AS (

    -- Your original QR code pages and fixed pages
    SELECT
        SAFE_CAST(page_id_final AS STRING) AS source_id,
        SAFE_CAST(page_id_final AS STRING) AS qr_code_id,
        'project' AS source_table,
        SAFE_CAST(slug AS STRING) AS content_slug,
        JSON_VALUE(title, '$') AS content_title,
        SAFE_CAST(brand_id_final AS STRING) AS brand_id_final,
        SAFE_CAST(art_show_id_final AS STRING) AS art_show_id_final,
        SAFE_CAST(event_id_final AS STRING) AS event_id_final,
        SAFE_CAST(artist_id AS STRING) AS artist_id,
        NULL AS display_post_id,
        NULL AS display_post_title,
        NULL AS display_post_slug,
        NULL AS display_post_type,
        NULL AS display_post_date,
        NULL AS event_timestamp
    FROM {{ ref('stg_wp__project') }}

    UNION ALL

    SELECT
        SAFE_CAST(page_id_final AS STRING),
        SAFE_CAST(page_id_final AS STRING),
        'project_page2',
        SAFE_CAST(slug AS STRING),
        JSON_VALUE(title, '$'),
        SAFE_CAST(brand_id_final AS STRING),
        SAFE_CAST(art_show_id_final AS STRING),
        SAFE_CAST(event_id_final AS STRING),
        SAFE_CAST(artist_id AS STRING),
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL AS event_timestamp
    FROM {{ ref('stg_wp__project_page2') }}

    UNION ALL

    SELECT
        SAFE_CAST(page_id_final AS STRING),
        SAFE_CAST(page_id_final AS STRING),
        'project_page3',
        SAFE_CAST(slug AS STRING),
        JSON_VALUE(title, '$'),
        SAFE_CAST(brand_id_final AS STRING),
        SAFE_CAST(art_show_id_final AS STRING),
        SAFE_CAST(event_id_final AS STRING),
        SAFE_CAST(artist_id AS STRING),
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL AS event_timestamp
    FROM {{ ref('stg_wp__project_page3') }}

    UNION ALL

    SELECT
        SAFE_CAST(page_id_final AS STRING),
        SAFE_CAST(page_id_final AS STRING),
        'project_page4',
        SAFE_CAST(slug AS STRING),
        JSON_VALUE(title, '$'),
        SAFE_CAST(brand_id_final AS STRING),
        SAFE_CAST(art_show_id_final AS STRING),
        SAFE_CAST(event_id_final AS STRING),
        SAFE_CAST(artist_id AS STRING),
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL AS event_timestamp
    FROM {{ ref('stg_wp__project_page4') }}

    UNION ALL

    -- Fixed manual pages
    SELECT
        SUBSTR(TO_HEX(SHA256('programme')), 1, 16),
        SUBSTR(TO_HEX(SHA256('programme')), 1, 16),
        'fixed_page',
        'programme',
        'Programme',
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL AS event_timestamp

    UNION ALL

    SELECT
        SUBSTR(TO_HEX(SHA256('a-propos')), 1, 16),
        SUBSTR(TO_HEX(SHA256('a-propos')), 1, 16),
        'fixed_page',
        'a-propos',
        'À propos',
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL AS event_timestamp

    UNION ALL

    SELECT
        SUBSTR(TO_HEX(SHA256('actualites')), 1, 16),
        SUBSTR(TO_HEX(SHA256('actualites')), 1, 16),
        'fixed_page',
        'actualites',
        'Actualités',
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL AS event_timestamp

    UNION ALL

    SELECT
        SUBSTR(TO_HEX(SHA256('evenements')), 1, 16),
        SUBSTR(TO_HEX(SHA256('evenements')), 1, 16),
        'fixed_page',
        'evenements',
        'Événements',
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL AS event_timestamp

),

base_qr_code_display AS (

    -- WordPress metadata to enrich
    SELECT
        SAFE_CAST(post_id AS STRING) AS post_display_id,
        post_title AS post_display_title,
        post_slug AS post_display_slug,
        post_type AS post_display_type,
        FORMAT_TIMESTAMP('%F %H:%M', CAST(post_date AS TIMESTAMP)) AS post_display_date
    FROM {{ ref('stg__wp_posts') }}
    WHERE post_type = 'page'

),

interaction_qr_code_enrichment AS (

    -- Join interactions with QR code
    SELECT
        inter.interaction_id,
        inter.session_id,
        inter.user_id,
        inter.source_platform,
        inter.cda_website_source,
        inter.slug_cleaned,
        inter.page_id_final,
        inter.social_media_action,
        FORMAT_TIMESTAMP('%F %H:%M', CAST(inter.timestamp AS TIMESTAMP)) AS event_timestamp,

        qr.source_table,
        qr.content_slug,
        qr.content_title,
        qr.brand_id_final,
        qr.art_show_id_final,
        qr.event_id_final,
        qr.artist_id,
        qr.post_display_id,
        qr.post_display_title,
        qr.post_display_slug,
        qr.post_display_type,
        qr.post_display_date

    FROM {{ ref('stg__interaction_with_page_id') }} AS inter
    LEFT JOIN (
        SELECT
            source_id,
            source_table,
            content_slug,
            content_title,
            brand_id_final,
            art_show_id_final,
            event_id_final,
            artist_id,
            post_display_id,
            post_display_title,
            post_display_slug,
            post_display_type,
            post_display_date
        FROM base_qr_code_content
        LEFT JOIN base_qr_code_display
            ON base_qr_code_content.content_slug = base_qr_code_display.post_display_slug
    ) AS qr
    ON inter.page_id_final = qr.source_id

)

-- Final union: existing QR pages + enriched interactions
SELECT
    content.source_id,
    content.qr_code_id,
    content.source_table,
    content.content_slug,
    content.content_title,
    content.brand_id_final,
    content.art_show_id_final,
    content.event_id_final,
    content.artist_id,
    display.post_display_id AS display_post_id,
    display.post_display_title AS display_post_title,
    display.post_display_slug AS display_post_slug,
    display.post_display_type AS display_post_type,
    display.post_display_date AS display_post_date,
    NULL AS event_timestamp
FROM base_qr_code_content AS content
LEFT JOIN base_qr_code_display AS display
    ON content.content_slug = display.post_display_slug

UNION ALL

SELECT
    enrich.page_id_final AS source_id,
    NULL AS qr_code_id,
    enrich.source_table,
    enrich.content_slug,
    enrich.content_title,
    enrich.brand_id_final,
    enrich.art_show_id_final,
    enrich.event_id_final,
    enrich.artist_id,
    enrich.post_display_id AS display_post_id,
    enrich.post_display_title AS display_post_title,
    enrich.post_display_slug AS display_post_slug,
    enrich.post_display_type AS display_post_type,
    enrich.post_display_date AS display_post_date,
    enrich.event_timestamp 
FROM interaction_qr_code_enrichment AS enrich
WHERE enrich.session_id IS NOT NULL
