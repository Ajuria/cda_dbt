{{ config(materialized='view') }}

WITH base_qr_code_content AS (

    -- Normal project pages
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
        NULL AS display_post_date
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
        NULL
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
        NULL
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
        NULL
    FROM {{ ref('stg_wp__project_page4') }}

    UNION ALL

    -- Fixed pages manually added (programme, à propos, etc.)
    SELECT
        SUBSTR(TO_HEX(SHA256('programme')), 1, 16) AS source_id,
        SUBSTR(TO_HEX(SHA256('programme')), 1, 16) AS qr_code_id,
        'fixed_page' AS source_table,
        'programme' AS content_slug,
        'Programme' AS content_title,
        CAST(NULL AS STRING) AS brand_id_final,
        CAST(NULL AS STRING) AS art_show_id_final,
        CAST(NULL AS STRING) AS event_id_final,
        CAST(NULL AS STRING) AS artist_id,
        CAST(NULL AS STRING) AS display_post_id,
        CAST(NULL AS STRING) AS display_post_title,
        CAST(NULL AS STRING) AS display_post_slug,
        CAST(NULL AS STRING) AS display_post_type,
        CAST(NULL AS STRING) AS display_post_date

    UNION ALL

    SELECT
        SUBSTR(TO_HEX(SHA256('a-propos')), 1, 16),
        SUBSTR(TO_HEX(SHA256('a-propos')), 1, 16),
        'fixed_page',
        'a-propos',
        'À propos',
        CAST(NULL AS STRING),
        CAST(NULL AS STRING),
        CAST(NULL AS STRING),
        CAST(NULL AS STRING),
        CAST(NULL AS STRING),
        CAST(NULL AS STRING),
        CAST(NULL AS STRING),
        CAST(NULL AS STRING),
        CAST(NULL AS STRING)

    UNION ALL

    SELECT
        SUBSTR(TO_HEX(SHA256('actualites')), 1, 16),
        SUBSTR(TO_HEX(SHA256('actualites')), 1, 16),
        'fixed_page',
        'actualites',
        'Actualités',
        CAST(NULL AS STRING),
        CAST(NULL AS STRING),
        CAST(NULL AS STRING),
        CAST(NULL AS STRING),
        CAST(NULL AS STRING),
        CAST(NULL AS STRING),
        CAST(NULL AS STRING),
        CAST(NULL AS STRING),
        CAST(NULL AS STRING)

    UNION ALL

    SELECT
        SUBSTR(TO_HEX(SHA256('evenements')), 1, 16),
        SUBSTR(TO_HEX(SHA256('evenements')), 1, 16),
        'fixed_page',
        'evenements',
        'Événements',
        CAST(NULL AS STRING),
        CAST(NULL AS STRING),
        CAST(NULL AS STRING),
        CAST(NULL AS STRING),
        CAST(NULL AS STRING),
        CAST(NULL AS STRING),
        CAST(NULL AS STRING),
        CAST(NULL AS STRING),
        CAST(NULL AS STRING)

),

base_qr_code_display AS (

    -- WordPress posts to link slugs to page metadata
    SELECT
        SAFE_CAST(post_id AS STRING) AS display_post_id,
        post_title AS display_post_title,
        post_slug AS display_post_slug,
        post_type AS display_post_type,
        FORMAT_TIMESTAMP('%F %H:%M', CAST(post_date AS TIMESTAMP)) AS display_post_date
    FROM {{ ref('stg__wp_posts') }}
    WHERE post_type = 'page'

)

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
    display.display_post_id,
    display.display_post_title,
    display.display_post_slug,
    display.display_post_type,
    display.display_post_date
FROM base_qr_code_content AS content
LEFT JOIN base_qr_code_display AS display
    ON content.content_slug = display.display_post_slug
