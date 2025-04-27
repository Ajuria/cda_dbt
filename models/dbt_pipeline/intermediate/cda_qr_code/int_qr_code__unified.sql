{{ config(materialized='view') }}

WITH base_qr_code_content AS (

    SELECT
        project_id AS source_id,
        'project' AS source_table,
        slug AS content_slug,
        title AS content_title
    FROM {{ ref('stg_wp__project') }}

    UNION ALL

    SELECT
        project_id AS source_id,
        'project_page2' AS source_table,
        slug AS content_slug,
        title AS content_title
    FROM {{ ref('stg_wp__project_page2') }}

    UNION ALL

    SELECT
        project_id AS source_id,
        'project_page3' AS source_table,
        slug AS content_slug,
        title AS content_title
    FROM {{ ref('stg_wp__project_page3') }}

    UNION ALL

    SELECT
        project_id AS source_id,
        'project_page4' AS source_table,
        slug AS content_slug,
        title AS content_title
    FROM {{ ref('stg_wp__project_page4') }}

),

base_qr_code_display AS (

    SELECT
        post_id AS display_post_id,
        post_title AS display_post_title,
        post_slug AS display_post_slug,
        post_type AS display_post_type,
        post_date AS display_post_date
    FROM {{ ref('stg__wp_posts') }}
    WHERE post_type = 'page'
)

SELECT
    content.source_id,
    content.source_table,
    content.content_slug,
    content.content_title,
    display.display_post_id,
    display.display_post_title,
    display.display_post_slug,
    display.display_post_type,
    display.display_post_date
FROM base_qr_code_content AS content
LEFT JOIN base_qr_code_display AS display
  ON content.content_slug = display.display_post_slug
