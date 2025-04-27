{{ config(materialized='view') }}

WITH project_data AS (

    SELECT
        CAST(project_id AS STRING) AS project_id,
        CAST(art_show_id AS STRING) AS art_show_id,
        title AS art_show_title,
        created_at AS created_at,
        modified_at AS modified_at
    FROM {{ ref('stg_wp__project') }}
    WHERE art_show_id IS NOT NULL

    UNION ALL

    SELECT
        CAST(project_id AS STRING) AS project_id,
        CAST(art_show_id AS STRING) AS art_show_id,
        title AS art_show_title,
        created_at AS created_at,
        modified_gmt AS modified_at
    FROM {{ ref('stg_wp__project_page2') }}
    WHERE art_show_id IS NOT NULL

    UNION ALL

    SELECT
        CAST(project_id AS STRING) AS project_id,
        CAST(art_show_id AS STRING) AS art_show_id,
        title AS art_show_title,
        created_at AS created_at,
        modified_gmt AS modified_at
    FROM {{ ref('stg_wp__project_page3') }}
    WHERE art_show_id IS NOT NULL

    UNION ALL

    SELECT
        CAST(project_id AS STRING) AS project_id,
        CAST(art_show_id AS STRING) AS art_show_id,
        title AS art_show_title,
        created_at AS created_at,
        modified_gmt AS modified_at
    FROM {{ ref('stg_wp__project_page4') }}
    WHERE art_show_id IS NOT NULL

)

SELECT
    art_show_id,
    MIN(created_at) AS created_at,
    MAX(modified_at) AS modified_at,
    ANY_VALUE(art_show_title) AS art_show_title
FROM project_data
GROUP BY art_show_id
