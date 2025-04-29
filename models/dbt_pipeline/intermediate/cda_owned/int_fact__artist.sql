{{ config(materialized='view') }}

WITH artist_data AS (

    SELECT
        page_id_final,
        artist_id,
        title AS artist_name,
        created_at,
        modified_at
    FROM {{ ref('stg_wp__project') }}
    WHERE artist_id IS NOT NULL

    UNION ALL

    SELECT
        page_id_final,
        artist_id,
        title AS artist_name,
        created_at,
        modified_at
    FROM {{ ref('stg_wp__project_page2') }}
    WHERE artist_id IS NOT NULL

    UNION ALL

    SELECT
        page_id_final,
        artist_id,
        title AS artist_name,
        created_at,
        modified_at
    FROM {{ ref('stg_wp__project_page3') }}
    WHERE artist_id IS NOT NULL

    UNION ALL

    SELECT
        page_id_final,
        artist_id,
        title AS artist_name,
        created_at,
        modified_at
    FROM {{ ref('stg_wp__project_page4') }}
    WHERE artist_id IS NOT NULL

)

SELECT
    artist_id,
    ANY_VALUE(artist_name) AS artist_name,
    MIN(created_at) AS created_at,
    MAX(modified_at) AS modified_at
FROM artist_data
GROUP BY artist_id

