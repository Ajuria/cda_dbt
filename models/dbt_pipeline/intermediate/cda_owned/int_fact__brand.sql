{{ config(materialized='view') }}

WITH brand_data AS (

    SELECT
        page_id_final,
        brand_id,
        title AS brand_name,
        created_at,
        modified_at
    FROM {{ ref('stg_wp__project') }}
    WHERE brand_id IS NOT NULL

    UNION ALL

    SELECT
        page_id_final,
        brand_id,
        title AS brand_name,
        created_at,
        modified_at
    FROM {{ ref('stg_wp__project_page2') }}
    WHERE brand_id IS NOT NULL

    UNION ALL

    SELECT
        page_id_final,
        brand_id,
        title AS brand_name,
        created_at,
        modified_at
    FROM {{ ref('stg_wp__project_page3') }}
    WHERE brand_id IS NOT NULL

    UNION ALL

    SELECT
        page_id_final,
        brand_id,
        title AS brand_name,
        created_at,
        modified_at
    FROM {{ ref('stg_wp__project_page4') }}
    WHERE brand_id IS NOT NULL

)

SELECT
    brand_id,
    ANY_VALUE(brand_name) AS brand_name,
    MIN(created_at) AS created_at,
    MAX(modified_at) AS modified_at
FROM brand_data
GROUP BY brand_id
