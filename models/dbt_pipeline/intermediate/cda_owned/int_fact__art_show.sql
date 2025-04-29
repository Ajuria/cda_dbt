WITH project_data AS (

    SELECT
        page_id_final,
        art_show_id_final,
        title,
        created_at,
        modified_at
    FROM {{ ref('stg_wp__project') }}
    WHERE art_show_id_final IS NOT NULL

    UNION ALL

    SELECT
        page_id_final,
        art_show_id_final,
        title,
        created_at,
        modified_at
    FROM {{ ref('stg_wp__project_page2') }}
    WHERE art_show_id_final IS NOT NULL

    UNION ALL

    SELECT
        page_id_final,
        art_show_id_final,
        title,
        created_at,
        modified_at
    FROM {{ ref('stg_wp__project_page3') }}
    WHERE art_show_id_final IS NOT NULL

    UNION ALL

    SELECT
        page_id_final,
        art_show_id_final,
        title,
        created_at,
        modified_at
    FROM {{ ref('stg_wp__project_page4') }}
    WHERE art_show_id_final IS NOT NULL

)

SELECT
    art_show_id_final AS art_show_id,
    MIN(created_at) AS created_at,
    MAX(modified_at) AS modified_at,
    ANY_VALUE(title) AS art_show_title
FROM project_data
GROUP BY art_show_id_final

