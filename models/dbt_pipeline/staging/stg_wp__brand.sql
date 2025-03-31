SELECT
    ID                          AS brand_id,
    post_title                  AS brand_name,
    post_name                   AS brand_slug,
    post_type,
    post_status,
    post_date,
    post_modified,
    post_parent,
    post_author                 AS user_id
FROM {{ source('wordpress', 'posts') }}
WHERE post_type = 'brand'
