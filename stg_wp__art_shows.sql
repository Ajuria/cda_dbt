SELECT
    ID                          AS art_show_id,
    post_title                  AS art_show_title,
    post_name                   AS art_show_slug,
    post_type,
    post_status,
    post_date,
    post_modified,
    post_parent,
    post_author                 AS user_id
FROM {{ source('wordpress', 'posts') }}
WHERE post_type = 'art_show'
