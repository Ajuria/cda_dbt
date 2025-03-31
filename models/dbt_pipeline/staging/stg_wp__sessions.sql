SELECT
    ID                          AS session_id,
    post_title                  AS session_label,
    post_name                   AS session_slug,
    post_type,
    post_status,
    post_date,
    post_modified,
    post_parent,
    post_author                 AS user_id
FROM {{ source('wordpress', 'posts') }}
WHERE post_type = 'session'
