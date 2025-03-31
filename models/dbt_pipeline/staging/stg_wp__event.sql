SELECT
    ID                          AS event_id,
    post_title                  AS event_title,
    post_name                   AS event_slug,
    post_type,
    post_status,
    post_date,
    post_modified,
    post_parent,
    post_author                 AS user_id
FROM {{ source('wordpress', 'posts') }}
WHERE post_type = 'event'
