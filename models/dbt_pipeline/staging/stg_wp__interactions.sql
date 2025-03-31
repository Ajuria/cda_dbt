SELECT
    ID                    AS interaction_id,
    post_title            AS interaction_label,
    post_type             AS interaction_type,
    post_status           AS status,
    post_date             AS published_at,
    post_modified         AS modified_at,
    post_author           AS user_id
FROM {{ source('wordpress', 'posts') }}
WHERE post_type IN ('event', 'product', 'service')
