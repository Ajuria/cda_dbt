SELECT
    ID                 AS user_id,
    user_login         AS login,
    user_email         AS email,
    display_name       AS display_name,
    user_registered    AS created_at
FROM {{ source('wordpress', 'users') }}
