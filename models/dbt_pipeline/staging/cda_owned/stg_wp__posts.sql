SELECT
    ID              AS user_id,
    user_login,
    user_email,
    user_registered
FROM
    {{ source('wordpress', 'wp_users') }}
