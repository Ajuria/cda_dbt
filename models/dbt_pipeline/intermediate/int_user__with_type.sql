WITH base_user AS (
    SELECT
        ID AS user_id,                       -- Correct user_id field
        first_name || ' ' || last_name AS user_name, -- Concatenate first and last name
        user_type_id,                         -- Adjust according to actual column names
        user_email AS email,                  -- Correct field for email
        user_registered AS created_at         -- Correct field for created_at
    FROM {{ source('wordpress', 'wp_users') }}        -- Use source instead of ref
)

SELECT
    u.*,
    ut.user_type_label
FROM base_user u
LEFT JOIN {{ ref('int_dim_user_type') }} ut
    ON u.user_type_id = ut.user_type_id
