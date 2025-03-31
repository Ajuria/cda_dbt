WITH base_user AS (
    SELECT
        user_id,
        user_name,
        user_type_id,
        email,
        created_at
    FROM {{ ref('stg_wp__users') }}
)

SELECT
    u.*,
    ut.user_type_label
FROM base_user u
LEFT JOIN {{ ref('int_dim_user_type') }} ut
    ON u.user_type_id = ut.user_type_id

