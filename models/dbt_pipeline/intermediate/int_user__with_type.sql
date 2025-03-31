SELECT
    u.user_id,
    u.user_type             AS raw_user_type,
    u.country,
    u.first_name,
    u.last_name,
    u.email,
    u.created_at,
    u.updated_at,
    u.user_type_id,

    ut.user_type_label      AS user_type_label

FROM {{ ref('stg_wp__users') }} u
LEFT JOIN {{ ref('stg_cda__ref_user_type') }} ut
    ON u.user_type_id = ut.user_type_id
