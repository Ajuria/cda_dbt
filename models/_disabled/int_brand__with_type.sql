SELECT
    b.brand_id,
    b.brand_name,
    b.brand_type_id,
    b.updated_at,
    bt.brand_type_label AS brand_type_label

FROM {{ ref('stg_wp__brand') }} b
LEFT JOIN {{ ref('int_dim_brand_type') }} bt
    ON b.brand_type_id = bt.brand_type_id
