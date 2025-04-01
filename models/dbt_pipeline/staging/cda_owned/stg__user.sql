-- models/dbt_pipeline/staging/stg_cda_owned__user.sql

SELECT
    user_id,
    user_type,
    user_type_id,
    first_name,
    last_name,
    email,
    country,
    created_at,
    updated_at
FROM {{ source('wordpress', 'user') }}
