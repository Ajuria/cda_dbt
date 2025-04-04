{{ config(materialized='view') }}

SELECT
    user_type_id,
    user_type_label,
    user_type_description
FROM {{ ref('dim_user_type') }}
