-- models/dbt_pipeline/intermediate/dimensions/int_dim__user_type.sql

{{ config(materialized='view') }}

SELECT
    user_type_id,
    user_type_label,
    user_type_description
FROM {{ ref('dim_user_type') }}

