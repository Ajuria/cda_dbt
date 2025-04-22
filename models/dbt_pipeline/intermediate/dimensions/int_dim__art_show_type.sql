-- models/dbt_pipeline/intermediate/dimensions/int_dim__user_type.sql

{{ config(materialized='view') }}

SELECT
    art_show_type_id,
    art_show_type_label,
    art_show_type_description
FROM {{ ref('int_dim__art_show_type') }}
