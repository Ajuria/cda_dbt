-- models/dbt_pipeline/intermediate/cda_owned/int_interaction__with_user_and_type.sql

{{ config(materialized='view') }}

WITH base AS (
    SELECT
        i.interaction_id AS interaction_id,
        SAFE_CAST(i.interaction_type AS INTEGER) AS interaction_type,
        i.user_id AS user_id,
        u.user_type_id AS user_type_id,
        SAFE_CAST(i.timestamp AS TIMESTAMP) AS published_at,
        i.target_id AS linked_event_id,
        SAFE_CAST(NULL AS STRING) AS brand_id  -- Placeholder if no brand logic yet
    FROM {{ ref('stg__cda_owned__interaction') }} AS i
    LEFT JOIN {{ ref('stg__cda_owned__user') }} AS u
        ON i.user_id = u.user_id
)

SELECT * FROM base
