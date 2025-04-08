-- models/dbt_pipeline/intermediate/cda_owned/int_social_interaction__normalized.sql

{{ config(materialized='view') }}

WITH instagram AS (
    SELECT
        i.interaction_id                    AS interaction_id,
        i.user_id                           AS user_id,
        'instagram'                         AS platform,
        i.action_type                       AS action_type,
        SAFE_CAST(i.timestamp AS TIMESTAMP) AS timestamp,
        i.target_id                         AS target_id,
        i.brand_id                          AS brand_id,
        i.session_id                        AS session_id
    FROM {{ ref('stg__cda_owned__instagram_interaction') }} AS i
),

facebook AS (
    SELECT
        f.interaction_id AS interaction_id,
        f.user_id AS user_id,
        'facebook' AS platform,
        f.action_type AS action_type,
        SAFE_CAST(f.timestamp AS TIMESTAMP) AS timestamp,
        f.target_id AS target_id,
        f.brand_id AS brand_id,
        f.session_id AS session_id
    FROM {{ ref('stg__cda_owned__facebook_interaction') }} AS f
)

SELECT * FROM instagram
UNION ALL
SELECT * FROM facebook
