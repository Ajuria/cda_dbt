{{ config(materialized='view') }}

WITH helloasso_logins AS (
    SELECT
        SUBSTR(TO_HEX(SHA256(LOWER(JSON_VALUE(payer, '$.email')))), 1, 16) AS user_id,
        'helloasso'                                                       AS login_platform,
        JSON_VALUE(payer, '$.email')                                      AS email,
        'cda_api.payments'                                                AS source_table,
        JSON_VALUE(payer, '$.firstName')                                  AS first_name,
        JSON_VALUE(payer, '$.lastName')                                   AS last_name,
        JSON_VALUE(payer, '$.country')                                    AS country,
        FORMAT_TIMESTAMP('%F %H:%M', CURRENT_TIMESTAMP())                 AS ingested_at
    FROM {{ source('cda_api', 'payments') }}
    WHERE JSON_VALUE(payer, '$.email') IS NOT NULL
),

wordpress_logins AS (
    SELECT
        SUBSTR(TO_HEX(SHA256(CAST(user_id AS STRING))), 1, 16)            AS user_id,
        'wordpress'                                                       AS login_platform,
        email                                                             AS email,
        'cda_owned.user'                                                  AS source_table,
        CAST(NULL AS STRING)                                              AS first_name,
        CAST(NULL AS STRING)                                              AS last_name,
        CAST(NULL AS STRING)                                              AS country,
        FORMAT_TIMESTAMP('%F %H:%M', CURRENT_TIMESTAMP())                 AS ingested_at
    FROM {{ source('cda_owned', 'user') }}
    WHERE email IS NOT NULL
),

instagram_logins AS (
    SELECT
        SUBSTR(TO_HEX(SHA256(CAST(user_id AS STRING))), 1, 16)            AS user_id,
        'instagram'                                                       AS login_platform,
        CAST(NULL AS STRING)                                              AS email,
        'cda_owned.interaction'                                           AS source_table,
        CAST(NULL AS STRING)                                              AS first_name,
        CAST(NULL AS STRING)                                              AS last_name,
        CAST(NULL AS STRING)                                              AS country,
        FORMAT_TIMESTAMP('%F %H:%M', CURRENT_TIMESTAMP())                 AS ingested_at
    FROM {{ source('cda_owned', 'interaction') }}
    WHERE CAST(source_platform AS STRING) = 'instagram'
      AND user_id IS NOT NULL
),

facebook_logins AS (
    SELECT
        SUBSTR(TO_HEX(SHA256(CAST(user_id AS STRING))), 1, 16)            AS user_id,
        'facebook'                                                        AS login_platform,
        CAST(NULL AS STRING)                                              AS email,
        'cda_owned.interaction'                                           AS source_table,
        CAST(NULL AS STRING)                                              AS first_name,
        CAST(NULL AS STRING)                                              AS last_name,
        CAST(NULL AS STRING)                                              AS country,
        FORMAT_TIMESTAMP('%F %H:%M', CURRENT_TIMESTAMP())                 AS ingested_at
    FROM {{ source('cda_owned', 'interaction') }}
    WHERE CAST(source_platform AS STRING) = 'facebook'
      AND user_id IS NOT NULL
)

SELECT * FROM helloasso_logins
UNION ALL
SELECT * FROM wordpress_logins
UNION ALL
SELECT * FROM instagram_logins
UNION ALL
SELECT * FROM facebook_logins
