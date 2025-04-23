-- models/dbt_pipeline/intermediate/cda_api/int_fact__user.sql
{{ config(materialized='view') }}

WITH wordpress_logins AS (
    SELECT
        SUBSTR(TO_HEX(SHA256(CAST(user_id AS STRING))), 1, 16)       AS user_id,
        'wordpress'                                                  AS login_platform,
        CAST(email AS STRING)                                        AS email,
        source_table,
        CAST(NULL AS STRING)                                         AS first_name,
        CAST(NULL AS STRING)                                         AS last_name,
        CAST(NULL AS STRING)                                         AS country,
        CAST(NULL AS STRING)                                         AS ingested_at
    FROM {{ ref('stg__login') }}
    WHERE login_platform = 'wordpress'
      AND email IS NOT NULL
),

instagram_logins AS (
    SELECT
        SUBSTR(TO_HEX(SHA256(CAST(user_id AS STRING))), 1, 16)       AS user_id,
        'instagram'                                                  AS login_platform,
        CAST(email AS STRING)                                        AS email,
        source_table,
        CAST(NULL AS STRING)                                         AS first_name,
        CAST(NULL AS STRING)                                         AS last_name,
        CAST(NULL AS STRING)                                         AS country,
        CAST(NULL AS STRING)                                         AS ingested_at
    FROM {{ ref('stg__login') }}
    WHERE login_platform = 'instagram'
      AND user_id IS NOT NULL
),

facebook_logins AS (
    SELECT
        SUBSTR(TO_HEX(SHA256(CAST(user_id AS STRING))), 1, 16)       AS user_id,
        'facebook'                                                   AS login_platform,
        CAST(email AS STRING)                                        AS email,
        source_table,
        CAST(NULL AS STRING)                                         AS first_name,
        CAST(NULL AS STRING)                                         AS last_name,
        CAST(NULL AS STRING)                                         AS country,
        CAST(NULL AS STRING)                                         AS ingested_at
    FROM {{ ref('stg__login') }}
    WHERE login_platform = 'facebook'
      AND user_id IS NOT NULL
),

helloasso_logins AS (
    SELECT
        SUBSTR(TO_HEX(SHA256(LOWER(payer_email))), 1, 16)            AS user_id,
        'helloasso'                                                  AS login_platform,
        payer_email                                                  AS email,
        'cda_api.payments'                                           AS source_table,
        payer_first_name                                             AS first_name,
        payer_last_name                                              AS last_name,
        payer_country                                                AS country,
        FORMAT_TIMESTAMP('%F %H:%M', CAST(ingested_at AS TIMESTAMP)) AS ingested_at
    FROM {{ ref('stg_ha__payments') }}
    WHERE payer_email IS NOT NULL
),

anonymous_users AS (
    SELECT
        SUBSTR(TO_HEX(SHA256(CAST(session_id AS STRING))), 1, 16)    AS user_id,
        'anonymous'                                                  AS login_platform,
        CAST(NULL AS STRING)                                         AS email,
        'cda_owned.interaction'                                      AS source_table,
        CAST(NULL AS STRING)                                         AS first_name,
        CAST(NULL AS STRING)                                         AS last_name,
        CAST(NULL AS STRING)                                         AS country,
        CAST(NULL AS STRING)                                         AS ingested_at
    FROM {{ ref('stg__interaction') }}
    WHERE session_id IS NOT NULL
)

, combined_users AS (
    SELECT * FROM wordpress_logins
    UNION ALL
    SELECT * FROM instagram_logins
    UNION ALL
    SELECT * FROM facebook_logins
    UNION ALL
    SELECT * FROM helloasso_logins
    UNION ALL
    SELECT * FROM anonymous_users
)

, deduplicated_users AS (
    SELECT
        user_id,
        ANY_VALUE(login_platform)     AS login_platform,
        MAX(email)                    AS email,
        MAX(source_table)             AS source_table,
        MAX(first_name)               AS first_name,
        MAX(last_name)                AS last_name,
        MAX(country)                  AS country,
        MAX(ingested_at)              AS ingested_at
    FROM combined_users
    GROUP BY user_id
)

SELECT
    *,
    CASE
        WHEN user_id = 'a6bf0cf2f9f8bfc6' THEN 'female'
        WHEN user_id = 'cf7edb387269eaf8' THEN 'female'
        WHEN user_id = 'da79fbd035ba2d1e' THEN 'male'
        WHEN user_id = '65a7b0cb29b7c125' THEN 'female'
        WHEN user_id = 'c6126e4a3978a7f5' THEN 'female'
        WHEN user_id = '405470dcdde90768' THEN 'male'
        WHEN user_id = '538d60f22a6bc383' THEN 'male'
        WHEN user_id = '16198b2c7b3469ca' THEN 'male'
        WHEN user_id = '17178e187ca13147' THEN 'female'
        WHEN user_id = '0a25a4d415b0c380' THEN 'female'
        WHEN user_id = '4eee8cdd6f682bc7' THEN 'male'
        WHEN user_id = '76ac8d475b13b067' THEN 'male'
        WHEN user_id = '40c59ed814c6d776' THEN 'female'
        WHEN user_id = 'c13882c7ea2f3fb2' THEN 'male'
        WHEN user_id = '057d3f2ecc20365f' THEN 'male'
        WHEN user_id = '1240dcdef87ba364' THEN 'female'
        ELSE NULL
    END AS gender
FROM deduplicated_users
