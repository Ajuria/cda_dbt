{{ config(materialized='view') }}

WITH base_logins AS (

    SELECT
        user_id,
        login_platform,
        email,
        source_table,
        first_name,
        last_name,
        country,
        ingested_at
    FROM {{ ref('stg__login') }}

    UNION ALL

    SELECT
        SUBSTR(TO_HEX(SHA256(CAST(session_id AS STRING))), 1, 16) AS user_id,
        'anonymous'                                               AS login_platform,
        NULL                                                      AS email,
        'cda_owned.interaction'                                   AS source_table,
        NULL                                                      AS first_name,
        NULL                                                      AS last_name,
        NULL                                                      AS country,
        FORMAT_TIMESTAMP('%F %H:%M', CURRENT_TIMESTAMP())         AS ingested_at
    FROM {{ ref('stg__interaction') }}
    WHERE session_id IS NOT NULL

),

anonymous_sessions AS (

    SELECT
        user_id,
        'anonymous' AS login_platform,
        CAST(NULL AS STRING) AS email,
        'cda_owned.session' AS source_table,
        CAST(NULL AS STRING) AS first_name,
        CAST(NULL AS STRING) AS last_name,
        CAST(NULL AS STRING) AS country,
        FORMAT_TIMESTAMP('%F %H:%M', CURRENT_TIMESTAMP()) AS ingested_at
    FROM {{ ref('int_fact__session') }}
    WHERE user_id IS NOT NULL
      AND user_id NOT IN (SELECT user_id FROM base_logins)

),

unioned_users AS (

    SELECT * FROM base_logins
    UNION ALL
    SELECT * FROM anonymous_sessions

),

deduplicated_users AS (

    SELECT
        user_id,
        ANY_VALUE(login_platform)   AS login_platform,
        MAX(email)                  AS email,
        MAX(source_table)           AS source_table,
        MAX(first_name)             AS first_name,
        MAX(last_name)              AS last_name,
        MAX(country)                AS country,
        MAX(ingested_at)            AS ingested_at
    FROM unioned_users
    GROUP BY user_id

),

enriched_users AS (

    SELECT
        du.user_id,
        du.login_platform,
        du.email,
        du.source_table,
        du.first_name,
        du.last_name,
        du.country,
        su.user_type_id,
        su.created_at,
        su.updated_at,
        du.ingested_at
    FROM deduplicated_users du
    LEFT JOIN {{ ref('stg__user') }} su
      ON du.user_id = su.user_id

)

SELECT
    user_id,
    login_platform,
    email,
    source_table,
    first_name,
    last_name,
    country,
    user_type_id,
    created_at,
    updated_at,
    ingested_at,

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

FROM enriched_users