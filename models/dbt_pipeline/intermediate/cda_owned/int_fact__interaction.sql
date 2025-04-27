{{ config(materialized='view') }}

WITH base_interaction AS (

    SELECT
        -- IDs
        SUBSTR(TO_HEX(SHA256(CAST(interaction_id AS STRING))), 1, 16) AS interaction_id,
        SUBSTR(TO_HEX(SHA256(CAST(session_id AS STRING))), 1, 16) AS session_id,

        -- Mapping source platform
        CASE CAST(source_platform AS INT64)
            WHEN 1 THEN 'cda_website'
            WHEN 2 THEN 'third_party_website'
            WHEN 3 THEN 'instagram'
            WHEN 4 THEN 'facebook'
            WHEN 5 THEN 'helloasso'
            WHEN 6 THEN 'qr_code'
            ELSE 'unknown'
        END AS source_platform,

        -- Mapping target pages
        CASE CAST(target_id AS INT64)
            WHEN 1 THEN 'cda_website_programme'
            WHEN 2 THEN 'cda_website_artistes'
            WHEN 3 THEN 'cda_website_event'
            WHEN 4 THEN 'cda_website_a_propos'
            WHEN 5 THEN 'cda_website_actualites'
            WHEN 6 THEN 'cda_website_reserver'
            ELSE 'unknown'
        END AS target_mapping,

        -- URL cleaned from domain and prefixes
        cda_website_source,

        -- Hashed user_id (user_id if available, else session_id)
        SUBSTR(TO_HEX(SHA256(CAST(COALESCE(user_id, session_id) AS STRING))), 1, 16) AS user_id,

        -- Other interaction fields
        CAST(social_media_action AS STRING) AS social_media_action,
        interaction_label,

        -- Timestamp formatted without seconds
        FORMAT_TIMESTAMP('%F %H:%M', CAST(timestamp AS TIMESTAMP)) AS event_timestamp

    FROM {{ ref('stg__interaction') }}
),

base_qr_code_mapping AS (

    SELECT
        display_post_slug,
        source_id
    FROM {{ ref('int_qr_code__unified') }}
)

SELECT
    bi.interaction_id,
    bi.session_id,
    bi.user_id,
    bi.source_platform,
    bi.target_mapping,
    bi.cda_website_source,
    bi.social_media_action,
    bi.interaction_label,
    bi.event_timestamp,

    -- New field: qr_code_id
    bqcm.source_id AS qr_code_id

FROM base_interaction AS bi

LEFT JOIN base_qr_code_mapping AS bqcm
    ON LOWER(TRIM(bi.cda_website_source, '/')) = LOWER(TRIM(bqcm.display_post_slug, '/'))

WHERE
    bi.session_id IN (SELECT session_id FROM {{ ref('int_fact__session') }})
