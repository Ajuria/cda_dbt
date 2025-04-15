{{ config(materialized='view') }}

SELECT
    i.interaction_id                                AS interaction_id_raw,
    SUBSTR(TO_HEX(SHA256(i.interaction_id)), 1, 16) AS interaction_id,
    i.session_id                                    AS session_id_raw,
    SUBSTR(TO_HEX(SHA256(i.session_id)), 1, 16)     AS session_id,
    s.user_id,
    i.timestamp                                     AS interacted_at,
    i.cda_website_source                            AS url,
    s.device,
    s.geo_ip,
    s.geo_name,
    CURRENT_TIMESTAMP()                             AS ingested_at

FROM {{ ref('stg__interaction') }}  AS i
LEFT JOIN {{ ref('stg__session') }} AS s
    ON i.session_id = s.session_id

