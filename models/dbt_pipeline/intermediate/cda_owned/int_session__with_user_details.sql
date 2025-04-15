{{ config(materialized='view') }}

SELECT
    session_id                                  AS session_id_raw,
    SUBSTR(TO_HEX(SHA256(session_id)), 1, 16)   AS session_id,
    s.session_source_url,
    FORMAT_TIMESTAMP('%F %T', CAST(s.started_at AS TIMESTAMP))   AS started_at,
    FORMAT_TIMESTAMP('%F %T', CAST(s.ended_at AS TIMESTAMP))     AS ended_at,

    COALESCE(u.payer_first_name, su.first_name) AS payer_first_name,
    COALESCE(u.payer_last_name, su.last_name)   AS payer_last_name,
    COALESCE(u.payer_email, su.email)           AS payer_email,
    COALESCE(u.payer_country, su.country)       AS payer_country,
    COALESCE(u.device, s.device)                AS device,
    COALESCE(u.geo_ip, s.geo_ip)                AS geo_ip,
    COALESCE(u.geo_name, s.geo_name)            AS geo_name,

    u.ingested_at                                AS user_ingested_at

FROM {{ ref('stg__session') }} AS s
LEFT JOIN {{ ref('int_user__consolidated') }} AS u
  ON s.user_id = u.user_id
LEFT JOIN {{ ref('stg__user') }} AS su
  ON s.user_id = su.user_id

