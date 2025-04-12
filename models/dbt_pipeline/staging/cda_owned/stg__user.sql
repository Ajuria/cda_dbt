{{ config(materialized='view') }}

SELECT
    user_id,
    user_type AS user_type_id,
    first_name,
    last_name,
    email,
    country,
    FORMAT_TIMESTAMP('%F %T', created_at)           AS created_at,
    FORMAT_TIMESTAMP('%F %T', updated_at)           AS updated_at,
    FORMAT_TIMESTAMP('%F %T', CURRENT_TIMESTAMP)    AS ingested_at,
FROM {{ source('cda_owned', 'user') }}




