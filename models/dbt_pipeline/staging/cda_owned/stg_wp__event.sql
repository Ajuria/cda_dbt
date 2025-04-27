{{ config(materialized='view') }}

SELECT
  event_id,
  event_name,
  event_type,

  FORMAT_TIMESTAMP('%F %T', event_start) AS event_start,
  FORMAT_TIMESTAMP('%F %T', event_end) AS event_end,
  FORMAT_TIMESTAMP('%F %T', updated_at) AS updated_at

FROM {{ source('cda_owned', 'event') }}

