{{ config(materialized='view') }}

SELECT
  id                              AS payment_id,
  SAFE_CAST(amount AS FLOAT64) / 100.0 AS amount,
  state                           AS payment_status,
  payer                           AS raw_payer_json,

  -- Normalize and format date string (HelloAsso uses RFC3339 with microseconds and offset)
  FORMAT_TIMESTAMP(
    '%F %T',
    PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E6S', 
      SUBSTR(REPLACE(date, '+02:00', ''), 1, 26)
    )
  ) AS payment_date,

  JSON_VALUE(payer, '$.email')       AS payer_email,
  JSON_VALUE(payer, '$.firstName')   AS payer_first_name,
  JSON_VALUE(payer, '$.lastName')    AS payer_last_name,
  JSON_VALUE(payer, '$.country')     AS payer_country,

  FORMAT_TIMESTAMP('%F %T', CURRENT_TIMESTAMP()) AS ingested_at

FROM `cda-database`.`cda_api`.`payments`

