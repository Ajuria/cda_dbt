-- File: models/intermediate/cda_api/int_fact_ig__user_lifetime_metrics.sql
{{ config(materialized='view') }}

WITH raw_metrics AS (
  SELECT
    metric                                AS metric,
    breakdown                             AS breakdown,
    SAFE_CAST(JSON_VALUE(value, '$.value') AS INT64)
                                         AS metric_value,
    FORMAT_TIMESTAMP('%F %H:%M', CURRENT_TIMESTAMP())
                                         AS formatted_timestamp,
    business_account_id                   AS business_account_id
  FROM {{ ref('stg_ig__user_lifetime_insights') }}
  WHERE value IS NOT NULL
),

deduplicated AS (
  SELECT *
  FROM (
    SELECT *,
           ROW_NUMBER() OVER (
             PARTITION BY metric, breakdown, business_account_id
             ORDER BY formatted_timestamp DESC
           )                             AS row_num
    FROM raw_metrics
  )
  WHERE row_num = 1
)

SELECT
  metric                                  AS metric,
  breakdown                               AS breakdown,
  metric_value                            AS metric_value,
  formatted_timestamp                     AS date,
  business_account_id                     AS business_account_id
FROM deduplicated
