-- models/dbt_pipeline/mart/mart_fact__payment.sql
{{ config(materialized='table') }}

WITH base AS (
  SELECT
    user_id,
    payer_country,
    amount,
    payer_gender,
    FORMAT_TIMESTAMP('%G-W%V', PARSE_TIMESTAMP('%F %H:%M', payment_date)) AS iso_week
  FROM {{ ref('int_fact__payment') }}
),

-- Revenue per country
revenue_per_country AS (
  SELECT
    payer_country,
    SAFE_CAST(SUM(amount) AS INT64) AS total_revenue_country
  FROM base
  GROUP BY payer_country
),

-- Revenue per gender
revenue_per_gender AS (
  SELECT
    payer_gender,
    COUNT(*) AS number_of_payments,
    SAFE_CAST(SUM(amount) AS INT64) AS total_revenue_gender,
    SAFE_CAST(MIN(amount) AS INT64) AS min_spend_gender,
    SAFE_CAST(MAX(amount) AS INT64) AS max_spend_gender,
    SAFE_CAST(AVG(amount) AS INT64) AS avg_spend_gender
  FROM base
  GROUP BY payer_gender
),

-- Weekly revenue stats
revenue_per_week AS (
  SELECT
    iso_week,
    SAFE_CAST(SUM(amount) AS INT64) AS total_revenue_week,
    SAFE_CAST(MIN(amount) AS INT64) AS min_spend_week,
    SAFE_CAST(MAX(amount) AS INT64) AS max_spend_week,
    SAFE_CAST(AVG(amount) AS INT64) AS avg_spend_week
  FROM base
  GROUP BY iso_week
),

-- Global stats
global_stats AS (
  SELECT
    SAFE_CAST(MIN(amount) AS INT64) AS global_min_spend,
    SAFE_CAST(MAX(amount) AS INT64) AS global_max_spend,
    SAFE_CAST(AVG(amount) AS INT64) AS global_avg_spend
  FROM base
)

-- Final output
SELECT
  gs.global_min_spend,
  gs.global_max_spend,
  gs.global_avg_spend,
  c.payer_country,
  c.total_revenue_country,
  g.payer_gender,
  g.total_revenue_gender,
  g.number_of_payments,
  g.min_spend_gender,
  g.max_spend_gender,
  g.avg_spend_gender,
  w.iso_week,
  w.total_revenue_week,
  w.min_spend_week,
  w.max_spend_week,
  w.avg_spend_week
FROM global_stats gs
CROSS JOIN revenue_per_country c
CROSS JOIN revenue_per_gender g
CROSS JOIN revenue_per_week w
