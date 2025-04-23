-- File: models/intermediate/cda_api/int_fact_ig__user_daily_metrics.sql
{{ config(materialized='view') }}

WITH daily_metrics AS (
  SELECT
    date                                  AS insight_date,
    page_id                               AS page_id,
    reach                                 AS reach,
    reach_week                            AS reach_week,
    reach_days_28                         AS reach_days_28,
    follower_count                        AS follower_count,
    online_followers_value                AS online_followers_value,
    business_account_id                   AS business_account_id,
    ROW_NUMBER() OVER (
      PARTITION BY page_id, date
      ORDER BY date DESC
    ) AS row_num
  FROM {{ ref('stg_ig__user_insights') }}
)

SELECT
  insight_date,
  page_id,
  reach,
  reach_week,
  reach_days_28,
  follower_count,
  online_followers_value,
  business_account_id
FROM daily_metrics
WHERE row_num = 1
