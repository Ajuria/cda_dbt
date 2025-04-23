-- File: models/intermediate/cda_api/int_fact_ig__account.sql
{{ config(materialized='view') }}

WITH deduplicated AS (
  SELECT
    id                           AS account_id,
    name                         AS name,
    ig_id                        AS ig_id,
    page_id                      AS page_id,
    website                      AS website,
    username                     AS username,
    biography                    AS biography,
    media_count                  AS media_count,
    follows_count                AS follows_count,
    followers_count              AS followers_count,
    profile_picture_url          AS profile_picture_url,
    FORMAT_TIMESTAMP('%F %H:%M', CURRENT_TIMESTAMP())
                                 AS inserted_at,
    ROW_NUMBER() OVER (
      PARTITION BY id
      ORDER BY CURRENT_TIMESTAMP() DESC
    )                            AS row_num
  FROM {{ ref('stg_ig__users') }}
)

SELECT
  account_id                    AS account_id,
  name                          AS name,
  ig_id                         AS ig_id,
  page_id                       AS page_id,
  website                       AS website,
  username                      AS username,
  biography                     AS biography,
  media_count                   AS media_count,
  follows_count                 AS follows_count,
  followers_count               AS followers_count,
  profile_picture_url           AS profile_picture_url,
  inserted_at                   AS inserted_at
FROM deduplicated
WHERE row_num = 1