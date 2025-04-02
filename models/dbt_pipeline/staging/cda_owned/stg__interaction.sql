{{ config(materialized='view') }}

SELECT
  interaction_id,
  interaction_type,
  source_platform,
  source_url,
  session_id,
  target_id,
  user_id,
  social_media_action,
  timestamp,
  details
FROM `cda-database`.`cda_owned`.`interaction`

