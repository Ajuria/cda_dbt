{{ config(materialized='view') }}

SELECT
  i.interaction_id,
  i.session_id,
  s.user_id,
  i.interaction_type,
  i.timestamp AS interacted_at,
  i.source_url AS url,
  s.device,
  s.geo_ip,
  s.geo_name,
  CURRENT_TIMESTAMP() AS ingested_at

FROM {{ ref('stg__interaction') }} AS i
LEFT JOIN {{ ref('stg__session') }} AS s
  ON i.session_id = s.session_id

