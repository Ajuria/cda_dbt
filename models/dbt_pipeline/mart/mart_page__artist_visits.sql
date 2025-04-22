{{ config(materialized='table') }}

SELECT
  REGEXP_EXTRACT(session_source_url, r'https?://costieresdelart\.fr/([^/?#]+)') AS artiste_slug,
  COUNT(DISTINCT session_id) AS page_visits
FROM {{ ref('stg__session') }}
WHERE session_source_url LIKE '%costieresdelart.fr/%'
GROUP BY artiste_slug
ORDER BY page_visits DESC

