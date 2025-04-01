-- models/dbt_pipeline/staging/stg_cda_owned__art_show.sql

SELECT
  id            AS art_show_id,
  title         AS art_show_title,
  slug          AS art_show_slug,
  type          AS post_type,
  date          AS post_date
FROM `cda-database`.`cda_owned`.`wp_posts`
WHERE type = 'art_show'

