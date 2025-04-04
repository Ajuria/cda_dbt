-- models/dbt_pipeline/staging/cda_owned/stg_wp__posts.sql

{{ config(materialized='view') }}

SELECT
    id     AS post_id,
    title  AS post_title,
    slug   AS post_slug,
    type   AS post_type,
    date   AS post_date
FROM `cda-database`.`cda_owned`.`wp_posts`
WHERE type IS NOT NULL
