-- Location: dbt Cloud / GitHub
-- File: models/dbt_pipeline/staging/stg_wp__posts.sql

SELECT
    ID AS post_id,
    post_title,
    post_content,
    post_status,
    post_date,
    post_type,
    post_name AS slug
FROM `cda-database`.`dbt_jdeajuriaguerra_cda_pipeline`.`raw_wp__posts`
WHERE post_type = 'art_show';
