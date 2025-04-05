-- models/dbt_pipeline/staging/cda_owned/stg__wp_pages.sql
{{ config(materialized='view') }}

SELECT
  id,
  title,
  slug,
  content,
  date,
  type
FROM `cda-database`.`cda_owned`.`wp_pages`
