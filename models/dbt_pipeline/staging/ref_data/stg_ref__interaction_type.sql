-- models/dbt_pipeline/staging/stg_cda_owned__ref_interaction_type.sql

SELECT
  interaction_type_id,
  interaction_type_name,
  interaction_type_label_fr
FROM `cda-database`.`cda_owned`.`ref_interaction_type`
