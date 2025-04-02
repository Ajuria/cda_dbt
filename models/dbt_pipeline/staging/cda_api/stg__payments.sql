-- models/dbt_pipeline/staging/stg_cda_api__helloasso_payment.sql

{{ config(materialized='view') }}

SELECT
    id                              AS payment_id,
    SAFE_CAST(amount AS FLOAT64)    AS amount,
    state                           AS payment_status,
    payer                           AS payer_json,
    date                            AS payment_date,
    CURRENT_TIMESTAMP()             AS created_at
FROM `cda-database`.`cda_api`.`payments`


