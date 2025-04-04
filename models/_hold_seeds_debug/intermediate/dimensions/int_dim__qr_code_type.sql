{{ config(materialized='view') }}

SELECT
    qr_code_type_id,
    qr_code_type_label,
    qr_code_type_description
FROM {{ ref('dim_qr_code_type') }}
