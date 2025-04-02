-- models/dbt_pipeline/intermediate/cda_owned/int_qr_code__with_context.sql

{{ config(materialized='view') }}

SELECT
    q.qr_id AS qr_id,
    SAFE_CAST(q.scan_timestamp AS TIMESTAMP) AS scan_timestamp,
    q.source_platform AS source_platform,
    q.session_id AS session_id,
    q.brand_id AS brand_id,
    q.art_show_id AS art_show_id,
    q.target_type AS target_type,
    q.target_id AS target_id
FROM {{ ref('stg__cda_owned__qr_code') }} AS q
WHERE q.qr_id IS NOT NULL
