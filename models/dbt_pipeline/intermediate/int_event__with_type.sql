SELECT
    e.event_id,
    e.event_name,
    e.event_type_id,
    e.event_start,
    e.event_end,
    e.updated_at,
    et.event_type_label AS event_type_label

FROM {{ ref('stg_cda_owned__event') }} e
LEFT JOIN {{ ref('int_dim_event_type') }} et
    ON e.event_type_id = et.event_type_id

