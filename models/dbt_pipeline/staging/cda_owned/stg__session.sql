SELECT
    session_id,
    source_platform,
    session_source_url,
    user_id,
    brand_id,
    device,
    geo_ip,
    geo_name,
    started_at,
    ended_at
FROM `cda-database.cda_owned.session`
