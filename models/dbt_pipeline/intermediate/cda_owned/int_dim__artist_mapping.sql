{{ config(materialized='view') }}

WITH raw AS (

    -- Only include shared columns across all project sources
    SELECT id FROM {{ ref('stg_wp__project') }}
    UNION ALL
    SELECT id FROM {{ ref('stg_wp__project_page2') }}
    UNION ALL
    SELECT id FROM {{ ref('stg_wp__project_page3') }}
    UNION ALL
    SELECT id FROM {{ ref('stg_wp__project_page4') }}

),

tagged AS (

    SELECT
        CAST(id AS STRING) AS raw_id,

        -- Manual artist tagging
        CASE
            WHEN CAST(id AS STRING) = '93'  THEN 'a93'
            WHEN CAST(id AS STRING) = '205' THEN 'a205'
            WHEN CAST(id AS STRING) = '354' THEN 'a354'
            WHEN CAST(id AS STRING) = '357' THEN 'a357'
            WHEN CAST(id AS STRING) = '358' THEN 'a358'
            WHEN CAST(id AS STRING) = '359' THEN 'a359'
            WHEN CAST(id AS STRING) = '360' THEN 'a360'
            WHEN CAST(id AS STRING) = '362' THEN 'a362'
            WHEN CAST(id AS STRING) = '364' THEN 'a364'
            WHEN CAST(id AS STRING) = '368' THEN 'a368'
            WHEN CAST(id AS STRING) = '370' THEN 'a370'
            WHEN CAST(id AS STRING) = '371' THEN 'a371'
            WHEN CAST(id AS STRING) = '375' THEN 'a375'
            WHEN CAST(id AS STRING) = '379' THEN 'a379'
            WHEN CAST(id AS STRING) = '383' THEN 'a383'
            WHEN CAST(id AS STRING) = '388' THEN 'a388'
            WHEN CAST(id AS STRING) = '392' THEN 'a392'
            WHEN CAST(id AS STRING) = '396' THEN 'a396'
            WHEN CAST(id AS STRING) = '455' THEN 'a455'
            WHEN CAST(id AS STRING) = '459' THEN 'a459'
            WHEN CAST(id AS STRING) = '460' THEN 'a460'
            WHEN CAST(id AS STRING) = '462' THEN 'a462'
            WHEN CAST(id AS STRING) = '464' THEN 'a464'
            WHEN CAST(id AS STRING) = '470' THEN 'a470'
            ELSE NULL
        END AS artist_id

    FROM raw
),

mapped AS (

    SELECT
        artist_id,

        CASE
            WHEN artist_id IN ('a93', 'a379', 'a383', 'a455') THEN 'b407'
            WHEN artist_id = 'a388' THEN 'b410'
            WHEN artist_id = 'a392' THEN 'b412'
            WHEN artist_id IN ('a354', 'a358', 'a464', 'a360', 'a362', 'a370', 'a357', 'a470') THEN 'b415'
            WHEN artist_id IN ('a375', 'a379', 'a93') THEN 'b416'
            WHEN artist_id IN ('a364', 'a371', 'a368') THEN 'b417'
            WHEN artist_id = 'a459' THEN 'b478'
            ELSE NULL
        END AS brand_id,

        CASE
            WHEN artist_id IN ('a93', 'a379', 'a383', 'a455') THEN 'as407'
            WHEN artist_id = 'a388' THEN 'as410'
            WHEN artist_id = 'a392' THEN 'as412'
            WHEN artist_id IN ('a354', 'a358', 'a464', 'a360', 'a362', 'a370', 'a357', 'a470') THEN 'as415'
            WHEN artist_id IN ('a375', 'a379', 'a93') THEN 'as416'
            WHEN artist_id IN ('a364', 'a371', 'a368') THEN 'as417'
            WHEN artist_id = 'a459' THEN 'as478'
            ELSE NULL
        END AS art_show_id,

        CASE
            WHEN artist_id IN ('a93', 'a379', 'a383', 'a455') THEN 'e435'
            WHEN artist_id IN ('a364', 'a371', 'a368') THEN 'e441'
            WHEN artist_id IN ('a93', 'a375', 'a379') THEN 'e442'
            WHEN artist_id IN ('a354', 'a357', 'a358', 'a388', 'a460', 'a462', 'a464', 'a470') THEN 'e446'
            ELSE NULL
        END AS event_id

    FROM tagged
    WHERE artist_id IS NOT NULL

)

SELECT DISTINCT * FROM mapped
