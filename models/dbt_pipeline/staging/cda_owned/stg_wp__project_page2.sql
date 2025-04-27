{{ config(materialized='view') }}

WITH raw AS (
    SELECT *
    FROM {{ source('cda_owned', 'project_page2') }}
),

main AS (
    SELECT
        _airbyte_raw_id,
        _airbyte_extracted_at,
        _airbyte_meta,
        _airbyte_generation_id,
        CAST(id AS STRING)                                               AS project_id,
        slug,
        link,
        title.rendered                                                    AS title,
        status,
        type,
        CAST(author AS STRING)                                            AS author_id,
        CAST(featured_media AS STRING)                                    AS featured_media_id,
        REGEXP_REPLACE(TO_JSON_STRING(categories), r'[\[\]]', '')         AS categories,
        REGEXP_REPLACE(TO_JSON_STRING(class_list), r'[\[\]"]', '')        AS class_list,
        content,
        FORMAT_TIMESTAMP('%F %H:%M', CAST(date AS TIMESTAMP))             AS created_at,
        FORMAT_TIMESTAMP('%F %H:%M', CAST(date_gmt AS TIMESTAMP))         AS created_at_gmt,
        modified,
        template,
        modified_gmt,

        -- Hard-coded brand_id
        CASE
            WHEN CAST(id AS STRING) = '407' THEN 'b407'
            WHEN CAST(id AS STRING) = '410' THEN 'b410'
            WHEN CAST(id AS STRING) = '412' THEN 'b412'
            WHEN CAST(id AS STRING) = '415' THEN 'b415'
            WHEN CAST(id AS STRING) = '416' THEN 'b416'
            WHEN CAST(id AS STRING) = '417' THEN 'b417'
            WHEN CAST(id AS STRING) = '478' THEN 'b478'
            ELSE NULL
        END AS brand_id,

        -- Hard-coded art_show_id
        CASE
            WHEN CAST(id AS STRING) = '407' THEN 'as407'
            WHEN CAST(id AS STRING) = '410' THEN 'as410'
            WHEN CAST(id AS STRING) = '412' THEN 'as412'
            WHEN CAST(id AS STRING) = '415' THEN 'as415'
            WHEN CAST(id AS STRING) = '416' THEN 'as416'
            WHEN CAST(id AS STRING) = '417' THEN 'as417'
            ELSE NULL
        END AS art_show_id,

        -- Hard-coded event_id
        CASE
            WHEN CAST(id AS STRING) = '435' THEN 'e435'
            WHEN CAST(id AS STRING) = '441' THEN 'e441'
            WHEN CAST(id AS STRING) = '442' THEN 'e442'
            WHEN CAST(id AS STRING) = '446' THEN 'e446'
            ELSE NULL
        END AS event_id,

        -- Hard-coded artist_id
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
)

SELECT
    *,
    CASE
        WHEN artist_id IN ('a93', 'a379', 'a383', 'a455') THEN 'b407'
        WHEN artist_id IN ('a388') THEN 'b410'
        WHEN artist_id IN ('a392') THEN 'b412'
        WHEN artist_id IN ('a354', 'a358', 'a464', 'a360', 'a362', 'a370', 'a357', 'a470') THEN 'b415'
        WHEN artist_id IN ('a375', 'a379', 'a93') THEN 'b416'
        WHEN artist_id IN ('a364', 'a371', 'a368') THEN 'b417'
        WHEN artist_id IN ('a459') THEN 'b478'
        WHEN artist_id = 'a205' THEN NULL
        WHEN artist_id = 'a396' THEN NULL
        ELSE brand_id
    END AS brand_id_final,

    CASE
        WHEN artist_id IN ('a93', 'a379', 'a383', 'a455') THEN 'as407'
        WHEN artist_id IN ('a388') THEN 'as410'
        WHEN artist_id IN ('a392') THEN 'as412'
        WHEN artist_id IN ('a354', 'a358', 'a464', 'a360', 'a362', 'a370', 'a357', 'a470') THEN 'as415'
        WHEN artist_id IN ('a375', 'a379', 'a93') THEN 'as416'
        WHEN artist_id IN ('a364', 'a371', 'a368') THEN 'as417'
        WHEN artist_id IN ('a459') THEN 'as478'
        WHEN artist_id = 'a205' THEN NULL
        WHEN artist_id = 'a396' THEN NULL
        ELSE art_show_id
    END AS art_show_id_final,

    CASE
        WHEN artist_id IN ('a93', 'a379', 'a383', 'a455') THEN 'e435'
        WHEN artist_id IN ('a364', 'a371', 'a368') THEN 'e441'
        WHEN artist_id IN ('a93', 'a375', 'a379') THEN 'e442'
        WHEN artist_id IN ('a354', 'a357', 'a358', 'a388', 'a460', 'a462', 'a464', 'a470') THEN 'e446'
        WHEN artist_id = 'a205' THEN NULL
        WHEN artist_id = 'a396' THEN NULL
        ELSE event_id
    END AS event_id_final

FROM main
