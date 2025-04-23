WITH source AS (

    SELECT *
    FROM {{ source('cda_owned', 'project') }}

)

SELECT

    -- Primary identifiers
    CAST(source.id AS STRING) AS project_id,
    source.slug,
    source.link,

    -- Titles
    source.title.rendered AS title,

    -- Publication and status fields
    source.status,
    source.type,

    -- Author and media
    CAST(source.author AS STRING) AS author_id,
    CAST(source.featured_media AS STRING) AS featured_media_id,

    -- Categorical info
    source.categories,
    source.class_list,

    -- Optional content (if empty, can be NULL downstream)
    source.content.rendered AS content_rendered,

    -- Timestamp formatting
    FORMAT_TIMESTAMP('%F %H:%M', CAST(source.date AS TIMESTAMP)) AS created_at,
    FORMAT_TIMESTAMP('%F %H:%M', CAST(source.modified AS TIMESTAMP)) AS modified_at,

    -- Airbyte metadata
    source._airbyte_raw_id,
    source._airbyte_extracted_at,
    source._airbyte_meta,
    source._airbyte_generation_id

FROM source

