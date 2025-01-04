WITH raw AS (
    SELECT 
        RAW AS doc
    FROM {{ source('RAW', 'RAW_OPENFOODFACTS') }}
)
SELECT
    -- Basic product info
    doc:"code"::string                 AS product_code,
    doc:"product_name"::string         AS product_name,
    doc:"brands"::string               AS brands,
    doc:"categories"::string           AS categories,
    doc:"countries"::string            AS countries,

    -- Additional product attributes
    doc:"nutriscore_grade"::string     AS nutriscore_grade,
    doc:"nova_group"::float            AS nova_group,
    doc:"manufacturing_places"::string AS manufacturing_places,
    doc:"expiration_date"::string      AS expiration_date,
    doc:"image_url"::string            AS image_url,

    -- Nutrients
    doc:"nutriments"."sugars_100g"::float          AS sugars_100g,
    doc:"nutriments"."salt_100g"::float            AS salt_100g,
    doc:"nutriments"."fat_100g"::float             AS fat_100g,
    doc:"nutriments"."carbohydrates_100g"::float   AS carbohydrates_100g,
    doc:"nutriments"."energy-kj_100g"::float       AS energy_kj_100g

FROM raw