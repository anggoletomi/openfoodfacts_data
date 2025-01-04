SELECT DISTINCT
    product_code,
    product_name,
    brands,
    categories,
    countries,
    manufacturing_places,
    expiration_date,
    image_url
FROM {{ ref('stg_openfoodfacts') }}
WHERE product_code IS NOT NULL