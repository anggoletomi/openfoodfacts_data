SELECT
    s.product_code,
    s.nutriscore_grade,
    s.nova_group,
    s.sugars_100g,
    s.salt_100g,
    s.fat_100g,
    s.carbohydrates_100g,
    s.energy_kj_100g
FROM {{ ref('stg_openfoodfacts') }} s
WHERE s.product_code IS NOT NULL