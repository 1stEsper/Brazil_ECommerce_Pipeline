SELECT
    product_id,
    product_category_name as category_name,
    product_weight_g as weight_g,
    product_length_cm as length_cm
FROM {{ source('olist_raw', 'products') }}