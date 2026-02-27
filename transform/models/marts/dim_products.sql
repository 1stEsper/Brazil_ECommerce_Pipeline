WITH products AS (
    SELECT * FROM {{ ref('stg_products')}}
), 

final AS (
    SELECT product_id, 
            category_name, 
            weight_g, 
            length_cm
    FROM products
)

SELECT * FROM final