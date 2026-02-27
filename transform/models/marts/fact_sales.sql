WITH orders AS (
    SELECT * FROM {{ ref('stg_orders')}}
), 

order_items AS (
    SELECT * FROM {{ ref('stg_order_items')}}
), 

final AS (
    SELECT 
        oi.order_item_id,
        oi.order_id,
        oi.product_id,
        o.customer_id,
        o.purchased_at,
        oi.price,
        oi.shipping_cost,
        round((oi.price + oi.shipping_cost)::NUMERIC, 2) AS total_amount
    FROM order_items oi
    JOIN orders o ON oi.order_id = o.order_id
)

SELECT * FROM final