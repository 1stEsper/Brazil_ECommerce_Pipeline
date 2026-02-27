WITH raw_orders AS (
    SELECT * FROM {{ source('olist_raw', 'orders')}}
)

SELECT order_id, customer_id, order_status, 
        order_purchase_timestamp AS purchased_at, 
        order_approved_at AS approved_at,
        order_delivered_carrier_date AS shipped_at,
        order_delivered_customer_date as delivered_at,
        order_estimated_delivery_date as estimated_delivery_at
FROM raw_orders