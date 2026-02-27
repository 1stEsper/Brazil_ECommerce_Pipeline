WITH customers AS (
    SELECT * FROM {{ ref('stg_customers')}}
), 
final AS (
    SELECT 
            customer_id, customer_unique_id, 
            MAX(city) AS city, 
            MAX(state) AS state
    FROM customers
    GROUP BY 1, 2
)

SELECT * FROM final