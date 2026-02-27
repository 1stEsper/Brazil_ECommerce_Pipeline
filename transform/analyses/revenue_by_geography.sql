-- analyses/revenue_by_geography.sql

WITH city_revenue AS (
    SELECT 
        c.state,
        c.city,
        ROUND(SUM(f.total_amount)::numeric, 2) AS total_revenue,
        COUNT(DISTINCT f.order_id) AS total_orders,
        COUNT(DISTINCT c.customer_unique_id) AS total_customers
    FROM {{ ref('fact_sales') }} f
    JOIN {{ ref('dim_customers') }} c ON f.customer_id = c.customer_id
    GROUP BY 1, 2
),

state_stats AS (
    SELECT 
        state,
        SUM(total_revenue) AS state_revenue,
        SUM(total_orders) AS state_orders,
        ROUND((SUM(total_revenue) / SUM(SUM(total_revenue)) OVER ()) * 100, 2) AS revenue_contribution_pct
    FROM city_revenue
    GROUP BY 1
)

SELECT 
    cr.state,
    s.state_revenue,
    s.revenue_contribution_pct,
    cr.city,
    cr.total_revenue AS city_revenue,
    RANK() OVER (PARTITION BY cr.state ORDER BY cr.total_revenue DESC) AS city_rank_in_state
FROM city_revenue cr
JOIN state_stats s ON cr.state = s.state
ORDER BY s.state_revenue DESC, cr.total_revenue DESC;