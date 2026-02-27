WITH cohort AS (
    SELECT 
        c.customer_unique_id,
        DATE_TRUNC('month', MIN(f.purchased_at)) AS cohort_month
    FROM {{ ref('fact_sales') }} f
    JOIN {{ ref('dim_customers') }} c ON f.customer_id = c.customer_id
    GROUP BY 1
),

activities AS (
    SELECT 
        c.customer_unique_id,
        ch.cohort_month,
        DATE_TRUNC('month', f.purchased_at) AS activity_month,
        (EXTRACT(YEAR FROM age(DATE_TRUNC('month', f.purchased_at), ch.cohort_month)) * 12 +
         EXTRACT(MONTH FROM age(DATE_TRUNC('month', f.purchased_at), ch.cohort_month))) AS month_number
    FROM {{ ref('fact_sales') }} f
    JOIN {{ ref('dim_customers') }} c ON f.customer_id = c.customer_id
    JOIN cohort ch ON c.customer_unique_id = ch.customer_unique_id
),

cohort_size AS (
    SELECT cohort_month, COUNT(DISTINCT customer_unique_id) AS total_customers
    FROM cohort
    GROUP BY 1
),

retention_counts AS (
    SELECT 
        cohort_month,
        month_number,
        COUNT(DISTINCT customer_unique_id) AS active_customers
    FROM activities
    GROUP BY 1, 2
)

SELECT 
    r.cohort_month,
    s.total_customers,
    r.month_number,
    r.active_customers,
    ROUND((r.active_customers::numeric / s.total_customers * 100), 2) AS retention_percentage
FROM retention_counts r
JOIN cohort_size s ON r.cohort_month = s.cohort_month
WHERE r.month_number <= 12
ORDER BY 1, 3;