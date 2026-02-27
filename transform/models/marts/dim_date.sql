-- The time period of the Olist dataset (2016 - 2018)
WITH date_series AS (
    SELECT generate_series('2016-01-01'::date, '2018-12-31'::date, '1 day'::interval)::date AS date_day
), 

final AS (
    SELECT date_day,
            EXTRACT(YEAR FROM date_day) AS year, 
            EXTRACT(MONTH FROM date_day) AS month, 
            EXTRACT(DAY FROM date_day) AS day, 
            EXTRACT(DOW FROM date_day) AS day_of_week, 
            to_char(date_day, 'MONTH') AS month_name, 
            CASE 
                WHEN EXTRACT(DOW FROM date_day) IN (0,6) THEN true
                ELSE false
            END AS is_weekend
    FROM date_series
)

SELECT * FROM final