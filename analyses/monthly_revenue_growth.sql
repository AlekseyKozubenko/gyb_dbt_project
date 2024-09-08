-- models/monthly_revenue_growth.sql
WITH monthly_revenue AS (
    SELECT
        date_trunc('month', order_date_kyiv) AS month,
        SUM(total_revenue) AS monthly_revenue
    FROM {{ ref('fct_sales') }}
    GROUP BY 1
)
SELECT
    month,
    monthly_revenue,
    LAG(monthly_revenue) OVER (ORDER BY month) AS previous_month_revenue,
    round(((monthly_revenue - LAG(monthly_revenue) OVER (ORDER BY month)) / LAG(monthly_revenue) OVER (ORDER BY month))::NUMERIC * 100, 2) AS growth_percentage
FROM monthly_revenue;
