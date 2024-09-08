-- Agent Statistics

WITH agent_stats AS (
    SELECT
        UNNEST(sales_agents) AS agent_name,
        AVG(total_revenue) AS avg_revenue,
        COUNT(*) AS num_sales,
        AVG(discount_amount) AS avg_discount,
        SUM(total_revenue) AS total_revenue
    --FROM fct_sales
    FROM {{ ref('fct_sales') }}
    GROUP BY UNNEST(sales_agents)
)
SELECT
    agent_name,
    avg_revenue,
    num_sales,
    avg_discount,
    total_revenue,
    RANK() OVER (ORDER BY total_revenue DESC) AS revenue_rank
FROM agent_stats
ORDER BY total_revenue DESC;