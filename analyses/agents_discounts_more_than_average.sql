-- Agents discounts more than Average

WITH avg_discount AS (
    SELECT AVG(discount_amount) AS overall_avg_discount
    --FROM fct_sales
    FROM {{ ref('fct_sales') }}
),
agent_avg_discount AS (
    SELECT
        UNNEST(sales_agents) AS agent_name,
        AVG(discount_amount) AS agent_avg_discount
    --FROM fct_sales
    FROM {{ ref('fct_sales') }}
    GROUP BY UNNEST(sales_agents)
)
SELECT
    a.agent_name,
    a.agent_avg_discount,
    d.overall_avg_discount
FROM agent_avg_discount a
CROSS JOIN avg_discount d
WHERE a.agent_avg_discount > d.overall_avg_discount
ORDER BY a.agent_avg_discount DESC;