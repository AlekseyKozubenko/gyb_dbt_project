-- models/fct_sales.sql
WITH sales_data AS (
    SELECT
        COALESCE("Reference ID", 'N/A') AS sale_id,
        COALESCE("Product Name", 'N/A') AS product_name,
        array_remove(array_agg(DISTINCT COALESCE("Sales Agent Name", 'N/A')), 'N/A') AS sales_agents,
        COALESCE("Country", 'N/A') AS country,
        array_remove(array_agg(DISTINCT COALESCE("Campaign Name", 'N/A')), 'N/A') AS campaign,
        array_remove(array_agg(DISTINCT COALESCE("Source", 'N/A')), 'N/A') AS source,
        COALESCE("Total Amount ($)", 0) AS total_amount,
        COALESCE("Total Rebill Amount", 0) AS rebill_revenue,
        COALESCE("Number Of Rebills", 0) AS number_of_rebills,
        COALESCE("Discount Amount ($)", 0) AS discount_amount,
        COALESCE("Returned Amount ($)", 0) AS returned_amount,
        "Return Date Kyiv" AS return_date_kyiv,
        "Return Date Kyiv" AT TIME ZONE 'UTC' AS return_date_utc,
        "Return Date Kyiv" AT TIME ZONE 'America/New_York' AS return_date_ny,
        "Order Date Kyiv" AS order_date_kyiv,
        "Order Date Kyiv" AT TIME ZONE 'UTC' AS order_date_utc,
        "Order Date Kyiv" AT TIME ZONE 'America/New_York' AS order_date_ny
    FROM {{ ref('sample') }}
    GROUP BY
        "Reference ID", "Product Name", "Country", 
        "Total Amount ($)", "Total Rebill Amount", "Number Of Rebills",
        "Discount Amount ($)", "Returned Amount ($)", "Return Date Kyiv", "Order Date Kyiv"
)
SELECT
    sale_id,
    product_name,
    sales_agents,
    country,
    campaign,
    source,
    (total_amount + COALESCE(rebill_revenue, 0) - COALESCE(returned_amount, 0)) AS total_revenue,
    rebill_revenue,
    number_of_rebills,
    discount_amount,
    returned_amount,
    return_date_kyiv,
    return_date_utc,
    return_date_ny,
    CASE
        WHEN return_date_kyiv IS NOT NULL AND order_date_kyiv IS NOT NULL
        THEN DATE_PART('day', return_date_kyiv::timestamp - order_date_kyiv::timestamp)
        ELSE NULL
    END AS days_to_return,
    order_date_kyiv,
    order_date_utc,
    order_date_ny
FROM sales_data