-- Identify top 5 countries by total revenue
WITH country_revenue AS (
    SELECT
        country,
        SUM(COALESCE(transactionRevenue, totalTransactionRevenue, productRevenue, 0)) AS total_revenue
    FROM
        ecommerce_data
    GROUP BY
        country
    ORDER BY
        total_revenue DESC
    LIMIT 5
),
-- Calculate revenue by channel for top 5 countries
channel_revenue AS (
    SELECT
        e.country,
        e.channelGrouping,
        SUM(COALESCE(e.transactionRevenue, e.totalTransactionRevenue, e.productRevenue, 0)) AS revenue
    FROM
        ecommerce_data e
    INNER JOIN
        country_revenue cr ON e.country = cr.country
    GROUP BY
        e.country, e.channelGrouping
    ORDER BY
        e.country, revenue DESC
)
SELECT
    country,
    channelGrouping,
    revenue,
    revenue / SUM(revenue) OVER (PARTITION BY country) * 100 AS percentage_of_country_revenue
FROM
    channel_revenue
ORDER BY
    country, revenue DESC;