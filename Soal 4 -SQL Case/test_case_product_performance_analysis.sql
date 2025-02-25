WITH product_metrics AS (
    SELECT
        v2ProductName,
        SUM(COALESCE(productRevenue, 0)) AS total_revenue,
        SUM(COALESCE(productQuantity, 0)) AS total_quantity,
        SUM(COALESCE(productRefundAmount, 0)) AS total_refunds,
        SUM(COALESCE(productRevenue, 0)) - SUM(COALESCE(productRefundAmount, 0)) AS net_revenue
    FROM
        ecommerce_data
    GROUP BY
        v2ProductName
)
SELECT
    v2ProductName,
    total_revenue,
    total_quantity,
    total_refunds,
    net_revenue,
    RANK() OVER (ORDER BY net_revenue DESC) AS revenue_rank,
    CASE
        WHEN total_revenue > 0 AND (total_refunds / total_revenue) * 100 > 10 THEN TRUE
        ELSE FALSE
    END AS high_refund_flag
FROM
    product_metrics
WHERE
    v2ProductName IS NOT NULL
ORDER BY
    net_revenue DESC;