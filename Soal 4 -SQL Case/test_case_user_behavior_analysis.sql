WITH user_metrics AS (
    SELECT
        fullVisitorId,
        AVG(COALESCE(timeOnSite, 0)) AS avg_time_on_site,
        AVG(COALESCE(pageviews, 0)) AS avg_pageviews,
        AVG(COALESCE(sessionQualityDim, 0)) AS avg_session_quality
    FROM
        ecommerce_data
    GROUP BY
        fullVisitorId
),
overall_averages AS (
    SELECT
        AVG(COALESCE(timeOnSite, 0)) AS overall_avg_time,
        AVG(COALESCE(pageviews, 0)) AS overall_avg_pageviews
    FROM
        ecommerce_data
)
SELECT
    um.fullVisitorId,
    um.avg_time_on_site,
    um.avg_pageviews,
    um.avg_session_quality,
    oa.overall_avg_time,
    oa.overall_avg_pageviews
FROM
    user_metrics um
CROSS JOIN
    overall_averages oa
WHERE
    um.avg_time_on_site > oa.overall_avg_time
    AND um.avg_pageviews < oa.overall_avg_pageviews
ORDER BY
    (um.avg_time_on_site - oa.overall_avg_time) DESC;