WITH page_views AS (
    SELECT
        al.product_id,
        p.product_name,
        DATE_TRUNC('day', al.timestamp) AS view_date,
        COUNT(*) AS page_views
    FROM
        {{ ref('fact_accesslog') }} al
    JOIN
        {{ ref('dim_product') }} p ON al.product_id = p.product_id
    WHERE
        al.endpoint LIKE '%product%'
    GROUP BY
        al.product_id, p.product_name, DATE_TRUNC('day', al.timestamp)
),

orders AS (
    SELECT
        o.product_id,
        p.product_name,
        DATE_TRUNC('day', o.order_dt) AS order_dt,
        COUNT(o.order_id) AS orders
    FROM
        {{ ref('fact_orders') }} o
    JOIN
        {{ ref('dim_product') }} p ON o.product_id = p.product_id
    GROUP BY
        o.product_id, p.product_name, DATE_TRUNC('day', o.order_dt)
)

SELECT
    pv.product_id,
    pv.product_name,
    pv.view_date,
    pv.page_views,
    COALESCE(o.orders, 0) AS orders,
    CASE
        WHEN pv.page_views = 0 THEN 0
        ELSE COALESCE(o.orders, 0)::float / pv.page_views
    END AS conversion_rate
FROM
    page_views pv
LEFT JOIN
    orders o ON pv.product_id = o.product_id AND pv.view_date = o.order_dt