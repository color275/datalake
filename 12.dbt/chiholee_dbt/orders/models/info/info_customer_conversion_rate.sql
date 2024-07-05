WITH page_views AS (
    SELECT
        al.customer_id,
        c.full_name AS customer_name,
        DATE_TRUNC('day', al.timestamp) AS view_date,
        COUNT(*) AS page_views
    FROM
        {{ ref('fact_accesslog') }} al
    JOIN
        {{ ref('dim_customer') }} c ON al.customer_id = c.customer_id
    WHERE
        al.endpoint LIKE '%product%'
    GROUP BY
        al.customer_id, c.full_name, DATE_TRUNC('day', al.timestamp)
),

orders AS (
    SELECT
        o.customer_id,
        c.full_name AS customer_name,
        DATE_TRUNC('day', o.order_dt) AS order_dt,
        COUNT(o.order_id) AS orders
    FROM
        {{ ref('fact_orders') }} o
    JOIN
        {{ ref('dim_customer') }} c ON o.customer_id = c.customer_id
    GROUP BY
        o.customer_id, c.full_name, DATE_TRUNC('day', o.order_dt)
)

SELECT
    pv.customer_id,
    pv.customer_name,
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
    orders o ON pv.customer_id = o.customer_id AND pv.view_date = o.order_dt