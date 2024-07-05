SELECT
    timestamp::timestamp AS timestamp,
    ip_address,
    http_method,
    endpoint,
    service,
    protocol,
    status_code,
    response_size,
    product_id,
    customer_id,
    order_id
FROM
    {{ source('source', 'access_log_topic_1st_processing') }}