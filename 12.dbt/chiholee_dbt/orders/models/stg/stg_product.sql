SELECT
    product_id,
    name,
    category,
    price,
    img_path,
    last_update_time
FROM
    {{ source('source', 'product_from_kafka') }}