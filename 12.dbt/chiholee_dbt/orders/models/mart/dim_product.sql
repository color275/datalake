SELECT
    product_id,
    name AS product_name,
    category,
    price,
    img_path
FROM
    {{ ref('stg_product') }}