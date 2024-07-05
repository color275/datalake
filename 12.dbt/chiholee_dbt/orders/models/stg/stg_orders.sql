{{
  config(
    materialized='table',
  )
}}

SELECT
    -- iceberg 오류로 중복값 존재 ㅠ
    distinct
    order_id,
    customer_id,
    product_id,
    order_cnt,
    order_price,
    order_dt,
    last_update_time
FROM
    {{ source('source', 'orders_from_kafka') }}
