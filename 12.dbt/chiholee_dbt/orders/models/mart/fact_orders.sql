{{
  config(
    materialized='incremental',
    unique_key='order_id',
    sort=['order_id'], 
    dist='order_dt',
    incremental_strategy='merge'
  )
}}



-- 테이블이 존재하지 않으면 초기 로딩 수행
{% if not is_incremental() %}

SELECT
    order_id,
    customer_id,
    product_id,
    order_cnt,
    order_price,
    order_dt,
    last_update_time
FROM
    {{ source('source', 'orders_from_kafka') }}

{% else %}

-- 증분 로딩 시 MERGE 사용
select *
from {{ source('source', 'orders_from_kafka') }}

{% endif %}