{{
    config(
        materialized='incremental'
    )
}}

SELECT
    *
FROM
    {{ ref('stg_accesslog') }}

{% if is_incremental() %}
where timestamp > ( 
                        select max(timestamp) 
                        from {{ this }}
                   )
{% endif %}


