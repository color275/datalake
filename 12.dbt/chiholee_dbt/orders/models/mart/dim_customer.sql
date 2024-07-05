SELECT
    customer_id,
    first_name || ' ' || last_name AS full_name,
    email,
    phone_number,
    age,
    gender,
    address,
    date_joined
FROM
    {{ ref('stg_customer') }}