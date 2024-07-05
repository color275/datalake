SELECT
    customer_id,
    username,
    first_name,
    last_name,
    email,
    phone_number,
    age,
    gender,
    address,
    date_joined,
    last_login,
    is_active,
    last_update_time
FROM
    {{ source('source', 'customer_from_kafka') }}
WHERE
    is_active = 1