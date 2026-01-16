-- Staging layer: Basic transformations and type casting
-- Source: raw_customers seed

select
    customer_id,
    first_name,
    last_name,
    email,
    cast(signup_date as date) as signup_date,
    current_timestamp as _loaded_at

from {{ ref('raw_customers') }}
