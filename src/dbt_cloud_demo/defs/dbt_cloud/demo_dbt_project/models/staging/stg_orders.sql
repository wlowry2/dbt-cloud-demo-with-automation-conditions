-- Staging layer: Clean and standardize orders data
-- Source: raw_orders seed

select
    order_id,
    customer_id,
    cast(order_date as date) as order_date,
    amount,
    lower(status) as status,
    current_timestamp as _loaded_at

from {{ ref('raw_orders') }}

-- Data quality filter
where amount > 0
