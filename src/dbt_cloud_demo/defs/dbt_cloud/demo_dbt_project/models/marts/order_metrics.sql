-- Marts layer: Order-level metrics and categorization
-- Dependencies: stg_orders, stg_customers

select
    o.order_id,
    o.customer_id,
    c.first_name || ' ' || c.last_name as customer_name,
    o.order_date,
    o.amount,
    o.status,

    -- Order categorization
    case
        when o.amount < 50 then 'small'
        when o.amount < 100 then 'medium'
        else 'large'
    end as order_size,

    -- Time-based metrics
    datediff('day', c.signup_date, o.order_date) as days_since_signup,

    -- Customer segment
    case
        when datediff('day', c.signup_date, o.order_date) <= 30 then 'new_customer'
        when datediff('day', c.signup_date, o.order_date) <= 90 then 'returning'
        else 'loyal'
    end as customer_segment

from {{ ref('stg_orders') }} o
inner join {{ ref('stg_customers') }} c
    on o.customer_id = c.customer_id
