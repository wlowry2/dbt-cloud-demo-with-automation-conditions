-- Marts layer: Customer summary with order metrics
-- Dependencies: stg_customers, stg_orders

with customer_orders as (

    select
        customer_id,
        count(*) as total_orders,
        count(case when status = 'completed' then 1 end) as completed_orders,
        sum(case when status = 'completed' then amount else 0 end) as total_revenue,
        max(order_date) as last_order_date

    from {{ ref('stg_orders') }}
    group by customer_id

),

final as (

    select
        c.customer_id,
        c.first_name,
        c.last_name,
        c.email,
        c.signup_date,
        coalesce(o.total_orders, 0) as total_orders,
        coalesce(o.completed_orders, 0) as completed_orders,
        coalesce(o.total_revenue, 0) as lifetime_value,
        o.last_order_date,
        datediff('day', c.signup_date, current_date) as days_since_signup

    from {{ ref('stg_customers') }} c
    left join customer_orders o
        on c.customer_id = o.customer_id

)

select * from final
