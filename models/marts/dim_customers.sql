with users as (
    select * from {{ ref('stg_users') }}
),
order_stats as (
    select
        user_id,
        count(order_id) as total_orders,
        sum(subtotal) as total_revenue, -- sourcing from fct_orders
        min(order_date) as first_order_date,
        max(order_date) as last_order_date
    from {{ ref('fct_orders') }}
    group by 1
)
select
    u.user_id as customer_id,
    u.email,
    u.customer_segment,
    coalesce(s.total_orders, 0) as total_orders,
    coalesce(s.total_revenue, 0) as total_revenue,
    
    -- Average Order Value
    case 
        when s.total_orders > 0 then round(s.total_revenue / s.total_orders, 2)
        else 0 
    end as avg_order_value,

    s.first_order_date,
    s.last_order_date,
    
    -- Days since last order
    (current_date - s.last_order_date) as days_since_last_order

from users u
left join order_stats s on u.user_id = s.user_id