with users as (
    select * from {{ ref('stg_users') }}
),
order_stats as (
    select
        user_id,
        count(order_id) as total_orders,
        sum(revenue) as total_lifetime_revenue,
        sum(profit) as total_lifetime_profit,
        min(order_date) as first_order_date,
        max(order_date) as last_order_date
    from {{ ref('fct_orders') }}
    group by 1
)
select
    u.user_id as customer_id,
    u.email,
    u.country,
    u.customer_segment,
    coalesce(s.total_orders, 0) as total_orders,
    coalesce(s.total_lifetime_revenue, 0) as total_revenue,
    coalesce(s.total_lifetime_profit, 0) as total_profit,
    s.first_order_date,
    s.last_order_date,
    -- Calculate days since last order
    (current_date - s.last_order_date) as days_since_last_order
from users u
left join order_stats s on u.user_id = s.user_id