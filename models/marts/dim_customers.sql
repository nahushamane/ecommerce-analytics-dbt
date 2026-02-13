with users as (
    select * from {{ ref('stg_users') }}
),
orders as (
    select * from {{ ref('int_order_metrics') }}
),
user_stats as (
    select
        user_id,
        count(order_id) as total_orders,
        sum(total_revenue) as total_revenue,
        min(order_date) as first_order_date,
        max(order_date) as last_order_date
    from orders
    group by 1
)
select
    u.user_id as customer_id,
    u.email,
    u.customer_segment,
    coalesce(s.total_orders, 0) as total_orders,
    coalesce(s.total_revenue, 0) as total_revenue,
    s.first_order_date,
    s.last_order_date
from users u
left join user_stats s on u.user_id = s.user_id