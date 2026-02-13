select
    date_trunc('month', order_date)::date as order_month,
    count(distinct order_id) as total_orders,
    count(distinct user_id) as active_customers,
    sum(revenue) as revenue,
    sum(profit) as profit,
    sum(total_refunded) as total_refunds_value
from {{ ref('fct_orders') }}
group by 1
order by 1 desc