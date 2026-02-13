-- Analysis: Which day of the week has the highest sales?
with orders as (
    select * from {{ ref('fct_orders') }}
)

select
    dayname(order_date) as day_of_week,
    count(order_id) as total_orders,
    sum(total_amount) as total_revenue
from orders
group by 1
order by total_revenue desc