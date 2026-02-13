-- Validates that no order has a negative revenue
select
    order_id,
    revenue  -- Changed from total_revenue to match fct_orders
from {{ ref('fct_orders') }}
where revenue < 0