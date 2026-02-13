-- Validates that no order has a negative revenue
select
    order_id,
    revenue
from {{ ref('fct_orders') }}
where revenue < 0