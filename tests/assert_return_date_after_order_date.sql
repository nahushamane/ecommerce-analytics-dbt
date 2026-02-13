-- Validates that return_date is >= order_date
select
    o.order_id,
    o.order_date,
    o.first_return_date
from {{ ref('fct_orders') }} as o
where o.is_returned = true
  and o.first_return_date < o.order_date