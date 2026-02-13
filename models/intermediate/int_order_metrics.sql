with items as (
    select * from {{ ref('int_order_items_joined') }}
),
orders as (
    select * from {{ ref('stg_orders') }}
),
order_aggs as (
    select
        order_id,
        sum(total_item_amount) as total_revenue,
        sum(quantity) as total_quantity,
        count(order_item_id) as total_items
    from items
    group by 1
)
select
    o.*,
    oa.total_revenue,
    oa.total_quantity,
    oa.total_items
from orders o
left join order_aggs oa on o.order_id = oa.order_id