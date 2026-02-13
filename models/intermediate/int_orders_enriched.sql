with orders as (
    select * from {{ ref('stg_orders') }}
),
items as (
    select * from {{ ref('int_order_items_joined') }}
),
returns as (
    select * from {{ ref('stg_returns') }}
),
-- 1. Aggregate Items to Order Level
item_aggs as (
    select
        order_id,
        count(order_item_id) as number_of_items,
        sum(quantity) as total_quantity,
        sum(item_sale_amount) as calculated_revenue,
        sum(item_cost_amount) as total_cost
    from items
    group by 1
),
-- 2. Aggregate Returns to Order Level (via order_item_id)
return_aggs as (
    select
        i.order_id,
        count(distinct r.return_id) as return_count,
        sum(r.refund_amount) as total_refunded
    from returns r
    join items i on r.order_item_id = i.order_item_id
    group by 1
)

select
    o.order_id,
    o.user_id,
    o.order_date,
    o.order_status,
    -- Use calculated revenue from items for consistency
    coalesce(ia.calculated_revenue, 0) as revenue,
    coalesce(ia.total_cost, 0) as cost,
    coalesce(ia.number_of_items, 0) as number_of_items,
    coalesce(ia.total_quantity, 0) as total_quantity,
    
    -- Return Logic
    coalesce(ra.return_count, 0) as return_count,
    coalesce(ra.total_refunded, 0) as total_refunded,
    CASE WHEN ra.return_count > 0 THEN TRUE ELSE FALSE END as is_returned
from orders o
left join item_aggs ia on o.order_id = ia.order_id
left join return_aggs ra on o.order_id = ra.order_id