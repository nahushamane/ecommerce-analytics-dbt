with items as (
    select * from {{ ref('stg_order_items') }}
),
products as (
    select * from {{ ref('stg_products') }}
)
select
    items.order_item_id,
    items.order_id,
    items.product_id,
    products.product_category,
    items.quantity,
    items.price,
    (items.quantity * items.price) as total_item_amount
from items
left join products on items.product_id = products.product_id