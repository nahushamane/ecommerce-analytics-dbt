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
    items.quantity,
    items.price_per_unit,
    (items.quantity * items.price_per_unit) as item_sale_amount,
    products.product_name,
    products.category,
    products.product_cost,
    (items.quantity * products.product_cost) as item_cost_amount
from items
left join products on items.product_id = products.product_id