{{ config(
    materialized='table'
) }}

with order_items as (
    select * from {{ ref('int_order_items_joined') }}
),

orders as (
    select * from {{ ref('stg_orders') }}
),

products as (
    select * from {{ ref('stg_products') }}
),

returns as (
    select * from {{ ref('stg_returns') }}
),

joined_data as (
    select
        items.product_id,
        products.product_name, -- Assuming column exists in stg_products
        products.category,
        items.quantity,
        items.total_item_amount as revenue,
        -- Simulate cost if not present in raw data (Using 60% of price as cost)
        (items.quantity * items.price_per_unit * 0.6) as cost,
        case when ret.return_id is not null then 1 else 0 end as is_returned,
        case when ret.return_id is not null then items.quantity else 0 end as returned_quantity
    from order_items as items
    left join orders on items.order_id = orders.order_id
    left join products on items.product_id = products.product_id
    left join returns as ret on items.order_id = ret.order_id
    where orders.order_status = 'completed' -- Only look at completed orders for performance
),

aggregated as (
    select
        product_id,
        product_name,
        category,
        sum(quantity) as total_quantity_sold,
        sum(revenue) as total_revenue,
        sum(cost) as total_cost,
        sum(returned_quantity) as total_returns
    from joined_data
    group by 1, 2, 3
)

select
    product_id,
    product_name,
    category,
    total_quantity_sold,
    total_revenue,
    total_returns,
    -- Return Rate: Returns / Sold
    case 
        when total_quantity_sold = 0 then 0 
        else round(total_returns::numeric / total_quantity_sold, 3) 
    end as return_rate,
    
    -- Gross Profit: Revenue - Cost
    (total_revenue - total_cost) as gross_profit,
    
    -- Profit Margin: (Revenue - Cost) / Revenue
    case 
        when total_revenue = 0 then 0
        else round((total_revenue - total_cost)::numeric / total_revenue, 3) 
    end as profit_margin
from aggregated
order by total_revenue desc