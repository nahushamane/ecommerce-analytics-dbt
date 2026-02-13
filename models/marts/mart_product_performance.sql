with items as (
    select * from {{ ref('int_order_items_joined') }}
),
returns as (
    select * from {{ ref('stg_returns') }}
),
joined as (
    select
        i.product_id,
        i.product_name,
        i.category,
        i.quantity,
        i.item_sale_amount,
        i.item_cost_amount,
        CASE WHEN r.return_id IS NOT NULL THEN i.quantity ELSE 0 END as returned_qty
    from items i
    left join returns r on i.order_item_id = r.order_item_id
)
select
    product_id,
    product_name,
    category,
    sum(quantity) as total_quantity_sold,
    sum(item_sale_amount) as total_revenue,
    sum(returned_qty) as total_returns,
    
    -- Return Rate
    case 
        when sum(quantity) = 0 then 0
        else round(sum(returned_qty)::numeric / sum(quantity), 3)
    end as return_rate,

    -- Gross Profit
    (sum(item_sale_amount) - sum(item_cost_amount)) as gross_profit,

    -- Profit Margin
    case
        when sum(item_sale_amount) = 0 then 0
        else round((sum(item_sale_amount) - sum(item_cost_amount))::numeric / sum(item_sale_amount), 3)
    end as profit_margin

from joined
group by 1, 2, 3