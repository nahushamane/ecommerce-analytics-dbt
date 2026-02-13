{{ config(
    materialized='incremental',
    unique_key='order_id'
) }}

with orders as (
    select * from {{ ref('int_orders_enriched') }}
),

users as (
    select * from {{ ref('stg_users') }}
)

select
    -- Identifiers
    o.order_id,
    o.order_date,
    o.user_id,
    
    -- Denormalized Dimensions (Example 3 requirement)
    u.email as customer_email,
    u.customer_segment,
    
    -- Status
    o.order_status,
    o.total_items,
    o.total_quantity,
    
    -- Financials (Mapping 'revenue' to multiple aliases for compatibility)
    o.revenue,               
    o.revenue as subtotal,   
    o.revenue as total_amount,
    (o.revenue - o.cost) as profit,
    
    -- Returns & Refunds (Monthly Mart requirement)
    o.is_returned,
    o.total_refunded,        -- Added back to fix the current error
    o.first_return_date,
    (o.first_return_date - o.order_date) as days_to_return

from orders o
left join users u on o.user_id = u.user_id

{% if is_incremental() %}
  where o.order_date >= (select max(order_date) from {{ this }})
{% endif %}