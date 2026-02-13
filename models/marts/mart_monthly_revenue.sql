{{ config(
    materialized='table'
) }}

with orders as (
    select * from {{ ref('stg_orders') }}
),

order_metrics as (
    select * from {{ ref('int_order_metrics') }}
)

select
    -- Truncate date to the first of the month
    date_trunc('month', o.order_date)::date as order_month,
    
    -- High-level metrics
    count(distinct o.order_id) as total_orders,
    count(distinct o.user_id) as active_customers,
    sum(om.total_revenue) as total_revenue,
    sum(om.total_quantity) as total_items_sold,
    
    -- Averages
    round(avg(om.total_revenue), 2) as avg_order_value,
    round(sum(om.total_revenue) / count(distinct o.user_id), 2) as revenue_per_customer

from orders o
join order_metrics om on o.order_id = om.order_id
where o.order_status = 'completed' -- Filter for valid revenue only
group by 1
order by 1 desc