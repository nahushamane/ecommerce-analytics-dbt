{{
    config(
        materialized='incremental',
        unique_key='order_id'
    )
}}

with orders as (
    select * from {{ ref('int_order_metrics') }}
),
returns as (
    select * from {{ ref('stg_returns') }}
)
select
    o.order_id,
    o.user_id,
    o.order_date,
    o.order_status,
    o.total_revenue,
    CASE WHEN r.return_id IS NOT NULL THEN TRUE ELSE FALSE END as is_returned,
    r.return_reason
from orders o
left join returns r on o.order_id = r.order_id

{% if is_incremental() %}
  -- this filter will only be applied on an incremental run
  where order_date >= (select max(order_date) from {{ this }})
{% endif %}