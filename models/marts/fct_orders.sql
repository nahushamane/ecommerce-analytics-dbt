{{ config(
    materialized='incremental',
    unique_key='order_id'
) }}

select
    order_id,
    user_id,
    order_date,
    order_status,
    revenue,
    cost,
    (revenue - cost) as profit,
    number_of_items,
    total_quantity,
    is_returned,
    return_count,
    total_refunded
from {{ ref('int_orders_enriched') }}

{% if is_incremental() %}
  where order_date >= (select max(order_date) from {{ this }})
{% endif %}