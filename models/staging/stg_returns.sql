with source as (
    select * from {{ source('ecommerce', 'raw_returns') }}
),
renamed as (
    select
        return_id,
        order_item_id, -- Links to specific items, not just the order
        CAST(return_date AS DATE) as return_date,
        return_reason,
        CAST(refund_amount AS NUMERIC) as refund_amount
    from source
)
select * from renamed