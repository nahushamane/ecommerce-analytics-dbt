with source as (
    select * from {{ source('ecommerce', 'raw_returns') }}
),
renamed as (
    select
        return_id,
        order_item_id,
        -- cleaning dates
        CAST(return_date AS DATE) as return_date,
        return_reason,
        refund_amount
    from source
)
select * from renamed