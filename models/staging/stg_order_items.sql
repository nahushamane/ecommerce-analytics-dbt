with source as (
    select * from {{ source('ecommerce', 'raw_order_items') }}
),
renamed as (
    select 
        order_item_id,
        order_id,
        product_id,
        quantity,
        price
    from source
)
select * from renamed