with source as (
    select * from {{ source('ecommerce', 'raw_order_items') }}
),
renamed as (
    select
        order_item_id,
        order_id,
        product_id,
        quantity,
        CAST(price AS NUMERIC) as price_per_unit
    from source
)
select * from renamed