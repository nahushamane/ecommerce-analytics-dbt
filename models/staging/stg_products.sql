with source as (
    select * from {{ source('ecommerce', 'raw_products') }}
),
renamed as (
    select
        product_id,
        product_name,
        category as product_category,
        supplier_id,
        cost as product_cost
    from source
)
select * from renamed