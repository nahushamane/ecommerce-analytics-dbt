with source as (
    select * from {{ source('ecommerce', 'raw_products') }}
),
renamed as (
    select
        product_id,
        product_name,
        category,
        supplier_id,
        CAST(cost AS NUMERIC) as product_cost
    from source
)
select * from renamed