with source as (
    select * from {{ source('ecommerce', 'raw_orders') }}
),
renamed as (
    select
        order_id,
        user_id,
        -- cleaning dates
        CAST(order_date AS DATE) as order_date,
        status as order_status,
        total_amount
    from source
)
select * from renamed