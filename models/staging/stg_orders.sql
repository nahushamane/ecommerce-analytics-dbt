with source as (
    select * from {{ source('ecommerce', 'raw_orders') }}
),
renamed as (
    select
        order_id,
        user_id,
        CAST(order_date AS DATE) as order_date,
        status as order_status,
        CAST(total_amount AS NUMERIC) as order_total_amount
    from source
)
select * from renamed