with source as (
    select * from {{ source('ecommerce', 'raw_users') }}
),
renamed as (
    select 
        user_id,
        email,
        -- cleaning dates
        CAST(signup_date AS DATE) as signup_date,
        country,
        customer_segment
    from source
)
select * from renamed